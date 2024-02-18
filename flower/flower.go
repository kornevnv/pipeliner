package flower

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

const checkInterval = 3 * time.Second

var (
	ErrFlowNotFound           = errors.New("flow not found")
	ErrFlowerClosed           = errors.New("flower is closed")
	ErrFlowerIdRequired       = errors.New("flower id is required")
	ErrUnsupportedQueueEngine = errors.New("unsupported queue engine")
)

// Queue определяет интерфейс очереди
type Queue[T any] interface {
	Push(val T) error
	Next() (T, bool, error)
	Len() int
	Clear() error
	// Signal канал для сигнализации появления элемента в очереди
	Signal() <-chan struct{}
}

type registry[T any] map[string]*flow[T]

type queueEngineParams map[string]string

type CreateQueueFunc[T any] func(name string) (Queue[T], error)

type Flower[T any] struct {
	id string

	// хранилище потоков
	reg registry[T]

	waitInput atomic.Bool

	closed atomic.Bool

	pendingCnt atomic.Int64

	// функция создания очередей
	cqf CreateQueueFunc[T]

	// сигнальные каналы завершения
	doneCh    chan struct{}
	suspendCh chan struct{}

	flowsCancelFunc context.CancelFunc
}

// NewFlower создает новый экземпляр
// waitInput определяет поведение оркестратора, для завершения обработки ожидает false.
// т.е. даже если все элементы обработаны - watcher не закроет очереди и процесс, т.к. будет ожидать еще данных.
// cfq - функция создания очереди для потоков (AddFlow)
func NewFlower[T any](id string, waitInput bool, cfq CreateQueueFunc[T]) (*Flower[T], error) {
	if id == "" {
		return nil, ErrFlowerIdRequired
	}

	if cfq == nil {
		return nil, ErrUnsupportedQueueEngine
	}

	f := &Flower[T]{
		id:  id,
		reg: make(registry[T]),
		//	queueEngine: defaultQueueEngine,
		cqf:       cfq,
		doneCh:    make(chan struct{}),
		suspendCh: make(chan struct{}),
	}

	f.waitInput.Store(waitInput)

	return f, nil
}

// Run запускает основной процесс выполнения координатора
// запускает процессы обработки всех потоков (fl.run),
// процесс отслеживания состояния очередей для завершения (watchQueues),
// процесс обработки результатов работы потоков (watchResults)
func (f *Flower[T]) Run(ctx context.Context) error {
	defer f.closed.Store(true)
	var wg sync.WaitGroup

	flowsCtx, cancel := context.WithCancel(ctx)
	f.flowsCancelFunc = cancel

	flows := f.flows()

	// запуск потоков
	for _, fl := range flows {
		wg.Add(1)
		go func(fl *flow[T]) {
			defer wg.Done()
			err := fl.run(flowsCtx, f.doneCh, f.suspendCh)
			if err != nil {
				log.Errorf("Run flower error: %s", err)
			}
		}(fl)
	}

	// процесс отслеживания состояния очередей
	wg.Add(1)
	go func() {
		defer wg.Done()
		f.watchQueues(ctx)
		close(f.doneCh)
	}()

	// процесс обработки данных из потоков
	wg.Add(1)
	go func() {
		defer wg.Done()
		f.watchResults(ctx)
	}()

	wg.Wait()

	return nil
}

// Suspend останавливает процесс обработки
func (f *Flower[T]) Suspend(force bool) {
	close(f.suspendCh)

	if force {
		// отменяем контекст потоков, чтобы донести отмену процессинговых функция
		f.flowsCancelFunc()
	}

}

// CleanUP очищает очереди всех потоков
func (f *Flower[T]) CleanUP() error {
	for _, f := range f.flows() {
		err := f.closeQueue(true)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddFlow регистрирует поток в координаторе
// В зависимости от выбранном в координаторе queueEngine создает соответсвующую очередь
func (f *Flower[T]) AddFlow(name string, wc int, out []string, pf ...processFunc[T]) error {
	if f.closed.Load() {
		return ErrFlowerClosed
	}

	queueName := f.id + "-" + name
	q, err := f.cqf(queueName)
	if err != nil {
		return fmt.Errorf("create queue error: %s", err)
	}

	fl, err := newFlow(f.id, name, q, out, wc, pf...)
	if err != nil {
		return err
	}

	f.addFlow(name, fl)
	return nil
}

// Push отправляет данные в соответствующий поток
func (f *Flower[T]) Push(name string, val T) error {
	fl, ok := f.flow(name)
	if !ok || fl == nil {
		return fmt.Errorf("flow %s error %w", name, ErrFlowNotFound)
	}

	if f.closed.Load() {
		return ErrFlowerClosed
	}

	return fl.push(context.Background(), val)
}

func (f *Flower[T]) flow(name string) (*flow[T], bool) {
	fl, ok := f.reg[name]
	return fl, ok
}

func (f *Flower[T]) addFlow(name string, fl *flow[T]) {
	f.reg[name] = fl
}

func (f *Flower[T]) flows() registry[T] {
	reg := f.reg
	return reg
}

func (f *Flower[T]) watchResults(ctx context.Context) {

	flows := f.flows()
	flowsOutCh := make([]chan flowOut[T], 0, len(flows))

	for _, fl := range flows {
		flowsOutCh = append(flowsOutCh, fl.outCh)

	}
	mChans := mergeChannels(flowsOutCh...)

	for data := range mChans {
		err := f.Push(data.target, data.data)
		if err != nil {
			log.Errorf("push data to flow: %s error: %s", f.id, err)
			continue
		}
	}

	log.Debugf("watchResults is done")
}

func (f *Flower[T]) watchQueues(ctx context.Context) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
watchLoop:
	for {
		select {
		case <-ctx.Done():
			break watchLoop
		case <-f.doneCh:
			break watchLoop
		case <-f.suspendCh:
			break watchLoop
		case <-ticker.C:
			// если координатор ждет завершения ввода - продолжаем наблюдение
			if f.waitInput.Load() {
				continue
			}
			// если есть хоть один "живой" поток - продолжаем наблюдение
			for _, fl := range f.flows() {
				mc, pc := fl.queue.Len(), fl.pendingCount.Load()
				if mc > 0 || pc > 0 {
					log.Printf("queue: %s; messages in queue: %d; messages is pending: %d\n", fl.name, mc, pc)
					continue watchLoop
				}
			}
			// если мы дошли до этого кода - нет активных задач или получили сигнал выхода
			break watchLoop
		}
	}
	// закрываем очереди

	for _, fl := range f.flows() {
		// ожидаем корректного завершения обработки
		for pc := fl.pendingCount.Load(); pc != 0; pc = fl.pendingCount.Load() {
			time.Sleep(checkInterval)
			log.Printf("queue: %s is pending: %d\n", fl.name, pc)
		}

		err := fl.closeQueue(false)
		if err != nil {
			log.Printf("close queue error: %s", err)
		}
		log.Printf("queue %s is closed", fl.id)
	}

	log.Debugf("watchQueues is done")
}

func mergeChannels[T any](cs ...chan T) <-chan T {
	var wg sync.WaitGroup
	out := make(chan T)

	output := func(c <-chan T) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
