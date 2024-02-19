package pipeliner

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
	ErrPipeNotFound          = errors.New("pipe not found")
	ErrPipelinerClosed       = errors.New("pipeliner is closed")
	ErrPipelinerNameRequired = errors.New("pipeliner name is required")
	ErrPipeNameRequired      = errors.New("pipe name is required")
	ErrCreteQueueFuncIsNil   = errors.New("create queue function is req required")
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

// pipeRegistry используется для хранения экземпляров потоков
type pipeRegistry[T any] map[string]*pipe[T]

// CreateQueueFunc функция создания очереди. (pipeliner.name + "-" + pipe.name + "-" + queue.name)
type CreateQueueFunc[T any] func(name string) (Queue[T], error)

// Pipeliner координатор потоков
type Pipeliner[T any] struct {
	// уникальный идентификатор.
	// необходим для создания уникальных очередей
	// (pipeliner.name + "-" + pipe.name + "-" + queue.name)
	name string

	// флаг ожидания окончания ввода данных
	waitInput atomic.Bool

	// флаг завершения работы
	closed atomic.Bool

	// счетчик объектов в обработке
	pendingCnt atomic.Int64

	// сигнальные каналы завершения
	doneCh    chan struct{}
	suspendCh chan struct{}

	// функция отмены контекстов дочерних процессов
	pipeCancelFunc context.CancelFunc
	// функция создания очередей
	cqf CreateQueueFunc[T]

	// хранилище потоков
	reg pipeRegistry[T]
}

// NewPipeliner создает новый экземпляр координатора
// waitInput определяет поведение оркестратора, для завершения обработки ожидает false.
// т.е. даже если все элементы обработаны - watcher не закроет очереди и процесс, т.к. будет ожидать еще данных.
// cfq - функция создания очереди для потоков (AddPipe)
func NewPipeliner[T any](id string, waitInput bool, cfq CreateQueueFunc[T]) (*Pipeliner[T], error) {
	if id == "" {
		return nil, ErrPipelinerNameRequired
	}

	if cfq == nil {
		return nil, ErrCreteQueueFuncIsNil
	}

	f := &Pipeliner[T]{
		name:      id,
		reg:       make(pipeRegistry[T]),
		cqf:       cfq,
		doneCh:    make(chan struct{}),
		suspendCh: make(chan struct{}),
	}

	f.waitInput.Store(waitInput)

	return f, nil
}

// Run запускает основной процесс выполнения координатора
// запускает процессы обработки всех потоков (pipe.run),
// процесс отслеживания состояния очередей для завершения (watchQueues),
// процесс обработки результатов работы потоков (watchResults)
func (p *Pipeliner[T]) Run(ctx context.Context) error {
	defer p.closed.Store(true)
	var wg sync.WaitGroup

	pipeCtx, cancel := context.WithCancel(ctx)
	p.pipeCancelFunc = cancel

	pipes := p.pipes()

	// запуск потоков
	for _, pInst := range pipes {
		wg.Add(1)
		go func(plInst *pipe[T]) {
			defer wg.Done()
			err := plInst.run(pipeCtx, p.doneCh, p.suspendCh)
			if err != nil {
				log.Errorf("Run pipeliner error: %s", err)
			}
		}(pInst)
	}

	// процесс отслеживания состояния очередей
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.watchQueues(ctx)
		close(p.doneCh)
	}()

	// процесс обработки данных из потоков
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.watchResults(ctx)
	}()

	wg.Wait()

	return nil
}

// Suspend останавливает процесс обработки.
// флаг force указывает на необходимость отмены контекста дочерних процессов,
// не дожидается завершения обработки уже отправленных в processFunc данных
func (p *Pipeliner[T]) Suspend(force bool) {
	close(p.suspendCh)

	if force {
		// отменяем контекст потоков, чтобы донести отмену процессинговой функция
		p.pipeCancelFunc()
	}

}

// CleanUP очищает очереди всех потоков
func (p *Pipeliner[T]) CleanUP() error {
	for _, f := range p.pipes() {
		err := f.closeQueue(true)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddPipe регистрирует поток в координаторе.
// Создает соответсвующую очередь
func (p *Pipeliner[T]) AddPipe(params PipeParams[T]) error {
	if p.closed.Load() {
		return ErrPipelinerClosed
	}
	if params.Name == "" {
		return ErrPipeNameRequired
	}

	q, err := p.cqf(p.name + "-" + params.Name)
	if err != nil {
		return fmt.Errorf("create queue error: %s", err)
	}

	pInst, err := newPipe(p.name, q, params)
	if err != nil {
		return err
	}

	p.addPipe(params.Name, pInst)
	return nil
}

// Push отправляет данные в соответствующий поток
func (p *Pipeliner[T]) Push(name string, val ...T) error {
	plInst, ok := p.pipe(name)
	if !ok || plInst == nil {
		return fmt.Errorf("pipe %s error %w", name, ErrPipeNotFound)
	}

	if p.closed.Load() {
		return ErrPipelinerClosed
	}

	return plInst.push(context.Background(), val...)
}

func (p *Pipeliner[T]) pipe(name string) (*pipe[T], bool) {
	plInst, ok := p.reg[name]
	return plInst, ok
}

func (p *Pipeliner[T]) addPipe(name string, plInst *pipe[T]) {
	p.reg[name] = plInst
}

func (p *Pipeliner[T]) pipes() pipeRegistry[T] {
	reg := p.reg
	return reg
}

// watchResults процесс получения данных из всех потоков
func (p *Pipeliner[T]) watchResults(ctx context.Context) {

	pipes := p.pipes()
	pipesOutCh := make([]chan pipeOut[T], 0, len(pipes))

	for _, pInst := range pipes {
		pipesOutCh = append(pipesOutCh, pInst.outCh)

	}
	mChans := mergeChannels(pipesOutCh...)

	for data := range mChans {
		err := p.Push(data.target, data.data)
		if err != nil {
			log.Errorf("push data to pipe: %s error: %s", p.name, err)
			continue
		}
	}

	log.Debugf("watchResults is done")
}

// watchQueues процесс отслеживания состояния очереди.
// при отмене контекста или "паузе" (suspend) - прекращает работу
func (p *Pipeliner[T]) watchQueues(ctx context.Context) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
watchLoop:
	for {
		select {
		case <-ctx.Done():
			break watchLoop
		case <-p.doneCh:
			break watchLoop
		case <-p.suspendCh:
			break watchLoop
		case <-ticker.C:
			// если координатор ждет завершения ввода - продолжаем наблюдение
			if p.waitInput.Load() {
				continue
			}
			// если есть хоть один "живой" поток - продолжаем наблюдение
			for _, plInst := range p.pipes() {
				mc, pc := plInst.queue.Len(), plInst.pendingCount.Load()
				if mc > 0 || pc > 0 {
					log.Printf("queue: %s; messages in queue: %d; messages is pending: %d\n", plInst.name, mc, pc)
					continue watchLoop
				}
			}
			// если мы дошли до этого кода - нет активных задач или получили сигнал выхода
			break watchLoop
		}
	}
	// закрываем очереди
	for _, pInst := range p.pipes() {
		// ожидаем корректного завершения обработки
		for pc := pInst.pendingCount.Load(); pc != 0; pc = pInst.pendingCount.Load() {
			time.Sleep(checkInterval)
			log.Printf("queue: %s is pending: %d\n", pInst.name, pc)
		}

		err := pInst.closeQueue(false)
		if err != nil {
			log.Printf("close queue error: %s", err)
		}
		log.Printf("queue %s is closed", pInst.id)
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
