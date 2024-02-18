package flower

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

var (
	errFlowNameIsNotSet       = errors.New("flow name is not set")
	errFlowProcFuncIsNotSet   = errors.New("flow processing functions is not set")
	errFlowAlreadyInitialized = errors.New("flow queue is already initialized")

	defaultWorkerCount = 1
)

// processFunc процессинговая функция. Возвращает массив обработанных результатов,
// массив результатов компенсации (тех объектов, которые не были обработаны) и ошибку
type processFunc[T any] func(ctx context.Context, input ...T) (results []T, compensate []T, err error)

// errFunc функция обработки ошибок
type errFunc func(context.Context, error)

// flow структура потока данных. содержит пул воркеров и функции обработки данных
type flow[T any] struct {
	id string
	// имя потока
	name string

	// перечень потоков, которым необходимо передать результат выполнения
	outFlows []string

	queue Queue[T]

	// функции обработки данных
	procFs []processFunc[T]

	// функции обработки ошибки
	errF errFunc

	// кол-во воркеров в пуле для данного потока
	workerCount int

	// указывает на наличие изменений (при получении/завершении обработки)
	changedSig chan struct{}

	// кол-во процессов, в текущий момент времени
	pendingCount atomic.Int64

	// флаг открытия потока. При отправке данных в поток - устанавливается в True
	opened atomic.Bool

	// флаг инициализации
	initialized atomic.Bool

	inCh  chan T
	outCh chan flowOut[T]
}

// flowOut результат выполнения
type flowOut[T any] struct {
	target string
	data   T
}

// newFlow инициализирует экземпляр потока данных
func newFlow[T any](flowerID string, name string, q Queue[T], outF []string, wCount int, pFunc ...processFunc[T]) (*flow[T], error) {
	if len(pFunc) == 0 {
		return nil, errFlowProcFuncIsNotSet
	}
	if name == "" {
		return nil, errFlowNameIsNotSet
	}

	if wCount == 0 {
		wCount = defaultWorkerCount
	}

	id := fmt.Sprintf("%s-%s", flowerID, name)

	return &flow[T]{
		id:          id,
		name:        name,
		outFlows:    outF,
		procFs:      pFunc,
		workerCount: wCount,
		changedSig:  make(chan struct{}, 1),
		inCh:        make(chan T, 1),
		outCh:       make(chan flowOut[T]),
		queue:       q,
	}, nil
}

// incPending увеличивает счетчик на 1
func (f *flow[T]) incPending() {
	f.pendingCount.Add(1)
}

func (f *flow[T]) push(ctx context.Context, val T) error {
	return f.queue.Push(val)
}

// decPending уменьшает счетчик на 1
func (f *flow[T]) decPending() {
	f.pendingCount.Add(-1)
}

// run инициализирует пул воркеров и запускает на них обработку потока данных
func (f *flow[T]) run(ctx context.Context, doneCh, suspendCh chan struct{}) error {
	defer close(f.outCh)
	// проверка и установка флага инициализации
	if f.initialized.Load() {
		return errFlowAlreadyInitialized
	}
	f.initialized.Store(true)

	var wg sync.WaitGroup

	// инициализируем пул воркеров
	for i := 0; i < f.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.do(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		f.readQueue(ctx, doneCh, suspendCh)
	}()

	wg.Wait()

	log.Debugf("flow run is done")

	return nil
}

// readQueue читает очередь и отправляет данные в канал
func (f *flow[T]) readQueue(ctx context.Context, doneCh, suspendCh chan struct{}) {
	defer close(f.inCh)
watchLoop:
	for {
		select {
		case <-ctx.Done():
			log.Debugf("stage %s is canceled", f.id)
			break watchLoop
		case <-doneCh:
			log.Debugf("stage %s context is done", f.id)
			break watchLoop
		case <-suspendCh:
			log.Debugf("stage %s context is done by suspend", f.id)
			break watchLoop
		case <-f.queue.Signal():
			next, ok, err := f.queue.Next()
			if !ok {
				break watchLoop
			}
			if err != nil {
				log.Errorf("stage %s get next val error: %s", f.id, err)
				continue
			}
			f.inCh <- next
		}
	}
	log.Debugf("readQueue is done")
}

// do запускает цикл потока. Получает данные из входного канала, выполняет процессинг и отправляет данные в следующие потоки
func (f *flow[T]) do(ctx context.Context) {
	for val := range f.inCh {
		// увеличиваем счетчик ожидания и последовательно выполняем процессинговые функции
		// отправляем ошибки в функцию обработки, если она определена
		f.incPending()
		for i, pf := range f.procFs {
			results, compensate, err := pf(ctx, val)
			if err != nil {
				if f.errF != nil {
					f.errF(ctx, err)
				}
				log.Debugf("flow %s was detected error at %d processing function result", f.name, i)
			}
			for _, cmp := range compensate {
				f.outCh <- flowOut[T]{
					target: f.name,
					data:   cmp,
				}
				log.Debugf("flow %s was detected retry obj: %v at %d processing function result", f.name, cmp, i)
			}

			// отправляем результат в указанные в outFlows потоки
			for _, result := range results {
				for _, target := range f.outFlows {
					f.outCh <- flowOut[T]{
						target: target,
						data:   result,
					}
				}
			}
		}
		f.decPending()
	}
}

func (f *flow[T]) closeQueue(clear bool) error {
	if clear {
		return f.queue.Clear()
	}
	return nil
}
