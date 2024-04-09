package step

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

var (
	errStepNameIsRequired = errors.New("step name is required")
	errStepIsClosed       = errors.New("step is closed")

	defaultWorkerCount = 1
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

// Counter счетчик операций, необходим для однозначного определения завершения всех активных операций
type Counter interface {
	Inc()
	Dec()
	Count() int64
}

// ProcessFunc процессинговая функция. Возвращает флаг необходимости ретрая (!ok) и ошибку.
// retry производится путем повторной публикации объекта в очередь
type ProcessFunc[T any] func(ctx context.Context, input T) (ok bool, err error)

// FilterFunc фильтрующая функция. Выполняется ДО процессинговой функции
type FilterFunc[T any] func(ctx context.Context, input T) (output *T, err error)

// ErrorFunc функция обработки ошибок
type ErrorFunc func(context.Context, error)

type Step[T any] struct {
	// имя этапа
	name string

	// кол-во воркеров
	workerCount int

	// очередь этапа
	queue Queue[T]

	// функции обработки данных
	filterFunc FilterFunc[T]
	procFunc   ProcessFunc[T]
	errFunc    ErrorFunc

	// счетчик активных операций
	// может быть как глобальным, так и локальным для экземпляра
	pendingOpsCounter Counter

	// флаг закрытого этапа
	closed atomic.Bool

	// канал входных данных
	inChan chan T
}

// Name возвращает имя этапа
func (s *Step[T]) Name() string {
	return s.name
}

// NewStep конструктор этапа
func NewStep[T any](
	name string,
	queue Queue[T],
	workerCnt int,
	counter Counter,
	filterFunc FilterFunc[T],
	processFunc ProcessFunc[T],
	errorFunc ErrorFunc,
) (*Step[T], error) {
	if name == "" {
		return nil, errStepNameIsRequired
	}

	// устанавливаем кол-во воркеров по умолчанию, если не задано
	if workerCnt == 0 {
		workerCnt = defaultWorkerCount
	}

	return &Step[T]{
		name:              name,
		workerCount:       workerCnt,
		pendingOpsCounter: counter,
		queue:             queue,
		inChan:            make(chan T),
		filterFunc:        filterFunc,
		procFunc:          processFunc,
		errFunc:           errorFunc,
	}, nil
}

// Publish публикует событие с соответствующим типом в очередь этапа
func (s *Step[T]) Publish(_ context.Context, val T) error {
	s.pendingOpsCounter.Inc()
	defer s.pendingOpsCounter.Dec()
	if s.closed.Load() {
		return errStepIsClosed
	}

	err := s.queue.Push(val)
	if err != nil {
		return err
	}
	return nil
}

// Close закрывает этап, при clean == true - очищает очередь этапа
func (s *Step[T]) Close(_ context.Context, clear bool) error {
	s.closed.Store(true)
	if clear {
		return s.queue.Clear()
	}
	return nil
}

// readQueue читает очередь и отправляет данные в канал
func (s *Step[T]) readQueue(ctx context.Context, doneCh, suspendCh chan struct{}) {
	defer close(s.inChan)
watchLoop:
	for {
		select {
		case <-ctx.Done():
			log.Debugf("step %s is canceled", s.name)
			break watchLoop
		case <-doneCh:
			log.Debugf("step %s context is done", s.name)
			break watchLoop
		case <-suspendCh:
			log.Debugf("step %s context is done by suspend", s.name)
			break watchLoop
		case <-s.queue.Signal():
			s.pendingOpsCounter.Inc()
			next, ok, err := s.queue.Next()
			s.pendingOpsCounter.Dec()
			if !ok {
				continue watchLoop
			}
			if err != nil {
				log.Errorf("step %s get next val error: %s", s.name, err)
				continue watchLoop
			}
			s.inChan <- next
		}
	}
	log.Debugf("readQueue is done")
}

// Run инициализирует пул воркеров и запускает на них обработку потока данных
func (s *Step[T]) Run(ctx context.Context, doneCh, suspendCh chan struct{}) error {
	var wg sync.WaitGroup

	// инициализируем пул воркеров
	for i := 0; i < s.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.do(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.readQueue(ctx, doneCh, suspendCh)
	}()
	wg.Wait()

	log.Debugf("step run is done")

	return nil
}

// do запускает цикл потока.
// Получает данные из входного канала, выполняет процессинг и отправляет данные в следующие потоки
func (s *Step[T]) do(ctx context.Context) {
	for val := range s.inChan {
		// увеличиваем счетчик ожидания и последовательно выполняем процессинговые функции
		// отправляем ошибки в функцию обработки, если она определена
		s.pendingOpsCounter.Inc()

		// выполняем функции обработки (фильтрация -> процессинг -> обработка ошибок -> [ретрай]
		if s.filterFunc != nil {
			fVal, err := s.filterFunc(ctx, val)
			s.errHandling(ctx, err)
			val = *fVal
		}

		ok, err := s.procFunc(ctx, val)
		s.errHandling(ctx, err)

		// производим ретрай, при необходимости
		if !ok {
			err := s.Publish(ctx, val)
			s.errHandling(ctx, err)
		}
		s.pendingOpsCounter.Dec()
	}
}

// errHandling обрабатывает ошибки этапа и отправляет в функцию обработки, если она определена
func (s *Step[T]) errHandling(ctx context.Context, err error) {
	if s.errFunc != nil && err != nil {
		s.errFunc(ctx, err)
	}
}
