package step

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	errStepNameIsRequired   = errors.New("step name is required")
	errStepIsClosed         = errors.New("step is closed")
	errStepIsNotInitialized = errors.New("step is not initialized, use *step.Init(...)")

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

// ProcessFunc процессинговая функция. Возвращает флаг необходимости ретрая и ошибку.
// retry (!ok) производится путем повторной публикации объекта в очередь
type ProcessFunc[T any] func(ctx context.Context, input T) (ok bool, err error)

// FilterFunc фильтрующая функция. Выполняется ДО процессинговой функции.
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

	initialized atomic.Bool
}

// Name возвращает имя этапа
func (s *Step[T]) Name() string {
	return s.name
}

// Init инициализирует этап
func (s *Step[T]) Init(
	name string,
	queue Queue[T],
	workerCnt int,
	counter Counter,
	filterFunc FilterFunc[T],
	processFunc ProcessFunc[T],
	errorFunc ErrorFunc,
) error {
	if name == "" {
		return errStepNameIsRequired
	}

	// устанавливаем кол-во воркеров по умолчанию, если не задано
	if workerCnt == 0 {
		workerCnt = defaultWorkerCount
	}

	s.name = name
	s.workerCount = workerCnt
	s.pendingOpsCounter = counter
	s.queue = queue
	s.inChan = make(chan T)
	s.filterFunc = filterFunc
	s.procFunc = processFunc
	s.errFunc = errorFunc

	s.initialized.Store(true)
	return nil
}

// Push публикует событие с соответствующим типом в очередь этапа
func (s *Step[T]) Push(_ context.Context, val T) error {
	if !s.initialized.Load() {
		return errStepIsNotInitialized
	}
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
	if !s.initialized.Load() {
		return errStepIsNotInitialized
	}
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
			break watchLoop
		case <-doneCh:
			break watchLoop
		case <-suspendCh:
			break watchLoop
		case <-s.queue.Signal():
			s.pendingOpsCounter.Inc()
			next, ok, err := s.queue.Next()
			s.pendingOpsCounter.Dec()
			if !ok {
				continue watchLoop
			}
			if err != nil {
				s.errHandling(ctx, fmt.Errorf("step %s get next val error: %s", s.name, err))
				continue watchLoop
			}
			s.inChan <- next
		}
	}
}

// Run инициализирует пул воркеров и запускает на них обработку потока данных
func (s *Step[T]) Run(ctx context.Context, doneCh, suspendCh chan struct{}) error {
	if !s.initialized.Load() {
		return errStepIsNotInitialized
	}

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
			// не прошло фильтрацию
			if fVal == nil {
				s.pendingOpsCounter.Dec()
				continue
			}
			val = *fVal
		}

		ok, err := s.procFunc(ctx, val)
		s.errHandling(ctx, err)

		// производим ретрай, при необходимости
		if !ok {
			err := s.Push(ctx, val)
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
