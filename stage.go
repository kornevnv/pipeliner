package pipeliner

import (
	"context"
	"errors"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/kornevnv/pipeliner/queue"
)

// stage фукнциональная структура, выполняет processFunc и передает результат в топики из out
// использует очередь и указанное кол-во воркеров
type stage[T any] struct {
	pipeline        *Pipeline[T]
	id              string
	q               *queue.Queue[T]
	in              chan T
	out             []string
	processFunc     processFunc[T]
	postProcessFunc postProcessingFunc[T]
	errFunc         errFunc
	workers         int

	pending int
	// флаг запуска этапа, при первом пуше устанавливается в true
	opened bool

	mu sync.RWMutex
}

func newStage[T any](p *Pipeline[T], id string, workersCount int, pFunc processFunc[T], ppFunc postProcessingFunc[T], eFunc errFunc, out ...string) (*stage[T], error) {
	if pFunc == nil {
		return nil, fmt.Errorf("process func is nil")
	}
	s := &stage[T]{
		pipeline:        p,
		id:              id,
		q:               queue.NewQueue[T](0),
		in:              make(chan T),
		out:             out,
		workers:         workersCount,
		processFunc:     pFunc,
		postProcessFunc: ppFunc,
		errFunc:         eFunc,

		opened: false,
	}
	return s, nil
}

// ID идентификатор этапа
func (s *stage[T]) name() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id
}

// incPending увеличивает счетчик ожидания завершения
func (s *stage[T]) incPending() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending++
}

// decPending уменьшает счетчик ожидания завершения
func (s *stage[T]) decPending() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending > 0 {
		s.pending--
	}
}

// isPending возвращает статус (есть ли задачи, ожидающие завершения)
func (s *stage[T]) isPending() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pending > 0
}

// open указывает, что в этап были отправлены данные
func (s *stage[T]) open() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opened = true
}

// isOpened возвращает статут этапа
func (s *stage[T]) isOpened() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.opened
}

func (s *stage[T]) push(val T) {
	var once sync.Once

	once.Do(s.open)
	s.q.Push(val)
}

func (s *stage[T]) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = 0
	s.opened = true
	s.q.Reset()
}

// process инициализирует воркеры и запускает обработку очереди
func (s *stage[T]) process(ctx context.Context) {
	var wg sync.WaitGroup
	s.initWorkers(ctx, &wg)
	defer close(s.in)

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.readQueue(ctx)
	}()

	wg.Wait()
}

// readQueue читает очередь и отправляет данные в канал
func (s *stage[T]) readQueue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Debugf("stage %s done", s.id)
			s.reset()
			return
		case <-s.q.Signal():
			next, ok := s.q.Next()
			if !ok {
				return
			}
			s.in <- next
		}
	}
}

// initWorkers инициализация воркеров (кол-во определяется параметрами этапа)
func (s *stage[T]) initWorkers(ctx context.Context, wg *sync.WaitGroup) {
	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.do(ctx)
		}()
	}
}

func (s *stage[T]) doErr(ctx context.Context, err error) {
	if s.errFunc != nil {
		s.errFunc(ctx, err)
	} else {
		log.WithError(err).Error("stage error")
	}
}

// do обрабатывает входящие данные из очереди
func (s *stage[T]) do(ctx context.Context) {
	for {
		select {
		case val, ok := <-s.in:
			if !ok {
				return
			}
			s.incPending()
			rawResults, err := s.processFunc(ctx, val)
			if err != nil {
				s.doErr(ctx, err)
			}

			results := rawResults
			if s.postProcessFunc != nil {
				results = s.postProcessFunc(ctx, results)
			}

		loop:
			for i := range results {
				for _, topic := range s.out {
					err := s.pipeline.Push(topic, results[i])
					if errors.Is(err, ErrPipelineClosed) {
						break loop
					}
					if err != nil {
						s.doErr(ctx, err)
						log.WithError(err).Error("pushing error")
					}
				}
			}
			s.decPending()
		case <-ctx.Done():
			return
		}
	}
}
