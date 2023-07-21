package pipeliner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	checkStagesStatusTimer = 3 * time.Second
)

var (
	ErrPipelineClosed    = errors.New("pipeline is closed")
	ErrTopicNotFound     = errors.New("topic not found")
	ErrStageUndefined    = errors.New("this topic not contains stage")
	ErrPipelineIsNotWait = errors.New("pipeline is not input waiting ")
)

// TopicsRegistry реестр топиков и этапов
type TopicsRegistry[T any] map[string]*stage[T]

// processFunc процессинговая функция, вызывается соответствующим этапом
type processFunc[T any] func(context.Context, T) ([]T, error)

// postprocessingFunc пост процессинг, отдает прошедшие фильтрацию или обработку данные
type postProcessingFunc[T any] func(context.Context, []T) []T

// errFunc функция обработки ошибок
type errFunc func(context.Context, error)

type Pipeline[T any] struct {
	topics    TopicsRegistry[T]
	waitInput bool

	// флаг закрытого пайплайна, при true, в него невозможно пушнуть новые данные
	closed bool

	mu sync.RWMutex
}

// NewPipeline определяет новый pipeline.
// waitInput определяет поведение: ожидать доп ввод или завершить pipeline после обработки входных данных
func NewPipeline[T any](waitInput bool) *Pipeline[T] {
	p := &Pipeline[T]{
		topics:    make(TopicsRegistry[T], 0),
		waitInput: waitInput,
		mu:        sync.RWMutex{},
	}

	return p
}

func (p *Pipeline[T]) Run(ctx context.Context) error {
	stages := p.stages()
	var wg sync.WaitGroup
	pCtx, cancel := context.WithCancel(ctx)

	for _, s := range stages {
		wg.Add(1)
		go func(s *stage[T]) {
			defer wg.Done()
			s.process(pCtx)
		}(s)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		p.check(ctx)
	}()

	wg.Wait()
	// TODO спорно, но мб требуется очистка
	for _, s := range stages {
		s.reset()
	}
	return nil
}

func (p *Pipeline[T]) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
}

func (p *Pipeline[T]) isClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

func (p *Pipeline[T]) stages() TopicsRegistry[T] {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.topics
}

func (p *Pipeline[T]) isWaitInput() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.waitInput
}

func (p *Pipeline[T]) setWaitInput(w bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waitInput = w
}

func (p *Pipeline[T]) check(ctx context.Context) {
	ticker := time.NewTicker(checkStagesStatusTimer)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			p.setWaitInput(false)
			p.close()
			return
		case <-ticker.C:
			// при exit == true считаем что все этапы завершены
			exit := func() bool {
				if p.isWaitInput() {
					return false
				}

				notOpenedStages := make([]*stage[T], 0)

				// если хоть один из топиков не завершен - идем на следующий тик
				for _, s := range p.topics {
					if !s.isOpened() {
						notOpenedStages = append(notOpenedStages, s)
					}
					if s.isPending() || !s.q.IsEmpty() {
						return false
					}
				}

				if len(notOpenedStages) > 0 {
					for i := range notOpenedStages {
						notOpenedStages[i].open()
					}
					return false
				}

				return true
			}()
			if exit {
				return
			}
		}
	}
}

// AddStage добавляет этап в пайплайн
// принимает имя(id) топика, функцию-обработчик, кол-во воркеров на функцию и имена выходных топиков
func (p *Pipeline[T]) AddStage(id string, processFunc processFunc[T], postProcFunc postProcessingFunc[T], errFunc errFunc, workers int, outTopics ...string) error {
	if p.closed {
		return ErrPipelineClosed
	}

	if workers < 1 {
		workers = 1
	}

	s, err := newStage[T](p, id, workers, processFunc, postProcFunc, errFunc, outTopics...)
	if err != nil {
		return err
	}

	p.addStage(s)

	return nil

}

func (p *Pipeline[T]) addStage(stage *stage[T]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.topics[stage.name()] = stage
}

// EndInput сигнализирует о прекращении ожидания входных данных
func (p *Pipeline[T]) EndInput() error {
	if !p.isWaitInput() {
		return ErrPipelineIsNotWait
	}
	p.setWaitInput(false)

	return nil
}

func (p *Pipeline[T]) topic(name string) (*stage[T], bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s, ok := p.topics[name]
	return s, ok
}

func (p *Pipeline[T]) Push(topic string, val T) error {

	s, ok := p.topic(topic)
	if !ok {
		return fmt.Errorf("stage %s error %w", topic, ErrTopicNotFound)
	}
	if s == nil {
		return fmt.Errorf("stage %s error %w", topic, ErrStageUndefined)
	}
	if p.isClosed() {
		return ErrPipelineClosed
	}
	s.push(val)
	return nil
}
