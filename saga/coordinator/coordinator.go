package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

// checkInterval интервал проверки статуса задача
const checkInterval = 3 * time.Second

var (
	ErrEmpty        = errors.New("coordinator is empty")
	ErrClosed       = errors.New("coordinator is closed")
	ErrNameRequired = errors.New("coordinator name is required")
)

type StepInstance interface {
	Name() string
	Run(ctx context.Context, done, suspend chan struct{}) error
	Close(ctx context.Context, clear bool) error
}

// Counter счетчик операций, необходим для однозначного определения завершения всех активных операций
type Counter interface {
	Inc()
	Dec()
	Count() int64
}

// stepRegistry используется для хранения экземпляров этапов
type stepRegistry map[string]StepInstance

// Coordinator экземпляр управляющей структуры.
// Запускает и отслеживает все этапы, отслеживает счетчик активных задач.
type Coordinator struct {
	// уникальный идентификатор
	name string

	// флаг ожидания окончания ввода данных
	// при true - не завершает процесс даже после выполнения всех активных задач
	waitInput atomic.Bool

	// флаг завершения работы
	closed atomic.Bool

	// сигнальные каналы завершения
	doneCh    chan struct{}
	suspendCh chan struct{}

	// функция отмены контекстов этапов
	stepCancelFunc context.CancelFunc

	// счетчик активных задач
	counter Counter

	// хранилище этапов
	reg stepRegistry
}

// New создает новый экземпляр координатора.
func New(name string, counter Counter, waitInput bool) (*Coordinator, error) {
	if name == "" {
		return nil, ErrNameRequired
	}

	f := &Coordinator{
		name:      name,
		reg:       make(stepRegistry),
		doneCh:    make(chan struct{}),
		suspendCh: make(chan struct{}),
		counter:   counter,
	}
	f.waitInput.Store(waitInput)

	return f, nil
}

// Run запускает основной процесс выполнения.
// запускает процессы обработки всех потоков (step.run),
// процесс отслеживания состояния очередей для завершения (watchQueues),
// процесс обработки результатов работы потоков (watchResults).
// Процессы запускаются только для тех этапов, которые зарегистрированы через RegSteps.
func (c *Coordinator) Run(ctx context.Context) error {
	defer c.closed.Store(true)

	if len(c.reg) == 0 {
		return ErrEmpty
	}

	var wg sync.WaitGroup

	stepCtx, cancel := context.WithCancel(ctx)
	c.stepCancelFunc = cancel

	// запуск процессов обработки очередей
	for _, step := range c.reg {
		wg.Add(1)
		go func(step StepInstance) {
			defer wg.Done()
			err := step.Run(stepCtx, c.doneCh, c.suspendCh)
			if err != nil {
				log.Errorf("run step %s error: %s", step.Name(), err)
			}
		}(step)
	}

	// запуск процесса отслеживания состояния очередей
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.watchQueues(ctx)
		close(c.doneCh)
	}()

	wg.Wait()

	// закрываем этапы и очереди
	for _, step := range c.reg {
		err := step.Close(ctx, false)
		if err != nil {
			return fmt.Errorf("close queue error: %s", err)
		}
	}

	return nil
}

// Suspend останавливает процесс обработки.
// флаг force указывает на необходимость отмены контекста дочерних процессов,
// не дожидается завершения обработки уже отправленных в processFunc данных.
func (c *Coordinator) Suspend(force bool) {
	close(c.suspendCh)

	if force {
		// отменяем контекст потоков, чтобы донести отмену процессинговой функция
		c.stepCancelFunc()
	}
}

// EndWaitInput завершает ожидание ввода. с этого момента считается,
// что мы можем получить данные только как результат выполнения
func (c *Coordinator) EndWaitInput() {
	c.waitInput.Store(false)
}

// CleanUP очищает очереди всех потоков.
func (c *Coordinator) CleanUP(ctx context.Context) error {
	for _, step := range c.reg {
		err := step.Close(ctx, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// RegSteps регистрирует этапы в координаторе.
func (c *Coordinator) RegSteps(steps ...StepInstance) error {
	if c.closed.Load() {
		return ErrClosed
	}

	for i := range steps {
		c.reg[steps[i].Name()] = steps[i]
	}
	return nil
}

// watchQueues процесс отслеживания состояния очереди.
// при отмене контекста или "паузе" (suspend) - прекращает работу
func (c *Coordinator) watchQueues(ctx context.Context) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
watchLoop:
	for {
		select {
		case <-ctx.Done():
			break watchLoop
		case <-c.doneCh:
			break watchLoop
		case <-c.suspendCh:
			break watchLoop
		case <-ticker.C:
			exit := func() bool {
				// если координатор ждет завершения ввода - продолжаем наблюдение
				if c.waitInput.Load() {
					log.Debugf("waiting end of input")
					return false
				}

				// если есть активные задачи - возвращаемся в цикл
				if pc := c.counter.Count(); pc > 0 {
					log.Debugf("%s pending %d processes", c.name, pc)
					return false
				}
				return true
			}()

			if exit {
				// если мы дошли до этого кода - нет активных задач или получили сигнал выхода
				break watchLoop
			}
		}
	}
}
