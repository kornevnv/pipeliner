package processor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

// checkInterval интервал проверки статуса задача
const checkInterval = 3 * time.Second

var (
	ErrEmptyProcessor        = errors.New("processor is empty")
	ErrProcessorClosed       = errors.New("processor is closed")
	ErrProcessorNameRequired = errors.New("processor name is required")
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

// Processor экземпляр управляющей структуры.
// Запускает и отслеживает все этапы, отслеживает счетчик активных задач
type Processor struct {
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

// New создает новый экземпляр процессора
func New(name string, counter Counter, waitInput bool) (*Processor, error) {
	if name == "" {
		return nil, ErrProcessorNameRequired
	}

	f := &Processor{
		name:      name,
		reg:       make(stepRegistry),
		doneCh:    make(chan struct{}),
		suspendCh: make(chan struct{}),
		counter:   counter,
	}
	f.waitInput.Store(waitInput)

	return f, nil
}

// Run запускает основной процесс выполнения
// запускает процессы обработки всех потоков (step.run),
// процесс отслеживания состояния очередей для завершения (watchQueues),
// процесс обработки результатов работы потоков (watchResults)
func (p *Processor) Run(ctx context.Context) error {
	defer p.closed.Store(true)

	if len(p.reg) == 0 {
		return ErrEmptyProcessor
	}

	var wg sync.WaitGroup

	stepCtx, cancel := context.WithCancel(ctx)
	p.stepCancelFunc = cancel

	// запуск процессов обработки очередей
	for _, step := range p.reg {
		wg.Add(1)
		go func(step StepInstance) {
			defer wg.Done()
			err := step.Run(stepCtx, p.doneCh, p.suspendCh)
			if err != nil {
				log.Errorf("run step %s error: %s", step.Name(), err)
			}
		}(step)
	}

	// запуск процесса отслеживания состояния очередей
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.watchQueues(ctx)
		close(p.doneCh)
	}()

	wg.Wait()

	// закрываем этапы и очереди
	for _, step := range p.reg {
		err := step.Close(ctx, false)
		if err != nil {
			log.Debugf("close queue error: %s", err)
		}
		log.Debugf("step %s: queue is closed", step.Name())
	}

	return nil
}

// Suspend останавливает процесс обработки.
// флаг force указывает на необходимость отмены контекста дочерних процессов,
// не дожидается завершения обработки уже отправленных в processFunc данных
func (p *Processor) Suspend(force bool) {
	close(p.suspendCh)

	if force {
		// отменяем контекст потоков, чтобы донести отмену процессинговой функция
		p.stepCancelFunc()
	}

}

// EndWaitInput завершает ожидание ввода. с этого момента считается,
// что мы можем получить данные только как результат выполнения
func (p *Processor) EndWaitInput() {
	p.waitInput.Store(false)
}

// CleanUP очищает очереди всех потоков
func (p *Processor) CleanUP(ctx context.Context) error {
	for _, step := range p.reg {
		err := step.Close(ctx, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// RegSteps регистрирует этапы в процессоре.
// Хранилище необходимо для централизованного управления всеми этапами
func (p *Processor) RegSteps(steps ...StepInstance) error {
	if p.closed.Load() {
		return ErrProcessorClosed
	}

	for i := range steps {
		p.reg[steps[i].Name()] = steps[i]
	}

	return nil
}

// watchQueues процесс отслеживания состояния очереди.
// при отмене контекста или "паузе" (suspend) - прекращает работу
func (p *Processor) watchQueues(ctx context.Context) {
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
			exit := func() bool {
				// если процессор ждет завершения ввода - продолжаем наблюдение
				if p.waitInput.Load() {
					log.Debugf("waiting end of input")
					return false
				}

				// если есть активные задачи - возвращаемся в цикл
				if pc := p.counter.Count(); pc > 0 {
					log.Debugf("processor pending %d messages", pc)
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

	log.Debugf("watchQueues is done")
}
