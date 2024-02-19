package pipeliner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

var (
	errPipeNameIsNotSet       = errors.New("pipe name is not set")
	errPipeProcFuncIsNotSet   = errors.New("pipe processing functions is not set")
	errPipeAlreadyInitialized = errors.New("pipe queue is already initialized")
	errPipeIsClosed           = errors.New("pipe is already closed")

	defaultWorkerCount = 1
)

// ProcessFunc процессинговая функция. Возвращает массив обработанных результатов,
// массив результатов компенсации (тех объектов, которые не были обработаны) и ошибку
type ProcessFunc[T any] func(ctx context.Context, input ...T) (results []T, compensate []T, err error)

// errFunc функция обработки ошибок
type errFunc func(context.Context, error)

// pipe структура потока данных. содержит пул воркеров и функции обработки данных
type pipe[T any] struct {
	id string
	// имя потока
	name string

	// перечень потоков, которым необходимо передать результат выполнения
	outPipes []string

	queue Queue[T]

	// функции обработки данных
	procFs []ProcessFunc[T]

	// функции обработки ошибки
	errF errFunc

	// кол-во воркеров в пуле для данного потока
	workerCount int

	// указывает на наличие изменений (при получении/завершении обработки)
	changedSig chan struct{}

	// кол-во процессов, в текущий момент времени
	pendingCount atomic.Int64

	// флаг статуса потока
	closed atomic.Bool

	// флаг инициализации
	initialized atomic.Bool

	inCh  chan T
	outCh chan pipeOut[T]
}

// pipeOut результат выполнения
type pipeOut[T any] struct {
	target string
	data   T
}

type PipeParams[T any] struct {
	Name         string
	WorkerCount  int
	ProcessFuncs []ProcessFunc[T]
	OutPipes     []string
}

// newPipe инициализирует экземпляр потока данных.
// обработка происходит по всем указанным processFunc ПОСЛЕДОВАТЕЛЬНО!
// т.е. pf1 -> pf2 -> pf3, и только после выполнения всех - результат уходит в outF.
// compensate обрабатывается из всех processFunc
func newPipe[T any](pipelinerName string, q Queue[T], params PipeParams[T]) (*pipe[T], error) {
	if len(params.ProcessFuncs) == 0 {
		return nil, errPipeProcFuncIsNotSet
	}
	if params.Name == "" {
		return nil, errPipeNameIsNotSet
	}

	if params.WorkerCount == 0 {
		params.WorkerCount = defaultWorkerCount
	}

	id := fmt.Sprintf("%s-%s", pipelinerName, params.Name)

	return &pipe[T]{
		id:          id,
		name:        params.Name,
		outPipes:    params.OutPipes,
		procFs:      params.ProcessFuncs,
		workerCount: params.WorkerCount,
		changedSig:  make(chan struct{}, 1),
		inCh:        make(chan T, 1),
		outCh:       make(chan pipeOut[T]),
		queue:       q,
	}, nil
}

// incPending увеличивает счетчик на 1
func (p *pipe[T]) incPending() {
	p.pendingCount.Add(1)
}

// decPending уменьшает счетчик на 1
func (p *pipe[T]) decPending() {
	p.pendingCount.Add(-1)
}

func (p *pipe[T]) push(ctx context.Context, val ...T) error {
	p.incPending()
	defer p.decPending()
	if p.closed.Load() {
		return errPipeIsClosed
	}
	for _, v := range val {
		err := p.queue.Push(v)
		if err != nil {
			return err
		}
	}
	return nil
}

// run инициализирует пул воркеров и запускает на них обработку потока данных
func (p *pipe[T]) run(ctx context.Context, doneCh, suspendCh chan struct{}) error {
	defer close(p.outCh)

	// проверка и установка флага инициализации
	if p.initialized.Load() {
		return errPipeAlreadyInitialized
	}
	p.initialized.Store(true)

	var wg sync.WaitGroup

	// инициализируем пул воркеров
	for i := 0; i < p.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.do(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.readQueue(ctx, doneCh, suspendCh)
	}()

	wg.Wait()

	log.Debugf("pipe run is done")

	return nil
}

// readQueue читает очередь и отправляет данные в канал
func (p *pipe[T]) readQueue(ctx context.Context, doneCh, suspendCh chan struct{}) {
	defer close(p.inCh)
watchLoop:
	for {
		select {
		case <-ctx.Done():
			log.Debugf("stage %s is canceled", p.id)
			break watchLoop
		case <-doneCh:
			log.Debugf("stage %s context is done", p.id)
			break watchLoop
		case <-suspendCh:
			log.Debugf("stage %s context is done by suspend", p.id)
			break watchLoop
		case <-p.queue.Signal():
			p.incPending()
			next, ok, err := p.queue.Next()
			p.decPending()
			if !ok {
				continue watchLoop
			}
			if err != nil {
				log.Errorf("stage %s get next val error: %s", p.id, err)
				continue watchLoop
			}
			p.inCh <- next
		}
	}
	log.Debugf("readQueue is done")
}

// do запускает цикл потока.
// Получает данные из входного канала, выполняет процессинг и отправляет данные в следующие потоки
func (p *pipe[T]) do(ctx context.Context) {
	for val := range p.inCh {
		// увеличиваем счетчик ожидания и последовательно выполняем процессинговые функции
		// отправляем ошибки в функцию обработки, если она определена
		p.incPending()
		results, _, _ := p.runProcFunc(ctx, 0, val)
		for _, result := range results {
			for _, target := range p.outPipes {
				p.outCh <- pipeOut[T]{
					target: target,
					data:   result,
				}
			}
		}
		p.decPending()
	}
}

func (p *pipe[T]) runProcFunc(ctx context.Context, pfIdx int, val ...T) ([]T, []T, error) {
	result, compensate, err := p.procFs[pfIdx](ctx, val...)
	if err != nil {
		if p.errF != nil {
			p.errF(ctx, err)
		}
		log.Debugf("pipe %s was detected error at %d processing function result", p.name, pfIdx)
	}
	for _, comp := range compensate {
		p.outCh <- pipeOut[T]{
			target: p.name,
			data:   comp,
		}
		log.Debugf("pipe %s was detected retry objs count: %v at %d processing function result", p.name, comp, pfIdx)
	}

	// если это не последня функция - замыкаем
	if pfIdx < len(p.procFs)-1 {
		return p.runProcFunc(ctx, pfIdx+1, result...)
	}

	// отправляем результат в указанные в outPipes потоки
	return result, nil, nil

}

func (p *pipe[T]) closeQueue(clear bool) error {
	p.closed.Store(true)
	if clear {
		return p.queue.Clear()
	}
	return nil
}
