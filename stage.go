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

	defaultWorkerCount = 1
)

// processFunc процессинговая функция. Возвращает массив обработанных результатов,
// массив результатов компенсации (тех объектов, которые не были обработаны) и ошибку
type processFunc[T any] func(ctx context.Context, input ...T) (results []T, compensate []T, err error)

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
	outCh chan pipeOut[T]
}

// pipeOut результат выполнения
type pipeOut[T any] struct {
	target string
	data   T
}

// newPipe инициализирует экземпляр потока данных
func newPipe[T any](pipelinerName, pipeName string, q Queue[T], outF []string, wCount int, pFunc ...processFunc[T]) (*pipe[T], error) {
	if len(pFunc) == 0 {
		return nil, errPipeProcFuncIsNotSet
	}
	if pipeName == "" {
		return nil, errPipeNameIsNotSet
	}

	if wCount == 0 {
		wCount = defaultWorkerCount
	}

	id := fmt.Sprintf("%s-%s", pipelinerName, pipeName)

	return &pipe[T]{
		id:          id,
		name:        pipeName,
		outPipes:    outF,
		procFs:      pFunc,
		workerCount: wCount,
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

func (p *pipe[T]) push(ctx context.Context, val T) error {
	return p.queue.Push(val)
}

// decPending уменьшает счетчик на 1
func (p *pipe[T]) decPending() {
	p.pendingCount.Add(-1)
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
			next, ok, err := p.queue.Next()
			if !ok {
				break watchLoop
			}
			if err != nil {
				log.Errorf("stage %s get next val error: %s", p.id, err)
				continue
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
		for i, pf := range p.procFs {
			results, compensate, err := pf(ctx, val)
			if err != nil {
				if p.errF != nil {
					p.errF(ctx, err)
				}
				log.Debugf("pipe %s was detected error at %d processing function result", p.name, i)
			}
			for _, cmp := range compensate {
				p.outCh <- pipeOut[T]{
					target: p.name,
					data:   cmp,
				}
				log.Debugf("pipe %s was detected retry obj: %v at %d processing function result", p.name, cmp, i)
			}

			// отправляем результат в указанные в outPipes потоки
			for _, result := range results {
				for _, target := range p.outPipes {
					p.outCh <- pipeOut[T]{
						target: target,
						data:   result,
					}
				}
			}
		}
		p.decPending()
	}
}

func (p *pipe[T]) closeQueue(clear bool) error {
	if clear {
		return p.queue.Clear()
	}
	return nil
}
