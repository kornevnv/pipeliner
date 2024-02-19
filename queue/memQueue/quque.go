package memQueue

import (
	"sync"
)

const (
	initialQueueCapacity = 1024
)

type Queue[T any] struct {
	name     string
	contents []T

	cap int

	signal chan struct{}
	mu     sync.RWMutex
}

// New возвращает новый экземпляр очереди, queueCap определяет начальную емкость
func New[T any](queueCap int, name string) *Queue[T] {

	// емкость массива элементов
	contentCap := initialQueueCapacity

	if queueCap > 0 {
		contentCap = queueCap
	}

	q := &Queue[T]{
		name:     name,
		contents: make([]T, 0, contentCap),
		cap:      contentCap,
		signal:   make(chan struct{}, 1),
		mu:       sync.RWMutex{},
	}

	return q
}

func (q *Queue[T]) IsEmpty() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.contents) == 0
}

func (q *Queue[T]) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.contents)
}

func (q *Queue[T]) Push(element T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.contents = append(q.contents, element)

	select {
	case q.signal <- struct{}{}:
	default:
	}
	return nil
}

// Peek возвращает текущее значение из очереди без удаления элемента
func (q *Queue[T]) Peek() T {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.contents[0]
}

func (q *Queue[T]) Next() (T, bool, error) {
	if q.IsEmpty() {
		q.drain()
		var empty T
		return empty, false, nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	item := q.contents[0]
	q.contents = q.contents[1:]
	return item, true, nil
}

func (q *Queue[T]) prepSignal() {
	q.mu.Lock()
	defer q.mu.Unlock()

	var send bool
	select {
	case _, send = <-q.signal:
	default:
	}

	if !send && len(q.contents) > 0 {
		send = true
	}
	if send {
		select {
		case q.signal <- struct{}{}:
		default:
		}
	}
}

func (q *Queue[T]) Signal() <-chan struct{} {
	q.prepSignal()
	return q.signal
}

func (q *Queue[T]) drain() {
loop:
	for {
		select {
		case <-q.signal:
		default:
			break loop
		}
	}
}

func (q *Queue[T]) Clear() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.contents = make([]T, q.cap)
	return nil
}
