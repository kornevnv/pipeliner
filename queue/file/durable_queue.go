package file

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"sync"
)

type DurableQueue[T any] struct {
	name     string
	content  []T
	filesIdx []int64
	path     string

	idx    int64
	signal chan struct{}

	mu sync.RWMutex
}

func New[T any](queueCap int64, dir, name string, tryRestore bool) (*DurableQueue[T], error) {
	if dir == "" {
		return nil, fmt.Errorf("path required")
	}
	files, err := dirContent(path.Join(dir, name))
	if err != nil {
		return nil, fmt.Errorf("read dir error: %w", err)
	}

	var maxIdx int64
	content := make([]T, 0, queueCap)
	filesIdx := make([]int64, 0, queueCap)
	for i := range files {
		if !tryRestore {
			err := os.Remove(path.Join(dir, name, files[i]))
			if err != nil {
				return nil, fmt.Errorf("remove entry error: %w", err)
			}
		}
		currIdx, _ := strconv.Atoi(files[i])
		if int64(currIdx) > maxIdx {
			maxIdx = int64(currIdx)
		}
		buf, err := os.ReadFile(path.Join(dir, name, files[i]))
		if err != nil {
			return nil, fmt.Errorf("read entry error: %w", err)
		}

		var data T
		decoded, err := decodeBinary(buf, &data)
		if err != nil {
			return nil, fmt.Errorf("decodeerror: %w", err)
		}
		content = append(content, *decoded)
		filesIdx = append(filesIdx, int64(currIdx))
	}

	return &DurableQueue[T]{
		name:     name,
		content:  content,
		filesIdx: filesIdx,
		path:     path.Join(dir, name),
		idx:      maxIdx,
		mu:       sync.RWMutex{},
	}, nil

}

func (q *DurableQueue[T]) IsEmpty() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.content) == 0
}

func (q *DurableQueue[T]) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.content)
}

func (q *DurableQueue[T]) Push(element T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.idx++

	buf, err := encodeBinary(element)
	if err != nil {
		return err
	}

	err = os.MkdirAll(q.path, os.ModeDir)
	if err != nil {
		return err
	}

	err = os.WriteFile(path.Join(q.path, fmt.Sprintf("%d", q.idx)), buf, 0644)

	if err != nil {
		return err
	}

	q.content = append(q.content, element)
	q.filesIdx = append(q.filesIdx, q.idx)

	return nil
}

func (q *DurableQueue[T]) Peek() T {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.content[0]
}

func (q *DurableQueue[T]) Next() (T, bool, error) {
	var empty T
	if q.IsEmpty() {
		q.drain()
		return empty, false, nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	err := os.Remove(path.Join(q.path, fmt.Sprintf("%d", q.filesIdx[0])))
	if err != nil {
		return empty, false, err
	}
	curr := q.content[0]
	q.content = q.content[1:]
	q.filesIdx = q.filesIdx[1:]

	return curr, true, nil
}

func (q *DurableQueue[T]) prepSignal() {
	q.mu.Lock()
	defer q.mu.Unlock()

	var send bool
	select {
	case _, send = <-q.signal:
	default:
	}

	if !send && len(q.content) > 0 {
		send = true
	}
	if send {
		select {
		case q.signal <- struct{}{}:
		default:
		}
	}
}

func (q *DurableQueue[T]) drain() {
loop:
	for {
		select {
		case <-q.signal:
		default:
			break loop
		}
	}
}

func dirContent(name string) ([]string, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	filenames, err := f.Readdirnames(0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return filenames, nil
}
