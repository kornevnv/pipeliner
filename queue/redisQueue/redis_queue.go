package redisQueue

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"sync/atomic"

	"github.com/go-redis/redis/v7"
	log "github.com/sirupsen/logrus"
)

type Queue[T any] struct {
	client *redis.Client
	name   string
	signal chan struct{}
	len    atomic.Int64
}

func New[T any](name string, options *redis.Options) (*Queue[T], error) {
	c := redis.NewClient(options)

	err := c.Ping().Err()
	if err != nil {
		return nil, err
	}

	q := &Queue[T]{
		client: c,
		name:   name,
		signal: make(chan struct{}, 1),
	}
	q.len.Store(q.client.LLen(q.name).Val())
	return q, nil
}

func (q *Queue[T]) Clear() error {
	return q.client.Del(q.name).Err()
}

func (q *Queue[T]) Len() int {
	ilen := q.len.Load()
	if ilen == 0 {
		return int(q.client.LLen(q.name).Val())
	}

	return int(ilen)
}

func (q *Queue[T]) Push(element T) error {
	encodedVal, err := encode(element)
	if err != nil {
		log.WithField("queue", q.name).Errorf("encode val: %v error: %s", element, err)
		return fmt.Errorf("encode val error: %s", err)
	}
	_, err = q.client.LPush(q.name, encodedVal).Result()
	if err != nil {
		log.WithField("queue", q.name).Errorf("push val: %v error: %s", element, err)
		return err
	}
	q.len.Add(1)

	select {
	case q.signal <- struct{}{}:
	default:
	}

	return nil
}

func (q *Queue[T]) Next() (T, bool, error) {
	var item T
	if q.Len() == 0 {
		q.drain()
		return item, false, nil
	}

	val, err := q.client.RPop(q.name).Result()
	if err != nil {
		log.WithField("queue", q.name).Errorf("empry pop val form queue: %s", q.name)
		return item, false, fmt.Errorf("get val error: %s", err)
	}

	err = decode(val, &item)
	if err != nil {
		log.WithField("queue", q.name).Errorf("decode val: %v error: %s", val, err)
		return item, false, fmt.Errorf("encode val error: %s", err)
	}
	q.len.Add(-1)

	return item, true, nil
}

func (q *Queue[T]) prepSignal() {

	var send bool
	select {
	case _, send = <-q.signal:
	default:
	}

	if !send && q.Len() > 0 {
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

func encode(value interface{}) (string, error) {
	buff := new(bytes.Buffer)
	err := gob.NewEncoder(buff).Encode(value)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(buff.Bytes()), nil

}

func decode(value string, dst interface{}) error {
	data, err := hex.DecodeString(value)
	if err != nil {
		return fmt.Errorf("decode hex error: %w", err)
	}

	buff := bytes.NewBuffer(data)

	return gob.NewDecoder(buff).Decode(dst)
}
