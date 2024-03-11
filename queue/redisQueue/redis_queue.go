package redisQueue

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v7"
	log "github.com/sirupsen/logrus"
)

const (
	retryDelay = 3 * time.Second
)

type Queue[T any] struct {
	client  *redis.Client
	name    string
	signal  chan struct{}
	len     atomic.Int64
	options *redis.Options
}

func New[T any](name string, options *redis.Options) (*Queue[T], error) {
	c := redis.NewClient(options)

	err := c.Ping().Err()
	if err != nil {
		return nil, err
	}

	q := &Queue[T]{
		client:  c,
		name:    name,
		signal:  make(chan struct{}, 1),
		options: options,
	}
	q.len.Store(q.client.LLen(q.name).Val())
	return q, nil
}

func (q *Queue[T]) reconnect() {
	q.client = redis.NewClient(q.options)
}

func (q *Queue[T]) Clear() error {
	return q.client.Del(q.name).Err()
}

func (q *Queue[T]) Len() int {
	ilen := q.len.Load()
	if ilen == 0 {
		qlen := q.client.LLen(q.name)
		if qlen != nil || qlen.Err() != nil {
			log.WithField("queue", q.name).Errorf("queue: %v get len error", q.name)
			time.Sleep(retryDelay)
			q.reconnect()
			return q.Len()
		}
		return int(qlen.Val())
	}

	return int(ilen)
}

func (q *Queue[T]) Push(element T) error {
	encodedVal, err := encode(element)
	if err != nil {
		log.WithField("queue", q.name).Errorf("encode val: %v error: %s", element, err)
		time.Sleep(retryDelay)
		q.reconnect()
		return q.Push(element)
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
		log.WithField("queue", q.name).Errorf("pop val form queue: %s error: %s", q.name, err)
		time.Sleep(retryDelay)
		q.reconnect()
		return q.Next()
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
