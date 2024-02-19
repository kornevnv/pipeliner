// nolint

package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	log "github.com/sirupsen/logrus"

	pl "pipeliner"

	rq "pipeliner/queue/redisQueue"
)

const (
	dialTimeout = 5 * time.Minute
	// Timeout for socket reads.
	readTimeout = dialTimeout
	// Timeout for redis socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is ReadTimeout.
	writeTimeout = dialTimeout
)

type Host struct {
	IP      string
	Domains []string
}

type res struct {
	mu sync.Mutex

	data []string
}

func (r *res) Add(v string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = append(r.data, v)
}

func (r *res) Content() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.data
}

var r = new(res)

type RTS struct {
	tasks map[int64]TaskController
}

type TaskController struct {
	CancelFunc  context.CancelFunc
	SuspendFunc func(force bool)
	CleanUPFunc func() error
}

// useRedisQueue враппер для инициализации очереди
func useRedisQueue[T any](addr, pwd string, db int) pl.CreateQueueFunc[T] {
	return func(name string) (pl.Queue[T], error) {
		q, err := rq.New[T](name, &redis.Options{
			Addr:         addr,
			Password:     pwd,
			DB:           db,
			DialTimeout:  dialTimeout,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
		})
		if err != nil {
			return nil, err
		}
		return q, nil
	}
}

func initPipeliner(ctx context.Context) (*pl.Pipeliner[Host], error) {
	// возвращаем функцию инициализации очереди
	redisQueueCF := useRedisQueue[Host]("localhost:6379", "password", 0)

	proc, err := pl.NewPipeliner[Host]("cpt-active-1", true, redisQueueCF)
	if err != nil {
		return nil, err
	}

	err = proc.AddPipe(pl.PipeParams[Host]{
		Name:         "1",
		WorkerCount:  2,
		ProcessFuncs: []pl.ProcessFunc[Host]{pf1},
		OutPipes:     []string{"2"},
	})
	if err != nil {
		log.WithError(err).Errorf("")
	}

	err = proc.AddPipe(pl.PipeParams[Host]{
		Name:         "2",
		WorkerCount:  2,
		ProcessFuncs: []pl.ProcessFunc[Host]{pf2},
		OutPipes:     []string{"3"},
	})
	if err != nil {
		log.WithError(err).Errorf("")
	}
	err = proc.AddPipe(pl.PipeParams[Host]{
		Name:         "3",
		WorkerCount:  10,
		ProcessFuncs: []pl.ProcessFunc[Host]{pf3},
		OutPipes:     []string{"save"},
	})
	if err != nil {
		log.WithError(err).Errorf("")
	}
	err = proc.AddPipe(pl.PipeParams[Host]{
		Name:         "save",
		WorkerCount:  1,
		ProcessFuncs: []pl.ProcessFunc[Host]{saveResults},
		OutPipes:     nil,
	})
	if err != nil {
		log.WithError(err).Errorf("")
	}
	err = proc.AddPipe(pl.PipeParams[Host]{
		Name:         "compensate",
		WorkerCount:  1,
		ProcessFuncs: []pl.ProcessFunc[Host]{compensate, compensate2},
		OutPipes:     nil,
	})
	if err != nil {
		log.WithError(err).Errorf("")
	}
	return proc, nil
}

func main() {
	err := Run()
	if err != nil {
		log.Fatal(err)
	}
}

func Run() error {
	ctx, cancel := context.WithCancel(context.Background())

	proc, err := initPipeliner(ctx)
	if err != nil {
		cancel()
		return fmt.Errorf("init pipeliner error")
	}

	rts := TaskController{
		CancelFunc:  cancel,
		SuspendFunc: proc.Suspend,
		CleanUPFunc: proc.CleanUP,
	}
	_ = rts

	err = proc.Push("1", Host{
		IP:      "127.0.0.1",
		Domains: nil,
	})
	if err != nil {
		log.WithError(err).Errorf("")
	}

	err = proc.Push("compensate", Host{
		IP:      "COMP",
		Domains: nil,
	})
	if err != nil {
		log.WithError(err).Errorf("")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = proc.Run(ctx)
		if err != nil {
			log.WithError(err).Errorf("")
		}
	}()

	proc2 := &pl.Pipeliner[Host]{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(3 * time.Second)
		fmt.Println("PAUSE|CANCEL")
		rts.SuspendFunc(true)
		time.Sleep(3 * time.Second)
		proc2, err = initPipeliner(ctx)
		if err != nil {
			log.WithError(fmt.Errorf("init pipeliner error"))
		}
		err = proc2.Run(ctx)
		if err != nil {
			log.WithError(fmt.Errorf("run pipeliner error"))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(15 * time.Second)
		fmt.Println("END WAIT proc")

		proc.EndWaitInput()
		time.Sleep(15 * time.Second)
		fmt.Println("END WAIT proc2")
		proc2.EndWaitInput()
	}()

	wg.Wait()

	a := r.Content()
	fmt.Println(len(a))

	// rts.CleanUPFunc()

	if len(a) != 10003 {
		return fmt.Errorf("struct must contains 10003 items")
	}

	return nil

}

func saveResults(ctx context.Context, fo ...Host) ([]Host, []Host, error) {
	for _, h := range fo {
		r.Add(h.IP)
	}

	return nil, nil, nil
}

func compensate2(ctx context.Context, fo ...Host) ([]Host, []Host, error) {
	fo2 := make([]Host, 0, len(fo))
	for _, h := range fo {
		if h.IP == "COMP-OK" {
			r.Add("COMP-OK-2")
		}
	}

	return fo2, nil, nil
}

func compensate(ctx context.Context, fo ...Host) ([]Host, []Host, error) {
	cmp := []Host{}
	for _, h := range fo {
		r.Add(h.IP)
		if h.IP == "COMP" {
			cmp = append(cmp, Host{IP: "COMP-OK"})
		}
	}

	return fo, cmp, nil
}

func pf1(ctx context.Context, fo ...Host) ([]Host, []Host, error) {
	// n := rand.Intn(1) // n will be between 0 and 10
	// time.Sleep(time.Duration(n) * time.Second)
	fmt.Println("PF - 1")

	res := make([]Host, 0)
	for i := 0; i < 10000; i++ {
		res = append(res, Host{IP: fmt.Sprintf("PF-1-%d", i)})
	}

	return res, nil, nil

}

func pf2(ctx context.Context, fo ...Host) ([]Host, []Host, error) {
	// n := rand.Intn(0)
	// time.Sleep(time.Duration(n) * time.Second)
	fmt.Println("PF - 2")

	res := make([]Host, 0)
	for i := 0; i < 1; i++ {
		res = append(res, Host{IP: fmt.Sprintf("PF-2-%d", i)})
	}

	return res, nil, nil
}

func pf3(ctx context.Context, fo ...Host) ([]Host, []Host, error) {
	// n := rand.Intn(0) // n will be between 0 and 10
	//	time.Sleep(time.Duration(n) * time.Second)
	fmt.Println("PF - 3")
	for i := range fo {
		fmt.Println(fo[i].IP)
	}

	res := make([]Host, 0)
	for i := 0; i < 1; i++ {
		res = append(res, Host{IP: fmt.Sprintf("PF-3-%d", i)})
	}
	// res = append(res, Host{IP: "FO-COMP"})
	return res, nil, nil
}
