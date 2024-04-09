// nolint

package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v7"
	log "github.com/sirupsen/logrus"

	"sdg-gitlab.solar.local/golang/pipeliner.git/edp/processor"
	"sdg-gitlab.solar.local/golang/pipeliner.git/edp/step"
	rq "sdg-gitlab.solar.local/golang/pipeliner.git/queue/redisQueue"
)

const (
	dialTimeout = 5 * time.Minute
	// Timeout for socket reads.
	readTimeout = dialTimeout
	// Timeout for redis socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is ReadTimeout.
	writeTimeout = dialTimeout

	redisAddr = "localhost:6379"
	redisPass = "password"
	redisDB   = 0
)

type Host struct {
	IP      string
	Domains []string
}

var r = new(res)

type TaskController struct {
	CancelFunc  context.CancelFunc
	SuspendFunc func(force bool)
	CleanUPFunc func(ctx context.Context) error
}

// useRedisQueue враппер для инициализации очереди
func useRedisQueue[T any](name string) (step.Queue[T], error) {
	q, err := rq.New[T](name, &redis.Options{
		Addr:         redisAddr,
		Password:     redisPass,
		DB:           redisDB,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	})
	return q, err

}

func main() {
	err := Run()
	if err != nil {
		log.Fatal(err)
	}
}

type GlobalCounter struct {
	count atomic.Int64
}

func (g *GlobalCounter) Inc() {
	g.count.Add(1)
}
func (g *GlobalCounter) Dec() {
	g.count.Add(-1)
}
func (g *GlobalCounter) Count() int64 {
	return g.count.Load()
}

func Run() error {
	gc := &GlobalCounter{}
	ctx, cancel := context.WithCancel(context.Background())

	pName := "cpt-active"

	p1, err := processor.New(pName, gc, true)
	if err != nil {
		cancel()
		return fmt.Errorf("init orchestrator error")
	}

	step1 := &step.Step[Host]{}
	step2 := &step.Step[Host]{}
	step3 := &step.Step[Host]{}
	stepSave := &step.Step[Host]{}
	stepComp := &step.Step[Host]{}

	// SAVE To DB
	stSave := &StepSaveResult[Host]{Name: "step-save"}
	stSaveQ, _ := useRedisQueue[Host](fmt.Sprintf("%s-%s", pName, stSave.Name))
	stepSave, _ = step.NewStep[Host](stSave.Name, stSaveQ, 1, gc, nil, stSave.saveResults, nil)

	// COMPENSATE
	stComp := &StepComp[Host]{
		Name:     "step-cmp",
		saveDB:   stepSave,
		wasRetry: false,
	}
	stCompQ, _ := useRedisQueue[Host](fmt.Sprintf("%s-%s", pName, stComp.Name))
	stepComp, _ = step.NewStep[Host](stComp.Name, stCompQ, 1, gc, nil, stComp.stepF, nil)

	// STEP 3
	st3 := &Step3[Host]{
		Name:   "step-3",
		saveDB: stepSave,
	}
	st3Q, _ := useRedisQueue[Host](fmt.Sprintf("%s-%s", pName, st3.Name))
	step3, _ = step.NewStep[Host](st3.Name, st3Q, 1, gc, nil, st3.stepF, nil)

	// STEP 2
	st2 := &Step2[Host]{
		Name:  "step-2",
		step3: step3,
	}
	st2Q, _ := useRedisQueue[Host](fmt.Sprintf("%s-%s", pName, st2.Name))
	step2, _ = step.NewStep[Host](st2.Name, st2Q, 1, gc, nil, st2.stepF, nil)

	st1 := &Step1[Host]{
		Name:  "step-1",
		step2: step2,
	}
	st1Q, _ := useRedisQueue[Host](fmt.Sprintf("%s-%s", pName, st1.Name))
	step1, _ = step.NewStep[Host](st1.Name, st1Q, 1, gc, nil, st1.stepF, nil)

	rts := TaskController{
		CancelFunc:  cancel,
		SuspendFunc: p1.Suspend,
		CleanUPFunc: p1.CleanUP,
	}
	_ = rts

	err = step1.Publish(ctx, Host{
		IP:      "127.0.0.1",
		Domains: nil,
	})
	if err != nil {
		log.WithError(err).Errorf("")
	}

	err = stepComp.Publish(ctx, Host{
		IP:      "COMP",
		Domains: nil,
	})
	if err != nil {
		log.WithError(err).Errorf("")
	}

	_ = p1.RegSteps(step1, step2, step3, stepSave, stepComp)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = p1.Run(ctx)
		if err != nil {
			log.WithError(err).Errorf("")
		}
	}()

	p2 := &processor.Processor{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(3 * time.Second)
		fmt.Println("PAUSE|CANCEL")
		// rts.SuspendFunc(true)
		time.Sleep(10 * time.Second)

		p2, err = processor.New(pName, gc, true)
		if err != nil {
			cancel()
			log.Errorf("init orchestrator error")
		}
		stepSave_2, _ := step.NewStep[Host](stSave.Name, stSaveQ, 1, gc, nil, stSave.saveResults, nil)
		stepComp_2, _ := step.NewStep[Host](stComp.Name, stCompQ, 1, gc, nil, stComp.stepF, nil)
		step3_2, _ := step.NewStep[Host](st3.Name, st3Q, 1, gc, nil, st3.stepF, nil)
		step2_2, _ := step.NewStep[Host](st2.Name, st2Q, 1, gc, nil, st2.stepF, nil)
		step1_2, _ := step.NewStep[Host](st1.Name, st1Q, 1, gc, nil, st1.stepF, nil)

		_ = p2.RegSteps(step1_2, step2_2, step3_2, stepSave_2, stepComp_2)

		err = p2.Run(ctx)
		if err != nil {
			log.WithError(fmt.Errorf("run pipeliner error"))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(15 * time.Second)
		fmt.Println("END WAIT proc")

		p1.EndWaitInput()
		time.Sleep(10 * time.Second)
		fmt.Println("END WAIT proc2")
		p2.EndWaitInput()
	}()

	wg.Wait()

	a := r.Content()
	fmt.Println(len(a))

	// rts.CleanUPFunc()

	if len(a) != 10001 {
		return fmt.Errorf("struct must contains 10003 items")
	}

	return nil

}

type Step1[T Host] struct {
	Name  string
	step2 *step.Step[Host]
}

func (s *Step1[T]) stepF(ctx context.Context, fo Host) (bool, error) {
	// n := rand.Intn(1) // n will be between 0 and 10
	// time.Sleep(time.Duration(n) * time.Second)
	fmt.Println(s.Name)

	res := make([]Host, 0)
	for i := 0; i < 10000; i++ {
		res = append(res, Host{IP: fmt.Sprintf("%s-%d", s.Name, i)})
	}

	for i := range res {
		_ = s.step2.Publish(ctx, res[i])
	}

	return true, nil

}

type Step2[T Host] struct {
	Name  string
	step3 *step.Step[Host]
}

func (s *Step2[T]) stepF(ctx context.Context, fo Host) (bool, error) {
	fmt.Println(s.Name)

	res := make([]Host, 0)
	for i := 0; i < 1; i++ {
		res = append(res, Host{IP: fmt.Sprintf("%s-%d", s.Name, i)})
	}

	for i := range res {
		_ = s.step3.Publish(ctx, res[i])
	}

	return true, nil
}

type Step3[T Host] struct {
	Name   string
	saveDB *step.Step[Host]
}

func (s *Step3[T]) stepF(ctx context.Context, fo Host) (bool, error) {
	// n := rand.Intn(0) // n will be between 0 and 10
	//	time.Sleep(time.Duration(n) * time.Second)
	fmt.Println(s.Name)

	res := make([]Host, 0)
	for i := 0; i < 1; i++ {
		res = append(res, Host{IP: fmt.Sprintf("%s-%d", s.Name, i)})
	}

	for i := range res {
		_ = s.saveDB.Publish(ctx, res[i])
	}
	// res = append(res, Host{IP: "FO-COMP"})
	return true, nil
}

type StepComp[T Host] struct {
	Name     string
	saveDB   *step.Step[Host]
	step3    *step.Step[Host]
	wasRetry bool
}

func (s *StepComp[T]) stepF(ctx context.Context, fo Host) (bool, error) {
	if fo.IP == "COMP" && !s.wasRetry {
		s.wasRetry = true
		return false, nil
	}

	_ = s.saveDB.Publish(ctx, fo)

	return true, nil
}

type StepSaveResult[T Host] struct {
	Name string
}

func (s *StepSaveResult[T]) saveResults(ctx context.Context, fo Host) (bool, error) {

	r.Add(fo.IP)

	return true, nil
}
