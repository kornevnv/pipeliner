// nolint

package saga

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v7"
	log "github.com/sirupsen/logrus"

	"sdg-gitlab.solar.local/golang/pipeliner.git/saga/step"

	rq "sdg-gitlab.solar.local/golang/pipeliner.git/queue/redisQueue"
	"sdg-gitlab.solar.local/golang/pipeliner.git/saga/coordinator"
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

// nolint: unused
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

	p1, err := coordinator.New(pName, gc, true)
	defer cancel()
	if err != nil {
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
	err = stepSave.Init(stSave.Name, stSaveQ, 1, gc, nil, stSave.saveResults, nil)
	if err != nil {
		return err
	}

	// COMPENSATE
	stComp := &StepComp[Host]{
		Name:     "step-cmp",
		saveDB:   stepSave,
		wasRetry: false,
	}
	stCompQ, _ := useRedisQueue[Host](fmt.Sprintf("%s-%s", pName, stComp.Name))
	err = stepComp.Init(stComp.Name, stCompQ, 1, gc, nil, stComp.stepF, nil)
	if err != nil {
		return err
	}

	// STEP 3
	st3 := &Step3[Host]{
		Name:   "step-3",
		saveDB: stepSave,
	}
	st3Q, _ := useRedisQueue[Host](fmt.Sprintf("%s-%s", pName, st3.Name))
	err = step3.Init(st3.Name, st3Q, 1, gc, nil, st3.stepF, nil)
	if err != nil {
		return err
	}

	// STEP 2
	st2 := &Step2[Host]{
		Name:  "step-2",
		step3: step3,
	}
	st2Q, _ := useRedisQueue[Host](fmt.Sprintf("%s-%s", pName, st2.Name))
	err = step2.Init(st2.Name, st2Q, 1, gc, nil, st2.stepF, nil)
	if err != nil {
		return err
	}

	st1 := &Step1{
		Name:  "step-1",
		step2: step2,
	}
	st1Q, _ := useRedisQueue[Host](fmt.Sprintf("%s-%s", pName, st1.Name))
	err = step1.Init(st1.Name, st1Q, 1, gc, nil, st1.stepF, nil)
	if err != nil {
		return err
	}
	rts := TaskController{
		CancelFunc:  cancel,
		SuspendFunc: p1.Suspend,
		CleanUPFunc: p1.CleanUP,
	}
	_ = rts

	err = step1.Push(ctx, Host{
		IP:      "127.0.0.1",
		Domains: nil,
	})
	if err != nil {
		log.WithError(err).Errorf("")
	}

	err = stepComp.Push(ctx, Host{
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

	p2 := &coordinator.Coordinator{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(3 * time.Second)
		fmt.Println("PAUSE|CANCEL")
		// rts.SuspendFunc(true)
		time.Sleep(10 * time.Second)

		p2, err = coordinator.New(pName, gc, true)
		if err != nil {
			cancel()
			log.Errorf("init orchestrator error")
		}
		stepsave2 := &step.Step[Host]{}
		err = stepsave2.Init(stSave.Name, stSaveQ, 1, gc, nil, stSave.saveResults, nil)
		if err != nil {
			log.WithError(err).Errorf("stepsave2")
		}
		stepcomp2 := &step.Step[Host]{}
		err = stepcomp2.Init(stComp.Name, stCompQ, 1, gc, nil, stComp.stepF, nil)
		if err != nil {
			log.WithError(err).Errorf("stepcomp2")
		}
		step32 := &step.Step[Host]{}
		err = step32.Init(st3.Name, st3Q, 1, gc, nil, st3.stepF, nil)
		if err != nil {
			log.WithError(err).Errorf("step32")
		}
		step22 := &step.Step[Host]{}
		err = step22.Init(st2.Name, st2Q, 1, gc, nil, st2.stepF, nil)
		if err != nil {
			log.WithError(err).Errorf("step22")
		}
		step12 := &step.Step[Host]{}
		err = step12.Init(st1.Name, st1Q, 1, gc, nil, st1.stepF, nil)
		if err != nil {
			log.WithError(err).Errorf("step12")
		}

		_ = p2.RegSteps(step12, step22, step32, stepsave2, stepcomp2)

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
		return fmt.Errorf("struct must contains 10001 items")
	}

	return nil

}

type Step1 struct {
	Name  string
	step2 *step.Step[Host]
}

func (s *Step1) stepF(ctx context.Context, fo Host) (bool, error) {
	// n := rand.Intn(1) // n will be between 0 and 10
	// time.Sleep(time.Duration(n) * time.Second)
	fmt.Println(s.Name)

	res := make([]Host, 0)
	for i := 0; i < 10000; i++ {
		res = append(res, Host{IP: fmt.Sprintf("%s-%d", s.Name, i)})
	}

	for i := range res {
		_ = s.step2.Push(ctx, res[i])
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
		_ = s.step3.Push(ctx, res[i])
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
		_ = s.saveDB.Push(ctx, res[i])
	}
	return true, nil
}

type StepComp[T Host] struct {
	Name     string
	saveDB   *step.Step[Host]
	wasRetry bool
}

func (s *StepComp[T]) stepF(ctx context.Context, fo Host) (bool, error) {
	if fo.IP == "COMP" && !s.wasRetry {
		s.wasRetry = true
		return false, nil
	}

	_ = s.saveDB.Push(ctx, fo)

	return true, nil
}

type StepSaveResult[T Host] struct {
	Name string
}

func (s *StepSaveResult[T]) saveResults(ctx context.Context, fo Host) (bool, error) {

	r.Add(fo.IP)

	return true, nil
}
