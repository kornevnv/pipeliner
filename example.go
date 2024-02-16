package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	fl "flower/flower"
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

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	flwr, err := fl.NewFlower[Host]("cpt-active-1", false)
	if err != nil {
		log.WithError(err).Errorf("")
	}

	flwr.UseRedisQueue("localhost:6379", "password", 0)

	rts := TaskController{
		CancelFunc:  cancel,
		SuspendFunc: flwr.Suspend,
		CleanUPFunc: flwr.CleanUP,
	}
	_ = rts

	err = flwr.AddFlow("1", 2, []string{"2", "save"}, pf1)
	if err != nil {
		log.WithError(err).Errorf("")
	}

	err = flwr.AddFlow("2", 2, []string{"3"}, pf2)
	if err != nil {
		log.WithError(err).Errorf("")
	}

	err = flwr.AddFlow("3", 2, []string{"save"}, pf3)
	if err != nil {
		log.WithError(err).Errorf("")
	}

	err = flwr.AddFlow("save", 1, nil, saveResults)
	if err != nil {
		log.WithError(err).Errorf("")
	}

	err = flwr.Push("1", Host{
		IP:      "127.0.0.1",
		Domains: nil,
	})
	if err != nil {
		log.WithError(err).Errorf("")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = flwr.Run(ctx)
		if err != nil {
			log.WithError(err).Errorf("")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Second)
		fmt.Println("PAUSE|CANCEL")
		rts.SuspendFunc(false)
		// rts.CancelFunc()
	}()

	wg.Wait()

	a := r.Content()

	// rts.CleanUPFunc()

	fmt.Println(a)

}

func saveResults(ctx context.Context, fo ...Host) ([]Host, []Host, error) {
	cmp := []Host{}
	for _, h := range fo {
		r.Add(h.IP)
		if h.IP == "FO-COMP" {
			cmp = append(cmp, Host{IP: "COMP-OK"})
		}
	}

	return nil, cmp, nil
}

func pf1(ctx context.Context, fo ...Host) ([]Host, []Host, error) {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(1) // n will be between 0 and 10
	fmt.Printf("Sleeping %d seconds...\n", n)
	time.Sleep(time.Duration(n) * time.Second)
	fmt.Println("PF - 1")

	res := make([]Host, 0)
	for i := 0; i < 100000; i++ {
		res = append(res, Host{IP: fmt.Sprintf("PF-1-%d", i)})
	}

	return res, nil, nil

}

func pf2(ctx context.Context, fo ...Host) ([]Host, []Host, error) {
	// rand.Seed(time.Now().UnixNano())
	// n := rand.Intn(2) // n will be between 0 and 10
	// fmt.Printf("Sleeping %d seconds...\n", n)
	// time.Sleep(time.Duration(n) * time.Second)
	fmt.Println("PF - 2")

	res := make([]Host, 0)
	for i := 0; i < 100; i++ {
		res = append(res, Host{IP: fmt.Sprintf("PF-2-%d", i)})
	}

	return res, nil, nil
}

func pf3(ctx context.Context, fo ...Host) ([]Host, []Host, error) {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(2) // n will be between 0 and 10
	fmt.Printf("Sleeping %d seconds...\n", n)
	//	time.Sleep(time.Duration(n) * time.Second)
	fmt.Println("PF - 3")
	for i := range fo {
		fmt.Println(fo[i].IP)
	}

	res := make([]Host, 0)
	for i := 0; i < 1000; i++ {
		res = append(res, Host{IP: fmt.Sprintf("PF-3-%d", i)})
	}
	res = append(res, Host{IP: "FO-COMP"})
	return res, nil, nil
}
