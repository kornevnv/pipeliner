package pipeliner_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/kornevnv/pipeliner"
)

const (
	stage1 = "stage_1"
	stage2 = "stage_2"
	stage3 = "stage_3"

	genCount = 3
)

// DataFlowObj object interface
type DataFlowObj interface {
	Clone() DataFlowObj
	Key() string
}

// Obj - same object
type Obj struct {
	key string
}

func (o Obj) Key() string {
	return o.key
}

// Clone
func (o Obj) Clone() DataFlowObj {
	return Obj{
		key: o.key,
	}
}

// sMap map with mutex
type sMap struct {
	mu sync.Mutex

	data map[string]struct{}
}

var dataMap1 = &sMap{
	mu:   sync.Mutex{},
	data: make(map[string]struct{}),
}
var dataMap2 = &sMap{
	mu:   sync.Mutex{},
	data: make(map[string]struct{}),
}
var dataMap3 = &sMap{
	mu:   sync.Mutex{},
	data: make(map[string]struct{}),
}

var dataMapPost = &sMap{
	mu:   sync.Mutex{},
	data: make(map[string]struct{}),
}

func TestPipeliner(t *testing.T) {
	ctx := context.TODO()
	activePipe := pipeliner.NewPipeline[DataFlowObj](false)
	err := activePipe.AddStage(stage1, sF1, postF, nil, 1, stage2, stage3)

	if err != nil {
		t.Fatal("[add stage1] err should not be empty")
	}

	err = activePipe.AddStage(stage2, sF2, nil, nil, 3, stage3)
	if err != nil {
		t.Fatal("[add stage2] err should not be empty")
	}

	err = activePipe.AddStage(stage3, sF3, postF, nil, 1)
	if err != nil {
		t.Fatal("[add stage3] err should not be empty")
	}

	err = activePipe.Push(stage1, Obj{key: "input-1"})
	if err != nil {
		t.Fatal("[add input] err should not be empty")
	}
	err = activePipe.Run(ctx)

	// 1x3
	if len(dataMap1.data) != 3 {
		t.Fatal("[stage1] should be returned 3 object ")
	}

	// 3x3
	if len(dataMap2.data) != 9 {
		t.Fatal("[stage2] should be returned 9 object ")
	}

	// 9x3
	if len(dataMap3.data) != 36 {
		t.Fatal("[stage3] should be returned 36 object ")
	}

	// 3+36 (stage1 out + stage3 out)
	if len(dataMapPost.data) != 13 {
		t.Fatal("[postFunc] should be returned 39 object ")
	}
}

// sF1 process function for stage1
func sF1(ctx context.Context, val DataFlowObj) ([]DataFlowObj, error) {
	objects := gen("sF1", val, genCount)

	dataMap1.mu.Lock()
	defer dataMap1.mu.Unlock()
	for i := range objects {
		dataMap1.data[objects[i].Key()] = struct{}{}
	}
	return objects, nil
}

// sF2 process function for stage2
func sF2(ctx context.Context, val DataFlowObj) ([]DataFlowObj, error) {
	objects := gen("sF2", val, genCount)

	dataMap2.mu.Lock()
	defer dataMap2.mu.Unlock()
	for i := range objects {
		dataMap2.data[objects[i].Key()] = struct{}{}
	}
	return objects, nil
}

// sF3 process function for stage3
func sF3(ctx context.Context, val DataFlowObj) ([]DataFlowObj, error) {
	objects := gen("sF3", val, genCount)

	dataMap3.mu.Lock()
	defer dataMap3.mu.Unlock()
	for i := range objects {
		dataMap3.data[objects[i].Key()] = struct{}{}
	}
	return objects, nil
}

// postF post process function for all stages
func postF(ctx context.Context, objects []DataFlowObj) []DataFlowObj {

	dataMapPost.mu.Lock()
	defer dataMapPost.mu.Unlock()

	for i := range objects {
		dataMapPost.data[objects[i].Key()] = struct{}{}
	}

	return objects
}

// gen helper function for generate synthetic data
func gen(name string, t DataFlowObj, count int) []DataFlowObj {
	res := make([]DataFlowObj, count)
	for i := 0; i < count; i++ {
		res[i] = Obj{
			key: fmt.Sprintf("%s -> %s (%d)", t.Key(), name, i),
		}
	}
	return res
}
