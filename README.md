# pipeliner

## async, parallel, data-flows pipeline

## pipeliner has signal-based queue reader

### examlpe

``` go

const (
	stage1 = "stage_1"
	stage2 = "stage_2"
	stage3 = "stage_3"
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

// Clone
func (r Obj) Clone() DataFlowObj {
	return Obj{
		key: r.key,
	}
}
func main {
    ctx := context.TODO()
    activePipe := pipeline.NewPipeline[DataFlowObj](false)
    _ = activePipe.AddStage(stage1, sf1, nil, nil, 1, stage2, stage3)
    _ = activePipe.AddStage(stage2, sf2, nil, nil, 3, stage3)
    _ = activePipe.AddStage(stage3, sf3, nil, nil, 1)
    
    _ = activePipe.Push(stage1, Obj{key: "input-1"})
    err := activePipe.Run(ctx)
}

func sf1(ctx context.Context, val DataFlowObj) ([]DataFlowObj, error) {
	time.Sleep(1 * time.Second)
	fmt.Println("sf1", val)
	return gen("sf1", val, genCount), nil
}

func sf2(ctx context.Context, val DataFlowObj) ([]DataFlowObj, error) {
	time.Sleep(3 * time.Second)
	fmt.Println("sf2", val)
	return gen("sf2", val, genCount), nil
}

func sf3(ctx context.Context, val DataFlowObj) ([]DataFlowObj, error) {
	time.Sleep(5 * time.Second)
	fmt.Println("sf3", val)
	return gen("sf3", val, genCount), nil
}

func gen(name string, t DataFlowObj, count int) []DataFlowObj {
	res := make([]DataFlowObj, count)
	for i := 0; i < count; i++ {
		res[i] = Obj{
			key: fmt.Sprintf("%s -> %s", t.Key(), name),
		}
	}
	return res
}
``