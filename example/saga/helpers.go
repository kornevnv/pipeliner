package saga

import (
	"sync"
)

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
