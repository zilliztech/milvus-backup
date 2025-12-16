package secondary

import "sync"

type tsAlloc struct {
	ts uint64

	mu sync.Mutex
}

func newTTAlloc() *tsAlloc {
	return &tsAlloc{ts: 1}
}

func (t *tsAlloc) Alloc() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	ts := t.ts
	t.ts++

	return ts
}
