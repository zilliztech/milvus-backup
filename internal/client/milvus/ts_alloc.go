package milvus

import "sync"

// tsAlloc is a process-local monotonic timestamp allocator. It is used by the
// stream client to stamp wire time-ticks on outgoing replicate messages and is
// also exposed via Stream.Alloc so that callers can keep message body
// timestamps (e.g. MsgBase.Timestamp) on the same monotonic axis as the wire
// time-ticks.
type tsAlloc struct {
	mu sync.Mutex
	ts uint64
}

func newTSAlloc() *tsAlloc {
	return &tsAlloc{ts: 1}
}

func (t *tsAlloc) Alloc() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	ts := t.ts
	t.ts++

	return ts
}
