package storage

import "context"

var _ ObjectIterator = (*MockObjectIterator)(nil)

type MockObjectIterator struct {
	objs []ObjectAttr
	idx  int
}

func (m *MockObjectIterator) Next(_ context.Context) (ObjectAttr, bool, error) {
	if m.idx >= len(m.objs) {
		return ObjectAttr{}, false, nil
	}
	obj := m.objs[m.idx]
	m.idx++
	return obj, true, nil
}

func (m *MockObjectIterator) Close() error { return nil }

func NewMockObjectIterator(objs []ObjectAttr) *MockObjectIterator {
	return &MockObjectIterator{objs: objs}
}
