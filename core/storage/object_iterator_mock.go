package storage

var _ ObjectIterator = (*MockObjectIterator)(nil)

type MockObjectIterator struct {
	objs []ObjectAttr
	idx  int
}

func (m *MockObjectIterator) HasNext() bool {
	return m.idx < len(m.objs)
}

func (m *MockObjectIterator) Next() (ObjectAttr, error) {
	if !m.HasNext() {
		return ObjectAttr{}, nil
	}
	obj := m.objs[m.idx]
	m.idx++
	return obj, nil
}

func NewMockObjectIterator(objs []ObjectAttr) *MockObjectIterator {
	return &MockObjectIterator{objs: objs}
}
