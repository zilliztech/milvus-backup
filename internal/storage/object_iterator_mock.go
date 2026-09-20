package storage

import "iter"

// NewMockObjectIterator returns a sequence that yields objs without error,
// for tests that stub Client.NewObjectIter. Living outside a _test.go file
// lets tests in importing packages use it too.
func NewMockObjectIterator(objs []ObjectAttr) iter.Seq2[ObjectAttr, error] {
	return func(yield func(ObjectAttr, error) bool) {
		for _, obj := range objs {
			if !yield(obj, nil) {
				return
			}
		}
	}
}
