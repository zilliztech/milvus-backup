// Code generated by mockery; DO NOT EDIT.
// github.com/vektra/mockery
// template: testify

package storage

import (
	"context"

	mock "github.com/stretchr/testify/mock"
)

// NewMockClient creates a new instance of MockClient. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockClient(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockClient {
	mock := &MockClient{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

// MockClient is an autogenerated mock type for the Client type
type MockClient struct {
	mock.Mock
}

type MockClient_Expecter struct {
	mock *mock.Mock
}

func (_m *MockClient) EXPECT() *MockClient_Expecter {
	return &MockClient_Expecter{mock: &_m.Mock}
}

// BucketExist provides a mock function for the type MockClient
func (_mock *MockClient) BucketExist(ctx context.Context, prefix string) (bool, error) {
	ret := _mock.Called(ctx, prefix)

	if len(ret) == 0 {
		panic("no return value specified for BucketExist")
	}

	var r0 bool
	var r1 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, string) (bool, error)); ok {
		return returnFunc(ctx, prefix)
	}
	if returnFunc, ok := ret.Get(0).(func(context.Context, string) bool); ok {
		r0 = returnFunc(ctx, prefix)
	} else {
		r0 = ret.Get(0).(bool)
	}
	if returnFunc, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = returnFunc(ctx, prefix)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// MockClient_BucketExist_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'BucketExist'
type MockClient_BucketExist_Call struct {
	*mock.Call
}

// BucketExist is a helper method to define mock.On call
//   - ctx context.Context
//   - prefix string
func (_e *MockClient_Expecter) BucketExist(ctx interface{}, prefix interface{}) *MockClient_BucketExist_Call {
	return &MockClient_BucketExist_Call{Call: _e.mock.On("BucketExist", ctx, prefix)}
}

func (_c *MockClient_BucketExist_Call) Run(run func(ctx context.Context, prefix string)) *MockClient_BucketExist_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		var arg1 string
		if args[1] != nil {
			arg1 = args[1].(string)
		}
		run(
			arg0,
			arg1,
		)
	})
	return _c
}

func (_c *MockClient_BucketExist_Call) Return(b bool, err error) *MockClient_BucketExist_Call {
	_c.Call.Return(b, err)
	return _c
}

func (_c *MockClient_BucketExist_Call) RunAndReturn(run func(ctx context.Context, prefix string) (bool, error)) *MockClient_BucketExist_Call {
	_c.Call.Return(run)
	return _c
}

// Config provides a mock function for the type MockClient
func (_mock *MockClient) Config() Config {
	ret := _mock.Called()

	if len(ret) == 0 {
		panic("no return value specified for Config")
	}

	var r0 Config
	if returnFunc, ok := ret.Get(0).(func() Config); ok {
		r0 = returnFunc()
	} else {
		r0 = ret.Get(0).(Config)
	}
	return r0
}

// MockClient_Config_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Config'
type MockClient_Config_Call struct {
	*mock.Call
}

// Config is a helper method to define mock.On call
func (_e *MockClient_Expecter) Config() *MockClient_Config_Call {
	return &MockClient_Config_Call{Call: _e.mock.On("Config")}
}

func (_c *MockClient_Config_Call) Run(run func()) *MockClient_Config_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockClient_Config_Call) Return(config Config) *MockClient_Config_Call {
	_c.Call.Return(config)
	return _c
}

func (_c *MockClient_Config_Call) RunAndReturn(run func() Config) *MockClient_Config_Call {
	_c.Call.Return(run)
	return _c
}

// CopyObject provides a mock function for the type MockClient
func (_mock *MockClient) CopyObject(ctx context.Context, i CopyObjectInput) error {
	ret := _mock.Called(ctx, i)

	if len(ret) == 0 {
		panic("no return value specified for CopyObject")
	}

	var r0 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, CopyObjectInput) error); ok {
		r0 = returnFunc(ctx, i)
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

// MockClient_CopyObject_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CopyObject'
type MockClient_CopyObject_Call struct {
	*mock.Call
}

// CopyObject is a helper method to define mock.On call
//   - ctx context.Context
//   - i CopyObjectInput
func (_e *MockClient_Expecter) CopyObject(ctx interface{}, i interface{}) *MockClient_CopyObject_Call {
	return &MockClient_CopyObject_Call{Call: _e.mock.On("CopyObject", ctx, i)}
}

func (_c *MockClient_CopyObject_Call) Run(run func(ctx context.Context, i CopyObjectInput)) *MockClient_CopyObject_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		var arg1 CopyObjectInput
		if args[1] != nil {
			arg1 = args[1].(CopyObjectInput)
		}
		run(
			arg0,
			arg1,
		)
	})
	return _c
}

func (_c *MockClient_CopyObject_Call) Return(err error) *MockClient_CopyObject_Call {
	_c.Call.Return(err)
	return _c
}

func (_c *MockClient_CopyObject_Call) RunAndReturn(run func(ctx context.Context, i CopyObjectInput) error) *MockClient_CopyObject_Call {
	_c.Call.Return(run)
	return _c
}

// CreateBucket provides a mock function for the type MockClient
func (_mock *MockClient) CreateBucket(ctx context.Context) error {
	ret := _mock.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for CreateBucket")
	}

	var r0 error
	if returnFunc, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = returnFunc(ctx)
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

// MockClient_CreateBucket_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CreateBucket'
type MockClient_CreateBucket_Call struct {
	*mock.Call
}

// CreateBucket is a helper method to define mock.On call
//   - ctx context.Context
func (_e *MockClient_Expecter) CreateBucket(ctx interface{}) *MockClient_CreateBucket_Call {
	return &MockClient_CreateBucket_Call{Call: _e.mock.On("CreateBucket", ctx)}
}

func (_c *MockClient_CreateBucket_Call) Run(run func(ctx context.Context)) *MockClient_CreateBucket_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		run(
			arg0,
		)
	})
	return _c
}

func (_c *MockClient_CreateBucket_Call) Return(err error) *MockClient_CreateBucket_Call {
	_c.Call.Return(err)
	return _c
}

func (_c *MockClient_CreateBucket_Call) RunAndReturn(run func(ctx context.Context) error) *MockClient_CreateBucket_Call {
	_c.Call.Return(run)
	return _c
}

// DeleteObject provides a mock function for the type MockClient
func (_mock *MockClient) DeleteObject(ctx context.Context, key string) error {
	ret := _mock.Called(ctx, key)

	if len(ret) == 0 {
		panic("no return value specified for DeleteObject")
	}

	var r0 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = returnFunc(ctx, key)
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

// MockClient_DeleteObject_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DeleteObject'
type MockClient_DeleteObject_Call struct {
	*mock.Call
}

// DeleteObject is a helper method to define mock.On call
//   - ctx context.Context
//   - key string
func (_e *MockClient_Expecter) DeleteObject(ctx interface{}, key interface{}) *MockClient_DeleteObject_Call {
	return &MockClient_DeleteObject_Call{Call: _e.mock.On("DeleteObject", ctx, key)}
}

func (_c *MockClient_DeleteObject_Call) Run(run func(ctx context.Context, key string)) *MockClient_DeleteObject_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		var arg1 string
		if args[1] != nil {
			arg1 = args[1].(string)
		}
		run(
			arg0,
			arg1,
		)
	})
	return _c
}

func (_c *MockClient_DeleteObject_Call) Return(err error) *MockClient_DeleteObject_Call {
	_c.Call.Return(err)
	return _c
}

func (_c *MockClient_DeleteObject_Call) RunAndReturn(run func(ctx context.Context, key string) error) *MockClient_DeleteObject_Call {
	_c.Call.Return(run)
	return _c
}

// GetObject provides a mock function for the type MockClient
func (_mock *MockClient) GetObject(ctx context.Context, key string) (*Object, error) {
	ret := _mock.Called(ctx, key)

	if len(ret) == 0 {
		panic("no return value specified for GetObject")
	}

	var r0 *Object
	var r1 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, string) (*Object, error)); ok {
		return returnFunc(ctx, key)
	}
	if returnFunc, ok := ret.Get(0).(func(context.Context, string) *Object); ok {
		r0 = returnFunc(ctx, key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*Object)
		}
	}
	if returnFunc, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = returnFunc(ctx, key)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// MockClient_GetObject_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetObject'
type MockClient_GetObject_Call struct {
	*mock.Call
}

// GetObject is a helper method to define mock.On call
//   - ctx context.Context
//   - key string
func (_e *MockClient_Expecter) GetObject(ctx interface{}, key interface{}) *MockClient_GetObject_Call {
	return &MockClient_GetObject_Call{Call: _e.mock.On("GetObject", ctx, key)}
}

func (_c *MockClient_GetObject_Call) Run(run func(ctx context.Context, key string)) *MockClient_GetObject_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		var arg1 string
		if args[1] != nil {
			arg1 = args[1].(string)
		}
		run(
			arg0,
			arg1,
		)
	})
	return _c
}

func (_c *MockClient_GetObject_Call) Return(object *Object, err error) *MockClient_GetObject_Call {
	_c.Call.Return(object, err)
	return _c
}

func (_c *MockClient_GetObject_Call) RunAndReturn(run func(ctx context.Context, key string) (*Object, error)) *MockClient_GetObject_Call {
	_c.Call.Return(run)
	return _c
}

// HeadObject provides a mock function for the type MockClient
func (_mock *MockClient) HeadObject(ctx context.Context, key string) (ObjectAttr, error) {
	ret := _mock.Called(ctx, key)

	if len(ret) == 0 {
		panic("no return value specified for HeadObject")
	}

	var r0 ObjectAttr
	var r1 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, string) (ObjectAttr, error)); ok {
		return returnFunc(ctx, key)
	}
	if returnFunc, ok := ret.Get(0).(func(context.Context, string) ObjectAttr); ok {
		r0 = returnFunc(ctx, key)
	} else {
		r0 = ret.Get(0).(ObjectAttr)
	}
	if returnFunc, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = returnFunc(ctx, key)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// MockClient_HeadObject_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HeadObject'
type MockClient_HeadObject_Call struct {
	*mock.Call
}

// HeadObject is a helper method to define mock.On call
//   - ctx context.Context
//   - key string
func (_e *MockClient_Expecter) HeadObject(ctx interface{}, key interface{}) *MockClient_HeadObject_Call {
	return &MockClient_HeadObject_Call{Call: _e.mock.On("HeadObject", ctx, key)}
}

func (_c *MockClient_HeadObject_Call) Run(run func(ctx context.Context, key string)) *MockClient_HeadObject_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		var arg1 string
		if args[1] != nil {
			arg1 = args[1].(string)
		}
		run(
			arg0,
			arg1,
		)
	})
	return _c
}

func (_c *MockClient_HeadObject_Call) Return(objectAttr ObjectAttr, err error) *MockClient_HeadObject_Call {
	_c.Call.Return(objectAttr, err)
	return _c
}

func (_c *MockClient_HeadObject_Call) RunAndReturn(run func(ctx context.Context, key string) (ObjectAttr, error)) *MockClient_HeadObject_Call {
	_c.Call.Return(run)
	return _c
}

// ListPrefix provides a mock function for the type MockClient
func (_mock *MockClient) ListPrefix(ctx context.Context, prefix string, recursive bool) (ObjectIterator, error) {
	ret := _mock.Called(ctx, prefix, recursive)

	if len(ret) == 0 {
		panic("no return value specified for ListPrefix")
	}

	var r0 ObjectIterator
	var r1 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, string, bool) (ObjectIterator, error)); ok {
		return returnFunc(ctx, prefix, recursive)
	}
	if returnFunc, ok := ret.Get(0).(func(context.Context, string, bool) ObjectIterator); ok {
		r0 = returnFunc(ctx, prefix, recursive)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(ObjectIterator)
		}
	}
	if returnFunc, ok := ret.Get(1).(func(context.Context, string, bool) error); ok {
		r1 = returnFunc(ctx, prefix, recursive)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

// MockClient_ListPrefix_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListPrefix'
type MockClient_ListPrefix_Call struct {
	*mock.Call
}

// ListPrefix is a helper method to define mock.On call
//   - ctx context.Context
//   - prefix string
//   - recursive bool
func (_e *MockClient_Expecter) ListPrefix(ctx interface{}, prefix interface{}, recursive interface{}) *MockClient_ListPrefix_Call {
	return &MockClient_ListPrefix_Call{Call: _e.mock.On("ListPrefix", ctx, prefix, recursive)}
}

func (_c *MockClient_ListPrefix_Call) Run(run func(ctx context.Context, prefix string, recursive bool)) *MockClient_ListPrefix_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		var arg1 string
		if args[1] != nil {
			arg1 = args[1].(string)
		}
		var arg2 bool
		if args[2] != nil {
			arg2 = args[2].(bool)
		}
		run(
			arg0,
			arg1,
			arg2,
		)
	})
	return _c
}

func (_c *MockClient_ListPrefix_Call) Return(objectIterator ObjectIterator, err error) *MockClient_ListPrefix_Call {
	_c.Call.Return(objectIterator, err)
	return _c
}

func (_c *MockClient_ListPrefix_Call) RunAndReturn(run func(ctx context.Context, prefix string, recursive bool) (ObjectIterator, error)) *MockClient_ListPrefix_Call {
	_c.Call.Return(run)
	return _c
}

// UploadObject provides a mock function for the type MockClient
func (_mock *MockClient) UploadObject(ctx context.Context, i UploadObjectInput) error {
	ret := _mock.Called(ctx, i)

	if len(ret) == 0 {
		panic("no return value specified for UploadObject")
	}

	var r0 error
	if returnFunc, ok := ret.Get(0).(func(context.Context, UploadObjectInput) error); ok {
		r0 = returnFunc(ctx, i)
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

// MockClient_UploadObject_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'UploadObject'
type MockClient_UploadObject_Call struct {
	*mock.Call
}

// UploadObject is a helper method to define mock.On call
//   - ctx context.Context
//   - i UploadObjectInput
func (_e *MockClient_Expecter) UploadObject(ctx interface{}, i interface{}) *MockClient_UploadObject_Call {
	return &MockClient_UploadObject_Call{Call: _e.mock.On("UploadObject", ctx, i)}
}

func (_c *MockClient_UploadObject_Call) Run(run func(ctx context.Context, i UploadObjectInput)) *MockClient_UploadObject_Call {
	_c.Call.Run(func(args mock.Arguments) {
		var arg0 context.Context
		if args[0] != nil {
			arg0 = args[0].(context.Context)
		}
		var arg1 UploadObjectInput
		if args[1] != nil {
			arg1 = args[1].(UploadObjectInput)
		}
		run(
			arg0,
			arg1,
		)
	})
	return _c
}

func (_c *MockClient_UploadObject_Call) Return(err error) *MockClient_UploadObject_Call {
	_c.Call.Return(err)
	return _c
}

func (_c *MockClient_UploadObject_Call) RunAndReturn(run func(ctx context.Context, i UploadObjectInput) error) *MockClient_UploadObject_Call {
	_c.Call.Return(run)
	return _c
}
