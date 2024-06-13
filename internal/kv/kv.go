// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

// CompareFailedError is a helper type for checking MetaKv CompareAndSwap series func error type
type CompareFailedError struct {
	internalError error
}

// Error implements error interface
func (e *CompareFailedError) Error() string {
	return e.internalError.Error()
}

// NewCompareFailedError wraps error into NewCompareFailedError
func NewCompareFailedError(err error) error {
	return &CompareFailedError{internalError: err}
}

// BaseKV contains base operations of kv. Include save, load and remove.
type BaseKV interface {
	Load(key string) (string, error)
	MultiLoad(keys []string) ([]string, error)
	LoadWithPrefix(key string) ([]string, []string, error)
	Save(key, value string) error
	MultiSave(kvs map[string]string) error
	Remove(key string) error
	MultiRemove(keys []string) error
	RemoveWithPrefix(key string) error

	Close()
}
