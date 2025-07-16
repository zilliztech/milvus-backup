// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSleep(t *testing.T) {
	opt := Sleep(100 * time.Millisecond)
	c := newDefaultConfig()
	opt(c)
	assert.Equal(t, 100*time.Millisecond, c.sleep)
}

func TestMaxSleepTime(t *testing.T) {
	// maxSleepTime is larger than sleep
	opt := MaxSleepTime(600 * time.Millisecond)
	c := newDefaultConfig()
	opt(c)
	assert.Equal(t, 600*time.Millisecond, c.maxSleepTime)

	// maxSleepTime is smaller than sleep
	opt = MaxSleepTime(100 * time.Millisecond)
	c = newDefaultConfig()
	opt(c)
	assert.Equal(t, 400*time.Millisecond, c.maxSleepTime)
}

func TestDo(t *testing.T) {
	n := 0
	testFn := func() error {
		if n < 3 {
			n++
			return errors.New("some error")
		}
		return nil
	}

	err := Do(context.Background(), testFn)
	assert.NoError(t, err)
}

func TestAllError(t *testing.T) {
	testFn := func() error {
		return errors.New("some error")
	}

	err := Do(context.Background(), testFn, Attempts(3))
	assert.Error(t, err)
}

func TestUnRecoveryError(t *testing.T) {
	attempts := 0
	testFn := func() error {
		attempts++
		return Unrecoverable(errors.New("some error"))
	}

	err := Do(context.Background(), testFn, Attempts(3))
	assert.Error(t, err)
	assert.Equal(t, 1, attempts)
}
