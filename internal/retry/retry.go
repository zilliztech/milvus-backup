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
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-backup/internal/log"
)

// Do will run function with retry mechanism.
// fn is the func to run.
// Option can control the retry times and timeout.
func Do(ctx context.Context, fn func() error, opts ...Option) error {

	c := newDefaultConfig()

	for _, opt := range opts {
		opt(c)
	}

	var err error

	for i := uint(0); i < c.attempts; i++ {
		if innerErr := fn(); innerErr != nil {
			if i%10 == 0 {
				log.Debug("retry func failed", zap.Uint("retry time", i), zap.Error(err))
			}

			if IsUnRecoverable(innerErr) {
				return innerErr
			}

			err = errors.Join(err, innerErr)

			select {
			case <-time.After(c.sleep):
			case <-ctx.Done():
				return errors.Join(err, ctx.Err())
			}

			c.sleep *= 2
			if c.sleep > c.maxSleepTime {
				c.sleep = c.maxSleepTime
			}
		} else {
			return nil
		}
	}

	return err
}

type unrecoverableError struct {
	error
}

// Unrecoverable method wrap an error to unrecoverableError. This will make retry
// quick return.
func Unrecoverable(err error) error {
	return unrecoverableError{err}
}

// IsUnRecoverable is used to judge whether the error is wrapped by unrecoverableError.
func IsUnRecoverable(err error) bool {
	var ue unrecoverableError
	isUnrecoverable := errors.As(err, &ue)
	return isUnrecoverable
}
