package common

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func Test0Worker(t *testing.T) {
	_, err := NewWorkerPool(context.Background(), 0, 0)
	assert.NotNil(t, err)
}

func TestRunTaskNoErr(t *testing.T) {
	wp, err := NewWorkerPool(context.Background(), 3, 10)
	assert.Nil(t, err)

	wp.Start()

	var v atomic.Int64
	for i := 0; i < 10; i++ {
		wp.Submit(func(ctx context.Context) error {
			v.Add(1)
			return nil
		})
	}

	wp.Done()
	assert.Nil(t, wp.Wait())
	assert.Equal(t, int64(10), v.Load())
}

func TestRunTaskReturnErr(t *testing.T) {
	wp, err := NewWorkerPool(context.Background(), 10, 10)
	assert.Nil(t, err)

	wp.Start()

	for i := 0; i < 100; i++ {
		wp.Submit(func(ctx context.Context) error {
			return errors.New("some err")
		})
	}

	wp.Done()
	assert.NotNil(t, wp.Wait())
}

func TestWaitJobs(t *testing.T) {
	wp, err := NewWorkerPool(context.Background(), 3, 10)
	assert.Nil(t, err)

	wp.Start()
	start := time.Now().Unix()
	jobs := make([]int64, 0)
	for i := 0; i < 10; i++ {
		job := func(ctx context.Context) error {
			//return errors.New("some err")
			time.Sleep(2 * time.Second)
			//return errors.New("some err")
			return nil
		}
		id := wp.SubmitWithId(job)
		jobs = append(jobs, id)
	}

	//time.Sleep(15 * time.Second)
	err = wp.WaitJobs(jobs)

	assert.NoError(t, err)
	duration := time.Now().Unix() - start
	assert.True(t, duration >= 8)
	//wp.Done()
}
