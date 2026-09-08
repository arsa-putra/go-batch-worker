package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	redis "gopkg.in/redis.v5"
)

type RetryTestPayload struct {
	ID string `json:"id"`
}

type FailOnceProcessor struct {
	called int32
}

func (p *FailOnceProcessor) Name() string {
	return "retry_test"
}

func (p *FailOnceProcessor) Process(
	ctx context.Context,
	job RetryTestPayload,
) error {

	if atomic.AddInt32(
		&p.called,
		1,
	) == 1 {

		return errors.New(
			"first fail",
		)
	}

	return nil
}

func TestRetryFailedJob_DLQ(
	t *testing.T,
) {

	rdb := redis.NewClient(
		&redis.Options{
			Addr: "localhost:6379",
			DB:   15,
		},
	)

	rdb.FlushDb()

	w := NewBulkWorker(
		rdb,
		1,
		10,
		&FailOnceProcessor{},
	)

	job := RetryTestPayload{
		ID: "job-1",
	}

	w.saveFailedJob(
		job,
		errors.New("failed"),
	)

	autoRetry :=
		NewAutoRetryWorker(
			rdb,
			100*time.Millisecond,
		)

	autoRetry.RegisterBulk(
		w,
	)

	ctx, cancel :=
		context.WithCancel(
			context.Background(),
		)
	defer cancel()

	autoRetry.Start(ctx)

	time.Sleep(
		300 * time.Millisecond,
	)

	total, _ := rdb.LLen(
		fmt.Sprintf(
			"worker:%s:failed:list",
			w.Name(),
		),
	).Result()

	if total != 0 {
		t.Fatalf(
			"expected 0 failed jobs got %d",
			total,
		)
	}
}
