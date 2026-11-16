package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
	redis5 "gopkg.in/redis.v5"
)

func redisTestAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set REDIS_TEST_ADDR to an isolated Redis instance for integration tests")
	}
	return addr
}

func newOptimizationTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisTestAddr(t), DB: 15})
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flush test Redis DB: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestRedisV5PipelineAndRetryIndex(t *testing.T) {
	rdb := redis5.NewClient(&redis5.Options{Addr: redisTestAddr(t), DB: 14})
	if err := rdb.FlushDb().Err(); err != nil {
		t.Fatalf("flush test Redis DB: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	tracker := NewRedisBatchTracker(
		rdb,
		BatchOptions{StoreSuccessItems: true, MaxStoredSuccesses: 10},
		DefaultTrackerConfig(),
	)
	if err := tracker.trackPendingPayloadAndLinkJob(
		"v5-batch",
		"item-1",
		"v5-job",
		map[string]string{"value": "test"},
	); err != nil {
		t.Fatalf("v5 pending pipeline: %v", err)
	}
	if !rdb.SIsMember(tracker.jobBatchesKey("v5-job"), "v5-batch").Val() {
		t.Fatal("v5 pipeline did not link batch to job")
	}
	if err := tracker.RegisterBatch("v5-summary", 1); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SaveResult("v5-summary", BatchItemResult{Key: "ok", Success: true}); err != nil {
		t.Fatal(err)
	}
	batches, err := tracker.ListBatches()
	if err != nil || len(batches) != 1 || batches[0].Success != 1 {
		t.Fatalf("v5 pipelined summary=%#v err=%v", batches, err)
	}

	state := &WebhookRetryState{BatchID: "v5-retry", NextRetryAt: time.Now().Add(-time.Second).Unix()}
	if err := SaveWebhookRetryState(rdb, state); err != nil {
		t.Fatalf("save v5 retry state: %v", err)
	}
	due, err := findDueWebhookRetryStates(rdb, time.Now().Unix(), 10)
	if err != nil || len(due) != 1 || due[0].BatchID != state.BatchID {
		t.Fatalf("v5 due states=%#v err=%v", due, err)
	}
}

func TestWebhookRetryIndexMigratesAndTracksState(t *testing.T) {
	rdb := newOptimizationTestRedis(t)
	legacy := &WebhookRetryState{
		BatchID:     "legacy-batch",
		Attempt:     1,
		NextRetryAt: time.Now().Add(-time.Second).Unix(),
	}
	payload, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(context.Background(), webhookRetryStateKey(legacy.BatchID), payload, 0).Err(); err != nil {
		t.Fatal(err)
	}

	states, err := FindWebhookRetryStates(rdb)
	if err != nil {
		t.Fatalf("list migrated retry state: %v", err)
	}
	if len(states) != 1 || states[0].BatchID != legacy.BatchID {
		t.Fatalf("unexpected migrated states: %#v", states)
	}

	future := &WebhookRetryState{
		BatchID:     "future-batch",
		Attempt:     2,
		NextRetryAt: time.Now().Add(time.Hour).Unix(),
	}
	if err := SaveWebhookRetryState(rdb, future); err != nil {
		t.Fatalf("save indexed retry state: %v", err)
	}

	due, err := findDueWebhookRetryStates(rdb, time.Now().Unix(), 100)
	if err != nil {
		t.Fatalf("find due retry states: %v", err)
	}
	if len(due) != 1 || due[0].BatchID != legacy.BatchID {
		t.Fatalf("unexpected due states: %#v", due)
	}

	count, err := countWebhookRetryStates(rdb)
	if err != nil || count != 2 {
		t.Fatalf("retry count = %d, err = %v; want 2", count, err)
	}
	if err := DeleteWebhookRetryState(rdb, legacy.BatchID); err != nil {
		t.Fatalf("delete retry state: %v", err)
	}
	count, err = countWebhookRetryStates(rdb)
	if err != nil || count != 1 {
		t.Fatalf("retry count after delete = %d, err = %v; want 1", count, err)
	}
}

func TestBuildResultPagePreservesAllPendingItems(t *testing.T) {
	rdb := newOptimizationTestRedis(t)
	tracker := NewRedisBatchTracker(
		rdb,
		BatchOptions{StoreSuccessItems: true, StoreFailedItems: true, MaxStoredSuccesses: 10, MaxStoredErrors: 10},
		DefaultTrackerConfig(),
	)
	if err := tracker.RegisterBatch("page-batch", 10); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := tracker.SaveResult("page-batch", BatchItemResult{
			Key:     fmt.Sprintf("item-%d", i),
			Success: true,
			Message: "ok",
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := tracker.TrackPendingPayload(
			"page-batch",
			fmt.Sprintf("pending-%d", i),
			map[string]string{"value": fmt.Sprintf("pending-%d", i)},
		); err != nil {
			t.Fatal(err)
		}
	}

	result, totalFailed, totalSuccess, err := tracker.BuildResultPage("page-batch", 2, 2)
	if err != nil {
		t.Fatalf("build result page: %v", err)
	}
	if totalSuccess != 5 || totalFailed != 0 || result.Success != 5 {
		t.Fatalf("unexpected result totals: success=%d failed=%d metadata=(%d,%d)", result.Success, result.Failed, totalSuccess, totalFailed)
	}
	if len(result.SuccessItems) != 2 || result.SuccessItems[0].Key != "item-2" || result.SuccessItems[1].Key != "item-3" {
		t.Fatalf("unexpected page items: %#v", result.SuccessItems)
	}
	if result.Processing != 5 || len(result.ProcessingItems) != 5 {
		t.Fatalf("pending items changed: processing=%d items=%#v", result.Processing, result.ProcessingItems)
	}
}

func TestBatchListUsesStoredResultCounts(t *testing.T) {
	rdb := newOptimizationTestRedis(t)
	tracker := NewRedisBatchTracker(
		rdb,
		BatchOptions{StoreSuccessItems: true, StoreFailedItems: true, MaxStoredSuccesses: 10, MaxStoredErrors: 10},
		DefaultTrackerConfig(),
	)
	if err := tracker.RegisterBatch("summary-batch", 3); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SaveResult("summary-batch", BatchItemResult{Key: "ok", Success: true}); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SaveResult("summary-batch", BatchItemResult{Key: "bad", Success: false}); err != nil {
		t.Fatal(err)
	}

	batches, err := tracker.ListBatches()
	if err != nil || len(batches) != 1 {
		t.Fatalf("list batches=%#v err=%v", batches, err)
	}
	if batches[0].Success != 1 || batches[0].Failed != 1 || batches[0].Done != 2 || batches[0].Processing != 1 {
		t.Fatalf("unexpected batch summary: %#v", batches[0])
	}

	if err := tracker.LinkBatchToJob("summary-job", "summary-batch"); err != nil {
		t.Fatal(err)
	}
	byJob, err := tracker.LoadBatchesByJob("summary-job")
	if err != nil || len(byJob) != 1 || byJob[0].Success != 1 || byJob[0].Failed != 1 {
		t.Fatalf("job batches=%#v err=%v", byJob, err)
	}
}

func TestTrackPendingPayloadAndLinkJob(t *testing.T) {
	rdb := newOptimizationTestRedis(t)
	tracker := NewRedisBatchTracker(rdb, BatchOptions{}, DefaultTrackerConfig())
	payload := map[string]string{"email": "user@example.com"}
	if err := tracker.trackPendingPayloadAndLinkJob("batch-1", "item-1", "job-1", payload); err != nil {
		t.Fatalf("track pending payload: %v", err)
	}

	if _, err := rdb.HGet(context.Background(), tracker.processingDataKey("batch-1"), "item-1").Result(); err != nil {
		t.Fatalf("pending payload was not stored: %v", err)
	}
	if _, err := rdb.HGet(context.Background(), tracker.processingStatusKey("batch-1"), "item-1").Result(); err != nil {
		t.Fatalf("pending status was not stored: %v", err)
	}
	if count, err := rdb.SIsMember(context.Background(), tracker.jobBatchesKey("job-1"), "batch-1").Result(); err != nil || !count {
		t.Fatalf("job batch link missing: member=%v err=%v", count, err)
	}
}
