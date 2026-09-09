package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	worker "github.com/arsa-putra/go-batch-worker"
	"github.com/gorilla/mux"
	redis "gopkg.in/redis.v5"
)

// ==========================================
// 1. SAMPLE PROCESSOR FOR BATCH WORKER
// ==========================================
type DeviceLog struct {
	ResponseID int64
	SessionID  string
	DeviceID   string
	MetaHash   uint64
	IP         string
	UserAgent  string
	CreatedAt  time.Time
}

type DeviceLogProcessor struct{}

func (p *DeviceLogProcessor) Name() string {
	return "device-logger-worker"
}

func (p *DeviceLogProcessor) Process(ctx context.Context, items []DeviceLog) error {
	if len(items) == 0 {
		return nil
	}
	fmt.Printf("[BatchWorker] Flushed %d device logs to database\n", len(items))
	return nil
}

// ==========================================
// 2. SAMPLE PROCESSOR FOR BULK WORKER
// ==========================================
type TransferUserPayload struct {
	BatchID      string          `json:"-"`
	Email        string          `json:"email"`
	TargetPlan   string          `json:"target_plan"`
	TargetUIC    string          `json:"target_uic"`
	TargetTestID map[int64]int64 `json:"target_test_id"`
	AdminUIC     string          `json:"admin_uic"`
}

func (p TransferUserPayload) GetBatchID() string { return p.BatchID }
func (p TransferUserPayload) GetItemID() string  { return p.Email }
func (p TransferUserPayload) SuccessResult() worker.BatchItemResult {
	return worker.BatchItemResult{
		Key:     p.Email,
		Success: true,
		Message: "transferred",
		Data: map[string]interface{}{
			"email":       p.Email,
			"target_plan": p.TargetPlan,
			"target_uic":  p.TargetUIC,
		},
	}
}
func (p TransferUserPayload) FailedResult(err error) worker.BatchItemResult {
	return worker.BatchItemResult{
		Key:     p.Email,
		Success: false,
		Message: err.Error(),
		Data: map[string]interface{}{
			"email":       p.Email,
			"target_plan": p.TargetPlan,
			"target_uic":  p.TargetUIC,
		},
	}
}

type TransferUserProcessor struct {
	Tracker worker.BatchTracker
}

func (p *TransferUserProcessor) Name() string {
	return "transfer_user"
}

func (p *TransferUserProcessor) Process(ctx context.Context, job TransferUserPayload) error {
	// Add artificial delay so processing state can be observed on dashboard
	time.Sleep(10 * time.Second)

	if job.Email == "error@example.com" {
		if p.Tracker != nil {
			_ = p.Tracker.CompleteFailed(job.BatchID, worker.BatchItemResult{
				Key:     job.Email,
				Success: false,
				Message: "The transfer can only be done to an organization within the same country or below.",
				Data:    job,
			})
		}
		return nil
	}

	fmt.Printf("[BulkWorker] Successfully transferred user: %s\n", job.Email)
	return nil
}

// ==========================================
// MAIN APPLICATION ENTRYPOINT
// ==========================================
func main() {
	// 1. Initialize Redis Client (v5)
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	if _, err := rdb.Ping().Result(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	fmt.Println("Connected to Redis successfully!")

	// --- AUTO FLUSH FOR CLEAN TESTING ---
	if err := rdb.FlushAll().Err(); err != nil {
		log.Fatalf("Failed to flush Redis: %v", err)
	}
	fmt.Println("🧹 Redis flushed successfully for a fresh test run!")

	// 2. Setup Trackers
	tracker := worker.NewRedisBatchTracker(
		rdb,
		worker.DefaultBatchOptions(),
		worker.DefaultTrackerConfig(),
	)

	detailTracker := worker.NewRedisBatchTracker(
		rdb,
		worker.BatchOptions{
			StoreSuccessItems:  true,
			StoreFailedItems:   true,
			MaxStoredSuccesses: 5000,
			MaxStoredErrors:    5000,
		},
		worker.DefaultTrackerConfig(),
	)

	// 3. Setup Subsystems
	completion := worker.NewWebhookCompletion(tracker)
	autoRetry := worker.NewAutoRetryWorker(rdb, time.Minute)

	// 4. Initialize BatchWorker
	deviceLogBatch := worker.NewBatchWorker(
		rdb,
		1,
		30000,
		200,
		2*time.Second,
		&DeviceLogProcessor{},
		nil,
	)

	// 5. Initialize BulkWorker (Set concurrency to 1 so items stay in processing state longer for demo)
	transferUser := worker.NewBulkWorker(
		rdb,
		1,
		1000,
		&TransferUserProcessor{Tracker: detailTracker},
	)
	transferUser.SetTracker(detailTracker)
	transferUser.SetCompletionHandler(completion)
	autoRetry.RegisterBulk(transferUser)

	// 6. Setup Worker Manager
	manager := worker.NewManager(
		autoRetry,
		deviceLogBatch,
		transferUser,
	)
	manager.SetDBLimiter(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start manager
	go func() {
		fmt.Println("🚀 Starting worker manager...")
		manager.Start(ctx)
	}()

	// ==========================================
	// SIMULATION: Testing End-to-End Workflow
	// ==========================================
	go func() {
		time.Sleep(2 * time.Second)

		// A. Test BatchWorker Push
		fmt.Println("[Simulation] Pushing logs to BatchWorker...")
		deviceLogBatch.Push(DeviceLog{
			ResponseID: 101,
			SessionID:  "sess-abc-123",
			DeviceID:   "dev-01",
			MetaHash:   123456789,
			IP:         "127.0.0.1",
			UserAgent:  "Mozilla/5.0",
			CreatedAt:  time.Now(),
		})

		// B. Test BulkWorker Submit (Larger batch to keep processing visible)
		batchID := "demo-batch-transfer-001"
		emails := []string{
			"user1@example.com",
			"user2@example.com",
			"user3@example.com",
			"user4@example.com",
			"user5@example.com",
			"user6@example.com",
			"error@example.com",
		}

		_ = detailTracker.RegisterBatch(batchID, len(emails))
		_ = detailTracker.SaveMetadata(batchID, &worker.BatchMetadata{
			CallbackURL: "https://webhook.site/example-callback",
		})

		fmt.Println("[Simulation] Submitting tasks to BulkWorker...")
		for _, email := range emails {
			_ = transferUser.Submit(TransferUserPayload{
				BatchID:      batchID,
				Email:        email,
				TargetPlan:   "enterprise",
				TargetUIC:    "UIC-999",
				TargetTestID: map[int64]int64{1: 101, 2: 202},
				AdminUIC:     "UIC-001",
			})
		}
	}()

	// 7. Setup HTTP Router for Dashboard & API Monitoring
	r := mux.NewRouter()

	r.HandleFunc("/batches", func(w http.ResponseWriter, r *http.Request) {
		worker.RenderDashboardList(w, detailTracker)
	}).Methods("GET")

	r.HandleFunc("/batches/{id}/detail", func(w http.ResponseWriter, r *http.Request) {
		batchID := mux.Vars(r)["id"]
		worker.RenderDashboardDetail(w, detailTracker, batchID)
	}).Methods("GET")

	r.HandleFunc("/api/batches", func(w http.ResponseWriter, r *http.Request) {
		worker.ServeDashboardListJSON(w, r, detailTracker)
	}).Methods("GET")

	r.HandleFunc("/api/batches/{id}/detail", func(w http.ResponseWriter, r *http.Request) {
		batchID := mux.Vars(r)["id"]
		worker.ServeDashboardDetailJSON(w, r, detailTracker, batchID)
	}).Methods("GET")

	server := &http.Server{
		Addr:    ":8081", // Fixed port matching port-printing info below
		Handler: r,
	}

	go func() {
		fmt.Println("🌐 Dashboard server running at http://localhost:8081/batches")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// 8. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("\nShutting down gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	server.Shutdown(shutdownCtx)
	manager.Shutdown(shutdownCtx)
	fmt.Println("Server stopped successfully.")
}
