# Go Batch & Bulk Worker Framework

[![Go Version](https://img.shields.io/badge/Go-%3E%3D1.18-blue)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A robust, enterprise-grade Go library designed for high-throughput batch logging and granular bulk task processing. Backed by Redis for state management, automatic retries, resource limiting, and an out-of-the-box web monitoring dashboard.

---

## 🚀 Key Features

* **Dual Worker Architecture**:
  * **`BatchWorker`**: Optimized for high-throughput, buffer-and-flush operations (ideal for audit logs, device telemetry, and event tracking).
  * **`BulkWorker`**: Event-driven, item-by-item processing with granular audit trails, individual success/failure tracking, and webhook completion notifications.
* **Resilient State Management**: Integrated with Redis for tracking batch progress, storing failed jobs, and supporting automatic retries.
* **Resource Limiting**: Built-in concurrency and database connection limiters using weighted semaphores to protect your database under heavy loads.
* **Built-in Monitoring Dashboard**: Ready-to-use HTML Web UI and JSON REST APIs with server-side pagination for tracking active and historical batches.
* **Graceful Shutdown**: Clean lifecycle management using a centralized manager to ensure no jobs are dropped during deployment or scaling.

---

## 📦 Project Structure

```text
go-batch-worker/
├── example/
│   └── main.go               # End-to-end integration example
├── .gitignore
├── LICENSE
├── README.md
├── go.mod
├── go.sum
├── batch_worker.go           # High-throughput buffer/flush worker
├── bulk_worker.go            # Granular item processor worker
├── manager.go                # Centralized worker lifecycle manager
├── batch_tracker.go          # Redis-backed state & error tracker
├── dashboard.go              # Built-in HTML Web UI & JSON REST API
└── ...                       # Other supporting utilities (retry, webhook, metrics)

```

---

## 🛠️ Installation

```bash
go get github.com/arsa-putra/go-batch-worker

```

---

## 💡 Quick Start Example

Here is a complete example of how to initialize the framework, register both workers, start the worker manager, and expose the monitoring dashboard using `gorilla/mux`.

```go
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

	"github.com/arsa-putra/go-batch-worker"
	"github.com/gorilla/mux"
	redis "gopkg.in/redis.v5"
)

// 1. Batch Worker Processor Example (High-throughput logging)
type DeviceLog struct {
	ResponseID int64
	SessionID  string
	CreatedAt  time.Time
}

type DeviceLogProcessor struct{}

func (p *DeviceLogProcessor) Name() string { return "device-logger-worker" }
func (p *DeviceLogProcessor) Process(ctx context.Context, items []DeviceLog) error {
	fmt.Printf("[BatchWorker] Flushed %d items to database\n", len(items))
	return nil
}

// 2. Bulk Worker Processor Example (Granular task tracking)
type TransferPayload struct {
	BatchID string `json:"-"`
	Email   string `json:"email"`
}

func (p TransferPayload) GetBatchID() string { return p.BatchID }
func (p TransferPayload) GetItemID() string  { return p.Email }
func (p TransferPayload) SuccessResult() worker.BatchItemResult {
	return worker.BatchItemResult{Key: p.Email, Success: true, Message: "transferred"}
}
func (p TransferPayload) FailedResult(err error) worker.BatchItemResult {
	return worker.BatchItemResult{Key: p.Email, Success: false, Message: err.Error()}
}

type TransferProcessor struct {
	Tracker worker.BatchTracker
}

func (p *TransferProcessor) Name() string { return "transfer_user_worker" }
func (p *TransferProcessor) Process(ctx context.Context, job TransferPayload) error {
	// Business logic here...
	return nil
}

func main() {
	// Initialize Redis Client
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	// Setup Trackers
	tracker := worker.NewRedisBatchTracker(rdb, worker.DefaultBatchOptions(), worker.DefaultTrackerConfig())
	detailTracker := worker.NewRedisBatchTracker(rdb, worker.BatchOptions{
		StoreSuccessItems: true,
		StoreFailedItems:  true,
	}, worker.DefaultTrackerConfig())

	// Setup Workers
	batchWorker := worker.NewBatchWorker(rdb, 3, 10000, 100, 2*time.Second, &DeviceLogProcessor{}, nil)
	bulkWorker := worker.NewBulkWorker(rdb, 5, 1000, &TransferProcessor{Tracker: detailTracker})
	bulkWorker.SetTracker(detailTracker)

	// Setup Manager
	manager := worker.NewManager(batchWorker, bulkWorker)
	manager.SetDBLimiter(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Workers
	go manager.Start(ctx)

	// Setup Router & Dashboard
	r := mux.NewRouter()
	r.HandleFunc("/batches", func(w http.ResponseWriter, r *http.Request) {
		worker.RenderDashboardList(w, detailTracker)
	}).Methods("GET")

	server := &http.Server{Addr: ":8080", Handler: r}
	go func() {
		fmt.Println("Dashboard running at http://localhost:8080/batches")
		server.ListenAndServe()
	}()

	// Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	server.Shutdown(shutdownCtx)
	manager.Shutdown(shutdownCtx)
	fmt.Println("Server stopped gracefully.")
}

```

---

## 📈 Monitoring Dashboard

The library provides built-in endpoints for visual and programmatic monitoring:

* **HTML Web UI**: `GET /batches` & `GET /batches/{id}/detail` (Interactive UI with live search and pagination).
* **JSON REST API**:
* List Batches: `GET /api/batches?page=1&limit=20`
* Batch Detail: `GET /api/batches/{id}/detail?page=1&limit=20`



---

## 📄 License

Distributed under the [MIT License](LICENSE).
