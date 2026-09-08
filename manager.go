package worker

import (
	"context"
	"log"

	"golang.org/x/sync/semaphore"
)

type Runner interface {
	Name() string
	Start(ctx context.Context)
	Close()
	Wait()
}

type Dashboardable interface {
	Dashboard() map[string]interface{}
}

type Manager struct {
	workers  []Runner
	registry map[string]Runner

	limiter *ResourceLimiter
}

func NewManager(workers ...Runner) *Manager {
	registry := make(map[string]Runner)

	for _, w := range workers {
		registry[w.Name()] = w
	}

	return &Manager{
		workers:  workers,
		registry: registry,
	}
}

func (m *Manager) Start(ctx context.Context) {
	for _, w := range m.workers {
		w.Start(ctx)
	}
}

func (m *Manager) Shutdown(ctx context.Context) {
	log.Printf("[WorkerManager] shutting down...")

	for _, w := range m.workers {
		w.Close()
	}

	done := make(chan struct{})

	go func() {
		for _, w := range m.workers {
			w.Wait()
		}

		close(done)
	}()

	select {
	case <-done:
		log.Printf("[WorkerManager] shutdown complete")

	case <-ctx.Done():
		log.Printf("[WorkerManager] shutdown timeout")
	}
}

func (m *Manager) Dashboard() map[string]interface{} {
	result := map[string]interface{}{}

	for _, w := range m.workers {
		if d, ok := w.(Dashboardable); ok {
			result[w.Name()] =
				d.Dashboard()
		}
	}

	return result
}

func (m *Manager) RegisteredWorkers() []string {
	names := make([]string, 0, len(m.registry))

	for name := range m.registry {
		names = append(names, name)
	}

	return names
}

func (m *Manager) Get(name string) (Runner, bool) {
	w, ok := m.registry[name]
	return w, ok
}

func (m *Manager) SetDBLimiter(maxConcurrent int64) {

	if m.limiter == nil {
		m.limiter = &ResourceLimiter{}
	}

	m.limiter.DB = semaphore.NewWeighted(maxConcurrent)

	for _, worker := range m.workers {
		if aware, ok := worker.(ResourceAware); ok {
			aware.SetResourceLimiter(m.limiter)
		}
	}
}
