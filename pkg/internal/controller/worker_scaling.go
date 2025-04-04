/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// WorkerScaler defines the interface that users can implement for worker count adjustment
type WorkerScaler interface {
	// EvaluateWorkerCount decides the worker count based on current situation
	// Returns a new target worker count
	EvaluateWorkerCount(ctx context.Context, info WorkerScalingInfo) (int, error)
}

// WorkerScalingInfo contains information needed for adjusting worker count
type WorkerScalingInfo struct {
	// Current active worker count
	CurrentWorkers int
	// Minimum worker count limit
	MinWorkers int
	// Maximum worker count limit
	MaxWorkers int
	// Current queue length
	QueueLength int
	// Controller name
	ControllerName string
	// Worker count from previous scaling operation
	PreviousWorkerCount int
}

// WorkerAction represents possible worker operation types
type WorkerAction int

const (
	// WorkerStart indicates starting a new worker
	WorkerStart WorkerAction = iota
	// WorkerStop indicates a request to stop a worker
	WorkerStop
	// WorkerFinished indicates a worker has completed and exited
	WorkerFinished
)

// String returns the string representation of WorkerAction, used for logging
func (wa WorkerAction) String() string {
	switch wa {
	case WorkerStart:
		return "start"
	case WorkerStop:
		return "stop"
	case WorkerFinished:
		return "finished"
	default:
		return "unknown"
	}
}

// workerSignal represents a worker lifecycle signal
type workerSignal struct {
	id     uint64
	action WorkerAction
}

// stats structure stores worker scaling related statistics
type stats struct {
	// Last scaling time
	lastScalingTime time.Time
	// Worker count from previous scaling operation
	previousWorkerCount int
}

// WorkerScalingContext encapsulates all worker scaling related fields and logic
type WorkerScalingContext struct {
	// Worker scaler implementation
	scaler WorkerScaler

	// Worker count limits
	minWorkers int
	maxWorkers int

	// Current active worker count
	currentWorkers int32

	// Worker management related
	workerSignalChan chan workerSignal
	workerStopChan   chan uint64
	activeWorkers    map[uint64]struct{}
	nextWorkerID     uint64

	// Mutex to protect worker scaling related operations
	workerMu sync.Mutex

	// Function reference to process the next work item
	processNextWorkItemFunc func(ctx context.Context) bool

	// Controller name, used for logging
	controllerName string

	// Performance and load statistics
	stats stats
}

// Initialize internal statistics
func (ws *WorkerScalingContext) initStats() {
	ws.stats = stats{
		lastScalingTime:     time.Now(),
		previousWorkerCount: ws.minWorkers,
	}
}

// Initialize worker management related fields and functionality
func (c *Controller[request]) initWorkerManagement() {
	if c.scaling != nil {
		return // Already initialized
	}

	// Create and initialize WorkerScalingContext
	c.scaling = &WorkerScalingContext{
		minWorkers:              1,
		maxWorkers:              c.MaxConcurrentReconciles,
		workerSignalChan:        make(chan workerSignal, 10),
		workerStopChan:          make(chan uint64, c.MaxConcurrentReconciles),
		activeWorkers:           make(map[uint64]struct{}),
		currentWorkers:          int32(c.MaxConcurrentReconciles),
		processNextWorkItemFunc: c.processNextWorkItem,
		controllerName:          c.Name,
	}

	// Initialize statistics
	c.scaling.initStats()
}

// Start worker management goroutine
func (c *Controller[request]) startWorkerManager(ctx context.Context, wg *sync.WaitGroup) {
	c.initWorkerManagement()

	// Start worker manager
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case signal := <-c.scaling.workerSignalChan:
				c.scaling.handleWorkerSignal(ctx, wg, signal)
			}
		}
	}()

	// Start initial number of workers
	c.LogConstructor(nil).Info("Starting workers", "worker count", c.MaxConcurrentReconciles)
	for i := 0; i < c.MaxConcurrentReconciles; i++ {
		c.scaling.workerSignalChan <- workerSignal{id: c.scaling.nextWorkerID, action: WorkerStart}
		c.scaling.nextWorkerID++
	}
}

// handleWorkerSignal processes worker signals
func (ws *WorkerScalingContext) handleWorkerSignal(ctx context.Context, wg *sync.WaitGroup, signal workerSignal) {
	ws.workerMu.Lock()
	defer ws.workerMu.Unlock()

	// Get logger from context to preserve context information
	log := log.FromContext(ctx)

	switch signal.action {
	case WorkerStart:
		ws.activeWorkers[signal.id] = struct{}{}
		go ws.runWorker(ctx, wg, signal.id)
		atomic.AddInt32(&ws.currentWorkers, 1)
		log.V(1).Info("Worker started", "worker_id", signal.id, "current_workers", ws.currentWorkers)
	case WorkerStop:
		// Request to stop a worker
		// Worker will send WorkerFinished signal after completing the current task
		if _, exists := ws.activeWorkers[signal.id]; exists {
			ws.workerStopChan <- signal.id
			log.V(1).Info("Worker stop requested", "worker_id", signal.id)
		}
	case WorkerFinished:
		delete(ws.activeWorkers, signal.id)
		atomic.AddInt32(&ws.currentWorkers, -1)
		log.V(1).Info("Worker finished", "worker_id", signal.id, "current_workers", ws.currentWorkers)
	}
}

// runWorker runs a single worker
func (ws *WorkerScalingContext) runWorker(ctx context.Context, wg *sync.WaitGroup, id uint64) {
	wg.Add(1)
	defer func() {
		wg.Done()
		// Notify that worker has completed
		ws.workerSignalChan <- workerSignal{id: id, action: WorkerFinished}
	}()

	stopCh := make(chan struct{})

	// Listen for stop signal
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(stopCh)
				return
			case workerID := <-ws.workerStopChan:
				if workerID == id {
					close(stopCh)
					return
				}
			}
		}
	}()

	// Process queue items until stop signal is received
	for {
		select {
		case <-stopCh:
			return
		default:
			if !ws.processNextWorkItemFunc(ctx) {
				return
			}
		}
	}
}

// SetWorkerScaler sets the scaler used for dynamically adjusting worker count
func (c *Controller[request]) SetWorkerScaler(scaler WorkerScaler, minWorkers, maxWorkers int) {
	c.initWorkerManagement()

	c.scaling.workerMu.Lock()
	defer c.scaling.workerMu.Unlock()

	c.scaling.scaler = scaler

	// Set minimum and maximum worker counts
	if minWorkers > 0 {
		c.scaling.minWorkers = minWorkers
	} else {
		c.scaling.minWorkers = 1 // Need at least 1 worker
	}

	if maxWorkers > c.scaling.minWorkers {
		c.scaling.maxWorkers = maxWorkers
	} else {
		c.scaling.maxWorkers = c.scaling.minWorkers
	}
}

// AdjustWorkers adjusts worker count based on current situation
func (c *Controller[request]) AdjustWorkers(ctx context.Context) error {
	if c.scaling == nil || c.scaling.scaler == nil {
		return fmt.Errorf("no WorkerScaler configured")
	}

	queueLen := c.Queue.Len()
	now := time.Now()

	// Get current worker count and build scaling info
	c.scaling.workerMu.Lock()
	currentWorkers := int(atomic.LoadInt32(&c.scaling.currentWorkers))

	// Build scaling info
	info := WorkerScalingInfo{
		CurrentWorkers:      currentWorkers,
		MinWorkers:          c.scaling.minWorkers,
		MaxWorkers:          c.scaling.maxWorkers,
		QueueLength:         queueLen,
		ControllerName:      c.Name,
		PreviousWorkerCount: c.scaling.stats.previousWorkerCount,
	}
	c.scaling.workerMu.Unlock()

	// Call user-provided scaler to evaluate worker count
	targetWorkers, err := c.scaling.scaler.EvaluateWorkerCount(ctx, info)
	if err != nil {
		return err
	}

	// Ensure target worker count is within valid range
	if targetWorkers < c.scaling.minWorkers {
		targetWorkers = c.scaling.minWorkers
	}
	if targetWorkers > c.scaling.maxWorkers {
		targetWorkers = c.scaling.maxWorkers
	}

	// If worker count needs adjustment
	if targetWorkers != currentWorkers {
		// Update scaling status
		c.scaling.workerMu.Lock()
		c.scaling.stats.previousWorkerCount = currentWorkers
		c.scaling.stats.lastScalingTime = now
		c.scaling.workerMu.Unlock()

		// Execute scaling
		c.LogConstructor(nil).Info("Adjusting worker count",
			"controller", c.Name,
			"current", currentWorkers,
			"target", targetWorkers,
			"queue_length", queueLen)

		return c.scaling.setWorkerCount(ctx, targetWorkers)
	}

	return nil
}

// setWorkerCount sets worker count
func (ws *WorkerScalingContext) setWorkerCount(ctx context.Context, count int) error {
	ws.workerMu.Lock()
	defer ws.workerMu.Unlock()

	log := log.FromContext(ctx)
	current := int(atomic.LoadInt32(&ws.currentWorkers))

	log.V(1).Info("Adjusting worker count", "current", current, "target", count)

	// Increase workers
	if count > current {
		for i := 0; i < count-current; i++ {
			workerID := ws.nextWorkerID
			ws.nextWorkerID++
			ws.workerSignalChan <- workerSignal{id: workerID, action: WorkerStart}
		}
	}

	// Decrease workers
	if count < current {
		// Find workers that can be stopped
		var workersToStop []uint64
		for id := range ws.activeWorkers {
			if len(workersToStop) < current-count {
				workersToStop = append(workersToStop, id)
			} else {
				break
			}
		}

		// Send stop signals
		for _, id := range workersToStop {
			ws.workerSignalChan <- workerSignal{id: id, action: WorkerStop}
		}
	}

	return nil
}

// GetCurrentWorkerCount returns the current worker count
func (c *Controller[request]) GetCurrentWorkerCount() int {
	if c.scaling == nil {
		return c.MaxConcurrentReconciles
	}
	return int(atomic.LoadInt32(&c.scaling.currentWorkers))
}
