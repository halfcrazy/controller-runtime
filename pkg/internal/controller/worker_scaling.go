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
	//activeWorkers    map[uint64]struct{}
	activeWorkers map[uint64]chan struct{}
	nextWorkerID  uint64

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
		minWorkers:       1,
		maxWorkers:       c.MaxConcurrentReconciles,
		workerSignalChan: make(chan workerSignal, 10),
		workerStopChan:   make(chan uint64, c.MaxConcurrentReconciles),
		//activeWorkers:           make(map[uint64]struct{}),
		activeWorkers:           make(map[uint64]chan struct{}),
		currentWorkers:          0,
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

func (ws *WorkerScalingContext) handleWorkerSignal(ctx context.Context, wg *sync.WaitGroup, signal workerSignal) {
	logger := log.FromContext(ctx) // Get logger outside lock initially if needed

	switch signal.action {
	case WorkerStart:
		ws.workerMu.Lock() // Lock for map modification
		logger.V(1).Info("Processing WorkerStart signal", "worker_id", signal.id)
		if _, exists := ws.activeWorkers[signal.id]; exists {
			logger.Info("Attempted to start an already active worker", "worker_id", signal.id)
			ws.workerMu.Unlock()
			return
		}
		stopCh := make(chan struct{})
		ws.activeWorkers[signal.id] = stopCh
		ws.workerMu.Unlock() // Unlock before starting goroutine

		go ws.runWorker(ctx, wg, signal.id, stopCh)

		newCount := atomic.AddInt32(&ws.currentWorkers, 1)
		logger.V(1).Info("Worker started", "worker_id", signal.id, "current_workers", newCount)

	case WorkerStop:
		ws.workerMu.Lock() // Lock for map modification
		logger.V(1).Info("Processing WorkerStop signal", "worker_id", signal.id)
		if stopCh, exists := ws.activeWorkers[signal.id]; exists {
			close(stopCh)
			delete(ws.activeWorkers, signal.id)
			logger.V(1).Info("Worker stop requested and signaled", "worker_id", signal.id)
		} else {
			logger.V(1).Info("Worker stop requested for inactive/unknown worker", "worker_id", signal.id)
		}
		ws.workerMu.Unlock() // Unlock after processing

	case WorkerFinished:
		logger.V(1).Info("Processing WorkerFinished signal", "worker_id", signal.id)
		// *** Move atomic decrement BEFORE the lock ***
		finalCount := atomic.AddInt32(&ws.currentWorkers, -1)
		logger.V(1).Info("Worker count decremented", "worker_id", signal.id, "current_workers", finalCount) // Log count immediately

		// Lock only for map cleanup
		ws.workerMu.Lock()
		if _, exists := ws.activeWorkers[signal.id]; exists {
			delete(ws.activeWorkers, signal.id)
			logger.V(2).Info("Removed finished worker from active map", "worker_id", signal.id)
		} else {
			// This is expected if the worker was explicitly stopped via WorkerStop before finishing
			logger.V(2).Info("Finished worker already removed or not found in active map", "worker_id", signal.id)
		}
		ws.workerMu.Unlock()
		// Log completion after potential map cleanup
		logger.V(1).Info("Worker finished signal fully processed", "worker_id", signal.id)

	default:
		// Unknown action, log error
		// No lock needed here as we aren't accessing shared state
		logger.Error(fmt.Errorf("unknown worker action"), "Received unknown worker signal", "action", signal.action)
	}
}

// runWorker runs a single worker
func (ws *WorkerScalingContext) runWorker(ctx context.Context, wg *sync.WaitGroup, id uint64, stopCh <-chan struct{}) {
	wg.Add(1)
	logger := log.FromContext(ctx)
	defer func() {
		wg.Done()
		// Send completion signal *unconditionally* on exit
		ws.workerSignalChan <- workerSignal{id: id, action: WorkerFinished}
		logger.V(1).Info("Worker routine finished", "worker_id", id)
	}()

	for {
		// Check stop signals first
		select {
		case <-ctx.Done(): // Check global context cancellation
			logger.V(1).Info("Worker stopping due to context cancellation", "worker_id", id)
			return
		case <-stopCh: // Check dedicated stop channel
			logger.V(1).Info("Worker stopping due to stop request", "worker_id", id)
			return
		default:
			// No stop signal, proceed
		}

		// Attempt to process the next work item
		logger.V(2).Info("Worker trying to process next item", "worker_id", id)
		if !ws.processNextWorkItemFunc(ctx) {
			// processNextWorkItemFunc returns false means queue is shutting down
			logger.V(1).Info("Worker stopping because queue is shutting down", "worker_id", id)
			return
		}
		// If processNextWorkItemFunc returns true, an item was processed
		logger.V(2).Info("Worker processed an item", "worker_id", id)
		// Loop continues, will check stop signals again first
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
	// Collect signals while holding the lock
	signalsToSend := []workerSignal{}
	ws.workerMu.Lock() // Lock earlier

	logger := log.FromContext(ctx)                       // Get logger within the lock scope
	current := int(atomic.LoadInt32(&ws.currentWorkers)) // Read current count safely under lock
	logger.V(1).Info("Planning worker count adjustment", "current", current, "target", count)

	// Increase workers
	if count > current {
		numToStart := count - current
		logger.V(1).Info("Planning to start workers", "count", numToStart)
		for i := 0; i < numToStart; i++ {
			workerID := ws.nextWorkerID
			ws.nextWorkerID++
			// Prepare start signals but don't send yet
			signalsToSend = append(signalsToSend, workerSignal{id: workerID, action: WorkerStart})
		}
	}

	// Decrease workers
	if count < current {
		numToStop := current - count
		logger.V(1).Info("Planning to stop workers", "count", numToStop)
		// Find workers that can be stopped
		var workersToStop []uint64
		// Iterate safely over activeWorkers map under lock
		for id := range ws.activeWorkers {
			if len(workersToStop) < numToStop {
				workersToStop = append(workersToStop, id)
			} else {
				break // Found enough workers to stop
			}
		}
		logger.V(1).Info("Identified workers to stop", "ids", workersToStop)
		// Prepare stop signals but don't send yet
		for _, id := range workersToStop {
			signalsToSend = append(signalsToSend, workerSignal{id: id, action: WorkerStop})
		}
	}

	ws.workerMu.Unlock() // *** Unlock BEFORE sending signals ***

	// Send collected signals without holding the lock
	if len(signalsToSend) > 0 {
		logger.V(1).Info("Sending worker signals", "count", len(signalsToSend))
		for _, signal := range signalsToSend {
			// Use a select with context cancellation for robust sending
			select {
			case ws.workerSignalChan <- signal:
				logger.V(2).Info("Sent worker signal", "id", signal.id, "action", signal.action)
			case <-ctx.Done():
				logger.Error(ctx.Err(), "Context cancelled while sending worker signal", "id", signal.id, "action", signal.action)
				// If context is cancelled during signal sending, the overall operation might be incomplete.
				// Returning error here is appropriate.
				return fmt.Errorf("failed to send worker signal due to context cancellation: %w", ctx.Err())
			}
		}
	} else {
		logger.V(1).Info("No worker count adjustment needed or no signals to send.")
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
