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

package controller_test

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	internalcontroller "sigs.k8s.io/controller-runtime/pkg/internal/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestQueueWorkerScaler is a worker scaler used for testing
type TestQueueWorkerScaler struct {
	itemsPerWorker int
}

// EvaluateWorkerCount implements the WorkerScaler interface
func (s *TestQueueWorkerScaler) EvaluateWorkerCount(ctx context.Context, info controller.WorkerScalingInfo) (int, error) {
	// Simple algorithm: each worker handles a certain number of queue items
	itemsPerWorker := s.itemsPerWorker
	if itemsPerWorker <= 0 {
		itemsPerWorker = 5 // Default: each worker processes 5 items
	}

	// Calculate required worker count
	desiredWorkers := (info.QueueLength + itemsPerWorker - 1) / itemsPerWorker

	// Ensure within minimum and maximum range
	if desiredWorkers < info.MinWorkers {
		desiredWorkers = info.MinWorkers
	}
	if desiredWorkers > info.MaxWorkers {
		desiredWorkers = info.MaxWorkers
	}

	return desiredWorkers, nil
}

func TestWorkerScaling(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Worker Scaling Suite")
}

var _ = Describe("Worker Scaling", func() {
	var (
		wg   sync.WaitGroup
		ctx  context.Context
		ctrl controller.Controller
		sc   controller.ScalableController[reconcile.Request]
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())

		// Create a test controller
		var err error
		ctrl, err = controller.NewUnmanaged("test-worker-scaling", controller.Options{
			Reconciler: reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
				defer wg.Done()
				// Simulate workload
				time.Sleep(20 * time.Millisecond)
				return reconcile.Result{}, nil
			}),
			MaxConcurrentReconciles:      2, // Initially set to 2 workers
			WorkerScaler:                 &TestQueueWorkerScaler{itemsPerWorker: 5},
			MinConcurrentReconciles:      1,
			MaxConcurrentReconcilesLimit: 10,
		})
		Expect(err).NotTo(HaveOccurred())
		var ok bool
		sc, ok = controller.AsScalableController(ctrl)
		Expect(ok).To(BeTrue())

		// Start controller
		go func() {
			defer GinkgoRecover()
			Expect(ctrl.Start(ctx)).To(Succeed())
		}()

		// Cleanup function
		DeferCleanup(func() {
			cancel()
		})

		// Wait for controller initialization
		time.Sleep(100 * time.Millisecond)
	})

	Context("Dynamic worker scaling", func() {
		It("should scale up workers when queue has many items", func() {
			reqCount := 15
			wg.Add(reqCount)

			// Create 15 requests
			for i := 0; i < reqCount; i++ {
				req := reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: "default",
						Name:      "test-obj-" + string(rune(i)),
					},
				}
				_, err := ctrl.(reconcile.Reconciler).Reconcile(ctx, req)
				Expect(err).NotTo(HaveOccurred())
			}

			// Adjust worker count
			Expect(sc.AdjustWorkers(ctx)).To(Succeed())

			// Wait for worker adjustment to take effect
			time.Sleep(100 * time.Millisecond)

			// Check if worker count has increased
			internalCtrl, isInternal := ctrl.(*internalcontroller.Controller[reconcile.Request])
			if isInternal {
				Expect(internalCtrl.GetCurrentWorkerCount()).To(BeNumerically(">", 2))
			}

			// Wait for all reconcile requests to complete
			wg.Wait()
			time.Sleep(100 * time.Millisecond)

			// Clear queue
			for i := 0; i < reqCount; i++ {
				if isInternal {
					internalCtrl.Queue.Done(reconcile.Request{})
				}
			}
		})

		It("should scale down workers when queue is empty", func() {
			// First scale up
			reqCount := 15
			wg.Add(reqCount)

			// Create 15 requests
			for i := 0; i < reqCount; i++ {
				req := reconcile.Request{
					NamespacedName: types.NamespacedName{
						Namespace: "default",
						Name:      "test-obj-" + string(rune(i)),
					},
				}
				_, err := ctrl.(reconcile.Reconciler).Reconcile(ctx, req)
				Expect(err).NotTo(HaveOccurred())
			}

			// Adjust worker count
			Expect(sc.AdjustWorkers(ctx)).To(Succeed())
			time.Sleep(100 * time.Millisecond)

			// Wait for all reconcile requests to complete
			wg.Wait()
			time.Sleep(100 * time.Millisecond)

			// Clear queue
			internalCtrl, isInternal := ctrl.(*internalcontroller.Controller[reconcile.Request])
			for i := 0; i < reqCount; i++ {
				if isInternal {
					internalCtrl.Queue.Done(reconcile.Request{})
				}
			}

			// Now test scale down: queue is empty, should reduce to minimum worker count
			// Adjust worker count
			Expect(sc.AdjustWorkers(ctx)).To(Succeed())

			// Wait for worker adjustment to take effect (scale down takes longer because it waits for existing work to complete)
			time.Sleep(300 * time.Millisecond)

			// Check if worker count has decreased
			if isInternal {
				// Should reduce to near minimum value
				Expect(internalCtrl.GetCurrentWorkerCount()).To(BeNumerically("<=", 3))
			}
		})
	})
})
