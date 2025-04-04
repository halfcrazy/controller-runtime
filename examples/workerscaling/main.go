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

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	ctrlEvent "sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var (
	setupLog = ctrl.Log.WithName("setup")
)

// PodReconciler reconciles a Pod object
type PodReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	podCount int32 // Track the number of processed pods
}

// Reconcile implements the reconcile.Reconciler interface
func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Simulate processing time
	time.Sleep(time.Duration(rand.Intn(50)+10) * time.Millisecond)

	// Get Pod
	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		if errors.IsNotFound(err) {
			// Object not found, likely deleted
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get Pod")
		return ctrl.Result{}, err
	}

	// Simple business logic: record Pod status
	log.Info("Reconciling Pod", "phase", pod.Status.Phase)

	// Increment processed Pod count
	atomic.AddInt32(&r.podCount, 1)

	return ctrl.Result{}, nil
}

func main() {
	// Set up logging
	log.SetLogger(zap.New())

	// Get Kubernetes config
	cfg, err := config.GetConfig()
	if err != nil {
		setupLog.Error(err, "unable to get Kubernetes config")
		os.Exit(1)
	}

	// Create Manager
	mgr, err := manager.New(cfg, manager.Options{
		Scheme:                 scheme.Scheme,
		Metrics:                metricsserver.Options{BindAddress: ":8080"},
		HealthProbeBindAddress: ":8081",
	})
	if err != nil {
		setupLog.Error(err, "unable to set up controller manager")
		os.Exit(1)
	}

	// Add health checks
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	// Create Pod Reconciler
	reconciler := &PodReconciler{
		Client: mgr.GetClient(),
		Scheme: scheme.Scheme,
	}

	// Create custom scaler
	scaler := &QueueBasedScaler{
		TargetItemsPerWorker: 5,   // Each worker processes 5 queue items
		MaxGrowthRatio:       0.5, // Maximum increase of 50%
		MaxReductionRatio:    0.2, // Maximum decrease of 20%
		Log:                  setupLog.Info,
	}

	// Create controller and configure worker scaling
	c, err := controller.New("pod-controller", mgr, controller.Options{
		Reconciler:                   reconciler,
		MaxConcurrentReconciles:      2,      // Initial worker count
		WorkerScaler:                 scaler, // Custom scaler
		MinConcurrentReconciles:      1,      // Minimum 1 worker
		MaxConcurrentReconcilesLimit: 10,     // Maximum 10 workers
	})
	if err != nil {
		setupLog.Error(err, "unable to create controller")
		os.Exit(1)
	}

	sc, ok := controller.AsScalableController(c)
	if !ok {
		setupLog.Info("unable to create scalable controller")
		os.Exit(1)
	}

	// Create a channel with the correct event type
	eventChan := make(chan ctrlEvent.GenericEvent)

	// Use function call syntax instead of struct initialization
	if err := c.Watch(source.Channel(eventChan, &handler.EnqueueRequestForObject{})); err != nil {
		setupLog.Error(err, "unable to watch test channel")
		os.Exit(1)
	}

	// Start load injection
	go injectTestLoad(eventChan, reconciler)

	// Start a goroutine to periodically adjust worker count
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		ctx := context.Background()
		for {
			select {
			case <-ticker.C:
				// Every 5 seconds, evaluate and adjust worker count
				if err := sc.AdjustWorkers(ctx); err != nil {
					setupLog.Error(err, "Failed to adjust workers")
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start Manager (this will block)
	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}
}

// injectTestLoad uses source.Channel to inject test load
func injectTestLoad(eventChan chan ctrlEvent.GenericEvent, reconciler *PodReconciler) {
	// Wait for controller to start
	time.Sleep(2 * time.Second)
	setupLog.Info("Starting load injection")

	// Simulate high load - 20 requests
	setupLog.Info("Injecting high load (20 requests)")
	for i := 0; i < 20; i++ {
		pod := &corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Name:      fmt.Sprintf("test-pod-%d", i),
				Namespace: "default",
			},
		}
		eventChan <- ctrlEvent.GenericEvent{
			Object: pod,
		}
	}

	// Wait for processing
	time.Sleep(10 * time.Second)

	// Simulate medium load - 10 requests
	setupLog.Info("Injecting medium load (10 requests)")
	for i := 0; i < 10; i++ {
		pod := &corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Name:      fmt.Sprintf("test-pod-medium-%d", i),
				Namespace: "default",
			},
		}
		eventChan <- ctrlEvent.GenericEvent{
			Object: pod,
		}
	}

	// Wait for processing
	time.Sleep(10 * time.Second)

	// Low load period - no new requests
	setupLog.Info("No load period")
	time.Sleep(10 * time.Second)

	// Simulate pulse load - inject batches every few seconds
	setupLog.Info("Starting pulse load pattern")
	for j := 0; j < 3; j++ {
		// Inject 5 requests
		for i := 0; i < 5; i++ {
			pod := &corev1.Pod{
				ObjectMeta: v1.ObjectMeta{
					Name:      fmt.Sprintf("test-pod-pulse-%d-%d", j, i),
					Namespace: "default",
				},
			}
			eventChan <- ctrlEvent.GenericEvent{
				Object: pod,
			}
		}

		// Wait for a while
		time.Sleep(5 * time.Second)
	}

	setupLog.Info("Total pods processed", "count", reconciler.podCount)

	// Keep running to observe worker reduction back to minimum
	for {
		setupLog.Info("Load test completed, controller running idle", "processed_pods", reconciler.podCount)
		time.Sleep(30 * time.Second)
	}
}
