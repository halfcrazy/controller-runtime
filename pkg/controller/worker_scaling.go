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

	internalcontroller "sigs.k8s.io/controller-runtime/pkg/internal/controller"
)

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
}

// WorkerScaler defines the interface that users can implement for worker count adjustment
type WorkerScaler interface {
	// EvaluateWorkerCount determines the worker count based on current situation
	// Returns a new target worker count
	EvaluateWorkerCount(ctx context.Context, info WorkerScalingInfo) (int, error)
}

// workerScalerAdapter adapts the public WorkerScaler interface to the internal one
type workerScalerAdapter struct {
	publicScaler WorkerScaler
}

// EvaluateWorkerCount implements the internal WorkerScaler interface
func (a *workerScalerAdapter) EvaluateWorkerCount(ctx context.Context, info internalcontroller.WorkerScalingInfo) (int, error) {
	// Convert internal WorkerScalingInfo to public WorkerScalingInfo
	publicInfo := WorkerScalingInfo{
		CurrentWorkers: info.CurrentWorkers,
		MinWorkers:     info.MinWorkers,
		MaxWorkers:     info.MaxWorkers,
		QueueLength:    info.QueueLength,
		ControllerName: info.ControllerName,
	}

	// Call the public scaler with converted info
	return a.publicScaler.EvaluateWorkerCount(ctx, publicInfo)
}
