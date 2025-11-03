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

package callerinfo

import (
	"context"
	"fmt"
	goruntime "runtime"
	"strings"
	"time"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey int

const (
	// callerInfoKey is the context key for storing caller information
	callerInfoKey contextKey = iota
)

// CallerInfo stores information about who triggered the informer creation
type CallerInfo struct {
	// Stack is the captured call stack at the point of controller/source creation
	Stack string
	// Location is a human-readable description of where this was called
	Location string
	// Timestamp is when this information was captured
	Timestamp time.Time
}

// WithCallerInfoStack adds caller information with a pre-captured stack to the context.
// This is used internally when the stack was captured earlier (e.g., before a goroutine boundary).
func WithCallerInfoStack(ctx context.Context, location string, stack string) context.Context {
	info := &CallerInfo{
		Stack:     stack,
		Location:  location,
		Timestamp: time.Now(),
	}
	
	return context.WithValue(ctx, callerInfoKey, info)
}

// GetCallerInfo retrieves caller information from the context
func GetCallerInfo(ctx context.Context) (*CallerInfo, bool) {
	info, ok := ctx.Value(callerInfoKey).(*CallerInfo)
	return info, ok
}

// CaptureStack captures and formats the call stack for debugging.
// It skips the first 'skip' frames and captures up to 'maxDepth' frames.
// Only runtime internal frames are filtered out.
func CaptureStack(skip, maxDepth int) string {
	var builder strings.Builder

	pcs := make([]uintptr, maxDepth)
	n := goruntime.Callers(skip, pcs)
	frames := goruntime.CallersFrames(pcs[:n])

	count := 0
	maxFrames := 15

	for {
		frame, more := frames.Next()
		if !more {
			break
		}

		// Skip only runtime internal frames
		if strings.HasPrefix(frame.Function, "runtime.") {
			continue
		}

		// Extract readable function name
		funcName := frame.Function
		if idx := strings.LastIndex(funcName, "/"); idx >= 0 {
			funcName = funcName[idx+1:]
		}

		if count > 0 {
			builder.WriteString(" <- ")
		}

		builder.WriteString(fmt.Sprintf("%s:%d", funcName, frame.Line))
		count++

		if count >= maxFrames {
			break
		}
	}

	if builder.Len() == 0 {
		return "[no stack available]"
	}

	return builder.String()
}


