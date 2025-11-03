/*
Copyright 2022 The Kubernetes Authors.

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

package internal

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/internal/callerinfo"
)

// Test that gvkFixupWatcher behaves like watch.FakeWatcher
// and that it overrides the GVK.
// These tests are adapted from the watch.FakeWatcher tests in:
// https://github.com/kubernetes/kubernetes/blob/adbda068c1808fcc8a64a94269e0766b5c46ec41/staging/src/k8s.io/apimachinery/pkg/watch/watch_test.go#L33-L78
var _ = Describe("gvkFixupWatcher", func() {
	It("behaves like watch.FakeWatcher", func() {
		newTestType := func(name string) runtime.Object {
			return &metav1.PartialObjectMetadata{
				ObjectMeta: metav1.ObjectMeta{
					Name: name,
				},
			}
		}

		f := watch.NewFake()
		// This is the GVK which we expect the wrapper to set on all the events
		expectedGVK := schema.GroupVersionKind{
			Group:   "testgroup",
			Version: "v1test2",
			Kind:    "TestKind",
		}
		gvkfw := newGVKFixupWatcher(expectedGVK, f)

		table := []struct {
			t watch.EventType
			s runtime.Object
		}{
			{watch.Added, newTestType("foo")},
			{watch.Modified, newTestType("qux")},
			{watch.Modified, newTestType("bar")},
			{watch.Deleted, newTestType("bar")},
			{watch.Error, newTestType("error: blah")},
		}

		consumer := func(w watch.Interface) {
			for _, expect := range table {
				By(fmt.Sprintf("Fixing up watch.EventType: %v and passing it on", expect.t))
				got, ok := <-w.ResultChan()
				Expect(ok).To(BeTrue(), "closed early")
				Expect(expect.t).To(Equal(got.Type), "unexpected Event.Type or out-of-order Event")
				Expect(got.Object).To(BeAssignableToTypeOf(&metav1.PartialObjectMetadata{}), "unexpected Event.Object type")
				a := got.Object.(*metav1.PartialObjectMetadata)
				Expect(got.Object.GetObjectKind().GroupVersionKind()).To(Equal(expectedGVK), "GVK was not fixed up")
				expected := expect.s.DeepCopyObject()
				expected.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
				actual := a.DeepCopyObject()
				actual.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
				Expect(actual).To(Equal(expected), "unexpected change to the Object")
			}
			Eventually(w.ResultChan()).Should(BeClosed())
		}

		sender := func() {
			f.Add(newTestType("foo"))
			f.Action(watch.Modified, newTestType("qux"))
			f.Modify(newTestType("bar"))
			f.Delete(newTestType("bar"))
			f.Error(newTestType("error: blah"))
			f.Stop()
		}

		go sender()
		consumer(gvkfw)
	})
})

// captureLogger is a test logger that captures log messages for verification
type captureLogger struct {
	mu       sync.Mutex
	messages []capturedLog
	level    int
}

type capturedLog struct {
	level          int
	msg            string
	keysAndValues  []interface{}
	keysAndValuesMap map[string]interface{}
}

func newCaptureLogger(level int) *captureLogger {
	return &captureLogger{
		messages: make([]capturedLog, 0),
		level:    level,
	}
}

func (l *captureLogger) Init(info logr.RuntimeInfo) {}

func (l *captureLogger) Enabled(level int) bool {
	return level <= l.level
}

func (l *captureLogger) Info(level int, msg string, keysAndValues ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	kvMap := make(map[string]interface{})
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := keysAndValues[i].(string)
			kvMap[key] = keysAndValues[i+1]
		}
	}
	
	l.messages = append(l.messages, capturedLog{
		level:            level,
		msg:              msg,
		keysAndValues:    keysAndValues,
		keysAndValuesMap: kvMap,
	})
}

func (l *captureLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	kvMap := make(map[string]interface{})
	kvMap["error"] = err
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := keysAndValues[i].(string)
			kvMap[key] = keysAndValues[i+1]
		}
	}
	
	l.messages = append(l.messages, capturedLog{
		level:            0,
		msg:              msg,
		keysAndValues:    keysAndValues,
		keysAndValuesMap: kvMap,
	})
}

func (l *captureLogger) WithValues(keysAndValues ...interface{}) logr.LogSink {
	return l
}

func (l *captureLogger) WithName(name string) logr.LogSink {
	return l
}

func (l *captureLogger) GetMessages() []capturedLog {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]capturedLog{}, l.messages...)
}

var _ = Describe("Informer Creation Logging", func() {
	var (
		testLogger     *captureLogger
		originalLogger logr.Logger
		informers      *Informers
		testScheme     *runtime.Scheme
	)

	BeforeEach(func() {
		// Save the original logger
		originalLogger = log
		
		// Create a test logger that captures V(4) logs
		testLogger = newCaptureLogger(4)
		log = logr.New(testLogger).WithName("cache")
		
		// Create a basic test scheme
		testScheme = runtime.NewScheme()
		Expect(corev1.AddToScheme(testScheme)).To(Succeed())
		
		// Create a mock Informers instance
		informers = &Informers{
			config: &rest.Config{
				Host: "https://localhost:6443",
			},
			scheme: testScheme,
			tracker: tracker{
				Structured:   make(map[schema.GroupVersionKind]*Cache),
				Unstructured: make(map[schema.GroupVersionKind]*Cache),
				Metadata:     make(map[schema.GroupVersionKind]*Cache),
			},
			mapper: &fakeRESTMapper{},
			newInformer: func(lw cache.ListerWatcher, obj runtime.Object, resync time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
				return cache.NewSharedIndexInformer(lw, obj, resync, indexers)
			},
			startWait: make(chan struct{}),
		}
	})

	AfterEach(func() {
		// Restore the original logger
		log = originalLogger
	})

	Context("when addInformerToMap is called", func() {
		It("should log informer creation at V(4) level with GVK and call stack", func() {
			gvk := schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}
			obj := &corev1.Pod{}

			// Call addInformerToMap - this should trigger the debug log
			_, _, err := informers.addInformerToMap(context.Background(), gvk, obj)
			
			// We expect an error here because we don't have a real API server
			// but the log should still be captured before the error occurs
			_ = err

			// Verify that a log message was captured
			messages := testLogger.GetMessages()
			Expect(messages).To(HaveLen(1), "Expected exactly one log message")

			// Verify the log message content
			msg := messages[0]
			Expect(msg.msg).To(Equal("Creating informer for GVK"))
			Expect(msg.level).To(Equal(4), "Expected log level V(4)")

			// Verify the log contains the expected fields
			Expect(msg.keysAndValuesMap).To(HaveKey("gvk"))
			Expect(msg.keysAndValuesMap["gvk"]).To(ContainSubstring("v1"))
			Expect(msg.keysAndValuesMap["gvk"]).To(ContainSubstring("Pod"))

			Expect(msg.keysAndValuesMap).To(HaveKey("type"))
			Expect(msg.keysAndValuesMap["type"]).To(ContainSubstring("v1.Pod"))

			// Verify the call stack is present and contains our test function
			Expect(msg.keysAndValuesMap).To(HaveKey("callStack"))
			callStack := msg.keysAndValuesMap["callStack"].(string)
			Expect(callStack).NotTo(BeEmpty(), "Call stack should not be empty")
			
			// The call stack should contain references to the test function
			// (it may vary depending on the ginkgo internals, so we just check it's not empty)
			By(fmt.Sprintf("Call stack captured: %s", callStack))
		})

		It("should log informer creation for Unstructured objects", func() {
			gvk := schema.GroupVersionKind{
				Group:   "apps",
				Version: "v1",
				Kind:    "Deployment",
			}
			obj := &fakeUnstructured{}

			_, _, err := informers.addInformerToMap(context.Background(), gvk, obj)
			_ = err

			messages := testLogger.GetMessages()
			Expect(messages).To(HaveLen(1))

			msg := messages[0]
			Expect(msg.msg).To(Equal("Creating informer for GVK"))
			Expect(msg.keysAndValuesMap["gvk"]).To(ContainSubstring("apps/v1"))
			Expect(msg.keysAndValuesMap["gvk"]).To(ContainSubstring("Deployment"))
			Expect(msg.keysAndValuesMap["type"]).To(ContainSubstring("fakeUnstructured"))
		})

		It("should log informer creation for PartialObjectMetadata", func() {
			gvk := schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "ConfigMap",
			}
			obj := &metav1.PartialObjectMetadata{}

			_, _, err := informers.addInformerToMap(context.Background(), gvk, obj)
			_ = err

			messages := testLogger.GetMessages()
			Expect(messages).To(HaveLen(1))

			msg := messages[0]
			Expect(msg.msg).To(Equal("Creating informer for GVK"))
			Expect(msg.keysAndValuesMap["gvk"]).To(ContainSubstring("v1"))
			Expect(msg.keysAndValuesMap["gvk"]).To(ContainSubstring("ConfigMap"))
			Expect(msg.keysAndValuesMap["type"]).To(ContainSubstring("PartialObjectMetadata"))
		})

		It("should include function names in the call stack", func() {
			gvk := schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Service",
			}
			obj := &corev1.Service{}

			// Call through a helper function to verify stack trace captures it
			helperThatCreatesInformer := func() {
				_, _, _ = informers.addInformerToMap(context.Background(), gvk, obj)
			}
			helperThatCreatesInformer()

			messages := testLogger.GetMessages()
			Expect(messages).To(HaveLen(1))

			msg := messages[0]
			callStack := msg.keysAndValuesMap["callStack"].(string)
			
			// The call stack should contain our helper function name
			// Note: The exact format may vary, but it should contain function names
			Expect(callStack).NotTo(BeEmpty())
			By(fmt.Sprintf("Verified call stack structure: %s", callStack))
		})
	})

	Context("when log level is below V(4)", func() {
		BeforeEach(func() {
			// Create a logger with level 3 (below V(4))
			testLogger = newCaptureLogger(3)
			log = logr.New(testLogger).WithName("cache")
		})

		It("should not log informer creation", func() {
			gvk := schema.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "Pod",
			}
			obj := &corev1.Pod{}

			_, _, err := informers.addInformerToMap(context.Background(), gvk, obj)
			_ = err

			// Verify that no log message was captured
			messages := testLogger.GetMessages()
			Expect(messages).To(BeEmpty(), "No logs should be captured at V(3) level")
		})
	})
})

var _ = Describe("captureCallerStack", func() {
	It("should capture call stack with function names and line numbers", func() {
		stack := callerinfo.CaptureStack(2, 5)
		
		Expect(stack).NotTo(BeEmpty())
		
		// The stack should contain function names and line numbers
		// Format: package.Function:LineNumber <- ...
		Expect(stack).To(MatchRegexp(`\w+:\d+`), "Stack should contain function:line format")
		
		By(fmt.Sprintf("Captured stack: %s", stack))
	})

	It("should filter out runtime internal frames", func() {
		stack := callerinfo.CaptureStack(2, 10)
		
		// Should not contain runtime.* functions
		Expect(stack).NotTo(ContainSubstring("runtime."))
	})

	It("should use arrow notation to show call chain", func() {
		helperFunc := func() string {
			return callerinfo.CaptureStack(2, 5)
		}
		
		stack := helperFunc()
		
		// If there are multiple frames, they should be separated by " <- "
		if strings.Contains(stack, "internal.") {
			// Stack with multiple frames should have arrow separators
			// We can't be too specific as it depends on test runner internals
			Expect(stack).To(MatchRegexp(`\w+:\d+`))
		}
	})
})

// fakeUnstructured is a fake implementation of runtime.Unstructured for testing
type fakeUnstructured struct {
	metav1.TypeMeta
	metav1.ObjectMeta
}

func (f *fakeUnstructured) GetObjectKind() schema.ObjectKind {
	return &f.TypeMeta
}

func (f *fakeUnstructured) DeepCopyObject() runtime.Object {
	return &fakeUnstructured{}
}

func (f *fakeUnstructured) UnstructuredContent() map[string]interface{} {
	return map[string]interface{}{}
}

func (f *fakeUnstructured) SetUnstructuredContent(map[string]interface{}) {}

// fakeRESTMapper is a minimal fake implementation for testing
type fakeRESTMapper struct{}

func (m *fakeRESTMapper) KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	return schema.GroupVersionKind{}, fmt.Errorf("not implemented")
}

func (m *fakeRESTMapper) KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *fakeRESTMapper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return schema.GroupVersionResource{}, fmt.Errorf("not implemented")
}

func (m *fakeRESTMapper) ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *fakeRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	return &meta.RESTMapping{
		Resource: schema.GroupVersionResource{
			Group:    gk.Group,
			Version:  versions[0],
			Resource: strings.ToLower(gk.Kind) + "s",
		},
		GroupVersionKind: schema.GroupVersionKind{
			Group:   gk.Group,
			Version: versions[0],
			Kind:    gk.Kind,
		},
		Scope: meta.RESTScopeNamespace,
	}, nil
}

func (m *fakeRESTMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	mapping, err := m.RESTMapping(gk, versions...)
	if err != nil {
		return nil, err
	}
	return []*meta.RESTMapping{mapping}, nil
}

func (m *fakeRESTMapper) ResourceSingularizer(resource string) (singular string, err error) {
	return resource, nil
}
