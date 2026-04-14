/*
Copyright 2024 The Lyra Authors.

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

package noderesources

import (
	"context"
	"testing"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	"github.com/dingyu123456/lyra/internal/scheduler/framework/parallelize"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

// mockHandle 是一个模拟的 framework.Handle 实现
type mockHandle struct{}

func (m *mockHandle) SnapshotSharedLister() framework.SharedLister {
	return nil
}

func (m *mockHandle) IterateOverWaitingPods(callback func(framework.WaitingPod)) {}

func (m *mockHandle) GetWaitingPod(uid types.UID) framework.WaitingPod {
	return nil
}

func (m *mockHandle) RejectWaitingPod(uid types.UID) bool {
	return false
}

func (m *mockHandle) ClientSet() kubernetes.Interface {
	return nil
}

func (m *mockHandle) SharedInformerFactory() informers.SharedInformerFactory {
	return nil
}

func (m *mockHandle) Parallelizer() parallelize.Parallelizer {
	return parallelize.NewParallelizer(1)
}

func (m *mockHandle) KarmadaClient() karmadaclientset.Interface {
	return nil
}

func (m *mockHandle) Logger() *zap.Logger {
	return nil
}

func TestComputePodResourceRequest(t *testing.T) {
	tests := []struct {
		name    string
		pod     *corev1.Pod
		wantCPU int64
		wantMem int64
	}{
		{
			name: "pod with single container",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("100Mi"),
								},
							},
						},
					},
				},
			},
			wantCPU: 100,               // 100 milliCPU
			wantMem: 100 * 1024 * 1024, // 100Mi in bytes
		},
		{
			name: "pod with multiple containers",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("100Mi"),
								},
							},
						},
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("200m"),
									corev1.ResourceMemory: resource.MustParse("200Mi"),
								},
							},
						},
					},
				},
			},
			wantCPU: 300,               // 100m + 200m
			wantMem: 300 * 1024 * 1024, // 100Mi + 200Mi
		},
		{
			name: "pod with init container larger than sum of containers",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("500Mi"),
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("100Mi"),
								},
							},
						},
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("200m"),
									corev1.ResourceMemory: resource.MustParse("200Mi"),
								},
							},
						},
					},
				},
			},
			wantCPU: 500,               // max(500, 300)
			wantMem: 500 * 1024 * 1024, // max(500Mi, 300Mi)
		},
		{
			name: "pod with init container smaller than sum of containers",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("200m"),
									corev1.ResourceMemory: resource.MustParse("200Mi"),
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("300m"),
									corev1.ResourceMemory: resource.MustParse("300Mi"),
								},
							},
						},
					},
				},
			},
			wantCPU: 300,               // max(200, 300)
			wantMem: 300 * 1024 * 1024, // max(200Mi, 300Mi)
		},
		{
			name: "pod with multiple init containers",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("100Mi"),
								},
							},
						},
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("400m"),
									corev1.ResourceMemory: resource.MustParse("400Mi"),
								},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("200m"),
									corev1.ResourceMemory: resource.MustParse("200Mi"),
								},
							},
						},
					},
				},
			},
			wantCPU: 400,               // max(100, 400, 200)
			wantMem: 400 * 1024 * 1024, // max(100Mi, 400Mi, 200Mi)
		},
		{
			name: "pod without resource requests",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{},
					},
				},
			},
			wantCPU: 0,
			wantMem: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := computePodResourceRequest(tt.pod)
			if state.MilliCPU != tt.wantCPU {
				t.Errorf("computePodResourceRequest() MilliCPU = %v, want %v", state.MilliCPU, tt.wantCPU)
			}
			if state.Memory != tt.wantMem {
				t.Errorf("computePodResourceRequest() Memory = %v, want %v", state.Memory, tt.wantMem)
			}
		})
	}
}

func TestFit_PreFilter(t *testing.T) {
	tests := []struct {
		name      string
		pod       *corev1.Pod
		wantError bool
	}{
		{
			name: "valid pod with resources",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("100Mi"),
								},
							},
						},
					},
				},
			},
			wantError: false,
		},
		{
			name: "pod without resources",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{}},
				},
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			plugin := &Fit{handle: &mockHandle{}}
			state := framework.NewCycleState()

			result, status := plugin.PreFilter(ctx, state, tt.pod)

			if tt.wantError {
				if status.IsSuccess() {
					t.Errorf("PreFilter() expected error, got success")
				}
				return
			}

			if !status.IsSuccess() {
				t.Errorf("PreFilter() unexpected error: %v", status)
				return
			}

			if result != nil {
				t.Errorf("PreFilter() expected nil result, got %v", result)
			}

			// Check if state was written correctly
			data, err := state.Read(preFilterStateKey)
			if err != nil {
				t.Errorf("PreFilter() failed to read state: %v", err)
				return
			}

			req := data.(*preFilterState)
			expectedReq := computePodResourceRequest(tt.pod)
			if req.MilliCPU != expectedReq.MilliCPU || req.Memory != expectedReq.Memory {
				t.Errorf("PreFilter() state mismatch: got %+v, want %+v", req, expectedReq)
			}
		})
	}
}

func TestFit_Filter(t *testing.T) {
	tests := []struct {
		name           string
		pod            *corev1.Pod
		nodeInfo       *framework.NodeInfo
		preFilterState *preFilterState
		wantStatus     framework.Code
	}{
		{
			name: "node with sufficient resources",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			},
			preFilterState: &preFilterState{
				MilliCPU: 1000,                   // 1 CPU
				Memory:   1 * 1024 * 1024 * 1024, // 1Gi
			},
			nodeInfo: &framework.NodeInfo{
				Allocatable: &framework.Resource{
					MilliCPU:         4000,                   // 4 CPU
					Memory:           4 * 1024 * 1024 * 1024, // 4Gi
					AllowedPodNumber: 110,
				},
				Requested: &framework.Resource{
					MilliCPU: 2000,                   // 2 CPU already used
					Memory:   2 * 1024 * 1024 * 1024, // 2Gi already used
				},
				Pods: make([]*framework.PodInfo, 100), // 100 pods already scheduled
			},
			wantStatus: framework.Success,
		},
		{
			name: "node with insufficient CPU",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			},
			preFilterState: &preFilterState{
				MilliCPU: 2000,                   // 2 CPU
				Memory:   1 * 1024 * 1024 * 1024, // 1Gi
			},
			nodeInfo: &framework.NodeInfo{
				Allocatable: &framework.Resource{
					MilliCPU:         4000,                   // 4 CPU
					Memory:           4 * 1024 * 1024 * 1024, // 4Gi
					AllowedPodNumber: 110,
				},
				Requested: &framework.Resource{
					MilliCPU: 3000,                   // 3 CPU already used
					Memory:   1 * 1024 * 1024 * 1024, // 1Gi already used
				},
				Pods: make([]*framework.PodInfo, 100),
			},
			wantStatus: framework.Unschedulable,
		},
		{
			name: "node with insufficient memory",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			},
			preFilterState: &preFilterState{
				MilliCPU: 1000,                   // 1 CPU
				Memory:   2 * 1024 * 1024 * 1024, // 2Gi
			},
			nodeInfo: &framework.NodeInfo{
				Allocatable: &framework.Resource{
					MilliCPU:         4000,                   // 4 CPU
					Memory:           4 * 1024 * 1024 * 1024, // 4Gi
					AllowedPodNumber: 110,
				},
				Requested: &framework.Resource{
					MilliCPU: 1000,                   // 1 CPU already used
					Memory:   3 * 1024 * 1024 * 1024, // 3Gi already used
				},
				Pods: make([]*framework.PodInfo, 100),
			},
			wantStatus: framework.Unschedulable,
		},
		{
			name: "node with too many pods",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			},
			preFilterState: &preFilterState{
				MilliCPU: 100,
				Memory:   100 * 1024 * 1024, // 100Mi
			},
			nodeInfo: &framework.NodeInfo{
				Allocatable: &framework.Resource{
					MilliCPU:         4000,
					Memory:           4 * 1024 * 1024 * 1024,
					AllowedPodNumber: 110,
				},
				Requested: &framework.Resource{
					MilliCPU: 1000,
					Memory:   1 * 1024 * 1024 * 1024,
				},
				Pods: make([]*framework.PodInfo, 110), // Already at max pod limit
			},
			wantStatus: framework.Unschedulable,
		},
		{
			name: "pod without resource requests",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			},
			preFilterState: &preFilterState{
				MilliCPU: 0,
				Memory:   0,
			},
			nodeInfo: &framework.NodeInfo{
				Allocatable: &framework.Resource{
					MilliCPU:         4000,
					Memory:           4 * 1024 * 1024 * 1024,
					AllowedPodNumber: 110,
				},
				Requested: &framework.Resource{
					MilliCPU: 4000, // Node is fully utilized
					Memory:   4 * 1024 * 1024 * 1024,
				},
				Pods: make([]*framework.PodInfo, 100),
			},
			wantStatus: framework.Success, // Pod without requests should always pass
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			plugin := &Fit{handle: &mockHandle{}}
			state := framework.NewCycleState()

			// Write pre-filter state
			state.Write(preFilterStateKey, tt.preFilterState)

			status := plugin.Filter(ctx, state, tt.pod, tt.nodeInfo)

			if status.Code() != tt.wantStatus {
				t.Errorf("Filter() status = %v, want %v. Message: %v", status.Code(), tt.wantStatus, status.Message())
			}
		})
	}
}

func TestFit_Score(t *testing.T) {
	tests := []struct {
		name           string
		pod            *corev1.Pod
		nodeInfo       *framework.NodeInfo
		preFilterState *preFilterState
		wantScore      int64
	}{
		{
			name: "empty node",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			},
			preFilterState: &preFilterState{
				MilliCPU: 1000,                   // 1 CPU
				Memory:   1 * 1024 * 1024 * 1024, // 1Gi
			},
			nodeInfo: &framework.NodeInfo{
				Allocatable: &framework.Resource{
					MilliCPU: 4000,                   // 4 CPU
					Memory:   4 * 1024 * 1024 * 1024, // 4Gi
				},
				Requested: &framework.Resource{
					MilliCPU: 0,
					Memory:   0,
				},
			},
			wantScore: int64((1.0 - 0.25) * float64(framework.MaxNodeScore)), // 25% utilization after scheduling
		},
		{
			name: "half utilized node",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			},
			preFilterState: &preFilterState{
				MilliCPU: 1000,                   // 1 CPU
				Memory:   1 * 1024 * 1024 * 1024, // 1Gi
			},
			nodeInfo: &framework.NodeInfo{
				Allocatable: &framework.Resource{
					MilliCPU: 4000,                   // 4 CPU
					Memory:   4 * 1024 * 1024 * 1024, // 4Gi
				},
				Requested: &framework.Resource{
					MilliCPU: 2000,                   // 2 CPU already used (50%)
					Memory:   2 * 1024 * 1024 * 1024, // 2Gi already used (50%)
				},
			},
			wantScore: int64((1.0 - 0.75) * float64(framework.MaxNodeScore)), // 75% utilization after scheduling
		},
		{
			name: "fully utilized node",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			},
			preFilterState: &preFilterState{
				MilliCPU: 1000,                   // 1 CPU
				Memory:   1 * 1024 * 1024 * 1024, // 1Gi
			},
			nodeInfo: &framework.NodeInfo{
				Allocatable: &framework.Resource{
					MilliCPU: 4000,                   // 4 CPU
					Memory:   4 * 1024 * 1024 * 1024, // 4Gi
				},
				Requested: &framework.Resource{
					MilliCPU: 4000,                   // 4 CPU already used (100%)
					Memory:   4 * 1024 * 1024 * 1024, // 4Gi already used (100%)
				},
			},
			wantScore: 0, // 125% utilization after scheduling, capped at 100%
		},
		{
			name: "pod without resource requests",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			},
			preFilterState: &preFilterState{
				MilliCPU: 0,
				Memory:   0,
			},
			nodeInfo: &framework.NodeInfo{
				Allocatable: &framework.Resource{
					MilliCPU: 4000,
					Memory:   4 * 1024 * 1024 * 1024,
				},
				Requested: &framework.Resource{
					MilliCPU: 4000,
					Memory:   4 * 1024 * 1024 * 1024,
				},
			},
			wantScore: 0, // Node is fully utilized
		},
		{
			name: "node with zero allocatable CPU",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			},
			preFilterState: &preFilterState{
				MilliCPU: 1000,
				Memory:   1 * 1024 * 1024 * 1024,
			},
			nodeInfo: &framework.NodeInfo{
				Allocatable: &framework.Resource{
					MilliCPU: 0, // Zero allocatable CPU
					Memory:   4 * 1024 * 1024 * 1024,
				},
				Requested: &framework.Resource{
					MilliCPU: 0,
					Memory:   0,
				},
			},
			wantScore: 37, // Only memory contributes (half score: 0.75 * 50 = 37.5 rounded down)
		},
		{
			name: "node with zero allocatable memory",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
			},
			preFilterState: &preFilterState{
				MilliCPU: 1000,
				Memory:   1 * 1024 * 1024 * 1024,
			},
			nodeInfo: &framework.NodeInfo{
				Allocatable: &framework.Resource{
					MilliCPU: 4000,
					Memory:   0, // Zero allocatable memory
				},
				Requested: &framework.Resource{
					MilliCPU: 0,
					Memory:   0,
				},
			},
			wantScore: 37, // Only CPU contributes (half score: 0.75 * 50 = 37.5 rounded down)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			plugin := &Fit{handle: &mockHandle{}}
			state := framework.NewCycleState()

			// Write pre-filter state
			state.Write(preFilterStateKey, tt.preFilterState)

			score, status := plugin.Score(ctx, state, tt.pod, tt.nodeInfo)

			if !status.IsSuccess() {
				t.Errorf("Score() unexpected error: %v", status)
				return
			}

			if score != tt.wantScore {
				t.Errorf("Score() = %v, want %v", score, tt.wantScore)
			}
		})
	}
}

func TestFit_New(t *testing.T) {
	ctx := context.Background()
	var obj runtime.Object
	handle := &mockHandle{}

	plugin, err := New(ctx, obj, handle)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if plugin == nil {
		t.Fatal("New() returned nil plugin")
	}

	if plugin.Name() != Name {
		t.Errorf("Name() = %v, want %v", plugin.Name(), Name)
	}
}
