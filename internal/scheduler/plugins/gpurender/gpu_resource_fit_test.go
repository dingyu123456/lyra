package gpurender

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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

// mockHandle is a mock implementation of framework.Handle for testing
type mockHandle struct{}

func (m *mockHandle) SnapshotSharedLister() framework.SharedLister { return nil }
func (m *mockHandle) IterateOverWaitingPods(callback func(framework.WaitingPod)) {}
func (m *mockHandle) GetWaitingPod(uid types.UID) framework.WaitingPod { return nil }
func (m *mockHandle) RejectWaitingPod(uid types.UID) bool           { return false }
func (m *mockHandle) ClientSet() kubernetes.Interface               { return nil }
func (m *mockHandle) KarmadaClient() karmadaclientset.Interface     { return nil }
func (m *mockHandle) Logger() *zap.Logger                           { return nil }
func (m *mockHandle) SharedInformerFactory() informers.SharedInformerFactory { return nil }
func (m *mockHandle) Parallelizer() parallelize.Parallelizer {
	return parallelize.NewParallelizer(1)
}

func TestGPUResourceFit_PreFilter_Skip(t *testing.T) {
	plugin := &GPUResourceFit{handle: &mockHandle{}}
	ctx := context.Background()
	state := framework.NewCycleState()

	// Pod without GPU requests should be skipped
	pod := &corev1.Pod{
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
	}

	result, status := plugin.PreFilter(ctx, state, pod)
	if !status.IsSkip() {
		t.Errorf("Expected Skip status for pod without GPU requests, got %v", status.Code())
	}
	if result != nil {
		t.Errorf("Expected nil PreFilterResult, got %v", result)
	}
}

func TestGPUResourceFit_Filter_NoGPURequest(t *testing.T) {
	plugin := &GPUResourceFit{handle: &mockHandle{}}
	ctx := context.Background()
	state := framework.NewCycleState()

	// Write GPU requirement with 0 cards (no GPU needed)
	req := &framework.GPURequirement{NumCards: 0}
	state.Write(framework.KeyPodGPUReq, req)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
	}
	nodeInfo := &framework.NodeInfo{
		NodeName:    "test-node",
		ClusterName: "test-cluster",
		Allocatable: &framework.Resource{},
		Requested:   &framework.Resource{},
	}

	status := plugin.Filter(ctx, state, pod, nodeInfo)
	if !status.IsSuccess() {
		t.Errorf("Expected Success for pod without GPU requests, got %v", status)
	}
}

func TestGPUResourceFit_Filter_InsufficientGPU(t *testing.T) {
	plugin := &GPUResourceFit{handle: &mockHandle{}}
	ctx := context.Background()
	state := framework.NewCycleState()

	req := &framework.GPURequirement{
		NumCards: 2,
		MemReq:   1024,
		CoreReq:  10,
	}
	state.Write(framework.KeyPodGPUReq, req)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
	}
	nodeInfo := &framework.NodeInfo{
		NodeName:    "test-node",
		ClusterName: "test-cluster",
		Allocatable: &framework.Resource{},
		Requested:   &framework.Resource{},
		GPUs: map[string]*framework.GPUInfo{
			"gpu-1": {
				UUID:            "gpu-1",
				Type:            "A100",
				AllocatableMem:  8192,
				RequestedMem:    7168,
				AllocatableCore: 100,
				RequestedCore:   90,
			},
		},
	}

	status := plugin.Filter(ctx, state, pod, nodeInfo)
	if status.IsSuccess() {
		t.Error("Expected Unschedulable for insufficient GPU, got Success")
	}
}

func TestGPUResourceFit_Score_NoGPURequest(t *testing.T) {
	plugin := &GPUResourceFit{handle: &mockHandle{}}
	ctx := context.Background()
	state := framework.NewCycleState()

	req := &framework.GPURequirement{NumCards: 0}
	state.Write(framework.KeyPodGPUReq, req)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod"},
	}
	nodeInfo := &framework.NodeInfo{
		NodeName:    "test-node",
		ClusterName: "test-cluster",
		Allocatable: &framework.Resource{},
		Requested:   &framework.Resource{},
	}

	score, status := plugin.Score(ctx, state, pod, nodeInfo)
	if !status.IsSuccess() {
		t.Errorf("Expected Success, got %v", status)
	}
	if score != framework.MaxNodeScore {
		t.Errorf("Expected MaxNodeScore for no GPU request, got %d", score)
	}
}

func TestGPUResourceFit_New(t *testing.T) {
	ctx := context.Background()
	plugin, err := New(ctx, nil, &mockHandle{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if plugin == nil {
		t.Fatal("New() returned nil")
	}
	if plugin.Name() != Name {
		t.Errorf("Name() = %v, want %v", plugin.Name(), Name)
	}
}
