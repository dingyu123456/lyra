package queue

import (
	"testing"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// TestQueueingHintDisabled_WakesAllPods 直接测试 isPodWorthRequeuing 在禁用模式下的行为
func TestQueueingHintDisabled_WakesAllPods(t *testing.T) {
	logger := zap.NewNop()

	queue := NewSchedulingQueue(
		func(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
			return pInfo1.Timestamp.Before(pInfo2.Timestamp)
		},
		nil,
		WithLogger(logger),
	).(*PriorityQueue)

	// 强制禁用 QueueingHint
	queue.isSchedulingQueueHintEnabled = false
	queue.queueingHintMap = nil

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
		Spec:       corev1.PodSpec{SchedulerName: "lyra-scheduler"},
	}
	pInfo := &framework.QueuedPodInfo{
		PodInfo:              &framework.PodInfo{Pod: pod},
		UnschedulablePlugins: sets.New[string](),
	}
	pInfo.UnschedulablePlugins.Insert("GPUResourceFit")

	nodeEvent := framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Update}

	// 调用 isPodWorthRequeuing
	hint := queue.isPodWorthRequeuing(logger, pInfo, nodeEvent, nil, nil)

	// 禁用时，应该返回 queueAfterBackoff（全量唤醒）
	if hint != queueAfterBackoff {
		t.Fatalf("Expected queueAfterBackoff when disabled, got %v", hint)
	}
	t.Log("PASS: QueueingHint disabled correctly returns queueAfterBackoff (wakes all)")
}

// TestQueueingHintEnabled_FiltersByType 直接测试启用模式下 GPU 类型过滤
func TestQueueingHintEnabled_FiltersByType(t *testing.T) {
	logger := zap.NewNop()

	// 模拟 GPUResourceFit 的 QueueingHintFn：
	// - 需要 A100 的 Pod：只被 A100 节点唤醒
	// - 需要 V100 的 Pod：只被 V100 节点唤醒
	// - AnyGPU Pod（无类型限制）：任何 GPU 节点都能唤醒
	hintMap := map[framework.ClusterEvent][]framework.ClusterEventWithPluginHint{
		{Resource: framework.Node, ActionType: framework.Update}: {
			{
				Event:      framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Update},
				PluginName: "GPUResourceFit",
				QueueingHintFn: func(logger *zap.Logger, pod *corev1.Pod, oldObj, newObj any) (framework.QueueingHint, error) {
					var requiredType string
					if pod.Annotations != nil {
						requiredType = pod.Annotations["nvidia.com/use-gputype"]
					}

					var nodeGPUType string
					if newObj != nil {
						if node, ok := newObj.(*corev1.Node); ok {
							if node.Annotations != nil {
								nodeGPUType = node.Annotations["test-gpu-type"]
							}
						}
					}

					if requiredType == "" {
						return framework.Queue, nil
					}
					if nodeGPUType == requiredType {
						return framework.Queue, nil
					}
					return framework.QueueSkip, nil
				},
			},
		},
	}

	queue := NewSchedulingQueue(
		func(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
			return pInfo1.Timestamp.Before(pInfo2.Timestamp)
		},
		nil,
		WithLogger(logger),
	).(*PriorityQueue)
	queue.SetQueueingHintMap(hintMap)

	tests := []struct {
		name          string
		podName       string
		podGPUType    string
		nodeGPUType   string
		expectedHint  queueingStrategy
	}{
		{"A100 Pod on A100 node", "pod-a100", "NVIDIA-A100-80GB", "NVIDIA-A100-80GB", queueAfterBackoff},
		{"V100 Pod on A100 node", "pod-v100", "NVIDIA-V100-32GB", "NVIDIA-A100-80GB", queueSkip},
		{"AnyGPU Pod on A100 node", "pod-any", "", "NVIDIA-A100-80GB", queueAfterBackoff},
		{"A100 Pod on V100 node", "pod-a100-v100node", "NVIDIA-A100-80GB", "NVIDIA-V100-32GB", queueSkip},
	}

	nodeEvent := framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Update}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        tt.podName,
					Namespace:   "default",
					Annotations: map[string]string{},
				},
				Spec: corev1.PodSpec{SchedulerName: "lyra-scheduler"},
			}
			if tt.podGPUType != "" {
				pod.Annotations["nvidia.com/use-gputype"] = tt.podGPUType
			}

			// 创建带 GPU 类型注解的节点
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-node",
				},
			}
			if tt.nodeGPUType != "" {
				node.Annotations = map[string]string{"test-gpu-type": tt.nodeGPUType}
			}

			pInfo := &framework.QueuedPodInfo{
				PodInfo:              &framework.PodInfo{Pod: pod},
				UnschedulablePlugins: sets.New[string](),
			}
			pInfo.UnschedulablePlugins.Insert("GPUResourceFit")

			hint := queue.isPodWorthRequeuing(logger, pInfo, nodeEvent, nil, node)

			if hint != tt.expectedHint {
				t.Errorf("Expected %v, got %v", tt.expectedHint, hint)
			} else {
				t.Logf("PASS: %s -> %v", tt.name, hint)
			}
		})
	}
}
