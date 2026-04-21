package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

// ============================================================================
// Benchmark 2: 队列 QueueingHint 智能唤醒性能测试
// ============================================================================

// makeTestQueuedPodInfo 创建一个带有失败插件信息的 QueuedPodInfo
func makeTestQueuedPodInfo(podName, namespace string, failedPlugins []string, pendingPlugins []string) *framework.QueuedPodInfo {
	annotations := map[string]string{}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   namespace,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			NodeName:      "",
			SchedulerName: "lyra-scheduler",
		},
	}

	now := time.Now()
	return &framework.QueuedPodInfo{
		PodInfo:              &framework.PodInfo{Pod: pod},
		Timestamp:            now,
		Attempts:             1,
		UnschedulablePlugins: sets.New(failedPlugins...),
		PendingPlugins:       sets.New(pendingPlugins...),
		Gated:               false,
	}
}

// ============================================================================
// 模拟两种队列实现
// ============================================================================

// MockSchedulingQueueWithHint 模拟实现了 QueueingHint 的队列
// 关键区别：只唤醒被对应插件拒绝过的 Pod
type MockSchedulingQueueWithHint struct {
	unschedulablePods map[string]*framework.QueuedPodInfo
	pluginHints       map[string]sets.Set[string] // plugin -> 关心的 Pod 集合

	mu sync.RWMutex
}

func newMockQueueWithHint() *MockSchedulingQueueWithHint {
	return &MockSchedulingQueueWithHint{
		unschedulablePods: make(map[string]*framework.QueuedPodInfo),
		pluginHints:       make(map[string]sets.Set[string]),
	}
}

// AddPodWithFailure 模拟 Pod 调度失败，记录失败插件信息
func (q *MockSchedulingQueueWithHint) AddPodWithFailure(pInfo *framework.QueuedPodInfo) {
	key := NewObjectNameForNS(pInfo.Pod.Namespace, pInfo.Pod.Name).String()
	q.mu.Lock()
	defer q.mu.Unlock()

	for plugin := range pInfo.UnschedulablePlugins {
		if _, exists := q.pluginHints[plugin]; !exists {
			q.pluginHints[plugin] = sets.New[string]()
		}
		q.pluginHints[plugin].Insert(key)
	}

	q.unschedulablePods[key] = pInfo
}

// SimulateEventAndSchedule 模拟触发某个插件关心的事件，并对唤醒的 Pod 执行调度
// 返回实际执行的调度次数（即无效调度次数的对比依据）
func (q *MockSchedulingQueueWithHint) SimulateEventAndSchedule(pluginName string, scheduleFn func(*framework.QueuedPodInfo)) int {
	q.mu.RLock()
	affectedPods := q.pluginHints[pluginName]
	pods := make([]*framework.QueuedPodInfo, 0, affectedPods.Len())
	for key := range affectedPods {
		if pInfo, ok := q.unschedulablePods[key]; ok {
			pods = append(pods, pInfo)
		}
	}
	q.mu.RUnlock()

	for _, pInfo := range pods {
		scheduleFn(pInfo)
	}
	return len(pods)
}

// ============================================================================
// MockSchedulingQueueWithoutHint 模拟没有 QueueingHint 的队列（全量唤醒）
// 每次事件触发都遍历所有不可调度 Pod
type MockSchedulingQueueWithoutHint struct {
	unschedulablePods map[string]*framework.QueuedPodInfo
	mu                sync.RWMutex
}

func newMockQueueWithoutHint() *MockSchedulingQueueWithoutHint {
	return &MockSchedulingQueueWithoutHint{
		unschedulablePods: make(map[string]*framework.QueuedPodInfo),
	}
}

func (q *MockSchedulingQueueWithoutHint) AddPodWithFailure(pInfo *framework.QueuedPodInfo) {
	key := NewObjectNameForNS(pInfo.Pod.Namespace, pInfo.Pod.Name).String()
	q.mu.Lock()
	defer q.mu.Unlock()
	q.unschedulablePods[key] = pInfo
}

// SimulateEventAndSchedule 模拟没有 QueueingHint：每次事件都唤醒所有 Pod 去调度
func (q *MockSchedulingQueueWithoutHint) SimulateEventAndSchedule(scheduleFn func(*framework.QueuedPodInfo)) int {
	q.mu.RLock()
	pods := make([]*framework.QueuedPodInfo, 0, len(q.unschedulablePods))
	for _, pInfo := range q.unschedulablePods {
		pods = append(pods, pInfo)
	}
	q.mu.RUnlock()

	for _, pInfo := range pods {
		scheduleFn(pInfo)
	}
	return len(pods)
}

// ============================================================================
// 工具函数
// ============================================================================

// NewObjectNameForNS 构造 namespace/name 格式的字符串 key
func NewObjectNameForNS(ns, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: ns, Name: name}
}

// ============================================================================
// 模拟一次调度的开销（Filter + Score 的轻量模拟）
// ============================================================================

// mockScheduleCost 模拟一次调度的 CPU 开销
// 在真实调度器中，Filter/Score 会遍历所有节点计算资源
// 这里用一段轻量计算来模拟（不是纯空循环，避免被编译器优化掉）
func mockScheduleCost(pInfo *framework.QueuedPodInfo) {
	// 模拟 Filter 阶段：遍历节点检查资源是否满足
	// 用一个简单的计算来产生可衡量的 CPU 开销
	var sum int64
	for i := 0; i < 100; i++ {
		sum += int64(pInfo.Attempts) * int64(i)
	}
	// 使用结果防止编译器优化
	_ = sum
}

// ============================================================================
// Benchmark: QueueingHint vs 全量唤醒 — 调度总开销对比
// ============================================================================

// BenchmarkQueueingHint_SchedulingOverhead 对比有 Hint 和无 Hint 的调度总开销
// 核心对比点：当 GPU 资源释放时，
//   - WithHint: 只唤醒被 GPUResourceFit 拒绝过的 Pod 去重新调度
//   - WithoutHint: 唤醒所有不可调度 Pod 去重新调度（大量无效工作）
func BenchmarkQueueingHint_SchedulingOverhead(b *testing.B) {
	tests := []struct {
		name             string
		totalPods        int
		affectedPodRatio float64 // 被 GPUResourceFit 拒绝的比例
	}{
		{"1000 Pods / 1% affected", 1000, 0.01},
		{"5000 Pods / 1% affected", 5000, 0.01},
		{"10000 Pods / 1% affected", 10000, 0.01},
		{"10000 Pods / 5% affected", 10000, 0.05},
		{"10000 Pods / 10% affected", 10000, 0.10},
	}

	for _, tt := range tests {
		affectedCount := int(float64(tt.totalPods) * tt.affectedPodRatio)

		b.Run(tt.name+"/WithHint", func(b *testing.B) {
			q := newMockQueueWithHint()

			for i := 0; i < tt.totalPods; i++ {
				var failedPlugins []string
				if i < affectedCount {
					failedPlugins = []string{"GPUResourceFit", "NodeResourcesFit"}
				} else {
					failedPlugins = []string{"NodeResourcesFit"}
				}
				pInfo := makeTestQueuedPodInfo(fmt.Sprintf("pod-%d", i), "default", failedPlugins, nil)
				q.AddPodWithFailure(pInfo)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 模拟 GPU 资源释放事件：只唤醒被 GPUResourceFit 拒绝过的 Pod
				q.SimulateEventAndSchedule("GPUResourceFit", mockScheduleCost)
			}
		})

		b.Run(tt.name+"/WithoutHint", func(b *testing.B) {
			q := newMockQueueWithoutHint()

			for i := 0; i < tt.totalPods; i++ {
				var failedPlugins []string
				if i < affectedCount {
					failedPlugins = []string{"GPUResourceFit", "NodeResourcesFit"}
				} else {
					failedPlugins = []string{"NodeResourcesFit"}
				}
				pInfo := makeTestQueuedPodInfo(fmt.Sprintf("pod-%d", i), "default", failedPlugins, nil)
				q.AddPodWithFailure(pInfo)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 模拟没有 Hint：GPU 释放事件唤醒所有不可调度 Pod
				q.SimulateEventAndSchedule(mockScheduleCost)
			}
		})
	}
}

// ============================================================================
// Benchmark: 无效调度次数统计（计数对比）
// ============================================================================

// BenchmarkQueueingHint_UselessSchedulingCount 统计无效调度次数
// 不测时间，测"做了多少次无效调度"
func BenchmarkQueueingHint_UselessSchedulingCount(b *testing.B) {
	tests := []struct {
		name             string
		totalPods        int
		affectedPodRatio float64
	}{
		{"10000 Pods / 1% affected", 10000, 0.01},
		{"10000 Pods / 5% affected", 10000, 0.05},
		{"10000 Pods / 10% affected", 10000, 0.10},
	}

	for _, tt := range tests {
		affectedCount := int(float64(tt.totalPods) * tt.affectedPodRatio)

		b.Run(tt.name+"/WithHint", func(b *testing.B) {
			q := newMockQueueWithHint()
			for i := 0; i < tt.totalPods; i++ {
				var failedPlugins []string
				if i < affectedCount {
					failedPlugins = []string{"GPUResourceFit", "NodeResourcesFit"}
				} else {
					failedPlugins = []string{"NodeResourcesFit"}
				}
				pInfo := makeTestQueuedPodInfo(fmt.Sprintf("pod-%d", i), "default", failedPlugins, nil)
				q.AddPodWithFailure(pInfo)
			}

			b.ResetTimer()
			var totalAttempts int
			for i := 0; i < b.N; i++ {
				count := q.SimulateEventAndSchedule("GPUResourceFit", func(_ *framework.QueuedPodInfo) {})
				totalAttempts += count
			}
			b.StopTimer()
			b.ReportMetric(float64(totalAttempts)/float64(b.N), "avg_attempts/op")
		})

		b.Run(tt.name+"/WithoutHint", func(b *testing.B) {
			q := newMockQueueWithoutHint()
			for i := 0; i < tt.totalPods; i++ {
				var failedPlugins []string
				if i < affectedCount {
					failedPlugins = []string{"GPUResourceFit", "NodeResourcesFit"}
				} else {
					failedPlugins = []string{"NodeResourcesFit"}
				}
				pInfo := makeTestQueuedPodInfo(fmt.Sprintf("pod-%d", i), "default", failedPlugins, nil)
				q.AddPodWithFailure(pInfo)
			}

			b.ResetTimer()
			var totalAttempts int
			for i := 0; i < b.N; i++ {
				count := q.SimulateEventAndSchedule(func(_ *framework.QueuedPodInfo) {})
				totalAttempts += count
			}
			b.StopTimer()
			b.ReportMetric(float64(totalAttempts)/float64(b.N), "avg_attempts/op")
		})
	}
}

// ============================================================================
// Benchmark: 高频事件场景 — 混合失败原因（真实比例）
// ============================================================================

// BenchmarkQueueingHint_MixedFailureScenario 模拟真实场景：
// 10000 个不可调度 Pod，混合了多种失败原因：
//   - 500 个因为 GPU 不足（GPUResourceFit）      ← 真正需要这个事件的
//   - 7000 个因为 CPU/内存不足（NodeResourcesFit） ← 完全不相关
//   - 2500 个因为拓扑约束（NodeAffinity）         ← 完全不相关
//
// 当一个 GPU 释放事件触发时：
//   - WithHint: 只唤醒 GPUResourceFit 的 500 个 Pod
//   - WithoutHint: 唤醒全部 10000 个 Pod（9500 个完全白跑）
func BenchmarkQueueingHint_MixedFailureScenario(b *testing.B) {
	b.Run("WithHint/GPU_Release_Event", func(b *testing.B) {
		q := newMockQueueWithHint()
		for i := 0; i < 500; i++ {
			pInfo := makeTestQueuedPodInfo(fmt.Sprintf("gpu-pod-%d", i), "default", []string{"GPUResourceFit"}, nil)
			q.AddPodWithFailure(pInfo)
		}
		for i := 0; i < 7000; i++ {
			pInfo := makeTestQueuedPodInfo(fmt.Sprintf("cpu-pod-%d", i), "default", []string{"NodeResourcesFit"}, nil)
			q.AddPodWithFailure(pInfo)
		}
		for i := 0; i < 2500; i++ {
			pInfo := makeTestQueuedPodInfo(fmt.Sprintf("topo-pod-%d", i), "default", []string{"NodeAffinity"}, nil)
			q.AddPodWithFailure(pInfo)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.SimulateEventAndSchedule("GPUResourceFit", mockScheduleCost)
		}
	})

	b.Run("WithoutHint/GPU_Release_Event", func(b *testing.B) {
		q := newMockQueueWithoutHint()
		for i := 0; i < 500; i++ {
			pInfo := makeTestQueuedPodInfo(fmt.Sprintf("gpu-pod-%d", i), "default", []string{"GPUResourceFit"}, nil)
			q.AddPodWithFailure(pInfo)
		}
		for i := 0; i < 7000; i++ {
			pInfo := makeTestQueuedPodInfo(fmt.Sprintf("cpu-pod-%d", i), "default", []string{"NodeResourcesFit"}, nil)
			q.AddPodWithFailure(pInfo)
		}
		for i := 0; i < 2500; i++ {
			pInfo := makeTestQueuedPodInfo(fmt.Sprintf("topo-pod-%d", i), "default", []string{"NodeAffinity"}, nil)
			q.AddPodWithFailure(pInfo)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q.SimulateEventAndSchedule(mockScheduleCost)
		}
	})
}

// ============================================================================
// Benchmark: 三级队列 ActiveQ Add + Pop 吞吐
// ============================================================================

// BenchmarkSchedulingQueue_AddAndPop 测试 ActiveQ 的 Add + Pop 吞吐量
// 测量：批量 Add 后，逐个 Pop+Done 消耗的吞吐
func BenchmarkSchedulingQueue_AddAndPop(b *testing.B) {
	tests := []struct {
		name string
		pods int
	}{
		{"100 Pods", 100},
		{"1000 Pods", 1000},
		{"5000 Pods", 5000},
		{"10000 Pods", 10000},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			lessFn := func(p1, p2 *framework.QueuedPodInfo) bool {
				return p1.Timestamp.Before(p2.Timestamp)
			}
			pq := NewPriorityQueue(lessFn, nil)
			logger := zap.NewNop()

			// 预创建 Pod 数组，避免循环内分配
			pods := make([]*corev1.Pod, tt.pods)
			for i := 0; i < tt.pods; i++ {
				pInfo := makeTestQueuedPodInfo(fmt.Sprintf("pod-%d", i), "default", nil, nil)
				pods[i] = pInfo.Pod
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 批量 Add
				for j := 0; j < tt.pods; j++ {
					pq.Add(logger, pods[j])
				}
				// 逐个 Pop + Done，保持队列稳态
				for j := 0; j < tt.pods; j++ {
					if pInfo, _ := pq.Pop(logger); pInfo != nil {
						pq.Done(pInfo.Pod.UID)
					}
				}
			}
		})
	}
}