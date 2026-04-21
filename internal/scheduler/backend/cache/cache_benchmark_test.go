package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
)

// ============================================================================
// Benchmark 1: 调度器缓存 UpdateSnapshot 增量快照性能测试
// ============================================================================

// makeTestCache 创建用于 Benchmark 的测试缓存，包含指定数量的集群、节点和 GPU
// clusters: 集群数量
// nodesPerCluster: 每个集群的节点数量
// gpusPerNode: 每个节点的 GPU 数量
func makeTestCache(tb testing.TB, clusters, nodesPerCluster, gpusPerNode int) (*cacheImpl, *Snapshot) {
	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)

	c := newCache(ctx, 30*time.Second, 1*time.Second)
	snapshot := NewEmptySnapshot()

	logger := zap.NewNop()

	// 添加集群和节点
	for ci := 0; ci < clusters; ci++ {
		clusterName := fmt.Sprintf("cluster-%d", ci)
		cluster := &clusterv1alpha1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName},
		}
		// SecretRef is only needed for actual kubeconfig fetching, not for cache testing
		c.AddCluster(logger, cluster)

		for ni := 0; ni < nodesPerCluster; ni++ {
			nodeName := fmt.Sprintf("node-%s-%d", clusterName, ni)
			node := makeTestNode(nodeName, clusterName, gpusPerNode)
			c.AddNode(logger, node)
		}
	}

	return c, snapshot
}

// makeTestNode 创建一个带有 GPU 信息的测试节点
func makeTestNode(nodeName, clusterName string, gpuCount int) *corev1.Node {
	// 生成 GPU 注解 (Hami 格式)
	var gpuAnnotation string
	for i := 0; i < gpuCount; i++ {
		uuid := fmt.Sprintf("GPU-%s-%s-%d", clusterName, nodeName, i)
		// 格式: GPU-UUID,MaxVGPUs,MemoryTotal,CoreTotal,Type:
		gpuAnnotation += fmt.Sprintf("GPU-%s,%d,%d,%d,NVIDIA-A100:", uuid, 1, 40*1024*1024*1024, 100)
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Annotations: map[string]string{
				framework.AnnotationTargetCluster:  clusterName,
				framework.AnnotationNodeNvidiaRegister: gpuAnnotation,
			},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("32"),
				corev1.ResourceMemory:            resource.MustParse("128Gi"),
				framework.ResourceGPU:           resource.MustParse(fmt.Sprintf("%d", gpuCount)),
				framework.ResourceGPUMem:        resource.MustParse(fmt.Sprintf("%d", 40*gpuCount)),
				framework.ResourceGPUCores:      resource.MustParse(fmt.Sprintf("%d", 100*gpuCount)),
			},
		},
	}
	return node
}

// makeTestPod 创建一个分配了 GPU 的测试 Pod
func makeTestPod(podName, clusterName, nodeName string, gpuCount int, gpuUUIDs []string) *corev1.Pod {
	// 生成 Pod GPU 分配注解 (Hami 格式)
	var podGPUAnnotation string
	for _, uuid := range gpuUUIDs {
		// 格式: GPU-UUID,Type,Memory,Core:
		podGPUAnnotation += fmt.Sprintf("%s,NVIDIA-A100,%d,%d:", uuid, 10*1024*1024*1024, 50)
	}
	podGPUAnnotation += ";"

	annotations := map[string]string{
		framework.AnnotationTargetCluster:      clusterName,
		framework.AnnotationPodVGPUAllocated: podGPUAnnotation,
		framework.AnnotationKarmadaNamespace: fmt.Sprintf("karmada-es-%s", clusterName),
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   "default",
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			NodeName:      nodeName,
			SchedulerName: "lyra-scheduler",
			Containers: []corev1.Container{{
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						framework.ResourceGPU:     resource.MustParse(fmt.Sprintf("%d", gpuCount)),
						framework.ResourceGPUMem: resource.MustParse(fmt.Sprintf("%d", 10*gpuCount)),
					},
				},
			}},
		},
	}
}

// ============================================================================
// Benchmark: 快照更新时间 vs 集群/节点规模
// ============================================================================

// BenchmarkSnapshotUpdate_NoChanges 测试稳态（无变更）下的快照更新时间
// 这是最常见的场景：集群稳定，只有少数 Pod 调度/完成
func BenchmarkSnapshotUpdate_NoChanges(b *testing.B) {
	tests := []struct {
		name           string
		clusters       int
		nodesPerCluster int
		gpusPerNode    int
	}{
		{"2 clusters × 10 nodes × 8 GPU", 2, 10, 8},
		{"5 clusters × 20 nodes × 8 GPU", 5, 20, 8},
		{"10 clusters × 50 nodes × 8 GPU", 10, 50, 8},
		{"20 clusters × 100 nodes × 8 GPU", 20, 100, 8},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			logger := zap.NewNop()
			c, snapshot := makeTestCache(&testing.T{}, tt.clusters, tt.nodesPerCluster, tt.gpusPerNode)

			// 先做一次快照同步，建立基准世代号
			c.UpdateSnapshot(logger, snapshot)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.UpdateSnapshot(logger, snapshot)
			}
		})
	}
}

// BenchmarkSnapshotUpdate_OneNodeChange 测试单节点变更时的快照更新时间
// 模拟一个 Pod 调度完成，触发一个节点的资源变更
func BenchmarkSnapshotUpdate_OneNodeChange(b *testing.B) {
	tests := []struct {
		name           string
		clusters       int
		nodesPerCluster int
		gpusPerNode    int
	}{
		{"2 clusters × 10 nodes", 2, 10, 8},
		{"5 clusters × 20 nodes", 5, 20, 8},
		{"10 clusters × 50 nodes", 10, 50, 8},
		{"20 clusters × 100 nodes", 20, 100, 8},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			logger := zap.NewNop()
			c, snapshot := makeTestCache(&testing.T{}, tt.clusters, tt.nodesPerCluster, tt.gpusPerNode)

			// 初始快照同步
			c.UpdateSnapshot(logger, snapshot)

			// 模拟一个 Pod 调度完成，变更第一个节点的资源
			pod := makeTestPod("test-pod", "cluster-0", "node-cluster-0-0", 1, []string{"GPU-cluster-0-node-cluster-0-0-0"})
			c.AddPod(logger, pod)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.UpdateSnapshot(logger, snapshot)
			}
		})
	}
}

// BenchmarkSnapshotUpdate_AllNodesChange 测试所有节点同时变更的场景
// 模拟大量 Pod 同时调度完成
func BenchmarkSnapshotUpdate_AllNodesChange(b *testing.B) {
	tests := []struct {
		name           string
		clusters       int
		nodesPerCluster int
		gpusPerNode    int
		podsPerNode    int
	}{
		{"2 clusters × 10 nodes × 2 pods", 2, 10, 8, 2},
		{"5 clusters × 20 nodes × 2 pods", 5, 20, 8, 2},
		{"10 clusters × 50 nodes × 2 pods", 10, 50, 8, 2},
		{"20 clusters × 100 nodes × 2 pods", 20, 100, 8, 2},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			logger := zap.NewNop()
			c, snapshot := makeTestCache(&testing.T{}, tt.clusters, tt.nodesPerCluster, tt.gpusPerNode)

			c.UpdateSnapshot(logger, snapshot)

			// 在每个节点上添加 Pod，触发所有节点变更
			for ci := 0; ci < tt.clusters; ci++ {
				clusterName := fmt.Sprintf("cluster-%d", ci)
				for ni := 0; ni < tt.nodesPerCluster; ni++ {
					nodeName := fmt.Sprintf("node-%s-%d", clusterName, ni)
					for pi := 0; pi < tt.podsPerNode; pi++ {
						gpuUUID := fmt.Sprintf("GPU-%s-%s-%d", clusterName, nodeName, 0)
						pod := makeTestPod(fmt.Sprintf("pod-%s-%d", nodeName, pi), clusterName, nodeName, 1, []string{gpuUUID})
						c.AddPod(logger, pod)
					}
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.UpdateSnapshot(logger, snapshot)
			}
		})
	}
}

// ============================================================================
// Benchmark: 缓存 Assume/Forget 性能测试
// ============================================================================

// BenchmarkCache_AssumeAndForget 测试预扣和回滚的性能
func BenchmarkCache_AssumeAndForget(b *testing.B) {
	logger := zap.NewNop()
	c, _ := makeTestCache(b, 5, 20, 8)

	// 预创建 Pod（使用固定数据集，避免 O(b.N) 内存分配）
	const podCount = 1000
	pods := make([]*corev1.Pod, podCount)
	for i := 0; i < podCount; i++ {
		clusterIdx := i % 5
		nodeIdx := i % 20
		clusterName := fmt.Sprintf("cluster-%d", clusterIdx)
		nodeName := fmt.Sprintf("node-%s-%d", clusterName, nodeIdx)
		gpuUUID := fmt.Sprintf("GPU-%s-%s-0", clusterName, nodeName)
		pod := makeTestPod(fmt.Sprintf("pod-%d", i), clusterName, nodeName, 1, []string{gpuUUID})
		pods[i] = pod
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % podCount
		c.AssumePod(logger, pods[idx])
		c.ForgetPod(logger, pods[idx])
	}
}

// ============================================================================
// Benchmark: 缓存 AddPod（转正/对账）性能测试
// ============================================================================

// BenchmarkCache_AddPod_Transition 测试从 Assumed 到 Real 的转正性能
func BenchmarkCache_AddPod_Transition(b *testing.B) {
	logger := zap.NewNop()
	c, _ := makeTestCache(b, 5, 20, 8)

	const podCount = 1000
	pods := make([]*corev1.Pod, podCount)
	for i := 0; i < podCount; i++ {
		clusterIdx := i % 5
		nodeIdx := i % 20
		clusterName := fmt.Sprintf("cluster-%d", clusterIdx)
		nodeName := fmt.Sprintf("node-%s-%d", clusterName, nodeIdx)
		gpuUUID := fmt.Sprintf("GPU-%s-%s-0", clusterName, nodeName)
		pod := makeTestPod(fmt.Sprintf("pod-%d", i), clusterName, nodeName, 1, []string{gpuUUID})
		pods[i] = pod
	}

	// 预扣
	for i := 0; i < podCount; i++ {
		c.AssumePod(logger, pods[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % podCount
		c.AddPod(logger, pods[idx])
	}
}

// ============================================================================
// 对比测试: 增量快照 vs 全量克隆（手动实现）
// ============================================================================

// fullCloneSnapshot 手动实现全量克隆（模拟没有增量优化时的方案）
func fullCloneSnapshot(c *cacheImpl, s *Snapshot) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 清空旧快照
	s.clusterInfoMap = make(map[string]*framework.ClusterInfo)
	s.clusterInfoList = make([]*framework.ClusterInfo, 0, len(c.clusters))

	for _, ci := range c.clusters {
		clone := &framework.ClusterInfo{
			ClusterName: ci.info.ClusterName,
			Generation:  ci.info.Generation,
			Nodes:       make(map[string]*framework.NodeInfo, len(ci.nodes)),
		}
		if ci.info.Allocatable != nil {
			clone.Allocatable = ci.info.Allocatable.Clone()
		}
		if ci.info.Requested != nil {
			clone.Requested = ci.info.Requested.Clone()
		}
		if ci.info.Cluster() != nil {
			clone.SetCluster(ci.info.Cluster())
		}

		for _, ni := range ci.nodes {
			clone.Nodes[ni.info.NodeName] = ni.info.Snapshot()
		}

		s.clusterInfoMap[clone.ClusterName] = clone
		s.clusterInfoList = append(s.clusterInfoList, clone)
	}
}

// BenchmarkCompare_IncrementalVsFullCopy 对比增量快照 vs 全量克隆
func BenchmarkCompare_IncrementalVsFullCopy(b *testing.B) {
	tests := []struct {
		name           string
		clusters       int
		nodesPerCluster int
		gpusPerNode    int
	}{
		{"5 clusters × 20 nodes × 8 GPU", 5, 20, 8},
		{"10 clusters × 50 nodes × 8 GPU", 10, 50, 8},
		{"20 clusters × 100 nodes × 8 GPU", 20, 100, 8},
	}

	for _, tt := range tests {
		b.Run(tt.name+"/Incremental", func(b *testing.B) {
			logger := zap.NewNop()
			c, snapshot := makeTestCache(&testing.T{}, tt.clusters, tt.nodesPerCluster, tt.gpusPerNode)
			c.UpdateSnapshot(logger, snapshot)

			// 只变更一个节点
			pod := makeTestPod("test-pod", "cluster-0", "node-cluster-0-0", 1, []string{"GPU-cluster-0-node-cluster-0-0-0"})
			c.AddPod(logger, pod)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.UpdateSnapshot(logger, snapshot)
			}
		})

		b.Run(tt.name+"/FullCopy", func(b *testing.B) {
			logger := zap.NewNop()
			c, snapshot := makeTestCache(&testing.T{}, tt.clusters, tt.nodesPerCluster, tt.gpusPerNode)
			c.UpdateSnapshot(logger, snapshot)

			// 只变更一个节点
			pod := makeTestPod("test-pod", "cluster-0", "node-cluster-0-0", 1, []string{"GPU-cluster-0-node-cluster-0-0-0"})
			c.AddPod(logger, pod)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fullCloneSnapshot(c, snapshot)
			}
		})
	}
}

// ============================================================================
// 集成测试: 模拟真实调度周期
// ============================================================================

// BenchmarkSchedulingCycle_Complete 模拟完整的调度周期
// Pod 入队 -> Pop -> 快照更新 -> Assume -> 下发 -> AddPod 转正
func BenchmarkSchedulingCycle_Complete(b *testing.B) {
	logger := zap.NewNop()
	c, snapshot := makeTestCache(b, 5, 20, 8)
	c.UpdateSnapshot(logger, snapshot)

	// 模拟 100 个并发调度
	pods := make([]*corev1.Pod, 100)
	for i := 0; i < 100; i++ {
		clusterName := fmt.Sprintf("cluster-%d", i%5)
		nodeName := fmt.Sprintf("node-%s-%d", clusterName, i%20)
		gpuUUID := fmt.Sprintf("GPU-%s-%s-0", clusterName, nodeName)
		pods[i] = makeTestPod(fmt.Sprintf("pod-%d", i), clusterName, nodeName, 1, []string{gpuUUID})
	}

	var wg sync.WaitGroup
	throughput := make(chan int, 100)

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pod := pods[idx]

			// 调度周期
			c.AssumePod(logger, pod)
			c.UpdateSnapshot(logger, snapshot)
			c.AddPod(logger, pod)

			select {
			case throughput <- idx:
			default:
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()

	b.ReportMetric(float64(elapsed.Milliseconds()), "total_ms")
	b.ReportMetric(float64(100*1000/elapsed.Milliseconds()), "pods_per_second")
}