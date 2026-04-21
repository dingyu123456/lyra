package queue

import (
	"fmt"
	"testing"
)

// ============================================================================
// GPU 集群场景的 QueueingHint 测试
// 模拟真实 GPU 集群：70% GPU 任务 + 30% 非 GPU 任务
// ============================================================================

type gpuClusterPodMeta struct {
	name            string
	isGPUPod        bool
	gpuTypeRequired string // "A100", "V100", "H100", "4090", "AnyGPU", ""(非GPU)
	failedPlugin    string // NodeResourcesFit 或 GPUResourceFit
}

func makeGPUClusterPods() []gpuClusterPodMeta {
	var pods []gpuClusterPodMeta
	// 非 GPU 任务 (300)
	for i := 0; i < 300; i++ {
		pods = append(pods, gpuClusterPodMeta{
			name:         fmt.Sprintf("cpu-pod-%d", i),
			failedPlugin: "NodeResourcesFit",
		})
	}
	// GPU - AnyGPU (210)
	for i := 0; i < 210; i++ {
		pods = append(pods, gpuClusterPodMeta{
			name:            fmt.Sprintf("gpu-any-pod-%d", i),
			isGPUPod:        true,
			gpuTypeRequired: "AnyGPU",
			failedPlugin:    "GPUResourceFit",
		})
	}
	// GPU - A100 (175)
	for i := 0; i < 175; i++ {
		pods = append(pods, gpuClusterPodMeta{
			name:            fmt.Sprintf("gpu-a100-pod-%d", i),
			isGPUPod:        true,
			gpuTypeRequired: "A100",
			failedPlugin:    "GPUResourceFit",
		})
	}
	// GPU - V100 (140)
	for i := 0; i < 140; i++ {
		pods = append(pods, gpuClusterPodMeta{
			name:            fmt.Sprintf("gpu-v100-pod-%d", i),
			isGPUPod:        true,
			gpuTypeRequired: "V100",
			failedPlugin:    "GPUResourceFit",
		})
	}
	// GPU - H100 (105)
	for i := 0; i < 105; i++ {
		pods = append(pods, gpuClusterPodMeta{
			name:            fmt.Sprintf("gpu-h100-pod-%d", i),
			isGPUPod:        true,
			gpuTypeRequired: "H100",
			failedPlugin:    "GPUResourceFit",
		})
	}
	// GPU - 4090 (70)
	for i := 0; i < 70; i++ {
		pods = append(pods, gpuClusterPodMeta{
			name:            fmt.Sprintf("gpu-4090-pod-%d", i),
			isGPUPod:        true,
			gpuTypeRequired: "4090",
			failedPlugin:    "GPUResourceFit",
		})
	}
	return pods
}

// 预生成的测试数据
var gpuClusterTestPods = makeGPUClusterPods()

// BenchmarkQueueingHint_GPUCluster GPU 集群场景下的 QueueingHint 效果对比
// 对比三种过滤级别的唤醒 Pod 数量：
//   - NoFilter: 无过滤，唤醒全部 1000 个 Pod
//   - PluginFilter: 插件级过滤，只唤醒 GPUResourceFit 拒绝的 700 个
//   - GPUTypeFilter: GPU 类型级过滤，只唤醒匹配类型的 Pod
func BenchmarkQueueingHint_GPUCluster(b *testing.B) {
	gpuTypes := []string{"A100", "V100", "H100", "4090"}

	for _, gpuType := range gpuTypes {
		eventName := gpuType + "_Release"

		// NoFilter: 唤醒全部 1000 个 Pod
		b.Run("NoFilter/"+eventName, func(b *testing.B) {
			b.ResetTimer()
			var total int
			for i := 0; i < b.N; i++ {
				total += len(gpuClusterTestPods)
			}
			b.ReportMetric(float64(total)/float64(b.N), "avg_attempts/op")
		})

		// PluginFilter: 只唤醒 GPUResourceFit 拒绝的 700 个
		b.Run("PluginFilter/"+eventName, func(b *testing.B) {
			var gpuPodCount int
			for _, p := range gpuClusterTestPods {
				if p.failedPlugin == "GPUResourceFit" {
					gpuPodCount++
				}
			}
			b.ResetTimer()
			var total int
			for i := 0; i < b.N; i++ {
				total += gpuPodCount
			}
			b.ReportMetric(float64(total)/float64(b.N), "avg_attempts/op")
		})

		// GPUTypeFilter: 只唤醒匹配类型 + AnyGPU 的 Pod
		b.Run("GPUTypeFilter/"+eventName, func(b *testing.B) {
			var matchedCount int
			for _, p := range gpuClusterTestPods {
				if !p.isGPUPod {
					continue
				}
				if p.gpuTypeRequired == gpuType || p.gpuTypeRequired == "AnyGPU" {
					matchedCount++
				}
			}
			b.ResetTimer()
			var total int
			for i := 0; i < b.N; i++ {
				total += matchedCount
			}
			b.ReportMetric(float64(total)/float64(b.N), "avg_attempts/op")
		})
	}
}
