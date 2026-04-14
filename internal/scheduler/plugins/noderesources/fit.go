package noderesources

import (
	"context"
	"fmt"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const Name = "NodeResourcesFit"

const (
	preFilterStateKey = "PreFilter" + Name
)

var _ framework.PreFilterPlugin = &Fit{}
var _ framework.FilterPlugin = &Fit{}
var _ framework.ScorePlugin = &Fit{}
var _ framework.EnqueueExtensions = &Fit{}

// Fit 是基础资源过滤和打分插件 (CPU, Memory, Pods)
type Fit struct {
	handle framework.Handle
}

// New 初始化插件
func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &Fit{
		handle: h,
	}, nil
}

func (f *Fit) Name() string {
	return Name
}

// -------------------------------------------------------------------
// 状态数据结构
// -------------------------------------------------------------------

// preFilterState 存储 Pod 对 CPU 和内存的总需求
type preFilterState struct {
	MilliCPU int64
	Memory   int64
}

func (s *preFilterState) Clone() framework.StateData {
	if s == nil {
		return nil
	}
	return &preFilterState{
		MilliCPU: s.MilliCPU,
		Memory:   s.Memory,
	}
}

// computePodResourceRequest 计算 Pod 的真实资源请求总量
// 逻辑：InitContainers 是串行执行的，取最大值；普通 Containers 是并行执行的，取累加和。最终取两者的最大值。
func computePodResourceRequest(pod *corev1.Pod) *preFilterState {
	reqs := &preFilterState{}

	// 1. 计算普通容器的累加和
	var podReqMilliCPU, podReqMemory int64
	for _, container := range pod.Spec.Containers {
		podReqMilliCPU += container.Resources.Requests.Cpu().MilliValue()
		podReqMemory += container.Resources.Requests.Memory().Value()
	}

	// 2. 遍历 InitContainers，取其中的单体最大值，并与普通容器总和做比较
	reqs.MilliCPU = podReqMilliCPU
	reqs.Memory = podReqMemory

	for _, container := range pod.Spec.InitContainers {
		initMilliCPU := container.Resources.Requests.Cpu().MilliValue()
		initMemory := container.Resources.Requests.Memory().Value()
		if initMilliCPU > reqs.MilliCPU {
			reqs.MilliCPU = initMilliCPU
		}
		if initMemory > reqs.Memory {
			reqs.Memory = initMemory
		}
	}

	return reqs
}

// -------------------------------------------------------------------
// PreFilter 阶段
// -------------------------------------------------------------------

func (f *Fit) PreFilter(ctx context.Context, cycleState *framework.CycleState, pod *corev1.Pod) (*framework.PreFilterResult, *framework.Status) {
	// 计算并缓存 Pod 的资源请求，供后续 Filter 和 Score 极速读取
	cycleState.Write(preFilterStateKey, computePodResourceRequest(pod))
	return nil, framework.NewStatus(framework.Success)
}

func (f *Fit) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// -------------------------------------------------------------------
// Filter 阶段
// -------------------------------------------------------------------

func (f *Fit) Filter(ctx context.Context, cycleState *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	data, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		return framework.AsStatus(fmt.Errorf("error reading %q from cycleState: %w", preFilterStateKey, err))
	}
	req := data.(*preFilterState)

	// 1. 校验 Pod 数量上限
	if len(nodeInfo.Pods)+1 > nodeInfo.Allocatable.AllowedPodNumber {
		return framework.NewStatus(framework.Unschedulable, "Too many pods")
	}

	// 2. 校验 CPU
	if req.MilliCPU > 0 {
		freeCPU := nodeInfo.Allocatable.MilliCPU - nodeInfo.Requested.MilliCPU
		if req.MilliCPU > freeCPU {
			return framework.NewStatus(framework.Unschedulable, "Insufficient cpu")
		}
	}

	// 3. 校验 Memory
	if req.Memory > 0 {
		freeMemory := nodeInfo.Allocatable.Memory - nodeInfo.Requested.Memory
		if req.Memory > freeMemory {
			return framework.NewStatus(framework.Unschedulable, "Insufficient memory")
		}
	}

	return framework.NewStatus(framework.Success)
}

// -------------------------------------------------------------------
// Score 阶段 (采用 LeastAllocated 算法)
// -------------------------------------------------------------------

// Score 评估节点的 CPU/Memory 负载，负载越低得分越高，促使普通任务在节点间均匀分布
// 🌟 注意：这里直接使用了我们之前优化的 *framework.NodeInfo 签名
func (f *Fit) Score(ctx context.Context, cycleState *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) (int64, *framework.Status) {
	data, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		return 0, framework.AsStatus(fmt.Errorf("error reading %q from cycleState: %w", preFilterStateKey, err))
	}
	req := data.(*preFilterState)

	var cpuScore, memScore int64

	// 计算 CPU 得分
	if nodeInfo.Allocatable.MilliCPU > 0 {
		cpuFraction := float64(nodeInfo.Requested.MilliCPU+req.MilliCPU) / float64(nodeInfo.Allocatable.MilliCPU)
		if cpuFraction > 1 {
			cpuFraction = 1
		}
		cpuScore = int64((1 - cpuFraction) * float64(framework.MaxNodeScore))
	} else {
		cpuScore = 0
	}

	// 计算 Memory 得分
	if nodeInfo.Allocatable.Memory > 0 {
		memFraction := float64(nodeInfo.Requested.Memory+req.Memory) / float64(nodeInfo.Allocatable.Memory)
		if memFraction > 1 {
			memFraction = 1
		}
		memScore = int64((1 - memFraction) * float64(framework.MaxNodeScore))
	} else {
		memScore = 0
	}

	// 取 CPU 和 Memory 得分的平均值作为基础资源最终得分
	finalScore := (cpuScore + memScore) / 2

	return finalScore, framework.NewStatus(framework.Success)
}

func (f *Fit) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// -------------------------------------------------------------------
// EnqueueExtensions (智能唤醒机制)
// -------------------------------------------------------------------

func (f *Fit) EventsToRegister(_ context.Context) ([]framework.ClusterEventWithHint, error) {
	return []framework.ClusterEventWithHint{
		{
			Event:          framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.Delete},
			QueueingHintFn: f.isSchedulableAfterPodEvent,
		},
		{
			Event:          framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Add | framework.UpdateNodeAllocatable},
			QueueingHintFn: f.isSchedulableAfterNodeChange,
		},
	}, nil
}

func (f *Fit) isSchedulableAfterPodEvent(logger *zap.Logger, pod *corev1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	if newObj == nil { // Delete event
		deletedPod, ok := oldObj.(*corev1.Pod)
		if !ok {
			return framework.Queue, nil
		}
		if deletedPod.Spec.NodeName == "" {
			// 未调度的 Pod 删除不会释放资源
			return framework.QueueSkip, nil
		}
		// 已调度的 Pod 删除会释放 CPU/Memory
		return framework.Queue, nil
	}
	return framework.QueueSkip, nil
}

func (f *Fit) isSchedulableAfterNodeChange(logger *zap.Logger, pod *corev1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	originalNode, _ := oldObj.(*corev1.Node)
	modifiedNode, ok := newObj.(*corev1.Node)
	if !ok || modifiedNode == nil {
		return framework.Queue, nil
	}

	if originalNode == nil {
		// 新增节点，可能可以调度
		return framework.Queue, nil
	}

	// 对于更新事件，简单起见，只要可分配资源发生变化就尝试唤醒
	return framework.Queue, nil
}
