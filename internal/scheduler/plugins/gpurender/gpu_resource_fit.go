package gpurender // 或者你的包名

import (
	"context"
	"fmt"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const Name = "GPUResourceFit"

type GPUResourceFit struct {
	handle framework.Handle
}

var _ framework.PreFilterPlugin = &GPUResourceFit{}
var _ framework.FilterPlugin = &GPUResourceFit{}
var _ framework.ScorePlugin = &GPUResourceFit{}

func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &GPUResourceFit{handle: h}, nil
}

func (g *GPUResourceFit) Name() string {
	return Name
}

// -------------------------------------------------------------------
// PreFilter 阶段
// -------------------------------------------------------------------
func (g *GPUResourceFit) PreFilter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod) (*framework.PreFilterResult, *framework.Status) {
	// 1. 调用你现有的解析方法 (返回的是值类型)
	req := framework.ParsePodGPUReqs(pod)

	// 2. 🌟 修复点 1：传入取地址符 &req，使其满足指针接收者的 StateData 接口
	state.Write(framework.KeyPodGPUReq, &req)

	if req.NumCards == 0 {
		return nil, framework.NewStatus(framework.Skip)
	}

	return nil, framework.NewStatus(framework.Success)
}

func (g *GPUResourceFit) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// -------------------------------------------------------------------
// Filter 阶段
// -------------------------------------------------------------------
func (g *GPUResourceFit) Filter(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	data, err := state.Read(framework.KeyPodGPUReq)
	if err != nil {
		return framework.AsStatus(fmt.Errorf("reading cycle state: %w", err))
	}
	req := data.(*framework.GPURequirement)

	if req.NumCards == 0 {
		return framework.NewStatus(framework.Success)
	}

	availableCards := 0
	for _, gpu := range nodeInfo.GPUs {
		if len(req.Types) > 0 {
			typeMatched := false
			for _, allowedType := range req.Types {
				if gpu.Type == allowedType {
					typeMatched = true
					break
				}
			}
			if !typeMatched {
				continue
			}
		}

		freeMem := gpu.AllocatableMem - gpu.RequestedMem
		freeCore := gpu.AllocatableCore - gpu.RequestedCore

		if freeMem >= req.MemReq && freeCore >= req.CoreReq {
			availableCards++
		}
	}

	if availableCards < req.NumCards {
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("Insufficient vGPU resources: need %d, available %d", req.NumCards, availableCards))
	}

	return framework.NewStatus(framework.Success)
}

// -------------------------------------------------------------------
// Score 阶段
// -------------------------------------------------------------------
// 🌟 修复点 3：签名直接使用 nodeInfo *framework.NodeInfo
func (g *GPUResourceFit) Score(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) (int64, *framework.Status) {
	data, _ := state.Read(framework.KeyPodGPUReq)
	req := data.(*framework.GPURequirement)

	// 🌟 修复点 2：使用 framework.MaxNodeScore
	if req.NumCards == 0 {
		return framework.MaxNodeScore, framework.NewStatus(framework.Success)
	}

	var bestFitScore int64 = 0

	for _, gpu := range nodeInfo.GPUs {
		freeMem := gpu.AllocatableMem - gpu.RequestedMem
		freeCore := gpu.AllocatableCore - gpu.RequestedCore

		if freeMem >= req.MemReq && freeCore >= req.CoreReq {
			remMemRatio := float64(freeMem-req.MemReq) / float64(gpu.AllocatableMem)
			remCoreRatio := float64(freeCore-req.CoreReq) / float64(gpu.AllocatableCore)

			// 归一化得分算法：残差比例越小，得分越高，映射到 0 ~ MaxNodeScore
			memScore := int64((1.0 - remMemRatio) * float64(framework.MaxNodeScore/2))
			coreScore := int64((1.0 - remCoreRatio) * float64(framework.MaxNodeScore/2))

			cardScore := memScore + coreScore
			if cardScore > bestFitScore {
				bestFitScore = cardScore
			}
		}
	}

	return bestFitScore, framework.NewStatus(framework.Success)
}

func (g *GPUResourceFit) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// 确保插件实现了 EnqueueExtensions 接口
var _ framework.EnqueueExtensions = &GPUResourceFit{}

// EventsToRegister 注册可能会让之前因为 GPU 不足而失败的 Pod 重新变得可调度的事件。
func (g *GPUResourceFit) EventsToRegister(ctx context.Context) ([]framework.ClusterEventWithHint, error) {
	// 1. Pod 动作：只有 Pod 被删除 (资源释放) 时，才有可能腾出 GPU 供其他任务使用
	// 注：我们不支持 Pod 的原地垂直缩容，所以不需要监听 UpdatePodScaleDown
	podActionType := framework.Delete

	// 2. Node 动作：节点新增，或者节点的 GPU 注解发生变化时
	// 注：HAMI GPU 信息存储在 hami.io/node-nvidia-register 注解中，需要监听 UpdateNodeAnnotation
	nodeActionType := framework.Add | framework.UpdateNodeAllocatable | framework.UpdateNodeAnnotation

	return []framework.ClusterEventWithHint{
		{
			Event:          framework.ClusterEvent{Resource: framework.Pod, ActionType: podActionType},
			QueueingHintFn: g.isSchedulableAfterPodEvent,
		},
		{
			Event:          framework.ClusterEvent{Resource: framework.Node, ActionType: nodeActionType},
			QueueingHintFn: g.isSchedulableAfterNodeChange,
		},
	}, nil
}

// isSchedulableAfterPodEvent 当有 Pod 发生事件 (主要是被删除) 时被调用。
// 它用来判断：这个 Pod 的消失，会不会腾出 GPU 资源？
func (g *GPUResourceFit) isSchedulableAfterPodEvent(logger *zap.Logger, pod *corev1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	// 如果 newObj 为 nil，说明这是一个 Delete 事件 (被删除的 Pod 放在 oldObj 中)
	if newObj == nil {
		deletedPod, ok := oldObj.(*corev1.Pod)
		if !ok {
			return framework.Queue, nil // 解析失败时安全兜底，宁可错杀不可放过
		}

		// 🌟 GPU 核心过滤逻辑：
		// 如果这个被删除的 Pod 压根就没有被调度到任何节点上 (NodeName 为空)，
		// 那么它的死活根本不会释放任何物理资源！直接 QueueSkip，继续让等算力的 Pod 睡觉。
		if deletedPod.Spec.NodeName == "" {
			logger.Debug("Deleted pod was unscheduled, won't free up GPU, skipping", zap.String("deletedPod", deletedPod.Name))
			return framework.QueueSkip, nil
		}

		// 如果它已经调度了，那它的删除极大概率会释放出 vGPU 显存/算力！
		// 立刻通知调度队列：赶紧把那些卡在 Unschedulable 池子里的 AI 任务唤醒！
		logger.Debug("A scheduled pod was deleted, it may free up GPU resources", zap.String("deletedPod", deletedPod.Name))
		return framework.Queue, nil
	}

	// 对于非 Delete 事件，保守起见直接 Skip
	return framework.QueueSkip, nil
}

// isSchedulableAfterNodeChange 当节点发生变化 (新增或资源更新) 时被调用。
// 它用来判断：这个新节点/新资源的出现，能否装下这口饭？
func (g *GPUResourceFit) isSchedulableAfterNodeChange(logger *zap.Logger, pod *corev1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	originalNode, _ := oldObj.(*corev1.Node)
	modifiedNode, ok := newObj.(*corev1.Node)
	if !ok || modifiedNode == nil {
		return framework.Queue, nil
	}

	// 场景 A: 这是一个全新加入集群的节点！它必然带着新鲜的 GPU 算力。
	// 立刻唤醒重试！
	if originalNode == nil {
		logger.Debug("A new node joined the cluster, unblocking GPU scheduling", zap.String("node", modifiedNode.Name))
		return framework.Queue, nil
	}

	// 场景 B: 节点发生了更新 (Update)
	// 检查 Pod 的 GPU 类型要求和节点 GPU 类型是否匹配
	podReq := framework.ParsePodGPUReqs(pod)

	// 如果 Pod 没有指定 GPU 类型要求（任何 GPU 都可以），唤醒
	if len(podReq.Types) == 0 {
		logger.Debug("Pod has no GPU type requirement, triggering retry", zap.String("pod", pod.Name))
		return framework.Queue, nil
	}

	// 如果 Pod 指定了 GPU 类型，解析节点的 GPU 类型
	nodeGPUs, err := framework.ParseNodeHamiAnnotation(modifiedNode)
	if err != nil || nodeGPUs == nil {
		// 节点没有 GPU 或解析失败，保守唤醒
		logger.Debug("Node has no GPU or parse failed, triggering retry", zap.String("node", modifiedNode.Name))
		return framework.Queue, nil
	}

	// 收集节点支持的 GPU 类型
	nodeTypes := make(map[string]bool)
	for _, gpu := range nodeGPUs {
		nodeTypes[gpu.Type] = true
	}

	// 检查 Pod 的 GPU 类型要求是否能被节点满足
	for _, requiredType := range podReq.Types {
		if nodeTypes[requiredType] {
			// 节点有这个类型的 GPU，Pod 可能可以调度
			logger.Debug("Node has matching GPU type, triggering retry",
				zap.String("pod", pod.Name),
				zap.String("requiredType", requiredType),
				zap.String("node", modifiedNode.Name))
			return framework.Queue, nil
		}
	}

	// 节点没有 Pod 需要的 GPU 类型，不唤醒
	logger.Debug("Node GPU type doesn't match pod requirement, skipping",
		zap.String("pod", pod.Name),
		zap.Strings("requiredTypes", podReq.Types),
		zap.String("node", modifiedNode.Name))
	return framework.QueueSkip, nil
}
