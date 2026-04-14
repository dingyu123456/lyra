package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/dingyu123456/lyra/internal/scheduler/backend/queue"
	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	karmadav1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// prepareNode 负责对原生 Node 进行深度拷贝并注入集群身份信息
// 这样后续的所有 Handler 都可以直接从对象中通过注解获取集群名
func (sched *Scheduler) prepareNode(clusterName string, obj interface{}) (*corev1.Node, error) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil, fmt.Errorf("cannot convert to *v1.Node: %v", obj)
	}

	node = node.DeepCopy()
	if node.Annotations == nil {
		node.Annotations = make(map[string]string)
	}
	// 注入集群标签，这是实现多集群路由的“锚点”
	node.Annotations[framework.AnnotationTargetCluster] = clusterName
	return node, nil
}

// preparePod 负责对原生 Pod 进行深度拷贝并注入集群身份信息
func (sched *Scheduler) preparePod(clusterName string, obj interface{}) (*corev1.Pod, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("cannot convert to *v1.Pod: %v", obj)
	}

	pod = pod.DeepCopy()
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	// 注入集群标签，供后续 Cache 路由使用
	pod.Annotations[framework.AnnotationTargetCluster] = clusterName
	return pod, nil
}

func (sched *Scheduler) addNodeToCache(obj interface{}) {
	evt := framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Add}
	logger := sched.logger // 从 sched 获取统一的 logger

	node, ok := obj.(*corev1.Node)
	if !ok {
		logger.Error("Cannot convert to *v1.Node", zap.Any("obj", obj))
		return
	}

	// 此时 node 已经被染色过，可以直接解析
	logger.Debug("Add event for node", zap.String("node", node.Name))

	// 1. 调用我们之前写的多集群 Cache 方法
	nodeInfo := sched.Cache.AddNode(logger, node)

	// 2. 唤醒队列中可能适合该节点的 Pod
	// preCheck 为 nil (遵循你之前提到的 QHint 演进思路)
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, nil, node, preCheckForNode(nodeInfo))
}

func (sched *Scheduler) updateNodeInCache(oldObj, newObj interface{}) {
	logger := sched.logger
	oldNode, ok := oldObj.(*corev1.Node)
	if !ok {
		return
	}
	newNode, ok := newObj.(*corev1.Node)
	if !ok {
		return
	}

	logger.Debug("Update event for node", zap.String("node", newNode.Name))

	// 1. 更新缓存（内部处理一减一加的算力重算）
	nodeInfo := sched.Cache.UpdateNode(logger, oldNode, newNode)

	// 2. 识别属性变化（包含 Hami GPU 资源变更识别）
	events := framework.NodeSchedulingPropertiesChange(newNode, oldNode)

	// 3. 广播唤醒
	for _, evt := range events {
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, oldNode, newNode, preCheckForNode(nodeInfo))
	}
}

func (sched *Scheduler) deleteNodeFromCache(obj interface{}) {
	evt := framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Delete}
	logger := sched.logger

	var node *corev1.Node
	switch t := obj.(type) {
	case *corev1.Node:
		node = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		node, ok = t.Obj.(*corev1.Node)
		if !ok {
			logger.Error("Cannot convert to *v1.Node in DeletedFinalStateUnknown", zap.Any("obj", t.Obj))
			return
		}
	default:
		logger.Error("Unknown object type in deleteNodeFromCache", zap.Any("obj", obj))
		return
	}

	logger.Info("Delete event for node", zap.String("node", node.Name))

	// 1. 尝试清空队列相关状态
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, node, nil, nil)

	// 2. 执行缓存移除逻辑
	if err := sched.Cache.RemoveNode(logger, node); err != nil {
		logger.Error("Scheduler cache RemoveNode failed", zap.Error(err))
	}
}

func (sched *Scheduler) addPodToCache(obj interface{}) {
	logger := sched.logger
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		logger.Error("Cannot convert to *v1.Pod", zap.Any("obj", obj))
		return
	}

	// 只有已经分配节点的 Pod 才需要进入 Cache 记账
	if !assignedPod(pod) {
		return
	}

	logger.Debug("Add event for scheduled pod", zap.String("pod", pod.Name), zap.String("node", pod.Spec.NodeName))

	// 1. 记账：更新 NodeInfo 里的 Requested 资源和 GPU 占用
	if err := sched.Cache.AddPod(logger, pod); err != nil {
		logger.Error("Scheduler cache AddPod failed", zap.Error(err), zap.String("pod", pod.Name))
	}

	// 2. 唤醒：新 Pod 的成功运行可能触发其他 Pod 的关联逻辑（如亲和性满足）
	// 遵循 QHints 趋势，preCheck 传 nil
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodAdd, nil, pod, nil)
}

func (sched *Scheduler) updatePodInCache(oldObj, newObj interface{}) {
	logger := sched.logger
	oldPod, ok := oldObj.(*corev1.Pod)
	if !ok {
		return
	}
	newPod, ok := newObj.(*corev1.Pod)
	if !ok {
		return
	}

	// 如果新旧 Pod 都没有分配节点，则不属于 Cache 管辖范围
	if !assignedPod(oldPod) && !assignedPod(newPod) {
		return
	}

	logger.Debug("Update event for scheduled pod", zap.String("pod", newPod.Name))

	// 1. 更新缓存账本（内部处理资源一减一加）
	if err := sched.Cache.UpdatePod(logger, oldPod, newPod); err != nil {
		logger.Error("Scheduler cache UpdatePod failed", zap.Error(err), zap.String("pod", newPod.Name))
	}

	// 2. 识别属性变化（如 Pod 优先级、调度要求等）
	events := framework.PodSchedulingPropertiesChange(newPod, oldPod)

	// 3. 广播唤醒
	for _, evt := range events {
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, evt, oldPod, newPod, nil)
	}
}

func (sched *Scheduler) deletePodFromCache(obj interface{}) {
	logger := sched.logger
	var pod *corev1.Pod

	switch t := obj.(type) {
	case *corev1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*corev1.Pod)
		if !ok {
			logger.Error("Cannot convert to *v1.Pod in DeletedFinalStateUnknown", zap.Any("obj", t.Obj))
			return
		}
	default:
		logger.Error("Unknown object type in deletePodFromCache", zap.Any("obj", obj))
		return
	}

	// 只有已分配过资源的 Pod 在删除时才需要归还账本
	if !assignedPod(pod) {
		return
	}

	logger.Info("Delete event for scheduled pod", zap.String("pod", pod.Name), zap.String("node", pod.Spec.NodeName))

	// 1. 归还算力：从 NodeInfo.Requested 中扣除
	if err := sched.Cache.RemovePod(logger, pod); err != nil {
		logger.Error("Scheduler cache RemovePod failed", zap.Error(err), zap.String("pod", pod.Name))
	}

	// 2. 重大唤醒：资源释放是 Unschedulable Pods “复活”的最核心动力
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodDelete, pod, nil, nil)
}

// assignedPod 检查 Pod 是否已经完成调度（即已绑定节点）
func assignedPod(pod *corev1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

// responsibleForPod 判断该 Pod 是否归属于 Lyra 调度器管辖
// 只有 SchedulerName 匹配的任务才会进入我们的调度队列
func (sched *Scheduler) responsibleForPod(pod *corev1.Pod) bool {
	// 假设你的配置文件中定义了调度器名称，默认为 "lyra-scheduler"
	return pod.Spec.SchedulerName == sched.Name
}

// preCheckForNode 顺应 K8s 演进趋势，直接返回 nil，让插件逻辑决定是否入队
func preCheckForNode(nodeInfo *framework.NodeInfo) queue.PreEnqueueCheck {
	return nil
}

func (sched *Scheduler) addPodToSchedulingQueue(obj interface{}) {
	logger := sched.logger
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		logger.Error("Cannot convert to *v1.Pod", zap.Any("obj", obj))
		return
	}

	logger.Debug("Add event for unscheduled pod", zap.String("pod", pod.Name), zap.String("namespace", pod.Namespace))

	// 调用我们之前写好的 PriorityQueue 的 Add 方法
	sched.SchedulingQueue.Add(logger, pod)
}

func (sched *Scheduler) updatePodInSchedulingQueue(oldObj, newObj interface{}) {
	logger := sched.logger
	oldPod, ok := oldObj.(*corev1.Pod)
	if !ok {
		return
	}
	newPod, ok := newObj.(*corev1.Pod)
	if !ok {
		return
	}

	// 1. 防御：资源版本一致，说明是系统内部的重发事件，直接忽略
	// 否则相同的 Pod 被重复塞入调度流程，会导致不可预期的抢占和雪崩
	if oldPod.ResourceVersion == newPod.ResourceVersion {
		return
	}

	// 2. 核心状态屏障：检查 Pod 是否已经被“预扣除”（Assumed）
	// 如果 isAssumed 为 true，说明调度算法已经为它选好了节点，正在等待 Karmada 下发 PP/OP。
	// 此时 APIServer 还没完全确认，但调度器已经不管它了，所以绝对不能再把它放回待调度队列！
	isAssumed, err := sched.Cache.IsAssumedPod(newPod)
	if err != nil {
		logger.Error("Failed to check whether pod is assumed",
			zap.Error(err),
			zap.String("pod", newPod.Name))
	}
	if isAssumed {
		logger.Debug("Pod is already assumed, bypassing queue update", zap.String("pod", newPod.Name))
		return
	}

	logger.Debug("Update event for unscheduled pod", zap.String("pod", newPod.Name))

	// 3. 更新队列中的对象位置（可能会因为 Priority 变化而重排）
	sched.SchedulingQueue.Update(logger, oldPod, newPod)
}

func (sched *Scheduler) deletePodFromSchedulingQueue(obj interface{}) {
	logger := sched.logger
	var pod *corev1.Pod

	switch t := obj.(type) {
	case *corev1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*corev1.Pod)
		if !ok {
			logger.Error("Cannot convert to *v1.Pod in DeletedFinalStateUnknown", zap.Any("obj", t.Obj))
			return
		}
	default:
		logger.Error("Unknown object type in deletePodFromSchedulingQueue", zap.Any("obj", obj))
		return
	}

	logger.Info("Delete event for unscheduled pod", zap.String("pod", pod.Name))

	// 1. 从等待队列中彻底删除
	sched.SchedulingQueue.Delete(pod)
	// 2. 调度框架层面的清理 (Permit 阶段清理)
	// 如果你的 Framework 实现了 RejectWaitingPod（比如处理那些在 Bind 阶段之前等待的 Pod）
	if sched.Framework != nil {
		// 如果有一个等待中的 Pod 被用户强制删除了（被 Reject），
		// 它原本可能锁定了某些资源，现在释放了，因此触发一次唤醒广播！
		if sched.Framework.RejectWaitingPod(pod.UID) {
			sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(logger, framework.EventAssignedPodDelete, pod, nil, nil)
		}
	}
}

const (
	// syncedPollPeriod controls how often you look at the status of your sync funcs
	syncedPollPeriod = 100 * time.Millisecond
)

// WaitForHandlersSync waits for EventHandlers to sync.
// It returns true if it was successful, false if the controller should shut down
func (sched *Scheduler) WaitForHandlersSync(ctx context.Context) error {
	return wait.PollUntilContextCancel(ctx, syncedPollPeriod, true, func(ctx context.Context) (done bool, err error) {
		for _, handler := range sched.registeredHandlers {
			if !handler.HasSynced() {
				return false, nil
			}
		}
		return true, nil
	})
}

func (sched *Scheduler) addAllEventHandlers(
	clusterName string,
	informerFactory informers.SharedInformerFactory,
) error {
	var (
		handlerRegistration cache.ResourceEventHandlerRegistration
		err                 error
		handlers            []cache.ResourceEventHandlerRegistration
	)
	// logger := sched.logger

	// =====================================================================
	// 1. Pod 事件处理：分流路由 (Cache vs Queue)
	// =====================================================================

	// 【路径 A】子集群：已调度 Pod (Assigned) -> 进入 Cache 记账
	// 目的：维护各个子集群真实的 GPU/算力消耗账本
	if clusterName != "" {
		if handlerRegistration, err = informerFactory.Core().V1().Pods().Informer().AddEventHandler(
			cache.FilteringResourceEventHandler{
				FilterFunc: func(obj interface{}) bool {
					switch t := obj.(type) {
					case *corev1.Pod:
						return assignedPod(t)
					case cache.DeletedFinalStateUnknown:
						// 为了防止内存泄露，删除事件一律放行进入 Handler
						return true
					default:
						return false
					}
				},
				Handler: cache.ResourceEventHandlerFuncs{
					AddFunc: func(obj interface{}) {
						p, _ := sched.preparePod(clusterName, obj)
						sched.addPodToCache(p)
					},
					UpdateFunc: func(old, new interface{}) {
						op, _ := sched.preparePod(clusterName, old)
						np, _ := sched.preparePod(clusterName, new)
						sched.updatePodInCache(op, np)
					},
					DeleteFunc: func(obj interface{}) {
						p, _ := sched.preparePod(clusterName, obj)
						sched.deletePodFromCache(p)
					},
				},
			},
		); err != nil {
			return err
		}
	}

	// 【路径 B】控制面 (Karmada)：未调度 Pod (Unscheduled) -> 进入 Queue 待调度
	// 目的：将属于 Lyra 调度的任务推入优先级队列
	if clusterName == "" {
		if handlerRegistration, err = informerFactory.Core().V1().Pods().Informer().AddEventHandler(
			cache.FilteringResourceEventHandler{
				FilterFunc: func(obj interface{}) bool {
					switch t := obj.(type) {
					case *corev1.Pod:
						// 满足：尚未分配节点 && 属于 Lyra 调度器管辖
						return !assignedPod(t) && sched.responsibleForPod(t)
					case cache.DeletedFinalStateUnknown:
						if pod, ok := t.Obj.(*corev1.Pod); ok {
							return sched.responsibleForPod(pod)
						}
						return false
					default:
						return false
					}
				},
				Handler: cache.ResourceEventHandlerFuncs{
					AddFunc:    sched.addPodToSchedulingQueue,
					UpdateFunc: sched.updatePodInSchedulingQueue,
					DeleteFunc: sched.deletePodFromSchedulingQueue,
				},
			},
		); err != nil {
			return err
		}
		handlers = append(handlers, handlerRegistration)
	}

	// =====================================================================
	// 2. Node 事件处理：算力供应 (仅限子集群)
	// =====================================================================
	// 目的：监听物理机加入/退出，以及 GPU 显存变化，驱动 Pod 重新入队调度
	if clusterName != "" {
		if handlerRegistration, err = informerFactory.Core().V1().Nodes().Informer().AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					n, _ := sched.prepareNode(clusterName, obj)
					sched.addNodeToCache(n)
				},
				UpdateFunc: func(old, new interface{}) {
					on, _ := sched.prepareNode(clusterName, old)
					nn, _ := sched.prepareNode(clusterName, new)
					sched.updateNodeInCache(on, nn)
				},
				DeleteFunc: func(obj interface{}) {
					sched.deleteNodeFromCache(obj)
				},
			},
		); err != nil {
			return err
		}
	}

	// =====================================================================
	// 3. 注册凭证归档 (仅针对控制面，用于启动同步检查)
	// =====================================================================
	if clusterName == "" {
		sched.registeredHandlers = append(sched.registeredHandlers, handlers...)
	}

	return nil
}

// -------------------------------------------------------------------
// Karmada Cluster 事件处理 (宏观集群的生老病死)
// -------------------------------------------------------------------

func (sched *Scheduler) addClusterToCache(obj interface{}) {
	cluster, ok := obj.(*karmadav1alpha1.Cluster)
	if !ok {
		sched.logger.Error("Cannot convert to *v1alpha1.Cluster", zap.Any("obj", obj))
		return
	}

	sched.logger.Info("Add event for Karmada Cluster", zap.String("cluster", cluster.Name))

	// 1. 记账：在 Cache 中开辟这个子集群的二维账本容器
	sched.Cache.AddCluster(sched.logger, cluster)

	// 2. 通电：触发 Manager 去获取凭证并拉起子集群监控
	if err := sched.ClusterManager.AccessCluster(cluster); err != nil {
		sched.logger.Error("Failed to access new cluster", zap.Error(err), zap.String("cluster", cluster.Name))
	}
}

func (sched *Scheduler) updateClusterInCache(oldObj, newObj interface{}) {
	oldCluster, ok1 := oldObj.(*karmadav1alpha1.Cluster)
	newCluster, ok2 := newObj.(*karmadav1alpha1.Cluster)
	if !ok1 || !ok2 {
		return
	}

	// 触发更新事件，通常是因为集群的 Taint (污点) 或 Label 发生了变化
	sched.logger.Debug("Update event for Karmada Cluster", zap.String("cluster", newCluster.Name))

	// 1. 同步账本：更新 Cache 中集群的元数据 (Taints/Labels/APIEndpoint 等)
	sched.Cache.UpdateCluster(sched.logger, oldCluster, newCluster)

	// 注意：通常 Update 事件不需要让 Manager 重新连一遍，
	// 除非你检测到 newCluster.Spec.APIEndpoint 发生了变化，那可能需要 Teardown 再 Access。
	// 这里我们保持简单，只更新 Cache 状态。
}

func (sched *Scheduler) deleteClusterFromCache(obj interface{}) {
	var cluster *karmadav1alpha1.Cluster
	switch t := obj.(type) {
	case *karmadav1alpha1.Cluster:
		cluster = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		cluster, ok = t.Obj.(*karmadav1alpha1.Cluster)
		if !ok {
			sched.logger.Error("Cannot convert to *v1alpha1.Cluster in DeletedFinalStateUnknown")
			return
		}
	default:
		return
	}

	sched.logger.Info("Delete event for Karmada Cluster", zap.String("cluster", cluster.Name))

	// 1. 断电：先让 Manager 优雅地关闭子集群所有的 Watch 协程和连接
	if err := sched.ClusterManager.TeardownCluster(cluster); err != nil {
		sched.logger.Error("Failed to teardown cluster", zap.Error(err), zap.String("cluster", cluster.Name))
	}

	// 2. 销账：调用 Cache 的绝杀逻辑，连根拔起该集群的所有残留数据
	if err := sched.Cache.RemoveCluster(sched.logger, cluster); err != nil {
		sched.logger.Error("Scheduler cache RemoveCluster failed", zap.Error(err))
	}
}

// addClusterEventHandlers 专门负责监听 Karmada 控制面的集群资源变更
func (sched *Scheduler) addClusterEventHandlers(clusterInformer cache.SharedIndexInformer) error {
	registration, err := clusterInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sched.addClusterToCache,
		UpdateFunc: sched.updateClusterInCache,
		DeleteFunc: sched.deleteClusterFromCache,
	})
	if err != nil {
		return err
	}

	// 将控制面的 Informer 凭证加入启动等待队列
	sched.registeredHandlers = append(sched.registeredHandlers, registration)
	return nil
}

// addControlPlanePodEventHandlers 专门监听 Karmada 控制面那些用户提交的、未调度的 Pod
func (sched *Scheduler) addControlPlanePodEventHandlers(podInformer cache.SharedIndexInformer) {
	registration, _ := podInformer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			pod, ok := extractPod(obj)
			// 🌟 核心过滤：没分节点 + 归我管 = 进入调度队列
			return ok && !assignedPod(pod) && sched.responsibleForPod(pod)
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    sched.addPodToSchedulingQueue,
			UpdateFunc: sched.updatePodInSchedulingQueue,
			DeleteFunc: sched.deletePodFromSchedulingQueue,
		},
	})
	sched.registeredHandlers = append(sched.registeredHandlers, registration)
}

// extractPod 尝试从不同类型的事件对象中提取出 Pod 指针
func extractPod(obj interface{}) (*corev1.Pod, bool) {
	// 1. 正常情况：直接断言
	if pod, ok := obj.(*corev1.Pod); ok {
		return pod, ok
	}

	// 2. 异常删除情况：处理 DeletedFinalStateUnknown 包装
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if pod, ok := tombstone.Obj.(*corev1.Pod); ok {
			return pod, ok
		}
	}

	return nil, false
}

// extractNode 提取节点对象
func extractNode(obj interface{}) (*corev1.Node, bool) {
	if node, ok := obj.(*corev1.Node); ok {
		return node, ok
	}
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if node, ok := tombstone.Obj.(*corev1.Node); ok {
			return node, ok
		}
	}
	return nil, false
}

// extractCluster 提取 Karmada 集群对象
func extractCluster(obj interface{}) (*karmadav1alpha1.Cluster, bool) {
	if cluster, ok := obj.(*karmadav1alpha1.Cluster); ok {
		return cluster, ok
	}
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if cluster, ok := tombstone.Obj.(*karmadav1alpha1.Cluster); ok {
			return cluster, ok
		}
	}
	return nil, false
}
