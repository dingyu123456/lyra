package framework

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	schedutil "github.com/dingyu123456/lyra/internal/scheduler/util"
	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

const (
	MaxNodeScore int64 = 100
	MinNodeScore int64 = 0
)

var generation int64

// ActionType is an integer to represent one type of resource change.
// Different ActionTypes can be bit-wised to compose new semantics.
type ActionType int64

// Constants for ActionTypes.
// CAUTION for contributors: When you add a new ActionType, you must update the following:
// - The list of basic, podOnly, and nodeOnly.
// - String() method.
const (
	Add ActionType = 1 << iota
	Delete

	// UpdateNodeXYZ is only applicable for Node events.
	// If you use UpdateNodeXYZ,
	// your plugin's QueueingHint is only executed for the specific sub-Update event.
	// It's better to narrow down the scope of the event by using them instead of just using Update event
	// for better performance in requeueing.
	UpdateNodeAllocatable
	UpdateNodeLabel
	// UpdateNodeTaint is an update for node's taints or node.Spec.Unschedulable.
	UpdateNodeTaint
	UpdateNodeCondition
	UpdateNodeAnnotation

	// UpdatePodXYZ is only applicable for Pod events.
	// If you use UpdatePodXYZ,
	// your plugin's QueueingHint is only executed for the specific sub-Update event.
	// It's better to narrow down the scope of the event by using them instead of Update event
	// for better performance in requeueing.
	UpdatePodLabel
	// UpdatePodScaleDown is an update for pod's scale down (i.e., any resource request is reduced).
	UpdatePodScaleDown
	// UpdatePodTolerations is an addition for pod's tolerations.
	// (Due to API validation, we can add, but cannot modify or remove tolerations.)
	UpdatePodTolerations
	// UpdatePodSchedulingGatesEliminated is an update for pod's scheduling gates, which eliminates all scheduling gates in the Pod.
	UpdatePodSchedulingGatesEliminated
	// UpdatePodGeneratedResourceClaim is an update of the list of ResourceClaims generated for the pod.
	// Depends on the DynamicResourceAllocation feature gate.
	UpdatePodGeneratedResourceClaim

	// updatePodOther is a update for pod's other fields.
	// It's used only for the internal event handling, and thus unexported.
	updatePodOther

	All ActionType = 1<<iota - 1

	// Use the general Update type if you don't either know or care the specific sub-Update type to use.
	Update = UpdateNodeAllocatable | UpdateNodeLabel | UpdateNodeTaint | UpdateNodeCondition | UpdateNodeAnnotation | UpdatePodLabel | UpdatePodScaleDown | UpdatePodTolerations | UpdatePodSchedulingGatesEliminated | UpdatePodGeneratedResourceClaim | updatePodOther
	// none is a special ActionType that is only used internally.
	none ActionType = 0
)

var (
	// basicActionTypes is a list of basicActionTypes ActionTypes.
	basicActionTypes = []ActionType{Add, Delete, Update}
	// podActionTypes is a list of ActionTypes that are only applicable for Pod events.
	podActionTypes = []ActionType{UpdatePodLabel, UpdatePodScaleDown, UpdatePodTolerations, UpdatePodSchedulingGatesEliminated, UpdatePodGeneratedResourceClaim}
	// nodeActionTypes is a list of ActionTypes that are only applicable for Node events.
	nodeActionTypes = []ActionType{UpdateNodeAllocatable, UpdateNodeLabel, UpdateNodeTaint, UpdateNodeCondition, UpdateNodeAnnotation}
)

func (a ActionType) String() string {
	switch a {
	case Add:
		return "Add"
	case Delete:
		return "Delete"
	case UpdateNodeAllocatable:
		return "UpdateNodeAllocatable"
	case UpdateNodeLabel:
		return "UpdateNodeLabel"
	case UpdateNodeTaint:
		return "UpdateNodeTaint"
	case UpdateNodeCondition:
		return "UpdateNodeCondition"
	case UpdateNodeAnnotation:
		return "UpdateNodeAnnotation"
	case UpdatePodLabel:
		return "UpdatePodLabel"
	case UpdatePodScaleDown:
		return "UpdatePodScaleDown"
	case UpdatePodTolerations:
		return "UpdatePodTolerations"
	case UpdatePodSchedulingGatesEliminated:
		return "UpdatePodSchedulingGatesEliminated"
	case UpdatePodGeneratedResourceClaim:
		return "UpdatePodGeneratedResourceClaim"
	case updatePodOther:
		return "Update"
	case All:
		return "All"
	case Update:
		return "Update"
	}

	// Shouldn't reach here.
	return ""
}

// EventResource is basically short for group/version/kind, which can uniquely represent a particular API resource.
type EventResource string

// Constants for GVKs.
//
// CAUTION for contributors: When you add a new EventResource, you must register a new one to allResources.
//
// Note:
// - UpdatePodXYZ or UpdateNodeXYZ: triggered by updating particular parts of a Pod or a Node, e.g. updatePodLabel.
// Use specific events rather than general ones (updatePodLabel vs update) can make the requeueing process more efficient
// and consume less memory as less events will be cached at scheduler.
const (
	// There are a couple of notes about how the scheduler notifies the events of Pods:
	// - Add: add events could be triggered by either a newly created Pod or an existing Pod that is scheduled to a Node.
	// - Delete: delete events could be triggered by:
	//           - a Pod that is deleted
	//           - a Pod that was assumed, but gets un-assumed due to some errors in the binding cycle.
	//           - an existing Pod that was unscheduled but gets scheduled to a Node.
	//
	// Note that the Pod event type includes the events for the unscheduled Pod itself.
	// i.e., when unscheduled Pods are updated, the scheduling queue checks with Pod/Update QueueingHint(s) whether the update may make the pods schedulable,
	// and requeues them to activeQ/backoffQ when at least one QueueingHint(s) return Queue.
	// Plugins **have to** implement a QueueingHint for Pod/Update event
	// if the rejection from them could be resolved by updating unscheduled Pods themselves.
	// Example: Pods that require excessive resources may be rejected by the noderesources plugin,
	// if this unscheduled pod is updated to require fewer resources,
	// the previous rejection from noderesources plugin can be resolved.
	// this plugin would implement QueueingHint for Pod/Update event
	// that returns Queue when such label changes are made in unscheduled Pods.
	Pod EventResource = "Pod"

	// These assignedPod and unschedulablePod are internal resources that are used to represent the type of Pod.
	// We don't expose them to the plugins deliberately because we don't publish Pod events with unschedulable Pods in the first place.
	assignedPod      EventResource = "AssignedPod"
	unschedulablePod EventResource = "UnschedulablePod"

	// A note about NodeAdd event and UpdateNodeTaint event:
	// When QHint is disabled, NodeAdd often isn't worked expectedly because of the internal feature called preCheck.
	// It's definitely not something expected for plugin developers,
	// and registering UpdateNodeTaint event is the only mitigation for now.
	// So, kube-scheduler registers UpdateNodeTaint event for plugins that has NodeAdded event, but don't have UpdateNodeTaint event.
	// It has a bad impact for the requeuing efficiency though, a lot better than some Pods being stuck in the
	// unschedulable pod pool.
	// This problematic preCheck feature is disabled when QHint is enabled,
	// and eventually will be removed along with QHint graduation.
	// See: https://github.com/kubernetes/kubernetes/issues/110175
	Node                  EventResource = "Node"
	PersistentVolume      EventResource = "PersistentVolume"
	PersistentVolumeClaim EventResource = "PersistentVolumeClaim"
	CSINode               EventResource = "storage.k8s.io/CSINode"
	CSIDriver             EventResource = "storage.k8s.io/CSIDriver"
	VolumeAttachment      EventResource = "storage.k8s.io/VolumeAttachment"
	CSIStorageCapacity    EventResource = "storage.k8s.io/CSIStorageCapacity"
	StorageClass          EventResource = "storage.k8s.io/StorageClass"
	ResourceClaim         EventResource = "resource.k8s.io/ResourceClaim"
	ResourceSlice         EventResource = "resource.k8s.io/ResourceSlice"
	DeviceClass           EventResource = "resource.k8s.io/DeviceClass"

	// WildCard is a special EventResource to match all resources.
	// e.g., If you register `{Resource: "*", ActionType: All}` in EventsToRegister,
	// all coming clusterEvents will be admitted. Be careful to register it, it will
	// increase the computing pressure in requeueing unless you really need it.
	//
	// Meanwhile, if the coming clusterEvent is a wildcard one, all pods
	// will be moved from unschedulablePod pool to activeQ/backoffQ forcibly.
	WildCard EventResource = "*"
)

var (
	// allResources is a list of all resources.
	allResources = []EventResource{
		Pod,
		assignedPod,
		unschedulablePod,
		Node,
		PersistentVolume,
		PersistentVolumeClaim,
		CSINode,
		CSIDriver,
		CSIStorageCapacity,
		StorageClass,
		VolumeAttachment,
		ResourceClaim,
		ResourceSlice,
		DeviceClass,
	}
)

type ClusterEventWithHint struct {
	Event ClusterEvent
	// QueueingHintFn is executed for the Pod rejected by this plugin when the above Event happens,
	// and filters out events to reduce useless retry of Pod's scheduling.
	// It's an optional field. If not set,
	// the scheduling of Pods will be always retried with backoff when this Event happens.
	// (the same as Queue)
	QueueingHintFn QueueingHintFn
}

// QueueingHintFn returns a hint that signals whether the event can make a Pod,
// which was rejected by this plugin in the past scheduling cycle, schedulable or not.
// It's called before a Pod gets moved from unschedulableQ to backoffQ or activeQ.
// If it returns an error, we'll take the returned QueueingHint as `Queue` at the caller whatever we returned here so that
// we can prevent the Pod from being stuck in the unschedulable pod pool.
//
// - `pod`: the Pod to be enqueued, which is rejected by this plugin in the past.
// - `oldObj` `newObj`: the object involved in that event.
//   - For example, the given event is "Node deleted", the `oldObj` will be that deleted Node.
//   - `oldObj` is nil if the event is add event.
//   - `newObj` is nil if the event is delete event.
type QueueingHintFn func(logger *zap.Logger, pod *corev1.Pod, oldObj, newObj interface{}) (QueueingHint, error)

type QueueingHint int

const (
	// QueueSkip implies that the cluster event has no impact on
	// scheduling of the pod.
	QueueSkip QueueingHint = iota

	// Queue implies that the Pod may be schedulable by the event.
	Queue
)

func (s QueueingHint) String() string {
	switch s {
	case QueueSkip:
		return "QueueSkip"
	case Queue:
		return "Queue"
	}
	return ""
}

// ClusterEvent abstracts how a system resource's state gets changed.
// Resource represents the standard API resources such as Pod, Node, etc.
// ActionType denotes the specific change such as Add, Update or Delete.
type ClusterEvent struct {
	Resource   EventResource
	ActionType ActionType

	// label describes this cluster event.
	// It's an optional field to control String(), which is used in logging and metrics.
	// Normally, it's not necessary to set this field; only used for special events like UnschedulableTimeout.
	label string
}

// Label is used for logging and metrics.
func (ce ClusterEvent) Label() string {
	if ce.label != "" {
		return ce.label
	}

	return fmt.Sprintf("%v%v", ce.Resource, ce.ActionType)
}

// AllClusterEventLabels returns all possible cluster event labels given to the metrics.
func AllClusterEventLabels() []string {
	labels := []string{UnschedulableTimeout, ForceActivate}
	for _, r := range allResources {
		for _, a := range basicActionTypes {
			labels = append(labels, ClusterEvent{Resource: r, ActionType: a}.Label())
		}
		if r == Pod {
			for _, a := range podActionTypes {
				labels = append(labels, ClusterEvent{Resource: r, ActionType: a}.Label())
			}
		} else if r == Node {
			for _, a := range nodeActionTypes {
				labels = append(labels, ClusterEvent{Resource: r, ActionType: a}.Label())
			}
		}
	}
	return labels
}

// IsWildCard returns true if ClusterEvent follows WildCard semantics
func (ce ClusterEvent) IsWildCard() bool {
	return ce.Resource == WildCard && ce.ActionType == All
}

// Match returns true if ClusterEvent is matched with the coming event.
// If the ce.Resource is "*", there's no requirement for the coming event' Resource.
// Contrarily, if the coming event's Resource is "*", the ce.Resource should only be "*".
//
// Note: we have a special case here when the coming event is a wildcard event,
// it will force all Pods to move to activeQ/backoffQ,
// but we take it as an unmatched event unless the ce is also a wildcard one.
func (ce ClusterEvent) Match(incomingEvent ClusterEvent) bool {
	return ce.IsWildCard() || ce.Resource.match(incomingEvent.Resource) && ce.ActionType&incomingEvent.ActionType != 0
}

// match returns true if the resource is matched with the coming resource.
func (r EventResource) match(resource EventResource) bool {
	// WildCard matches all resources
	return r == WildCard ||
		// Exact match
		r == resource ||
		// Pod matches assignedPod and unscheduledPod.
		// (assignedPod and unscheduledPod aren't exposed and hence only used for incoming events and never used in EventsToRegister)
		r == Pod && (resource == assignedPod || resource == unschedulablePod)
}

func UnrollWildCardResource() []ClusterEventWithHint {
	return []ClusterEventWithHint{
		{Event: ClusterEvent{Resource: Pod, ActionType: All}},
		{Event: ClusterEvent{Resource: Node, ActionType: All}},
		{Event: ClusterEvent{Resource: PersistentVolume, ActionType: All}},
		{Event: ClusterEvent{Resource: PersistentVolumeClaim, ActionType: All}},
		{Event: ClusterEvent{Resource: CSINode, ActionType: All}},
		{Event: ClusterEvent{Resource: CSIDriver, ActionType: All}},
		{Event: ClusterEvent{Resource: CSIStorageCapacity, ActionType: All}},
		{Event: ClusterEvent{Resource: StorageClass, ActionType: All}},
		{Event: ClusterEvent{Resource: ResourceClaim, ActionType: All}},
		{Event: ClusterEvent{Resource: DeviceClass, ActionType: All}},
	}
}

// QueuedPodInfo 任务在队列中的封装 (极其重要，防抖基石)
// -----------------------------------------------------------------
type QueuedPodInfo struct {
	*PodInfo

	Timestamp               time.Time // 入队时间
	Attempts                int       // 尝试调度次数，用于算 Backoff
	InitialAttemptTimestamp *time.Time
	// 记录曾经拒绝过它的插件。当事件发生时，只唤醒这些插件关心的任务
	UnschedulablePlugins sets.Set[string]
	PendingPlugins       sets.Set[string]
	// Whether the Pod is scheduling gated (by PreEnqueuePlugins) or not.
	Gated bool
}

// DeepCopy returns a deep copy of the QueuedPodInfo object.
func (pqi *QueuedPodInfo) DeepCopy() *QueuedPodInfo {
	return &QueuedPodInfo{
		PodInfo:                 pqi.PodInfo.DeepCopy(),
		Timestamp:               pqi.Timestamp,
		Attempts:                pqi.Attempts,
		InitialAttemptTimestamp: pqi.InitialAttemptTimestamp,
		UnschedulablePlugins:    pqi.UnschedulablePlugins.Clone(),
		PendingPlugins:          pqi.PendingPlugins.Clone(),
		Gated:                   pqi.Gated,
	}
}

// nextGeneration: Let's make sure history never forgets the name...
// Increments the generation number monotonically ensuring that generation numbers never collide.
// Collision of the generation numbers would be particularly problematic if a node was deleted and
// added back with the same name. See issue#63262.
func nextGeneration() int64 {
	return atomic.AddInt64(&generation, 1)
}

// ClusterInfo 暴露给调度算法的宏观集群快照
type ClusterInfo struct {
	cluster     *clusterv1alpha1.Cluster
	ClusterName string
	// 该集群下的节点集合，供 Filter 阶段进一步遍历
	Nodes       map[string]*NodeInfo
	Allocatable *Resource
	Requested   *Resource
	Generation  int64
}

// NewClusterInfo 创建一个新的集群宏观账本
func NewClusterInfo(clusterName string) *ClusterInfo {
	return &ClusterInfo{
		ClusterName: clusterName,
		// 初始化空集合和空资源账本
		Nodes:       make(map[string]*NodeInfo),
		Allocatable: &Resource{},
		Requested:   &Resource{},
		// 🌟 诞生之初，推高全局世代号！
		Generation: nextGeneration(),
	}
}

func (ci *ClusterInfo) BumpGeneration() {
	ci.Generation = nextGeneration() // 调用内部的原子时钟
}

// AddPod 直接调用底层 update
func (ci *ClusterInfo) AddPod(pod *corev1.Pod) {
	ci.update(pod, 1) // 记账，加法
}

// RemovePod 极致精简：不做切片遍历，直接暴力反向记账！
func (ci *ClusterInfo) RemovePod(logger *zap.Logger, pod *corev1.Pod) {
	ci.update(pod, -1) // 销账，减法
}

// update 是集群宏观账本的标量加减法引擎。
// sign = 1 表示 AddPod (扣减算力)，sign = -1 表示 RemovePod (释放算力)
func (ci *ClusterInfo) update(pod *corev1.Pod, sign int64) {
	// ==========================================
	// 1. 基础资源 (CPU / Mem) 的宏观加减法
	// ==========================================
	milliCPU, memory, storage := calculateBasicResource(pod)
	if ci.Requested == nil {
		ci.Requested = &Resource{}
	}
	ci.Requested.MilliCPU += sign * milliCPU
	ci.Requested.Memory += sign * memory
	ci.Requested.EphemeralStorage += sign * storage

	// ==========================================
	// 2. [Lyra 定制] GPU 宏观标量聚i合
	// ==========================================
	// 初始化扩展资源字典
	if ci.Requested.ScalarResources == nil {
		ci.Requested.ScalarResources = make(map[corev1.ResourceName]int64)
	}

	// 尝试解析 Hami 注解（可能是Informer推送回来的真实事件，也可能是预扣pod我们的调度系统注入的注解）
	allocGPUs, err := ParsePodHamiAnnotation(pod)

	if err == nil && len(allocGPUs) > 0 {
		// 场景 A：这是一个真正被 Hami 分配了显卡的 Pod
		var totalMemUsed, totalCoreUsed int64
		for _, alloc := range allocGPUs {
			totalMemUsed += alloc.MemoryUsed
			totalCoreUsed += alloc.CoreUsed
		}

		// 累加到宏观标量池中
		// ResourceGPU 代表 vGPU 个数，ResourceGPUMem 代表显存，ResourceGPUCores 代表算力比例
		ci.Requested.ScalarResources[ResourceGPU] += sign * int64(len(allocGPUs))
		ci.Requested.ScalarResources[ResourceGPUMem] += sign * totalMemUsed
		ci.Requested.ScalarResources[ResourceGPUCores] += sign * totalCoreUsed

	}

	// ==========================================
	// 3. 触发增量世代号
	// ==========================================
	ci.Generation = nextGeneration()
}

// 暴露给外部 Cache 调用的标准方法
func (ci *ClusterInfo) AddNode(nodeInfo *NodeInfo) {
	ci.updateNode(nodeInfo, 1)
}

func (ci *ClusterInfo) RemoveNode(nodeInfo *NodeInfo) {
	ci.updateNode(nodeInfo, -1)
}

// updateNode 聚合或扣减底层节点的总算力到集群宏观账本中
func (ci *ClusterInfo) updateNode(nodeInfo *NodeInfo, sign int64) {
	if ci.Allocatable == nil {
		ci.Allocatable = &Resource{}
	}
	if ci.Allocatable.ScalarResources == nil {
		ci.Allocatable.ScalarResources = make(map[corev1.ResourceName]int64)
	}

	// ==========================================
	// 1. 基础资源加减 (CPU, Memory, Storage, Pods 数量)
	// ==========================================
	ci.Allocatable.MilliCPU += sign * nodeInfo.Allocatable.MilliCPU
	ci.Allocatable.Memory += sign * nodeInfo.Allocatable.Memory
	ci.Allocatable.EphemeralStorage += sign * nodeInfo.Allocatable.EphemeralStorage
	ci.Allocatable.AllowedPodNumber += int(sign) * nodeInfo.Allocatable.AllowedPodNumber

	// 原生扩展资源兜底 (如果节点上有非 GPU 的其他扩展资源，如 RDMA、FPGA，这里依然能兼容累加)
	for resName, resValue := range nodeInfo.Allocatable.ScalarResources {
		ci.Allocatable.ScalarResources[resName] += sign * resValue
	}

	// ==========================================
	// 🌟 2. Lyra AI 定制：直接从微观拓扑提取并聚合 GPU 总算力
	// ==========================================
	// 完美解决了隐性依赖：不要求 NodeInfo 去维护宏观标量，直接从真实物理设备映射表里算！
	if len(nodeInfo.GPUs) > 0 {
		var totalVGPU, totalMem, totalCore int64
		for _, gpu := range nodeInfo.GPUs {
			totalVGPU += gpu.AllocatableVGPU
			totalMem += gpu.AllocatableMem
			totalCore += gpu.AllocatableCore
		}

		ci.Allocatable.ScalarResources[ResourceGPU] += sign * totalVGPU
		ci.Allocatable.ScalarResources[ResourceGPUMem] += sign * totalMem
		ci.Allocatable.ScalarResources[ResourceGPUCores] += sign * totalCore
	}

	// ==========================================
	// 3. 触发增量世代号
	// ==========================================
	ci.Generation = nextGeneration()
}

func (ci *ClusterInfo) DeepCopy() *ClusterInfo {
	clone := &ClusterInfo{
		cluster:     ci.cluster.DeepCopy(),
		ClusterName: ci.ClusterName,
		Nodes:       make(map[string]*NodeInfo, len(ci.Nodes)),
		Generation:  ci.Generation,
	}

	for k, node := range ci.Nodes {
		clone.Nodes[k] = node.DeepCopy()
	}
	if ci.Allocatable != nil {
		clone.Allocatable = ci.Allocatable.Clone() // 需自己实现 Resource.Clone
	}
	if ci.Requested != nil {
		clone.Requested = ci.Requested.Clone()
	}
	return clone
}

func (ci *ClusterInfo) Cluster() *clusterv1alpha1.Cluster {
	if ci == nil {
		return nil
	}
	return ci.cluster
}

// SetCluster 更新集群宏观控制面信息 (绝不干涉由 Node 聚合上来的资源账本)
func (ci *ClusterInfo) SetCluster(c *clusterv1alpha1.Cluster) {
	ci.cluster = c
	ci.ClusterName = c.Name

	// 防御性初始化：只在创世的第一刻分配内存，绝不覆盖已有的账本！
	if ci.Allocatable == nil {
		ci.Allocatable = &Resource{
			ScalarResources: make(map[corev1.ResourceName]int64),
		}
	}

	// 触发增量世代号 (控制面元数据变化，也需要通知快照更新)
	ci.Generation = nextGeneration()
}

// NodeInfo 暴露给调度算法的微观节点与 GPU 快照
type NodeInfo struct {
	NodeName    string
	ClusterName string
	Node        *corev1.Node

	// 这里保存分配到该节点的 Shadow Pods
	Pods []*PodInfo

	Allocatable *Resource
	Requested   *Resource
	GPUs        map[string]*GPUInfo

	Generation int64
}

// NewNodeInfo returns a ready to use empty NodeInfo object.
// If any pods are given in arguments, their information will be aggregated in
// the returned object.
func NewNodeInfo(pods ...*corev1.Pod) *NodeInfo {
	ni := &NodeInfo{
		Requested:   &Resource{},
		Allocatable: &Resource{},
		Generation:  nextGeneration(),
	}
	for _, pod := range pods {
		ni.AddPod(pod)
	}
	return ni
}

// SetNode 设置或更新节点级别的基础信息、宏观算力账本以及微观 GPU 拓扑
func (n *NodeInfo) SetNode(node *corev1.Node) {
	n.Node = node
	n.NodeName = node.Name
	n.ClusterName = GetClusterNameFromNode(node)

	// K8s 宏观容量更新 (不触碰 n.Requested)
	n.Allocatable = NewResource(node.Status.Allocatable)

	// ==========================================
	// 🌟 Lyra AI 微观账本更新 (带防误杀继承逻辑)
	// ==========================================
	parsedGPUs, err := ParseNodeHamiAnnotation(node)
	if err == nil && len(parsedGPUs) > 0 {

		// 🌟 核心修复：暂存旧的 GPU 字典，用来继承“池子里的水”
		oldGPUs := n.GPUs

		// 初始化新字典
		n.GPUs = make(map[string]*GPUInfo, len(parsedGPUs))

		for _, parsed := range parsedGPUs {
			newGPU := &GPUInfo{
				UUID:            parsed.UUID,
				Type:            parsed.Type,
				AllocatableVGPU: parsed.MaxVGPUs,
				AllocatableMem:  parsed.MemoryTotal,
				AllocatableCore: parsed.CoreTotal,
			}

			// 🌟 核心修复：如果这块卡之前就在机器上（没有被物理拔除），
			// 必须继承它身上已经跑着的 AI 任务账本！
			if oldGPUs != nil {
				if oldGPU, exists := oldGPUs[parsed.UUID]; exists {
					newGPU.RequestedVGPU = oldGPU.RequestedVGPU
					newGPU.RequestedMem = oldGPU.RequestedMem
					newGPU.RequestedCore = oldGPU.RequestedCore
				}
			}

			n.GPUs[parsed.UUID] = newGPU
		}
	} else {
		// 防御：如果是把显卡全拔了的极端情况
		// 注意：这种极端情况通常会导致上面的 Pod 被驱逐，Pod 驱逐事件会慢慢把 Requested 扣平
		n.GPUs = nil
	}

	// 核心时钟推进
	n.Generation = nextGeneration()
}

// RemoveNode 在底层物理抹除节点信息 (变成幽灵节点)
func (n *NodeInfo) RemoveNode() {
	n.Node = nil
	// 物理机都没了，总容量清零
	n.Allocatable = &Resource{}
	n.GPUs = nil
	// 数据发生剧变，自己推高世代号
	n.Generation = nextGeneration()
}

// Snapshot 为调度算法打快照时，提供 NodeInfo 的安全副本
func (n *NodeInfo) Snapshot() *NodeInfo {
	clone := &NodeInfo{
		NodeName:    n.NodeName,
		ClusterName: n.ClusterName,
		// K8s API 对象通常视为不可变(Immutable)，直接拷贝指针即可，节省开销
		Node: n.Node,
		// Resource 对象必须深拷贝，防止打分时被恶意插件篡改账本
		Allocatable: n.Allocatable.Clone(),
		Requested:   n.Requested.Clone(),
		Generation:  n.Generation,
	}

	// 1. 拷贝 Pods 切片
	// 浅拷贝切片内的指针即可，因为 PodInfo 一旦进入 Cache 就被视为只读对象
	if len(n.Pods) > 0 {
		clone.Pods = append([]*PodInfo(nil), n.Pods...)
	}

	// 2. 🌟 拷贝 Lyra 核心资产：GPUs 字典
	if len(n.GPUs) > 0 {
		clone.GPUs = make(map[string]*GPUInfo, len(n.GPUs))
		for gpuID, gpuInfo := range n.GPUs {
			// 如果你的 GPUInfo 是结构体指针，建议在这里做一次结构体值拷贝
			// 假设 GPUInfo 结构体没有更深层的嵌套切片：
			gpuCopy := *gpuInfo
			clone.GPUs[gpuID] = &gpuCopy

			// 如果你给 GPUInfo 也实现了 Clone() 方法，那就更完美了：
			// clone.GPUs[gpuID] = gpuInfo.Clone()
		}
	}

	return clone
}

// AddPod is a wrapper around AddPodInfo.
func (n *NodeInfo) AddPod(pod *corev1.Pod) {
	// ignore this err since apiserver doesn't properly validate affinity terms
	// and we can't fix the validation for backwards compatibility.
	podInfo, _ := NewPodInfo(pod)
	n.AddPodInfo(podInfo)
}

// AddPodInfo adds pod information to this NodeInfo.
// Consider using this instead of AddPod if a PodInfo is already computed.
func (n *NodeInfo) AddPodInfo(podInfo *PodInfo) {
	n.Pods = append(n.Pods, podInfo)
	n.update(podInfo.Pod, 1)
}

// RemovePod subtracts pod information from this NodeInfo.
func (n *NodeInfo) RemovePod(logger *zap.Logger, pod *corev1.Pod) error {
	k, err := GetPodKey(pod)
	if err != nil {
		return err
	}

	var removed bool
	if n.Pods, removed = removeFromSlice(logger, n.Pods, k); removed {
		n.update(pod, -1)
		return nil
	}
	return fmt.Errorf("no corresponding pod %s in pods of node %s", pod.Name, n.Node.Name)
}

// update 是节点账本的加减法引擎。
// sign = 1 表示 AddPod (记账扣减算力)，sign = -1 表示 RemovePod (销账释放算力)
func (n *NodeInfo) update(pod *corev1.Pod, sign int64) {
	// ==========================================
	// 1. K8s 基础资源 (CPU / Mem) 的加减法
	// ==========================================
	milliCPU, memory, storage := calculateBasicResource(pod)
	if n.Requested == nil {
		n.Requested = &Resource{} // 假设你有自己的 Resource 结构体
	}
	n.Requested.MilliCPU += sign * milliCPU
	n.Requested.Memory += sign * memory
	n.Requested.EphemeralStorage += sign * storage

	// ==========================================
	// 2. [Lyra 核心定制] Hami GPU 账单的精准加减法
	// ==========================================
	// Cache 在处理事件时，直接通过解析注解来确认物理卡的消耗
	allocGPUs, _ := ParsePodHamiAnnotation(pod)

	if len(allocGPUs) > 0 && n.GPUs != nil {
		for _, alloc := range allocGPUs {
			// 在物理机的 GPU 字典里找到那张被选中的具体显卡
			if gpu, exists := n.GPUs[alloc.UUID]; exists {
				// 🌟 魔法再次显现：
				// 如果 sign 是 1 (新增Pod)，这里就是 RequestedMem + Used，相当于锁定资源。
				// 如果 sign 是 -1 (删除Pod)，这里就是 RequestedMem - Used，相当于释放资源！
				gpu.RequestedMem += sign * alloc.MemoryUsed
				gpu.RequestedCore += sign * alloc.CoreUsed
				gpu.RequestedVGPU += sign * 1
			}
		}
	}

	// ==========================================
	// 3. 更新世代号，用于触发外层的增量快照
	// ==========================================
	n.Generation = nextGeneration() // 假设你的发号器在这里
}

func calculateBasicResource(pod *corev1.Pod) (milliCPU int64, memory int64, storage int64) {
	for _, container := range pod.Spec.Containers {
		// 通常调度器只看 Requests，如果没有 Requests 则回退到 Limits
		requests := container.Resources.Requests
		if requests == nil {
			continue
		}

		if cpu, ok := requests[corev1.ResourceCPU]; ok {
			milliCPU += cpu.MilliValue()
		}
		if mem, ok := requests[corev1.ResourceMemory]; ok {
			memory += mem.Value()
		}
		if bytesStorage, ok := requests[corev1.ResourceStorage]; ok {
			storage += bytesStorage.Value()
		}
	}
	return milliCPU, memory, storage
}

// DeepCopy 复制节点和它的 GPU 矢量状态
func (n *NodeInfo) DeepCopy() *NodeInfo {
	clone := &NodeInfo{
		NodeName:    n.NodeName,
		ClusterName: n.ClusterName,
		Node:        n.Node.DeepCopy(),
		Pods:        make([]*PodInfo, len(n.Pods)),
		Generation:  n.Generation,
		GPUs:        make(map[string]*GPUInfo, len(n.GPUs)),
	}

	// 深拷贝资源，防止多线程踩踏
	for i, pod := range n.Pods {
		clone.Pods[i] = pod.DeepCopy()
	}

	if n.Allocatable != nil {
		clone.Allocatable = n.Allocatable.Clone() // 需自己实现 Resource.Clone
	}
	if n.Requested != nil {
		clone.Requested = n.Requested.Clone()
	}

	for k, gpu := range n.GPUs {
		clone.GPUs[k] = gpu.DeepCopy()
	}

	return clone
}

// GPUInfo 单张物理卡的信息
type GPUInfo struct {
	UUID            string
	Type            string // e.g., "A100", "RTX4090"
	AllocatableMem  int64  // 显存容量
	RequestedMem    int64  // 已用显存
	AllocatableCore int64  // 算力百分比容量
	RequestedCore   int64  // 已用算力百分比
	AllocatableVGPU int64  // 可划分虚拟GPU容量
	RequestedVGPU   int64  // 已用虚拟GPU个数
}

func (gpu *GPUInfo) DeepCopy() *GPUInfo {
	clone := &GPUInfo{
		UUID:            gpu.UUID,
		Type:            gpu.Type,
		AllocatableMem:  gpu.AllocatableMem,
		RequestedMem:    gpu.RequestedMem,
		AllocatableCore: gpu.AllocatableCore,
		RequestedCore:   gpu.RequestedCore,
		AllocatableVGPU: gpu.AllocatableVGPU,
		RequestedVGPU:   gpu.RequestedVGPU,
	}
	return clone
}

// PodInfo is a wrapper to a Pod with additional pre-computed information to
// accelerate processing. This information is typically immutable (e.g., pre-processed
// inter-pod affinity selectors).
type PodInfo struct {
	Pod *corev1.Pod
}

// NewPodInfo returns a new PodInfo.
func NewPodInfo(pod *corev1.Pod) (*PodInfo, error) {
	pInfo := &PodInfo{}
	err := pInfo.Update(pod)
	return pInfo, err
}

// Update creates a full new PodInfo by default. And only updates the pod when the PodInfo
// has been instantiated and the passed pod is the exact same one as the original pod.
func (pi *PodInfo) Update(pod *corev1.Pod) error {
	pi.Pod = pod
	return nil
}

// DeepCopy returns a deep copy of the PodInfo object.
func (pi *PodInfo) DeepCopy() *PodInfo {
	return &PodInfo{
		Pod: pi.Pod.DeepCopy(),
	}
}

// Resource 资源定义 (保留 K8s 的 int64 标量加速设计，预留 ScalarResources 扩展)
type Resource struct {
	MilliCPU         int64
	Memory           int64
	EphemeralStorage int64
	AllowedPodNumber int
	ScalarResources  map[corev1.ResourceName]int64
}

// NewResource creates a Resource from ResourceList
func NewResource(rl corev1.ResourceList) *Resource {
	r := &Resource{}
	r.Add(rl)
	return r
}

// Add adds ResourceList into Resource.
func (r *Resource) Add(rl corev1.ResourceList) {
	if r == nil {
		return
	}

	for rName, rQuant := range rl {
		switch rName {
		case corev1.ResourceCPU:
			r.MilliCPU += rQuant.MilliValue()
		case corev1.ResourceMemory:
			r.Memory += rQuant.Value()
		case corev1.ResourcePods:
			r.AllowedPodNumber += int(rQuant.Value())
		case corev1.ResourceEphemeralStorage:
			r.EphemeralStorage += rQuant.Value()
		default:
			if schedutil.IsScalarResourceName(rName) {
				r.AddScalar(rName, rQuant.Value())
			}
		}
	}
}

// Clone returns a copy of this resource.
func (r *Resource) Clone() *Resource {
	res := &Resource{
		MilliCPU:         r.MilliCPU,
		Memory:           r.Memory,
		AllowedPodNumber: r.AllowedPodNumber,
		EphemeralStorage: r.EphemeralStorage,
	}
	if r.ScalarResources != nil {
		res.ScalarResources = make(map[corev1.ResourceName]int64, len(r.ScalarResources))
		for k, v := range r.ScalarResources {
			res.ScalarResources[k] = v
		}
	}
	return res
}

// AddScalar adds a resource by a scalar value of this resource.
func (r *Resource) AddScalar(name corev1.ResourceName, quantity int64) {
	r.SetScalar(name, r.ScalarResources[name]+quantity)
}

// SetScalar sets a resource by a scalar value of this resource.
func (r *Resource) SetScalar(name corev1.ResourceName, quantity int64) {
	// Lazily allocate scalar resource map.
	if r.ScalarResources == nil {
		r.ScalarResources = map[corev1.ResourceName]int64{}
	}
	r.ScalarResources[name] = quantity
}

// SetMaxResource compares with ResourceList and takes max value for each Resource.
func (r *Resource) SetMaxResource(rl corev1.ResourceList) {
	if r == nil {
		return
	}

	for rName, rQuantity := range rl {
		switch rName {
		case corev1.ResourceMemory:
			r.Memory = max(r.Memory, rQuantity.Value())
		case corev1.ResourceCPU:
			r.MilliCPU = max(r.MilliCPU, rQuantity.MilliValue())
		case corev1.ResourceEphemeralStorage:
			r.EphemeralStorage = max(r.EphemeralStorage, rQuantity.Value())
		default:
			if schedutil.IsScalarResourceName(rName) {
				r.SetScalar(rName, max(r.ScalarResources[rName], rQuantity.Value()))
			}
		}
	}
}

// ScheduleResult represents the result of scheduling a pod.
type ScheduleResult struct {
	SuggestedCluster string
	SuggestedNode    string

	// 具体 GPU UUID 列表
	SuggestedGPUs []string // e.g., ["uuid-1", "uuid-2"]

	// The number of nodes the scheduler evaluated the pod against in the filtering
	// phase and beyond.
	EvaluatedNodes int
	// The number of nodes out of the evaluated ones that fit the pod.
	FeasibleNodes int
}

// Diagnosis 汇集了调度失败的病历本
type Diagnosis struct {
	// 记录到底是哪些插件导致了这个 Pod 调度失败
	// 这些名字最终会被提取出来，交给 QueueingHint 唤醒使用！
	UnschedulablePlugins sets.Set[string]
}

// FitError 实现了 error 接口，包含了丰富的诊断信息
type FitError struct {
	Pod         *corev1.Pod
	NumAllNodes int
	Diagnosis   Diagnosis
}

func (f *FitError) Error() string {
	return fmt.Sprintf("0/%d nodes are available: pod %s/%s failed to fit", f.NumAllNodes, f.Pod.Namespace, f.Pod.Name)
}

// GetPodKey returns the string key of a pod.
func GetPodKey(pod *corev1.Pod) (string, error) {
	uid := string(pod.UID)
	if len(uid) == 0 {
		return "", errors.New("cannot get cache key for pod with empty UID")
	}
	return uid, nil
}

func removeFromSlice(logger *zap.Logger, s []*PodInfo, k string) ([]*PodInfo, bool) {
	var removed bool
	for i := range s {
		tmpKey, err := GetPodKey(s[i].Pod)
		if err != nil {
			logger.Error("Cannot get pod key",
				zap.Error(err),
				zap.Stringer("pod", klog.KObj(s[i].Pod)),
			)
			continue
		}
		if k == tmpKey {
			// delete the element
			s[i] = s[len(s)-1]
			s = s[:len(s)-1]
			removed = true
			break
		}
	}
	// resets the slices to nil so that we can do DeepEqual in unit tests.
	if len(s) == 0 {
		return nil, removed
	}
	return s, removed
}
