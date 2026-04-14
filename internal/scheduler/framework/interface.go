package framework

import (
	"context"
	"time"

	"github.com/dingyu123456/lyra/internal/scheduler/framework/parallelize"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
)

// NodeScoreList declares a list of nodes and their scores.
type NodeScoreList []NodeScore

// NodeScore is a struct with node name and score.
type NodeScore struct {
	Name    string
	Cluster string
	Score   int64
}

// NodePluginScores is a struct with node name and scores for that node.
type NodePluginScores struct {
	// Name is node name.
	Name string
	// Cluster if cluster name. 因为不同集群可能有相同的node name，所以增加该字段
	Cluster string
	// Scores is scores from plugins and extenders.
	Scores []PluginScore
	// TotalScore is the total score in Scores.
	TotalScore int64
}

// PluginScore is a struct with plugin/extender name and score.
type PluginScore struct {
	// Name is the name of plugin or extender.
	Name  string
	Score int64
}

// WaitingPod represents a pod currently waiting in the permit phase.
type WaitingPod interface {
	// GetPod returns a reference to the waiting pod.
	GetPod() *corev1.Pod
	// GetPendingPlugins returns a list of pending Permit plugin's name.
	GetPendingPlugins() []string
	// Allow declares the waiting pod is allowed to be scheduled by the plugin named as "pluginName".
	// If this is the last remaining plugin to allow, then a success signal is delivered
	// to unblock the pod.
	Allow(pluginName string)
	// Reject declares the waiting pod unschedulable.
	Reject(pluginName, msg string)
}

// PodActivator abstracts operations in the scheduling queue.
type PodActivator interface {
	// Activate moves the given pods to activeQ.
	// If a pod isn't found in unschedulablePods or backoffQ and it's in-flight,
	// the wildcard event is registered so that the pod will be requeued when it comes back.
	// But, if a pod isn't found in unschedulablePods or backoffQ and it's not in-flight (i.e., completely unknown pod),
	// Activate would ignore the pod.
	Activate(logger *zap.Logger, pods map[string]*corev1.Pod)
}

// Handle 插件”百宝箱”
type Handle interface {
	SnapshotSharedLister() SharedLister

	IterateOverWaitingPods(callback func(WaitingPod))
	GetWaitingPod(uid types.UID) WaitingPod
	RejectWaitingPod(uid types.UID) bool

	ClientSet() clientset.Interface
	KarmadaClient() karmadaclientset.Interface
	Logger() *zap.Logger
	SharedInformerFactory() informers.SharedInformerFactory

	// Parallelizer 极其重要：用于并发执行 Filter 插件，加速百级别集群的节点过滤
	Parallelizer() parallelize.Parallelizer
}

type Framework interface {
	Handle

	// --- 扩展点 ---
	PreEnqueuePlugins() []PreEnqueuePlugin
	EnqueueExtensions() []EnqueueExtensions
	QueueSortFunc() LessFunc

	// --- 调度计算流水线 ---
	RunPreFilterPlugins(ctx context.Context, state *CycleState, pod *corev1.Pod) (*PreFilterResult, *Status, sets.Set[string])
	RunFilterPlugins(ctx context.Context, state *CycleState, pod *corev1.Pod, node *NodeInfo) *Status
	RunPreScorePlugins(ctx context.Context, state *CycleState, pod *corev1.Pod, nodes []*NodeInfo) *Status
	RunScorePlugins(ctx context.Context, state *CycleState, pod *corev1.Pod, nodes []*NodeInfo) ([]NodePluginScores, *Status)

	RunPreBindPlugins(ctx context.Context, state *CycleState, pod *corev1.Pod, result ScheduleResult) *Status
	RunBindPlugins(ctx context.Context, state *CycleState, pod *corev1.Pod, result ScheduleResult) *Status
	RunPostBindPlugins(ctx context.Context, state *CycleState, pod *corev1.Pod, result ScheduleResult)

	// --- 逻辑状态预留阶段 (为 Gang 调度铺路) ---
	RunReservePluginsReserve(ctx context.Context, state *CycleState, pod *corev1.Pod) *Status
	RunReservePluginsUnreserve(ctx context.Context, state *CycleState, pod *corev1.Pod)

	RunPermitPlugins(ctx context.Context, state *CycleState, pod *corev1.Pod) *Status
	WaitOnPermit(ctx context.Context, pod *corev1.Pod) *Status

	// --- 内部管控 ---
	HasFilterPlugins() bool
	HasScorePlugins() bool

	SetPodActivator(activator PodActivator) // 允许插件强行激活 Pod

	Close() error
}

// PreFilterResult wraps needed info for scheduler framework to act upon PreFilter phase.
type PreFilterResult struct {
	// 宏观剪枝：只在这些子集群中调度 (nil 表示允许所有集群)
	ClusterNames sets.Set[string]
}

// AllClusters 判断是否允许所有集群
func (p *PreFilterResult) AllClusters() bool {
	return p == nil || p.ClusterNames == nil || p.ClusterNames.Len() == 0
}

// Merge 取两个 PreFilterResult 的交集
func (p *PreFilterResult) Merge(in *PreFilterResult) *PreFilterResult {
	if p.AllClusters() && in.AllClusters() {
		return nil
	}

	r := PreFilterResult{}
	// 1. 集群维度的交集
	if p.AllClusters() {
		r.ClusterNames = in.ClusterNames.Clone() // 拷贝 in 的约束
		return &r
	}
	if in.AllClusters() {
		r.ClusterNames = p.ClusterNames.Clone() // 拷贝 p 的约束
		return &r
	}

	r.ClusterNames = p.ClusterNames.Intersection(in.ClusterNames)
	return &r
}

// Plugin is the parent type for all the scheduling framework plugins.
type Plugin interface {
	Name() string
}

// PreEnqueuePlugin is an interface that must be implemented by "PreEnqueue" plugins.
// These plugins are called prior to adding Pods to activeQ.
// Note: an preEnqueue plugin is expected to be lightweight and efficient, so it's not expected to
// involve expensive calls like accessing external endpoints; otherwise it'd block other
// Pods' enqueuing in event handlers.
type PreEnqueuePlugin interface {
	Plugin
	// PreEnqueue is called prior to adding Pods to activeQ.
	PreEnqueue(ctx context.Context, p *corev1.Pod) *Status
}

// LessFunc is the function to sort pod info
type LessFunc func(podInfo1, podInfo2 *QueuedPodInfo) bool

// QueueSortPlugin is an interface that must be implemented by "QueueSort" plugins.
// These plugins are used to sort pods in the scheduling queue. Only one queue sort
// plugin may be enabled at a time.
type QueueSortPlugin interface {
	Plugin
	// Less are used to sort pods in the scheduling queue.
	Less(*QueuedPodInfo, *QueuedPodInfo) bool
}

// EnqueueExtensions is an optional interface that plugins can implement to efficiently
// move unschedulable Pods in internal scheduling queues.
// In the scheduler, Pods can be unschedulable by PreEnqueue, PreFilter, Filter, Reserve, and Permit plugins,
// and Pods rejected by these plugins are requeued based on this extension point.
// Failures from other extension points are regarded as temporal errors (e.g., network failure),
// and the scheduler requeue Pods without this extension point - always requeue Pods to activeQ after backoff.
// This is because such temporal errors cannot be resolved by specific cluster events,
// and we have no choose but keep retrying scheduling until the failure is resolved.
//
// Plugins that make pod unschedulable (PreEnqueue, PreFilter, Filter, Reserve, and Permit plugins) should implement this interface,
// otherwise the default implementation will be used, which is less efficient in requeueing Pods rejected by the plugin.
// And, if plugins other than above extension points support this interface, they are just ignored.
type EnqueueExtensions interface {
	Plugin
	// EventsToRegister returns a series of possible events that may cause a Pod
	// failed by this plugin schedulable. Each event has a callback function that
	// filters out events to reduce useless retry of Pod's scheduling.
	// The events will be registered when instantiating the internal scheduling queue,
	// and leveraged to build event handlers dynamically.
	// When it returns an error, the scheduler fails to start.
	// Note: the returned list needs to be determined at a startup,
	// and the scheduler only evaluates it once during start up.
	// Do not change the result during runtime, for example, based on the cluster's state etc.
	//
	// Appropriate implementation of this function will make Pod's re-scheduling accurate and performant.
	EventsToRegister(context.Context) ([]ClusterEventWithHint, error)
}

// PreFilterExtensions is an interface that is included in plugins that allow specifying
// callbacks to make incremental updates to its supposedly pre-calculated
// state.
type PreFilterExtensions interface {
	// AddPod is called by the framework while trying to evaluate the impact
	// of adding podToAdd to the node while scheduling podToSchedule.
	AddPod(ctx context.Context, state *CycleState, podToSchedule *corev1.Pod, podInfoToAdd *PodInfo, nodeInfo *NodeInfo) *Status
	// RemovePod is called by the framework while trying to evaluate the impact
	// of removing podToRemove from the node while scheduling podToSchedule.
	RemovePod(ctx context.Context, state *CycleState, podToSchedule *corev1.Pod, podInfoToRemove *PodInfo, nodeInfo *NodeInfo) *Status
}

// PreFilterPlugin is an interface that must be implemented by "PreFilter" plugins.
// These plugins are called at the beginning of the scheduling cycle.
type PreFilterPlugin interface {
	Plugin
	// PreFilter is called at the beginning of the scheduling cycle. All PreFilter
	// plugins must return success or the pod will be rejected. PreFilter could optionally
	// return a PreFilterResult to influence which nodes to evaluate downstream. This is useful
	// for cases where it is possible to determine the subset of nodes to process in O(1) time.
	// When PreFilterResult filters out some Nodes, the framework considers Nodes that are filtered out as getting "UnschedulableAndUnresolvable".
	// i.e., those Nodes will be out of the candidates of the preemption.
	//
	// When it returns Skip status, returned PreFilterResult and other fields in status are just ignored,
	// and coupled Filter plugin/PreFilterExtensions() will be skipped in this scheduling cycle.
	PreFilter(ctx context.Context, state *CycleState, p *corev1.Pod) (*PreFilterResult, *Status)
	// PreFilterExtensions returns a PreFilterExtensions interface if the plugin implements one,
	// or nil if it does not. A Pre-filter plugin can provide extensions to incrementally
	// modify its pre-processed info. The framework guarantees that the extensions
	// AddPod/RemovePod will only be called after PreFilter, possibly on a cloned
	// CycleState, and may call those functions more than once before calling
	// Filter again on a specific node.
	PreFilterExtensions() PreFilterExtensions
}

// FilterPlugin is an interface for Filter plugins. These plugins are called at the
// filter extension point for filtering out hosts that cannot run a pod.
// This concept used to be called 'predicate' in the original scheduler.
// These plugins should return "Success", "Unschedulable" or "Error" in Status.code.
// However, the scheduler accepts other valid codes as well.
// Anything other than "Success" will lead to exclusion of the given host from
// running the pod.
type FilterPlugin interface {
	Plugin
	// Filter is called by the scheduling framework.
	// All FilterPlugins should return "Success" to declare that
	// the given node fits the pod. If Filter doesn't return "Success",
	// it will return "Unschedulable", "UnschedulableAndUnresolvable" or "Error".
	//
	// "Error" aborts pod scheduling and puts the pod into the backoff queue.
	//
	// For the node being evaluated, Filter plugins should look at the passed
	// nodeInfo reference for this particular node's information (e.g., pods
	// considered to be running on the node) instead of looking it up in the
	// NodeInfoSnapshot because we don't guarantee that they will be the same.
	// For example, during preemption, we may pass a copy of the original
	// nodeInfo object that has some pods removed from it to evaluate the
	// possibility of preempting them to schedule the target pod.
	Filter(ctx context.Context, state *CycleState, pod *corev1.Pod, nodeInfo *NodeInfo) *Status
}

// PreScorePlugin is an interface for "PreScore" plugin. PreScore is an
// informational extension point. Plugins will be called with a list of nodes
// that passed the filtering phase. A plugin may use this data to update internal
// state or to generate logs/metrics.
type PreScorePlugin interface {
	Plugin
	// PreScore is called by the scheduling framework after a list of nodes
	// passed the filtering phase. All prescore plugins must return success or
	// the pod will be rejected
	// When it returns Skip status, other fields in status are just ignored,
	// and coupled Score plugin will be skipped in this scheduling cycle.
	PreScore(ctx context.Context, state *CycleState, pod *corev1.Pod, nodes []*NodeInfo) *Status
}

// ScoreExtensions is an interface for Score extended functionality.
type ScoreExtensions interface {
	// NormalizeScore is called for all node scores produced by the same plugin's "Score"
	// method. A successful run of NormalizeScore will update the scores list and return
	// a success status.
	NormalizeScore(ctx context.Context, state *CycleState, p *corev1.Pod, scores NodeScoreList) *Status
}

// ScorePlugin is an interface that must be implemented by "Score" plugins to rank
// nodes that passed the filtering phase.
type ScorePlugin interface {
	Plugin
	// Score is called on each filtered node. It must return success and an integer
	// indicating the rank of the node. All scoring plugins must return success or
	// the pod will be rejected.
	Score(ctx context.Context, state *CycleState, p *corev1.Pod, nodeInfo *NodeInfo) (int64, *Status)

	// ScoreExtensions returns a ScoreExtensions interface if it implements one, or nil if does not.
	ScoreExtensions() ScoreExtensions
}

// ReservePlugin is an interface for plugins with Reserve and Unreserve
// methods. These are meant to update the state of the plugin. This concept
// used to be called 'assume' in the original scheduler. These plugins should
// return only Success or Error in Status.code. However, the scheduler accepts
// other valid codes as well. Anything other than Success will lead to
// rejection of the pod.
type ReservePlugin interface {
	Plugin
	// Reserve is called by the scheduling framework when the scheduler cache is
	// updated. If this method returns a failed Status, the scheduler will call
	// the Unreserve method for all enabled ReservePlugins.
	Reserve(ctx context.Context, state *CycleState, p *corev1.Pod) *Status
	// Unreserve is called by the scheduling framework when a reserved pod was
	// rejected, an error occurred during reservation of subsequent plugins, or
	// in a later phase. The Unreserve method implementation must be idempotent
	// and may be called by the scheduler even if the corresponding Reserve
	// method for the same plugin was not called.
	Unreserve(ctx context.Context, state *CycleState, p *corev1.Pod)
}

// PreBindPlugin is an interface that must be implemented by "PreBind" plugins.
// These plugins are called before a pod being scheduled.
type PreBindPlugin interface {
	Plugin
	// PreBind is called before binding a pod. All prebind plugins must return
	// success or the pod will be rejected and won't be sent for binding.
	PreBind(ctx context.Context, state *CycleState, p *corev1.Pod, result ScheduleResult) *Status
}

// PermitPlugin is an interface that must be implemented by "Permit" plugins.
// These plugins are called before a pod is bound to a node.
type PermitPlugin interface {
	Plugin
	// Permit is called before binding a pod (and before prebind plugins). Permit
	// plugins are used to prevent or delay the binding of a Pod. A permit plugin
	// must return success or wait with timeout duration, or the pod will be rejected.
	// The pod will also be rejected if the wait timeout or the pod is rejected while
	// waiting. Note that if the plugin returns "wait", the framework will wait only
	// after running the remaining plugins given that no other plugin rejects the pod.
	Permit(ctx context.Context, state *CycleState, p *corev1.Pod) (*Status, time.Duration)
}

// BindPlugin is an interface that must be implemented by "Bind" plugins. Bind
// plugins are used to bind a pod to a Node.
type BindPlugin interface {
	Plugin
	// Bind plugins will not be called until all pre-bind plugins have completed. Each
	// bind plugin is called in the configured order. A bind plugin may choose whether
	// or not to handle the given Pod. If a bind plugin chooses to handle a Pod, the
	// remaining bind plugins are skipped. When a bind plugin does not handle a pod,
	// it must return Skip in its Status code. If a bind plugin returns an Error, the
	// pod is rejected and will not be bound.
	Bind(ctx context.Context, state *CycleState, p *corev1.Pod, result ScheduleResult) *Status
}

// PostBindPlugin is an interface that must be implemented by "PostBind" plugins.
// These plugins are called after a pod is successfully bound to a node.
type PostBindPlugin interface {
	Plugin
	// PostBind is called after a pod is successfully bound. These plugins are
	// informational. A common application of this extension point is for cleaning
	// up. If a plugin needs to clean-up its state after a pod is scheduled and
	// bound, PostBind is the extension point that it should register.
	PostBind(ctx context.Context, state *CycleState, p *corev1.Pod, result ScheduleResult)
}
