package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dingyu123456/lyra/internal/scheduler/backend/heap"
	lyralog "github.com/dingyu123456/lyra/pkg/logger"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
)

const (
	// DefaultPodMaxInUnschedulablePodsDuration is the default value for the maximum
	// time a pod can stay in unschedulablePods. If a pod stays in unschedulablePods
	// for longer than this value, the pod will be moved from unschedulablePods to
	// backoffQ or activeQ. If this value is empty, the default value (5min)
	// will be used.
	DefaultPodMaxInUnschedulablePodsDuration time.Duration = 5 * time.Minute
	// Scheduling queue names
	activeQ           = "Active"
	backoffQ          = "Backoff"
	unschedulablePods = "Unschedulable"

	preEnqueue = "PreEnqueue"
)

const (
	// 定义标准的退避时间常量
	DefaultPodInitialBackoffDuration = 1 * time.Second
	DefaultPodMaxBackoffDuration     = 10 * time.Second
)

// PriorityQueue 是 SchedulingQueue 的真实实现
type PriorityQueue struct {
	stop  chan struct{}
	clock clock.Clock

	// 队列主锁
	lock sync.RWMutex

	// 退避时间配置 (如：初始 1 秒，最大 10 秒)
	podInitialBackoffDuration         time.Duration
	podMaxBackoffDuration             time.Duration
	podMaxInUnschedulablePodsDuration time.Duration // 强制唤醒钉子户的最大时间 (如 5 分钟)

	// moveRequestCycle caches the sequence number of scheduling cycle when we
	// received a move request. Unschedulable pods in and before this scheduling
	// cycle will be put back to activeQueue if we were trying to schedule them
	// when we received move request.
	// TODO: this will be removed after SchedulingQueueHint goes to stable and the feature gate is removed.
	moveRequestCycle int64

	// --- 核心三级队列 ---

	// 1. 活跃队列：存放随时准备调度的 Pod (底层可以基于 heap 实现优先队列)
	activeQ activeQueuer

	// 2. 退避队列：存放刚调度失败的 Pod，按退避到期时间排序的堆 (Heap)
	podBackoffQ *heap.Heap[*framework.QueuedPodInfo]

	// 3. 不可调度池：存放因为“算力不足”被拒绝的 Pod 字典
	unschedulablePods *UnschedulablePods

	// preEnqueuePlugins registered preEnqueue plugins.
	preEnqueuePlugins []framework.PreEnqueuePlugin
	// --- 智能唤醒引擎 (防惊群效应) ---
	// 注册表：记录不同事件(如 NodeAdd)应该触发哪些插件的 QueueingHintFn
	queueingHintMap map[framework.ClusterEvent][]*queueingHintFunction

	// isSchedulingQueueHintEnabled indicates whether the feature gate for the scheduling queue is enabled.
	isSchedulingQueueHintEnabled bool
}

// priorityQueueOptions 存放所有的可选配置
type priorityQueueOptions struct {
	clock                             clock.Clock
	podInitialBackoffDuration         time.Duration
	podMaxBackoffDuration             time.Duration
	podMaxInUnschedulablePodsDuration time.Duration
	preEnqueuePlugins                 []framework.PreEnqueuePlugin
	queueingHintMap                   map[framework.ClusterEvent][]*queueingHintFunction
	logger                            *zap.Logger
}

// Option 定义配置注入函数
type Option func(*priorityQueueOptions)

// WithLogger 注入 zap logger
func WithLogger(logger *zap.Logger) Option {
	return func(o *priorityQueueOptions) {
		o.logger = logger
	}
}

// NewSchedulingQueue 初始化一个满足 SchedulingQueue 接口的实例
func NewSchedulingQueue(
	lessFn framework.LessFunc,
	informerFactory informers.SharedInformerFactory,
	opts ...Option,
) SchedulingQueue {
	return NewPriorityQueue(lessFn, informerFactory, opts...)
}

// NewPriorityQueue 创建并初始化优先级队列的核心结构
func NewPriorityQueue(
	lessFn framework.LessFunc,
	// 注意：在你的轻量级架构中，如果队列内部不查 Label 或 Namespace，
	// 这个 informerFactory 甚至可以只用来获取一些初始配置，或者直接去掉。
	informerFactory informers.SharedInformerFactory,
	opts ...Option,
) *PriorityQueue {
	// 1. 初始化默认配置
	options := priorityQueueOptions{
		clock:                             clock.RealClock{},
		podInitialBackoffDuration:         DefaultPodInitialBackoffDuration,
		podMaxBackoffDuration:             DefaultPodMaxBackoffDuration,
		podMaxInUnschedulablePodsDuration: DefaultPodMaxInUnschedulablePodsDuration,
		logger:                            zap.NewNop(),
	}

	// 2. 应用外部 Option
	for _, opt := range opts {
		opt(&options)
	}

	isSchedulingQueueHintEnabled := true

	// 3. 构造队列主体
	pq := &PriorityQueue{
		clock:                             options.clock,
		stop:                              make(chan struct{}),
		podInitialBackoffDuration:         options.podInitialBackoffDuration,
		podMaxBackoffDuration:             options.podMaxBackoffDuration,
		podMaxInUnschedulablePodsDuration: options.podMaxInUnschedulablePodsDuration,
		moveRequestCycle:                  -1,
		preEnqueuePlugins:                 options.preEnqueuePlugins,
		queueingHintMap:                   options.queueingHintMap,
		isSchedulingQueueHintEnabled:      isSchedulingQueueHintEnabled,
	}

	// ==========================================
	// 🌟 初始化核心三级队列 (对齐重构后的构造函数)
	// ==========================================

	// 1. 活跃队列 (Active Queue)
	// 修正：调用我们刚刚重构的 newActiveQueue，只传 queue, bool, logger
	pq.activeQ = newActiveQueue(
		heap.New(podInfoKeyFunc, heap.LessFunc[*framework.QueuedPodInfo](lessFn)),
		isSchedulingQueueHintEnabled,
		options.logger,
	)

	// 2. 退避队列 (Backoff Queue)
	// 修正：使用纯净的 heap.New，去掉所有 recorder
	pq.podBackoffQ = heap.New(podInfoKeyFunc, pq.podsCompareBackoffCompleted)

	// 3. 不可调度池 (Unschedulable Pool)
	// 修正：调用我们重构后的 newUnschedulablePods，只传 logger
	pq.unschedulablePods = newUnschedulablePods(options.logger)

	// 🌟 Lyra 剪裁：去掉了 K8s 原生的 pq.nsLister 和 pq.nominator
	// 因为我们不处理单集群内的复杂抢占，也不需要 Namespace 级别的配额检查。

	return pq
}

// clusterEvent has the event and involved objects.
type clusterEvent struct {
	event framework.ClusterEvent
	// oldObj is the object that involved this event.
	oldObj interface{}
	// newObj is the object that involved this event.
	newObj interface{}
}

// PreEnqueueCheck is a function type. It's used to build functions that
// run against a Pod and the caller can choose to enqueue or skip the Pod
// by the checking result.
type PreEnqueueCheck func(pod *corev1.Pod) bool

// 内部封装的小结构
type queueingHintFunction struct {
	PluginName     string
	QueueingHintFn framework.QueueingHintFn
}

// queueingStrategy 定义了 Pod 被事件唤醒后的去向
type queueingStrategy int

const (
	// queueSkip: 忽略此事件，继续在不可调度池里待着
	queueSkip queueingStrategy = iota
	// queueAfterBackoff: 放入退避队列 (BackoffQ)，等惩罚时间过了再进活跃队列
	queueAfterBackoff
	// queueImmediately: 无视惩罚时间，立刻放入活跃队列 (ActiveQ) 最高优先级抢占
	queueImmediately
)

// Add adds a pod to the active queue. It should be called only when a new pod
// is added so there is no chance the pod is already in active/unschedulable/backoff queues
func (p *PriorityQueue) Add(logger *zap.Logger, pod *corev1.Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()

	pInfo := p.newQueuedPodInfo(pod)
	if added := p.moveToActiveQ(logger, pInfo, framework.EventUnscheduledPodAdd.Label()); added {
		p.activeQ.broadcast()
	}
}

// newQueuedPodInfo builds a QueuedPodInfo object.
func (p *PriorityQueue) newQueuedPodInfo(pod *corev1.Pod, plugins ...string) *framework.QueuedPodInfo {
	now := p.clock.Now()
	// ignore this err since apiserver doesn't properly validate affinity terms
	// and we can't fix the validation for backwards compatibility.
	podInfo, _ := framework.NewPodInfo(pod)
	return &framework.QueuedPodInfo{
		PodInfo:                 podInfo,
		Timestamp:               now,
		InitialAttemptTimestamp: nil,
		UnschedulablePlugins:    sets.New(plugins...),
	}
}

// Pop removes the head of the active queue and returns it. It blocks if the
// activeQ is empty and waits until a new item is added to the queue. It
// increments scheduling cycle when a pod is popped.
// Note: This method should NOT be locked by the p.lock at any moment,
// as it would lead to scheduling throughput degradation.
func (p *PriorityQueue) Pop(logger *zap.Logger) (*framework.QueuedPodInfo, error) {
	return p.activeQ.pop(logger)
}

// Done must be called for pod returned by Pop. This allows the queue to
// keep track of which pods are currently being processed.
func (p *PriorityQueue) Done(pod types.UID) {
	if !p.isSchedulingQueueHintEnabled {
		// do nothing if schedulingQueueHint is disabled.
		// In that case, we don't have inFlightPods and inFlightEvents.
		return
	}
	p.activeQ.done(pod)
}

// isEventOfInterest 判断集群中发生的这个事件，有没有任何插件关心
func (p *PriorityQueue) isEventOfInterest(logger *zap.Logger, event framework.ClusterEvent) bool {
	if event.IsWildCard() {
		// 强制通配符事件，直接放行
		return true
	}

	// 🌟 降维优化：直接遍历单层 Map，不再查 Profile
	for eventToMatch := range p.queueingHintMap {
		if eventToMatch.Match(event) {
			return true
		}
	}

	logger.Debug("Received an event that isn't interested by any enabled plugins", zap.Any("event", event))
	return false
}

// isPodWorthRequeuing 核心大脑：判断 Pod 是否值得被重新放入活跃/退避队列
func (p *PriorityQueue) isPodWorthRequeuing(logger *zap.Logger, pInfo *framework.QueuedPodInfo, event framework.ClusterEvent, oldObj, newObj interface{}) queueingStrategy {
	// 1. 汇总曾经拒绝过该 Pod 的所有插件
	rejectorPlugins := pInfo.UnschedulablePlugins.Union(pInfo.PendingPlugins)
	if rejectorPlugins.Len() == 0 {
		logger.Debug("Worth requeuing because no failed plugins", zap.String("pod", pInfo.Pod.Name))
		return queueAfterBackoff
	}

	// 2. 处理通配符事件 (例如 EventForceActivate 强制激活)
	if event.IsWildCard() {
		if newObj != nil {
			if pod, ok := newObj.(*corev1.Pod); !ok || pod.UID != pInfo.Pod.UID {
				return queueSkip
			}
		}
		logger.Debug("Worth requeuing because the event is wildcard", zap.String("pod", pInfo.Pod.Name))
		return queueAfterBackoff
	}

	queueStrategy := queueSkip

	// 🌟 降维优化：直接遍历全局唯一的 queueingHintMap
	for eventToMatch, hintfns := range p.queueingHintMap {
		if !eventToMatch.Match(event) {
			continue // 事件不匹配，跳过
		}

		for _, hintfn := range hintfns {
			// 过滤：如果这个插件以前根本没有拒绝过这个 Pod，那这个事件对它无意义
			if !rejectorPlugins.Has(hintfn.PluginName) {
				continue
			}

			// 询问当初拒绝该 Pod 的插件：“现在值得重试吗？”
			hint, err := hintfn.QueueingHintFn(logger, pInfo.Pod, oldObj, newObj)
			if err != nil {
				logger.Error("QueueingHintFn returns error",
					zap.Error(err),
					zap.Any("event", event),
					zap.String("plugin", hintfn.PluginName),
					zap.String("pod", pInfo.Pod.Name))
				hint = framework.Queue
			}

			if hint == framework.QueueSkip {
				continue
			}

			// ==========================================
			// 优先级博弈
			// ==========================================
			if pInfo.PendingPlugins.Has(hintfn.PluginName) {
				// Gang 调度等待的兄弟到齐了，立刻最高优插队！
				return queueImmediately
			}

			if pInfo.PendingPlugins.Len() == 0 {
				// 普通的算力资源不足，等 Backoff 惩罚过了就行
				return queueAfterBackoff
			}

			// 如果既有 Filter 失败，又有其它挂起，先暂定 Backoff，继续遍历看能不能升舱
			queueStrategy = queueAfterBackoff
		}
	}

	return queueStrategy
}

type UnschedulablePods struct {
	// podInfoMap 存储真正因为资源不足而无法调度的 Pod
	podInfoMap map[string]*framework.QueuedPodInfo
	// 注入日志组件替代原生的 metrics
	logger *zap.Logger
}

// newUnschedulablePods 初始化不可调度池
func newUnschedulablePods(logger *zap.Logger) *UnschedulablePods {
	return &UnschedulablePods{
		podInfoMap: make(map[string]*framework.QueuedPodInfo),
		logger:     logger.With(zap.String("component", "unschedulable_pods")),
	}
}

// addOrUpdate 将 Pod 加入不可调度池
func (u *UnschedulablePods) addOrUpdate(pInfo *framework.QueuedPodInfo) {
	podID, _ := framework.GetPodKey(pInfo.Pod)
	u.podInfoMap[podID] = pInfo
	u.logger.Debug("Pod added to unschedulable pool", zap.String("podKey", podID))
}

// delete 将 Pod 从不可调度池中彻底抹除
// 🌟 优化点：删掉了原生 K8s 里的 gated 参数，因为你的 Lyra 没有用到 PodSchedulingGate 机制
func (u *UnschedulablePods) delete(pod *corev1.Pod, gated bool) {
	podID, _ := framework.GetPodKey(pod)
	if _, exists := u.podInfoMap[podID]; exists {
		delete(u.podInfoMap, podID)
		u.logger.Debug("Pod removed from unschedulable pool", zap.String("podKey", podID))
	}
}

// get returns the QueuedPodInfo if a pod with the same key is found.
func (u *UnschedulablePods) get(pod *corev1.Pod) *framework.QueuedPodInfo {
	podKey, _ := framework.GetPodKey(pod)
	return u.podInfoMap[podKey]
}

// clear 清空所有不可调度的 Pod (在特定的全局事件下触发)
func (u *UnschedulablePods) clear() {
	// 直接重新初始化 map，让 Go 的 GC 自动回收旧内存，效率极高
	u.podInfoMap = make(map[string]*framework.QueuedPodInfo)
	u.logger.Debug("Unschedulable pods pool cleared")
}

// requeuePodViaQueueingHint 尝试将 Pod 重新排队。返回它最终进入的队列名称。
// 注意：调用此方法前，必须已经持有 PriorityQueue 的主写锁 (p.lock)
func (p *PriorityQueue) requeuePodViaQueueingHint(logger *zap.Logger, pInfo *framework.QueuedPodInfo, strategy queueingStrategy, event string) string {
	// 1. 策略判定为跳过：原路打回不可调度池，继续睡
	if strategy == queueSkip {
		p.unschedulablePods.addOrUpdate(pInfo)
		return unschedulablePods
	}

	// 2. 策略判定为退避：且该 Pod 确实还没过惩罚时间，放入退避堆 (Heap)
	if strategy == queueAfterBackoff && p.isPodBackingoff(pInfo) {
		p.podBackoffQ.AddOrUpdate(pInfo)
		return backoffQ
	}

	// 3. 策略判定为立刻唤醒，或者惩罚时间已过：尝试冲击活跃队列！
	if added := p.moveToActiveQ(logger, pInfo, event); added {
		return activeQ
	}

	// 4. 冲击失败的兜底逻辑
	if pInfo.Gated {
		// 如果是因为门控(PreEnqueue)未通过，moveToActiveQ 内部会把它放回 unschedulablePods
		return unschedulablePods
	}

	// 异常兜底：重新塞回不可调度池
	p.unschedulablePods.addOrUpdate(pInfo)
	return unschedulablePods
}

// isPodBackingoff returns true if a pod is still waiting for its backoff timer.
// If this returns true, the pod should not be re-tried.
func (p *PriorityQueue) isPodBackingoff(podInfo *framework.QueuedPodInfo) bool {
	boTime := p.getBackoffTime(podInfo)
	return boTime.After(p.clock.Now())
}

// getBackoffTime returns the time that podInfo completes backoff
func (p *PriorityQueue) getBackoffTime(podInfo *framework.QueuedPodInfo) time.Time {
	duration := p.calculateBackoffDuration(podInfo)
	backoffTime := podInfo.Timestamp.Add(duration)
	return backoffTime
}

// calculateBackoffDuration is a helper function for calculating the backoffDuration
// based on the number of attempts the pod has made.
func (p *PriorityQueue) calculateBackoffDuration(podInfo *framework.QueuedPodInfo) time.Duration {
	if podInfo.Attempts == 0 {
		// When the Pod hasn't experienced any scheduling attempts,
		// they aren't obliged to get a backoff penalty at all.
		return 0
	}

	duration := p.podInitialBackoffDuration
	for i := 1; i < podInfo.Attempts; i++ {
		// Use subtraction instead of addition or multiplication to avoid overflow.
		if duration > p.podMaxBackoffDuration-duration {
			return p.podMaxBackoffDuration
		}
		duration += duration
	}
	return duration
}

// moveToActiveQ 尝试将 Pod 加入活跃队列，并从其他队列中抹除它的痕迹。
func (p *PriorityQueue) moveToActiveQ(logger *zap.Logger, pInfo *framework.QueuedPodInfo, event string) bool {
	// 记录之前的门控状态
	gatedBefore := pInfo.Gated
	// 执行前置检查插件 (如：依赖的外部存储准备好了没？)
	pInfo.Gated = !p.runPreEnqueuePlugins(context.Background(), pInfo)

	added := false

	// 🌟 绝对原子操作区：锁住活跃队列的条件变量
	p.activeQ.underLock(func(unlockedActiveQ unlockedActiveQueuer) {
		// 1. 如果 Pod 被门控卡住了 (没通过检查)
		if pInfo.Gated {
			// 如果它已经在活跃队列或退避队列里了，别乱动它
			if unlockedActiveQ.Has(pInfo) || p.podBackoffQ.Has(pInfo) {
				return
			}
			// 否则，乖乖回到不可调度池
			p.unschedulablePods.addOrUpdate(pInfo)
			return
		}

		// 2. 初始化首次尝试时间戳 (用于计算排队等待的 SLA)
		if pInfo.InitialAttemptTimestamp == nil {
			now := p.clock.Now()
			pInfo.InitialAttemptTimestamp = &now
		}

		// 3. 核心突破：加入活跃队列！
		unlockedActiveQ.AddOrUpdate(pInfo)
		added = true

		// 4. 毁尸灭迹：从其他队列彻底清理
		p.unschedulablePods.delete(pInfo.Pod, gatedBefore)
		p.podBackoffQ.Delete(pInfo) // 忽略找不到的错误

		logger.Debug("Pod moved to an internal scheduling queue",
			zap.String("pod", pInfo.Pod.Name),
			zap.String("event", event),
			zap.String("queue", activeQ))

		// 注：Lyra 暂无抢占特性，这里删除了原生的 p.AddNominatedPod 逻辑，极大地简化了代码
	})

	return added
}

// runPreEnqueuePlugins 运行入队前的门控插件。返回 true 表示通过，false 表示被拦截。
func (p *PriorityQueue) runPreEnqueuePlugins(ctx context.Context, pInfo *framework.QueuedPodInfo) bool {
	logger := lyralog.FromContext(ctx)
	pod := pInfo.Pod

	for _, pl := range p.preEnqueuePlugins {
		status := pl.PreEnqueue(ctx, pod)
		if status.IsSuccess() {
			continue
		}

		// 🌟 核心：被拒绝了，立刻记仇！把这个插件的名字写进病历本，下次事件发生时就找它确认
		pInfo.UnschedulablePlugins.Insert(pl.Name())

		if status.Code() == framework.Error {
			logger.Error("Unexpected error running PreEnqueue plugin",
				zap.Error(status.AsError()),
				zap.String("pod", pod.Name),
				zap.String("plugin", pl.Name()))
		} else {
			logger.Debug("Pod gated by PreEnqueue plugin",
				zap.String("pod", pod.Name),
				zap.String("plugin", pl.Name()),
				zap.String("status", status.Message()))
		}
		return false // 被拦截
	}

	return true // 畅通无阻
}

// MoveAllToActiveOrBackoffQueue 遍历不可调度池，根据事件将 Pod 重新放回活跃或退避队列。
// 它最后会通过条件变量发送广播，唤醒正在等待 Pop() 的调度协程。
func (p *PriorityQueue) MoveAllToActiveOrBackoffQueue(logger *zap.Logger, event framework.ClusterEvent, oldObj, newObj interface{}, preCheck PreEnqueueCheck) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.moveAllToActiveOrBackoffQueue(logger, event, oldObj, newObj, preCheck)
}

// NOTE: 调用此方法前必须确保持有 p.lock 写锁
func (p *PriorityQueue) moveAllToActiveOrBackoffQueue(logger *zap.Logger, event framework.ClusterEvent, oldObj, newObj interface{}, preCheck PreEnqueueCheck) {
	// 第一道全局闸门：如果整个系统里没有任何插件注册过这个事件，直接光速返回！
	// 防止在 10 万个不可调度 Pod 时进行无意义的遍历。
	if !p.isEventOfInterest(logger, event) {
		return
	}

	// 预分配切片容量，避免扩容带来的性能损耗
	unschedulablePods := make([]*framework.QueuedPodInfo, 0, len(p.unschedulablePods.podInfoMap))

	// 遍历“失败者集中营”
	for _, pInfo := range p.unschedulablePods.podInfoMap {
		// 第二道局部闸门：执行你传进来的匿名过滤函数（比如：不要唤醒刚刚失败的 Pod 本身）
		if preCheck == nil || preCheck(pInfo.Pod) {
			unschedulablePods = append(unschedulablePods, pInfo)
		}
	}

	// 进入真正的核心流转逻辑
	p.movePodsToActiveOrBackoffQueue(logger, unschedulablePods, event, oldObj, newObj)
}

// NOTE: 调用此方法前必须确保持有 p.lock 写锁
func (p *PriorityQueue) movePodsToActiveOrBackoffQueue(logger *zap.Logger, podInfoList []*framework.QueuedPodInfo, event framework.ClusterEvent, oldObj, newObj interface{}) {
	if len(podInfoList) == 0 || !p.isEventOfInterest(logger, event) {
		return
	}

	activated := false // 标记位：记录是否至少有一个 Pod 成功进入了活跃队列
	eventStr := fmt.Sprintf("%v", event)

	for _, pInfo := range podInfoList {
		// 1. 呼叫大脑，进行精准预判：“你看它还有救吗？”
		schedulingHint := p.isPodWorthRequeuing(logger, pInfo, event, oldObj, newObj)

		if schedulingHint == queueSkip {
			// 大脑说：没救，继续睡。
			logger.Debug("Event is not making pod schedulable",
				zap.String("pod", pInfo.Pod.Name),
				zap.Any("event", event))
			continue
		}

		// 2. 大脑说有救！先把旧账消了：从不可调度池里把它彻底抹除
		p.unschedulablePods.delete(pInfo.Pod, pInfo.Gated)

		// 3. 呼叫调度枢纽：根据大脑给出的策略，给它安排正确的去处
		queueName := p.requeuePodViaQueueingHint(logger, pInfo, schedulingHint, eventStr)

		logger.Debug("Pod evaluated and moved",
			zap.String("pod", pInfo.Pod.Name),
			zap.Any("event", event),
			zap.String("queue", queueName))

		// 4. 关键记录：只要它进了 ActiveQ，就说明有活干了！
		if queueName == activeQ {
			activated = true
		}
	}

	// （注：Lyra 中暂略原生的 inFlightEvents / moveRequestCycle 在途并发保护机制，
	// 若未来遇到正在调度的 Pod 与新事件并发冲突的罕见边界条件，可再做补齐。）

	// 5. 🌟 终极广播！惊蛰唤醒！
	if activated {
		// activeQ 内部拥有一个 sync.Cond (条件变量)。
		// 当调度循环发现 activeQ 为空时，它会调用 Wait() 挂起休眠，不吃一点 CPU。
		// broadcast() 会唤醒所有休眠的调度主协程：“快起床，有机器空出来了，开始干活！”
		p.activeQ.broadcast()
	}
}

// AddUnschedulableIfNotPresent 尝试将调度失败的 Pod 放入不可调度池。
// 如果在它调度期间发生了潜在的资源释放事件，则直接放入退避队列以防错过。
// AddUnschedulableIfNotPresent inserts a pod that cannot be scheduled into
// the queue, unless it is already in the queue. Normally, PriorityQueue puts
// unschedulable pods in `unschedulablePods`. But if there has been a recent move
// request, then the pod is put in `podBackoffQ`.
func (p *PriorityQueue) AddUnschedulableIfNotPresent(logger *zap.Logger, pInfo *framework.QueuedPodInfo, podSchedulingCycle int64) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	// 兜底释放锁
	defer p.Done(pInfo.Pod.UID)

	pod := pInfo.Pod
	podKey, _ := framework.GetPodKey(pod)

	// 1. 存在性防重校验
	if _, exists := p.unschedulablePods.podInfoMap[podKey]; exists {
		return fmt.Errorf("Pod %s is already present in unschedulable queue", pod.Name)
	}
	if p.activeQ.has(pInfo) { // 假设 activeQ 实现了 Has
		return fmt.Errorf("Pod %s is already present in the active queue", pod.Name)
	}
	if p.podBackoffQ.Has(pInfo) { // 假设 podBackoffQ 实现了 Has
		return fmt.Errorf("Pod %s is already present in the backoff queue", pod.Name)
	}

	// 2. 刷新入队时间戳，用于重新计算退避惩罚时间
	pInfo.Timestamp = p.clock.Now()

	// 3. 汇总所有的失败插件
	rejectorPlugins := pInfo.UnschedulablePlugins.Union(pInfo.PendingPlugins)

	// ==========================================
	// 🌟 4. 解决 In-Flight 在途并发竞争的神级判断
	// ==========================================
	// p.moveRequestCycle 记录了最近一次 "资源释放/事件唤醒" 发生时的调度周期号。
	// 如果 p.moveRequestCycle >= podSchedulingCycle，说明在当前 Pod 离开队列去调度的这段时间里，
	// 集群里发生了资源变动。这个变动极有可能刚好能满足当前 Pod 的需求！
	if p.moveRequestCycle >= podSchedulingCycle || rejectorPlugins.Len() == 0 {
		// 宁可错杀，不可放过！直接放进 BackoffQ（退避队列）。
		// 等退避时间一过，它就能立刻进入 ActiveQ 重试，绝对不会卡死在不可调度池里。
		p.podBackoffQ.AddOrUpdate(pInfo)
		logger.Debug("In-flight race detected or no rejectors, pod moved to BackoffQ",
			zap.String("pod", pod.Name),
			zap.Int64("podCycle", podSchedulingCycle),
			zap.Int64("moveCycle", p.moveRequestCycle))
	} else {
		// 调度期间风平浪静，乖乖回到不可调度池，等待下一个明确的事件唤醒它
		p.unschedulablePods.addOrUpdate(pInfo)
		logger.Debug("Pod safely moved to UnschedulablePods", zap.String("pod", pod.Name))
	}

	return nil
}

// SchedulingCycle returns current scheduling cycle.
func (p *PriorityQueue) SchedulingCycle() int64 {
	return p.activeQ.schedulingCycle()
}

// Run starts the goroutine to pump from podBackoffQ to activeQ
func (p *PriorityQueue) Run(logger *zap.Logger) {
	go wait.Until(func() {
		p.flushBackoffQCompleted(logger)
	}, 1.0*time.Second, p.stop)
	go wait.Until(func() {
		p.flushUnschedulablePodsLeftover(logger)
	}, 30*time.Second, p.stop)
}

// Close closes the priority queue.
func (p *PriorityQueue) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	close(p.stop)
	p.activeQ.close()
	p.activeQ.broadcast()
}

// flushBackoffQCompleted Moves all pods from backoffQ which have completed backoff in to activeQ
func (p *PriorityQueue) flushBackoffQCompleted(logger *zap.Logger) {
	p.lock.Lock()
	defer p.lock.Unlock()
	activated := false
	for {
		pInfo, ok := p.podBackoffQ.Peek()
		if !ok || pInfo == nil {
			break
		}
		pod := pInfo.Pod
		if p.isPodBackingoff(pInfo) {
			break
		}
		_, err := p.podBackoffQ.Pop()
		if err != nil {
			logger.Error("Unable to pop pod from backoff queue despite backoff completion",
				zap.Error(err),
				zap.String("pod", klog.KObj(pod).String()),
			)
			break
		}
		if added := p.moveToActiveQ(logger, pInfo, framework.BackoffComplete); added {
			activated = true
		}
	}

	if activated {
		p.activeQ.broadcast()
	}
}

// flushUnschedulablePodsLeftover moves pods which stay in unschedulablePods
// longer than podMaxInUnschedulablePodsDuration to backoffQ or activeQ.
func (p *PriorityQueue) flushUnschedulablePodsLeftover(logger *zap.Logger) {
	p.lock.Lock()
	defer p.lock.Unlock()

	var podsToMove []*framework.QueuedPodInfo
	currentTime := p.clock.Now()
	for _, pInfo := range p.unschedulablePods.podInfoMap {
		lastScheduleTime := pInfo.Timestamp
		if currentTime.Sub(lastScheduleTime) > p.podMaxInUnschedulablePodsDuration {
			podsToMove = append(podsToMove, pInfo)
		}
	}

	if len(podsToMove) > 0 {
		p.movePodsToActiveOrBackoffQueue(logger, podsToMove, framework.EventUnschedulableTimeout, nil, nil)
	}
}

// Update 处理队列中 Pod 的更新事件。
// 如果 Pod 已经处于 ActiveQ 或 BackoffQ，直接原地更新。
// 如果 Pod 处于 UnschedulablePods (不可调度池)，更新它可能会让它满足调度条件，此时会触发智能唤醒评估。
// 如果 Pod 哪儿都不在 (比如刚创建，或者是正在被调度协程处理的 In-Flight Pod)，则直接加入 ActiveQ。
// Update 处理队列中 Pod 的更新事件
// Update 处理队列中 Pod 的更新事件
// Update 处理队列中 Pod 的更新事件
func (p *PriorityQueue) Update(logger *zap.Logger, oldPod, newPod *corev1.Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()

	events := framework.PodSchedulingPropertiesChange(newPod, oldPod)

	// 1. 尝试在活跃或退避队列原地更新
	if oldPod != nil {
		oldPodInfo := newQueuedPodInfoForLookup(oldPod)
		if pInfo := p.activeQ.update(newPod, oldPodInfo); pInfo != nil {
			return
		}
		if pInfo, exists := p.podBackoffQ.Get(oldPodInfo); exists {
			pInfo.Pod = newPod
			p.podBackoffQ.AddOrUpdate(pInfo)
			return
		}
	}

	// 2. 尝试在不可调度池中进行策略评估
	if pInfo := p.unschedulablePods.get(newPod); pInfo != nil {
		pInfo.Pod = newPod // 原地更新

		// 预计算最高优先级策略，防状态机撕裂
		maxStrategy := queueSkip
		var triggerEvent framework.ClusterEvent

		for _, evt := range events {
			hint := p.isPodWorthRequeuing(logger, pInfo, evt, oldPod, newPod)

			if hint == queueImmediately {
				maxStrategy = queueImmediately
				triggerEvent = evt
				break
			}
			if hint == queueAfterBackoff && maxStrategy == queueSkip {
				maxStrategy = queueAfterBackoff
				triggerEvent = evt
			}
		}

		if maxStrategy != queueSkip {
			// 枢纽流转
			queue := p.requeuePodViaQueueingHint(logger, pInfo, maxStrategy, "PodUpdated")

			// ==========================================
			// 🌟 修复护栏：只有确认它去了其他队列，才能在老家注销户口！
			// 如果它因为某种原因 (如 Gated) 又被打回了老家，千万别删它！
			// ==========================================
			if queue != unschedulablePods {
				p.unschedulablePods.delete(pInfo.Pod, pInfo.Gated)
				logger.Debug("Pod evaluated across all events and moved",
					zap.String("pod", newPod.Name),
					zap.Any("triggerEvent", triggerEvent),
					zap.String("queue", queue))
			}

			if queue == activeQ {
				p.activeQ.broadcast()
			}
		} else {
			// 如果评估结果还是没救，确保其更新后的指针存回池子
			p.unschedulablePods.addOrUpdate(pInfo)
		}
		return
	}

	// 3. 它哪儿都不在 (新 Pod 或 In-Flight)，直接加入活跃队列
	pInfo := p.newQueuedPodInfo(newPod)
	if added := p.moveToActiveQ(logger, pInfo, "PodUpdated"); added {
		p.activeQ.broadcast()
	}
}

func (p *PriorityQueue) Delete(pod *corev1.Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// 构造轻量级查询外壳
	pInfo := newQueuedPodInfoForLookup(pod)

	// 1. 尝试从活跃队列 (ActiveQ) 中删除
	if err := p.activeQ.delete(pInfo); err != nil {
		// 活跃队列中没有找到该 Pod，它大概率在退避或不可调度池中

		// 2. 尝试从退避队列 (BackoffQ) 中删除
		// 假设你的 podBackoffQ.Delete 返回被删除的对象或直接无视找不到的错误
		_ = p.podBackoffQ.Delete(pInfo)

		// 3. 尝试从不可调度池 (UnschedulablePods) 中删除
		if pInfo = p.unschedulablePods.get(pod); pInfo != nil {
			p.unschedulablePods.delete(pod, pInfo.Gated)
		}
	}
}

// newQueuedPodInfoForLookup 构造一个仅供在队列中查找 (Lookup) 使用的轻量级对象。
// 避免实例化完整的 framework.PodInfo，提升高并发下的垃圾回收 (GC) 性能。
func newQueuedPodInfoForLookup(pod *corev1.Pod, plugins ...string) *framework.QueuedPodInfo {
	return &framework.QueuedPodInfo{
		PodInfo:              &framework.PodInfo{Pod: pod},
		UnschedulablePlugins: sets.New[string](plugins...), // 初始化空集合防止 nil panic
		PendingPlugins:       sets.New[string](),           // 初始化空集合防止 nil panic
	}
}

// GetPod searches for a pod in the activeQ, backoffQ, and unschedulablePods.
func (p *PriorityQueue) GetPod(name, namespace string) (pInfo *framework.QueuedPodInfo, ok bool) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	pInfoLookup := &framework.QueuedPodInfo{
		PodInfo: &framework.PodInfo{
			Pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
				},
			},
		},
	}
	if pInfo, ok = p.podBackoffQ.Get(pInfoLookup); ok {
		return pInfo, true
	}
	if pInfo = p.unschedulablePods.get(pInfoLookup.Pod); pInfo != nil {
		return pInfo, true
	}

	p.activeQ.underRLock(func(unlockedActiveQ unlockedActiveQueueReader) {
		pInfo, ok = unlockedActiveQ.Get(pInfoLookup)
	})
	return
}

// PodsInActiveQ returns all the Pods in the activeQ.
func (p *PriorityQueue) PodsInActiveQ() []*corev1.Pod {
	return p.activeQ.list()
}

func podInfoKeyFunc(pInfo *framework.QueuedPodInfo) string {
	return cache.NewObjectName(pInfo.Pod.Namespace, pInfo.Pod.Name).String()
}

func (p *PriorityQueue) podsCompareBackoffCompleted(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
	bo1 := p.getBackoffTime(pInfo1)
	bo2 := p.getBackoffTime(pInfo2)
	return bo1.Before(bo2)
}

// Activate moves the given pods to activeQ.
// If a pod isn't found in unschedulablePods or backoffQ and it's in-flight,
// the wildcard event is registered so that the pod will be requeued when it comes back.
// But, if a pod isn't found in unschedulablePods or backoffQ and it's not in-flight (i.e., completely unknown pod),
// Activate would ignore the pod.
func (p *PriorityQueue) Activate(logger *zap.Logger, pods map[string]*corev1.Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()

	activated := false
	for _, pod := range pods {
		if p.activate(logger, pod) {
			activated = true
			continue
		}

		// If this pod is in-flight, register the activation event (for when QHint is enabled) or update moveRequestCycle (for when QHints is disabled)
		// so that the pod will be requeued when it comes back.
		// Specifically in the in-tree plugins, this is for the scenario with the preemption plugin
		// where the async preemption API calls are all done or fail at some point before the Pod comes back to the queue.
		p.activeQ.addEventsIfPodInFlight(nil, pod, []framework.ClusterEvent{framework.EventForceActivate})
		p.moveRequestCycle = p.activeQ.schedulingCycle()
	}

	if activated {
		p.activeQ.broadcast()
	}
}
func (p *PriorityQueue) activate(logger *zap.Logger, pod *corev1.Pod) bool {
	var pInfo *framework.QueuedPodInfo
	// Verify if the pod is present in unschedulablePods or backoffQ.
	if pInfo = p.unschedulablePods.get(pod); pInfo == nil {
		// If the pod doesn't belong to unschedulablePods or backoffQ, don't activate it.
		// The pod can be already in activeQ.
		var exists bool
		pInfo, exists = p.podBackoffQ.Get(newQueuedPodInfoForLookup(pod))
		if !exists {
			return false
		}
	}

	if pInfo == nil {
		// Redundant safe check. We shouldn't reach here.
		logger.Error("Internal error: cannot obtain pInfo")
		return false
	}

	return p.moveToActiveQ(logger, pInfo, framework.ForceActivate)
}
