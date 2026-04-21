package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync/atomic"
	"time"

	lyralog "github.com/dingyu123456/lyra/pkg/logger"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/trace"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	"github.com/dingyu123456/lyra/internal/scheduler/framework/parallelize"
)

const (
	// numberOfHighestScoredNodesToReport is the number of node scores
	// to be included in ScheduleResult.
	numberOfHighestScoredNodesToReport = 3

	// SnapshotModeFull 环境变量值，启用全量快照模式
	SnapshotModeFull = "full"
)

// ScheduleOne 包含了单个 Pod 调度的完整工程化工作流
func (sched *Scheduler) ScheduleOne(ctx context.Context) {
	// 1. 从上下文中获取基础 Logger
	logger := lyralog.FromContext(ctx)

	podInfo, err := sched.NextPod(logger)
	if err != nil {
		logger.Error("Error while retrieving next pod from scheduling queue", zap.Error(err))
		return
	}
	// 防御性编程：防止队列关闭时拿到空指针
	if podInfo == nil || podInfo.Pod == nil {
		return
	}

	pod := podInfo.Pod

	// 2. 核心工程规范：Contextual Logging (上下文日志)
	// 给 Logger 打上该 Pod 的专属标签，并注入回 Context 中。
	// 这样后续 cycle 里打印的任何日志，都会自带这个 Pod 的追踪信息！
	logger = logger.With(zap.String("pod", pod.Name), zap.String("namespace", pod.Namespace))
	ctx = lyralog.NewContext(ctx, logger)

	if sched.skipPodSchedule(ctx, pod) {
		// 不放回队列，直接标记完成
		sched.SchedulingQueue.Done(pod.UID)
		return
	}

	logger.Debug("About to try and schedule pod")

	// 记录端到端调度的耗时
	start := time.Now()
	state := framework.NewCycleState()

	// 3. 调度周期上下文 (确保同步阶段的安全退出)
	schedulingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// ==========================================
	// 阶段一：同步调度周期 (Scheduling Cycle)
	// ==========================================
	scheduleResult, assumedPod, status := sched.schedulingCycle(schedulingCycleCtx, state, sched.Framework, podInfo, start)
	schedulingCycleLatency := time.Since(start)
	if !status.IsSuccess() {
		// 调度失败（没算力等），打回队列
		sched.FailureHandler(schedulingCycleCtx, sched.Framework, podInfo, status, start)
		return
	}
	logger.Info("SchedulingCycle completed",
		zap.Duration("scheduling_cycle_latency", schedulingCycleLatency),
		zap.String("cluster", scheduleResult.SuggestedCluster),
		zap.String("node", scheduleResult.SuggestedNode),
		zap.Int("feasible_nodes", scheduleResult.FeasibleNodes),
		zap.Int("evaluated_nodes", scheduleResult.EvaluatedNodes),
		zap.Int64("e2e_ns", schedulingCycleLatency.Nanoseconds()),
	)

	// ==========================================
	// 阶段二：异步绑定/下发周期 (Binding Cycle)
	// ==========================================
	// 极其重要：因为上一步已经完成了 Cache Assume，内存中已经锁定了资源，
	// 所以真实的下发动作必须放进 Goroutine，不能阻塞后面的任务出队！
	go func() {
		// TODO 注意细节：这里必须从最顶层的 ctx 派生，不能用 schedulingCycleCtx，因为它马上就会被外层的 defer cancel() 取消！
		bindingCycleCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		status := sched.bindingCycle(bindingCycleCtx, state, sched.Framework, scheduleResult, assumedPod, start)
		if !status.IsSuccess() {
			sched.handleBindingCycleError(bindingCycleCtx, state, sched.Framework, assumedPod, start, scheduleResult, status)
			return
		}
	}()
}

// schedulingCycle 同步调度周期：包含算法打分和内存预扣 (Assume)
func (sched *Scheduler) schedulingCycle(
	ctx context.Context,
	state *framework.CycleState,
	fwk framework.Framework,
	podInfo *framework.QueuedPodInfo,
	start time.Time,
) (framework.ScheduleResult, *framework.QueuedPodInfo, *framework.Status) {
	logger := lyralog.FromContext(ctx)
	pod := podInfo.Pod

	// 1. 算法管道 (PreFilter -> Filter -> Score)
	scheduleResult, err := sched.SchedulePod(ctx, fwk, state, pod)
	if err != nil {
		return framework.ScheduleResult{}, podInfo, framework.NewStatus(framework.Unschedulable).WithError(err)
	}

	// 2. 内存预扣 (Assume)
	// 这一步必须在同步周期内完成，因为它是独占状态机的写操作
	// Tell the cache to assume that a pod now is running on a given node, even though it hasn't been bound yet.
	// This allows us to keep scheduling without waiting on binding to occur.
	assumeStart := time.Now()
	assumedPodInfo := podInfo.DeepCopy()
	assumedPod := assumedPodInfo.Pod

	// assume modifies `assumedPod` by setting NodeName=scheduleResult.SuggestedHost
	// TODO 将scheduleResult中的集群，节点，GPU等决策结果写入assumedPod
	// TODO 之所以要将调度结果写到pod是因为assume底层会调用cache.addPod(),该方法不仅用于预扣逻辑，pod的事件回调函数也会用到，
	// TODO 比如说绑定成功后pod事件会触发cache.addPod()进行数据对账（实际分配的资源信息与决策信息是否相同），
	// TODO 而这时候是没有调度结果的，完全靠解析pod中的字段或者注解，所以我们在预扣逻辑中也要将决策信息以同样格式写到pod中。
	err = sched.assume(logger, assumedPod, scheduleResult)
	assumeDuration := time.Since(assumeStart)
	if err != nil {
		logger.Error("Failed to assume pod in cache", zap.Error(err))
		return framework.ScheduleResult{}, assumedPodInfo, framework.NewStatus(framework.Error, "Assume failed")
	}

	// Run the Reserve method of reserve plugins.
	if sts := fwk.RunReservePluginsReserve(ctx, state, assumedPod); !sts.IsSuccess() {
		// trigger un-reserve to clean up state associated with the reserved Pod
		fwk.RunReservePluginsUnreserve(ctx, state, assumedPod)
		if forgetErr := sched.Cache.ForgetPod(logger, assumedPod); forgetErr != nil {
			logger.Error("Scheduler cache ForgetPod failed", zap.Error(forgetErr))
		}
		return scheduleResult, assumedPodInfo, sts
	}

	// 4. Gang 调度前半场 (非阻塞注册)
	// 只是登记一下，瞬间返回，绝不卡死主循环！
	runPermitStatus := fwk.RunPermitPlugins(ctx, state, assumedPod)
	if !runPermitStatus.IsWait() && !runPermitStatus.IsSuccess() {
		// trigger un-reserve to clean up state associated with the reserved Pod
		fwk.RunReservePluginsUnreserve(ctx, state, assumedPod)
		if forgetErr := sched.Cache.ForgetPod(logger, assumedPod); forgetErr != nil {
			logger.Error("Scheduler cache ForgetPod failed", zap.Error(forgetErr))
		}
		return framework.ScheduleResult{}, nil, runPermitStatus
	}
	// 记录 Assume 阶段耗时
	logger.Info("[PERF] Assume timing",
		zap.String("pod", pod.Name),
		zap.Duration("assume_us", assumeDuration),
	)
	return scheduleResult, assumedPodInfo, runPermitStatus
}

// bindingCycle 异步绑定周期：包含 Gang 等待和 Karmada 联邦下发
func (sched *Scheduler) bindingCycle(
	ctx context.Context,
	state *framework.CycleState,
	fwk framework.Framework,
	scheduleResult framework.ScheduleResult,
	assumedPodInfo *framework.QueuedPodInfo,
	start time.Time,
) *framework.Status {
	logger := lyralog.FromContext(ctx)
	assumedPod := assumedPodInfo.Pod

	// Run "permit" plugins.
	// 1. Gang 调度后半场 (真正的死等)
	if status := fwk.WaitOnPermit(ctx, assumedPod); !status.IsSuccess() {
		return status
	}

	// 2. 你的联邦下发逻辑 (Dispatch / Bind)
	logger.Info("Dispatching pod to target",
		zap.String("cluster", scheduleResult.SuggestedCluster),
		zap.String("node", scheduleResult.SuggestedNode))

	err := sched.Dispatcher.Dispatch(ctx, assumedPod, scheduleResult)
	if err != nil {
		logger.Error("Failed to dispatch pod via Karmada", zap.Error(err))
		return framework.NewStatus(framework.Error, "Dispatch failed")
	}

	// Dispatch 成功后，标记绑定完成，启动 TTL 倒计时
	// 如果子集群在 TTL 时间内没有上报 Pod 事件进行对账，预扣资源会被自动回收
	if err := sched.Cache.FinishBinding(logger, assumedPod); err != nil {
		logger.Error("Failed to finish binding", zap.Error(err))
	}

	logger.Info("Pod successfully dispatched", zap.Duration("latency", time.Since(start)))
	return framework.NewStatus(framework.Success)
}

// handleBindingCycleError 异步周期失败后的回滚机制
func (sched *Scheduler) handleBindingCycleError(
	ctx context.Context,
	state *framework.CycleState,
	fwk framework.Framework,
	podInfo *framework.QueuedPodInfo,
	start time.Time,
	scheduleResult framework.ScheduleResult,
	status *framework.Status,
) {
	logger := lyralog.FromContext(ctx)
	logger.Warn("Binding cycle failed, rolling back", zap.String("reason", status.Message()))

	assumedPod := podInfo.Pod

	// 清理 Karmada 上的残留策略
	_ = sched.Dispatcher.TearDown(ctx, assumedPod)

	// 清理框架层插件的中间状态
	fwk.RunReservePluginsUnreserve(ctx, state, assumedPod)

	// 回滚 Cache 中的预扣资源
	if forgetErr := sched.Cache.ForgetPod(logger, assumedPod); forgetErr != nil {
		logger.Error("Scheduler cache ForgetPod failed", zap.Error(forgetErr))
	} else {
		// 极其核心的唤醒逻辑 (修复你指出的 Bug)：
		// 预扣失败导致资源释放，这等同于发生了一次 "Pod 被删除" 的集群事件。
		// 必须立刻通知队列，唤醒那些因为资源不足而挂起的其他任务。
		//
		// 传入的匿名函数用于过滤：不要唤醒刚刚失败的这个 Pod 本身，防止死循环。
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(
			logger,
			framework.EventAssignedPodDelete,
			assumedPod,
			nil,
			func(pod *v1.Pod) bool {
				return assumedPod.UID != pod.UID
			},
		)
	}

	// 将当前失败的任务打回退避队列 (BackoffQ)
	sched.FailureHandler(ctx, fwk, podInfo, status, start)
}

// handleSchedulingFailure 处理调度失败的 Pod。
// 它负责诊断失败原因、更新 Pod 状态，并决定是否将其安全送回调度队列。
func (sched *Scheduler) handleSchedulingFailure(ctx context.Context, fwk framework.Framework, podInfo *framework.QueuedPodInfo, status *framework.Status, start time.Time) {
	logger := zap.L() // 或者从 ctx 中提取你封装的 logger
	pod := podInfo.Pod

	// 1. 核心并发控制：确保 In-Flight 锁一定会被释放
	calledDone := false
	defer func() {
		if !calledDone {
			// 如果中途 abort (比如 Pod 被删了)，必须在这里显式调用 Done 释放主循环的并发锁
			sched.SchedulingQueue.Done(podInfo.Pod.UID)
		}
	}()

	err := status.AsError()
	errMsg := status.Message()

	// 2. 诊断并记录"失败病历本"
	// 从 FitError.Diagnosis 中提取所有失败的插件名称，
	// 这样 QueueingHint 引擎后续就能通过这个"病历本"精准唤醒它。
	if status.Code() == framework.Unschedulable {
		// 优先从 FitError.Diagnosis 提取所有失败插件
		var fitErr *framework.FitError
		if errors.As(err, &fitErr) && fitErr.Diagnosis.UnschedulablePlugins.Len() > 0 {
			for _, plugin := range fitErr.Diagnosis.UnschedulablePlugins.UnsortedList() {
				podInfo.UnschedulablePlugins.Insert(plugin)
			}
		} else if failedPlugin := status.Plugin(); failedPlugin != "" {
			podInfo.UnschedulablePlugins.Insert(failedPlugin)
		}
		logger.Debug("Unable to schedule pod; no fit; waiting",
			zap.String("pod", pod.Name),
			zap.Strings("failed_plugins", podInfo.UnschedulablePlugins.UnsortedList()),
			zap.String("err", errMsg))
	} else {
		// 调度器内部遇到了真正的 Error（如网络超时、内部组件 panic 等）
		logger.Error("Error scheduling pod; retrying",
			zap.String("pod", pod.Name),
			zap.Error(err))
	}

	// ==========================================
	// 🌟 3. 核心防御：Informer Cache 校验
	// ==========================================
	// 必须从 SharedInformer 获取集群中这个 Pod 的最新真实状态！
	podLister := fwk.SharedInformerFactory().Core().V1().Pods().Lister()
	cachedPod, e := podLister.Pods(pod.Namespace).Get(pod.Name)

	if e != nil {
		// 情况 A：Pod 已经被用户删除了 (Not Found)
		logger.Info("Pod doesn't exist in informer cache, abort requeue",
			zap.String("pod", pod.Name), zap.Error(e))
		return // 直接 return，defer 会执行 Done() 彻底销毁它
	}

	if cachedPod.Spec.NodeName != "" {
		// 情况 B：Pod 已经被其他调度器 (如 K8s default-scheduler) 或外部组件绑定了节点
		logger.Info("Pod has been assigned to node. Abort adding it back to queue.",
			zap.String("pod", pod.Name),
			zap.String("node", cachedPod.Spec.NodeName))
		return // 同样直接 return 丢弃
	}

	// ==========================================
	// 4. 安全回队
	// ==========================================
	// 必须使用 cachedPod 做 DeepCopy，因为在调度期间，Pod 的 Label/Annotation 极有可能被其他控制器修改过！
	// 带着最新状态回队列，才能保证后续评估的准确性。
	newPodInfo, _ := framework.NewPodInfo(cachedPod.DeepCopy())
	podInfo.PodInfo = newPodInfo

	// 带着当前的 SchedulingCycle (用于防并发漏洞) 塞回队列
	addErr := sched.SchedulingQueue.AddUnschedulableIfNotPresent(logger, podInfo, sched.SchedulingQueue.SchedulingCycle())
	if addErr != nil {
		logger.Error("Error occurred while adding pod back to queue", zap.Error(addErr))
	}

	// 标记回队成功，AddUnschedulableIfNotPresent 内部会负责调用 Done
	calledDone = true

	// ==========================================
	// 5. APIServer 状态更新 (可选，根据性能权衡)
	// ==========================================
	// 原生 K8s 会在这里向 APIServer 发送 Event，并更新 PodCondition 为 PodScheduled=False。
	// 对于 Lyra AI 算力调度器：如果集群吞吐量极大（每秒成百上千个 Pod 失败），
	// 频繁写 APIServer 会导致 ETCD 崩溃。
	// 建议做法：保留发 Event 的逻辑（方便排障），或者仅在状态真正变更时才去 Update Condition。

	if err != nil {
		// 发送事件到 Kubernetes (方便用户通过 kubectl describe pod 看到失败原因)
		// fwk.EventRecorder().Eventf(pod, nil, corev1.EventTypeWarning, "FailedScheduling", "Scheduling", errMsg)
	}
}

func (sched *Scheduler) skipPodSchedule(ctx context.Context, pod *v1.Pod) bool {
	logger := lyralog.FromContext(ctx)

	// Case 1: 已经被打上删除标记，取消调度
	if pod.DeletionTimestamp != nil {
		logger.Info("Skip scheduling deleting pod")
		return true
	}

	// Case 2: 并发导致的已经 Assume 过的任务二次入队，直接跳过
	isAssumed, _ := sched.Cache.IsAssumedPod(pod) // 需在 Cache 接口补充此方法
	if isAssumed {
		logger.Info("Skip scheduling already assumed pod")
		return true
	}

	return false
}

// schedulePod 是单次调度的纯算法推演阶段，负责找出最佳节点和分配具体的 GPU
func (sched *Scheduler) schedulePod(
	ctx context.Context,
	fwk framework.Framework,
	state *framework.CycleState,
	pod *v1.Pod,
) (result framework.ScheduleResult, err error) {

	logger := lyralog.FromContext(ctx)

	// 1. 初始化性能追踪器 (Trace)
	// 这个工具会在方法 defer 结束时，如果总耗时超过阈值，自动打印出完整的耗时火焰图日志
	// 阈值降低到 1ms，确保正常调度也能输出各阶段耗时（用于性能分析）
	traceObj := trace.New("Scheduling", trace.Field{Key: "namespace", Value: pod.Namespace}, trace.Field{Key: "name", Value: pod.Name})
	defer traceObj.LogIfLong(1 * time.Millisecond)

	// 2. 极速克隆全局二维快照
	traceStart := time.Now()
	var snapshotMode string
	if os.Getenv("LYRA_SNAPSHOT_MODE") == SnapshotModeFull {
		if err := sched.Cache.FullUpdateSnapshot(logger, sched.ClusterInfoSnapshot); err != nil {
			return result, err
		}
		snapshotMode = "full"
	} else {
		if err := sched.Cache.UpdateSnapshot(logger, sched.ClusterInfoSnapshot); err != nil {
			return result, err
		}
		snapshotMode = "incremental"
	}
	snapshotDuration := time.Since(traceStart)
	traceObj.Step("Snapshotting scheduler cache done")

	if sched.ClusterInfoSnapshot.NumClusters() == 0 {
		return result, ErrNoClustersAvailable
	}

	// 3. 寻找符合条件的节点 (内部包含了 PreFilter 和 Filter 逻辑)
	filterStart := time.Now()
	feasibleNodes, diagnosis, err := sched.findNodesThatFitPod(ctx, fwk, state, pod)
	if err != nil {
		return result, err
	}
	filterDuration := time.Since(filterStart)
	traceObj.Step("Computing predicates done")

	// 如果没有节点满足，返回携带有 Diagnosis (病历本) 的专属 FitError
	if len(feasibleNodes) == 0 {
		return result, &framework.FitError{
			Pod:         pod,
			NumAllNodes: sched.ClusterInfoSnapshot.NodeCount(),
			Diagnosis:   diagnosis,
		}
	}

	// 4. 优选打分 (Score)
	scoreStart := time.Now()
	var bestNode *framework.NodeInfo
	if len(feasibleNodes) == 1 {
		// 只有一个满足，直接钦定，省去打分开销
		bestNode = feasibleNodes[0]
	} else {
		// 多个节点满足，进入 prioritizeNodes 进行打分
		priorityList, err := sched.prioritizeNodes(ctx, fwk, state, pod, feasibleNodes)
		if err != nil {
			return result, err
		}

		// ⚠️ 注意：我们忽略第一个返回值 host 字符串，因为它没包含集群信息
		_, topScores, err := sched.selectHost(priorityList, numberOfHighestScoredNodesToReport)
		if err != nil {
			return result, err
		}

		// 从最高分记录中提取绝对唯一的二维坐标 (ClusterName + NodeName)
		winnerScore := topScores[0]
		bestClusterName := winnerScore.Cluster
		bestNodeName := winnerScore.Name

		// 桥接逻辑：直接从 Snapshot 进行 O(1) 精确查找，彻底杜绝跨集群重名 Bug！
		bestNode, err = sched.ClusterInfoSnapshot.GetNode(bestClusterName, bestNodeName)
		if err != nil {
			return result, err
		}
	}
	scoreDuration := time.Since(scoreStart)
	traceObj.Step("Prioritizing done")

	// 5. 异构算力精细化分配 (GPU Allocation)
	gpuStart := time.Now()
	// 在确定了宇宙最强节点后，从该节点上挑出具体的物理卡 UUID
	targetGPUs, err := sched.allocateGPUsOnNode(ctx, state, bestNode, pod)
	if err != nil {
		return result, err
	}
	gpuDuration := time.Since(gpuStart)
	traceObj.Step("GPU UUID allocation done")

	clonedClusters, clonedNodes := sched.ClusterInfoSnapshot.LastClonedStats()

	// 打印各阶段耗时日志（Info 级别 + [PERF] 前缀，方便 grep 解析）
	// 使用纳秒精度输出，避免 sub-microsecond 耗时被截断为 0
	logger.Info("[PERF] SchedulePod stage timing",
		zap.String("pod", pod.Name),
		zap.Int64("snapshot_ns", snapshotDuration.Nanoseconds()),
		zap.Int64("filter_ns", filterDuration.Nanoseconds()),
		zap.Int64("score_ns", scoreDuration.Nanoseconds()),
		zap.Int64("gpu_alloc_ns", gpuDuration.Nanoseconds()),
		zap.String("snapshot_mode", snapshotMode),
			zap.Int("feasible_nodes", len(feasibleNodes)),
		zap.Int("total_nodes", sched.ClusterInfoSnapshot.NodeCount()),
		zap.Int("cloned_clusters", clonedClusters),
		zap.Int("cloned_nodes", clonedNodes),
	)

	// 6. 组装结果返回 (去掉了 Reserve，它属于外层的 schedulingCycle)
	return framework.ScheduleResult{
		SuggestedCluster: bestNode.ClusterName,
		SuggestedNode:    bestNode.NodeName,
		SuggestedGPUs:    targetGPUs,
		EvaluatedNodes:   sched.ClusterInfoSnapshot.NodeCount(),
		FeasibleNodes:    len(feasibleNodes),
	}, nil
}

// findNodesThatFitPod 过滤二维快照中的节点，找出所有满足 Pod 算力需求的节点集合。
func (sched *Scheduler) findNodesThatFitPod(
	ctx context.Context,
	fwk framework.Framework,
	state *framework.CycleState,
	pod *v1.Pod,
) ([]*framework.NodeInfo, framework.Diagnosis, error) {

	logger := lyralog.FromContext(ctx)
	diagnosis := framework.Diagnosis{
		UnschedulablePlugins: sets.New[string](),
	}

	// 1. 宏观剪枝 (PreFilter) - 严格限定只过滤集群
	preRes, preFilterStatus, unscheduledPlugins := fwk.RunPreFilterPlugins(ctx, state, pod)
	diagnosis.UnschedulablePlugins = unscheduledPlugins

	if !preFilterStatus.IsSuccess() {
		if !preFilterStatus.IsRejected() {
			return nil, diagnosis, preFilterStatus.AsError()
		}
		logger.Debug("Status after running PreFilter plugins", zap.String("status", preFilterStatus.Message()))
		return nil, diagnosis, nil
	}

	// 2. 获取全局集群列表
	allClusters, err := sched.ClusterInfoSnapshot.List()
	if len(allClusters) == 0 {
		return nil, diagnosis, nil
	}

	// 3. 应用 PreFilter 的集群剪枝逻辑
	var nodesToEvaluate []*framework.NodeInfo

	if preRes != nil && !preRes.AllClusters() {
		// 场景 A: 插件明确指定了某些集群
		for clusterName := range preRes.ClusterNames {
			if clusterInfo, err := sched.ClusterInfoSnapshot.Get(clusterName); err == nil {
				// 提取只读快照中的节点指针
				for _, nodeInfo := range clusterInfo.Nodes {
					nodesToEvaluate = append(nodesToEvaluate, nodeInfo)
				}
			}
		}

		// 剪枝后如果没有可用节点，直接返回
		if len(nodesToEvaluate) == 0 {
			return nil, diagnosis, nil
		}
	} else {
		// 场景 B: 插件没有限制集群，平铺所有集群的节点
		// 给切片一个预估容量，减少底层的扩容拷贝开销
		nodesToEvaluate = make([]*framework.NodeInfo, 0, 100)
		for _, clusterInfo := range allClusters {
			for _, nodeInfo := range clusterInfo.Nodes {
				nodesToEvaluate = append(nodesToEvaluate, nodeInfo)
			}
		}
	}

	// 4. 微观并发过滤 (Filter)
	// 直接将所有的过滤压力交给并发引擎，不再算什么轮询 Index
	feasibleNodes, err := sched.findNodesThatPassFilters(ctx, fwk, state, pod, &diagnosis, nodesToEvaluate)
	if err != nil {
		return nil, diagnosis, err
	}

	return feasibleNodes, diagnosis, nil
}

// findNodesThatPassFilters 利用多核 CPU 并发执行 Filter 插件，找出所有满足条件的节点
func (sched *Scheduler) findNodesThatPassFilters(
	ctx context.Context,
	fwk framework.Framework,
	state *framework.CycleState,
	pod *v1.Pod,
	diagnosis *framework.Diagnosis,
	nodes []*framework.NodeInfo,
) ([]*framework.NodeInfo, error) {

	numAllNodes := len(nodes)
	if numAllNodes == 0 {
		return nil, nil
	}

	// 1.如果调度器根本没配置任何 Filter 插件，直接全量返回
	if !fwk.HasFilterPlugins() {
		return nodes, nil
	}

	// 提前分配好最大容量的切片，避免并发 append 时的扩容拷贝开销
	feasibleNodes := make([]*framework.NodeInfo, numAllNodes)
	var feasibleNodesLen int32

	// TODO这里后续得好好理解，能把优化逻辑讲出来
	// ⚠️ 并发安全魔法：预分配一个无锁状态数组。
	// 每个 Goroutine 只写自己负责的索引 i，彻底避免并发写 Set 导致的 Panic！
	statuses := make([]*framework.Status, numAllNodes)

	errCh := parallelize.NewErrorChannel()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 2. 定义单个节点的并发校验闭包
	checkNode := func(i int) {
		nodeInfo := nodes[i]

		// 执行具体的 Filter 流水线
		status := fwk.RunFilterPlugins(ctx, state, pod, nodeInfo)

		// 场景 A: 插件执行出错 (如连不上数据库)，立刻通知全局 Cancel 终止所有 Goroutine
		if status.Code() == framework.Error {
			errCh.SendErrorWithCancel(status.AsError(), cancel)
			return
		}

		// 场景 B: 过滤通过，利用原子操作 (Atomic) 安全放入结果集
		if status.IsSuccess() {
			length := atomic.AddInt32(&feasibleNodesLen, 1)
			feasibleNodes[length-1] = nodeInfo
		} else {
			// 场景 C: 过滤失败 (算力不足等)，无锁记录失败状态
			statuses[i] = status
		}
	}

	// 3. 启动高并发引擎轰炸节点池
	// Parallelizer 会根据当前机器的 GOMAXPROCS 动态分配 Worker 池
	fwk.Parallelizer().Until(ctx, numAllNodes, checkNode)

	// 4. 检查是否有致命 Error 打断了并发过程
	if err := errCh.ReceiveError(); err != nil {
		return nil, err
	}

	// 5. 并发结束后，主协程单线程汇总"病历本" (绝对安全)
	for _, status := range statuses {
		if status != nil && status.Plugin() != "" {
			diagnosis.UnschedulablePlugins.Insert(status.Plugin())
		}
	}

	// 6. 截断切片，返回真实通过过滤的节点
	return feasibleNodes[:feasibleNodesLen], nil
}

// prioritizeNodes 运行所有的 Score 插件给节点打分。
// 每个插件会给出一个分数，最终所有插件的分数乘以各自的权重相加，得出节点的总分。
func (sched *Scheduler) prioritizeNodes(
	ctx context.Context,
	fwk framework.Framework,
	state *framework.CycleState,
	pod *v1.Pod,
	nodes []*framework.NodeInfo,
) ([]framework.NodePluginScores, error) {

	logger := lyralog.FromContext(ctx)

	// 1. 如果完全没有配置打分插件，直接给所有节点赋满分 (或者 1 分)，免去后续流程
	if !fwk.HasScorePlugins() {
		result := make([]framework.NodePluginScores, 0, len(nodes))
		for _, node := range nodes {
			result = append(result, framework.NodePluginScores{
				Name:       node.NodeName,
				Cluster:    node.ClusterName, // [Lyra 独有] 记录该节点所属集群
				TotalScore: 1,
			})
		}
		return result, nil
	}

	// 2. 宏观打分准备 (PreScore)
	// 这是一个非常重要的钩子。比如某个打分插件需要知道"所有候选节点的显存总量"，
	// 它可以在 PreScore 阶段遍历一次 nodes 算好，存进 state 黑板里，
	// 避免在后面的并发 Score 阶段重复计算。
	preScoreStatus := fwk.RunPreScorePlugins(ctx, state, pod, nodes)
	if !preScoreStatus.IsSuccess() {
		return nil, preScoreStatus.AsError()
	}

	// 3. 微观并发打分 (Score)
	// 这里面会利用 parallelize.Parallelizer 并发给所有节点打分，并自动处理权重 (Weight)
	nodesScores, scoreStatus := fwk.RunScorePlugins(ctx, state, pod, nodes)
	if !scoreStatus.IsSuccess() {
		return nil, scoreStatus.AsError()
	}

	// 4. 打印详细的打分日志，极大地帮助后续开发排查"为什么选了这台机器"
	// 【修改点】使用 zap 的 Core().Enabled() 来拦截高性能 Debug 日志
	// 如果当前日志级别高于 Debug（比如线上环境是 Info），这个 if 就进不去，
	// 完美避免了成千上万个节点的 for 循环和字符串拼接开销！
	if logger.Core().Enabled(zap.DebugLevel) {
		for _, nodeScore := range nodesScores {
			logger.Debug("Calculated node's final score for pod",
				zap.String("cluster", nodeScore.Cluster),
				zap.String("node", nodeScore.Name),
				zap.Int64("score", nodeScore.TotalScore))

			for _, pluginScore := range nodeScore.Scores {
				logger.Debug("Plugin scored node",
					zap.String("plugin", pluginScore.Name),
					zap.String("cluster", nodeScore.Cluster),
					zap.String("node", nodeScore.Name),
					zap.Int64("score", pluginScore.Score))
			}
		}
	}

	return nodesScores, nil
}

var errEmptyPriorityList = errors.New("empty priorityList")

// selectHost 从打分列表中选出分数最高的节点。
// 使用了 container/heap 和水塘抽样（Reservoir Sampling）保证平局时的绝对公平。
func (sched *Scheduler) selectHost(nodeScoreList []framework.NodePluginScores, count int) (string, []framework.NodePluginScores, error) {
	if len(nodeScoreList) == 0 {
		return "", nil, errEmptyPriorityList
	}

	// 1. 初始化最大堆
	var h nodeScoreHeap = nodeScoreList
	heap.Init(&h)

	cntOfMaxScore := 1
	selectedIndex := 0

	// 2. 弹出堆顶（目前已知最高分的节点）
	sortedNodeScoreList := make([]framework.NodePluginScores, 0, count)
	sortedNodeScoreList = append(sortedNodeScoreList, heap.Pop(&h).(framework.NodePluginScores))

	// 3. 不断弹出剩下的节点，执行水塘抽样
	for h.Len() > 0 {
		ns := heap.Pop(&h).(framework.NodePluginScores)

		// 如果弹出的节点分数比当前最高分低，且我们已经收集够了 count 个节点，直接结束
		if ns.TotalScore != sortedNodeScoreList[0].TotalScore && len(sortedNodeScoreList) == count {
			break
		}

		// 如果遇到了同等最高分的节点，启动水塘抽样
		if ns.TotalScore == sortedNodeScoreList[0].TotalScore {
			cntOfMaxScore++
			if rand.Intn(cntOfMaxScore) == 0 {
				// 以 1/cntOfMaxScore 的概率替换掉选中的头号种子
				selectedIndex = cntOfMaxScore - 1
			}
		}

		sortedNodeScoreList = append(sortedNodeScoreList, ns)
	}

	// 4. 将最终胜出者换到数组的首位
	if selectedIndex != 0 {
		previous := sortedNodeScoreList[0]
		sortedNodeScoreList[0] = sortedNodeScoreList[selectedIndex]
		sortedNodeScoreList[selectedIndex] = previous
	}

	// 5. 截断数组，只返回要求的个数（用于日志打印等）
	if len(sortedNodeScoreList) > count {
		sortedNodeScoreList = sortedNodeScoreList[:count]
	}

	// 完美返回：胜出者的节点名称，最高分列表，无错误
	return sortedNodeScoreList[0].Name, sortedNodeScoreList, nil
}

// nodeScoreHeap is a heap of framework.NodePluginScores.
type nodeScoreHeap []framework.NodePluginScores

// nodeScoreHeap implements heap.Interface.
var _ heap.Interface = &nodeScoreHeap{}

func (h *nodeScoreHeap) Len() int           { return len(*h) }
func (h *nodeScoreHeap) Less(i, j int) bool { return (*h)[i].TotalScore > (*h)[j].TotalScore }
func (h *nodeScoreHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *nodeScoreHeap) Push(x interface{}) {
	*h = append(*h, x.(framework.NodePluginScores))
}

func (h *nodeScoreHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// allocateGPUsOnNode 在确认节点可用后，挑选出最适合的物理卡 UUID 列表
func (sched *Scheduler) allocateGPUsOnNode(ctx context.Context, state *framework.CycleState, bestNode *framework.NodeInfo, pod *corev1.Pod) ([]string, error) {
	// TODO 这块要注意，pod对GPU的需求量可能需要在preFilter插件里面执行，然后将执行结果放到*framework.CycleState
	// TODO 就是k8s做亲和性解析的地方，因为GPU需求在过滤和评分阶段应该都要使用，在这里解析太晚了，也不能在每个节点过滤或评分时都解析一遍
	// TODO 这里主要是因为preFilter阶段插件的执行是串行的，我们需要把pod的GPU需求解析放到第一个插件，然后第二个可能是过滤集群，这样过滤集群的时候
	// TODO 可以直接从cycleState中拿到pod的GPU需求
	// TODO 这里是个性能优化
	data, err := state.Read(framework.KeyPodGPUReq)
	if err != nil {
		return nil, fmt.Errorf("failed to read GPU req from cycle state: %w", err)
	}

	req, ok := data.(*framework.GPURequirement)
	if !ok {
		return nil, fmt.Errorf("invalid type in cycle state, expected *framework.GPURequirement")
	}

	// 如果这个任务根本不需要 GPU，直接返回空
	if req.NumCards == 0 {
		return nil, nil
	}

	var feasibleGPUs []*framework.GPUInfo

	// 1. 过滤满足硬性条件的物理卡
	for _, gpu := range bestNode.GPUs {
		// 🌟 型号二次校验 (兜底逻辑)：如果业务端指定了型号，当前卡必须在候选名单中
		if len(req.Types) > 0 {
			typeMatched := false
			for _, allowedType := range req.Types {
				if gpu.Type == allowedType {
					typeMatched = true
					break
				}
			}
			// 型号不匹配，直接跳过这张卡
			if !typeMatched {
				continue
			}
		}

		// 计算当前卡的剩余资源
		freeMem := gpu.AllocatableMem - gpu.RequestedMem
		freeCore := gpu.AllocatableCore - gpu.RequestedCore

		// 检查这块卡能不能塞下当前的单份 vGPU 需求
		if freeMem >= req.MemReq && freeCore >= req.CoreReq {
			feasibleGPUs = append(feasibleGPUs, gpu)
		}
	}

	// 2. 核心红线校验：能够满足单份需求的独立物理卡数量，是否 >= 任务申请的总卡数？
	if len(feasibleGPUs) < req.NumCards {
		return nil, fmt.Errorf("insufficient distinct physical GPUs on node %s (found %d, need %d)",
			bestNode.NodeName, len(feasibleGPUs), req.NumCards)
	}

	// 3. Best-Fit 二维装箱打分 (归一化残差排序)
	sort.Slice(feasibleGPUs, func(i, j int) bool {
		gpuI := feasibleGPUs[i]
		gpuJ := feasibleGPUs[j] // 修正了上一版的拼写错误

		// 计算装入该任务后，卡 I 和卡 J 的剩余资源比例 (比例越小，说明装得越满，碎片越少)
		remMemRatioI := float64(gpuI.AllocatableMem-gpuI.RequestedMem-req.MemReq) / float64(gpuI.AllocatableMem)
		remCoreRatioI := float64(gpuI.AllocatableCore-gpuI.RequestedCore-req.CoreReq) / float64(gpuI.AllocatableCore)
		scoreI := remMemRatioI + remCoreRatioI

		remMemRatioJ := float64(gpuJ.AllocatableMem-gpuJ.RequestedMem-req.MemReq) / float64(gpuJ.AllocatableMem)
		remCoreRatioJ := float64(gpuJ.AllocatableCore-gpuJ.RequestedCore-req.CoreReq) / float64(gpuJ.AllocatableCore)
		scoreJ := remMemRatioJ + remCoreRatioJ

		// 优先选择残余率小的卡 (Best-Fit 最佳适应)
		return scoreI < scoreJ
	})

	// 4. 拔卡！按装箱最优解截取所需的物理卡 UUID
	var targetUUIDs []string
	for i := 0; i < req.NumCards; i++ {
		targetUUIDs = append(targetUUIDs, feasibleGPUs[i].UUID)
	}

	return targetUUIDs, nil
}

// assume signals to the cache that a pod is already in the cache, so that binding can be asynchronous.
// assume modifies `assumed`.
func (sched *Scheduler) assume(logger *zap.Logger, assumed *corev1.Pod, result framework.ScheduleResult) error {
	// 1. 设置原生的 NodeName (K8s 原生调度契约)
	assumed.Spec.NodeName = result.SuggestedNode

	if assumed.Annotations == nil {
		assumed.Annotations = make(map[string]string)
	}

	// 2. 注入集群信息 (Karmada 契约)
	if result.SuggestedCluster != "" {
		karmadaNs := fmt.Sprintf("karmada-es-%s", result.SuggestedCluster)
		assumed.Annotations[framework.AnnotationKarmadaNamespace] = karmadaNs
	}

	// 3. 注入 GPU 分配凭证 (Hami 契约)
	// 将脏活累活全部交给 framework 包处理
	framework.InjectHamiVGPUAnnotation(assumed, result.SuggestedGPUs)

	logger.Debug("Constructed Pod annotations for assume",
		zap.String("node", assumed.Spec.NodeName),
		zap.Any("annotations", assumed.Annotations))

	// 4. 内存乐观预扣
	if err := sched.Cache.AssumePod(logger, assumed); err != nil {
		logger.Error("Scheduler cache AssumePod failed", zap.Error(err))
		return err
	}

	return nil
}
