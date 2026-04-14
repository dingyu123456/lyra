package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	"github.com/dingyu123456/lyra/internal/scheduler/framework/parallelize"
	lyralog "github.com/dingyu123456/lyra/pkg/logger"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

const (
	// Specifies the maximum timeout a permit plugin can return.
	maxTimeout = 15 * time.Minute
)

// frameworkImpl is the component responsible for initializing and running scheduler
// plugins.
type frameworkImpl struct {
	registry             Registry
	snapshotSharedLister framework.SharedLister
	waitingPods          *waitingPodsMap
	scorePluginWeight    map[string]int
	preEnqueuePlugins    []framework.PreEnqueuePlugin
	enqueueExtensions    []framework.EnqueueExtensions
	queueSortPlugins     []framework.QueueSortPlugin
	preFilterPlugins     []framework.PreFilterPlugin
	filterPlugins        []framework.FilterPlugin
	preScorePlugins      []framework.PreScorePlugin
	scorePlugins         []framework.ScorePlugin
	reservePlugins       []framework.ReservePlugin
	preBindPlugins       []framework.PreBindPlugin
	bindPlugins          []framework.BindPlugin
	postBindPlugins      []framework.PostBindPlugin
	permitPlugins        []framework.PermitPlugin

	// pluginsMap contains all plugins, by name.
	pluginsMap map[string]framework.Plugin

	clientSet       clientset.Interface
	karmadaClient   karmadaclientset.Interface
	kubeConfig      *restclient.Config
	eventRecorder   events.EventRecorder
	informerFactory informers.SharedInformerFactory
	logger          *zap.Logger

	metricsRecorder          *metrics.MetricAsyncRecorder
	percentageOfNodesToScore *int32

	framework.PodActivator

	parallelizer parallelize.Parallelizer
}

// 🌟 3. NewFramework 核心初始化
func NewFramework(ctx context.Context, r Registry, plugins *Plugins, opts ...Option) (framework.Framework, error) {
	// 1. 初始化默认参数并应用 Options
	options := frameworkOptions{
		parallelizer: parallelize.NewParallelizer(parallelize.DefaultParallelism),
		logger:       lyralog.FromContext(ctx),
	}
	for _, opt := range opts {
		opt(&options)
	}

	// 2. 构造基础 Framework 实例
	f := &frameworkImpl{
		registry:             r,
		snapshotSharedLister: options.snapshotSharedLister,
		scorePluginWeight:    make(map[string]int),
		waitingPods:          NewWaitingPodsMap(), // 初始化并发安全的 Wait 队列
		clientSet:            options.clientSet,
		karmadaClient:        options.karmadaClient,
		informerFactory:      options.informerFactory,
		parallelizer:         options.parallelizer,
		logger:               options.logger,
		pluginsMap:           make(map[string]framework.Plugin),
	}

	// 如果没有传入配置，直接返回一个空壳（用于某些极端测试）
	if plugins == nil {
		return f, nil
	}

	// 3. 提取所有需要初始化的插件名单，去重
	pluginsNeeded := make(map[string]struct{})
	appendNeeded := func(set PluginSet) {
		for _, pl := range set.Enabled {
			pluginsNeeded[pl.Name] = struct{}{}
		}
	}

	appendNeeded(plugins.PreEnqueue)
	appendNeeded(plugins.QueueSort)
	appendNeeded(plugins.PreFilter)
	appendNeeded(plugins.Filter)
	appendNeeded(plugins.PreScore)
	appendNeeded(plugins.Score)
	appendNeeded(plugins.Reserve)
	appendNeeded(plugins.Permit)
	appendNeeded(plugins.PreBind)
	appendNeeded(plugins.Bind)
	appendNeeded(plugins.PostBind)

	// 4. 调用工厂方法真正实例化插件
	for name := range pluginsNeeded {
		factory, ok := r[name]
		if !ok {
			return nil, fmt.Errorf("plugin %q not found in the registry", name)
		}

		// 这里简化了 args 的传递，如果后续你的插件需要配置文件，可以在这里传入 runtime.Object
		var args runtime.Object = nil

		p, err := factory(ctx, args, f)
		if err != nil {
			return nil, fmt.Errorf("initializing plugin %q: %w", name, err)
		}
		f.pluginsMap[name] = p

		// 如果插件实现了 EnqueueExtensions (用于精确重试机制)，将其记录下来
		if ext, ok := p.(framework.EnqueueExtensions); ok {
			f.enqueueExtensions = append(f.enqueueExtensions, ext)
		}
	}

	// 5. 按扩展点将插件组装到流水线上
	var err error
	if f.preEnqueuePlugins, err = getExtension[framework.PreEnqueuePlugin](f, plugins.PreEnqueue); err != nil {
		return nil, err
	}
	if f.queueSortPlugins, err = getExtension[framework.QueueSortPlugin](f, plugins.QueueSort); err != nil {
		return nil, err
	}
	if f.preFilterPlugins, err = getExtension[framework.PreFilterPlugin](f, plugins.PreFilter); err != nil {
		return nil, err
	}
	if f.filterPlugins, err = getExtension[framework.FilterPlugin](f, plugins.Filter); err != nil {
		return nil, err
	}
	if f.preScorePlugins, err = getExtension[framework.PreScorePlugin](f, plugins.PreScore); err != nil {
		return nil, err
	}
	if f.scorePlugins, err = getExtension[framework.ScorePlugin](f, plugins.Score); err != nil {
		return nil, err
	}
	if f.reservePlugins, err = getExtension[framework.ReservePlugin](f, plugins.Reserve); err != nil {
		return nil, err
	}
	if f.permitPlugins, err = getExtension[framework.PermitPlugin](f, plugins.Permit); err != nil {
		return nil, err
	}
	if f.preBindPlugins, err = getExtension[framework.PreBindPlugin](f, plugins.PreBind); err != nil {
		return nil, err
	}
	if f.bindPlugins, err = getExtension[framework.BindPlugin](f, plugins.Bind); err != nil {
		return nil, err
	}
	if f.postBindPlugins, err = getExtension[framework.PostBindPlugin](f, plugins.PostBind); err != nil {
		return nil, err
	}

	// 6. 处理打分权重
	for _, pl := range plugins.Score.Enabled {
		if pl.Weight == 0 {
			return nil, fmt.Errorf("score plugin %q is not configured with weight", pl.Name)
		}
		f.scorePluginWeight[pl.Name] = int(pl.Weight)
	}

	// 7. 兜底校验
	if len(f.queueSortPlugins) != 1 {
		return nil, fmt.Errorf("exactly one queue sort plugin is required, but got %d", len(f.queueSortPlugins))
	}
	if len(f.bindPlugins) == 0 {
		return nil, fmt.Errorf("at least one bind plugin is needed")
	}

	f.logger.Info("Lyra Framework initialized successfully", zap.Int("total_plugins", len(f.pluginsMap)))
	return f, nil
}

// --- 辅助泛型提取器，用于将抽象的 plugin 转换为具体接口切片 ---
func getExtension[T framework.Plugin](f *frameworkImpl, set PluginSet) ([]T, error) {
	var result []T
	for _, plConfig := range set.Enabled {
		p, ok := f.pluginsMap[plConfig.Name]
		if !ok {
			return nil, fmt.Errorf("plugin %q was not initialized", plConfig.Name)
		}
		typedPlugin, ok := p.(T)
		if !ok {
			return nil, fmt.Errorf("plugin %q does not implement the requested extension point", plConfig.Name)
		}
		result = append(result, typedPlugin)
	}
	return result, nil
}

// RunPreFilterPlugins 执行所有的宏观剪枝插件 (纯集群级别)
func (f *frameworkImpl) RunPreFilterPlugins(
	ctx context.Context,
	state *framework.CycleState,
	pod *corev1.Pod,
) (*framework.PreFilterResult, *framework.Status, sets.Set[string]) {

	// 准备记录需要跳过的插件
	skipPlugins := sets.New[string]()
	defer func() {
		// 退出时注入到共享黑板中，供 Filter 阶段跳过使用
		state.SkipFilterPlugins = skipPlugins
	}()

	var result *framework.PreFilterResult

	// ⚠️ 修改点 1：变量名改为 pluginsWithClusters
	pluginsWithClusters := sets.New[string]()

	for _, pl := range f.preFilterPlugins {
		// 1. 执行具体插件
		r, s := f.runPreFilterPlugin(ctx, pl, state, pod)

		// 2. 处理 Skip：如果插件说自己不用参与本次调度，加入免检名单
		if s.IsSkip() {
			skipPlugins.Insert(pl.Name())
			continue
		}

		// 3. 处理失败：直接打断并返回！(性能极致优化)
		if !s.IsSuccess() {
			s.SetPlugin(pl.Name())
			// 直接返回该插件的名字，用于后续 QueueingHint 精准唤醒
			return nil, s, sets.New[string](pl.Name())
		}

		// 4. 处理集群缩小：如果插件指定了特定集群，记录下来
		// ⚠️ 修改点 2：改为调用 AllClusters()
		if !r.AllClusters() {
			pluginsWithClusters.Insert(pl.Name())
		}

		// 5. 交集魔法：将当前插件的集群要求与之前的要求取交集
		result = result.Merge(r)

		// 6. 如果交集变成了 0，说明没有任何一个集群能同时满足这些插件的苛刻要求！
		// ⚠️ 修改点 3：改为判断 ClusterNames 的长度
		if !result.AllClusters() && len(result.ClusterNames) == 0 {
			// ⚠️ 修改点 4：日志和报错信息改为 cluster(s)
			msg := fmt.Sprintf("cluster(s) didn't satisfy plugin(s) %v simultaneously", sets.List(pluginsWithClusters))
			if len(pluginsWithClusters) == 1 {
				msg = fmt.Sprintf("cluster(s) didn't satisfy plugin %v", sets.List(pluginsWithClusters)[0])
			}

			// 返回 Unschedulable (失败)，并交出导致集群交集为空的所有罪魁祸首插件名单
			return result, framework.NewStatus(framework.Unschedulable, msg), pluginsWithClusters
		}
	}

	return result, framework.NewStatus(framework.Success, ""), pluginsWithClusters
}

// runPreFilterPlugin 极简的插件调用包装器
func (f *frameworkImpl) runPreFilterPlugin(
	ctx context.Context,
	pl framework.PreFilterPlugin,
	state *framework.CycleState,
	pod *corev1.Pod,
) (*framework.PreFilterResult, *framework.Status) {
	return pl.PreFilter(ctx, state, pod)
}

// RunFilterPlugins 遍历并执行所有已注册的 Filter 插件。
// 如果有任何一个插件返回非 Success，则判定该节点不适合运行该 Pod，并立刻打断循环。
func (f *frameworkImpl) RunFilterPlugins(
	ctx context.Context,
	state *framework.CycleState,
	pod *corev1.Pod,
	nodeInfo *framework.NodeInfo, // 这里的 NodeInfo 已经自带了 ClusterName 属性
) *framework.Status {
	// 获取日志器，保持日志上下文连贯
	// logger := lyralog.FromContext(ctx)

	for _, pl := range f.filterPlugins {
		// 1. 🚄 免检通道：如果在 PreFilter 阶段该插件申请了 Skip，这里直接跳过！
		// 结合之前的并发过滤引擎，这里能极大地节省无用的 CPU 计算开销。
		if state.SkipFilterPlugins.Has(pl.Name()) {
			continue
		}

		// 2. 执行核心过滤逻辑
		status := f.runFilterPlugin(ctx, pl, state, pod, nodeInfo)

		// 3. 失败拦截与病历本登记
		if !status.IsSuccess() {
			// 如果返回的不是单纯的“资源不满足”(Rejected)，而是系统内部崩溃，则包装一下 Error 以便排查
			if !status.IsRejected() {
				status = framework.AsStatus(fmt.Errorf("running %q filter plugin: %w", pl.Name(), status.AsError()))
			}

			// ⚠️ 极其核心：把拒绝调度的罪魁祸首插件名盖上章！
			// 这个名字会一路向上抛出，最终装进 QueueingHint 的病历本中。
			status.SetPlugin(pl.Name())
			return status
		}
	}

	// 所有插件都亮绿灯，节点过滤通过！
	return framework.NewStatus(framework.Success, "")
}

// runFilterPlugin 极简的调用包装器
func (f *frameworkImpl) runFilterPlugin(
	ctx context.Context,
	pl framework.FilterPlugin,
	state *framework.CycleState,
	pod *corev1.Pod,
	nodeInfo *framework.NodeInfo,
) *framework.Status {
	// 剔除了原生 K8s 臃肿的 metrics 探针代码，让高并发 Goroutine 纯粹执行业务逻辑
	return pl.Filter(ctx, state, pod, nodeInfo)
}

// RunReservePluginsReserve 遍历执行所有配置的 Reserve 插件。
func (f *frameworkImpl) RunReservePluginsReserve(ctx context.Context, state *framework.CycleState, pod *corev1.Pod) *framework.Status {
	// 从统一的 Context 中提取 Logger
	logger := lyralog.FromContext(ctx).With(
		zap.String("pod", pod.Name),
		zap.String("node", pod.Spec.NodeName),
	)

	// 顺序执行所有的 Reserve 插件
	for _, pl := range f.reservePlugins {
		status := f.runReservePluginReserve(ctx, logger, pl, state, pod)

		if !status.IsSuccess() {
			if status.IsRejected() {
				logger.Debug("Pod rejected by Reserve plugin",
					zap.String("plugin", pl.Name()),
					zap.String("message", status.Message()))
				status.SetPlugin(pl.Name())
				return status
			}

			err := status.AsError()
			logger.Error("Reserve plugin failed",
				zap.String("plugin", pl.Name()),
				zap.Error(err))
			return framework.AsStatus(fmt.Errorf("running Reserve plugin %q: %w", pl.Name(), err))
		}
	}

	return nil
}

func (f *frameworkImpl) runReservePluginReserve(ctx context.Context, logger *zap.Logger, pl framework.ReservePlugin, state *framework.CycleState, pod *corev1.Pod) *framework.Status {
	// 记录插件执行时间 (可接入你自己的 Metrics 系统)
	startTime := time.Now()
	status := pl.Reserve(ctx, state, pod)

	logger.Debug("Reserve plugin executed",
		zap.String("plugin", pl.Name()),
		zap.Duration("duration", time.Since(startTime)))

	return status
}

// RunReservePluginsUnreserve 遍历执行所有配置的 Unreserve 插件。
// 核心逻辑：必须以与 Reserve 完全相反的顺序（LIFO）执行。
func (f *frameworkImpl) RunReservePluginsUnreserve(ctx context.Context, state *framework.CycleState, pod *corev1.Pod) {
	logger := lyralog.FromContext(ctx).With(
		zap.String("pod", pod.Name),
		zap.String("node", pod.Spec.NodeName),
	)

	// 逆序执行 Unreserve 操作
	for i := len(f.reservePlugins) - 1; i >= 0; i-- {
		pl := f.reservePlugins[i]
		f.runReservePluginUnreserve(ctx, logger, pl, state, pod)
	}
}

func (f *frameworkImpl) runReservePluginUnreserve(ctx context.Context, logger *zap.Logger, pl framework.ReservePlugin, state *framework.CycleState, pod *corev1.Pod) {
	startTime := time.Now()

	// Unreserve 方法不返回状态，它必须尽最大努力完成状态清理
	pl.Unreserve(ctx, state, pod)

	logger.Debug("Unreserve plugin executed",
		zap.String("plugin", pl.Name()),
		zap.Duration("duration", time.Since(startTime)))
}

// RunPermitPlugins 遍历执行所有的 Permit 插件。
func (f *frameworkImpl) RunPermitPlugins(ctx context.Context, state *framework.CycleState, pod *corev1.Pod) *framework.Status {
	logger := lyralog.FromContext(ctx).With(
		zap.String("pod", pod.Name),
		zap.String("node", pod.Spec.NodeName),
	)

	pluginsWaitTime := make(map[string]time.Duration)
	statusCode := framework.Success

	for _, pl := range f.permitPlugins {
		status, timeout := f.runPermitPlugin(ctx, logger, pl, state, pod)

		if !status.IsSuccess() {
			// 1. 遭遇强拒绝 (Reject)
			if status.IsRejected() {
				logger.Debug("Pod rejected by Permit plugin",
					zap.String("plugin", pl.Name()),
					zap.String("message", status.Message()))
				return status.WithPlugin(pl.Name())
			}

			// 2. 遭遇要求等待 (Wait)
			if status.IsWait() {
				// 防御性编程：不允许无休止的死锁等待
				if timeout > maxTimeout {
					logger.Warn("Permit plugin requested timeout greater than max allowed, capping to max",
						zap.String("plugin", pl.Name()),
						zap.Duration("requested", timeout),
						zap.Duration("max", maxTimeout))
					timeout = maxTimeout
				}
				pluginsWaitTime[pl.Name()] = timeout
				statusCode = framework.Wait
			} else {
				// 3. 遭遇系统级内部错误 (Error)
				err := status.AsError()
				logger.Error("Permit plugin failed with error",
					zap.String("plugin", pl.Name()),
					zap.Error(err))
				return framework.AsStatus(fmt.Errorf("running Permit plugin %q: %w", pl.Name(), err)).WithPlugin(pl.Name())
			}
		}
	}

	// 汇总阶段：只要有人提出 Wait（且没人 Reject），就将 Pod 放入等待队列
	if statusCode == framework.Wait {
		// newWaitingPod 负责将 Pod 包装成一个可以被异步唤醒的等待对象
		waitingPod := newWaitingPod(pod, pluginsWaitTime)
		f.waitingPods.add(waitingPod) // 存入框架内部的并发安全等待池

		msg := fmt.Sprintf("one or more plugins asked to wait and no plugin rejected pod %s", pod.Name)
		logger.Debug("Pod put into Permit waiting state", zap.Any("pluginsWaitTime", pluginsWaitTime))
		return framework.NewStatus(framework.Wait, msg)
	}

	return nil // Success
}

// runPermitPlugin 内部的辅助执行器
func (f *frameworkImpl) runPermitPlugin(ctx context.Context, logger *zap.Logger, pl framework.PermitPlugin, state *framework.CycleState, pod *corev1.Pod) (*framework.Status, time.Duration) {
	startTime := time.Now()

	status, timeout := pl.Permit(ctx, state, pod)

	logger.Debug("Permit plugin executed",
		zap.String("plugin", pl.Name()),
		zap.String("status", status.Code().String()),
		zap.Duration("duration", time.Since(startTime)))

	return status, timeout
}

func (f *frameworkImpl) SetPodActivator(a framework.PodActivator) {
	f.PodActivator = a
}

// PreEnqueuePlugins returns the registered preEnqueue plugins.
func (f *frameworkImpl) PreEnqueuePlugins() []framework.PreEnqueuePlugin {
	return f.preEnqueuePlugins
}

// EnqueueExtensions returns the registered reenqueue plugins.
func (f *frameworkImpl) EnqueueExtensions() []framework.EnqueueExtensions {
	return f.enqueueExtensions
}

// QueueSortFunc returns the function to sort pods in scheduling queue
func (f *frameworkImpl) QueueSortFunc() framework.LessFunc {
	if f == nil {
		// If frameworkImpl is nil, simply keep their order unchanged.
		// NOTE: this is primarily for tests.
		return func(_, _ *framework.QueuedPodInfo) bool { return false }
	}

	if len(f.queueSortPlugins) == 0 {
		panic("No QueueSort plugin is registered in the frameworkImpl.")
	}

	// Only one QueueSort plugin can be enabled.
	return f.queueSortPlugins[0].Less
}

// RunPreScorePlugins 运行所有的 PreScore 插件。
func (f *frameworkImpl) RunPreScorePlugins(
	ctx context.Context,
	state *framework.CycleState,
	pod *corev1.Pod,
	nodes []*framework.NodeInfo,
) *framework.Status {
	skipPlugins := sets.New[string]()
	defer func() {
		state.SkipScorePlugins = skipPlugins
	}()

	for _, pl := range f.preScorePlugins {
		status := f.runPreScorePlugin(ctx, pl, state, pod, nodes)
		if status.IsSkip() {
			skipPlugins.Insert(pl.Name())
			continue
		}
		if !status.IsSuccess() {
			return framework.AsStatus(fmt.Errorf("running PreScore plugin %q: %w", pl.Name(), status.AsError()))
		}
	}

	return nil
}

func (f *frameworkImpl) runPreScorePlugin(ctx context.Context, pl framework.PreScorePlugin, state *framework.CycleState, pod *corev1.Pod, nodes []*framework.NodeInfo) *framework.Status {
	return pl.PreScore(ctx, state, pod, nodes)
}

// RunScorePlugins 运行配置的评分插件，并返回每个节点的总分。
func (f *frameworkImpl) RunScorePlugins(
	ctx context.Context,
	state *framework.CycleState,
	pod *corev1.Pod,
	nodes []*framework.NodeInfo,
) ([]framework.NodePluginScores, *framework.Status) {
	allNodePluginScores := make([]framework.NodePluginScores, len(nodes))
	numPlugins := len(f.scorePlugins)

	// 1. 过滤掉在 PreScore 阶段要求跳过的插件
	plugins := make([]framework.ScorePlugin, 0, numPlugins)
	pluginToNodeScores := make(map[string]framework.NodeScoreList, numPlugins)
	for _, pl := range f.scorePlugins {
		if state.SkipScorePlugins.Has(pl.Name()) {
			continue
		}
		plugins = append(plugins, pl)
		pluginToNodeScores[pl.Name()] = make(framework.NodeScoreList, len(nodes))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := parallelize.NewErrorChannel()

	if len(plugins) > 0 {
		// 2. 并行计算每个节点在每个插件下的得分
		f.Parallelizer().Until(ctx, len(nodes), func(index int) {
			node := nodes[index]
			for _, pl := range plugins {
				s, status := f.runScorePlugin(ctx, pl, state, pod, node)
				if !status.IsSuccess() {
					err := fmt.Errorf("plugin %q failed with: %w", pl.Name(), status.AsError())
					errCh.SendErrorWithCancel(err, cancel)
					return
				}
				pluginToNodeScores[pl.Name()][index] = framework.NodeScore{
					Name:    node.NodeName,
					Cluster: node.ClusterName,
					Score:   s,
				}
			}
		})
		if err := errCh.ReceiveError(); err != nil {
			return nil, framework.AsStatus(fmt.Errorf("running Score plugins: %w", err))
		}
	}

	// 3. 并行运行归一化（Normalize）逻辑
	f.Parallelizer().Until(ctx, len(plugins), func(index int) {
		pl := plugins[index]
		if pl.ScoreExtensions() == nil {
			return
		}
		nodeScoreList := pluginToNodeScores[pl.Name()]
		status := pl.ScoreExtensions().NormalizeScore(ctx, state, pod, nodeScoreList)
		if !status.IsSuccess() {
			err := fmt.Errorf("plugin %q failed during normalize: %w", pl.Name(), status.AsError())
			errCh.SendErrorWithCancel(err, cancel)
			return
		}
	})
	if err := errCh.ReceiveError(); err != nil {
		return nil, framework.AsStatus(fmt.Errorf("running Normalize on Score plugins: %w", err))
	}

	// 4. 并行应用权重并计算每个节点的总分
	f.Parallelizer().Until(ctx, len(nodes), func(index int) {
		node := nodes[index]
		nodePluginScores := framework.NodePluginScores{
			Name:    node.NodeName,
			Cluster: node.ClusterName,
			Scores:  make([]framework.PluginScore, len(plugins)),
		}

		for i, pl := range plugins {
			weight := f.scorePluginWeight[pl.Name()]
			nodeScoreList := pluginToNodeScores[pl.Name()]
			score := nodeScoreList[index].Score

			// 权重计算
			weightedScore := score * int64(weight)
			nodePluginScores.Scores[i] = framework.PluginScore{
				Name:  pl.Name(),
				Score: weightedScore,
			}
			nodePluginScores.TotalScore += weightedScore
		}
		allNodePluginScores[index] = nodePluginScores
	})

	if err := errCh.ReceiveError(); err != nil {
		return nil, framework.AsStatus(fmt.Errorf("applying weights on Score plugins: %w", err))
	}

	return allNodePluginScores, nil
}

func (f *frameworkImpl) runScorePlugin(ctx context.Context, pl framework.ScorePlugin, state *framework.CycleState, pod *corev1.Pod, nodeInfo *framework.NodeInfo) (int64, *framework.Status) {
	return pl.Score(ctx, state, pod, nodeInfo)
}

// Parallelizer returns a parallelizer holding parallelism for scheduler.
func (f *frameworkImpl) Parallelizer() parallelize.Parallelizer {
	return f.parallelizer
}

// WaitOnPermit 阻塞等待，直到在 Permit 阶段被挂起 (Wait) 的 Pod 被允许 (Allowed) 或拒绝 (Rejected)
func (f *frameworkImpl) WaitOnPermit(ctx context.Context, pod *corev1.Pod) *framework.Status {
	// 1. 从并发安全的 waitingPods 池中尝试获取该 Pod
	waitingPod := f.waitingPods.get(pod.UID)
	if waitingPod == nil {
		// 如果不在等待队列中，说明要么已经处理完毕，要么根本没被要求等待，直接放行
		return nil
	}

	// 确保方法退出时，将该 Pod 从等待字典中清理掉，防止内存泄漏
	defer f.waitingPods.remove(pod.UID)

	logger := lyralog.FromContext(ctx).With(
		zap.String("pod", pod.Name),
		zap.String("namespace", pod.Namespace),
	)

	logger.Debug("Pod is blocking and waiting on permit")

	// 2. 核心阻塞逻辑：等待内部的 channel 传回状态。
	// 当外部调用 waitingPod.Allow() 或 waitingPod.Reject()，或者超时时间到达时，会向该 channel 发送信号
	s := <-waitingPod.s

	// 3. 处理唤醒后的结果
	if !s.IsSuccess() {
		// 场景 A: 被明确拒绝 (例如其他并发调度的资源抢占失败，或者超时被强制打回)
		if s.IsRejected() {
			logger.Debug("Pod rejected while waiting on permit",
				zap.String("message", s.Message()),
				zap.String("plugin", s.Plugin()))
			return s
		}

		// 场景 B: 发生了不可预期的系统级错误
		err := s.AsError()
		logger.Error("Failed waiting on permit for pod", zap.Error(err))
		return framework.AsStatus(fmt.Errorf("waiting on permit for pod: %w", err)).WithPlugin(s.Plugin())
	}

	// 场景 C: 成功放行，进入下一个绑定周期 (Bind Cycle)
	logger.Debug("Pod permit allowed, ready for binding")
	return nil
}

// RunPreBindPlugins 运行所有的 PreBind 插件。
// 如果有任何一个插件返回失败，则拒绝该 Pod。
func (f *frameworkImpl) RunPreBindPlugins(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, result framework.ScheduleResult) *framework.Status {
	logger := lyralog.FromContext(ctx).With(
		zap.String("pod", pod.Name),
		zap.String("cluster", result.SuggestedCluster),
		zap.String("node", result.SuggestedNode),
	)

	for _, pl := range f.preBindPlugins {
		status := f.runPreBindPlugin(ctx, pl, state, pod, result)

		if !status.IsSuccess() {
			if status.IsRejected() {
				logger.Debug("Pod rejected by PreBind plugin",
					zap.String("plugin", pl.Name()),
					zap.String("message", status.Message()))
				status.SetPlugin(pl.Name())
				return status
			}

			err := status.AsError()
			logger.Error("PreBind plugin failed",
				zap.String("plugin", pl.Name()),
				zap.Error(err))
			return framework.AsStatus(fmt.Errorf("running PreBind plugin %q: %w", pl.Name(), err))
		}
	}

	return nil
}

func (f *frameworkImpl) runPreBindPlugin(ctx context.Context, pl framework.PreBindPlugin, state *framework.CycleState, pod *corev1.Pod, result framework.ScheduleResult) *framework.Status {
	// 注意：这里假设你将 PreBindPlugin 的参数从 nodeName 改成了 result framework.ScheduleResult
	return pl.PreBind(ctx, state, pod, result)
}

// RunBindPlugins 运行配置的 Bind 插件，直到有一个插件返回非 Skip 状态。
// 这就是你要调用 Karmada API 下发 PP/OP 的扩展点。
func (f *frameworkImpl) RunBindPlugins(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, result framework.ScheduleResult) *framework.Status {
	if len(f.bindPlugins) == 0 {
		return framework.NewStatus(framework.Skip, "no bind plugins configured")
	}

	logger := lyralog.FromContext(ctx).With(
		zap.String("pod", pod.Name),
		zap.String("cluster", result.SuggestedCluster),
		zap.String("node", result.SuggestedNode),
	)

	for _, pl := range f.bindPlugins {
		status := f.runBindPlugin(ctx, pl, state, pod, result)

		// 如果当前插件选择不处理该 Pod (比如只处理特定类型的 Pod)，则跳过，交给下一个插件
		if status.IsSkip() {
			continue
		}

		if !status.IsSuccess() {
			if status.IsRejected() {
				logger.Debug("Pod rejected by Bind plugin",
					zap.String("plugin", pl.Name()),
					zap.String("message", status.Message()))
				status.SetPlugin(pl.Name())
				return status
			}

			err := status.AsError()
			logger.Error("Bind plugin failed",
				zap.String("plugin", pl.Name()),
				zap.Error(err))
			return framework.AsStatus(fmt.Errorf("running Bind plugin %q: %w", pl.Name(), err))
		}

		// 🌟 核心逻辑：只要有一个 Bind 插件成功执行（比如成功下发了 PP/OP），就直接返回成功，不再执行后续 Bind 插件。
		return status
	}

	// 如果所有插件都 Skip 了，返回 Skip
	return framework.NewStatus(framework.Skip, "")
}

func (f *frameworkImpl) runBindPlugin(ctx context.Context, bp framework.BindPlugin, state *framework.CycleState, pod *corev1.Pod, result framework.ScheduleResult) *framework.Status {
	// 注意：这里假设你将 BindPlugin 的参数从 nodeName 改成了 result framework.ScheduleResult
	return bp.Bind(ctx, state, pod, result)
}

// RunPostBindPlugins 运行所有的 PostBind 插件。
// 这是一个信息通知类扩展点，通常用于清理缓存、记录日志或触发其他异步操作，不影响调度结果。
func (f *frameworkImpl) RunPostBindPlugins(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, result framework.ScheduleResult) {
	// 如果你的 frameworkImpl 结构体中还没有 postBindPlugins 字段，记得在 registry 或者 types 中加上
	// type frameworkImpl struct { ... postBindPlugins []framework.PostBindPlugin ... }

	// 这里通常不需要复杂的错误处理，因为到了 PostBind 阶段，Pod 事实上已经调度成功了
	for _, pl := range f.postBindPlugins { // 假设 f 包含 postBindPlugins
		f.runPostBindPlugin(ctx, pl, state, pod, result)
	}
}

func (f *frameworkImpl) runPostBindPlugin(ctx context.Context, pl framework.PostBindPlugin, state *framework.CycleState, pod *corev1.Pod, result framework.ScheduleResult) {
	// 注意：这里假设你将 PostBindPlugin 的参数从 nodeName 改成了 result framework.ScheduleResult
	pl.PostBind(ctx, state, pod, result)
}

func (f *frameworkImpl) SnapshotSharedLister() framework.SharedLister {
	return f.snapshotSharedLister
}

// IterateOverWaitingPods acquires a read lock and iterates over the WaitingPods map.
func (f *frameworkImpl) IterateOverWaitingPods(callback func(framework.WaitingPod)) {
	f.waitingPods.iterate(callback)
}

// GetWaitingPod returns a reference to a WaitingPod given its UID.
func (f *frameworkImpl) GetWaitingPod(uid types.UID) framework.WaitingPod {
	if wp := f.waitingPods.get(uid); wp != nil {
		return wp
	}
	return nil // Returning nil instead of *waitingPod(nil).
}

// RejectWaitingPod rejects a WaitingPod given its UID.
// The returned value indicates if the given pod is waiting or not.
func (f *frameworkImpl) RejectWaitingPod(uid types.UID) bool {
	if waitingPod := f.waitingPods.get(uid); waitingPod != nil {
		waitingPod.Reject("", "removed")
		return true
	}
	return false
}

// HasFilterPlugins returns true if at least one filter plugin is defined.
func (f *frameworkImpl) HasFilterPlugins() bool {
	return len(f.filterPlugins) > 0
}

// HasScorePlugins returns true if at least one score plugin is defined.
func (f *frameworkImpl) HasScorePlugins() bool {
	return len(f.scorePlugins) > 0
}

// ClientSet returns a kubernetes clientset.
func (f *frameworkImpl) ClientSet() clientset.Interface {
	return f.clientSet
}

// KarmadaClient returns a karmada clientset.
func (f *frameworkImpl) KarmadaClient() karmadaclientset.Interface {
	return f.karmadaClient
}

// Logger returns the logger.
func (f *frameworkImpl) Logger() *zap.Logger {
	return f.logger
}

// SharedInformerFactory returns a shared informer factory.
func (f *frameworkImpl) SharedInformerFactory() informers.SharedInformerFactory {
	return f.informerFactory
}

// Close closes each plugin, when they implement io.Closer interface.
func (f *frameworkImpl) Close() error {
	var errs []error
	for name, plugin := range f.pluginsMap {
		if closer, ok := plugin.(io.Closer); ok {
			err := closer.Close()
			if err != nil {
				errs = append(errs, fmt.Errorf("%s failed to close: %w", name, err))
				// We try to close all plugins even if we got errors from some.
			}
		}
	}
	return errors.Join(errs...)
}
