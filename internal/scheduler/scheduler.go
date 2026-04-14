package scheduler

import (
	"context"
	"fmt"
	"time"

	internalcache "github.com/dingyu123456/lyra/internal/scheduler/backend/cache"
	internalqueue "github.com/dingyu123456/lyra/internal/scheduler/backend/queue"
	"github.com/dingyu123456/lyra/internal/scheduler/dispatcher"
	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	"github.com/dingyu123456/lyra/internal/scheduler/framework/runtime"
	"github.com/dingyu123456/lyra/internal/scheduler/multicluster"
	"github.com/dingyu123456/lyra/internal/scheduler/multicluster/client"
	"github.com/dingyu123456/lyra/internal/scheduler/multicluster/kubeconfig"
	"github.com/dingyu123456/lyra/internal/scheduler/plugins/gpurender"
	"github.com/dingyu123456/lyra/internal/scheduler/plugins/karmadabind"
	"github.com/dingyu123456/lyra/internal/scheduler/plugins/noderesources"
	"github.com/dingyu123456/lyra/internal/scheduler/plugins/prioritysort"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	karmadainformers "github.com/karmada-io/karmada/pkg/generated/informers/externalversions"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

var ErrNoClustersAvailable = fmt.Errorf("no clusters available to schedule pods")
var ErrNoNodesAvailable = fmt.Errorf("no nodes available to schedule pods")

// Scheduler watches for new unscheduled pods. It attempts to find
// nodes that they fit on and writes bindings back to the api server.
type Scheduler struct {
	// pod.Spec.SchedulerName == sched.Name
	// pod必须配置pod.Spec.SchedulerName为我们的调度器名称，我们的调度器才会识别调度
	// 默认为 "lyra-scheduler"
	Name string
	// It is expected that changes made via Cache will be observed
	// by NodeLister and Algorithm.
	Cache internalcache.Cache

	ClusterManager multicluster.ClusterAccessManager

	// NextPod should be a function that blocks until the next pod
	// is available. We don't use a channel for this, because scheduling
	// a pod may take some amount of time and we don't want pods to get
	// stale while they sit in a channel.
	NextPod func(logger *zap.Logger) (*framework.QueuedPodInfo, error)

	// FailureHandler is called upon a scheduling failure.
	FailureHandler FailureHandlerFn

	// SchedulePod tries to schedule the given pod to one of the nodes in the node list.
	// Return a struct of ScheduleResult with the name of suggested host on success,
	// otherwise will return a FitError with reasons.
	SchedulePod func(ctx context.Context, fwk framework.Framework, state *framework.CycleState, pod *v1.Pod) (framework.ScheduleResult, error)

	// Close this to shut down the scheduler.
	StopEverything <-chan struct{}

	// SchedulingQueue holds pods to be scheduled
	SchedulingQueue internalqueue.SchedulingQueue

	Framework framework.Framework

	kubeClient    clientset.Interface
	karmadaClient karmadaclientset.Interface

	ClusterInfoSnapshot *internalcache.Snapshot

	Dispatcher dispatcher.Dispatcher

	// TODO 这不是核心亮点，为了避免混乱，我们直接去掉这三个字段的相关逻辑
	//// TODO 这个字段到时候和多集群调度可能有点冲突，到时候注意下
	//percentageOfNodesToScore int32
	//
	//// TODO 下面这两个字段后面得改，因为在多集群下只用一个int不知道能不能直接对应到nodeIndex
	//nextStartClusterIndex int
	//nextStartNodeIndex    int
	//// TODO 上面两字段后面注意，之后看看这两字段具体是怎么用的，根据具体情况修改

	// logger *must* be initialized when creating a Scheduler,
	// otherwise logging functions will access a nil sink and
	// panic.
	logger *zap.Logger

	// registeredHandlers contains the registrations of all handlers. It's used to check if all handlers have finished syncing before the scheduling cycles start.
	// TODO 这里我们好像只能放karmada相关的handler，因为子集群的handler是动态接入的，放进去会影响调度循环
	registeredHandlers []cache.ResourceEventHandlerRegistration
}

type FailureHandlerFn func(ctx context.Context, fwk framework.Framework, podInfo *framework.QueuedPodInfo, status *framework.Status, start time.Time)

// TODO 这两个方法是ScheduleOne()函数内部调用的方法，那时候实现
func (sched *Scheduler) applyDefaultHandlers() {
	sched.SchedulePod = sched.schedulePod
	sched.FailureHandler = sched.handleSchedulingFailure
}

// getPodPriority 安全地获取 Pod 的优先级。
// 如果 Pod 没有显式设置 PriorityClass，它的 Spec.Priority 会是 nil，此时默认视为 0。
func getPodPriority(pod *v1.Pod) int32 {
	if pod != nil && pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	return 0
}

// podLessFunc 决定了活跃队列 (ActiveQueue) 中 Pod 的出队顺序。
// 返回 true 表示 pInfo1 应该排在 pInfo2 的前面（优先被调度）。
func podLessFunc(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
	prio1 := getPodPriority(pInfo1.Pod)
	prio2 := getPodPriority(pInfo2.Pod)

	if prio1 != prio2 {
		// 1. 绝对优先级比较：数值越大，优先级越高，越早出队
		return prio1 > prio2
	}

	// 2. 相同优先级下的公平性保证 (FIFO)
	// Timestamp 记录了 Pod 被加入调度队列的时间。加入越早，越优先。
	return pInfo1.Timestamp.Before(pInfo2.Timestamp)
}

func NewScheduler(
	ctx context.Context,
	kubeClient clientset.Interface,
	karmadaClient karmadaclientset.Interface,
	kubeFactory informers.SharedInformerFactory,
	karmadaFactory karmadainformers.SharedInformerFactory,
	logger *zap.Logger,
) (*Scheduler, error) {

	stopEverything := ctx.Done()

	// 1. 初始化 Cache 和 Snapshot
	schedulerCache := internalcache.New(ctx, 30*time.Second)
	snapshot := internalcache.NewEmptySnapshot()

	// 2. 初始化 Registry 并注册插件工厂方法
	registry := make(runtime.Registry)
	_ = registry.Register(prioritysort.Name, prioritysort.New)
	_ = registry.Register(gpurender.Name, gpurender.New)
	_ = registry.Register(noderesources.Name, noderesources.New)
	_ = registry.Register(karmadabind.Name, karmadabind.New)

	// 3. 初始化 Framework
	fwk, err := runtime.NewFramework(ctx, registry, getDefaultPlugins(),
		runtime.WithClientSet(kubeClient),
		runtime.WithKarmadaClient(karmadaClient),
		runtime.WithInformerFactory(kubeFactory),
		runtime.WithSnapshotSharedLister(snapshot), // 此时 Snapshot 必须已实现 ClusterInfos()
		runtime.WithLogger(logger),
	)
	if err != nil {
		return nil, fmt.Errorf("initializing framework: %w", err)
	}

	// 4. 初始化队列
	podQueue := internalqueue.NewSchedulingQueue(
		fwk.QueueSortFunc(),
		kubeFactory,
		internalqueue.WithLogger(logger),
	)
	fwk.SetPodActivator(podQueue)

	sched := &Scheduler{
		Name:                "lyra-scheduler",
		Cache:               schedulerCache,
		SchedulingQueue:     podQueue,
		Framework:           fwk,
		StopEverything:      stopEverything,
		kubeClient:          kubeClient,
		karmadaClient:       karmadaClient,
		ClusterInfoSnapshot: snapshot,
		Dispatcher:          dispatcher.NewDispatcher(kubeClient, karmadaClient, logger),
		logger:              logger,
	}

	sched.NextPod = podQueue.Pop
	sched.SchedulePod = sched.schedulePod
	sched.FailureHandler = sched.handleSchedulingFailure

	// 5. 初始化多集群管理器
	sched.ClusterManager = multicluster.NewClusterAccessManager(
		ctx,
		kubeconfig.NewSecretKubeconfigProvider(kubeClient, logger),
		client.NewDefaultClusterClientRegistry(logger),
		sched.addAllEventHandlers,
		logger,
	)

	// 6. 挂载事件处理器 (通过新定义的封装方法)
	sched.addControlPlaneEventHandlers(kubeFactory, karmadaFactory)

	return sched, nil
}

// 封装方法：统一处理控制面 Informer 注册
func (sched *Scheduler) addControlPlaneEventHandlers(kubeFactory informers.SharedInformerFactory, karmadaFactory karmadainformers.SharedInformerFactory) {
	// A. 监听 Pod 事件
	sched.addControlPlanePodEventHandlers(kubeFactory.Core().V1().Pods().Informer())

	// B. 监听集群状态变化 (从 karmadaFactory 获取)
	clusterInformer := karmadaFactory.Cluster().V1alpha1().Clusters().Informer()
	sched.addClusterEventHandlers(clusterInformer)
}

// getDefaultPlugins 定义 Lyra 调度器默认开启的插件集
func getDefaultPlugins() *runtime.Plugins {
	return &runtime.Plugins{
		QueueSort: runtime.PluginSet{
			Enabled: []runtime.Plugin{{Name: prioritysort.Name}},
		},
		Filter: runtime.PluginSet{
			Enabled: []runtime.Plugin{
				{Name: gpurender.Name},
				{Name: noderesources.Name},
			},
		},
		Score: runtime.PluginSet{
			Enabled: []runtime.Plugin{
				{Name: gpurender.Name, Weight: 1},
				{Name: noderesources.Name, Weight: 1},
			},
		},
		Bind: runtime.PluginSet{
			Enabled: []runtime.Plugin{{Name: karmadabind.Name}},
		},
	}
}

func (sched *Scheduler) Run(ctx context.Context) {
	sched.logger.Info("Starting Lyra scheduler...")

	// 1. 启动调度队列 (此时外部 main.go 已经保证缓存同步完毕)
	sched.SchedulingQueue.Run(sched.logger)

	// 2. 启动主调度循环
	// 使用 wait.UntilWithContext 确保 scheduleOne 在异常退出后能自动重启
	// 这里的 0 表示两次执行之间不额外等待
	go wait.UntilWithContext(ctx, sched.ScheduleOne, 0)

	// 阻塞直到 context 取消
	<-ctx.Done()
	sched.SchedulingQueue.Close()
	sched.logger.Info("Lyra scheduler stopping...")
}
