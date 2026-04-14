package runtime

import (
	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	"github.com/dingyu123456/lyra/internal/scheduler/framework/parallelize"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	"go.uber.org/zap"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
)

// 🌟 1. 简化的插件配置模型 (替代原生臃肿的 KubeSchedulerProfile)
type Plugin struct {
	Name   string
	Weight int32 // 仅对 Score 插件有效
}

type PluginSet struct {
	Enabled []Plugin
}

type Plugins struct {
	PreEnqueue PluginSet
	QueueSort  PluginSet
	PreFilter  PluginSet
	Filter     PluginSet
	PreScore   PluginSet
	Score      PluginSet
	Reserve    PluginSet
	Permit     PluginSet
	PreBind    PluginSet
	Bind       PluginSet
	PostBind   PluginSet
}

// 🌟 2. 框架配置项与 Options 模式
type frameworkOptions struct {
	clientSet            clientset.Interface
	karmadaClient        karmadaclientset.Interface // Lyra 特供：用于下发 PP/OP
	informerFactory      informers.SharedInformerFactory
	snapshotSharedLister framework.SharedLister
	logger               *zap.Logger
	parallelizer         parallelize.Parallelizer
}

type Option func(*frameworkOptions)

func WithClientSet(clientSet clientset.Interface) Option {
	return func(o *frameworkOptions) { o.clientSet = clientSet }
}

func WithKarmadaClient(karmadaClient karmadaclientset.Interface) Option {
	return func(o *frameworkOptions) { o.karmadaClient = karmadaClient }
}

func WithInformerFactory(factory informers.SharedInformerFactory) Option {
	return func(o *frameworkOptions) { o.informerFactory = factory }
}

func WithSnapshotSharedLister(lister framework.SharedLister) Option {
	return func(o *frameworkOptions) { o.snapshotSharedLister = lister }
}

func WithLogger(logger *zap.Logger) Option {
	return func(o *frameworkOptions) { o.logger = logger }
}
