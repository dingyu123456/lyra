package app

import (
	"context"

	"github.com/dingyu123456/lyra/internal/config"
	internalScheduler "github.com/dingyu123456/lyra/internal/scheduler"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	karmadainformers "github.com/karmada-io/karmada/pkg/generated/informers/externalversions"
	"go.uber.org/zap"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// StartScheduler 初始化并启动调度器模块
func StartScheduler(ctx context.Context, logger *zap.Logger) {
	// 0. 加载配置文件
	if err := config.Load(); err != nil {
		logger.Fatal("Failed to load config", zap.Error(err))
	}
	logger = logger.With(zap.String("component", "lyra-scheduler"))

	// 1. 加载 Karmada kubeconfig
	kubeconfigPath := config.Cfg.KarmadaConfig.KubeConfig
	karmadaConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		logger.Fatal("Failed to load Karmada kubeconfig", zap.Error(err), zap.String("path", kubeconfigPath))
	}

	karmadaConfig.QPS = 100
	karmadaConfig.Burst = 200

	// 2. 创建控制面 Clients
	kClient, err := kubernetes.NewForConfig(karmadaConfig)
	if err != nil {
		logger.Fatal("Failed to create kube client", zap.Error(err))
	}

	kmClient, err := karmadaclientset.NewForConfig(karmadaConfig)
	if err != nil {
		logger.Fatal("Failed to create karmada client", zap.Error(err))
	}

	// 3. 创建控制面 Factories
	kubeFactory := informers.NewSharedInformerFactory(kClient, 0)
	karmadaFactory := karmadainformers.NewSharedInformerFactory(kmClient, 0)

	// 4. 构造调度器
	sched, err := internalScheduler.NewScheduler(ctx, kClient, kmClient, kubeFactory, karmadaFactory, logger)
	if err != nil {
		logger.Fatal("Failed to initialize scheduler", zap.Error(err))
	}

	// 5. 启动 Informer
	logger.Info("Starting informers...")
	kubeFactory.Start(ctx.Done())
	karmadaFactory.Start(ctx.Done())

	// 6. 启动主调度循环（阻塞）
	sched.Run(ctx)
}
