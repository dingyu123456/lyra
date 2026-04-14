package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dingyu123456/lyra/internal/config"
	"github.com/dingyu123456/lyra/internal/scheduler"
	pkglogger "github.com/dingyu123456/lyra/pkg/logger"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	karmadainformers "github.com/karmada-io/karmada/pkg/generated/informers/externalversions"
	"go.uber.org/zap"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	// 1. ## 加载配置 ##
	if err := config.Load(); err != nil {
		panic(fmt.Sprintf("Load config failed: %v", err))
	}

	// 2. ## 初始化日志 ##
	if err := pkglogger.Init(config.Cfg.LogConfig); err != nil {
		panic(fmt.Sprintf("Init log failed: %v", err))
	}
	defer zap.L().Sync()
	logger := zap.L()
	logger.Info("Lyra initializing...", zap.String("version", "v0.1.0"))

	// 3. ## 构建 KubeConfig (控制面) ##
	// 假设你 config.yaml 里配置了指向 Karmada/控制面 K8s 的路径
	restConfig, err := clientcmd.BuildConfigFromFlags("", config.Cfg.KubeConfig)
	if err != nil {
		logger.Fatal("Failed to build kubeconfig", zap.Error(err))
	}

	// 4. ## 创建 Clients ##
	// K8s 原生 Client (用于 Pod, Node 事件)
	kubeClient := kubernetes.NewForConfigOrDie(restConfig)
	// Karmada Client (用于 Cluster, PP, OP 资源)
	karmadaClient := karmadaclientset.NewForConfigOrDie(restConfig)

	// 5. ## 创建 Factory (共享 Informer) ##
	kubeFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	karmadaFactory := karmadainformers.NewSharedInformerFactory(karmadaClient, 0)

	// 6. ## 优雅退出与上下文管理 ##
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 监听系统退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
		cancel() // 触发 Context 取消，通知下游组件停止
	}()

	// 7. ## 装配调度器 ##
	// 传入所有必要的依赖：两个 Client，两个 Factory，以及全局 Logger
	sched, err := scheduler.NewScheduler(
		ctx,
		kubeClient,
		karmadaClient,
		kubeFactory,
		karmadaFactory,
		logger,
	)
	if err != nil {
		logger.Fatal("Failed to create scheduler", zap.Error(err))
	}

	// 8. ## 启动所有 Informer ##
	kubeFactory.Start(ctx.Done())
	karmadaFactory.Start(ctx.Done())
	logger.Info("Shared Informer Factories started, waiting for cache sync...")

	// 🌟 核心改动：使用 K8s 原生方式，通过 Factory 等待所有缓存全量同步完成
	kubeSynced := kubeFactory.WaitForCacheSync(ctx.Done())
	karmadaSynced := karmadaFactory.WaitForCacheSync(ctx.Done())

	// 校验各个工厂的资源是否都同步成功
	for v, ok := range kubeSynced {
		if !ok {
			logger.Fatal("Failed to sync kube cache", zap.Any("type", v))
		}
	}
	for v, ok := range karmadaSynced {
		if !ok {
			logger.Fatal("Failed to sync karmada cache", zap.Any("type", v))
		}
	}
	logger.Info("All caches synced successfully!")

	// 9. ## 调度器点火 ##
	logger.Info("Lyra Scheduler is running")
	sched.Run(ctx)
}
