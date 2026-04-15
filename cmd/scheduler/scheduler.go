package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/dingyu123456/lyra/internal/config"
	"github.com/dingyu123456/lyra/internal/scheduler"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	karmadainformers "github.com/karmada-io/karmada/pkg/generated/informers/externalversions"
	"go.uber.org/zap"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	// =========================================================================
	// 1. 初始化 Logger (这里使用生产模式，也可以用 zap.NewDevelopment())
	// =========================================================================
	logger, err := zap.NewProduction()
	if err != nil {
		panic("Failed to initialize logger: " + err.Error())
	}
	defer logger.Sync() // 确保程序退出前刷新缓冲区日志

	// =========================================================================
	// 2. 初始化全局 Context (监听操作系统的退出信号，实现优雅停机)
	// =========================================================================
	// signal.NotifyContext 是 Go 1.16+ 的原生方法，完美替代了老版本复杂的 channel 监听
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger.Info("Starting Lyra Scheduler process...")

	// 3. 调用你的启动逻辑，把做好的 ctx 和 logger 传进去
	startScheduler(ctx, logger)

	logger.Info("Lyra Scheduler process exited cleanly.")
}

// startScheduler 接收外部传来的 ctx 和 logger
func startScheduler(ctx context.Context, logger *zap.Logger) {
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

	// 调优：调度器作为核心组件，需要极高的与 APIServer 通信频率
	karmadaConfig.QPS = 100
	karmadaConfig.Burst = 200

	// 2. 创建控制面 Clients
	kClient, err := kubernetes.NewForConfig(karmadaConfig) // 用于获取 Secret、监听 Pod
	if err != nil {
		logger.Fatal("Failed to create kube client", zap.Error(err))
	}

	kmClient, err := karmadaclientset.NewForConfig(karmadaConfig) // 用于监听 Cluster、写 PP/OP
	if err != nil {
		logger.Fatal("Failed to create karmada client", zap.Error(err))
	}

	// 3. 创建控制面 Factories
	kubeFactory := informers.NewSharedInformerFactory(kClient, 0)
	karmadaFactory := karmadainformers.NewSharedInformerFactory(kmClient, 0)

	// 4. 构造调度器
	sched, err := scheduler.NewScheduler(ctx, kClient, kmClient, kubeFactory, karmadaFactory, logger)
	if err != nil {
		logger.Fatal("Failed to initialize scheduler", zap.Error(err))
	}

	// 5. 在所有组件准备好后，统一开启数据流
	logger.Info("Starting informers...")
	kubeFactory.Start(ctx.Done())
	karmadaFactory.Start(ctx.Done())

	// 6. 点火启动主调度循环
	// Run() 内部会阻塞运行（wait.UntilWithContext），直到 ctx 被 cancel
	sched.Run(ctx)
}
