package multicluster

import (
	"context"
	"fmt"
	"sync"

	karmadav1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	"go.uber.org/zap"
	"k8s.io/client-go/informers"

	"github.com/dingyu123456/lyra/internal/scheduler/multicluster/client"
	"github.com/dingyu123456/lyra/internal/scheduler/multicluster/kubeconfig"
)

// RegisterHandlerFunc 是一个回调函数，由 Scheduler 提供，用于给子集群 Informer 挂载业务逻辑
type RegisterHandlerFunc func(clusterName string, factory informers.SharedInformerFactory) error

// ClusterAccessState 记录单个子集群的接入详情
type ClusterAccessState struct {
	ClusterID      string
	ClusterName    string
	CancelInformer context.CancelFunc // 用于优雅停止该集群的所有 Informer
	IsActive       bool
}

type ClusterAccessManager interface {
	AccessCluster(cluster *karmadav1alpha1.Cluster) error
	TeardownCluster(cluster *karmadav1alpha1.Cluster) error
	IsClusterAccessible(clusterID string) bool
	GetUIDByName(name string) (string, bool)
}

type clusterAccessManager struct {
	ctx                context.Context
	kubeconfigProvider kubeconfig.KubeconfigProvider
	clientRegistry     client.ClusterClientRegistry
	registerHandler    RegisterHandlerFunc // 联动 eventhandlers.go，无需感知 Cache
	logger             *zap.Logger

	accessStates map[string]*ClusterAccessState
	statesMu     sync.RWMutex
}

// NewClusterAccessManager 构造函数 (彻底去除了 Cache 依赖)
func NewClusterAccessManager(
	ctx context.Context,
	kubeconfigProvider kubeconfig.KubeconfigProvider,
	clientRegistry client.ClusterClientRegistry,
	regFunc RegisterHandlerFunc,
	logger *zap.Logger,
) ClusterAccessManager {
	return &clusterAccessManager{
		ctx:                ctx,
		kubeconfigProvider: kubeconfigProvider,
		clientRegistry:     clientRegistry,
		registerHandler:    regFunc,
		logger:             logger.With(zap.String("component", "cluster_access_manager")),
		accessStates:       make(map[string]*ClusterAccessState),
	}
}

// AccessCluster 核心接入流程
func (m *clusterAccessManager) AccessCluster(cluster *karmadav1alpha1.Cluster) error {
	clusterID := string(cluster.UID)
	clusterName := cluster.Name

	m.statesMu.Lock()
	defer m.statesMu.Unlock()

	// 1. 幂等检查：如果已经接入且状态正常，直接返回
	if state, exists := m.accessStates[clusterID]; exists && state.IsActive {
		return nil
	}

	m.logger.Info("Attempting to access member cluster", zap.String("name", clusterName), zap.String("uid", clusterID))

	// 2. 凭证准备：调用 Provider 获取 Kubeconfig
	kubeConfigBytes, err := m.kubeconfigProvider.GetKubeconfig(m.ctx, cluster)
	if err != nil {
		return fmt.Errorf("get kubeconfig failed: %w", err)
	}

	// 3. 注册客户端：创建 Clientset 并存入 Registry
	if err := m.clientRegistry.RegisterClient(clusterID, kubeConfigBytes); err != nil {
		return fmt.Errorf("register client failed: %w", err)
	}

	clusterClient, exists := m.clientRegistry.GetClient(clusterID)
	if !exists {
		return fmt.Errorf("client registered but not found for cluster %s", clusterName)
	}

	// 4. 动态启动监控管道 (Informers)
	// 为该集群创建一个独立的上下文，方便后续物理断网
	subCtx, cancel := context.WithCancel(m.ctx)

	// 创建针对该子集群的 Informer 工厂（无需全量同步，设为 0）
	factory := informers.NewSharedInformerFactory(clusterClient.ClientSet, 0)

	// 调用 Scheduler 传入的 addAllEventHandlers 挂载监听逻辑
	if err := m.registerHandler(clusterName, factory); err != nil {
		cancel() // 挂载失败，立刻取消上下文防泄漏
		return fmt.Errorf("register event handlers failed: %w", err)
	}

	// 异步启动该集群的 Informer 协程
	go factory.Start(subCtx.Done())

	// 5. 状态持久化
	m.accessStates[clusterID] = &ClusterAccessState{
		ClusterID:      clusterID,
		ClusterName:    clusterName,
		CancelInformer: cancel,
		IsActive:       true,
	}

	m.logger.Info("Member cluster monitor started", zap.String("name", clusterName))
	return nil
}

// TeardownCluster 核心销毁流程
func (m *clusterAccessManager) TeardownCluster(cluster *karmadav1alpha1.Cluster) error {
	clusterID := string(cluster.UID)

	m.statesMu.Lock()
	defer m.statesMu.Unlock()

	state, exists := m.accessStates[clusterID]
	if !exists {
		return nil
	}

	m.logger.Warn("Tearing down member cluster access", zap.String("name", state.ClusterName))

	// 1. 物理断网：通过 Cancel 信号瞬间杀掉该集群所有的 Informer Watch 协程
	if state.CancelInformer != nil {
		state.CancelInformer()
	}

	// 2. 移除客户端连接，释放底层 TCP 连接池
	m.clientRegistry.UnregisterClient(clusterID)

	// 3. 清理 Manager 内部状态
	delete(m.accessStates, clusterID)

	m.logger.Info("Successfully tore down cluster", zap.String("name", cluster.Name))
	return nil
}

// IsClusterAccessible 检查集群是否已接入
func (m *clusterAccessManager) IsClusterAccessible(clusterID string) bool {
	m.statesMu.RLock()
	defer m.statesMu.RUnlock()
	state, exists := m.accessStates[clusterID]
	return exists && state.IsActive
}

// GetUIDByName 通过集群名称查找 UID
func (m *clusterAccessManager) GetUIDByName(name string) (string, bool) {
	m.statesMu.RLock()
	defer m.statesMu.RUnlock()

	for uid, state := range m.accessStates {
		if state.ClusterName == name {
			return uid, true
		}
	}
	return "", false
}
