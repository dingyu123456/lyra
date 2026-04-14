package client

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// ClusterClient 封装了子集群的连接凭证与状态
type ClusterClient struct {
	ClusterID  string
	ClientSet  kubernetes.Interface
	RestConfig *rest.Config
	CreatedAt  time.Time

	mu         sync.RWMutex
	lastUsedAt time.Time
}

// UpdateLastUsed 线程安全地更新最后使用时间
func (c *ClusterClient) UpdateLastUsed() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastUsedAt = time.Now()
}

// GetLastUsedAt 线程安全地获取最后使用时间
func (c *ClusterClient) GetLastUsedAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastUsedAt
}

// ClusterClientRegistry 接口定义
type ClusterClientRegistry interface {
	RegisterClient(clusterID string, kubeconfig []byte) error
	UnregisterClient(clusterID string)
	GetClient(clusterID string) (*ClusterClient, bool)
	GetAllClients() map[string]*ClusterClient
}

type DefaultClusterClientRegistry struct {
	clients sync.Map // 直接使用值类型，Go 官方推荐的做法
	logger  *zap.Logger
}

func NewDefaultClusterClientRegistry(logger *zap.Logger) ClusterClientRegistry {
	return &DefaultClusterClientRegistry{
		logger: logger.With(zap.String("component", "client_registry")),
	}
}

func (r *DefaultClusterClientRegistry) RegisterClient(clusterID string, kubeconfig []byte) error {
	// 1. 利用 kubeconfig 字节流解析出 restConfig
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to get rest config from kubeconfig for cluster %s: %w", clusterID, err)
	}

	// 🌟 2. 核心调优：破解 client-go 默认的限流瓶颈
	// 默认 QPS 为 5，Burst 为 10，对于调度器的高频 Watch 绝对不够
	restConfig.QPS = 100
	restConfig.Burst = 200

	// 3. 构造 ClientSet
	clientSet, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client for cluster %s: %w", clusterID, err)
	}

	// 4. 构造并存储自定义的 client 包装器
	now := time.Now()
	clusterClient := &ClusterClient{
		ClusterID:  clusterID,
		ClientSet:  clientSet,
		RestConfig: restConfig,
		CreatedAt:  now,
		lastUsedAt: now,
	}

	r.clients.Store(clusterID, clusterClient)
	r.logger.Info("Registered cluster client successfully", zap.String("clusterID", clusterID))
	return nil
}

func (r *DefaultClusterClientRegistry) UnregisterClient(clusterID string) {
	r.clients.Delete(clusterID)
	r.logger.Info("Unregistered cluster client successfully", zap.String("clusterID", clusterID))
}

func (r *DefaultClusterClientRegistry) GetClient(clusterID string) (*ClusterClient, bool) {
	if val, exists := r.clients.Load(clusterID); exists {
		clusterClient := val.(*ClusterClient)
		// 每次获取时刷新活跃时间
		clusterClient.UpdateLastUsed()
		return clusterClient, true
	}
	return nil, false
}

func (r *DefaultClusterClientRegistry) GetAllClients() map[string]*ClusterClient {
	// 🌟 核心修复：必须初始化 map，否则写操作会直接 Panic
	clusterClientMap := make(map[string]*ClusterClient)

	r.clients.Range(func(key, value interface{}) bool {
		clusterClientMap[key.(string)] = value.(*ClusterClient)
		return true // 返回 true 继续遍历
	})
	return clusterClientMap
}
