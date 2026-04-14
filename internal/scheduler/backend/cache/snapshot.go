package cache

import (
	"fmt"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
)

// Snapshot 是提供给算法插件打分的无锁全局快照
type Snapshot struct {
	// 用于 O(1) 的宏观集群属性查找 (PreFilter 阶段使用)
	clusterInfoMap map[string]*framework.ClusterInfo

	// 连续内存切片，用于高速遍历和 Round-Robin 轮询
	clusterInfoList []*framework.ClusterInfo

	// 全局世代计数器：用于判断本次克隆是否可以提前截断
	generation int64
}

// NewEmptySnapshot 预分配内存，供 Scheduler 结构体常驻复用
func NewEmptySnapshot() *Snapshot {
	return &Snapshot{
		clusterInfoMap:  make(map[string]*framework.ClusterInfo),
		clusterInfoList: make([]*framework.ClusterInfo, 0),
	}
}

// NumClusters returns the number of clusters in the snapshot.
func (s *Snapshot) NumClusters() int {
	return len(s.clusterInfoMap)
}

// NodeCount 动态计算当前快照中所有节点的总和
func (s *Snapshot) NodeCount() int {
	var totalNodes int

	// 遍历 O(C) 次，C 为集群总数，通常极小
	for _, clusterInfo := range s.clusterInfoList {
		if clusterInfo != nil {
			// len() 在 Go 中是 O(1) 操作
			totalNodes += len(clusterInfo.Nodes)
		}
	}

	return totalNodes
}

// GetNode O(1) 极速获取特定集群下的特定节点
func (s *Snapshot) GetNode(clusterName, nodeName string) (*framework.NodeInfo, error) {
	// 从外层 map 找集群
	cluster, ok := s.clusterInfoMap[clusterName]
	if !ok {
		return nil, fmt.Errorf("cluster %s not found in snapshot", clusterName)
	}

	// 从内层 map 找节点
	node, ok := cluster.Nodes[nodeName]
	if !ok {
		return nil, fmt.Errorf("node %s not found in cluster %s", nodeName, clusterName)
	}

	return node, nil
}

func (s *Snapshot) List() ([]*framework.ClusterInfo, error) {
	return s.clusterInfoList, nil
}

func (s *Snapshot) Get(clusterName string) (*framework.ClusterInfo, error) {
	if v, ok := s.clusterInfoMap[clusterName]; ok && v.Cluster() != nil {
		return v, nil
	}
	return nil, fmt.Errorf("clusterInfo not found for node name %q", clusterName)

}

// ClusterInfos 返回集群信息的 Lister，用于满足 framework.SharedLister 接口
func (s *Snapshot) ClusterInfos() framework.ClusterInfoLister {
	return s // 假设 Snapshot 已经实现了 ClusterInfoLister 接口的方法
}
