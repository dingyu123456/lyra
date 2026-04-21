package cache

import (
	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
)

type Cache interface {
	// --- 1. 调度主循环契约 ---

	// AssumePod 这里因为我们要预扣到具体的GPU，所以只穿pod可能不知道预扣哪个GPU
	// 这里有两种方案：1. 调用预扣之前，在pod注解中写入gpu的uuid
	// 2. 将调度结果传进来，但是这样会导致cache包依赖scheduler包形成循环依赖，可以
	// 通过把ScheduleResult放到公共包framework来解决
	// *******************************************************************
	//    framework就是调度器各个包的一个底层公共包，他不依赖cache，queue，scheduler等包，
	// 其他包都可以依赖他，这样都是单向依赖，不会循环依赖。
	AssumePod(logger *zap.Logger, pod *corev1.Pod) error

	// FinishBinding binding下发之后以当前时间点往后推ttl作为预扣pod的ddl
	FinishBinding(logger *zap.Logger, pod *corev1.Pod) error

	ForgetPod(logger *zap.Logger, pod *corev1.Pod) error
	IsAssumedPod(pod *corev1.Pod) (bool, error)

	UpdateSnapshot(logger *zap.Logger, snapshot *Snapshot) error
	FullUpdateSnapshot(logger *zap.Logger, snapshot *Snapshot) error

	// --- 2. Informer 事件刷新契约 ---
	AddPod(logger *zap.Logger, pod *corev1.Pod) error
	UpdatePod(logger *zap.Logger, oldPod, newPod *corev1.Pod) error
	RemovePod(logger *zap.Logger, pod *corev1.Pod) error
	GetPod(pod *corev1.Pod) (*corev1.Pod, error)

	AddNode(logger *zap.Logger, node *corev1.Node) *framework.NodeInfo
	UpdateNode(logger *zap.Logger, oldNode, newNode *corev1.Node) *framework.NodeInfo
	RemoveNode(logger *zap.Logger, node *corev1.Node) error

	AddCluster(logger *zap.Logger, cluster *clusterv1alpha1.Cluster)
	UpdateCluster(logger *zap.Logger, oldCluster, newCluster *clusterv1alpha1.Cluster)
	RemoveCluster(logger *zap.Logger, cluster *clusterv1alpha1.Cluster) error

	// --- 3. 测试与监控 ---
	ClusterCount() int
	NodeCount() int
	PodCount() (int, error)
	Dump() *Dump
}

// Dump 是 Lyra 调度器缓存的只读调试快照
// 仅用于 Debug 诊断，绝不要在核心调度热路径（Critical Path）上调用它
type Dump struct {
	AssumedPods sets.Set[string]                  // 正在被预扣资源、但还没真正绑定成功的 Pod 集合
	Clusters    map[string]*framework.ClusterInfo // 包含了集群宏观算力和底层所有 Node 快照的字典
}
