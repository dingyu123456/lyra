package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	mylogger "github.com/dingyu123456/lyra/pkg/logger"
	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
)

var (
	cleanAssumedPeriod = 1 * time.Second
)

// cacheImpl 是 Cache 接口的真实实现
type cacheImpl struct {
	stop   <-chan struct{}
	ttl    time.Duration
	period time.Duration

	// 全局大锁，保护所有状态机的读写
	mu sync.RWMutex

	assumedPods sets.Set[string]
	podStates   map[string]*podState

	// ==========================================
	// 🌟 Lyra 核心：二维增量状态机 (Twin Cache)
	// ==========================================

	// clusters 提供 O(1) 的宏观集群精确查找
	clusters map[string]*clusterInfoListItem

	// headCluster 是顶层双向链表的头指针。
	// 任何子集群发生哪怕一丁点资源变化，它都会被立刻移动到这个链表的头部！
	headCluster *clusterInfoListItem

	// imageStates 等不需要的 K8s 历史包袱已经被我们砍掉
}

// newCache 内部构造函数
func newCache(ctx context.Context, ttl, period time.Duration) *cacheImpl {
	return &cacheImpl{
		ttl:    ttl,
		period: period,
		stop:   ctx.Done(),

		// 初始化基础状态跟踪
		assumedPods: sets.New[string](),
		podStates:   make(map[string]*podState),

		// 🌟 初始化 Lyra 核心：二维增量状态机
		clusters:    make(map[string]*clusterInfoListItem),
		headCluster: nil, // 初始时没有任何集群接入
	}
}

// New 是暴露给 NewScheduler 使用的构造方法
// ttl: 预扣除(Assumed) Pod 的过期时间，通常设为 30s
func New(ctx context.Context, ttl time.Duration) Cache {
	logger := mylogger.FromContext(ctx)
	// 1. 创建缓存实例
	c := newCache(ctx, ttl, cleanAssumedPeriod)

	// 2. 启动后台过期清理协程
	// 该协程会定期检查那些 Assumed 但迟迟没有在子集群观测到 Running 的 Pod
	// 在你的 zap 日志体系下，这里建议传入 logger
	c.run(logger)

	return c
}

// run 启动后台协程定期清理过期的 Assumed Pod
func (cache *cacheImpl) run(logger *zap.Logger) {
	// wait.Until 会每隔 cache.period 时间执行一次闭包函数，
	// 直到 cache.stop 收到关闭信号（即 ctx.Done()）为止。
	go wait.Until(func() {
		cache.cleanupAssumedPods(logger, time.Now())
	}, cache.period, cache.stop)
}

// cleanupAssumedPods 遍历并清理过期的 Assumed Pod
func (cache *cacheImpl) cleanupAssumedPods(logger *zap.Logger, now time.Time) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	// 遍历所有处于 Assumed 状态的 Pod
	for key := range cache.assumedPods {
		ps, ok := cache.podStates[key]
		if !ok {
			// 🌟 容错优化：K8s 原生在这里用的是 klog.FlushAndExit 直接把调度器干挂。
			// 在生产级多集群架构中，直接干挂代价太大。我们选择记录 Error 并主动修剪脏数据。
			logger.Error("Key found in assumed set but not in podStates, cleaning up dirty data", zap.String("podKey", key))
			cache.assumedPods.Delete(key)
			continue
		}

		// 如果 Binding 过程（Karmada PP/OP 创建）还没有结束，则不能判定为过期。
		// 这防止了调度器自身 API 调用慢导致 Pod 算力被错误释放的问题。
		if !ps.bindingFinished {
			logger.Debug("Could not expire cache for pod as binding is still in progress", zap.String("podKey", key))
			continue
		}

		// 检查是否超过了设定的 TTL (Time To Live)
		if cache.ttl != 0 && now.After(*ps.deadline) {
			logger.Warn("Assumed Pod expired, removing from cache", zap.String("podKey", key))

			// 🌟 核心清理逻辑：调用内部的 removePod 进行销账
			// 注意⚠️：由于当前已经持有了 cache.mu.Lock()，
			// 这里必须调用内部的、不带锁的 removePod 方法，而不能调用对外的 RemovePod 接口，否则会死锁！
			if err := cache.removePod(logger, ps.pod); err != nil {
				logger.Error("ExpirePod failed during cleanup", zap.String("podKey", key), zap.Error(err))
			}
		}
	}
}

// ---------------------------------------------------------
// 第一维：宏观集群链表节点 (Cluster Level)
// ---------------------------------------------------------
type clusterInfoListItem struct {
	// 暴露给 Framework 算法层使用的纯净无锁数据
	info *framework.ClusterInfo

	// 双向链表指针
	next *clusterInfoListItem
	prev *clusterInfoListItem

	// 该集群内部的节点字典，O(1) 查找 Node
	nodes map[string]*nodeInfoListItem

	// 🌟 第二维入口：该集群内部的 Node MRU 链表头！
	// 集群内任何节点资源变化，该节点会被移到 headNode 头部。
	headNode *nodeInfoListItem
}

func newClusterInfoListItem(info *framework.ClusterInfo) *clusterInfoListItem {
	return &clusterInfoListItem{
		info:  info,
		nodes: make(map[string]*nodeInfoListItem),
	}
}

// ---------------------------------------------------------
// 第二维：微观节点链表节点 (Node Level)
// ---------------------------------------------------------
type nodeInfoListItem struct {
	// 暴露给 Framework 算法层使用的纯净无锁数据 (包含 GPU 矢量信息)
	info *framework.NodeInfo

	// 双向链表指针
	next *nodeInfoListItem
	prev *nodeInfoListItem
}

// newNodeInfoListItem initializes a new nodeInfoListItem.
func newNodeInfoListItem(ni *framework.NodeInfo) *nodeInfoListItem {
	return &nodeInfoListItem{
		info: ni,
	}
}

// podState 记录任务预扣生命周期
type podState struct {
	pod *corev1.Pod
	// Used by assumedPod to determinate expiration.
	// If deadline is nil, assumedPod will never expire.
	deadline *time.Time
	// Used to block cache from expiring assumedPod if binding still runs
	bindingFinished bool
}

// UpdateSnapshot 将 cacheImpl 中的最新物理状态，极速增量同步到只读的 Snapshot 中。
func (cache *cacheImpl) UpdateSnapshot(logger *zap.Logger, snapshot *Snapshot) error {
	// 全局读写锁：在克隆期间锁住整个 Cache，防止 Informer 并发写入
	cache.mu.Lock()
	defer cache.mu.Unlock()

	// 拿到上一次快照的世代戳
	snapshotGeneration := snapshot.generation
	updateClusterList := false // 标记是否需要重建连续的 slice 列表

	// 诊断日志：记录进入 UpdateSnapshot 时的 Generation 状态
	if cache.headCluster != nil {
		logger.Debug("[SNAPSHOT-DIAG] UpdateSnapshot entry",
			zap.Int64("snapshot_gen", snapshotGeneration),
			zap.Int64("head_cluster_gen", cache.headCluster.info.Generation),
			zap.String("head_cluster", cache.headCluster.info.ClusterName),
		)
	}

	clonedClusters := 0
	clonedNodes := 0

	// ==========================================
	// 🌟 核心一：第一维宏观集群遍历 (Cluster MRU)
	// ==========================================
	for clusterNode := cache.headCluster; clusterNode != nil; clusterNode = clusterNode.next {
		// 【降维打击 1】如果当前集群的 Generation <= 快照 Generation
		// 说明这个集群以及它后面的所有集群，自上次调度以来都没有发生任何变化！直接 Break！
		if clusterNode.info.Generation <= snapshotGeneration {
			logger.Debug("[SNAPSHOT-DIAG] Cluster generation <= snapshot, breaking",
				zap.String("cluster", clusterNode.info.ClusterName),
				zap.Int64("cluster_gen", clusterNode.info.Generation),
				zap.Int64("snapshot_gen", snapshotGeneration))
			break
		}

		clonedClusters++
		// 在快照中寻找这个集群，如果没有则初始化
		existingCluster, ok := snapshot.clusterInfoMap[clusterNode.info.ClusterName]
		if !ok {
			updateClusterList = true
			existingCluster = &framework.ClusterInfo{
				ClusterName: clusterNode.info.ClusterName,
				Nodes:       make(map[string]*framework.NodeInfo),
			}
			snapshot.clusterInfoMap[clusterNode.info.ClusterName] = existingCluster
		}

		// 增量同步宏观属性 (安全拷贝)
		if c := clusterNode.info.Cluster(); c != nil {
			existingCluster.SetCluster(c.DeepCopy())
		}
		if clusterNode.info.Allocatable != nil {
			existingCluster.Allocatable = clusterNode.info.Allocatable.Clone()
		}
		if clusterNode.info.Requested != nil {
			existingCluster.Requested = clusterNode.info.Requested.Clone()
		}
		existingCluster.Generation = clusterNode.info.Generation

		// ==========================================
		// 🌟 核心二：第二维微观节点遍历 (Node MRU)
		// ==========================================
		for n := clusterNode.headNode; n != nil; n = n.next {
			// 【降维打击 2】在这个发生变化的集群内部，如果某个节点的 Generation <= 快照 Generation
			// 说明这个节点以及后面的节点都没变！直接 Break，跳出节点循环！
			if n.info.Generation <= snapshotGeneration {
				break
			}

			// 深拷贝发生变化的节点，并更新到快照中
			existingCluster.Nodes[n.info.NodeName] = n.info.DeepCopy()
			clonedNodes++
		}

		// 诊断日志：记录本次快照克隆了多少集群和节点
		logger.Debug("[SNAPSHOT-DIAG] UpdateSnapshot summary",
			zap.Int("cloned_clusters", clonedClusters),
			zap.Int("cloned_nodes", clonedNodes),
			zap.Int64("new_snapshot_gen", snapshot.generation),
		)

		// 记录到快照，供性能分析使用
		snapshot.lastClonedClusters = clonedClusters
		snapshot.lastClonedNodes = clonedNodes
	}

	// 更新全局快照的最新世代戳
	if cache.headCluster != nil {
		snapshot.generation = cache.headCluster.info.Generation
	}

	// ==========================================
	// 🌟 核心三：垃圾回收 (处理物理删除事件)
	// ==========================================
	// 1. 处理被彻底删除的集群
	if len(snapshot.clusterInfoMap) > len(cache.clusters) {
		for clusterName := range snapshot.clusterInfoMap {
			if _, exists := cache.clusters[clusterName]; !exists {
				delete(snapshot.clusterInfoMap, clusterName)
			}
		}
		updateClusterList = true
	}

	// 2. 处理被删除的节点 (遍历有交集的集群)
	for clusterName, snapshotCluster := range snapshot.clusterInfoMap {
		if cacheCluster, exists := cache.clusters[clusterName]; exists {
			// 如果快照里的节点数 > Cache 里的节点数，说明有节点宕机被移除了
			if len(snapshotCluster.Nodes) > len(cacheCluster.nodes) {
				for nodeName := range snapshotCluster.Nodes {
					if _, nodeExists := cacheCluster.nodes[nodeName]; !nodeExists {
						delete(snapshotCluster.Nodes, nodeName)
					}
				}
			}
		}
	}

	// ==========================================
	// 🌟 核心四：重建连续切片 (供 Round-Robin 极速轮询)
	// ==========================================
	// Map 适合 O(1) 查找，Slice 适合高速遍历。只有拓扑结构发生增删时才重建 Slice。
	if updateClusterList {
		snapshot.clusterInfoList = make([]*framework.ClusterInfo, 0, len(snapshot.clusterInfoMap))
		for _, c := range snapshot.clusterInfoMap {
			snapshot.clusterInfoList = append(snapshot.clusterInfoList, c)
		}
	}

	// 兜底一致性校验
	if len(snapshot.clusterInfoList) != len(cache.clusters) {
		errMsg := fmt.Sprintf("snapshot state is not consistent, list len=%d, map len=%d, cache len=%d",
			len(snapshot.clusterInfoList), len(snapshot.clusterInfoMap), len(cache.clusters))
		logger.Error(errMsg)
		return fmt.Errorf("%s", errMsg)
	}

	return nil
}

// FullUpdateSnapshot 全量克隆快照，用于性能对比实验
// 不使用 Generation 号和 MRU 链表优化，每次调度都完整克隆所有集群和节点数据
func (cache *cacheImpl) FullUpdateSnapshot(logger *zap.Logger, snapshot *Snapshot) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	// 清空旧快照，全量重建
	snapshot.clusterInfoMap = make(map[string]*framework.ClusterInfo, len(cache.clusters))
	snapshot.clusterInfoList = make([]*framework.ClusterInfo, 0, len(cache.clusters))

	for _, ci := range cache.clusters {
		clone := &framework.ClusterInfo{
			ClusterName: ci.info.ClusterName,
			Nodes:       make(map[string]*framework.NodeInfo, len(ci.nodes)),
		}
		if ci.info.Allocatable != nil {
			clone.Allocatable = ci.info.Allocatable.Clone()
		}
		if ci.info.Requested != nil {
			clone.Requested = ci.info.Requested.Clone()
		}
		if ci.info.Cluster() != nil {
			clone.SetCluster(ci.info.Cluster().DeepCopy())
		}

		for _, ni := range ci.nodes {
			clone.Nodes[ni.info.NodeName] = ni.info.DeepCopy()
		}

		snapshot.clusterInfoMap[clone.ClusterName] = clone
		snapshot.clusterInfoList = append(snapshot.clusterInfoList, clone)
	}

	// 更新 generation 为最新的
	if cache.headCluster != nil {
		snapshot.generation = cache.headCluster.info.Generation
	}

	// 全量快照统计：复制的集群数和节点数
	snapshot.lastClonedClusters = len(cache.clusters)
	totalNodes := 0
	for _, ci := range cache.clusters {
		totalNodes += len(ci.nodes)
	}
	snapshot.lastClonedNodes = totalNodes

	return nil
}

// AssumePod 在内存中乐观地预扣 Pod 所需的资源。
func (cache *cacheImpl) AssumePod(logger *zap.Logger, pod *corev1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	// 幂等与防御：如果已经在预扣名单里了，直接报错防止重复扣减
	if _, ok := cache.podStates[key]; ok {
		return fmt.Errorf("pod %v is already in the cache, cannot assume", key)
	}

	// 核心预扣逻辑，传入 true 表示这只是乐观预扣（Assume），不是物理转正（Add）
	return cache.addPod(logger, pod, true)
}

// addPod 是真实的底层写入动作，它负责扣减算力并触发二维树的多米诺骨牌效应
// ！！！该函数的上层函数都需要加cache的大锁
func (cache *cacheImpl) addPod(logger *zap.Logger, pod *corev1.Pod, assumePod bool) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	// 从 Pod 本身提取调度决策
	targetNode := pod.Spec.NodeName
	// 假设你把我们之前定义的 AnnotationTargetCluster 放到了 framework 包下
	targetCluster := framework.GetClusterNameFromPod(pod)
	// 1. 定位宏观集群
	// TODO 这里因为我们先通过karmada的informer监控到子集群，然后才能创建子集群的client来监控node和pod，
	// TODO 所以当pod事件过来，缓存中一定有对应的cluster，除非集群此时忽然挂了，但是集群挂了我们也没有必要
	// TODO 继续更新该集群的缓存了，而且当集群恢复，还会进行全量事件的同步，数据一致性也能保证。
	clusterItem, ok := cache.clusters[targetCluster]
	if !ok {
		// 防御性编程：理论上不会发生，除非调度期间集群被拔网线了
		return fmt.Errorf("cluster %s not found in cache", targetCluster)
	}

	// 这里只负责集群宏观资源的更新，具体NodeInfo的细节资源更新放到下面的nodeItem.info.AddPod(pod)
	clusterItem.info.AddPod(pod)

	// 2. 定位微观节点
	nodeItem, ok := clusterItem.nodes[targetNode]
	if !ok {
		// K8s 原生设计：如果节点在缓存里还没来得及建立，直接兜底新建
		logger.Debug("Node not found in cache, creating ghost node", zap.String("cluster", targetCluster), zap.String("node", targetNode))
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.NodeName = targetNode
		nodeInfo.ClusterName = targetCluster
		nodeItem = &nodeInfoListItem{info: nodeInfo}
		clusterItem.nodes[targetNode] = nodeItem
	}

	// 3. 核心算力扣减：不仅扣 CPU/Mem，更要精准锁定 GPU UUID 的显存和算力比例
	nodeItem.info.AddPod(pod)

	// 4. 触发二维多米诺效应 (增量快照的灵魂)
	// 第一级：把变动的 Node 移到它所在 Cluster 链表的头部
	cache.moveNodeInfoToHead(logger, clusterItem, nodeItem)
	// 第二级：把变动的 Cluster 移到全局双向链表的头部
	cache.moveClusterToHead(logger, clusterItem)

	// 5. 记录预扣去向，用于后续的 TTL 超时回滚 (ForgetPod)
	ps := &podState{
		pod: pod,
	}
	cache.podStates[key] = ps

	// 6. 加入预扣花名册
	if assumePod {
		cache.assumedPods.Insert(key)
	}

	return nil
}

// moveNodeInfoToHead 将发生了变动的 Node 移动到它所属 Cluster 内部链表的头部
// 假设外层已经加了写锁 (cache.mu.Lock)
func (cache *cacheImpl) moveNodeInfoToHead(logger *zap.Logger, clusterItem *clusterInfoListItem, ni *nodeInfoListItem) {
	// 如果已经在头部了，直接返回，避免无意义的指针操作
	if ni == clusterItem.headNode {
		return
	}

	// 1. 将该节点从当前位置“摘除”
	if ni.prev != nil {
		ni.prev.next = ni.next
	}
	if ni.next != nil {
		ni.next.prev = ni.prev
	}

	// 2. 将该节点“安插”到链表最头部
	if clusterItem.headNode != nil {
		clusterItem.headNode.prev = ni
	}
	ni.next = clusterItem.headNode
	ni.prev = nil
	clusterItem.headNode = ni
}

// removeNodeInfoFromList 从 Cluster 内部链表和宏观账本中彻底摘除一个幽灵/下线节点
func (cache *cacheImpl) removeNodeInfoFromList(logger *zap.Logger, clusterItem *clusterInfoListItem, nodeName string) {
	ni, ok := clusterItem.nodes[nodeName]
	if !ok {
		logger.Warn("No node info found in cluster cache when trying to remove from list",
			zap.String("cluster", clusterItem.info.ClusterName),
			zap.String("node", nodeName))
		return
	}

	// 1. 修复前后邻居的指针关联 (Cache 物理层)
	if ni.prev != nil {
		ni.prev.next = ni.next
	}
	if ni.next != nil {
		ni.next.prev = ni.prev
	}

	// 2. 如果被删除的刚好是头节点，必须更新头指针 (Cache 物理层)
	if ni == clusterItem.headNode {
		clusterItem.headNode = ni.next
	}

	// 3. 从 Cache 层的内部路由字典中彻底抹杀
	delete(clusterItem.nodes, nodeName)

	// ==========================================
	// 🌟 4. [神级补漏] 从 Framework 层的宏观只读字典中彻底抹杀
	// ==========================================
	if clusterItem.info != nil && clusterItem.info.Nodes != nil {
		delete(clusterItem.info.Nodes, nodeName)
	}
}

// moveClusterToHead 将内部发生了资源变动（Pod增删/节点状态更新）的 Cluster 移动到全局链表头部
func (cache *cacheImpl) moveClusterToHead(logger *zap.Logger, ci *clusterInfoListItem) {
	// 如果已经在头部了，直接返回
	if ci == cache.headCluster {
		return
	}

	// 1. 将该集群从当前位置“摘除”
	if ci.prev != nil {
		ci.prev.next = ci.next
	}
	if ci.next != nil {
		ci.next.prev = ci.prev
	}

	// 2. 将该集群“安插”到全局链表最头部
	if cache.headCluster != nil {
		cache.headCluster.prev = ci
	}
	ci.next = cache.headCluster
	ci.prev = nil
	cache.headCluster = ci
}

// removeClusterFromList 当 Karmada 宣判某个子集群彻底死亡/被移除时，从全局链表中将其摘除
func (cache *cacheImpl) removeClusterFromList(logger *zap.Logger, clusterName string) {
	ci, ok := cache.clusters[clusterName]
	if !ok {
		logger.Warn("No cluster info found in cache when trying to remove from list", zap.String("cluster", clusterName))
		return
	}

	// 1. 修复前后邻居的指针关联
	if ci.prev != nil {
		ci.prev.next = ci.next
	}
	if ci.next != nil {
		ci.next.prev = ci.prev
	}

	// 2. 如果被删除的刚好是头节点，更新头指针
	if ci == cache.headCluster {
		cache.headCluster = ci.next
	}

	// 3. 从字典中彻底抹杀 (内部的节点链表会被 Go GC 自动回收，非常安全)
	delete(cache.clusters, clusterName)
}

func (cache *cacheImpl) ForgetPod(logger *zap.Logger, pod *corev1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]

	// 1. 节点一致性校验：防止缓存里的状态和要回滚的状态发生错乱
	if ok && currState.pod.Spec.NodeName != pod.Spec.NodeName {
		return fmt.Errorf("pod %s was assumed on node %s but assigned to node %s",
			key, currState.pod.Spec.NodeName, pod.Spec.NodeName)
	}

	// 2. [Lyra 核心定制] 集群一致性防脑裂校验
	// 在多集群场景下，除了节点要对齐，目标集群也必须绝对一致！
	if ok {
		currCluster := framework.GetClusterNameFromPod(currState.pod)
		targetCluster := framework.GetClusterNameFromPod(pod)
		if currCluster != targetCluster {
			return fmt.Errorf("pod %s was assumed on cluster %s but assigned to cluster %s",
				key, currCluster, targetCluster)
		}
	}

	// 3. 核心断言：只有处于“预扣（Assumed）”状态的 Pod 才有资格被回滚！
	// 如果它已经被 Informer 推送的真实事件转正（Added），或者根本没预扣过，拒绝回滚。
	if ok && cache.assumedPods.Has(key) {
		// 所有的复杂数学逻辑（宏观集群销账、微观节点销账、释放 GPU UUID、LRU/MRU 链表更新）
		// 全都完美封装在了我们之前写好的 removePod 里，这里直接一行调用！
		return cache.removePod(logger, pod)
	}

	return fmt.Errorf("pod %s wasn't assumed so cannot be forgotten", key)
}

// Assumes that lock is already acquired.
func (cache *cacheImpl) updatePod(logger *zap.Logger, oldPod, newPod *corev1.Pod) error {
	if err := cache.removePod(logger, oldPod); err != nil {
		return err
	}
	return cache.addPod(logger, newPod, false)
}

func (cache *cacheImpl) removePod(logger *zap.Logger, pod *corev1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	targetNode := pod.Spec.NodeName
	targetCluster := framework.GetClusterNameFromPod(pod)

	// 防御性拦截：如果是还未调度的 Pod（无目标节点），直接清理内存字典并返回
	if targetNode == "" || targetCluster == "" {
		delete(cache.podStates, key)
		delete(cache.assumedPods, key)
		return nil
	}

	// 1. 定位宏观集群
	clusterItem, ok := cache.clusters[targetCluster]
	if !ok {
		logger.Error("Cluster not found when trying to remove pod",
			zap.String("cluster", targetCluster),
			zap.String("podKey", key))
	} else {
		// 🌟 宏观算力销账：绝对安全的标量减法
		clusterItem.info.RemovePod(logger, pod)

		// 2. 定位微观节点
		nodeItem, ok := clusterItem.nodes[targetNode]
		if !ok {
			logger.Error("Node not found when trying to remove pod",
				zap.String("cluster", targetCluster),
				zap.String("node", targetNode),
				zap.String("podKey", key))
		} else {
			// 🌟 微观算力销账：从切片中精准剔除，并释放 GPU UUID
			if err := nodeItem.info.RemovePod(logger, pod); err != nil {
				return err
			}

			// 3. 幽灵节点垃圾回收 (Ghost Node GC)
			// 如果这台机器本来就不存在（没被真实 Node 事件转正过），且上面唯一的一个预扣 Pod 也被删了
			if len(nodeItem.info.Pods) == 0 && nodeItem.info.Node == nil {
				cache.removeNodeInfoFromList(logger, clusterItem, targetNode)
			} else {
				// 正常节点发生资源变更，推到所属集群的局部链表头部
				cache.moveNodeInfoToHead(logger, clusterItem, nodeItem)
			}
		}

		// 4. 幽灵集群垃圾回收 (Ghost Cluster GC)
		// 级联清理：如果集群下面已经没有任何节点了，且它本身是个没被 Karmada 承认的幽灵集群
		// (假设你在 clusterInfo 里用 cluster 字段存储真实的 *clusterv1alpha1.Cluster 指针)
		if len(clusterItem.nodes) == 0 && clusterItem.info.Cluster() == nil {
			cache.removeClusterFromList(logger, targetCluster)
		} else {
			// 正常集群发生资源变更，推到全局链表头部
			cache.moveClusterToHead(logger, clusterItem)
		}
	}

	// 5. 最后，从全局字典和预扣花名册中抹杀痕迹
	delete(cache.podStates, key)
	delete(cache.assumedPods, key)
	return nil
}

// IsAssumedPod 检查一个 Pod 是否处于预扣（Assumed）状态。
// 调度器在触发 Update 事件时，会依靠此方法判断是否忽略自身的假更新。
func (cache *cacheImpl) IsAssumedPod(pod *corev1.Pod) (bool, error) {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return false, err
	}

	// 纯读操作，使用读锁，提高并发性能
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	return cache.assumedPods.Has(key), nil
}

// GetPod might return a pod for which its node has already been deleted from
// the main cache. This is useful to properly process pod update events.
func (cache *cacheImpl) GetPod(pod *corev1.Pod) (*corev1.Pod, error) {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return nil, err
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	podState, ok := cache.podStates[key]
	if !ok {
		return nil, fmt.Errorf("pod %v(%v) does not exist in scheduler cache", key, klog.KObj(pod))
	}

	return podState.pod, nil
}

func (cache *cacheImpl) FinishBinding(logger *zap.Logger, pod *corev1.Pod) error {
	return cache.finishBinding(logger, pod, time.Now())
}

// finishBinding exists to make tests deterministic by injecting now as an argument
func (cache *cacheImpl) finishBinding(logger *zap.Logger, pod *corev1.Pod, now time.Time) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.RLock()
	defer cache.mu.RUnlock()

	logger.Info("Finished binding for pod, can be expired", zap.String("podKey", key), zap.String("pod", klog.KObj(pod).String()))
	currState, ok := cache.podStates[key]
	if ok && cache.assumedPods.Has(key) {
		if cache.ttl == time.Duration(0) {
			currState.deadline = nil
		} else {
			dl := now.Add(cache.ttl)
			currState.deadline = &dl
		}
		currState.bindingFinished = true
	}
	return nil
}

func (cache *cacheImpl) ClusterCount() int {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return len(cache.clusters)
}

func (cache *cacheImpl) NodeCount() int {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	var nodeCount int
	for _, cluster := range cache.clusters {
		nodeCount += len(cluster.nodes)
	}
	return nodeCount
}

func (cache *cacheImpl) PodCount() (int, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	count := 0
	for _, cluster := range cache.clusters {
		for _, node := range cluster.nodes {
			count += len(node.info.Pods)
		}
	}
	return count, nil
}

func (cache *cacheImpl) AddPod(logger *zap.Logger, pod *corev1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	switch {
	case ok && cache.assumedPods.Has(key):
		// When assuming, we've already added the Pod to cache,
		// Just update here to make sure the Pod's status is up-to-date.
		if err = cache.updatePod(logger, currState.pod, pod); err != nil {
			logger.Error("Error occurred while updating pod", zap.Error(err))
		}
		if currState.pod.Spec.NodeName != pod.Spec.NodeName {
			// The pod was added to a different node than it was assumed to.
			logger.Info("Pod was added to a different node than it was assumed",
				zap.String("podKey", key),
				zap.String("pod", klog.KObj(pod).String()), // 或者直接展开 pod 的名称和命名空间
				zap.String("assumedNode", klog.KRef("", pod.Spec.NodeName).String()),
				zap.String("currentNode", klog.KRef("", currState.pod.Spec.NodeName).String()),
			)
			return nil
		}
	case !ok:
		// Pod was expired. We should add it back.
		if err = cache.addPod(logger, pod, false); err != nil {
			logger.Error("Error occurred while adding pod", zap.Error(err))
		}
	default:
		return fmt.Errorf("pod %v(%v) was already in added state", key, klog.KObj(pod))
	}
	return nil
}

func (cache *cacheImpl) UpdatePod(logger *zap.Logger, oldPod, newPod *corev1.Pod) error {
	key, err := framework.GetPodKey(oldPod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	if !ok {
		return fmt.Errorf("pod %v(%v) is not added to scheduler cache, so cannot be updated", key, klog.KObj(oldPod))
	}

	// An assumed pod won't have Update/Remove event. It needs to have Add event
	// before Update event, in which case the state would change from Assumed to Added.
	if cache.assumedPods.Has(key) {
		return fmt.Errorf("assumed pod %v(%v) should not be updated", key, klog.KObj(oldPod))
	}

	if currState.pod.Spec.NodeName != newPod.Spec.NodeName {
		logger.Error("Pod updated on a different node than previously added to",
			zap.String("podKey", key),
			zap.String("pod", klog.KObj(oldPod).String()),
		)
		logger.Error("scheduler cache is corrupted and can badly affect scheduling decisions")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	return cache.updatePod(logger, oldPod, newPod)
}

func (cache *cacheImpl) RemovePod(logger *zap.Logger, pod *corev1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	currState, ok := cache.podStates[key]
	if !ok {
		return fmt.Errorf("pod %v(%v) is not found in scheduler cache, so cannot be removed from it", key, klog.KObj(pod))
	}
	if currState.pod.Spec.NodeName != pod.Spec.NodeName {
		logger.Error("Pod was added to a different node than it was assumed",
			zap.String("podKey", key),
			zap.String("pod", klog.KObj(pod).String()),
			zap.String("assumedNode", klog.KRef("", pod.Spec.NodeName).String()),
			zap.String("currentNode", klog.KRef("", currState.pod.Spec.NodeName).String()),
		)
		if pod.Spec.NodeName != "" {
			// An empty NodeName is possible when the scheduler misses a Delete
			// event and it gets the last known state from the informer cache.
			logger.Error("scheduler cache is corrupted and can badly affect scheduling decisions")
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
	}
	return cache.removePod(logger, currState.pod)
}

// AddNode 将新节点加入缓存
func (cache *cacheImpl) AddNode(logger *zap.Logger, node *corev1.Node) *framework.NodeInfo {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	clusterName := framework.GetClusterNameFromNode(node)
	nodeName := node.Name

	cNode, exists := cache.clusters[clusterName]
	if !exists {
		cNode = newClusterInfoListItem(framework.NewClusterInfo(clusterName))
		cache.clusters[clusterName] = cNode
	}

	n, exists := cNode.nodes[nodeName]
	if !exists {
		// 1. 你的标准写法：在对象诞生的第一刻，立刻赋予其绝对的身份标识
		info := framework.NewNodeInfo()
		info.NodeName = nodeName
		info.ClusterName = clusterName

		// 2. 包装为双向链表节点，并存入 Cache 层的内部高速字典
		n = newNodeInfoListItem(info)
		cNode.nodes[nodeName] = n

		// ==========================================
		// 🌟 3. 神级补漏：呼应上一回合，把它同步挂载到 Framework 层的宏观只读字典中！
		// ==========================================
		if cNode.info.Nodes == nil {
			cNode.info.Nodes = make(map[string]*framework.NodeInfo)
		}
		cNode.info.Nodes[nodeName] = info
	}

	// 1. 底层数据更新 (内含 Node 世代号推高)
	n.info.SetNode(node)
	// ==========================================
	// 🌟 2. 父集装箱算力聚合
	// ==========================================
	// 将该节点的算力累加到集群宏观账本中 (内部会自动调用 BumpGeneration 推高集群世代号)
	cNode.info.AddNode(n.info)

	// 3. 完美的纯指针物理重排
	cache.moveNodeInfoToHead(logger, cNode, n)
	cache.moveClusterToHead(logger, cNode)

	// 使用你的 Snapshot() 签名
	return n.info.Snapshot()
}

// UpdateNode 更新缓存中已有的节点信息
func (cache *cacheImpl) UpdateNode(logger *zap.Logger, oldNode, newNode *corev1.Node) *framework.NodeInfo {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	// 1. 解析归属信息
	clusterName := framework.GetClusterNameFromNode(newNode)
	nodeName := newNode.Name

	// 2. 宏观寻址：获取或创建集群容器 (防御性兜底)
	cNode, cExists := cache.clusters[clusterName]
	if !cExists {
		cNode = newClusterInfoListItem(framework.NewClusterInfo(clusterName))
		cache.clusters[clusterName] = cNode
	}

	// 3. 微观寻址：获取或创建节点容器 (防御性兜底)
	n, nExists := cNode.nodes[nodeName]
	if !nExists {
		n = newNodeInfoListItem(framework.NewNodeInfo())
		cNode.nodes[nodeName] = n
	}

	// ==========================================
	// 🌟 4. 核心状态机推进
	// ==========================================
	// 1. 先把老节点的算力从集群宏观账本中扣除
	cNode.info.RemoveNode(n.info)

	// 2. 更新节点底层信息 (这里面会用 newNode 覆盖旧账本)
	n.info.SetNode(newNode)

	// 3. 再把新节点的算力累加到集群宏观账本中
	cNode.info.AddNode(n.info)

	// ==========================================
	// 🌟 5. 纯物理指针重排 (时空一致性)
	// ==========================================
	// 将变动的 Node 和 Cluster 双双推到二维 MRU 链表的绝对头部
	cache.moveNodeInfoToHead(logger, cNode, n)
	cache.moveClusterToHead(logger, cNode)

	// 6. 返回安全快照给可能正在等待的组件
	return n.info.Snapshot()
}

// RemoveNode 将节点从缓存中移除
func (cache *cacheImpl) RemoveNode(logger *zap.Logger, node *corev1.Node) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	clusterName := framework.GetClusterNameFromNode(node)
	nodeName := node.Name

	cNode, cExists := cache.clusters[clusterName]
	if !cExists {
		return fmt.Errorf("cluster %v is not found", clusterName)
	}

	n, nExists := cNode.nodes[nodeName]
	if !nExists {
		return fmt.Errorf("node %v is not found in cluster %v", nodeName, clusterName)
	}

	// 在清空节点之前，先把它的算力从集群总账本中减掉
	cNode.info.RemoveNode(n.info)
	// 底层数据抹除 (内含 Node 级别世代号推高)
	n.info.RemoveNode()

	// ==========================================
	// 核心分流：幽灵防御机制
	// ==========================================
	if len(n.info.Pods) == 0 {
		// 情况 A：节点上干干净净，没有正在运行的 Pod。安全抹杀！
		// 调用你写的抹除方法 (内含 delete(cNode.nodes, nodeName) 和链表摘除)
		cache.removeNodeInfoFromList(logger, cNode, nodeName)

		// 抹杀子节点导致父集装箱发生物理改变，上移集群位置！
		cache.moveClusterToHead(logger, cNode)
	} else {
		// 情况 B：机器死了，但网络延迟导致上面的 Pod Delete 事件还没送到调度器手里！
		// 幽灵节点防御：保留它，等待 Pod 驱逐事件到来时再去彻底清理。
		cache.moveNodeInfoToHead(logger, cNode, n)
		cache.moveClusterToHead(logger, cNode)

		logger.Warn("Node removed but pods still present, keeping as ghost node",
			zap.String("cluster", clusterName),
			zap.String("node", nodeName),
			zap.Int("remainingPods", len(n.info.Pods)))
	}

	return nil
}

// AddCluster 处理新子集群接入事件
func (cache *cacheImpl) AddCluster(logger *zap.Logger, cluster *clusterv1alpha1.Cluster) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	clusterName := cluster.Name

	cNode, exists := cache.clusters[clusterName]
	if !exists {
		// 集群容器的创世时刻
		cNode = newClusterInfoListItem(framework.NewClusterInfo(clusterName))
		cache.clusters[clusterName] = cNode
	}

	// 调用你之前写好的 SetCluster，保存 CRD 信息并推高世代号
	cNode.info.SetCluster(cluster)

	// 物理重排：活跃的集群推到链表最前方
	cache.moveClusterToHead(logger, cNode)

	logger.Info("Added new cluster to scheduler cache", zap.String("cluster", clusterName))
}

// UpdateCluster 处理子集群状态更新事件
func (cache *cacheImpl) UpdateCluster(logger *zap.Logger, oldCluster, newCluster *clusterv1alpha1.Cluster) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	clusterName := newCluster.Name

	cNode, exists := cache.clusters[clusterName]
	if !exists {
		// 防御性兜底：如果在 Add 之前收到了 Update
		cNode = newClusterInfoListItem(framework.NewClusterInfo(clusterName))
		cache.clusters[clusterName] = cNode
	}

	// 更新底层的集群 CRD 信息，覆盖老状态
	cNode.info.SetCluster(newCluster)

	// 状态变化，推到链表头部
	cache.moveClusterToHead(logger, cNode)
}

// RemoveCluster 处理子集群被移除的事件 (属于毁灭性操作)
func (cache *cacheImpl) RemoveCluster(logger *zap.Logger, cluster *clusterv1alpha1.Cluster) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	clusterName := cluster.Name

	_, exists := cache.clusters[clusterName]
	if !exists {
		logger.Warn("Cluster not found in cache during removal", zap.String("cluster", clusterName))
		return nil // 幂等操作，不存在就直接返回
	}

	// 🌟 调用你之前写好的绝杀方法
	// 这里面包含了：修复前后指针、更新 headCluster、以及最重要的 delete(cache.clusters, clusterName)
	cache.removeClusterFromList(logger, clusterName)

	// 注意：我们不需要去深度遍历删除 cNode.nodes 里面的节点。
	// 因为整个 cNode 对象失去了 Cache 字典的引用后，
	// Go 语言极其强悍的 GC（垃圾回收器）会自动把挂载在它下面的所有 NodeInfo 和 Pod 账本全部回收掉！
	// 这就是切断根节点的暴力美学。

	logger.Info("Successfully removed cluster and all its internal nodes from cache", zap.String("cluster", clusterName))
	return nil
}

// Dump 导出当前调度器缓存的完整快照，用于 Debug 诊断
func (cache *cacheImpl) Dump() *Dump {
	// 使用读锁：允许并发读，但不允许 Informer 在导出的这一瞬间发生写操作
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	// 1. 初始化最外层的集群快照字典
	clusters := make(map[string]*framework.ClusterInfo, len(cache.clusters))

	// 2. 遍历所有集群
	for clusterName, cItem := range cache.clusters {

		// 深度拷贝集群宏观账本
		cClone := &framework.ClusterInfo{
			ClusterName: cItem.info.ClusterName,
			Generation:  cItem.info.Generation,
			Nodes:       make(map[string]*framework.NodeInfo, len(cItem.nodes)),
		}
		cClone.SetCluster(cItem.info.Cluster())

		// 深度拷贝宏观资源
		if cItem.info.Allocatable != nil {
			cClone.Allocatable = cItem.info.Allocatable.Clone()
		}
		if cItem.info.Requested != nil {
			cClone.Requested = cItem.info.Requested.Clone()
		}

		// 3. 遍历该集群下的所有节点，调用我们在前几回合写好的 Snapshot 方法
		for nodeName, nItem := range cItem.nodes {
			cClone.Nodes[nodeName] = nItem.info.Snapshot()
		}

		clusters[clusterName] = cClone
	}

	// 4. 导出
	return &Dump{
		Clusters: clusters,
		// 使用 k8s 原生 sets 的 Clone/Union 方法深拷贝字符串集合
		AssumedPods: cache.assumedPods.Union(nil),
	}
}
