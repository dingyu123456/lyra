# 缓存设计 - 面试准备文档

## 一句话总结
设计二维 MRU 链表 + Generation 原子计数器的增量快照缓存，实现三级资源视图，快照更新仅深拷贝变更节点（O(K) vs O(N)），结合 AssumePod 乐观预扣与 TTL 回滚支撑高吞吐调度。

---

## 核心知识点

### 1. 为什么需要调度缓存？

**回答要点**：
- 调度器需要实时视图：所有集群、所有节点、所有 Pod、所有 GPU 的资源状态
- Informer 事件驱动更新，避免每次调度都调用 API Server
- 缓存提供"一致性快照"，调度周期内视图不变，防止决策漂移
- 支持 AssumePod 乐观预扣，在绑定完成前预留资源

### 2. 二维 MRU 链表的结构？

**回答要点**：
- 第一维：集群级 MRU 链表 `headCluster -> cluster1 -> cluster2 -> ...`
- 第二维：节点级 MRU 链表（每个集群内）`headNode -> node1 -> node2 -> ...`
- 使用侵入式双向链表（指针直接嵌入 struct），移动到头部 O(1)
- MRU = Most Recently Used，最近修改的排在最前面

**追问**：为什么是二维而不是一维扁平结构？
- 多集群天然分层：先选集群（PreFilter 剪枝），再选节点
- 集群级聚合：ClusterInfo 维护该集群总资源，用于宏观剪枝
- 独立 GC：删除集群时，其下所有节点一起清理

**追问**：为什么用 MRU 而不是 LRU？
- 不是做缓存淘汰，而是做变更检测
- 最近修改的排在头部，UpdateSnapshot 从头部遍历，遇到未修改项立即 break
- MRU 顺序保证"变更的先被遍历"，实现增量更新

### 3. Generation 原子计数器的工作原理？

**回答要点**：
- 全局原子变量 `var generation int64`，每次状态变更调用 `atomic.AddInt64(&generation, 1)`
- 每个 ClusterInfo 和 NodeInfo 都有 `.Generation` 字段，记录最后变更版本
- Snapshot 维护 `.generation` 字段，记录上次快照的版本

**追问**：UpdateSnapshot 如何利用 Generation 实现增量？
```go
// 简化逻辑
for cluster := headCluster; cluster != nil; cluster = cluster.next {
    if cluster.info.Generation <= snapshotGeneration {
        break  // 该集群未变更，后面都没变更，提前终止
    }
    // 该集群变更了，深拷贝
    for node := cluster.headNode; node != nil; node = node.next {
        if node.info.Generation <= snapshotGeneration {
            break  // 该节点未变更
        }
        // 深拷贝变更节点
        snapshot.clusters[clusterName].nodes[nodeName] = node.info.DeepCopy()
    }
}
snapshot.generation = currentGeneration
```

**追问**：为什么 Generation 能保证不变项排在后面？
- 每次变更都会调用 `moveToHead()`，把变更项移到链表头部
- 未变更项自然沉底，Generation 小的排在后面
- 单线程修改（全局 RWMutex），顺序稳定

### 4. AssumePod 乐观预扣机制？

**回答要点**：
- 调度决策后，调用 `AssumePod(pod)`：
  - Pod 加入 `assumedPods` set（标记为"已预扣"）
  - 更新 NodeInfo.Requested（扣除 CPU/Memory/GPU）
  - 立即返回，下一调度周期可以看到已预扣的资源
- 绑定成功后调用 `FinishBinding(pod)`：
  - 设置 `bindingFinished = true`
  - 计算 `deadline = now + ttl`（默认 30 秒）
- 后台 `cleanupAssumedPods` 每秒检查：
  - bindingFinished 且 deadline 已过 → 从 assumedPods 移除（等待 Informer 事件确认）
  - Informer 收到真实 Pod 事件 → 从 assumedPods 转为正式 added

**追问**：如果 Informer 事件迟迟不来怎么办？
- TTL 超时后，AssumePod 自动过期，资源释放
- 防止"假绑定"长期占用资源
- 正常流程：Karmada dispatch → 成功 → 成员集群创建 Pod → Informer 收到事件 → 状态转换

**追问**：调度失败如何回滚？
- `ForgetPod(pod)`：
  - 从 assumedPods 移除
  - `removePod()` 释放所有资源
  - 不等 TTL，立即生效

### 5. Snapshot 与 Cache 的关系？

**回答要点**：
- Cache 是"写"视图，接收 Informer 事件，维护完整状态
- Snapshot 是"读"视图，供调度算法使用，只读不可变
- 同一个 Snapshot 对象被复用，UpdateSnapshot 在原地增量更新
- 调度周期内 Snapshot 不变，周期结束后更新

**追问**：为什么复用 Snapshot 而不是每次新建？
- 避免 GC 压力：每次调度都新建，大集群会产生大量对象
- 增量更新只拷贝变更部分，复用未变更部分
- 内存友好，稳态下每次调度只产生少量新对象（变更节点）

**追问**：DeepCopy 拷贝了什么？
- NodeInfo 整体拷贝：Node、Pods 列表、Allocatable/Requested、GPUs map
- GPUs map 是 map[string]*GPUInfo，需要逐项拷贝
- ClusterInfo 只拷贝聚合数据，不拷贝 Nodes map（引用 Snapshot 的）

---

## 深度拷打问题

### Q1: 1000 节点集群，只有 2 个节点变更，快照开销？

**回答**：
- K8s 默认：全量 DeepCopy，O(1000) 次拷贝
- Lyra：Generation 检查 + MRU 顺序，只拷贝 2 个节点，O(2)
- 开销差距：500 倍

**追问**：如何证明 Generation 能覆盖所有变更？
- 所有状态变更入口（AddPod、RemovePod、AddNode 等）都调用 `nextGeneration()`
- 没有遗漏路径，单入口全局锁保证一致性
- Generation bump 后立即 moveToHead，顺序同步

### Q2: 如果节点被删除但还有 Pod 怎么办？

**陷阱**：Node 删除事件先到，Pod 删除事件后到

**回答**：
- Ghost Node 机制：`RemoveNode` 时如果还有 Pod，Node 不真删
- Node 标记为"ghost"：`.Node = nil`，`.Allocatable = {}`
- 等所有 Pod 删除后才彻底移除
- Ghost Node 不参与调度（无 Allocatable），但保留 Pod 记录

**追问**：Pod 事件先于 Node 事件怎么办？
- `addPod` 时如果节点不存在，创建 ghost node placeholder
- 等 Node 事件来时，填充真实 Node 信息
- 事件乱序是 Informer 的常态，必须容忍

### Q3: 全局 RWMutex 会成为瓶颈吗？

**陷阱**：Informer 写阻塞调度读

**回答**：
- 理论上是瓶颈，但实际影响有限：
  - Informer 写频率低（秒级事件量）
  - UpdateSnapshot 临界区短（Generation 检查 + 少量 DeepCopy）
  - 调度周期间隔（每周期一次 Snapshot）
- 可改进方向：分片锁（按集群分片），但增加复杂度

**追问**：为什么不用 channel 驱动？
- Cache 需要支持随机访问（GetNode(cluster, node)）
- channel 是队列模式，不适合 point lookup
- MRU 链表需要指针操作，channel 难以表达

### Q4: DeepCopy 的 GPU map 如何处理？

**陷阱**：GPU map 是 map[string]*GPUInfo，拷贝不彻底会共享指针

**回答**：
- NodeInfo.DeepCopy() 遍历 GPUs map，逐项 DeepCopy
- GPUInfo.DeepCopy() 拷贝所有字段（UUID、Type、Mem/Core 计数）
- 不共享指针，完全独立副本

**追问**：GPU Requested 计数何时更新？
- Pod Add/Remove：ParsePodHamiAnnotation 解析分配，调整 RequestedMem/Core/VGPU
- Node Set：保留已有 Requested（inherit 机制），防止 annotation 刷新丢失预扣

### Q5: Snapshot 的 clusterInfoList 何时重建？

**陷阱**：List 是切片，节点变更后是否重建？

**回答**：
- clusterInfoList 是 `[]ClusterInfo`，用于 List() 方法快速迭代
- 只在集群拓扑变更（Add/Remove Cluster）时重建
- 节点变更不重建，因为 ClusterInfo 本身引用 Snapshot 内的 Nodes map
- rebuild 标志在 UpdateSnapshot 中判断，只有 GC 发现新/删集群才触发

---

## 性能对比表

| 操作 | Lyra | Kubernetes 默认 | 原因 |
|------|------|----------------|------|
| Snapshot 更新 | O(K) 变更节点数 | O(N) 全量节点 | Generation + MRU |
| Generation 检测 | O(1) 整数比较 | O(fields) 全字段比较 | atomic int64 |
| moveToHead | O(1) 指针交换 | N/A 无顺序概念 | 侵入式链表 |
| GetNode | O(1) nested map | O(1) map | 相同 |
| AssumePod | O(1) set + map update | O(1) | 相同 |

---

## 设计决策辩护

### 为什么用侵入式链表而不是独立 container/list？

- 侵入式：指针直接嵌入 struct，无额外分配
- container/list：Element 包装，多一层间接
- moveToHead 只需指针交换，无内存操作

### 为什么 Generation 是全局而不是每节点独立？

- Snapshot 需要一个统一的"版本基准"
- 比较时用 `<= snapshotGeneration`，全局单调递增
- 独立版本需要遍历比较每个，不如原子递增高效

### 为什么 TTL 默认 30 秒？

- 经验值：Karmada dispatch 通常秒级完成
- 30 秒容忍网络抖动、API Server 慢响应
- 过短：正常绑定可能被误清理
- 过长：异常绑定占用资源太久

---

## 代码引用点

| 功能 | 文件路径 | 关键函数/结构 |
|------|---------|--------------|
| Cache 接口 | `internal/scheduler/backend/cache/interface.go` | `Cache` interface |
| Cache 实现 | `internal/scheduler/backend/cache/cache.go` | `cacheImpl`, `UpdateSnapshot` |
| Snapshot 结构 | `internal/scheduler/backend/cache/snapshot.go` | `Snapshot`, `GetNode`, `List` |
| Generation | `internal/scheduler/framework/types.go:22` | `nextGeneration()` |
| NodeInfo/ClusterInfo | `internal/scheduler/framework/types.go` | `NodeInfo`, `ClusterInfo`, `GPUInfo` |
| DeepCopy | `internal/scheduler/framework/types.go` | `DeepCopy()` methods |
| AssumePod | `internal/scheduler/backend/cache/cache.go` | `AssumePod`, `FinishBinding`, `ForgetPod` |
| Ghost Node | `internal/scheduler/backend/cache/cache.go` | `RemoveNode` ghost logic |