# 架构融合 - 面试准备文档

## 一句话总结
基于 Karmada 的多集群调度器，融合 Hami GPU 虚拟化实现 GPU 精细化调度，采用插件化调度框架支持全生命周期扩展点，通过 PP/OP 实现跨集群分发与精准注入。

---

## 核心知识点

### 1. 为什么选择 Karmada 而不是直接操作多集群 API？

**回答要点**：
- Karmada 提供 PropagationPolicy（分发策略）和 OverridePolicy（覆盖策略）两个 CRD，实现声明式多集群资源管理
- PP 控制"去哪个集群"，OP 控制"落地时的差异化配置"
- 相比直接调用各集群 API，Karmada 作为控制面统一管理，具备重试、状态同步、故障迁移能力
- Lyra 调度器只做决策，Karmada 负责执行，职责分离清晰

**追问**：PP 和 OP 具体如何工作？
- PP：指定 resourceSelector（匹配 Pod），clusterAffinity（目标集群列表）
- OP：指定 targetCluster，使用 JSON Patch overrider 注入 `/spec/nodeName` 和 annotations
- Lyra 创建 PP 时注入 `{podName}-pp`，OP 注入 `{podName}-op`，命名唯一防止冲突

### 2. Hami GPU 虚拟化的原理是什么？

**回答要点**：
- Hami（Heterogeneous AI Computing Virtualization Middleware）实现 GPU 时间片和内存切分
- 物理 GPU 被虚拟化为多个 vGPU，每个 Pod 可申请部分显存和算力
- 通过 Node annotation `hami.io/node-nvidia-register` 上报物理卡库存
- 通过 Pod annotation `hami.io/vgpu-devices-allocated` 记录实际分配

**追问**：Lyra 如何解析和调度 GPU？
- `ParseNodeHamiAnnotation` 解析 Node 上的 GPU inventory：`UUID,MaxVGPU,MemTotal,CoreTotal,Index,Type`
- `ParsePodGPUReqs` 从 Pod 的 resource limit 提取需求：`nvidia.com/gpu`（卡数）、`nvidia.com/gpumem`（显存）、`nvidia.com/gpucores`（算力百分比）
- Filter 阶段检查每张物理卡的剩余资源是否满足需求
- Score 阶段使用 Best-Fit 算法，优先选择碎片化最小的卡
- Bind 后通过 `InjectHamiVGPUAnnotation` 注入 `nvidia.com/use-gpuuuid` 指定物理卡 UUID

### 3. 插件化调度框架的设计？

**回答要点**：
- 参考 Kubernetes Scheduler Framework，定义 9 个扩展点：
  - QueueSort：队列排序（PrioritySort 插件）
  - PreFilter：宏观剪枝，可返回 PreFilterResult 约束候选集群范围
  - Filter：微观过滤，并发检查每个节点
  - PreScore：打分前预处理
  - Score：并发打分，权重合并
  - Reserve：资源预留，失败触发 Unreserve 回滚
  - Permit：Gang 调度等待点，非阻塞注册
  - Bind：绑定执行（KarmadaBind 插件）
  - PostBind：后处理清理

**追问**：PreFilterResult.Merge 如何工作？
- 多个 PreFilter 插件各自返回候选集群集合
- Merge 取交集，逐步缩小范围
- 交集为空时立即返回 Unschedulable，跳过后续 Filter/Score

**追问**：插件如何注册和初始化？
- Registry 模式：`map[string]PluginFactory`，工厂函数签名 `(ctx, obj, handle) -> (Plugin, error)`
- NewFramework 阶段遍历配置中所有插件名，调用工厂创建实例
- 使用泛型 `getExtension[T]` 将插件归类到对应扩展点切片

### 4. CycleState 如何在阶段间传递数据？

**回答要点**：
- CycleState 是 `sync.Map` 包装的键值存储，生命周期为一个调度周期
- `StateData` 接口只需实现 `Clone()` 方法
- GPUResourceFit 在 PreFilter 写入 `GPURequirement`，Filter/Score/allocateGPUsOnNode 读取

**追问**：为什么用 sync.Map 而不是普通 map？
- Filter/Score 并发执行，多个 goroutine 可能同时读取
- sync.Map 读优化，适合"写一次读多次"场景

### 5. 并行化设计？

**回答要点**：
- Filter：使用 Parallelizer 并发检查所有节点，`atomic.AddInt32` 无锁分配索引
- Score：三阶段并行（打分 → NormalizeScore → 权重合并）
- 默认 parallelism=16，chunkSize 基于 `sqrt(n)` 动态计算

**追问**：并发 Filter 如何处理错误？
- ErrorChannel 单槽缓冲，首个 Error 立即 cancel context
- 其他 goroutine 感知 cancel 后退出，避免无效工作

---

## 深度拷打问题

### Q1: 多集群调度如何处理跨集群同名节点冲突？

**陷阱**：不同集群可能有同名 Node，比如都叫 `node-1`

**回答**：
- NodePluginScores 包含 `Name` + `Cluster` 双字段
- ScheduleResult 返回 `(SuggestedCluster, SuggestedNode)` 坐标
- Cache 和 Snapshot 使用 `map[clusterName]map[nodeName]` 二级嵌套结构
- selectHost 选出的 `(cluster, node)` 可以唯一解析

### Q2: AssumePod 乐观预扣后，如果绑定失败如何回滚？

**陷阱**：资源已预扣，其他 Pod 可能因为资源不足被拒绝

**回答**：
- AssumePod 将 Pod 加入 `assumedPods` set，更新 Cache.Requested
- 绑定失败时调用 `ForgetPod`：
  - 从 assumedPods 移除
  - 调用 `removePod` 释放资源
  - 触发 `MoveAllToActiveOrBackoffQueue` 唤醒被拒绝的 Pod
- 完整回滚链：Karmada PP/OP 删除 → Unreserve → ForgetPod → 唤醒等待 Pod

### Q3: Gang 调度如何实现？Permit 阶段不阻塞会怎样？

**陷阱**：Gang 要求所有成员同时调度，单个 Pod 成功会怎样？

**回答**：
- Permit 阶段返回 Wait 时，Pod 注册到 `waitingPods` map
- 主调度循环立即返回，不阻塞，下一个 Pod 继续调度
- 所有成员到达后，Permit 插件调用 `Allow`，bindingCycle goroutine 继续执行
- 任一成员超时/失败，调用 `Reject`，全部成员回滚

### Q4: 如何防止 GPU 分配的碎片化？

**陷阱**：多次分配后，每张卡都剩一点，但无法满足新的大需求

**回答**：
- Score 阶段使用 Best-Fit：优先选择"刚好够用"的卡，减少残渣
- `allocateGPUsOnNode` 在选定节点上再做一次 Best-Fit 精细排序
- 公式：`score = (1 - residualRatio)`，residual 小的得分高
- 多卡需求时，选择组合残渣最小的卡组

### Q5: OverridePolicy 如何注入 GPU UUID？

**陷阱**：JSON Patch 中的路径转义规则

**回答**：
- annotation key 包含 `/`，如 `nvidia.com/use-gpuuuid`
- JSON Patch 路径中 `/` 必须转义为 `~1`
- 路径写法：`/metadata/annotations/nvidia.com~1use-gpuuuid`
- OverridePolicy 使用 PlaintextOverrider，直接注入值

---

## 设计决策辩护

### 为什么用 PP/OP 而不是直接创建 Pod？

- 解耦决策与执行，调度器不感知集群 API 细节
- Karmada 统一处理重试、状态同步、冲突解决
- PP/OP 是 CRD，可被其他工具查询、修改、审计

### 为什么 GPU 调度是单独插件而不是通用资源？

- GPU 有拓扑约束（显存+算力+卡数三维需求）
- 需要 UUID 级别精准绑定，普通 ScalarResources 不支持
- Hami annotation 格式特殊，需要专用解析逻辑

### 为什么 Reserve 阶段失败要逆序 Unreserve？

- 多个 Reserve 插件可能依赖前序插件的状态
- 逆序清理确保依赖关系不被破坏（类似 destructor 逆序调用）

---

## 代码引用点

面试时可自信提到的关键文件和函数：

| 功能 | 文件路径 | 关键函数/结构 |
|------|---------|--------------|
| 主调度循环 | `internal/scheduler/schedule_one.go` | `ScheduleOne`, `schedulingCycle`, `bindingCycle` |
| GPU 解析 | `internal/scheduler/framework/gpu_parse.go` | `ParsePodGPUReqs`, `ParseNodeHamiAnnotation` |
| GPU 调度插件 | `internal/scheduler/plugins/gpurender/gpu_resource_fit.go` | `PreFilter`, `Filter`, `Score` |
| GPU 分配 | `internal/scheduler/schedule_one.go:722` | `allocateGPUsOnNode` |
| Karmada 分发 | `internal/scheduler/dispatcher/dispatcher.go` | `Dispatch`, `ensurePropagationPolicy`, `ensureOverridePolicy` |
| 框架接口 | `internal/scheduler/framework/interface.go` | `Framework`, `Handle`, 所有 Plugin 接口 |
| CycleState | `internal/scheduler/framework/cycle_state.go` | `Write`, `Read`, `Clone` |
| 并行化 | `internal/scheduler/framework/parallelize/` | `Parallelizer.Until`, `ErrorChannel` |