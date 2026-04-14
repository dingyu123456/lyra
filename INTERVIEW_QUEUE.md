# 智能唤醒（队列设计） - 面试准备文档

## 一句话总结
实现三级调度队列（ActiveQ + BackoffQ + UnschedulablePods）配合 QueueingHint 回调机制，插件注册细粒度事件匹配函数实现精准唤醒，避免雷群式重调度风暴，指数退避 + 5 分钟超时兜底防止饥饿。

---

## 核心知识点

### 1. 为什么需要三级队列？

**回答要点**：
- ActiveQ：优先级堆，立即调度，Pop 阻塞等待
- BackoffQ：退避堆，失败后冷却，防止频繁重试
- UnschedulablePods：等待池，资源不足时的 Pod 存储，等事件唤醒

**追问**：两级不够吗？
- 两级（Active + Backoff）会导致所有等待 Pod 都在 BackoffQ
- 每次事件到来需要扫描全部 Pod 判断是否值得唤醒
- UnschedulablePods 作为"冷存储"，只有相关事件才扫描，大幅减少无效唤醒
- 三级分工：立即执行 / 冷却等待 / 事件驱动唤醒

**追问**：Pod 如何移动？
- 新 Pod → ActiveQ
- 调度失败 → BackoffQ（短退避）或 UnschedulablePods（等事件）
- 事件到来 → MoveAllToActiveOrBackoffQueue 批量唤醒
- Backoff 超时 → 自动回 ActiveQ（每秒 flush）
- UnschedulablePods 超时 → 强制回 ActiveQ（5 分钟兜底）

### 2. QueueingHint 机制原理？

**回答要点**：
- 问题：节点加 GPU，10 个 Pod 都在 UnschedulablePods，该唤醒谁？
- 解决：插件注册 `EventsToRegister()` + `QueueingHintFn` 回调
- 调用时机：`MoveAllToActiveOrBackoffQueue(event)` 时遍历所有 Pod
- 对每个 Pod：调用相关插件的 HintFn，返回 Queue/QueueSkip

**追问**：QueueingHintFn 签名？
```go
type QueueingHintFn func(logger, pod, oldObj, newObj) (QueueingHint, error)
```
- 输入：Pod + 事件涉及的旧/新对象（Node、Pod 等）
- 输出：Queue（唤醒）或 QueueSkip（不唤醒）
- 错误默认 Queue（宁可多唤醒，不可丢 Pod）

**追问**：GPUResourceFit 注册了什么事件？
```go
EventsToRegister() []ClusterEvent {
    return []ClusterEvent{
        {Resource: Node, ActionType: Add},
        {Resource: Node, ActionType: Update},
        {Resource: AssignedPod, ActionType: Delete},  // Pod 删除释放 GPU
    }
}
```
- HintFn 检查：Pod 需要 GPU + 事件 Node 有足够 GPU / 删除的 Pod 占用了 GPU

### 3. 唤醒决策流程？

**回答要点**：
- `MoveAllToActiveOrBackoffQueue(event)` 调用
- 先 `isEventOfInterest(event)`：有插件关心此事件类型吗？O(1) 快路径
- 遍历 UnschedulablePods：
  - 获取 Pod 的 `UnschedulablePlugins`（哪些插件拒绝了它）
  - 只检查这些插件的 QueueingHintFn（优化：不相关插件跳过）
  - 任一 HintFn 返回 Queue → 唤醒
  - 全部返回 QueueSkip → 不唤醒

**追问**：唤醒策略有哪些？
- `queueImmediately`：立即进 ActiveQ（Gang 调度成员到达）
- `queueAfterBackoff`：进 BackoffQ 或 ActiveQ（退避状态决定）
- `queueSkip`：留在 UnschedulablePods

**追问**：为什么有 queueImmediately？
- Gang 调度场景：最后一个成员到达，必须立即全部调度
- Permit 阶段返回 Wait 的 Pod 在 PendingPlugins 中
- 检测到 PendingPlugins → queueImmediately，绕过退避

### 4. 指数退避算法？

**回答要点**：
- 初始退避：1 秒
- 最大退避：10 秒
- 算法：每次失败翻倍，达到最大后保持
```
attempt 1: 1s
attempt 2: 2s
attempt 3: 4s
attempt 4: 8s
attempt 5+: 10s (capped)
```

**追问**：BackoffQ 如何组织？
- 最小堆，按退避过期时间排序
- `getBackoffTime(pod) = pod.Timestamp + backoffDuration`
- `flushBackoffQCompleted` 每秒弹出所有过期 Pod → ActiveQ

**追问**：为什么用堆而不是定时器？
- 大量 Pod 时，每 Pod 一个定时器开销大
- 堆：批量 pop，O(k log n) 处理 k 个过期 Pod
- 单次 goroutine 调度处理多个 Pod，高效

### 5. 如何防止饥饿？

**回答要点**：
- `flushUnschedulablePodsLeftover` 每 30 秒检查
- Pod 在 UnschedulablePods 超过 5 分钟 → 强制唤醒
- 使用 `EventUnschedulableTimeout`（wildcard event）触发
- 兜底机制：即使没有相关事件，Pod 也不会永远等待

**追问**：wildcard event 是什么？
- `ClusterEvent{Resource: Wildcard, ActionType: All}`
- 匹配所有 Pod，相当于无条件唤醒
- 用于超时兜底和某些全局事件

---

## 深度拷打问题

### Q1: 10000 Pod 在 UnschedulablePods，节点加 GPU，唤醒开销？

**陷阱**：遍历 10000 Pod 调用 HintFn？

**回答**：
- 先 `isEventOfInterest`：O(1) 检查是否有插件关心此事件
- 只遍历关心此事件的插件注册的 HintFn（过滤）
- 对每个 Pod，只检查 `UnschedulablePlugins`（拒绝它的插件）
- 其他 9000 Pod 可能被不同插件拒绝，GPU 插件拒绝的只有 1000 个
- 实际调用 HintFn：1000 次，不是 10000 次

**追问**：如果 Pod 被 GPU + Memory 两个插件拒绝？
- 分别调用 GPU 插件和 Memory 插件的 HintFn
- GPU 事件：GPU HintFn 返回 Queue，Memory 返回 QueueSkip
- 任一 Queue → 唤醒（策略取最高优先级）
- Memory 插件不关心 Node 事件 → 不注册，根本不调用

### Q2: 调度期间事件到来，Pod 如何处理？

**陷阱**：Pod 正在被调度，资源刚释放，但 Pod 已在 UnschedulablePods？

**回答**：
- `moveRequestCycle` vs `podSchedulingCycle` 机制
- `MoveAllToActiveOrBackoffQueue` 更新 `moveRequestCycle = currentCycle`
- `AddUnschedulableIfNotPresent` 检查：`moveRequestCycle >= podSchedulingCycle`
- 如果事件在 Pod 调度期间到来 → Pod 进 BackoffQ，快速重试
- 防止"错过了释放事件"的死锁

**追问**：具体场景？
- Pod A 调度时无资源，进入 UnschedulablePods
- Pod B 释放资源，事件触发 MoveAll → moveRequestCycle 更新
- Pod A 此时刚完成失败处理，AddUnschedulableIfNotPresent
- 检查 moveRequestCycle >= podSchedulingCycle → Pod A 去 BackoffQ
- 1-2 秒后 Pod A 重新调度，可能获得资源

### Q3: Gang 调度如何通过队列实现？

**陷阱**：Gang 要求同时调度，队列如何支持？

**回答**：
- Permit 阶段返回 Wait → Pod 注册到 `waitingPods` map
- Pod 同时进入 `PendingPlugins` 集合（记录在 QueuedPodInfo）
- 最后一个成员到达时，Permit 插件检测 Gang 完成，调用 Allow
- Allow 触发 bindingCycle 继续执行
- 任一成员失败 → Reject → 全部成员的 MoveAll 被触发

**追问**：队列如何参与？
- Pod 在 Permit Wait 状态仍占用队列位置（inFlight）
- Gang 成员到达触发 `MoveAllToActiveOrBackoffQueue`
- 检测 PendingPlugins → queueImmediately
- 全部成员立即进入 ActiveQ，下一轮调度全部成功

### Q4: HintFn 错误处理？

**陷阱**：HintFn 返回错误会怎样？

**回答**：
- 错误默认 Queue（保守唤醒）
- 设计原则：宁可多唤醒一次，不可丢失 Pod
- 多唤醒成本：一次无效调度尝试
- 丢失 Pod：永远不调度，严重问题

**追问**：HintFn 可能的错误？
- 对象为 nil（删除事件 newObj 为 nil）
- 解析失败（annotation 格式错误）
- 内部 panic（被 recover 捕获，转为 error）

### Q5: 堆的并发安全？

**陷阱**：多个 goroutine 同时 Push/Pop 堆？

**回答**：
- 堆操作在 PriorityQueue 大锁内（`pq.lock`）
- Add/Update/Delete/MoveAll 都持有锁
- Pop 使用 `sync.Cond` 等待，不轮询
- `flushBackoffQCompleted` 和 `flushUnschedulablePodsLeftover` 也持锁

**追问**：Cond 如何工作？
- Pop 时 ActiveQ 为空 → `cond.Wait()` 阻塞
- Add 时 → `cond.Broadcast()` 唤醒 Pop
- 无轮询开销，goroutine 挂起等待

---

## 设计决策辩护

### 为什么用自定义堆而不是 container/heap？

- 需要额外功能：按不同键排序（ActiveQ 按优先级，BackoffQ 按过期时间）
- 需要随机删除（Update/Delete 按 UID 查找）
- container/heap 不支持 key lookup，需要额外 map

### 为什么 Backoff 最大 10 秒？

- 资源释放事件通常秒级到来
- 10 秒内大概率有新资源
- 过长：浪费调度机会
- 过短：频繁无效重试

### 为什么 UnschedulablePods 用 map 而不是堆？

- 无需排序，只需存储和遍历
- map 按 UID 查找 O(1)，堆查找 O(n)
- 遍历顺序不重要（每个 Pod 独立判断）

---

## 性能对比表

| 操作 | Lyra QueueingHint | K8s 旧版 preCheck | 无智能唤醒 |
|------|------------------|------------------|----------|
| 节点事件唤醒 | O(relevantPods) 调用 HintFn | O(allPods) preCheck | O(allPods) 全唤醒 |
| 无关事件 | O(1) isEventOfInterest 快路径跳过 | O(allPods) | O(allPods) |
| HintFn 失败 | 安全唤醒（保守） | 可能丢失 | 无此问题 |
| Gang 支持 | queueImmediately 绕过退避 | 无特殊支持 | 不支持 |

---

## 代码引用点

| 功能 | 文件路径 | 关键函数/结构 |
|------|---------|--------------|
| 队列接口 | `internal/scheduler/backend/queue/interface.go` | `SchedulingQueue` interface |
| 三级队列 | `internal/scheduler/backend/queue/scheduling_queue.go` | `PriorityQueue` struct |
| 堆实现 | `internal/scheduler/backend/heap/heap.go` | `Heap` struct, `Push`, `Pop` |
| QueueingHint | `internal/scheduler/framework/types.go` | `QueueingHintFn`, `ClusterEvent` |
| 事件定义 | `internal/scheduler/framework/events.go` | `EventNodeAdd`, `EventAssignedPodDelete` 等 |
| 唤醒逻辑 | `internal/scheduler/backend/queue/scheduling_queue.go` | `MoveAllToActiveOrBackoffQueue`, `isPodWorthRequeuing` |
| 退避算法 | `internal/scheduler/backend/queue/scheduling_queue.go:438` | `calculateBackoffDuration` |
| 托底超时 | `internal/scheduler/backend/queue/scheduling_queue.go` | `flushUnschedulablePodsLeftover` |
| 插件注册事件 | `internal/scheduler/plugins/gpurender/gpu_resource_fit.go` | `EventsToRegister` |