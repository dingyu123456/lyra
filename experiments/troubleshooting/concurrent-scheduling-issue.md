# Lyra 调度器并发调度问题排查报告

## 问题背景

**发现时间**: 2026-04-16
**问题描述**: 并发提交多个 GPU Pod，无论提交 10 个、30 个还是 100 个，都只有恰好 5 个任务能成功运行，其余任务一直 Pending。

## 环境说明

| 集群 | CPU/内存 | GPU | 说明 |
|-----|---------|-----|------|
| karmada | 多 | 无 | 部署了 Karmada 控制面，也是成员集群 |
| cluster1 | 较多 | 3 张卡 (GTX 1660 SUPER, 6144MiB, 100% 算力) | 子集群 |
| cluster2 | 较少 | 1 张卡 | 子集群 |

每个 GPU Pod 请求: gpumem=60, gpucores=1 (1% 算力)

---

## 排查过程

### 第一阶段：确认问题范围

**测试 1 - 串行提交 5 个 Pod**: 全部 Running，串行调度功能正常

**测试 2 - 在已有 10 个 Running Pod 基础上，并发提交 20 个 Pod**:
```
lyra-perf-11~15: Running (前 5 个成功)
lyra-perf-16~30: Pending (后 15 个失败)
```

### 第二阶段：对比 Running vs Pending 的差异

**关键发现 1: schedulerName 不一致**
```bash
# Running 的 pod (perf-11)
spec.schedulerName: hami-scheduler   # OP 覆盖生效

# Pending 的 pod (perf-16)
spec.schedulerName: lyra-scheduler   # OP 覆盖未生效
```

**关键发现 2: GPU UUID 注解缺失**
- Running 的 pod 有 `nvidia.com/use-gpuuuid` 注解
- Pending 的 pod 没有该注解

### 第三阶段：确认 OP 在 Work 中存在但未生效

检查 Work 资源发现，所有 Work（包括失败的）的 `applied-overrides` 注解中都包含正确的 OP 信息（schedulerName 覆盖 + GPU UUID 注入），但子集群上的实际 Pod 没有应用这些覆盖。

### 第四阶段：确认根因

事件时间线对比：
- **perf-15 (Running)**: SyncWorkSucceed → ApplyOverridePolicySucceed → SyncSucceed（OP 在 Work 同步前被处理）
- **perf-16 (Pending)**: ApplyOverridePolicySucceed → SyncWorkSucceed → SyncSucceed → **SyncFailed**（Work 同步到子集群时 OP 还未处理，Pod 以错误的 schedulerName 创建，后续 reconcile 因 Istio sidecar 注入导致 containers 变更而失败）

---

## 根因

**PP 和 OP 创建顺序导致的竞态条件**。

在 `dispatcher.go` 的 `Dispatch` 方法中，**PP 先于 OP 创建**：

```go
// 原来的顺序
d.ensurePropagationPolicy(...)   // 1. 创建 PP
d.ensureOverridePolicy(...)      // 2. 创建 OP
```

当 Karmada binding controller 处理 PP 时，会立即创建 ResourceBinding 和 Work。如果 OP 还没被创建，Work 就会被同步到子集群，但不带 overrides（schedulerName 仍然是 lyra-scheduler，没有 GPU UUID 注解）。

子集群上没有 lyra-scheduler，Pod 无法被调度，一直 Pending。

---

## 修复方案

**调换 PP 和 OP 的创建顺序，OP 先于 PP 创建**：

```go
// 修复后的顺序
d.ensureOverridePolicy(...)      // 1. 先创建 OP
d.ensurePropagationPolicy(...)   // 2. 再创建 PP
```

这样当 Karmada binding controller 处理 PP 时，OP 已经存在，ResourceBinding 和 Work 会带上正确的 overrides。

---

## 修复验证

### 测试 1: 并发提交 20 个 Pod
```
修复前: 只有 5 个 Running
修复后: 全部 20 个 Running (所有 pod schedulerName=hami-scheduler)
```

### 测试 2: 并发提交 30 个 Pod（累计 50 个）
```
修复前: 只能成功 5 个
修复后: 全部 50 个 Running (约 90 秒内全部就绪)
```

---

## 修改文件

- `internal/scheduler/dispatcher/dispatcher.go`: 调换 PP 和 OP 的创建顺序

---

## 经验教训

1. 在 Karmada 架构下，PP 和 OP 有隐含的顺序依赖：OP 必须在 PP 创建之前存在，否则 Work 可能以不完整的状态被同步到子集群
2. Karmada 的 execution controller 不会在 Work 同步后自动修复因竞态导致的错误状态
3. Istio sidecar 注入会使问题更复杂：Pod 创建后 containers 不可变，导致后续 reconcile 也失败
