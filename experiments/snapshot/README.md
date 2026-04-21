# Lyra 调度器性能测试脚本说明

## 一、脚本概览

| 脚本 | 用途 | 使用场景 |
|------|------|----------|
| `run_full_perf_test.sh` | 全面性能测试 | **主力测试脚本** |
| `deploy_fast.sh` | 快速部署 KWOK 测试环境 | 环境准备（共享脚本） |
| `cleanup_fast.sh` | 快速清理 KWOK 测试环境 | 环境清理（共享脚本） |
| `submit_pods.sh` | 批量提交 Pod（串行/并发） | 手动测试（共享脚本） |
| `submit_sustained.sh` | 持续压力提交 | 手动测试（共享脚本） |
| `collect_metrics.sh` | 收集性能指标 | 从日志提取 CSV（共享脚本） |
| `plot_perf.py` | 生成性能图表 | 分析结果可视化（共享脚本） |

> **共享脚本位置**: `experiments/shared/scripts/`（deploy_fast, cleanup_fast 等基础设施脚本）

---

## 二、核心脚本详解

### 2.1 主力测试脚本

#### `run_full_perf_test.sh` - 全面性能测试 (**推荐**)
```
用法: bash run_full_perf_test.sh <scale> <pod_rate> <duration_min> <event_rate>
scale: small | medium | large | extreme
pod_rate: 每秒提交的 pod 数量 (默认: 100)
duration_min: 持续时间分钟数 (默认: 5)
event_rate: 每秒最大背景事件数 (默认: 50，随机 1~event_rate)
```

**特性：**
- 持续压力提交（可配置速率和持续时间）
- **背景事件模拟**：Pod 提交期间模拟集群节点标签更新，使增量快照更接近真实场景
- 自动测试 incremental 和 full 两种模式
- 完整的性能报告，包含：
  - E2E 时延分布 (P50/P95/Max)
  - 调度吞吐量 (pods/s)
  - 快照加速比
  - 增量快照数据复制量 (cloned_nodes/cloned_ratio)

**示例：**
```bash
# 标准测试：大规模，每秒100个pod，持续5分钟，每秒最多50个背景事件
bash experiments/snapshot/scripts/run_full_perf_test.sh large 100 5 50

# 极限压力：每秒200个pod，持续10分钟，每秒最多100个背景事件
bash experiments/snapshot/scripts/run_full_perf_test.sh large 200 10 100

# 快速验证：每秒50个，持续1分钟，每秒最多30个背景事件
bash experiments/snapshot/scripts/run_full_perf_test.sh medium 50 1 30
```

**输出报告示例：**
```markdown
# Lyra 调度器全面性能测试报告

## 快照性能对比
| 指标 | 增量快照 | 全量快照 | 加速比 |
|------|---------|---------|--------|
| 快照更新 (μs) | 226.43 | 7375.35 | **32.57x** |

## 端到端性能
| 指标 | 增量快照 | 全量快照 |
|------|---------|---------|
| 平均 E2E 时延 | 74.5 ms | 75.2 ms |
| **调度吞吐量** | **13.4 pods/s** | **13.3 pods/s** |

## 增量快照数据复制量
| 指标 | 值 |
|------|-----|
| 平均克隆节点数 | 1.52 |
| 克隆比例 | 0.15% |
```

---

### 2.2 环境准备

#### `deploy_fast.sh` - 快速部署测试环境
```
用法: bash deploy_fast.sh <scale>
scale: small | medium | large | extreme
```
**优化点：**
- 批量生成节点 YAML，一次 SSH 完成一个集群
- 并行创建多个集群
- 本地生成 GPU UUID，避免远程调用
- 独立 kubeconfig，避免 context 合并问题

**规模配置：**
| 规模 | 集群数 | 节点数/集群 | 总节点数 | 部署耗时 |
|------|--------|------------|---------|---------|
| small | 2 | 10 | 20 | ~15秒 |
| medium | 4 | 50 | 200 | ~35秒 |
| large | 10 | 100 | 1000 | ~2.5分钟 |
| extreme | 20 | 100 | 2000 | ~4.5分钟 |

#### `cleanup_fast.sh` - 快速清理测试环境
```
用法: bash cleanup_fast.sh <scale>
```
**优化点：**
- 并行删除 KWOK 集群
- xargs -P 20 批量删除 PP/OP
- 强制删除 Pod (--force --grace-period=0)
- 清理远程服务器临时文件

---

### 2.3 手动测试工具

#### `submit_pods.sh` - 批量提交
```
用法: bash submit_pods.sh <count> [namespace] [mode]
mode: serial | concurrent (默认: serial)
```

#### `submit_sustained.sh` - 持续压力提交
```
用法: bash submit_sustained.sh <rate> <duration> [namespace]
rate: 每秒提交的 pod 数量
duration: 持续时间（秒）
```

---

### 2.4 指标收集与分析

#### `collect_metrics.sh` - 收集性能指标
```
用法: bash collect_metrics.sh <log_file> [output_csv]
```
**输出文件：**
- `{output_csv}.csv` - 各阶段耗时
- `{output_csv}_e2e.csv` - 端到端调度时延

#### `plot_perf.py` - 生成性能图表
```
用法: python3 plot_perf.py <results_dir>
```

---

## 三、推荐测试流程

### 快速验证（5 分钟）
```bash
bash run_full_perf_test.sh medium 50 1
# 查看 experiments/snapshot/perf_results/medium_full_perf_*/comparison.md
```

### 标准测试（30 分钟）
```bash
bash experiments/snapshot/scripts/run_full_perf_test.sh large 100 5
```

### 极限压力测试（1 小时）
```bash
bash experiments/snapshot/scripts/run_full_perf_test.sh large 200 10
```

---

## 四、关键指标说明

| 指标 | 定义 | 用途 |
|------|------|------|
| `snapshot_us` | 快照更新时间 | 核心对比指标 |
| `cloned_nodes` | 增量快照复制的节点数 | 验证增量效果 |
| `cloned_ratio` | `cloned_nodes / total_nodes` | 增量比例（越低越好） |
| `e2e_ms` | Pod 从出队到调度完成的时间 | 端到端性能 |
| `throughput` | `1000 / avg_e2e_ms` | 调度吞吐量 |
| `speedup` | `full_snapshot / incremental_snapshot` | 快照加速比 |

---

## 五、常见问题

**Q: 调度器启动失败？**
- 检查 Karmada 连接：`kubectl --kubeconfig=../kubeconfig/karmada-apiserver.config get cluster`
- 检查日志：`tail -50 scheduler.log`

**Q: Pod 提交失败？**
- 检查命名冲突：Pod 已存在时会报错
- 使用 `stress-pod-{序号}` 命名避免冲突

**Q: metrics.csv 为空？**
- 检查日志是否包含 `[PERF] SchedulePod stage timing`
- 确认快照模式环境变量正确设置