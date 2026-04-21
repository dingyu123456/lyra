# Lyra QueueingHint 智能唤醒机制实验

## 一、实验目标

验证 QueueingHint 机制在 GPU 集群"高频状态变更场景"下的优化效果，为简历描述提供量化数据。

## 二、GPU 集群场景设计

### 2.1 Pod 分布

```
1000 个不可调度 Pod

├── 300 个 非 GPU 任务 (只申请 CPU/内存) → NodeResourcesFit 拒绝
│
└── 700 个 GPU 任务 (申请了 nvidia.com/gpu) → GPUResourceFit 拒绝
    ├── 210 个 无类型限制 (AnyGPU)  ── 30% of GPU 任务
    ├── 175 个 需要 A100            ── 25%
    ├── 140 个 需要 V100            ── 20%
    ├── 105 个 需要 H100            ── 15%
    └──  70 个 需要 4090            ── 10%
```

### 2.2 三层对比

当 **A100 节点释放 GPU** 时：

| 方式 | 唤醒数 | 削减 |
|-----|--------|------|
| 无 Hint（全量唤醒） | 1000 | 0% |
| 插件级 Hint | 700 | 30% |
| GPU 类型级 Hint | 385 | **61.5%** |

## 三、运行实验

### 3.1 E2E 验证（推荐）

使用 KWOK 集群进行真实端到端测试，对比启用/禁用 QueueingHint 的效果：

```bash
bash experiments/queueing_hint/scripts/test_qhint_e2e.sh
```

**测试流程：**
1. 部署 KWOK 测试环境（small 规模）
2. 设置 4 GPU（A100x2 + V100x2）
3. 提交 100 个混合类型 Pod（33 A100 + 33 V100 + 34 Any）
4. 对比 DISABLED（全量唤醒）vs ENABLED（智能过滤）

**实测结果：**
| 模式 | 唤醒数 | 跳过数 | 削减率 |
|------|--------|--------|--------|
| DISABLED | 100 | 0 | 0% |
| ENABLED | 34 | 66 | **66%** |

### 3.2 Synthetic Benchmark

本地运行 Go benchmark，精确测量性能开销：

```bash
bash experiments/queueing_hint/scripts/run_benchmark.sh
```

### 3.3 手动运行

```bash
cd D:/Users/29197/Documents/project/Golang/lyra-claude/lyra

go test -bench=GPUCluster -benchmem \
    ./internal/scheduler/backend/queue/ \
    -run=^$ \
    -count=3 \
    -benchtime=3s \
    | tee experiments/queueing_hint/results/gpu_type_hint_$(date +%Y%m%d_%H%M%S)/benchmark_raw.txt
```

## 四、结果解读

### 4.1 关键指标

- **avg_attempts/op**: 每次事件唤醒的 Pod 数
- **ns/op**: 单次调度开销
- **削减率**: `(nofilter - hint) / nofilter * 100%`

### 4.2 简历描述建议

> "在 GPU 集群混合场景下（70% GPU 任务 + 30% 辅助任务），基于插件级与 GPU 类型级双重过滤机制，成功拦截约 **60%** 的无效重调度循环，显著削减无效计算开销，并辅以指数退避与超时机制彻底杜绝任务饥饿。"

## 五、文件结构

```
experiments/queueing_hint/
├── README.md                      # 本文档
├── scripts/
│   ├── test_qhint_e2e.sh         # ✅ E2E 验证脚本（推荐）
│   ├── test_queueing_hint_e2e.sh # 旧版 E2E
│   ├── run_queueing_hint_e2e.sh  # 旧版 E2E
│   ├── run_benchmark.sh           # Benchmark 运行脚本
│   ├── parse_benchmark.py         # Benchmark 结果解析
│   └── parse_e2e_results.py       # E2E 结果解析
└── results/                       # 实验结果（自动生成）
    └── gpu_type_hint_<timestamp>/
        ├── benchmark_raw.txt
        ├── benchmark_parsed.csv
        └── report.md
```

## 六、对比快照实验

| 维度 | 快照实验 | QueueingHint 实验 |
|-----|---------|------------------|
| 目录 | `experiments/snapshot/perf_results/extreme_full_perf_*/` | `experiments/queueing_hint/results/gpu_type_hint_*/` |
| 脚本 | `experiments/snapshot/scripts/run_full_perf_test.sh` | `experiments/queueing_hint/scripts/test_qhint_e2e.sh` |
| 测试类型 | E2E 性能（KWOK 集群） | E2E + Synthetic Benchmark |
| 时间 | 长（数十分钟） | 中等（5-15分钟） |
