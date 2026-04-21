#!/bin/bash
# =============================================================================
# Lyra 调度器性能测试 - 持续压力测试
# 用法: ./run_sustained_test.sh <scale> <mode> <rate> <duration>
# scale: small | medium | large
# mode: incremental | full
# rate: 每秒提交的 pod 数量 (默认: 50)
# duration: 持续时间秒数 (默认: 60)
#
# 示例: ./run_sustained_test.sh medium incremental 50 120
#       # 中规模，增量模式，每秒50个，持续2分钟
# =============================================================================

set -e

SCALE=${1:-medium}
MODE=${2:-incremental}
RATE=${3:-50}
DURATION=${4:-60}
DATE=$(date +%Y%m%d_%H%M%S)

# 根据 scale 调整 (与 deploy_fast.sh 保持一致)
case $SCALE in
    small)
        CLUSTERS=2
        NODES_PER_CLUSTER=10
        ;;
    medium)
        CLUSTERS=4
        NODES_PER_CLUSTER=50
        ;;
    large)
        CLUSTERS=10
        NODES_PER_CLUSTER=100
        ;;
    extreme)
        CLUSTERS=20
        NODES_PER_CLUSTER=100
        ;;
    *)
        CLUSTERS=2
        NODES_PER_CLUSTER=10
        ;;
esac

TOTAL_NODES=$((CLUSTERS * NODES_PER_CLUSTER))

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_err() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "=========================================="
echo "  Lyra 调度器持续压力测试"
echo "=========================================="
echo "规模: ${SCALE} (${CLUSTERS}集群 x ${NODES_PER_CLUSTER}节点 = ${TOTAL_NODES}节点)"
echo "模式: ${MODE}"
echo "提交速率: ${RATE} pods/秒"
echo "持续时间: ${DURATION} 秒"
echo "预计 pod 数: $((RATE * DURATION))"
echo "=========================================="

cd "$(dirname "$0")/.."

# 编译
log_info "编译调度器..."
(cd ../.. && go build -o lyra.exe .)

# 部署测试环境
log_info "部署测试环境 (${SCALE})..."
bash scripts/deploy_fast.sh ${SCALE}
sleep 10

# 结果目录
RESULT_DIR="perf_results/${SCALE}_${MODE}_${DATE}"
mkdir -p "${RESULT_DIR}"

# 保存配置
cat > "${RESULT_DIR}/config.json" << EOF
{
  "scale": "${SCALE}",
  "mode": "${MODE}",
  "rate": ${RATE},
  "duration_sec": ${DURATION},
  "total_nodes": ${TOTAL_NODES},
  "clusters": ${CLUSTERS},
  "nodes_per_cluster": ${NODES_PER_CLUSTER}
}
EOF

# 设置快照模式
if [ "${MODE}" = "full" ]; then
    export LYRA_SNAPSHOT_MODE=full
else
    unset LYRA_SNAPSHOT_MODE
fi

# 启动调度器
log_info "启动调度器 (${MODE} 模式)..."
(cd ../.. && ./lyra.exe) > "${RESULT_DIR}/scheduler.log" 2>&1 &
LYRA_PID=$!
sleep 8

# 验证调度器启动
if ! ps -p ${LYRA_PID} > /dev/null 2>&1; then
    log_err "调度器启动失败！"
    cat "${RESULT_DIR}/scheduler.log" | tail -20
    exit 1
fi

# 清理旧 pod
log_info "清理旧 Pod..."
kubectl --kubeconfig="../../kubeconfig/karmada-apiserver.config" delete pods -l batch=sustained-test --ignore-not-found 2>/dev/null || true
sleep 3

# 记录开始时间
TEST_START=$(date +%s)

# 开始持续提交
log_info "开始持续压力测试..."
log_info "速率: ${RATE} pods/s, 持续: ${DURATION}s, 预计: $((RATE * DURATION)) pods"
bash scripts/submit_sustained.sh ${RATE} ${DURATION} default

# 测试结束后等待一段时间，让调度器处理完积压
log_info "等待调度器处理积压..."
sleep 30

# 记录结束时间
TEST_END=$(date +%s)
TOTAL_ELAPSED=$((TEST_END - TEST_START))

# 收集指标
log_info "收集性能指标..."
bash scripts/collect_metrics.sh "${RESULT_DIR}/scheduler.log" "${RESULT_DIR}/metrics.csv"

# 计算统计
log_info "计算统计数据..."
python3 << EOF
import csv
import json
from collections import defaultdict

csv_path = "${RESULT_DIR}/metrics.csv"
summary_path = "${RESULT_DIR}/summary.json"

# 读取 metrics.csv
records = []
with open(csv_path, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row.get('timestamp'):
            records.append(row)

if records:
    total_pods = len(records)
    total_nodes = int(records[0]['total_nodes']) if records else 0

    # 按 snapshot_mode 分组
    incr = [r for r in records if r.get('snapshot_mode') == 'incremental']
    full = [r for r in records if r.get('snapshot_mode') == 'full']

    def calc_stats(recs):
        if not recs:
            return {}
        snapshot = [float(r['snapshot_us']) for r in recs if r.get('snapshot_us')]
        filter_us = [float(r['filter_us']) for r in recs if r.get('filter_us')]
        score_us = [float(r['score_us']) for r in recs if r.get('score_us')]
        gpu_alloc = [float(r['gpu_alloc_us']) for r in recs if r.get('gpu_alloc_us')]
        cloned_c = [int(r['cloned_clusters']) for r in recs if r.get('cloned_clusters')]
        cloned_n = [int(r['cloned_nodes']) for r in recs if r.get('cloned_nodes')]

        return {
            'count': len(recs),
            'snapshot_us_avg': sum(snapshot)/len(snapshot) if snapshot else 0,
            'filter_us_avg': sum(filter_us)/len(filter_us) if filter_us else 0,
            'score_us_avg': sum(score_us)/len(score_us) if score_us else 0,
            'gpu_alloc_us_avg': sum(gpu_alloc)/len(gpu_alloc) if gpu_alloc else 0,
            'cloned_clusters_avg': sum(cloned_c)/len(cloned_c) if cloned_c else 0,
            'cloned_nodes_avg': sum(cloned_n)/len(cloned_n) if cloned_n else 0,
            'cloned_nodes_max': max(cloned_n) if cloned_n else 0,
        }

    summary = {
        'test_config': {
            'scale': '${SCALE}',
            'mode': '${MODE}',
            'rate': ${RATE},
            'duration_sec': ${DURATION},
            'total_nodes': total_nodes,
        },
        'incremental': calc_stats(incr),
        'full': calc_stats(full),
    }

    # 计算加速比
    if incr and full:
        incr_avg = summary['incremental']['snapshot_us_avg']
        full_avg = summary['full']['snapshot_us_avg']
        if incr_avg > 0:
            summary['snapshot_speedup'] = round(full_avg / incr_avg, 2)

    # 计算 cloned_ratio
    if incr and total_nodes > 0:
        avg_cloned = summary['incremental']['cloned_nodes_avg']
        summary['incremental']['cloned_ratio'] = round(avg_cloned / total_nodes, 3)

    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    print(json.dumps(summary, indent=2))
else:
    print("No records found")
EOF

# 停止调度器
log_info "停止调度器..."
kill ${LYRA_PID} 2>/dev/null || true
sleep 2
kill -9 ${LYRA_PID} 2>/dev/null || true

# 清理环境
log_info "清理测试环境..."
bash scripts/cleanup_fast.sh ${SCALE} 2>/dev/null || true

echo ""
echo "=========================================="
echo "  持续压力测试完成！"
echo "=========================================="
echo "结果目录: ${RESULT_DIR}"
echo "查看报告: cat ${RESULT_DIR}/summary.json"
echo "=========================================="