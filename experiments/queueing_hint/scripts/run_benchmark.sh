#!/bin/bash
# =============================================================================
# Lyra QueueingHint GPU 集群场景 Benchmark 运行脚本
# 用法: bash run_benchmark.sh [benchtime]
# =============================================================================

set -e

cd "$(dirname "$0")/../../.."

BENCHTIME=${1:-3s}
DATE=$(date +%Y%m%d_%H%M%S)
RESULT_BASE="experiments/queueing_hint/results"
RESULT_DIR="${RESULT_BASE}/gpu_type_hint_${DATE}"

mkdir -p "${RESULT_DIR}"

echo "=========================================="
echo "  Lyra QueueingHint GPU 集群场景 Benchmark"
echo "=========================================="
echo "结果目录: ${RESULT_DIR}"
echo "benchtime: ${BENCHTIME}"
echo "=========================================="

# 运行 benchmark
echo ""
echo "正在运行 benchmark..."
go test -bench=GPUCluster -benchmem \
    ./internal/scheduler/backend/queue/ \
    -run=^$ \
    -count=3 \
    -benchtime=${BENCHTIME} \
    | tee "${RESULT_DIR}/benchmark_raw.txt"

echo ""
echo "Benchmark 完成，正在解析结果..."

# 解析结果
if command -v python3 &>/dev/null; then
    python3 experiments/queueing_hint/scripts/parse_benchmark.py \
        "${RESULT_DIR}/benchmark_raw.txt" \
        "${RESULT_DIR}"
    echo ""
    echo "=========================================="
    echo "  结果已保存到: ${RESULT_DIR}"
    echo "=========================================="
    cat "${RESULT_DIR}/report.md"
else
    echo "警告: python3 未安装，跳过结果解析"
    echo "原始结果: ${RESULT_DIR}/benchmark_raw.txt"
fi
