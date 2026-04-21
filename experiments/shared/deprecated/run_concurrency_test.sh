#!/bin/bash
# =============================================================================
# Lyra 调度器性能测试 - 并发测试脚本
# 用法: ./run_concurrency_test.sh <scale> <mode>
# scale: small | medium | large
# mode: incremental | full
#
# 会依次测试并发数: 10, 20, 50, 100, 200
# =============================================================================

set -e

cd "$(dirname "$0")/.."

SCALE=${1:-small}
MODE=${2:-incremental}
DATE=$(date +%Y%m%d)

# 并发数列表
CONCURRENCY_LIST="10 20 50 100"

# 根据 scale 调整最大并发数
case $SCALE in
    small)
        CONCURRENCY_LIST="10 20 50 100"
        ;;
    medium)
        CONCURRENCY_LIST="10 50 100 200 500"
        ;;
    large)
        CONCURRENCY_LIST="50 100 200 500 1000"
        ;;
esac

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_err() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "=========================================="
echo "  Lyra 调度器并发性能测试"
echo "=========================================="
echo "规模: ${SCALE}"
echo "模式: ${MODE}"
echo "并发数列表: ${CONCURRENCY_LIST}"
echo "日期: ${DATE}"
echo "=========================================="

# 编译
log_info "编译调度器..."
(cd ../.. && go build -o lyra.exe .)

# 部署测试环境（只部署一次）
log_info "部署测试环境 (${SCALE})..."
bash scripts/deploy_fast.sh ${SCALE}
sleep 10

# 结果目录
BASE_RESULT_DIR="perf_results/${SCALE}_${MODE}_${DATE}"
mkdir -p "${BASE_RESULT_DIR}"

# 保存测试配置
cat > "${BASE_RESULT_DIR}/config.json" << EOF
{
  "scale": "${SCALE}",
  "mode": "${MODE}",
  "date": "${DATE}",
  "concurrency_list": "${CONCURRENCY_LIST}"
}
EOF

# 设置快照模式
if [ "${MODE}" = "full" ]; then
    export LYRA_SNAPSHOT_MODE=full
else
    unset LYRA_SNAPSHOT_MODE
fi

# 对每个并发数运行测试
for CONCURRENT in ${CONCURRENCY_LIST}; do
    RESULT_DIR="${BASE_RESULT_DIR}/c${CONCURRENT}"
    mkdir -p "${RESULT_DIR}"

    log_info "====== 测试并发数: ${CONCURRENT} ======"

    # 清理旧 Pod
    log_info "清理旧Pod..."
    kubectl --kubeconfig="../../kubeconfig/karmada-apiserver.config" delete pods -l app=test-pod --ignore-not-found 2>/dev/null || true
    sleep 3

    # 启动调度器
    log_info "启动调度器 (${MODE} 模式)..."
    (cd ../.. && ./lyra.exe) > "${RESULT_DIR}/scheduler.log" 2>&1 &
    LYRA_PID=$!
    sleep 8

    # 验证调度器启动
    if ! ps -p ${LYRA_PID} > /dev/null 2>&1; then
        log_err "调度器启动失败！"
        cat "${RESULT_DIR}/scheduler.log" | tail -20
        continue
    fi

    # 记录开始时间
    START_TIME=$(date +%s)

    # 提交 Pod (并发提交，产生真正的并发压力)
    log_info "并发提交 ${CONCURRENT} 个Pod..."
    bash scripts/submit_pods.sh ${CONCURRENT} default concurrent

    # 等待调度完成
    log_info "等待调度完成..."
    sleep 30

    # 记录结束时间
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))

    # 收集指标
    log_info "收集指标..."
    bash scripts/collect_metrics.sh "${RESULT_DIR}/scheduler.log" "${RESULT_DIR}/metrics.csv"

    # 统计 - "首次调度成功" = 第一次调度就找到可行节点 (feasible_nodes > 0)
    # metrics.csv 已经去重，只保留每个 pod 的首次调度记录
    DISPATCHED=$(tail -n +2 "${RESULT_DIR}/metrics.csv" 2>/dev/null | awk -F',' '$8 > 0 {count++} END {print count+0}')
    [ -z "$DISPATCHED" ] && DISPATCHED=0

    # 调度尝试总数（无论成功失败）
    SCHEDULED=$(tail -n +2 "${RESULT_DIR}/metrics.csv" 2>/dev/null | wc -l | awk '{print $1}')
    [ -z "$SCHEDULED" ] && SCHEDULED=0

    # 计算平均 E2E 时延
    E2E_COUNT=$(tail -n +2 "${RESULT_DIR}/metrics_e2e.csv" 2>/dev/null | wc -l | awk '{print $1}')
    [ -z "$E2E_COUNT" ] && E2E_COUNT=0
    if [ "${E2E_COUNT}" -gt 0 ]; then
        AVG_E2E=$(tail -n +2 "${RESULT_DIR}/metrics_e2e.csv" 2>/dev/null | cut -d',' -f4 | awk '{sum+=$1; count++} END {if(count>0) printf "%.3f", sum/count; else print "0"}')
    else
        AVG_E2E=0
    fi

    # 调度吞吐量 = 1000 / avg_e2e_ms (串行调度器理论吞吐)
    if [ "${AVG_E2E}" != "0" ] && [ "${AVG_E2E}" != "" ]; then
        SCHED_THROUGHPUT=$(awk "BEGIN {printf \"%.1f\", 1000 / ${AVG_E2E}}")
    else
        SCHED_THROUGHPUT=0
    fi

    log_info "结果: ${DISPATCHED}/${SCHEDULED} 首次调度成功, 平均E2E ${AVG_E2E}ms, 调度吞吐 ${SCHED_THROUGHPUT} pods/s"

    # 写入摘要
    cat > "${RESULT_DIR}/summary.json" << EOF
{
  "concurrent": ${CONCURRENT},
  "scheduled": ${SCHEDULED},
  "first_attempt_success": ${DISPATCHED},
  "avg_e2e_ms": ${AVG_E2E},
  "scheduling_throughput_pods_per_sec": ${SCHED_THROUGHPUT}
}
EOF

    # 停止调度器
    log_info "停止调度器..."
    kill ${LYRA_PID} 2>/dev/null || true
    sleep 2
    kill -9 ${LYRA_PID} 2>/dev/null || true

    log_info "====== 并发 ${CONCURRENT} 测试完成 ======"
    echo ""
done

# 清理环境
log_info "清理测试环境..."
bash scripts/cleanup_fast.sh ${SCALE} 2>/dev/null || true

# 生成汇总报告
log_info "生成汇总报告..."
python scripts/plot_perf.py "${BASE_RESULT_DIR}/c"* 2>/dev/null || log_warn "Python出图失败，请手动运行 plot_perf.py"

# 生成 CSV 汇总
SUMMARY_CSV="${BASE_RESULT_DIR}/summary.csv"
echo "concurrent,scheduled,first_attempt_success,avg_e2e_ms,scheduling_throughput_pods_per_sec" > "${SUMMARY_CSV}"
for CONCURRENT in ${CONCURRENCY_LIST}; do
    SUMMARY_FILE="${BASE_RESULT_DIR}/c${CONCURRENT}/summary.json"
    if [ -f "${SUMMARY_FILE}" ]; then
        python -c "
import json
with open('${SUMMARY_FILE}') as f:
    d = json.load(f)
print(f\"{d['concurrent']},{d['scheduled']},{d['first_attempt_success']},{d['avg_e2e_ms']:.3f},{d['scheduling_throughput_pods_per_sec']:.1f}\")
" >> "${SUMMARY_CSV}"
    fi
done

echo ""
echo "=========================================="
echo "  并发测试完成！"
echo "=========================================="
echo "结果目录: ${BASE_RESULT_DIR}"
echo ""
echo "汇总数据:"
cat "${SUMMARY_CSV}"
echo ""
echo "=========================================="