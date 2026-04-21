#!/bin/bash
# =============================================================================
# Lyra 调度器性能测试 - 自动化运行脚本
# 自动执行：清理 → 部署 → 增量快照测试 → 全量快照测试 → 收集结果 → 清理
# 用法: ./scripts/run_perf_test.sh <scale>
# scale: small | medium | large | extreme
# =============================================================================

set -e

cd "$(dirname "$0")/.."

SCALE=${1:-small}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="perf_results/${SCALE}_${TIMESTAMP}"
KARMADA_KUBECONFIG="../../kubeconfig/karmada-apiserver.config"

mkdir -p "${RESULT_DIR}"

echo "=========================================="
echo "  Lyra 调度器性能测试"
echo "=========================================="
echo "测试规模: ${SCALE}"
echo "结果目录: ${RESULT_DIR}"
echo "=========================================="

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_err() { echo -e "${RED}[ERROR]${NC} $1"; }

# 获取各规模的Pod数量
get_pod_count() {
    case $SCALE in
        small)  echo 50 ;;
        medium) echo 200 ;;
        large)  echo 500 ;;
        extreme) echo 1000 ;;
    esac
}

POD_COUNT=$(get_pod_count)

# =============================================================================
# 函数：运行单次测试（增量或全量快照）
# =============================================================================
run_test() {
    local snapshot_type=$1  # incremental 或 full
    local log_file="${RESULT_DIR}/${snapshot_type}.log"
    local csv_file="${RESULT_DIR}/${snapshot_type}.csv"

    log_info "====== 开始 ${snapshot_type} 快照测试 ======"

    # 设置快照模式
    if [ "${snapshot_type}" = "full" ]; then
        export LYRA_SNAPSHOT_MODE=full
    else
        unset LYRA_SNAPSHOT_MODE
    fi

    # 清理旧Pod
    log_info "清理旧Pod..."
    kubectl --kubeconfig="${KARMADA_KUBECONFIG}" delete pods -l batch=perf-test --ignore-not-found 2>/dev/null || true
    kubectl --kubeconfig="${KARMADA_KUBECONFIG}" delete pods -l app=test-pod --ignore-not-found 2>/dev/null || true
    kubectl --kubeconfig="${KARMADA_KUBECONFIG}" delete pods -l app=test-cpu --ignore-not-found 2>/dev/null || true
    sleep 5

    # 启动调度器（必须在仓库根目录运行，因为 config.yaml 在那里）
    log_info "启动Lyra调度器 (${snapshot_type} 快照模式)..."
    (cd ../.. && ./lyra.exe) > "${log_file}" 2>&1 &
    LYRA_PID=$!
    sleep 8

    # 验证调度器启动
    if ! ps -p ${LYRA_PID} > /dev/null 2>&1; then
        log_err "调度器启动失败！"
        cat "${log_file}" | tail -20
        return 1
    fi

    # 等待Cache同步完成
    log_info "等待Cache同步..."
    sleep 5

    # 记录开始时间
    local start_time=$(date +%s)

    # 提交测试Pod
    log_info "提交 ${POD_COUNT} 个Pod..."
    bash scripts/submit_pods.sh ${POD_COUNT}

    # 等待调度完成
    log_info "等待调度完成..."
    sleep 30

    # 记录结束时间
    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))

    # 收集指标
    log_info "收集性能指标..."
    bash scripts/collect_metrics.sh "${log_file}" "${csv_file}"

    # 统计调度成功数
    local dispatched=$(grep "Successfully dispatched" "${log_file}" | grep "test-pod" | wc -l || echo 0)
    local total_perf=$(grep "\[PERF\]" "${log_file}" | grep "test-pod" | wc -l || echo 0)

    log_info "调度结果: ${dispatched}/${POD_COUNT} 成功分发"
    log_info "PERF记录: ${total_perf} 条"
    log_info "总耗时: ${elapsed}s"

    # 停止调度器
    log_info "停止调度器..."
    kill ${LYRA_PID} 2>/dev/null || true
    sleep 3
    kill -9 ${LYRA_PID} 2>/dev/null || true

    # 写入测试摘要
    cat > "${RESULT_DIR}/${snapshot_type}_summary.txt" << EOF
快照模式: ${snapshot_type}
测试规模: ${SCALE}
Pod数量: ${POD_COUNT}
成功分发: ${dispatched}
PERF记录: ${total_perf}
总耗时: ${elapsed}s
EOF

    log_info "====== ${snapshot_type} 快照测试完成 ======"
}

# =============================================================================
# 主流程
# =============================================================================

# Step 0: 编译
log_info "编译调度器..."
(cd ../.. && go build -o lyra.exe .)
cp ../../lyra.exe .

# Step 1: 清理旧环境
log_info "Step 1: 清理旧环境..."
bash scripts/cleanup_fast.sh ${SCALE} 2>/dev/null || true
sleep 5

# Step 2: 部署测试环境
log_info "Step 2: 部署测试环境 (${SCALE})..."
bash scripts/deploy_fast.sh ${SCALE}
sleep 10

# Step 3: 运行增量快照测试
log_info "Step 3: 运行增量快照测试..."
run_test incremental

# Step 4: 运行全量快照测试
log_info "Step 4: 运行全量快照测试..."
run_test full

# Step 5: 生成对比报告
log_info "Step 5: 生成对比报告..."
cat > "${RESULT_DIR}/comparison.md" << 'REPORT_HEADER'
# Lyra 调度器性能对比报告
REPORT_HEADER

echo "" >> "${RESULT_DIR}/comparison.md"
echo "## 测试配置" >> "${RESULT_DIR}/comparison.md"
echo "" >> "${RESULT_DIR}/comparison.md"
echo "| 项目 | 值 |" >> "${RESULT_DIR}/comparison.md"
echo "|------|-----|" >> "${RESULT_DIR}/comparison.md"
echo "| 测试规模 | ${SCALE} |" >> "${RESULT_DIR}/comparison.md"
echo "| Pod数量 | ${POD_COUNT} |" >> "${RESULT_DIR}/comparison.md"
echo "| 时间 | $(date '+%Y-%m-%d %H:%M:%S') |" >> "${RESULT_DIR}/comparison.md"
echo "" >> "${RESULT_DIR}/comparison.md"

# 解析CSV生成对比表
if [ -f "${RESULT_DIR}/incremental.csv" ] && [ -f "${RESULT_DIR}/full.csv" ]; then
    echo "## 性能对比" >> "${RESULT_DIR}/comparison.md"
    echo "" >> "${RESULT_DIR}/comparison.md"
    echo "| 指标 | 增量快照 | 全量快照 | 倍数 |" >> "${RESULT_DIR}/comparison.md"
    echo "|------|---------|---------|------|" >> "${RESULT_DIR}/comparison.md"

    # 计算平均值
    for metric_name in "snapshot" "filter" "score" "gpu_alloc"; do
        col_map_snapshot="snapshot_us:4,filter_us:5,score_us:6,gpu_alloc_us:7"
        case ${metric_name} in
            snapshot) incr_avg=$(tail -n +2 "${RESULT_DIR}/incremental.csv" | cut -d',' -f4 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}'); full_avg=$(tail -n +2 "${RESULT_DIR}/full.csv" | cut -d',' -f4 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}'); label="快照更新(μs)" ;;
            filter) incr_avg=$(tail -n +2 "${RESULT_DIR}/incremental.csv" | cut -d',' -f5 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}'); full_avg=$(tail -n +2 "${RESULT_DIR}/full.csv" | cut -d',' -f5 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}'); label="Filter(μs)" ;;
            score) incr_avg=$(tail -n +2 "${RESULT_DIR}/incremental.csv" | cut -d',' -f6 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}'); full_avg=$(tail -n +2 "${RESULT_DIR}/full.csv" | cut -d',' -f6 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}'); label="Score(μs)" ;;
            gpu_alloc) incr_avg=$(tail -n +2 "${RESULT_DIR}/incremental.csv" | cut -d',' -f7 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}'); full_avg=$(tail -n +2 "${RESULT_DIR}/full.csv" | cut -d',' -f7 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f", sum/count; else print "0"}'); label="GPU分配(μs)" ;;
        esac

        # 计算倍数
        if [ "${incr_avg}" != "0" ] && [ "${incr_avg}" != "" ] && [ "${full_avg}" != "0" ] && [ "${full_avg}" != "" ]; then
            ratio=$(echo "${full_avg} ${incr_avg}" | awk '{if($2>0) printf "%.2fx", $1/$2; else print "N/A"}')
        else
            ratio="N/A"
        fi

        echo "| ${label} | ${incr_avg} | ${full_avg} | ${ratio} |" >> "${RESULT_DIR}/comparison.md"
    done
fi

echo "" >> "${RESULT_DIR}/comparison.md"
echo "---" >> "${RESULT_DIR}/comparison.md"
echo "报告生成时间: $(date '+%Y-%m-%d %H:%M:%S')" >> "${RESULT_DIR}/comparison.md"

# Step 6: 清理环境
log_info "Step 6: 清理测试环境..."
bash scripts/cleanup_fast.sh ${SCALE} 2>/dev/null || true

# 打印最终结果
echo ""
echo "=========================================="
echo "  性能测试完成！"
echo "=========================================="
echo "结果目录: ${RESULT_DIR}"
echo ""
echo "文件列表:"
ls -la "${RESULT_DIR}/"
echo ""
echo "--- 对比报告 ---"
cat "${RESULT_DIR}/comparison.md"
echo ""
echo "=========================================="
