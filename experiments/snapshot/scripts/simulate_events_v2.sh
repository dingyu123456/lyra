#!/bin/bash
# =============================================================================
# 模拟集群背景事件 - 极限并发版 v2
# 用法: bash simulate_events_v2.sh <max_rate> <duration> <nodes_per_cluster> <cluster_list>
#
# 优化点:
#   1. printf 直接管道输出，避免 O(n²) 字符串拼接
#   2. xargs -P 250 固定并发池，控制最大并发数
#   3. 同步等待（不加 &），每秒内事件严格 1 秒内完成
#   4. 预计算集群索引，消除循环内 grep
#   5. Bash 内置算术替代 awk/date
# =============================================================================

# 解除限制
ulimit -n 65535 2>/dev/null || true
ulimit -u 65535 2>/dev/null || true

MAX_RATE=${1:-1000}
DURATION=${2:-300}
NODES_PER_CLUSTER=${3:-100}
shift 3
CLUSTER_LIST=("$@")

NUM_CLUSTERS=${#CLUSTER_LIST[@]}
TOTAL_NODES=$((NUM_CLUSTERS * NODES_PER_CLUSTER))

echo "=========================================="
echo "  模拟集群背景事件 (极限并发版 v2)"
echo "=========================================="
echo "最大事件速率: ${MAX_RATE} events/s (随机 1~${MAX_RATE})"
echo "持续时间: ${DURATION}s"
echo "集群数: ${NUM_CLUSTERS}"
echo "总节点数: ${TOTAL_NODES}"
echo "并发池: 250"
echo "=========================================="

# 预计算集群索引和正确的 context 名称，消除循环内 grep
# KWOK context 名称格式: kwok-<cluster-name>
# 例如: kwok-cluster-0 -> kwok-kwok-cluster-0
declare -a CLUSTER_NUMS
declare -a CONTEXT_NAMES
for i in "${!CLUSTER_LIST[@]}"; do
    cluster="${CLUSTER_LIST[$i]}"
    CLUSTER_NUMS[$i]=$(echo "${cluster}" | grep -o '[0-9]*$')
    CONTEXT_NAMES[$i]="kwok-${cluster}"
done

START_TIME=$(date +%s)
TOTAL_EVENTS=0

for sec in $(seq 1 ${DURATION}); do
    SEC_START=$(date +%s.%N)

    # 随机决定本秒事件数 (1 ~ max_rate)
    RATE=$((1 + RANDOM % MAX_RATE))

    # printf 直接管道输出，避免字符串拼接 O(n²)
    # 每行 8 个参数: --context <cluster> annotate node <node> <ts_key>=<ts> <cpu_key>=<cpu> --overwrite
    {
        for i in $(seq 1 ${RATE}); do
            cluster_idx=$((RANDOM % NUM_CLUSTERS))
            context="${CONTEXT_NAMES[${cluster_idx}]}"
            cluster_num="${CLUSTER_NUMS[${cluster_idx}]}"
            node_idx=$((RANDOM % NODES_PER_CLUSTER))
            node_name="gpu-node-${cluster_num}-${node_idx}"

            # Bash 内置生成时间戳和 CPU 值
            ts="${SECONDS}${RANDOM}000"
            cpu_rand=$((RANDOM % 8000))
            cpu_int=$((cpu_rand / 100))
            cpu_dec=$((cpu_rand % 100 / 10))
            cpu_usage="${cpu_int}.${cpu_dec}"

            printf '%s\n' "--context" "${context}" "annotate" "node" "${node_name}" \
                "simulation.alpha/ts=${ts}" "simulation.alpha/cpu=${cpu_usage}" "--overwrite"
        done
    } | xargs -P 250 -n 8 kubectl --grace-period=0 >/dev/null 2>&1

    TOTAL_EVENTS=$((TOTAL_EVENTS + RATE))

    # 节流控制
    SEC_END=$(date +%s.%N)
    SEC_ELAPSED=$(echo "$SEC_END - $SEC_START" | awk '{printf "%.3f", $1}')
    REMAINING=$(echo "1.0 - $SEC_ELAPSED" | awk '{if($1 > 0) printf "%.3f", $1; else print "0"}')

    if [ "$(echo "$REMAINING > 0" | awk '{print ($1 > 0)}')" = "1" ]; then
        sleep "$REMAINING"
    fi

    # 每 10 秒输出进度
    if [ $((sec % 10)) -eq 0 ]; then
        ELAPSED=$(($(date +%s) - START_TIME))
        AVG_RATE=$(awk "BEGIN {printf \"%.1f\", ${TOTAL_EVENTS} / ${ELAPSED}}")
        echo "  [${sec}/${DURATION}s] 已发送: ${TOTAL_EVENTS}, 实际速率: ${AVG_RATE} events/s"
    fi
done

END_TIME=$(date +%s)
TOTAL_ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "=========================================="
echo "  完成"
echo "=========================================="
echo "总事件数: ${TOTAL_EVENTS}"
echo "总耗时: ${TOTAL_ELAPSED}s"
echo "实际平均速率: $(awk "BEGIN {printf \"%.1f\", ${TOTAL_EVENTS} / ${TOTAL_ELAPSED}}") events/s"
echo "=========================================="
