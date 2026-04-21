#!/bin/bash
# =============================================================================
# 模拟集群背景事件 - 极限版 v3 (curl 直连 API)
# 用法: bash simulate_events_remote.sh <max_rate> <duration> <nodes_per_cluster> <cluster_list>
#
# 由 run_full_perf_test.sh 通过 scp 传到远程服务器执行
#
# 核心优化:
#   1. curl 直连 Kubernetes API，绕过 kubectl 进程开销 (~35ms vs ~100ms)
#   2. 批量后台执行，每批 200 个并发
#   3. 预计算证书路径和 API 端口
# =============================================================================

ulimit -n 65535 2>/dev/null || true
ulimit -u 65535 2>/dev/null || true

USAGE="用法: bash simulate_events_remote.sh <max_rate> <nodes_per_cluster> <cluster_list>"

MAX_RATE=${1:?"${USAGE}"}
NODES_PER_CLUSTER=${2:-100}
shift 2
CLUSTER_LIST=("$@")

NUM_CLUSTERS=${#CLUSTER_LIST[@]}
TOTAL_NODES=$((NUM_CLUSTERS * NODES_PER_CLUSTER))
CONCURRENCY=200

# 事件统计日志文件
STATS_FILE="/tmp/event_stats_$$.log"
echo "timestamp,actual_rate,total_events" > "${STATS_FILE}"

echo "=========================================="
echo "  模拟集群背景事件 (极限版 v3 - curl)"
echo "=========================================="
echo "目标速率: 1~${MAX_RATE} events/s (随机)"
echo "集群数: ${NUM_CLUSTERS} | 总节点: ${TOTAL_NODES}"
echo "统计文件: ${STATS_FILE}"
echo "并发数: ${CONCURRENCY}"
echo "=========================================="

# 预计算每个集群的 API 端口和证书路径
declare -a CLUSTER_NUMS API_PORTS CA_CERTS ADMIN_CERTS ADMIN_KEYS
for i in "${!CLUSTER_LIST[@]}"; do
    cluster="${CLUSTER_LIST[$i]}"
    cnum=$(echo "${cluster}" | grep -o '[0-9]*$')
    CLUSTER_NUMS[$i]=$cnum

    cert_dir="/root/.kwok/clusters/${cluster}/pki"
    CA_CERTS[$i]="${cert_dir}/ca.crt"
    ADMIN_CERTS[$i]="${cert_dir}/admin.crt"
    ADMIN_KEYS[$i]="${cert_dir}/admin.key"

    # 从 kubeconfig 提取 API server 端口
    kubeconfig="/root/.kwok/clusters/${cluster}/kubeconfig.yaml"
    port=$(grep 'server:' "$kubeconfig" | grep -oP ':\K[0-9]+' | head -1)
    API_PORTS[$i]=$port
done

START_TIME=$(date +%s)
TOTAL_EVENTS=0

while true; do
    SEC_START=$(date +%s.%N)

    RATE=$((1 + RANDOM % MAX_RATE))

    # 批量后台执行 curl
    for ((i=1; i<=RATE; i++)); do
        ci=$((RANDOM % NUM_CLUSTERS))
        cnum="${CLUSTER_NUMS[${ci}]}"
        nidx=$((RANDOM % NODES_PER_CLUSTER))
        node="gpu-node-${cnum}-${nidx}"
        ts="${SECONDS}${RANDOM}000"
        cpu_rand=$((RANDOM % 8000))
        cpu="${cpu_int=$((cpu_rand / 100)).$((cpu_rand % 100 / 10))}"

        curl -s -o /dev/null \
            --cacert "${CA_CERTS[${ci}]}" \
            --cert "${ADMIN_CERTS[${ci}]}" \
            --key "${ADMIN_KEYS[${ci}]}" \
            -X PATCH \
            -H "Content-Type: application/merge-patch+json" \
            -d "{\"metadata\":{\"annotations\":{\"simulation.alpha/ts\":\"${ts}\",\"simulation.alpha/cpu\":\"${cpu}\"}}}" \
            "https://127.0.0.1:${API_PORTS[${ci}]}/api/v1/nodes/${node}" &

        # 每 CONCURRENCY 个请求等一批完成
        if (( i % CONCURRENCY == 0 )); then
            wait
        fi
    done
    wait  # 等待本秒剩余的请求

    TOTAL_EVENTS=$((TOTAL_EVENTS + RATE))

    # 记录每秒实际事件数
    ELAPSED=$(($(date +%s) - START_TIME))
    echo "${ELAPSED},${RATE},${TOTAL_EVENTS}" >> "${STATS_FILE}"

    # 节流控制
    SEC_END=$(date +%s.%N)
    SEC_ELAPSED=$(echo "$SEC_END - $SEC_START" | awk '{printf "%.3f", $1}')
    REMAINING=$(echo "1.0 - $SEC_ELAPSED" | awk '{if($1 > 0) printf "%.3f", $1; else print "0"}')
    if [ "$(echo "$REMAINING > 0" | awk '{print ($1 > 0)}')" = "1" ]; then
        sleep "$REMAINING"
    fi

    ELAPSED=$(($(date +%s) - START_TIME))
    if [ $((ELAPSED % 10)) -eq 0 ]; then
        AVG_RATE=$(awk "BEGIN {printf \"%.1f\", ${TOTAL_EVENTS} / ${ELAPSED}}")
        echo "  [${ELAPSED}s] 已发送: ${TOTAL_EVENTS}, 实际速率: ${AVG_RATE} events/s"
    fi
done

END_TIME=$(date +%s)
TOTAL_ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "=========================================="
echo "  完成"
echo "=========================================="
echo "总事件: ${TOTAL_EVENTS}"
echo "总耗时: ${TOTAL_ELAPSED}s"
echo "实际速率: $(awk "BEGIN {printf \"%.1f\", ${TOTAL_EVENTS} / ${TOTAL_ELAPSED}}") events/s"
echo "=========================================="
