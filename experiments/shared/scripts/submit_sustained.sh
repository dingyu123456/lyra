#!/bin/bash
# =============================================================================
# Lyra 调度器性能测试 - 持续压力提交 (优化版)
# 用法: ./submit_sustained.sh <rate> <duration_sec> [namespace]
# rate: 每秒提交的 pod 数量
# duration_sec: 持续时间（秒）
#
# 优化:
#   1. 每秒所有 Pod 拼成多文档 YAML，一次 kubectl apply 批量提交
#   2. 大 batch 自动拆分为子批次并发提交（每子批次 100 个 Pod）
#   3. 去掉 wait，kubectl 后台执行不阻塞下一秒（开环压力）
#   4. 管道直传，不写临时文件
#
# 示例: ./submit_sustained.sh 100 300  # 每秒100个，持续5分钟
# =============================================================================

RATE=${1:-100}           # pods/秒
DURATION=${2:-60}        # 持续时间（秒）
NAMESPACE=${3:-default}
KARMADA_KUBECONFIG="../../kubeconfig/karmada-apiserver.config"
IMAGE="swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/alpine:3.20.2"
BATCH_SIZE=20            # 每个子批次的 Pod 数（小批次 + 多并发 → 更高吞吐）

# 全局 pod 计数器
COUNTER_FILE="/tmp/lyra_pod_counter.txt"
if [ -f "$COUNTER_FILE" ]; then
    POD_INDEX=$(cat "$COUNTER_FILE")
else
    POD_INDEX=1
fi

TOTAL_PODS=$((RATE * DURATION))

echo "=========================================="
echo "  持续压力提交测试 (优化版)"
echo "=========================================="
echo "速率: ${RATE} pods/秒"
echo "持续时间: ${DURATION} 秒"
echo "预计总数: ${TOTAL_PODS} pods"
echo "子批次大小: ${BATCH_SIZE}"
echo "命名空间: ${NAMESPACE}"
echo "起始序号: ${POD_INDEX}"
echo "=========================================="

# 生成一个子批次的多文档 YAML 到 stdout (使用 awk，比 echo 循环快 10 倍)
gen_batch() {
    local start_idx=$1
    local count=$2
    local end_idx=$((start_idx + count - 1))
    awk -v s=$start_idx -v e=$end_idx -v ns="$NAMESPACE" -v img="$IMAGE" '
    BEGIN { for (i=s; i<=e; i++) {
        print "---"
        print "apiVersion: v1"
        print "kind: Pod"
        print "metadata:"
        print "  name: stress-pod-" i
        print "  namespace: \"" ns "\""
        print "  labels:"
        print "    app: stress-pod"
        print "    batch: \"sustained-test\""
        print "spec:"
        print "  schedulerName: lyra-scheduler"
        print "  containers:"
        print "  - name: fake-container"
        print "    image: \"" img "\""
        print "    command: [\"sleep\", \"3600\"]"
        print "    resources:"
        print "      requests:"
        print "        cpu: \"500m\""
        print "        memory: \"512Mi\""
        print "        nvidia.com/gpu: \"1\""
        print "        nvidia.com/gpumem: \"8192\""
        print "        nvidia.com/gpucores: \"50\""
        print "      limits:"
        print "        cpu: \"500m\""
        print "        memory: \"512Mi\""
        print "        nvidia.com/gpu: \"1\""
        print "        nvidia.com/gpumem: \"8192\""
        print "        nvidia.com/gpucores: \"50\""
        print "  tolerations:"
        print "  - key: \"kwok.x-k8s.io/node\""
        print "    operator: \"Exists\""
        print "    effect: \"NoSchedule\""
    }}'
}

START_TIME=$(date +%s)
SUBMITTED=0

echo "开始提交..."
echo ""

# 持续提交循环（开环：不等待 kubectl 完成）
for sec in $(seq 1 ${DURATION}); do
    SEC_START=$(date +%s.%N)

    # 拆分为子批次并发提交
    NUM_BATCHES=$(( (RATE + BATCH_SIZE - 1) / BATCH_SIZE ))
    for b in $(seq 0 $((NUM_BATCHES-1))); do
        batch_start=$((POD_INDEX + b * BATCH_SIZE))
        if [ $((b + 1)) -eq $NUM_BATCHES ]; then
            batch_count=$((RATE - b * BATCH_SIZE))
        else
            batch_count=$BATCH_SIZE
        fi
        gen_batch $batch_start $batch_count | kubectl --kubeconfig="${KARMADA_KUBECONFIG}" create -f - 2>/dev/null 1>/dev/null &
    done

    POD_INDEX=$((POD_INDEX + RATE))
    SUBMITTED=$((SUBMITTED + RATE))

    # 控制提交节奏（不等待 kubectl 响应）
    SEC_END=$(date +%s.%N)
    SEC_ELAPSED=$(echo "$SEC_END - $SEC_START" | awk '{printf "%.3f", $1}')
    REMAINING=$(echo "1.0 - $SEC_ELAPSED" | awk '{if($1 > 0) printf "%.3f", $1; else print "0"}')

    if [ "$(echo "$REMAINING > 0" | awk '{print ($1 > 0)}')" = "1" ]; then
        sleep "$REMAINING"
    fi

    # 每10秒输出一次进度
    if [ $((sec % 10)) -eq 0 ]; then
        ELAPSED=$(($(date +%s) - START_TIME))
        CURRENT_RATE=$(awk "BEGIN {printf \"%.1f\", ${SUBMITTED} / ${ELAPSED}}")
        echo "  [${sec}/${DURATION}s] 已提交: ${SUBMITTED} pods, 实际速率: ${CURRENT_RATE} pods/s"
    fi
done

# 等待所有后台 kubectl 完成
echo "等待所有提交完成..."
wait

END_TIME=$(date +%s)
TOTAL_ELAPSED=$((END_TIME - START_TIME))

# 保存计数器供下次使用
echo "${POD_INDEX}" > "$COUNTER_FILE"

echo ""
echo "=========================================="
echo "  提交完成"
echo "=========================================="
echo "总提交数: ${SUBMITTED} pods"
echo "总耗时: ${TOTAL_ELAPSED} 秒"
echo "实际速率: $(awk "BEGIN {printf \"%.1f\", ${SUBMITTED} / ${TOTAL_ELAPSED}}") pods/s"
echo "下一个 pod 序号: ${POD_INDEX}"
echo "=========================================="