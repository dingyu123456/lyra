#!/bin/bash
# =============================================================================
# Lyra QueueingHint E2E 实验
# 用法: bash run_queueing_hint_e2e.sh [scale] [pod_rate] [duration_min] [event_rate]
#
# 对比有/无 QueueingHint 的队列唤醒效率：
#   Round 1: LYRA_QUEUEING_HINT=enabled  (有 GPU 类型过滤)
#   Round 2: LYRA_QUEUEING_HINT=disabled (无过滤，全量唤醒)
#
# 结果输出到: results/queueing_hint_e2e_<timestamp>/
# =============================================================================

set -e

cd "$(dirname "$0")/../../.."

SCALE=${1:-small}
POD_RATE=${2:-50}
DURATION_MIN=${3:-3}
USER_EVENT_RATE=${4:-}
DURATION_SEC=$((DURATION_MIN * 60))
DATE=$(date +%Y%m%d_%H%M%S)

SERVER1="10.10.100.5"
SERVER2="10.10.100.9"
KCFG="D:/Users/29197/Documents/project/Golang/lyra-claude/lyra/kubeconfig/karmada-apiserver.config"

# 小规模测试即可验证 QueueingHint 效果
case $SCALE in
    small)
        CLUSTERS_PER_SERVER=1
        NODES_PER_CLUSTER=10
        DEFAULT_EVENT_RATE=20
        ;;
    medium)
        CLUSTERS_PER_SERVER=2
        NODES_PER_CLUSTER=30
        DEFAULT_EVENT_RATE=50
        ;;
    *)
        CLUSTERS_PER_SERVER=1
        NODES_PER_CLUSTER=10
        DEFAULT_EVENT_RATE=20
        ;;
esac

CLUSTERS=$((CLUSTERS_PER_SERVER * 2))
TOTAL_NODES=$((CLUSTERS * NODES_PER_CLUSTER))
EVENT_RATE=${USER_EVENT_RATE:-$DEFAULT_EVENT_RATE}
EXPECTED_PODS=$((POD_RATE * DURATION_SEC))

# GPU 类型分配：不同集群分配不同 GPU 类型
declare -A CLUSTER_GPU_TYPES
CLUSTER_GPU_TYPES=(
    ["0"]="NVIDIA-A100-80GB"
    ["1"]="NVIDIA-V100-32GB"
)
DEFAULT_GPU_TYPE="NVIDIA-A100-80GB"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_err() { echo -e "${RED}[ERROR]${NC} $1"; }

RESULT_BASE="experiments/queueing_hint/results"
RESULT_DIR="${RESULT_BASE}/queueing_hint_e2e_${DATE}"
mkdir -p "${RESULT_DIR}"

echo "=========================================="
echo "  Lyra QueueingHint E2E 实验"
echo "=========================================="
echo "规模: ${SCALE} (${CLUSTERS}集群 x ${NODES_PER_CLUSTER}节点 = ${TOTAL_NODES}节点)"
echo "Pod提交速率: ${POD_RATE} pods/s"
echo "背景事件速率: ${EVENT_RATE} events/s"
echo "持续时间: ${DURATION_MIN} 分钟"
echo "结果目录: ${RESULT_DIR}"
echo "=========================================="

# =============================================================================
# 部署环境：不同集群使用不同 GPU 类型
# =============================================================================
deploy_with_mixed_gpu_types() {
    log_info "部署混合 GPU 类型环境..."

    # 清理旧环境
    bash experiments/shared/scripts/cleanup_fast.sh ${SCALE} 2>/dev/null || true
    sleep 5

    # 部署基础集群（复用 deploy_fast.sh）
    bash experiments/shared/scripts/deploy_fast.sh ${SCALE}

    # 修改部分节点的 GPU 类型注解
    log_info "修改节点 GPU 类型注解..."
    for ci in $(seq 0 $((CLUSTERS-1))); do
        gpu_type=${CLUSTER_GPU_TYPES[$ci]:-$DEFAULT_GPU_TYPE}
        log_info "集群 kwok-cluster-${ci}: GPU 类型 = ${gpu_type}"

        # 确定服务器
        if [ $ci -lt $CLUSTERS_PER_SERVER ]; then
            server=$SERVER1
        else
            server=$SERVER2
        fi

        cluster_name="kwok-cluster-${ci}"
        kc="/tmp/kwok_${cluster_name}.kubeconfig"

        # 更新所有节点的 GPU 注解中的类型字段
        for ni in $(seq 0 $((NODES_PER_CLUSTER-1))); do
            node_name="gpu-node-${ci}-${ni}"

            # 生成新的 GPU 注解（替换类型）
            new_annotation=""
            for g in $(seq 0 3); do
                uuid="GPU-c${ci}n${ni}g${g}-$(head -c 16 /dev/urandom | xxd -p | head -c 32)"
                new_annotation="${new_annotation}${uuid},100,81920,100,${gpu_type}:"
            done

            ssh root@$server "kubectl --kubeconfig=$kc annotate node ${node_name} hami.io/node-nvidia-register='${new_annotation}' --overwrite" 2>/dev/null || true
        done
    done
}

# =============================================================================
# 提交带 GPU 类型需求的 Pod
# =============================================================================
submit_pods_with_gpu_types() {
    local rate=$1
    local duration=$2
    local namespace=$3
    local gpu_type_ratio=$4  # 指定 GPU 类型的 Pod 比例 (0.0~1.0)

    local image="swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/alpine:3.20.2"
    local total=$((rate * duration))
    local typed_count=$(echo "$total * $gpu_type_ratio" | awk '{printf "%d", $1}')
    local any_count=$((total - typed_count))

    log_info "提交 ${total} 个 Pod (指定GPU类型: ${typed_count}, 无限制: ${any_count})"

    local counter=0
    local start_time=$(date +%s)

    # 批量提交无类型限制的 Pod
    for i in $(seq 1 $any_count); do
        local pod_name="stress-pod-$(printf '%06d' $counter)"
        cat <<EOF | kubectl --kubeconfig="$KCFG" apply -f - 2>/dev/null || true
apiVersion: v1
kind: Pod
metadata:
  name: ${pod_name}
  namespace: ${namespace}
  labels:
    app: stress-pod
    batch: qhint-test
spec:
  schedulerName: lyra-scheduler
  containers:
  - name: fake-container
    image: "${image}"
    command: ["sleep", "3600"]
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
      limits:
        cpu: "500m"
        memory: "512Mi"
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
EOF
        counter=$((counter + 1))

        # 控制提交速率
        if [ $((counter % rate)) -eq 0 ]; then
            elapsed=$(($(date +%s) - start_time))
            target=$((elapsed + 1))
            while [ $(($(date +%s) - start_time)) -lt $target ]; do
                sleep 0.1
            done
        fi
    done

    # 批量提交指定 GPU 类型的 Pod
    local gpu_types=("NVIDIA-A100-80GB" "NVIDIA-V100-32GB")
    for i in $(seq 1 $typed_count); do
        local pod_name="stress-pod-$(printf '%06d' $counter)"
        local gpu_type=${gpu_types[$((RANDOM % ${#gpu_types[@]}))]}
        cat <<EOF | kubectl --kubeconfig="$KCFG" apply -f - 2>/dev/null || true
apiVersion: v1
kind: Pod
metadata:
  name: ${pod_name}
  namespace: ${namespace}
  labels:
    app: stress-pod
    batch: qhint-test
  annotations:
    nvidia.com/use-gputype: "${gpu_type}"
spec:
  schedulerName: lyra-scheduler
  containers:
  - name: fake-container
    image: "${image}"
    command: ["sleep", "3600"]
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
      limits:
        cpu: "500m"
        memory: "512Mi"
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
EOF
        counter=$((counter + 1))

        if [ $((counter % rate)) -eq 0 ]; then
            elapsed=$(($(date +%s) - start_time))
            target=$((elapsed + 1))
            while [ $(($(date +%s) - start_time)) -lt $target ]; do
                sleep 0.1
            done
        fi
    done

    log_info "提交完成: ${counter} 个 Pod"
}

# =============================================================================
# 启动背景事件模拟
# =============================================================================
start_event_sim() {
    log_info "启动背景事件模拟 (速率: ${EVENT_RATE} events/s)..."

    S1_CLUSTERS=""
    S2_CLUSTERS=""
    for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
        S1_CLUSTERS="${S1_CLUSTERS} kwok-cluster-${i}"
    done
    for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
        idx=$((CLUSTERS_PER_SERVER + i))
        S2_CLUSTERS="${S2_CLUSTERS} kwok-cluster-${idx}"
    done

    scp experiments/shared/scripts/simulate_events_remote.sh root@${SERVER1}:/tmp/simulate_events.sh 2>/dev/null
    scp experiments/shared/scripts/simulate_events_remote.sh root@${SERVER2}:/tmp/simulate_events.sh 2>/dev/null

    ssh root@${SERVER1} "bash /tmp/simulate_events.sh ${EVENT_RATE} ${NODES_PER_CLUSTER} ${S1_CLUSTERS}" > /dev/null 2>&1 &
    SIM_PID1=$!
    ssh root@${SERVER2} "bash /tmp/simulate_events.sh ${EVENT_RATE} ${NODES_PER_CLUSTER} ${S2_CLUSTERS}" > /dev/null 2>&1 &
    SIM_PID2=$!

    log_info "背景事件模拟已启动 (PID: ${SIM_PID1}, ${SIM_PID2})"
}

stop_event_sim() {
    log_info "停止背景事件模拟..."
    kill ${SIM_PID1} ${SIM_PID2} 2>/dev/null || true
    ssh root@${SERVER1} "pkill -f simulate_events" 2>/dev/null || true
    ssh root@${SERVER2} "pkill -f simulate_events" 2>/dev/null || true
}

# =============================================================================
# 清理旧资源
# =============================================================================
cleanup_resources() {
    log_info "清理旧 Pod/PP/OP..."
    kubectl --kubeconfig="$KCFG" delete pod -l app=stress-pod --ignore-not-found --force --grace-period=0 --wait=false 2>/dev/null || true

    OP_LIST=$(kubectl --kubeconfig="$KCFG" get op -A -l app=stress-pod -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' 2>/dev/null)
    if [ -n "$OP_LIST" ]; then
        echo "$OP_LIST" | xargs -P 20 -L 1 bash -c 'kubectl --kubeconfig="D:/Users/29197/Documents/project/Golang/lyra-claude/lyra/kubeconfig/karmada-apiserver.config" delete op -n "$0" "$1" --ignore-not-found --wait=false 2>/dev/null'
    fi

    PP_LIST=$(kubectl --kubeconfig="$KCFG" get pp -A -l app=stress-pod -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' 2>/dev/null)
    if [ -n "$PP_LIST" ]; then
        echo "$PP_LIST" | xargs -P 20 -L 1 bash -c 'kubectl --kubeconfig="D:/Users/29197/Documents/project/Golang/lyra-claude/lyra/kubeconfig/karmada-apiserver.config" delete pp -n "$0" "$1" --ignore-not-found --wait=false 2>/dev/null'
    fi

    log_info "等待资源删除..."
    WAIT_MAX=120
    WAIT_START=$(date +%s)
    while true; do
        PP_COUNT=$(kubectl --kubeconfig="$KCFG" get pp -A -l app=stress-pod --no-headers 2>/dev/null | wc -l)
        POD_COUNT=$(kubectl --kubeconfig="$KCFG" get pod -l app=stress-pod --no-headers 2>/dev/null | wc -l)
        if [ "$PP_COUNT" -eq 0 ] && [ "$POD_COUNT" -eq 0 ]; then
            break
        fi
        WAIT_ELAPSED=$(($(date +%s) - WAIT_START))
        if [ $WAIT_ELAPSED -ge $WAIT_MAX ]; then
            log_warn "清理超时"
            break
        fi
        sleep 5
    done
}

# =============================================================================
# 解析 [PERF] Queue wakeup stats 日志
# =============================================================================
parse_wakeup_stats() {
    local log_file=$1
    local output_csv=$2

    echo "timestamp,event,total_pods,woken_pods,skipped_pods,skip_ratio" > "${output_csv}"

    awk '/\[PERF\] Queue wakeup stats/ {
        timestamp = ""
        event = ""
        total_pods = 0
        woken_pods = 0
        skipped_pods = 0
        skip_ratio = 0

        if (match($0, /"time":"([^"]+)"/, t)) timestamp = t[1]
        if (match($0, /"event":"([^"]+)"/, e)) event = e[1]
        if (match($0, /"total_pods":([0-9]+)/, tp)) total_pods = tp[1]
        if (match($0, /"woken_pods":([0-9]+)/, wp)) woken_pods = wp[1]
        if (match($0, /"skipped_pods":([0-9]+)/, sp)) skipped_pods = sp[1]
        if (match($0, /"skip_ratio":([0-9.]+)/, sr)) skip_ratio = sr[1]

        printf "%s,%s,%s,%s,%s,%.4f\n", timestamp, event, total_pods, woken_pods, skipped_pods, skip_ratio
    }' "${log_file}" >> "${output_csv}"
}

# =============================================================================
# 运行单次测试
# =============================================================================
run_test() {
    local mode=$1  # "hint_enabled" 或 "hint_disabled"
    local result_dir=$2

    log_info "====== 开始 ${mode} 模式测试 ======"

    # 设置环境变量
    if [ "${mode}" = "hint_disabled" ]; then
        export LYRA_QUEUEING_HINT=disabled
    else
        unset LYRA_QUEUEING_HINT
    fi

    # 清理旧资源
    cleanup_resources

    # 启动调度器
    log_info "启动调度器 (${mode})..."
    ./lyra.exe > "${result_dir}/scheduler.log" 2>&1 &
    LYRA_PID=$!
    sleep 5

    if ! ps -p ${LYRA_PID} > /dev/null 2>&1; then
        log_err "调度器启动失败！"
        tail -30 "${result_dir}/scheduler.log"
        return 1
    fi

    # 启动背景事件
    start_event_sim

    # 提交 Pod：50% 指定 GPU 类型，50% 无限制
    submit_pods_with_gpu_types ${POD_RATE} ${DURATION_SEC} default 0.5

    # 等待调度器处理积压
    log_info "等待调度器处理积压..."
    WAIT_MAX=300
    WAIT_ELAPSED=0
    LAST_PP_COUNT=-1
    STABLE_COUNT=0
    while [ $WAIT_ELAPSED -lt $WAIT_MAX ]; do
        PP_COUNT=$(kubectl --kubeconfig="$KCFG" get pp -A -l app=stress-pod --no-headers 2>/dev/null | wc -l)
        if [ "$PP_COUNT" -eq "$LAST_PP_COUNT" ]; then
            STABLE_COUNT=$((STABLE_COUNT + 5))
        else
            STABLE_COUNT=0
        fi
        if [ $STABLE_COUNT -ge 10 ]; then
            log_info "调度器处理完毕: ${PP_COUNT} 个 Pod (${WAIT_ELAPSED}s)"
            break
        fi
        LAST_PP_COUNT=$PP_COUNT
        log_info "已调度 ${PP_COUNT} 个 Pod (${WAIT_ELAPSED}s/${WAIT_MAX}s)"
        sleep 5
        WAIT_ELAPSED=$((WAIT_ELAPSED + 5))
    done

    # 停止背景事件
    stop_event_sim

    # 停止调度器
    log_info "停止调度器..."
    kill ${LYRA_PID} 2>/dev/null || true
    sleep 2
    kill -9 ${LYRA_PID} 2>/dev/null || true

    # 解析日志
    log_info "解析 [PERF] Queue wakeup stats..."
    parse_wakeup_stats "${result_dir}/scheduler.log" "${result_dir}/wakeup_stats.csv"

    # 也收集原有的 schedulePod 指标
    bash experiments/shared/scripts/collect_metrics.sh "${result_dir}/scheduler.log" "${result_dir}/metrics.csv"

    log_info "====== ${mode} 模式测试完成 ======"
}

# =============================================================================
# 主流程
# =============================================================================
START_TIME=$(date +%s)

# 编译
log_info "编译调度器..."
go build -o lyra.exe .

# 部署环境
deploy_with_mixed_gpu_types
sleep 10

# 保存测试配置
cat > "${RESULT_DIR}/config.json" << EOF
{
  "scale": "${SCALE}",
  "clusters": ${CLUSTERS},
  "nodes_per_cluster": ${NODES_PER_CLUSTER},
  "total_nodes": ${TOTAL_NODES},
  "pod_rate": ${POD_RATE},
  "event_rate": ${EVENT_RATE},
  "duration_min": ${DURATION_MIN},
  "duration_sec": ${DURATION_SEC},
  "expected_pods": ${EXPECTED_PODS},
  "gpu_type_ratio": 0.5,
  "date": "${DATE}"
}
EOF

# Round 1: 有 QueueingHint
HINT_DIR="${RESULT_DIR}/hint_enabled"
mkdir -p "${HINT_DIR}"
run_test hint_enabled "${HINT_DIR}"

# Round 2: 无 QueueingHint
NO_HINT_DIR="${RESULT_DIR}/hint_disabled"
mkdir -p "${NO_HINT_DIR}"
run_test hint_disabled "${NO_HINT_DIR}"

# 生成对比报告
log_info "生成对比报告..."
python3 experiments/queueing_hint/scripts/parse_e2e_results.py \
    "${RESULT_DIR}" || true

# 清理
log_info "清理测试环境..."
bash experiments/shared/scripts/cleanup_fast.sh ${SCALE} 2>/dev/null || true

END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))

echo ""
echo "=========================================="
echo "  QueueingHint E2E 实验完成!"
echo "=========================================="
echo "总耗时: $((TOTAL_TIME / 60)) 分钟 $((TOTAL_TIME % 60)) 秒"
echo "结果目录: ${RESULT_DIR}"
echo "=========================================="
