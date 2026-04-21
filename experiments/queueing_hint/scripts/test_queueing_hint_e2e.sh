#!/bin/bash
# =============================================================================
# Lyra QueueingHint 小规模 E2E 验证实验
# 验证：禁用模式下全量唤醒，启用模式下按 GPU 类型过滤
# =============================================================================

set -e

cd "$(dirname "$0")/../../.."  # 从 scripts/ 到 lyra/

SCALE="small"
POD_COUNT=30  # 提交 30 个 Pod，远超可用 GPU 数量
KCFG="D:/Users/29197/Documents/project/Golang/lyra-claude/lyra/kubeconfig/karmada-apiserver.config"

SERVER1="10.10.100.5"
SERVER2="10.10.100.9"

log_info() { echo -e "\033[0;32m[INFO]\033[0m $1"; }
log_warn() { echo -e "\033[1;33m[WARN]\033[0m $1"; }
log_err() { echo -e "\033[0;31m[ERROR]\033[0m $1"; }

RESULT_BASE="experiments/queueing_hint/results"
RESULT_DIR="${RESULT_BASE}/queueing_hint_e2e_$(date +%Y%m%d_%H%M%S)"
mkdir -p "${RESULT_DIR}"

# =============================================================================
# 部署极小规模环境
# =============================================================================
deploy_tiny() {
    log_info "部署极小规模环境..."
    bash experiments/shared/scripts/cleanup_fast.sh ${SCALE} 2>/dev/null || true
    sleep 10
    bash experiments/shared/scripts/deploy_fast.sh ${SCALE}
    sleep 10
}

# =============================================================================
# 修改集群 GPU 类型注解
# =============================================================================
annotate_gpu_types() {
    log_info "设置节点 GPU 类型注解..."

    # cluster-0: A100
    local kc0="/tmp/kwok_kwok-cluster-0.kubeconfig"
    for i in $(seq 0 1); do  # 只用 2 个节点
        local node="gpu-node-0-${i}"
        local ann="GPU-c0n${i}g0-$(head -c 16 /dev/urandom | xxd -p | head -c 32),81920,100,100,NVIDIA-A100-80GB:"
        ann="${ann}GPU-c0n${i}g1-$(head -c 16 /dev/urandom | xxd -p | head -c 32),81920,100,100,NVIDIA-A100-80GB:"
        ssh root@${SERVER1} "kubectl --kubeconfig=$kc0 annotate node $node hami.io/node-nvidia-register='${ann}' --overwrite" 2>/dev/null || true
    done

    # cluster-1: V100
    local kc1="/tmp/kwok_kwok-cluster-1.kubeconfig"
    for i in $(seq 0 1); do
        local node="gpu-node-1-${i}"
        local ann="GPU-c1n${i}g0-$(head -c 16 /dev/urandom | xxd -p | head -c 32),81920,100,100,NVIDIA-V100-32GB:"
        ann="${ann}GPU-c1n${i}g1-$(head -c 16 /dev/urandom | xxd -p | head -c 32),81920,100,100,NVIDIA-V100-32GB:"
        ssh root@${SERVER2} "kubectl --kubeconfig=$kc1 annotate node $node hami.io/node-nvidia-register='${ann}' --overwrite" 2>/dev/null || true
    done
}

# =============================================================================
# 提交混合 GPU 类型 Pod（带类型注解的 Pod 会被 GPUResourceFit 过滤）
# =============================================================================
submit_mixed_pods() {
    local count=$1
    local image="swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/alpine:3.20.2"

    log_info "提交 ${count} 个混合 GPU 类型 Pod..."

    # 计算比例：1/3 A100, 1/3 V100, 1/3 AnyGPU
    local a100_count=$((count / 3))
    local v100_count=$((count / 3))
    local anygpu_count=$((count - a100_count - v100_count))

    log_info "分布: A100=${a100_count}, V100=${v100_count}, AnyGPU=${anygpu_count}"

    # 提交 A100 Pod
    for i in $(seq 1 $a100_count); do
        cat <<EOF | kubectl --kubeconfig="$KCFG" apply -f - 2>/dev/null || true
apiVersion: v1
kind: Pod
metadata:
  name: pod-a100-$(printf '%04d' $i)
  namespace: default
  labels:
    app: stress-pod
  annotations:
    nvidia.com/use-gputype: "NVIDIA-A100-80GB"
spec:
  schedulerName: lyra-scheduler
  containers:
  - name: c
    image: "${image}"
    command: ["sleep", "3600"]
    resources:
      requests:
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
      limits:
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
EOF
    done

    # 提交 V100 Pod
    for i in $(seq 1 $v100_count); do
        cat <<EOF | kubectl --kubeconfig="$KCFG" apply -f - 2>/dev/null || true
apiVersion: v1
kind: Pod
metadata:
  name: pod-v100-$(printf '%04d' $i)
  namespace: default
  labels:
    app: stress-pod
  annotations:
    nvidia.com/use-gputype: "NVIDIA-V100-32GB"
spec:
  schedulerName: lyra-scheduler
  containers:
  - name: c
    image: "${image}"
    command: ["sleep", "3600"]
    resources:
      requests:
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
      limits:
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
EOF
    done

    # 提交 AnyGPU Pod（无类型限制）
    for i in $(seq 1 $anygpu_count); do
        cat <<EOF | kubectl --kubeconfig="$KCFG" apply -f - 2>/dev/null || true
apiVersion: v1
kind: Pod
metadata:
  name: pod-anygpu-$(printf '%04d' $i)
  namespace: default
  labels:
    app: stress-pod
spec:
  schedulerName: lyra-scheduler
  containers:
  - name: c
    image: "${image}"
    command: ["sleep", "3600"]
    resources:
      requests:
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
      limits:
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
EOF
    done

    log_info "Pod 提交完成"
}

# =============================================================================
# 运行单次测试并收集指标
# =============================================================================
run_test() {
    local mode=$1  # "enabled" 或 "disabled"

    log_info "========== ${mode} 模式测试 =========="

    # 设置环境变量
    if [ "${mode}" = "disabled" ]; then
        export LYRA_QUEUEING_HINT=disabled
        log_info "QueueingHint 已禁用（全量唤醒模式）"
    else
        unset LYRA_QUEUEING_HINT
        log_info "QueueingHint 已启用（智能过滤模式）"
    fi

    # 清理旧资源
    kubectl --kubeconfig="$KCFG" delete pod -l app=stress-pod --ignore-not-found --force --grace-period=0 2>/dev/null || true
    sleep 3

    # 提交 Pod
    submit_mixed_pods ${POD_COUNT}

    # 等待 Pod 入队
    sleep 5

    # 启动调度器
    log_info "启动调度器..."
    ./lyra.exe > "${RESULT_DIR}/scheduler_${mode}.log" 2>&1 &
    LYRA_PID=$!
    sleep 8

    if ! ps -p ${LYRA_PID} > /dev/null 2>&1; then
        log_err "调度器启动失败"
        tail -30 "${RESULT_DIR}/scheduler_${mode}.log"
        return 1
    fi

    # 触发节点更新事件（模拟资源变化）
    log_info "触发节点更新事件..."
    local kc0="/tmp/kwok_kwok-cluster-0.kubeconfig"
    ssh root@${SERVER1} "kubectl --kubeconfig=$kc0 annotate node gpu-node-0-0 hami.io/node-nvidia-register='fake-update' --overwrite" 2>/dev/null || true

    # 等待调度器处理
    sleep 10

    # 统计不可调度 Pod 数量
    local unsched_count=$(kubectl --kubeconfig="$KCFG" get pod -l app=stress-pod --field-selector=status.phase=Pending --no-headers 2>/dev/null | wc -l)
    log_info "剩余 Pending Pod: ${unsched_count}"

    # 停止调度器
    kill ${LYRA_PID} 2>/dev/null || true
    sleep 2
    kill -9 ${LYRA_PID} 2>/dev/null || true

    # 解析 [PERF] 日志
    grep "\[PERF\] Queue wakeup stats" "${RESULT_DIR}/scheduler_${mode}.log" | head -5

    return 0
}

# =============================================================================
# 主流程
# =============================================================================
START_TIME=$(date +%s)

# 编译
log_info "编译调度器..."
go build -o lyra.exe .

# 部署环境
deploy_tiny
annotate_gpu_types

# 等待集群就绪
sleep 10

# 保存测试配置
cat > "${RESULT_DIR}/config.json" <<EOF
{
  "scale": "${SCALE}",
  "pod_count": ${POD_COUNT},
  "pod_distribution": "1/3 A100, 1/3 V100, 1/3 AnyGPU",
  "gpu_types": {"cluster-0": "NVIDIA-A100-80GB", "cluster-1": "NVIDIA-V100-32GB"}
}
EOF

# 模式 1: QueueingHint 禁用（全量唤醒）
run_test disabled

sleep 10

# 模式 2: QueueingHint 启用（智能过滤）
run_test enabled

# 清理
log_info "清理测试环境..."
bash experiments/shared/scripts/cleanup_fast.sh ${SCALE} 2>/dev/null || true

END_TIME=$(date +%s)
echo ""
echo "=========================================="
echo "  测试完成! 耗时: $(( (END_TIME - START_TIME) / 60 )) 分钟"
echo "  结果目录: ${RESULT_DIR}"
echo "=========================================="
