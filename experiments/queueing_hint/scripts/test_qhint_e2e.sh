#!/bin/bash
# =============================================================================
# Lyra QueueingHint E2E 验证实验
# 策略：提交大量 Pod 超过 GPU 总容量 → 大量 Pod 进入不可调度队列
#       触发节点事件 → 观察唤醒行为
# =============================================================================

set -e

# 意外退出时清理调度器
trap 'echo "检测到中断，正在清理..."; pkill -f lyra.exe 2>/dev/null || true; bash experiments/shared/scripts/cleanup_fast.sh small 2>/dev/null || true; exit 130' INT TERM

cd "$(dirname "$0")/../../.."

KCFG="D:/Users/29197/Documents/project/Golang/lyra-claude/lyra/kubeconfig/karmada-apiserver.config"
SERVER1="10.10.100.5"
SERVER2="10.10.100.9"
IMAGE="swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/alpine:3.20.2"
POD_COUNT=100

log_info() { echo -e "\033[0;32m[INFO]\033[0m $1"; }
log_warn() { echo -e "\033[1;33m[WARN]\033[0m $1"; }
log_err() { echo -e "\033[0;31m[ERROR]\033[0m $1"; }

RESULT_BASE="experiments/queueing_hint/results"
RESULT_DIR="${RESULT_BASE}/qhint_e2e_$(date +%Y%m%d_%H%M%S)"
mkdir -p "${RESULT_DIR}"

# =============================================================================
# 部署小集群
# =============================================================================
deploy() {
    log_info "部署小规模集群..."
    bash experiments/shared/scripts/cleanup_fast.sh small 2>/dev/null || true
    sleep 5
    bash experiments/shared/scripts/deploy_fast.sh small
    sleep 15
    log_info "集群部署完成"
}

# =============================================================================
# 为前 2 个节点的每个节点设置 2 GPU（总共 4 GPU）
# =============================================================================
annotate_nodes() {
    log_info "设置 GPU 注解（总共 4 GPU）..."

    # cluster-0 前 2 节点: A100
    kc="/tmp/kwok_kwok-cluster-0.kubeconfig"
    for ni in 0 1; do
        node="gpu-node-0-${ni}"
        ann=""
        for gi in 0 1; do
            uuid="GPU-$(head -c 24 /dev/urandom | xxd -p)"
            ann="${ann}${uuid},81920,100,100,NVIDIA-A100-80GB:"
        done
        ssh root@${SERVER1} "kubectl --kubeconfig=$kc annotate node $node hami.io/node-nvidia-register='${ann}' --overwrite" 2>/dev/null || true
        log_info "  $node: 2xA100"
    done

    # cluster-1 前 2 节点: V100
    kc="/tmp/kwok_kwok-cluster-1.kubeconfig"
    for ni in 0 1; do
        node="gpu-node-1-${ni}"
        ann=""
        for gi in 0 1; do
            uuid="GPU-$(head -c 24 /dev/urandom | xxd -p)"
            ann="${ann}${uuid},81920,100,100,NVIDIA-V100-32GB:"
        done
        ssh root@${SERVER2} "kubectl --kubeconfig=$kc annotate node $node hami.io/node-nvidia-register='${ann}' --overwrite" 2>/dev/null || true
        log_info "  $node: 2xV100"
    done

    # 其余节点无 GPU
    kc="/tmp/kwok_kwok-cluster-0.kubeconfig"
    for ni in $(seq 2 9); do
        ssh root@${SERVER1} "kubectl --kubeconfig=$kc annotate node gpu-node-0-${ni} hami.io/node-nvidia-register='-' --overwrite" 2>/dev/null || true
    done
    kc="/tmp/kwok_kwok-cluster-1.kubeconfig"
    for ni in $(seq 2 9); do
        ssh root@${SERVER2} "kubectl --kubeconfig=$kc annotate node gpu-node-1-${ni} hami.io/node-nvidia-register='-' --overwrite" 2>/dev/null || true
    done

    log_info "GPU 注解完成: cluster-0=2xA100, cluster-1=2xV100 (共4GPU)"
}

# =============================================================================
# 生成并提交 Pod
# =============================================================================
submit_pods() {
    local count=$1
    local yaml_file="/tmp/test_pods.yaml"

    log_info "生成 ${count} 个 Pod 的 YAML..."

    local a100=$((count / 3))
    local v100=$((count / 3))
    local any=$((count - a100 - v100))
    log_info "分布: A100=${a100}, V100=${v100}, Any=${any}"

    rm -f "$yaml_file"

    # A100 Pods
    for i in $(seq 1 $a100); do
        cat >> "$yaml_file" << EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-a100-$(printf '%04d' $i)
  namespace: default
  labels:
    app: stress-pod
  annotations:
    nvidia.com/use-gputype: NVIDIA-A100-80GB
spec:
  schedulerName: lyra-scheduler
  containers:
  - name: c
    image: ${IMAGE}
    command: ["sleep", "7200"]
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
  - key: kwok.x-k8s.io/node
    operator: Exists
    effect: NoSchedule
---
EOF
    done

    # V100 Pods
    for i in $(seq 1 $v100); do
        cat >> "$yaml_file" << EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-v100-$(printf '%04d' $i)
  namespace: default
  labels:
    app: stress-pod
  annotations:
    nvidia.com/use-gputype: NVIDIA-V100-32GB
spec:
  schedulerName: lyra-scheduler
  containers:
  - name: c
    image: ${IMAGE}
    command: ["sleep", "7200"]
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
  - key: kwok.x-k8s.io/node
    operator: Exists
    effect: NoSchedule
---
EOF
    done

    # AnyGPU Pods
    for i in $(seq 1 $any); do
        cat >> "$yaml_file" << EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-any-$(printf '%04d' $i)
  namespace: default
  labels:
    app: stress-pod
spec:
  schedulerName: lyra-scheduler
  containers:
  - name: c
    image: ${IMAGE}
    command: ["sleep", "7200"]
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
  - key: kwok.x-k8s.io/node
    operator: Exists
    effect: NoSchedule
---
EOF
    done

    log_info "提交 Pod..."
    kubectl --kubeconfig="$KCFG" apply -f "$yaml_file" 2>/dev/null || true
    log_info "Pod 提交完成"
}

# =============================================================================
# 等待大部分 Pod 进入 Pending（不可调度）
# =============================================================================
wait_for_pending() {
    log_info "等待 Pod 入队..."

    for i in $(seq 1 24); do  # 最多等 2 分钟
        local pending=$(kubectl --kubeconfig="$KCFG" get pod -l app=stress-pod --field-selector=status.phase=Pending --no-headers 2>/dev/null | wc -l)
        local running=$(kubectl --kubeconfig="$KCFG" get pod -l app=stress-pod --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
        log_info "  Pending=$pending, Running=$running (${i}x5s)"

        if [ $pending -gt 80 ]; then
            log_info "入队完毕: ${pending} Pending"
            return 0
        fi
        sleep 5
    done

    log_warn "等待超时"
    return 1
}

# =============================================================================
# 运行单次测试
# =============================================================================
run_test() {
    local mode=$1

    log_info "========== ${mode} 模式测试 =========="

    # 环境变量
    if [ "$mode" = "disabled" ]; then
        export LYRA_QUEUEING_HINT=disabled
        log_info "QueueingHint: DISABLED (全量唤醒)"
    else
        unset LYRA_QUEUEING_HINT
        log_info "QueueingHint: ENABLED (智能过滤)"
    fi

    # 清理旧 Pod
    kubectl --kubeconfig="$KCFG" delete pod -l app=stress-pod --ignore-not-found --force --grace-period=0 2>/dev/null || true
    sleep 5

    # 提交 Pod
    submit_pods ${POD_COUNT}

    # 等待入队
    wait_for_pending

    local pending_before=$(kubectl --kubeconfig="$KCFG" get pod -l app=stress-pod --field-selector=status.phase=Pending --no-headers 2>/dev/null | wc -l)
    log_info "入队后: ${pending_before} 个 Pending"

    # 启动调度器
    log_info "启动调度器..."
    ./lyra.exe > "${RESULT_DIR}/scheduler_${mode}.log" 2>&1 &
    local lyra_pid=$!
    sleep 10

    if ! ps -p $lyra_pid > /dev/null 2>&1; then
        log_err "调度器启动失败"
        tail -30 "${RESULT_DIR}/scheduler_${mode}.log"
        return 1
    fi

    # 触发节点更新事件（修改 GPU 注解以触发 UpdateNodeAllocatable）
    log_info "触发 GPU 资源变更事件..."
    kc="/tmp/kwok_kwok-cluster-0.kubeconfig"
    # 修改 A100 节点的 GPU 注解（模拟资源变更）
    ssh root@${SERVER1} "kubectl --kubeconfig=$kc annotate node gpu-node-0-0 hami.io/node-nvidia-register='GPU-CHANGED-A100-1,81920,100,100,NVIDIA-A100-80GB:GPU-CHANGED-A100-2,81920,100,100,NVIDIA-A100-80GB:' --overwrite" 2>/dev/null || true
    sleep 3
    kc2="/tmp/kwok_kwok-cluster-1.kubeconfig"
    # 修改 V100 节点的 GPU 注解
    ssh root@${SERVER2} "kubectl --kubeconfig=$kc2 annotate node gpu-node-1-0 hami.io/node-nvidia-register='GPU-CHANGED-V100-1,81920,100,100,NVIDIA-V100-32GB:GPU-CHANGED-V100-2,81920,100,100,NVIDIA-V100-32GB:' --overwrite" 2>/dev/null || true
    sleep 5

    # 提取 [PERF] 日志
    log_info "[PERF] Queue wakeup stats:"
    grep "\[PERF\] Queue wakeup stats" "${RESULT_DIR}/scheduler_${mode}.log"

    # 停止调度器
    log_info "停止调度器 (PID=$lyra_pid)..."
    kill $lyra_pid 2>/dev/null || true
    sleep 3
    if ps -p $lyra_pid > /dev/null 2>&1; then
        log_warn "调度器仍在运行，强制终止..."
        kill -9 $lyra_pid 2>/dev/null || true
        sleep 1
    fi
    log_info "调度器已停止"

    local pending_after=$(kubectl --kubeconfig="$KCFG" get pod -l app=stress-pod --field-selector=status.phase=Pending --no-headers 2>/dev/null | wc -l)
    log_info "${mode} 后: ${pending_after} Pending"

    return 0
}

# =============================================================================
# 主流程
# =============================================================================
START_TIME=$(date +%s)

log_info "编译调度器..."
go build -o lyra.exe .

deploy
annotate_nodes

# 保存配置
cat > "${RESULT_DIR}/config.json" <<EOF
{
  "pod_count": ${POD_COUNT},
  "gpu_capacity": "4 GPU (cluster-0: 2xA100, cluster-1: 2xV100)",
  "pod_distribution": "A100=33, V100=33, AnyGPU=34",
  "expected_schedulable": 4,
  "expected_unschedulable": 96
}
EOF

# 测试
run_test disabled
sleep 15
run_test enabled

# 对比报告
echo ""
echo "========================================"
echo "  QueueingHint E2E 对比结果"
echo "========================================"
echo "GPU容量: 4 (cluster-0: 2xA100, cluster-1: 2xV100)"
echo "Pod分布: 100个 (A100=33, V100=33, Any=34)"
echo ""
echo "=== DISABLED 模式 (全量唤醒) ==="
grep "\[PERF\] Queue wakeup stats" "${RESULT_DIR}/scheduler_disabled.log" || echo "(无数据)"
echo ""
echo "=== ENABLED 模式 (智能过滤) ==="
grep "\[PERF\] Queue wakeup stats" "${RESULT_DIR}/scheduler_enabled.log" || echo "(无数据)"

# 清理
log_info "清理环境..."
bash experiments/shared/scripts/cleanup_fast.sh small 2>/dev/null || true

END_TIME=$(date +%s)
echo ""
echo "========================================"
echo "  测试完成! 耗时: $(( (END_TIME - START_TIME) / 60 )) 分钟"
echo "  结果目录: ${RESULT_DIR}"
echo "========================================"
