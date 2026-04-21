#!/bin/bash
# =============================================================================
# Lyra 调度器性能测试 - 提交Pod
# 用法: ./submit_pods.sh <pod_count> [namespace] [mode]
# mode: serial | concurrent (default: serial)
#
# serial:     串行逐个提交，用于快速验证和回归测试
# concurrent: 真正并发提交，使用 GNU parallel 或后台任务模拟并发
# =============================================================================

set -e

POD_COUNT=${1:-100}
NAMESPACE=${2:-default}
MODE=${3:-serial}
KARMADA_KUBECONFIG="../../kubeconfig/karmada-apiserver.config"
IMAGE="swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/alpine:3.20.2"

echo "=== 提交 ${POD_COUNT} 个Pod到Karmada (${MODE} 模式) ==="
echo "镜像: ${IMAGE}"
START_TIME=$(date +%s)

# Pod YAML 模板
gen_pod() {
    local idx=$1
    cat << EOF
apiVersion: v1
kind: Pod
metadata:
  name: test-pod-${idx}
  namespace: ${NAMESPACE}
  labels:
    app: test-pod
    batch: "perf-test"
spec:
  schedulerName: lyra-scheduler
  containers:
  - name: fake-container
    image: ${IMAGE}
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
}

case ${MODE} in
    serial)
        # 串行提交：逐个 kubectl apply
        for i in $(seq 1 ${POD_COUNT}); do
            gen_pod ${i} | kubectl --kubeconfig="${KARMADA_KUBECONFIG}" apply -f -

            if [ $((i % 50)) -eq 0 ]; then
                echo "  已提交 ${i}/${POD_COUNT}..."
            fi
        done
        ;;

    concurrent)
        # 并发提交：使用后台任务批量提交
        # 计算并发度：每批提交的数量
        BATCH_SIZE=${BATCH_SIZE:-50}
        PID_DIR=$(mktemp -d)

        echo "  使用 ${BATCH_SIZE} 并发度提交..."
        submitted=0
        pids=()

        for i in $(seq 1 ${POD_COUNT}); do
            # 创建临时 YAML 文件
            YAML_FILE="${PID_DIR}/pod-${i}.yaml"
            gen_pod ${i} > "${YAML_FILE}"

            # 后台提交
            kubectl --kubeconfig="${KARMADA_KUBECONFIG}" apply -f "${YAML_FILE}" &
            pids+=($!)

            # 控制并发度
            if [ $((submitted % BATCH_SIZE)) -eq $((BATCH_SIZE - 1)) ]; then
                # 等待这批完成
                for pid in "${pids[@]}"; do
                    wait ${pid} 2>/dev/null || true
                done
                pids=()
                echo "  已提交 $((submitted + 1))/${POD_COUNT}..."
            fi

            submitted=$((submitted + 1))
        done

        # 等待最后一批
        for pid in "${pids[@]}"; do
            wait ${pid} 2>/dev/null || true
        done

        # 清理临时文件
        rm -rf "${PID_DIR}"
        ;;

    *)
        echo "ERROR: Unknown mode '${MODE}'. Use 'serial' or 'concurrent'"
        exit 1
        ;;
esac

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
echo "=== 提交完成: ${POD_COUNT} 个Pod, 耗时 ${ELAPSED}s ==="