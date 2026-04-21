#!/bin/bash
# =============================================================================
# Lyra 调度器性能测试 - 快速部署脚本 (优化版)
# 用法: bash deploy_fast.sh <scale>
# 优化: 批量生成YAML + 并行创建 + 独立kubeconfig
# =============================================================================

set -e

cd "$(dirname "$0")/.."

KARMADA_KUBECONFIG="../../kubeconfig/karmada-apiserver.config"
SERVER1="10.10.100.5"
SERVER2="10.10.100.9"
BASE_PORT=32100
GPUS_PER_NODE=4

# 测试规模配置
SCALE=${1:-small}
case $SCALE in
    small)
        CLUSTERS_PER_SERVER=1
        NODES_PER_CLUSTER=10
        ;;
    medium)
        CLUSTERS_PER_SERVER=2
        NODES_PER_CLUSTER=50
        ;;
    large)
        CLUSTERS_PER_SERVER=5
        NODES_PER_CLUSTER=100
        ;;
    extreme)
        CLUSTERS_PER_SERVER=10
        NODES_PER_CLUSTER=100
        ;;
    *)
        echo "Unknown scale: $SCALE"
        exit 1
        ;;
esac

TOTAL_CLUSTERS=$((CLUSTERS_PER_SERVER * 2))
TOTAL_NODES=$((TOTAL_CLUSTERS * NODES_PER_CLUSTER))

echo "=========================================="
echo "  Lyra 快速部署 (优化版)"
echo "=========================================="
echo "规模: $SCALE | 集群: $TOTAL_CLUSTERS | 节点: $TOTAL_NODES"
echo "=========================================="

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }

# 生成GPU注解 (本地生成, 不SSH)
generate_gpu_annotation() {
    local cluster_idx=$1
    local node_idx=$2
    local result=""
    for g in $(seq 0 $((GPUS_PER_NODE-1))); do
        local uuid="GPU-c${cluster_idx}n${node_idx}g${g}-$(head -c 16 /dev/urandom | xxd -p | head -c 32)"
        result="${result}${uuid},100,81920,100,NVIDIA-A100-80GB:"
    done
    echo "$result"
}

# 批量生成节点YAML
generate_nodes_yaml() {
    local cluster_idx=$1
    local node_count=$2
    local yaml=""

    for n in $(seq 0 $((node_count-1))); do
        local node_name="gpu-node-${cluster_idx}-${n}"
        local gpu_annotation=$(generate_gpu_annotation $cluster_idx $n)

        yaml="${yaml}
---
apiVersion: v1
kind: Node
metadata:
  annotations:
    kwok.x-k8s.io/node: fake
    hami.io/node-nvidia-register: '${gpu_annotation}'
  labels:
    type: kwok
    gpu-node: \"true\"
    cluster: kwok-cluster-${cluster_idx}
    kubernetes.io/arch: amd64
    kubernetes.io/os: linux
  name: ${node_name}
spec:
  taints:
  - effect: NoSchedule
    key: kwok.x-k8s.io/node
    value: fake
status:
  allocatable:
    cpu: \"64\"
    memory: 512Gi
    pods: \"200\"
    nvidia.com/gpu: \"${GPUS_PER_NODE}\"
    nvidia.com/gpumem: \"$((81920 * GPUS_PER_NODE))\"
    nvidia.com/gpucores: \"$((100 * GPUS_PER_NODE))\"
  capacity:
    cpu: \"64\"
    memory: 512Gi
    pods: \"200\"
    nvidia.com/gpu: \"${GPUS_PER_NODE}\"
    nvidia.com/gpumem: \"$((81920 * GPUS_PER_NODE))\"
    nvidia.com/gpucores: \"$((100 * GPUS_PER_NODE))\"
  nodeInfo:
    architecture: amd64
    kubeletVersion: v1.28.0
    operatingSystem: linux
  phase: Running
"
    done
    echo "$yaml"
}

# 并行创建集群 (后台运行)
create_kwok_cluster_async() {
    local server=$1
    local cluster_idx=$2
    local port=$((BASE_PORT + cluster_idx))
    local cluster_name="kwok-cluster-${cluster_idx}"

    ssh root@$server "kwokctl create cluster --name=$cluster_name --kube-apiserver-port=$port" 2>&1 | grep -E "created|started" || true
}

# 批量创建节点 (一次性apply)
create_gpu_nodes_batch() {
    local server=$1
    local cluster_idx=$2
    local node_count=$3
    local cluster_name="kwok-cluster-${cluster_idx}"

    log_info "集群 $cluster_name: 批量创建 $node_count 节点..."

    # 本地生成YAML
    local yaml_file="/tmp/nodes_${cluster_idx}.yaml"
    generate_nodes_yaml $cluster_idx $node_count > "$yaml_file"

    # 使用临时 kubeconfig，避免 context 合并问题
    cat "$yaml_file" | ssh root@$server "KUBECONFIG=/tmp/kwok_${cluster_name}.kubeconfig kwokctl get kubeconfig --name=$cluster_name > /tmp/kwok_${cluster_name}.kubeconfig && kubectl --kubeconfig=/tmp/kwok_${cluster_name}.kubeconfig apply -f -"

    rm -f "$yaml_file"
}

# 快速加入Karmada
join_karmada_fast() {
    local server=$1
    local cluster_idx=$2
    local cluster_name="kwok-cluster-${cluster_idx}"
    local port=$((BASE_PORT + cluster_idx))
    local kc="/tmp/kwok_${cluster_name}.kubeconfig"

    # 导出kubeconfig到临时文件
    ssh root@$server "kwokctl get kubeconfig --name=$cluster_name > $kc"

    # 提取CA
    local ca_bundle=$(ssh root@$server "grep certificate-authority-data: $kc | sed 's/.*certificate-authority-data: //'")

    # 创建SA和RBAC
    ssh root@$server "kubectl --kubeconfig=$kc apply -f -" << 'EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: karmada-controller
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: karmada-controller-admin
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
- kind: ServiceAccount
  name: karmada-controller
  namespace: kube-system
EOF

    # 生成Token
    local token=$(ssh root@$server "kubectl --kubeconfig=$kc create token karmada-controller -n kube-system --duration=8760h")

    # 创建Secret和Cluster
    kubectl --kubeconfig="$KARMADA_KUBECONFIG" apply -f - << EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${cluster_name}
  namespace: karmada-cluster
  labels:
    karmada.io/system: "true"
type: Opaque
data:
  caBundle: ${ca_bundle}
  token: $(echo -n "$token" | base64 -w0)
---
apiVersion: cluster.karmada.io/v1alpha1
kind: Cluster
metadata:
  name: ${cluster_name}
spec:
  apiEndpoint: https://${server}:${port}
  syncMode: Push
  secretRef:
    name: ${cluster_name}
    namespace: karmada-cluster
  insecureSkipTLSVerification: true
EOF
}

# =============================================================================
# 主流程 - 并行化
# =============================================================================

START_TIME=$(date +%s)

# Step 1: 并行创建所有KWOK集群
log_info "Step 1: 并行创建 $TOTAL_CLUSTERS 个 KWOK 集群..."
pids=()
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    create_kwok_cluster_async "$SERVER1" $i &
    pids+=($!)
done
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    idx=$((CLUSTERS_PER_SERVER + i))
    create_kwok_cluster_async "$SERVER2" $idx &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait $pid; done

# 等待所有集群就绪 (检查 kubeconfig 是否可用)
log_info "等待所有集群就绪..."
for i in $(seq 0 $((TOTAL_CLUSTERS-1))); do
    if [ $i -lt $CLUSTERS_PER_SERVER ]; then
        server=$SERVER1
    else
        server=$SERVER2
    fi
    cluster_name="kwok-cluster-${i}"
    for attempt in $(seq 1 10); do
        if ssh root@$server "kwokctl get kubeconfig --name=$cluster_name" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
done

# Step 2: 批量创建节点 (每台服务器并行)
log_info "Step 2: 批量创建节点..."
pids=()
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    create_gpu_nodes_batch "$SERVER1" $i $NODES_PER_CLUSTER &
    pids+=($!)
done
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    idx=$((CLUSTERS_PER_SERVER + i))
    create_gpu_nodes_batch "$SERVER2" $idx $NODES_PER_CLUSTER &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait $pid; done

# Step 3: 并行加入Karmada
log_info "Step 3: 并行加入 Karmada..."
pids=()
for i in $(seq 0 $((TOTAL_CLUSTERS-1))); do
    if [ $i -lt $CLUSTERS_PER_SERVER ]; then
        join_karmada_fast "$SERVER1" $i &
    else
        join_karmada_fast "$SERVER2" $i &
    fi
    pids+=($!)
done
for pid in "${pids[@]}"; do wait $pid; done

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

log_info "=========================================="
log_info "  部署完成! 耗时: ${ELAPSED} 秒"
log_info "=========================================="
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get clusters
echo ""
echo "下一步: 启动调度器并运行测试"