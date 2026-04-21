#!/bin/bash
# =============================================================================
# Lyra 调度器性能测试 - 一键部署脚本
# 用法: ./scripts/deploy.sh <scale>
# scale: small(2集群×10节点) | medium(5集群×50节点) | large(10集群×100节点) | extreme(20集群×100节点)
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
        POD_COUNT=50
        ;;
    medium)
        CLUSTERS_PER_SERVER=2
        NODES_PER_CLUSTER=50
        POD_COUNT=200
        ;;
    large)
        CLUSTERS_PER_SERVER=5
        NODES_PER_CLUSTER=100
        POD_COUNT=500
        ;;
    extreme)
        CLUSTERS_PER_SERVER=10
        NODES_PER_CLUSTER=100
        POD_COUNT=1000
        ;;
    *)
        echo "Unknown scale: $SCALE"
        echo "Usage: $0 <small|medium|large|extreme>"
        exit 1
        ;;
esac

TOTAL_CLUSTERS=$((CLUSTERS_PER_SERVER * 2))
TOTAL_NODES=$((TOTAL_CLUSTERS * NODES_PER_CLUSTER))

echo "=========================================="
echo "  Lyra 调度器性能测试环境部署"
echo "=========================================="
echo "测试规模: $SCALE"
echo "集群数: $TOTAL_CLUSTERS (每台服务器 $CLUSTERS_PER_SERVER 个)"
echo "每集群节点数: $NODES_PER_CLUSTER"
echo "总节点数: $TOTAL_NODES"
echo "测试Pod数: $POD_COUNT"
echo "=========================================="

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_err() { echo -e "${RED}[ERROR]${NC} $1"; }

# 生成唯一UUID
generate_uuid() {
    head -c 16 /dev/urandom | xxd -p | head -c 32
}

# 创建KWOK集群
create_kwok_cluster() {
    local server=$1
    local cluster_idx=$2
    local cluster_name="kwok-cluster-${cluster_idx}"
    local port=$((BASE_PORT + cluster_idx))

    log_info "创建集群 $cluster_name (端口: $port)..."

    # 创建集群
    ssh root@$server "kwokctl create cluster --name=$cluster_name --kube-apiserver-port=$port" 2>&1 | grep -E "created|started" || true

    sleep 2
}

# 创建带GPU注解的节点
create_gpu_nodes() {
    local server=$1
    local cluster_idx=$2
    local node_count=$3
    local cluster_name="kwok-cluster-${cluster_idx}"

    log_info "为集群 $cluster_name 创建 ${node_count} 个GPU节点..."

    for n in $(seq 0 $((node_count-1))); do
        local node_name="gpu-node-${cluster_idx}-${n}"

        # 生成GPU注解
        local gpu_annotation=""
        for g in $(seq 0 $((GPUS_PER_NODE-1))); do
            local uuid="GPU-c${cluster_idx}n${n}g${g}-$(ssh root@$server 'head -c 16 /dev/urandom | xxd -p | head -c 32')"
            gpu_annotation="${gpu_annotation}${uuid},100,81920,100,NVIDIA-A100-80GB:"
        done

        # 在远程服务器上创建节点
        ssh root@$server "kubectl --context kwok-$cluster_name apply -f -" << EOF
apiVersion: v1
kind: Node
metadata:
  annotations:
    kwok.x-k8s.io/node: fake
    hami.io/node-nvidia-register: '${gpu_annotation}'
  labels:
    type: kwok
    gpu-node: "true"
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
    cpu: "64"
    memory: 512Gi
    pods: "200"
    nvidia.com/gpu: "${GPUS_PER_NODE}"
    nvidia.com/gpumem: "$((81920 * GPUS_PER_NODE))"
    nvidia.com/gpucores: "$((100 * GPUS_PER_NODE))"
  capacity:
    cpu: "64"
    memory: 512Gi
    pods: "200"
    nvidia.com/gpu: "${GPUS_PER_NODE}"
    nvidia.com/gpumem: "$((81920 * GPUS_PER_NODE))"
    nvidia.com/gpucores: "$((100 * GPUS_PER_NODE))"
  nodeInfo:
    architecture: amd64
    kubeletVersion: v1.28.0
    operatingSystem: linux
  phase: Running
EOF

        if [ $((n % 20)) -eq 0 ] && [ $n -gt 0 ]; then
            log_info "  已创建 $n/$node_count 个节点"
        fi
    done

    local created=$(ssh root@$server "kubectl --context kwok-$cluster_name get nodes | grep -c gpu-node-${cluster_idx}-" || echo 0)
    log_info "  集群 $cluster_name 创建了 $created 个节点"
}

# 将集群加入Karmada（创建Cluster资源和Secret）
join_karmada() {
    local server=$1
    local cluster_idx=$2
    local cluster_name="kwok-cluster-${cluster_idx}"
    local port=$((BASE_PORT + cluster_idx))

    log_info "将集群 $cluster_name 加入Karmada..."

    # 1. 导出kubeconfig并修改server地址
    local kubeconfig="/tmp/${cluster_name}.kubeconfig"
    ssh root@$server "kwokctl get kubeconfig --name=$cluster_name" | sed "s/127.0.0.1/$server/g" > "$kubeconfig"

    # 2. 从kubeconfig提取CA证书
    local ca_bundle=$(grep "certificate-authority-data:" "$kubeconfig" | sed 's/.*certificate-authority-data: //')

    # 3. 在KWOK集群中创建ServiceAccount和RBAC
    ssh root@$server "kubectl --context kwok-$cluster_name apply -f -" << EOF
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

    # 4. 生成长期Token (有效期1年)
    local token=$(ssh root@$server "kubectl --context kwok-$cluster_name create token karmada-controller -n kube-system --duration=8760h")

    # 5. 创建Secret (名称必须与Cluster名称一致)
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
EOF

    # 6. 创建Cluster资源
    kubectl --kubeconfig="$KARMADA_KUBECONFIG" apply -f - << EOF
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

    # 验证
    sleep 2
    if kubectl --kubeconfig="$KARMADA_KUBECONFIG" get cluster "$cluster_name" 2>/dev/null; then
        log_info "  集群 $cluster_name 已加入Karmada"
    else
        log_warn "  集群 $cluster_name 加入Karmada失败"
    fi

    rm -f "$kubeconfig"
}

# =============================================================================
# 主流程
# =============================================================================

log_info "Step 1: 创建 KWOK 集群..."
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    create_kwok_cluster "$SERVER1" $i
done
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    idx=$((CLUSTERS_PER_SERVER + i))
    create_kwok_cluster "$SERVER2" $idx
done

log_info "Step 2: 创建 GPU 节点..."
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    create_gpu_nodes "$SERVER1" $i $NODES_PER_CLUSTER
done
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    idx=$((CLUSTERS_PER_SERVER + i))
    create_gpu_nodes "$SERVER2" $idx $NODES_PER_CLUSTER
done

log_info "Step 3: 将集群加入 Karmada..."
for i in $(seq 0 $((TOTAL_CLUSTERS-1))); do
    if [ $i -lt $CLUSTERS_PER_SERVER ]; then
        join_karmada "$SERVER1" $i
    else
        join_karmada "$SERVER2" $i
    fi
done

# 验证环境
log_info "=========================================="
log_info "  环境部署完成 - 验证结果"
log_info "=========================================="

echo ""
log_info "Karmada 集群列表:"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get clusters

echo ""
log_info "总节点统计:"
echo "  预期: $TOTAL_NODES 个节点 (来自 $TOTAL_CLUSTERS 个集群)"

echo ""
log_info "=========================================="
log_info "  部署完成!"
log_info "=========================================="
echo "下一步:"
echo "  1. 启动 Lyra 调度器: go run ."
echo "  2. 运行测试: ./scripts/submit_pods.sh $POD_COUNT"
echo "  3. 清理环境: ./scripts/cleanup.sh $SCALE"
echo ""