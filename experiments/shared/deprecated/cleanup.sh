#!/bin/bash
# =============================================================================
# Lyra 调度器性能测试 - 清理脚本
# 用法: ./cleanup.sh <scale>
# =============================================================================

cd "$(dirname "$0")/.."

KARMADA_KUBECONFIG="../../kubeconfig/karmada-apiserver.config"
SERVER1="10.10.100.5"
SERVER2="10.10.100.9"

# 测试规模配置
SCALE=${1:-small}
case $SCALE in
    small)
        CLUSTERS_PER_SERVER=1
        ;;
    medium)
        CLUSTERS_PER_SERVER=2
        ;;
    large)
        CLUSTERS_PER_SERVER=5
        ;;
    extreme)
        CLUSTERS_PER_SERVER=10
        ;;
    *)
        echo "Unknown scale: $SCALE"
        echo "Usage: $0 <small|medium|large|extreme>"
        exit 1
        ;;
esac

TOTAL_CLUSTERS=$((CLUSTERS_PER_SERVER * 2))

echo "=========================================="
echo "  清理 Lyra 性能测试环境"
echo "=========================================="
echo "清理集群数: $TOTAL_CLUSTERS"
echo "=========================================="

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }

# Step 1: 删除服务器上的KWOK集群 (先执行，避免残留)
log_info "Step 1: 删除服务器上的KWOK集群..."
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    cluster_name="kwok-cluster-${i}"
    echo "  删除 $cluster_name (服务器1)..."
    ssh root@$SERVER1 "kwokctl delete cluster --name=$cluster_name" 2>/dev/null || true
done
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    idx=$((CLUSTERS_PER_SERVER + i))
    cluster_name="kwok-cluster-${idx}"
    echo "  删除 $cluster_name (服务器2)..."
    ssh root@$SERVER2 "kwokctl delete cluster --name=$cluster_name" 2>/dev/null || true
done

# Step 2: 从Karmada移除所有集群 (使用 --grace-period=0 强制删除)
log_info "Step 2: 从Karmada移除集群..."
for i in $(seq 0 $((TOTAL_CLUSTERS-1))); do
    cluster_name="kwok-cluster-${i}"
    echo "  移除 $cluster_name..."
    kubectl --kubeconfig="$KARMADA_KUBECONFIG" delete cluster "$cluster_name" --ignore-not-found --grace-period=0 2>/dev/null || true
    kubectl --kubeconfig="$KARMADA_KUBECONFIG" delete secret "$cluster_name" -n karmada-cluster --ignore-not-found 2>/dev/null || true
done

# 等待集群真正删除
sleep 2

# Step 3: 清理所有测试相关的PP和OP
log_info "Step 3: 清理所有测试PP和OP..."
# 获取所有 namespace 的 PP 和 OP
for ns in $(kubectl --kubeconfig="$KARMADA_KUBECONFIG" get ns -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    # 删除所有 lyra-*, perf-*, test-* 开头的 PP
    for name in $(kubectl --kubeconfig="$KARMADA_KUBECONFIG" get pp -n "$ns" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | tr ' ' '\n' | grep -E '^(lyra-|perf-|test-|wakeup-)'); do
        echo "  删除 PP $ns/$name..."
        kubectl --kubeconfig="$KARMADA_KUBECONFIG" delete pp "$name" -n "$ns" --ignore-not-found 2>/dev/null || true
    done
    # 删除所有 lyra-*, perf-*, test-* 开头的 OP
    for name in $(kubectl --kubeconfig="$KARMADA_KUBECONFIG" get op -n "$ns" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | tr ' ' '\n' | grep -E '^(lyra-|perf-|test-|wakeup-)'); do
        echo "  删除 OP $ns/$name..."
        kubectl --kubeconfig="$KARMADA_KUBECONFIG" delete op "$name" -n "$ns" --ignore-not-found 2>/dev/null || true
    done
done

# Step 4: 清理所有测试Pod
log_info "Step 4: 清理所有测试Pod..."
for ns in $(kubectl --kubeconfig="$KARMADA_KUBECONFIG" get ns -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    # 删除所有 test-*, perf-*, wakeup-* 开头的 Pod
    for name in $(kubectl --kubeconfig="$KARMADA_KUBECONFIG" get pods -n "$ns" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | tr ' ' '\n' | grep -E '^(test-|perf-|wakeup-|lyra-)'); do
        echo "  删除 Pod $ns/$name..."
        kubectl --kubeconfig="$KARMADA_KUBECONFIG" delete pod "$name" -n "$ns" --ignore-not-found 2>/dev/null || true
    done
done

# Step 5: 清理本地临时文件
log_info "Step 5: 清理临时文件..."
rm -f /tmp/kwok-cluster-*.kubeconfig 2>/dev/null || true

# Step 6: 验证清理结果
log_info "Step 6: 验证清理结果..."
echo ""
echo "--- 剩余集群 ---"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get cluster 2>/dev/null || echo "无"
echo ""
echo "--- 剩余PP ---"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get pp --all-namespaces 2>/dev/null | grep -E '^(NAMESPACE|lyra-|perf-|test-|wakeup-)' || echo "无"
echo ""
echo "--- 剩余OP ---"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get op --all-namespaces 2>/dev/null | grep -E '^(NAMESPACE|lyra-|perf-|test-|wakeup-)' || echo "无"

echo ""
echo "=========================================="
echo "  清理完成!"
echo "=========================================="