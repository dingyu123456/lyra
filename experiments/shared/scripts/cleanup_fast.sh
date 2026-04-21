#!/bin/bash
# =============================================================================
# Lyra 调度器性能测试 - 快速清理脚本 (优化版)
# 用法: ./cleanup_fast.sh <scale>
# 优化: 并行删除 + xargs批量处理 + 强制删除
# =============================================================================

set -e

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
        echo "Usage: $0 <small|medium|large|extreme>"
        exit 1
        ;;
esac

TOTAL_CLUSTERS=$((CLUSTERS_PER_SERVER * 2))

echo "=========================================="
echo "  Lyra 快速清理 (优化版)"
echo "=========================================="
echo "集群数: $TOTAL_CLUSTERS"
echo "=========================================="

GREEN='\033[0;32m'
NC='\033[0m'
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }

START_TIME=$(date +%s)

# Step 1: 批量删除 KWOK 集群 (并行)
log_info "Step 1: 并行删除 KWOK 集群..."
pids=()
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    ssh root@$SERVER1 "kwokctl delete cluster --name=kwok-cluster-${i}" 2>/dev/null &
    pids+=($!)
done
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    idx=$((CLUSTERS_PER_SERVER + i))
    ssh root@$SERVER2 "kwokctl delete cluster --name=kwok-cluster-${idx}" 2>/dev/null &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait $pid; done

# Step 2: 并行从 Karmada 移除集群
log_info "Step 2: 并行移除 Karmada 集群..."
pids=()
for i in $(seq 0 $((TOTAL_CLUSTERS-1))); do
    kubectl --kubeconfig="$KARMADA_KUBECONFIG" delete cluster "kwok-cluster-${i}" --ignore-not-found --grace-period=0 2>/dev/null &
    kubectl --kubeconfig="$KARMADA_KUBECONFIG" delete secret "kwok-cluster-${i}" -n karmada-cluster --ignore-not-found 2>/dev/null &
done
wait

# Step 3: 批量清理 PP/OP/Pod (分批并发删除，加速 PP 删除)
log_info "Step 3: 批量清理 PP/OP/Pod..."
PIDS=""
KCFG="$KARMADA_KUBECONFIG"

# Pod 强制删
kubectl --kubeconfig="$KCFG" delete pod -l app=stress-pod --ignore-not-found --force --grace-period=0 --wait=false 2>/dev/null &
PIDS="$PIDS $!"
kubectl --kubeconfig="$KCFG" delete pod -l app=test-pod --ignore-not-found --force --grace-period=0 --wait=false 2>/dev/null &
PIDS="$PIDS $!"

# OP 并发删（每行输出: namespace name）
OP_LIST=$(kubectl --kubeconfig="$KCFG" get op -A -l app=stress-pod -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' 2>/dev/null)
if [ -n "$OP_LIST" ]; then
    echo "$OP_LIST" | xargs -P 20 -L 1 bash -c 'kubectl --kubeconfig="'"$KCFG"'" delete op -n "$0" "$1" --ignore-not-found --wait=false 2>/dev/null' &
    PIDS="$PIDS $!"
fi

# PP 并发删
PP_LIST=$(kubectl --kubeconfig="$KCFG" get pp -A -l app=stress-pod -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' 2>/dev/null)
if [ -n "$PP_LIST" ]; then
    echo "$PP_LIST" | xargs -P 20 -L 1 bash -c 'kubectl --kubeconfig="'"$KCFG"'" delete pp -n "$0" "$1" --ignore-not-found --wait=false 2>/dev/null' &
    PIDS="$PIDS $!"
fi

# 等待所有后台进程完成
for pid in $PIDS; do wait $pid 2>/dev/null; done

# Step 5: 清理临时文件
log_info "Step 5: 清理临时文件..."
rm -f /tmp/kwok-cluster-*.kubeconfig 2>/dev/null || true
rm -f /tmp/nodes_*.yaml 2>/dev/null || true
# 清理远程服务器上的临时 kubeconfig
ssh root@$SERVER1 "rm -f /tmp/kwok_*.kubeconfig /tmp/kwok-cluster-*.kubeconfig" 2>/dev/null || true
ssh root@$SERVER2 "rm -f /tmp/kwok_*.kubeconfig /tmp/kwok-cluster-*.kubeconfig" 2>/dev/null || true

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

log_info "=========================================="
log_info "  清理完成! 耗时: ${ELAPSED} 秒"
log_info "=========================================="
echo ""
echo "--- 剩余集群 ---"
kubectl --kubeconfig="$KARMADA_KUBECONFIG" get cluster 2>/dev/null || echo "无"