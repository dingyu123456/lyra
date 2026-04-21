#!/bin/bash
# =============================================================================
# 事件模拟器基准测试
# 用法: bash benchmark_events.sh
# 目的: 测试 v2 脚本在不同目标速率下的实际事件速率
# =============================================================================

cd "$(dirname "$0")"

SERVER1="10.10.100.5"
SERVER2="10.10.100.9"
KUBECONFIG1="/tmp/kwok-cluster-0.kubeconfig"
KUBECONFIG2="/tmp/kwok-cluster-1.kubeconfig"

echo "=========================================="
echo "  事件模拟器基准测试"
echo "=========================================="
echo "测试时长: 10 秒"
echo "目标速率: 100, 500, 1000, 2000"
echo "=========================================="

# 检查集群是否存在
echo "检查 KWOK 集群..."
if ! ssh root@$SERVER1 "kwokctl get clusters" 2>/dev/null | grep -q "kwok-cluster-0"; then
    echo "错误: kwok-cluster-0 不存在，请先部署测试环境"
    echo "运行: bash deploy_fast.sh small"
    exit 1
fi
if ! ssh root@$SERVER2 "kwokctl get clusters" 2>/dev/null | grep -q "kwok-cluster-1"; then
    echo "错误: kwok-cluster-1 不存在，请先部署测试环境"
    echo "运行: bash deploy_fast.sh small"
    exit 1
fi

# 传输脚本到远程服务器
echo "传输脚本到远程服务器..."
scp simulate_events_v2.sh root@$SERVER1:/tmp/ 2>/dev/null
scp simulate_events_v2.sh root@$SERVER2:/tmp/ 2>/dev/null

# 获取 kubeconfig
echo "获取 kubeconfig..."
ssh root@$SERVER1 "kwokctl export kubeconfig --name kwok-cluster-0" > /tmp/kwok-cluster-0.kubeconfig 2>/dev/null
ssh root@$SERVER2 "kwokctl export kubeconfig --name kwok-cluster-1" > /tmp/kwok-cluster-1.kubeconfig 2>/dev/null

# 测试函数
test_rate() {
    local rate=$1
    local duration=10
    local nodes_per_cluster=10

    echo ""
    echo "----------------------------------------"
    echo "测试目标速率: ${rate} events/s"
    echo "----------------------------------------"

    # 启动两个服务器的模拟（每个服务器处理一半事件）
    START=$(date +%s)
    ssh root@$SERVER1 "bash /tmp/simulate_events_v2.sh ${rate} ${duration} ${nodes_per_cluster} kwok-cluster-0" 2>&1 &
    PID1=$!
    ssh root@$SERVER2 "bash /tmp/simulate_events_v2.sh ${rate} ${duration} ${nodes_per_cluster} kwok-cluster-1" 2>&1 &
    PID2=$!

    wait $PID1 $PID2
    END=$(date +%s)

    echo "测试完成，耗时: $((END - START))s"
}

# 运行不同速率的测试
for rate in 100 500 1000 2000; do
    test_rate $rate
done

echo ""
echo "=========================================="
echo "  基准测试完成"
echo "=========================================="
echo "查看上方输出中的 '实际平均速率' 了解各目标速率下的真实吞吐"
echo "=========================================="
