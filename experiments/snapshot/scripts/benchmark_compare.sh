#!/bin/bash
# 快速对比 v2 vs v3 的实际吞吐量

echo "=========================================="
echo "  v2 vs v3 事件模拟器对比测试"
echo "=========================================="

# 测试 v2 (kubectl + xargs)
echo ""
echo "[测试 v2] kubectl + xargs 方式 (目标: 2000 events/s)"
ssh root@10.10.100.5 "bash /tmp/simulate_events_v2.sh 2000 10 10 kwok-cluster-0" 2>&1
echo "---"

# 测试 v3 (curl 直连)
echo ""
echo "[测试 v3] curl 直连 API 方式 (目标: 2000 events/s)"
ssh root@10.10.100.5 "bash /tmp/simulate_events_v3.sh 2000 10 10 kwok-cluster-0" 2>&1
echo "---"

echo ""
echo "=========================================="
echo "  对比完成"
echo "=========================================="
