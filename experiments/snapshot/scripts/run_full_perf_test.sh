#!/bin/bash
# =============================================================================
# Lyra 调度器性能测试 - 全面压力测试
# 用法: bash run_full_perf_test.sh <scale> <pod_rate> <duration_min> <event_rate>
# scale: small | medium | large | extreme
# pod_rate: 每秒提交的 pod 数量 (默认: 100)
# duration_min: 持续时间分钟数 (默认: 5)
# event_rate: 每秒最大背景事件数 (默认: 50，随机 1~event_rate)
#
# 示例: bash run_full_perf_test.sh large 100 10 80
#       # 大规模，每秒100个pod，持续10分钟，每秒最多80个背景事件
# =============================================================================

set -e

cd "$(dirname "$0")/.."

SCALE=${1:-medium}
POD_RATE=${2:-100}
DURATION_MIN=${3:-5}
USER_EVENT_RATE=${4:-}  # 用户可指定，为空则根据规模自动设置
DURATION_SEC=$((DURATION_MIN * 60))
DATE=$(date +%Y%m%d_%H%M%S)

# 服务器配置
SERVER1="10.10.100.5"
SERVER2="10.10.100.9"

# 根据 scale 调整集群配置和默认事件速率
case $SCALE in
    small)
        CLUSTERS=2
        NODES_PER_CLUSTER=10
        CLUSTERS_PER_SERVER=1
        DEFAULT_EVENT_RATE=10
        ;;
    medium)
        CLUSTERS=4
        NODES_PER_CLUSTER=50
        CLUSTERS_PER_SERVER=2
        DEFAULT_EVENT_RATE=50
        ;;
    large)
        CLUSTERS=10
        NODES_PER_CLUSTER=100
        CLUSTERS_PER_SERVER=5
        DEFAULT_EVENT_RATE=200
        ;;
    extreme)
        CLUSTERS=20
        NODES_PER_CLUSTER=100
        CLUSTERS_PER_SERVER=10
        DEFAULT_EVENT_RATE=1000
        ;;
    *)
        CLUSTERS=2
        NODES_PER_CLUSTER=10
        CLUSTERS_PER_SERVER=1
        DEFAULT_EVENT_RATE=10
        ;;
esac

# 事件速率：用户指定优先，否则用规模默认值
if [ -n "${USER_EVENT_RATE}" ]; then
    EVENT_RATE=${USER_EVENT_RATE}
else
    EVENT_RATE=${DEFAULT_EVENT_RATE}
fi

TOTAL_NODES=$((CLUSTERS * NODES_PER_CLUSTER))
EXPECTED_PODS=$((POD_RATE * DURATION_SEC))

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_err() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "=========================================="
echo "  Lyra 调度器全面压力测试"
echo "=========================================="
echo "规模: ${SCALE} (${CLUSTERS}集群 x ${NODES_PER_CLUSTER}节点 = ${TOTAL_NODES}节点)"
echo "Pod提交速率: ${POD_RATE} pods/秒"
echo "背景事件速率: ${EVENT_RATE} events/秒 (随机 1~${EVENT_RATE})"
echo "持续时间: ${DURATION_MIN} 分钟 (${DURATION_SEC} 秒)"
echo "预计 Pod 数: ${EXPECTED_PODS}"
echo "=========================================="

# =============================================================================
# 函数：启停背景事件模拟
# =============================================================================
start_event_sim() {
    log_info "启动背景事件模拟 (速率: ${EVENT_RATE} events/s)..."

    # 构建 server1 和 server2 的集群列表
    S1_CLUSTERS=""
    S2_CLUSTERS=""
    for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
        S1_CLUSTERS="${S1_CLUSTERS} kwok-cluster-${i}"
    done
    for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
        idx=$((CLUSTERS_PER_SERVER + i))
        S2_CLUSTERS="${S2_CLUSTERS} kwok-cluster-${idx}"
    done

    # 传脚本到服务器并后台启动
    scp ../../shared/../../shared/scripts/simulate_events_remote.sh root@${SERVER1}:/tmp/simulate_events.sh 2>/dev/null
    scp ../../shared/../../shared/scripts/simulate_events_remote.sh root@${SERVER2}:/tmp/simulate_events.sh 2>/dev/null

    # 事件模拟器无时间限制，由 stop_event_sim 统一控制生命周期
    ssh root@${SERVER1} "bash /tmp/simulate_events.sh ${EVENT_RATE} ${NODES_PER_CLUSTER} ${S1_CLUSTERS}" > /dev/null 2>&1 &
    SIM_PID1=$!
    ssh root@${SERVER2} "bash /tmp/simulate_events.sh ${EVENT_RATE} ${NODES_PER_CLUSTER} ${S2_CLUSTERS}" > /dev/null 2>&1 &
    SIM_PID2=$!

    log_info "背景事件模拟已启动 (PID: ${SIM_PID1}, ${SIM_PID2})"
}

stop_event_sim() {
    local result_dir=${1:-"."}
    log_info "停止背景事件模拟..."
    kill ${SIM_PID1} ${SIM_PID2} 2>/dev/null || true
    ssh root@${SERVER1} "pkill -f simulate_events" 2>/dev/null || true
    ssh root@${SERVER2} "pkill -f simulate_events" 2>/dev/null || true
    sleep 1

    # 收集远程事件统计文件
    log_info "收集背景事件统计..."
    mkdir -p "${result_dir}/event_stats"
    scp root@${SERVER1}:/tmp/event_stats_*.log "${result_dir}/event_stats/server1.csv" 2>/dev/null || true
    scp root@${SERVER2}:/tmp/event_stats_*.log "${result_dir}/event_stats/server2.csv" 2>/dev/null || true
    ssh root@${SERVER1} "rm -f /tmp/event_stats_*.log" 2>/dev/null || true
    ssh root@${SERVER2} "rm -f /tmp/event_stats_*.log" 2>/dev/null || true

    # 打印汇总
    local s1_avg="0" s2_avg="0"
    if [ -f "${result_dir}/event_stats/server1.csv" ]; then
        s1_avg=$(tail -n +2 "${result_dir}/event_stats/server1.csv" | awk -F',' '{sum+=$2;n++} END {printf "%.1f", sum/n}')
    fi
    if [ -f "${result_dir}/event_stats/server2.csv" ]; then
        s2_avg=$(tail -n +2 "${result_dir}/event_stats/server2.csv" | awk -F',' '{sum+=$2;n++} END {printf "%.1f", sum/n}')
    fi
    log_info "平均事件速率: server1=${s1_avg} events/s, server2=${s2_avg} events/s, 合计=$(awk "BEGIN{printf \"%.1f\", ${s1_avg}+${s2_avg}}") events/s"
}

# =============================================================================
# 函数：运行单次测试
# =============================================================================
run_test() {
    local mode=$1  # incremental 或 full
    local result_dir=$2

    log_info "====== 开始 ${mode} 模式测试 ======"

    # 设置快照模式
    if [ "${mode}" = "full" ]; then
        export LYRA_SNAPSHOT_MODE=full
    else
        unset LYRA_SNAPSHOT_MODE
    fi

    # 并行清理 Pod/PP/OP，等待真正删除完成
    log_info "并发清理旧 Pod/PP/OP..."
    KCFG="D:/Users/29197/Documents/project/Golang/lyra-claude/lyra/kubeconfig/karmada-apiserver.config"
    # Pod 强制删
    kubectl --kubeconfig="$KCFG" delete pod -l app=stress-pod --ignore-not-found --force --grace-period=0 --wait=false 2>/dev/null
    # OP 并发删
    OP_LIST=$(kubectl --kubeconfig="$KCFG" get op -A -l app=stress-pod -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' 2>/dev/null)
    if [ -n "$OP_LIST" ]; then
        echo "$OP_LIST" | xargs -P 20 -L 1 bash -c 'kubectl --kubeconfig="D:/Users/29197/Documents/project/Golang/lyra-claude/lyra/kubeconfig/karmada-apiserver.config" delete op -n "$0" "$1" --ignore-not-found --wait=false 2>/dev/null'
    fi
    # PP 并发删
    PP_LIST=$(kubectl --kubeconfig="$KCFG" get pp -A -l app=stress-pod -o jsonpath='{range .items[*]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' 2>/dev/null)
    if [ -n "$PP_LIST" ]; then
        echo "$PP_LIST" | xargs -P 20 -L 1 bash -c 'kubectl --kubeconfig="D:/Users/29197/Documents/project/Golang/lyra-claude/lyra/kubeconfig/karmada-apiserver.config" delete pp -n "$0" "$1" --ignore-not-found --wait=false 2>/dev/null'
    fi
    # 根据规模设置等待超时（PP finalizer 清理很慢，每分钟约 150-200 个）
    # small: 2min, medium: 10min, large: 30min, extreme: 60min
    case $SCALE in
        small)   WAIT_MAX=120 ;;
        medium)  WAIT_MAX=600 ;;
        large)   WAIT_MAX=1800 ;;
        extreme) WAIT_MAX=3600 ;;
        *)       WAIT_MAX=120 ;;
    esac
    log_info "等待资源删除完成 (最长 ${WAIT_MAX}s)..."
    WAIT_START=$(date +%s)
    while true; do
        PP_COUNT=$(kubectl --kubeconfig="$KCFG" get pp -A -l app=stress-pod --no-headers 2>/dev/null | wc -l)
        OP_COUNT=$(kubectl --kubeconfig="$KCFG" get op -A -l app=stress-pod --no-headers 2>/dev/null | wc -l)
        POD_COUNT=$(kubectl --kubeconfig="$KCFG" get pod -l app=stress-pod --no-headers 2>/dev/null | wc -l)
        if [ "$PP_COUNT" -eq 0 ] && [ "$OP_COUNT" -eq 0 ] && [ "$POD_COUNT" -eq 0 ]; then
            WAIT_ELAPSED=$(($(date +%s) - WAIT_START))
            log_info "资源清理完成 (${WAIT_ELAPSED}s)"
            break
        fi
        WAIT_ELAPSED=$(($(date +%s) - WAIT_START))
        if [ $WAIT_ELAPSED -ge $WAIT_MAX ]; then
            log_warn "清理超时 (${WAIT_MAX}s)，仍有 PP=$PP_COUNT OP=$OP_COUNT Pod=$POD_COUNT"
            log_warn "建议：手动等待清理完成后再继续，否则全量实验数据会被污染"
            break
        fi
        # 每 30 秒输出一次进度
        if [ $((WAIT_ELAPSED % 30)) -eq 0 ]; then
            log_info "等待中... PP=$PP_COUNT OP=$OP_COUNT Pod=$POD_COUNT (${WAIT_ELAPSED}s/${WAIT_MAX}s)"
        fi
        sleep 5
    done

    # 验证 KWOK 集群是否仍在 Karmada 中（增量测试后可能被 Karmada 控制器移除）
    if [ "${mode}" = "full" ]; then
        log_info "验证 KWOK 集群注册状态..."
        MISSING_CLUSTERS=""
        for i in $(seq 0 $((CLUSTERS-1))); do
            if ! kubectl --kubeconfig="$KCFG" get cluster "kwok-cluster-${i}" &>/dev/null; then
                MISSING_CLUSTERS="${MISSING_CLUSTERS} ${i}"
                log_warn "kwok-cluster-${i} 已从 Karmada 消失"
            fi
        done

        if [ -n "$MISSING_CLUSTERS" ]; then
            log_warn "检测到缺失集群，重新注册: $MISSING_CLUSTERS"
            SERVER1="10.10.100.5"
            SERVER2="10.10.100.9"
            BASE_PORT=32000

            # 构建服务器1和服务器2的集群列表
            S1_CLUSTERS=""
            S2_CLUSTERS=""
            for i in $MISSING_CLUSTERS; do
                if [ $i -lt $CLUSTERS_PER_SERVER ]; then
                    S1_CLUSTERS="${S1_CLUSTERS} kwok-cluster-${i}"
                else
                    S2_CLUSTERS="${S2_CLUSTERS} kwok-cluster-${i}"
                fi
            done

            # 使用 deploy_fast.sh 的 join_karmada 逻辑重新注册
            log_info "在服务器1重新注册: $S1_CLUSTERS"
            for cluster_name in $S1_CLUSTERS; do
                i=$(echo $cluster_name | grep -o '[0-9]*$')
                port=$((BASE_PORT + i))
                kc="/tmp/kwok_${cluster_name}.kubeconfig"

                ssh root@$SERVER1 "kwokctl get kubeconfig --name=$cluster_name > $kc 2>/dev/null" || true
                ca_bundle=$(ssh root@$SERVER1 "grep certificate-authority-data: $kc 2>/dev/null | sed 's/.*certificate-authority-data: //'") || true

                if [ -n "$ca_bundle" ]; then
                    kubectl --kubeconfig="$KCFG" apply -f - 2>/dev/null << EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${cluster_name}
  namespace: karmada-cluster
data:
  ca.crt: ${ca_bundle}
  token: $(ssh root@$SERVER1 "kubectl --kubeconfig=$kc create token karmada-controller -n kube-system --duration=8760h 2>/dev/null" | base64 -w0)
---
apiVersion: cluster.karmada.io/v1alpha1
kind: Cluster
metadata:
  name: ${cluster_name}
spec:
  insecureSkipTLSVerification: true
  apiEndpoint: https://${SERVER1}:${port}
EOF
                    log_info "${cluster_name} 重新注册完成"
                else
                    log_warn "${cluster_name} 重新注册失败：无法获取 CA"
                fi
            done

            log_info "在服务器2重新注册: $S2_CLUSTERS"
            for cluster_name in $S2_CLUSTERS; do
                i=$(echo $cluster_name | grep -o '[0-9]*$')
                port=$((BASE_PORT + i))
                kc="/tmp/kwok_${cluster_name}.kubeconfig"

                ssh root@$SERVER2 "kwokctl get kubeconfig --name=$cluster_name > $kc 2>/dev/null" || true
                ca_bundle=$(ssh root@$SERVER2 "grep certificate-authority-data: $kc 2>/dev/null | sed 's/.*certificate-authority-data: //'") || true

                if [ -n "$ca_bundle" ]; then
                    kubectl --kubeconfig="$KCFG" apply -f - 2>/dev/null << EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${cluster_name}
  namespace: karmada-cluster
data:
  ca.crt: ${ca_bundle}
  token: $(ssh root@$SERVER2 "kubectl --kubeconfig=$kc create token karmada-controller -n kube-system --duration=8760h 2>/dev/null" | base64 -w0)
---
apiVersion: cluster.karmada.io/v1alpha1
kind: Cluster
metadata:
  name: ${cluster_name}
spec:
  insecureSkipTLSVerification: true
  apiEndpoint: https://${SERVER2}:${port}
EOF
                    log_info "${cluster_name} 重新注册完成"
                else
                    log_warn "${cluster_name} 重新注册失败：无法获取 CA"
                fi
            done

            # 等待集群状态同步
            sleep 5
        fi
    fi

    # 启动调度器
    log_info "启动调度器 (${mode} 模式)..."
    (cd ../.. && ./lyra.exe) > "${result_dir}/scheduler.log" 2>&1 &
    LYRA_PID=$!
    sleep 5

    # 验证调度器启动
    if ! ps -p ${LYRA_PID} > /dev/null 2>&1; then
        log_err "调度器启动失败！"
        cat "${result_dir}/scheduler.log" | tail -30
        return 1
    fi

    # 记录开始时间
    TEST_START=$(date +%s)

    # 启动背景事件模拟
    start_event_sim

    # 持续压力提交
    log_info "开始持续压力测试..."
    log_info "速率: ${POD_RATE} pods/s, 持续: ${DURATION_SEC}s, 预计: ${EXPECTED_PODS} pods"
    bash ../../shared/../../shared/scripts/submit_sustained.sh ${POD_RATE} ${DURATION_SEC} default

    # 轮询等待调度器处理积压（PP 数量不再增长说明调度器处理完毕）
    # 注意：背景事件在调度器处理积压期间继续运行，以测试增量快照机制
    log_info "等待调度器处理积压..."
    WAIT_MAX=300  # 最长等待 5 分钟
    WAIT_ELAPSED=0
    LAST_PP_COUNT=-1
    STABLE_COUNT=0
    while [ $WAIT_ELAPSED -lt $WAIT_MAX ]; do
        PP_COUNT=$(kubectl --kubeconfig="../../kubeconfig/karmada-apiserver.config" get pp -A -l app=stress-pod --no-headers 2>/dev/null | wc -l)
        if [ "$PP_COUNT" -eq "$LAST_PP_COUNT" ]; then
            STABLE_COUNT=$((STABLE_COUNT + 5))
        else
            STABLE_COUNT=0
        fi
        # PP 数量连续 10 秒不变，说明调度器已处理完毕
        if [ $STABLE_COUNT -ge 10 ]; then
            log_info "调度器处理完毕，共调度 ${PP_COUNT} 个 Pod (${WAIT_ELAPSED}s)"
            break
        fi
        LAST_PP_COUNT=$PP_COUNT
        log_info "已调度 ${PP_COUNT} 个 Pod，等待中... (${WAIT_ELAPSED}s/${WAIT_MAX}s)"
        sleep 5
        WAIT_ELAPSED=$((WAIT_ELAPSED + 5))
    done
    if [ $WAIT_ELAPSED -ge $WAIT_MAX ]; then
        log_warn "等待超时 (${WAIT_MAX}s)，已调度 ${PP_COUNT} 个 Pod"
    fi

    # 积压处理完毕后停止背景事件
    stop_event_sim "${result_dir}"

    # 记录结束时间
    TEST_END=$(date +%s)
    TOTAL_ELAPSED=$((TEST_END - TEST_START))

    # 收集指标
    log_info "收集性能指标..."
    bash ../../shared/../../shared/scripts/collect_metrics.sh "${result_dir}/scheduler.log" "${result_dir}/metrics.csv"

    # 计算统计数据
    log_info "计算统计数据..."
    python3 << PYEOF
import csv
import json
import statistics

csv_path = "${result_dir}/metrics.csv"
e2e_path = "${result_dir}/metrics_e2e.csv"
summary_path = "${result_dir}/summary.json"

# 读取 metrics.csv
records = []
with open(csv_path, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row.get('timestamp'):
            records.append(row)

# 读取 E2E 数据
e2e_records = []
with open(e2e_path, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row.get('timestamp'):
            e2e_records.append(row)

if records:
    total_pods = len(records)
    total_nodes = int(records[0]['total_nodes']) if records else 0

    # 提取各指标
    snapshot_us = [float(r['snapshot_us']) for r in records if r.get('snapshot_us')]
    filter_us = [float(r['filter_us']) for r in records if r.get('filter_us')]
    score_us = [float(r['score_us']) for r in records if r.get('score_us')]
    gpu_alloc_us = [float(r['gpu_alloc_us']) for r in records if r.get('gpu_alloc_us')]
    cloned_clusters = [int(r['cloned_clusters']) for r in records if r.get('cloned_clusters')]
    cloned_nodes = [int(r['cloned_nodes']) for r in records if r.get('cloned_nodes')]

    # E2E 指标
    e2e_ms = [float(r['e2e_ms']) for r in e2e_records if r.get('e2e_ms')]

    # 计算统计量
    def calc_stats(data):
        if not data:
            return {'avg': 0, 'p50': 0, 'p95': 0, 'max': 0}
        sorted_data = sorted(data)
        n = len(sorted_data)
        return {
            'avg': round(statistics.mean(data), 2),
            'p50': round(sorted_data[n//2], 2),
            'p95': round(sorted_data[int(n*0.95)], 2) if n >= 20 else round(sorted_data[-1], 2),
            'max': round(max(data), 2)
        }

    summary = {
        'test_config': {
            'scale': '${SCALE}',
            'mode': '${mode}',
            'rate': ${POD_RATE},
            'duration_sec': ${DURATION_SEC},
            'total_nodes': total_nodes,
            'clusters': ${CLUSTERS},
            'event_rate': ${EVENT_RATE},
        },
        'scheduled': total_pods,
        'expected_pods': ${EXPECTED_PODS},
        'total_elapsed_sec': ${TOTAL_ELAPSED},
        'snapshot_us': calc_stats(snapshot_us),
        'filter_us': calc_stats(filter_us),
        'score_us': calc_stats(score_us),
        'gpu_alloc_us': calc_stats(gpu_alloc_us),
        'cloned_clusters': {
            'avg': round(statistics.mean(cloned_clusters), 2) if cloned_clusters else 0,
            'max': max(cloned_clusters) if cloned_clusters else 0
        },
        'cloned_nodes': {
            'avg': round(statistics.mean(cloned_nodes), 2) if cloned_nodes else 0,
            'max': max(cloned_nodes) if cloned_nodes else 0
        },
        'cloned_ratio': round(statistics.mean(cloned_nodes) / total_nodes, 4) if cloned_nodes and total_nodes > 0 else 0,
        'e2e_ms': calc_stats(e2e_ms),
        'throughput_pods_per_sec': round(1000 / statistics.mean(e2e_ms), 1) if e2e_ms else 0
    }

    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    print(json.dumps(summary, indent=2, ensure_ascii=False))
else:
    print("No records found")
PYEOF

    # 停止调度器
    log_info "停止调度器..."
    kill ${LYRA_PID} 2>/dev/null || true
    sleep 2
    kill -9 ${LYRA_PID} 2>/dev/null || true

    log_info "====== ${mode} 模式测试完成 ======"
}

# =============================================================================
# 主流程
# =============================================================================

START_TIME=$(date +%s)

# 编译
log_info "编译调度器..."
(cd ../.. && go build -o lyra.exe .)

# 清理旧环境
log_info "清理旧环境..."
bash ../../shared/scripts/cleanup_fast.sh ${SCALE} 2>/dev/null || true
sleep 5

# 部署测试环境
log_info "部署测试环境 (${SCALE})..."
bash ../../shared/scripts/deploy_fast.sh ${SCALE}
sleep 10

# 结果目录
BASE_RESULT_DIR="perf_results/${SCALE}_full_perf_${DATE}"
mkdir -p "${BASE_RESULT_DIR}"

# 保存测试配置
cat > "${BASE_RESULT_DIR}/config.json" << EOF
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
  "date": "${DATE}"
}
EOF

# Step 1: 运行增量快照测试
INCR_DIR="${BASE_RESULT_DIR}/incremental"
mkdir -p "${INCR_DIR}"
run_test incremental "${INCR_DIR}"

# Step 2: 运行全量快照测试（集群复用，不重建）
FULL_DIR="${BASE_RESULT_DIR}/full"
mkdir -p "${FULL_DIR}"
run_test full "${FULL_DIR}"

# Step 4: 生成对比报告
log_info "生成对比报告..."
RESULT_DIR="${BASE_RESULT_DIR}" python3 << 'PYEOF'
import json, os

base_dir = os.environ["RESULT_DIR"]
incr_file = os.path.join(base_dir, "incremental", "summary.json")
full_file = os.path.join(base_dir, "full", "summary.json")
report_file = os.path.join(base_dir, "comparison.md")

with open(incr_file, encoding='utf-8') as f:
    incr = json.load(f)
with open(full_file, encoding='utf-8') as f:
    full = json.load(f)

def calc_speedup(full_val, incr_val):
    if incr_val > 0:
        return round(full_val / incr_val, 2)
    return "N/A"

report = f"""# Lyra 调度器全面性能测试报告

## 测试配置

| 项目 | 值 |
|------|-----|
| 测试规模 | {incr['test_config']['scale']} |
| 集群数 | {incr['test_config']['clusters']} |
| 总节点数 | {incr['test_config']['total_nodes']} |
| 提交速率 | {incr['test_config']['rate']} pods/s |
| 背景事件速率 | {incr['test_config'].get('event_rate', 'N/A')} events/s |
| 持续时间 | {incr['test_config']['duration_sec']} 秒 |
| 预计 Pod 数 | {incr['expected_pods']} |

## 调度统计

| 指标 | 增量快照 | 全量快照 |
|------|---------|---------|
| 实际调度数 | {incr['scheduled']} | {full['scheduled']} |
| 总耗时 | {incr['total_elapsed_sec']}s | {full['total_elapsed_sec']}s |

## 快照性能对比

| 指标 | 增量快照 (avg) | 全量快照 (avg) | 加速比 |
|------|---------------|---------------|--------|
| 快照更新 (μs) | {incr['snapshot_us']['avg']} | {full['snapshot_us']['avg']} | **{calc_speedup(full['snapshot_us']['avg'], incr['snapshot_us']['avg'])}x** |
| Filter (μs) | {incr['filter_us']['avg']} | {full['filter_us']['avg']} | {calc_speedup(full['filter_us']['avg'], incr['filter_us']['avg'])}x |
| Score (μs) | {incr['score_us']['avg']} | {full['score_us']['avg']} | {calc_speedup(full['score_us']['avg'], incr['score_us']['avg'])}x |
| GPU分配 (μs) | {incr['gpu_alloc_us']['avg']} | {full['gpu_alloc_us']['avg']} | {calc_speedup(full['gpu_alloc_us']['avg'], incr['gpu_alloc_us']['avg'])}x |

## 增量快照数据复制量

| 指标 | 值 |
|------|-----|
| 平均克隆集群数 | {incr['cloned_clusters']['avg']} |
| 平均克隆节点数 | {incr['cloned_nodes']['avg']} |
| 最大克隆节点数 | {incr['cloned_nodes']['max']} |
| 克隆比例 | {incr['cloned_ratio'] * 100:.2f}% |

## 端到端性能

| 指标 | 增量快照 | 全量快照 | 加速比 |
|------|---------|---------|--------|
| 平均 E2E 时延 | {incr['e2e_ms']['avg']} ms | {full['e2e_ms']['avg']} ms | {calc_speedup(full['e2e_ms']['avg'], incr['e2e_ms']['avg'])}x |
| P50 E2E 时延 | {incr['e2e_ms']['p50']} ms | {full['e2e_ms']['p50']} ms | - |
| P95 E2E 时延 | {incr['e2e_ms']['p95']} ms | {full['e2e_ms']['p95']} ms | - |
| **调度吞吐量** | **{incr['throughput_pods_per_sec']} pods/s** | **{full['throughput_pods_per_sec']} pods/s** | - |

## E2E 时延分布 (增量快照)

| 百分位 | 时延 (ms) |
|--------|----------|
| P50 | {incr['e2e_ms']['p50']} |
| P95 | {incr['e2e_ms']['p95']} |
| Max | {incr['e2e_ms']['max']} |

---
报告生成时间: {incr['test_config']['scale']}
"""

with open(report_file, 'w', encoding='utf-8') as f:
    f.write(report)

print(report)
PYEOF

# Step 5: 清理环境
log_info "清理测试环境..."
bash ../../shared/scripts/cleanup_fast.sh ${SCALE} 2>/dev/null || true

END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))

echo ""
echo "=========================================="
echo "  全面性能测试完成!"
echo "=========================================="
echo "总耗时: $((TOTAL_TIME / 60)) 分钟 $((TOTAL_TIME % 60)) 秒"
echo "结果目录: ${BASE_RESULT_DIR}"
echo "对比报告: ${BASE_RESULT_DIR}/comparison.md"
echo "=========================================="