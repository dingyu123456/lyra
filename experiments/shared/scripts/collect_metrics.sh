#!/bin/bash
# =============================================================================
# Lyra 调度器性能指标收集脚本 (Windows兼容版)
# 用法: ./collect_metrics.sh <log_file> [output_csv]
# =============================================================================

set -e

LOG_FILE=${1:-"../monitor.log"}
OUTPUT_FILE=${2:-"metrics_$(date +%Y%m%d_%H%M%S).csv"}
E2E_FILE="${OUTPUT_FILE%.csv}_e2e.csv"

echo "收集性能指标..."
echo "日志文件: ${LOG_FILE}"
echo "输出文件: ${OUTPUT_FILE}"

# CSV 头部
echo "timestamp,pod,snapshot_mode,snapshot_us,filter_us,score_us,gpu_alloc_us,feasible_nodes,total_nodes,cloned_clusters,cloned_nodes" > "${OUTPUT_FILE}"

# E2E CSV 头部
echo "timestamp,pod,e2e_us,e2e_ms" > "${E2E_FILE}"

# 使用 awk 解析 JSON 日志
# 只匹配 "SchedulePod stage timing" 行，排除 "Assume timing" 行
# 每个 Pod 只取第一次成功调度记录
awk '
/\[PERF\]/ && /SchedulePod stage timing/ {
    timestamp = ""
    pod = ""
    snapshot_mode = ""
    snapshot_ns = 0
    filter_ns = 0
    score_ns = 0
    gpu_alloc_ns = 0
    feasible = 0
    total = 0
    cloned_clusters = 0
    cloned_nodes = 0

    if (match($0, /"time":"([^"]+)"/, t)) timestamp = t[1]
    if (match($0, /"pod":"([^"]+)"/, p)) pod = p[1]

    # 只保留首次调度
    if (pod == "" || seen[pod]) next
    seen[pod] = 1

    # 提取所有指标
    if (match($0, /"snapshot_mode":"([^"]+)"/, m)) snapshot_mode = m[1]
    if (match($0, /"snapshot_ns":([0-9]+)/, s)) snapshot_ns = s[1]
    if (match($0, /"filter_ns":([0-9]+)/, f)) filter_ns = f[1]
    if (match($0, /"score_ns":([0-9]+)/, sc)) score_ns = sc[1]
    if (match($0, /"gpu_alloc_ns":([0-9]+)/, g)) gpu_alloc_ns = g[1]
    if (match($0, /"feasible_nodes":([0-9]+)/, fe)) feasible = fe[1]
    if (match($0, /"total_nodes":([0-9]+)/, to)) total = to[1]
    if (match($0, /"cloned_clusters":([0-9]+)/, cc)) cloned_clusters = cc[1]
    if (match($0, /"cloned_nodes":([0-9]+)/, cn)) cloned_nodes = cn[1]

    # 输出所有记录
    snapshot_us = snapshot_ns / 1000
    filter_us = filter_ns / 1000
    score_us = score_ns / 1000
    gpu_alloc_us = gpu_alloc_ns / 1000

    printf "%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%s,%s,%s,%s\n", timestamp, pod, snapshot_mode, snapshot_us, filter_us, score_us, gpu_alloc_us, feasible, total, cloned_clusters, cloned_nodes
}
' "${LOG_FILE}" >> "${OUTPUT_FILE}"

# 解析 E2E 日志 (SchedulingCycle completed with e2e_ns)
awk '
/SchedulingCycle completed/ && /e2e_ns/ {
    timestamp = ""
    pod = ""
    e2e_ns = 0

    if (match($0, /"time":"([^"]+)"/, t)) timestamp = t[1]
    if (match($0, /"pod":"([^"]+)"/, p)) pod = p[1]
    if (match($0, /"e2e_ns":([0-9]+)/, e)) e2e_ns = e[1]

    if (pod != "" && e2e_ns > 0) {
        e2e_us = e2e_ns / 1000
        e2e_ms = e2e_ns / 1000000
        printf "%s,%s,%.2f,%.3f\n", timestamp, pod, e2e_us, e2e_ms
    }
}
' "${LOG_FILE}" >> "${E2E_FILE}"

echo "指标已保存到: ${OUTPUT_FILE}"
echo "E2E指标已保存到: ${E2E_FILE}"

# 打印统计摘要
if [ -s "${OUTPUT_FILE}" ]; then
    echo ""
    echo "=== 性能统计摘要 ==="
    echo ""
    echo "总记录数: $(tail -n +2 "${OUTPUT_FILE}" | wc -l)"
    echo ""
    echo "快照模式分布:"
    tail -n +2 "${OUTPUT_FILE}" | cut -d',' -f3 | sort | uniq -c

    echo ""
    echo "平均耗时 (微秒):"
    echo -n "  快照更新: "
    tail -n +2 "${OUTPUT_FILE}" | cut -d',' -f4 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f\n", sum/count; else print "N/A"}'

    echo -n "  Filter: "
    tail -n +2 "${OUTPUT_FILE}" | cut -d',' -f5 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f\n", sum/count; else print "N/A"}'

    echo -n "  Score: "
    tail -n +2 "${OUTPUT_FILE}" | cut -d',' -f6 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f\n", sum/count; else print "N/A"}'

    echo -n "  GPU分配: "
    tail -n +2 "${OUTPUT_FILE}" | cut -d',' -f7 | awk '{sum+=$1; count++} END {if(count>0) printf "%.2f\n", sum/count; else print "N/A"}'
fi

# E2E 统计
if [ -s "${E2E_FILE}" ]; then
    echo ""
    echo "=== E2E 调度时延统计 ==="
    echo ""
    echo "总记录数: $(tail -n +2 "${E2E_FILE}" | wc -l)"
    echo -n "  平均时延: "
    tail -n +2 "${E2E_FILE}" | cut -d',' -f4 | awk '{sum+=$1; count++} END {if(count>0) printf "%.3f ms\n", sum/count; else print "N/A"}'
    echo -n "  P50时延: "
    tail -n +2 "${E2E_FILE}" | cut -d',' -f4 | sort -n | awk '{arr[NR]=$1} END {if(NR>0) printf "%.3f ms\n", arr[int(NR*0.5)]}'
    echo -n "  P95时延: "
    tail -n +2 "${E2E_FILE}" | cut -d',' -f4 | sort -n | awk '{arr[NR]=$1} END {if(NR>0) printf "%.3f ms\n", arr[int(NR*0.95)]}'
fi
