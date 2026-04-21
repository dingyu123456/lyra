#!/usr/bin/env python3
"""
解析 Lyra QueueingHint GPU 集群场景 Benchmark 结果，生成报告。
用法: python3 parse_benchmark.py <benchmark_raw.txt> <output_dir>
"""
import sys
import os
import re
import csv
from datetime import datetime

def parse_benchmark(filepath):
    """解析 go test -bench 输出"""
    results = []
    pattern = re.compile(
        r'^(Benchmark\w+/[\w/]+)\s+'  # name
        r'(\d+)\s+'                    # iterations
        r'([\d.]+)\s+ns/op\s+'        # ns_per_op
        r'([\d.]+)\s+ns/op \[avg'     # avg_attempts (custom metric, may not exist)
    )

    # 更灵活的解析
    line_pattern = re.compile(
        r'^(Benchmark\S+)\s+'
        r'(\d+)\s+'
        r'([\d.]+)\s+ns/op'
    )

    custom_metric_pattern = re.compile(
        r'(\d+(?:\.\d+)?)\s+avg_attempts/op'
    )

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()

            # 匹配 benchmark 结果行
            m = line_pattern.match(line)
            if not m:
                continue

            name = m.group(1)
            iterations = int(m.group(2))
            ns_per_op = float(m.group(3))

            # 提取自定义 metric (avg_attempts/op)
            avg_attempts = 0
            cm = custom_metric_pattern.search(line)
            if cm:
                avg_attempts = float(cm.group(1))

            results.append({
                'name': name,
                'iterations': iterations,
                'ns_per_op': ns_per_op,
                'avg_attempts': avg_attempts,
            })

    return results

def extract_run_info(name):
    """从 benchmark 名提取 filter_level 和 gpu_event_type"""
    parts = name.split('/')
    if len(parts) >= 3:
        filter_level = parts[1]  # NoFilter, PluginFilter, GPUTypeFilter
        gpu_event = parts[2]     # A100_Release, V100_Release, etc.
        return filter_level, gpu_event
    return name, ''

def generate_report(results, output_dir):
    """生成 Markdown 报告"""
    # 按 filter_level 和 gpu_event 分组
    grouped = {}
    for r in results:
        filter_level, gpu_event = extract_run_info(r['name'])
        key = gpu_event
        if key not in grouped:
            grouped[key] = {}
        if filter_level not in grouped[key]:
            grouped[key][filter_level] = []
        grouped[key][filter_level].append(r)

    # 计算统计
    report_lines = []
    report_lines.append("# Lyra QueueingHint GPU 集群场景实验报告")
    report_lines.append("")
    report_lines.append(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("")

    # 场景描述
    report_lines.append("## 测试场景")
    report_lines.append("")
    report_lines.append("| 类型 | 数量 | 失败插件 |")
    report_lines.append("|------|------|---------|")
    report_lines.append("| 非 GPU 任务 (CPU/内存) | 300 | NodeResourcesFit |")
    report_lines.append("| GPU 任务 - 无类型限制 | 210 | GPUResourceFit |")
    report_lines.append("| GPU 任务 - A100 | 175 | GPUResourceFit |")
    report_lines.append("| GPU 任务 - V100 | 140 | GPUResourceFit |")
    report_lines.append("| GPU 任务 - H100 | 105 | GPUResourceFit |")
    report_lines.append("| GPU 任务 - 4090 | 70 | GPUResourceFit |")
    report_lines.append("| **总计** | **1000** | |")
    report_lines.append("")

    # 无效唤醒对比表
    report_lines.append("## 无效唤醒对比")
    report_lines.append("")
    report_lines.append("| GPU 释放事件 | 无 Hint | 插件级 Hint | GPU 类型级 Hint | 总削减 |")
    report_lines.append("|-------------|---------|------------|----------------|--------|")

    total_nofilter = 0
    total_plugin = 0
    total_gputype = 0

    for gpu_event in sorted(grouped.keys()):
        nofilter_attempts = 0
        plugin_attempts = 0
        gputype_attempts = 0

        if 'NoFilter' in grouped[gpu_event]:
            nofilter_attempts = sum(r['avg_attempts'] for r in grouped[gpu_event]['NoFilter']) / len(grouped[gpu_event]['NoFilter'])
        if 'PluginFilter' in grouped[gpu_event]:
            plugin_attempts = sum(r['avg_attempts'] for r in grouped[gpu_event]['PluginFilter']) / len(grouped[gpu_event]['PluginFilter'])
        if 'GPUTypeFilter' in grouped[gpu_event]:
            gputype_attempts = sum(r['avg_attempts'] for r in grouped[gpu_event]['GPUTypeFilter']) / len(grouped[gpu_event]['GPUTypeFilter'])

        total_nofilter += nofilter_attempts
        total_plugin += plugin_attempts
        total_gputype += gputype_attempts

        reduction = f"{(1 - gputype_attempts / nofilter_attempts) * 100:.1f}%" if nofilter_attempts > 0 else "N/A"

        report_lines.append(
            f"| {gpu_event} | {nofilter_attempts:.0f} | {plugin_attempts:.0f} | {gputype_attempts:.0f} | **{reduction}** |"
        )

    # 总计行
    if total_nofilter > 0:
        total_reduction = f"{(1 - total_gputype / total_nofilter) * 100:.1f}%"
        report_lines.append(f"| **总计** | **{total_nofilter:.0f}** | **{total_plugin:.0f}** | **{total_gputype:.0f}** | **{total_reduction}** |")

    report_lines.append("")

    # 详细数据
    report_lines.append("## 详细 Benchmark 数据")
    report_lines.append("")
    report_lines.append("| 测试用例 | 迭代次数 | ns/op | avg_attempts/op |")
    report_lines.append("|---------|---------|-------|----------------|")

    for r in results:
        report_lines.append(f"| {r['name']} | {r['iterations']} | {r['ns_per_op']:.1f} | {r['avg_attempts']:.0f} |")

    report_lines.append("")
    report_lines.append("---")
    report_lines.append(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    report_content = "\n".join(report_lines)

    # 写入报告
    report_path = os.path.join(output_dir, "report.md")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_content)

    # 写入 CSV
    csv_path = os.path.join(output_dir, "benchmark_parsed.csv")
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'iterations', 'ns_per_op', 'avg_attempts'])
        writer.writeheader()
        writer.writerows(results)

    return report_content

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(f"用法: python3 {sys.argv[0]} <benchmark_raw.txt> <output_dir>")
        sys.exit(1)

    raw_file = sys.argv[1]
    output_dir = sys.argv[2]

    results = parse_benchmark(raw_file)
    if not results:
        print("未找到 benchmark 结果，请检查输入文件")
        sys.exit(1)

    report = generate_report(results, output_dir)
    print(f"解析完成: {len(results)} 条记录")
