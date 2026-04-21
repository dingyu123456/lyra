#!/usr/bin/env python3
"""
解析 Lyra QueueingHint E2E 实验结果，生成对比报告。
用法: python3 parse_e2e_results.py <result_dir>
"""
import sys
import os
import csv
import json
from datetime import datetime

def parse_wakeup_csv(csv_path):
    """解析 wakeup_stats.csv"""
    records = []
    if not os.path.exists(csv_path):
        return records
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append({
                'timestamp': row.get('timestamp', ''),
                'event': row.get('event', ''),
                'total_pods': int(row.get('total_pods', 0)),
                'woken_pods': int(row.get('woken_pods', 0)),
                'skipped_pods': int(row.get('skipped_pods', 0)),
                'skip_ratio': float(row.get('skip_ratio', 0)),
            })
    return records

def calc_stats(records):
    """计算统计"""
    if not records:
        return {}
    total_events = len(records)
    total_pods = sum(r['total_pods'] for r in records)
    total_woken = sum(r['woken_pods'] for r in records)
    total_skipped = sum(r['skipped_pods'] for r in records)
    avg_skip_ratio = sum(r['skip_ratio'] for r in records) / total_events
    return {
        'total_events': total_events,
        'total_pods': total_pods,
        'total_woken': total_woken,
        'total_skipped': total_skipped,
        'avg_skip_ratio': avg_skip_ratio,
        'avg_woken_per_event': total_woken / total_events if total_events > 0 else 0,
        'avg_total_per_event': total_pods / total_events if total_events > 0 else 0,
    }

def generate_report(result_dir):
    """生成 Markdown 报告"""
    # 读取配置
    config_path = os.path.join(result_dir, 'config.json')
    config = {}
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

    # 解析两种模式的数据
    hint_records = parse_wakeup_csv(os.path.join(result_dir, 'hint_enabled', 'wakeup_stats.csv'))
    no_hint_records = parse_wakeup_csv(os.path.join(result_dir, 'hint_disabled', 'wakeup_stats.csv'))

    hint_stats = calc_stats(hint_records)
    no_hint_stats = calc_stats(no_hint_records)

    # 生成报告
    lines = []
    lines.append("# Lyra QueueingHint E2E 实验报告")
    lines.append("")
    lines.append(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # 测试配置
    lines.append("## 测试配置")
    lines.append("")
    lines.append("| 项目 | 值 |")
    lines.append("|------|-----|")
    lines.append(f"| 测试规模 | {config.get('scale', 'N/A')} |")
    lines.append(f"| 集群数 | {config.get('clusters', 'N/A')} |")
    lines.append(f"| 总节点数 | {config.get('total_nodes', 'N/A')} |")
    lines.append(f"| Pod 提交速率 | {config.get('pod_rate', 'N/A')} pods/s |")
    lines.append(f"| 背景事件速率 | {config.get('event_rate', 'N/A')} events/s |")
    lines.append(f"| 持续时间 | {config.get('duration_sec', 'N/A')} 秒 |")
    lines.append(f"| 指定 GPU 类型的 Pod 比例 | 50% |")
    lines.append("")

    # GPU 类型分布说明
    lines.append("## GPU 类型分布")
    lines.append("")
    lines.append("| 集群 | GPU 类型 |")
    lines.append("|------|---------|")
    lines.append("| kwok-cluster-0 | NVIDIA-A100-80GB |")
    lines.append("| kwok-cluster-1 | NVIDIA-V100-32GB |")
    lines.append("")

    # 唤醒统计对比
    lines.append("## 队列唤醒统计对比")
    lines.append("")
    lines.append("| 指标 | 有 QueueingHint | 无 QueueingHint | 对比 |")
    lines.append("|------|----------------|----------------|------|")

    if hint_stats and no_hint_stats:
        lines.append(f"| 事件总数 | {hint_stats.get('total_events', 0)} | {no_hint_stats.get('total_events', 0)} | - |")
        lines.append(f"| 每次事件平均扫描 Pod | {hint_stats.get('avg_total_per_event', 0):.1f} | {no_hint_stats.get('avg_total_per_event', 0):.1f} | - |")
        lines.append(f"| 每次事件平均唤醒 Pod | {hint_stats.get('avg_woken_per_event', 0):.1f} | {no_hint_stats.get('avg_woken_per_event', 0):.1f} | - |")
        lines.append(f"| **平均跳过率** | **{hint_stats.get('avg_skip_ratio', 0)*100:.1f}%** | **{no_hint_stats.get('avg_skip_ratio', 0)*100:.1f}%** | - |")

        # 计算削减比例
        hint_woken = hint_stats.get('total_woken', 0)
        no_hint_woken = no_hint_stats.get('total_woken', 0)
        if no_hint_woken > 0:
            reduction = (1 - hint_woken / no_hint_woken) * 100
            lines.append(f"| 总唤醒 Pod 数 | {hint_woken} | {no_hint_woken} | **削减 {reduction:.1f}%** |")
    else:
        lines.append("| (无数据) | - | - | - |")

    lines.append("")

    # 详细数据（有 hint）
    if hint_records:
        lines.append("## 有 QueueingHint - 详细唤醒记录")
        lines.append("")
        lines.append("| 事件 | 扫描 Pod | 唤醒 | 跳过 | 跳过率 |")
        lines.append("|------|---------|------|------|--------|")
        for r in hint_records[:20]:
            lines.append(f"| {r['event'][:30]} | {r['total_pods']} | {r['woken_pods']} | {r['skipped_pods']} | {r['skip_ratio']*100:.1f}% |")
        if len(hint_records) > 20:
            lines.append(f"| ... | ... | ... | ... | ... |")
            lines.append(f"| (共 {len(hint_records)} 条记录) | | | | |")

    lines.append("")
    lines.append("---")
    lines.append(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    report_content = "\n".join(lines)

    # 写入报告
    report_path = os.path.join(result_dir, "report.md")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_content)

    print(report_content)
    return report_content

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"用法: python3 {sys.argv[0]} <result_dir>")
        sys.exit(1)

    generate_report(sys.argv[1])
