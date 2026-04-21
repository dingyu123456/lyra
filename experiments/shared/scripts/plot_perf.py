#!/usr/bin/env python3
"""
Lyra Scheduler Performance Report Generator

Usage:
    python3 plot_perf.py <results_dir>

Example:
    python3 docs/experiments/scripts/plot_perf.py docs/experiments/perf_results/small_20260417_202017

Reads incremental.log and full.log, generates:
  - latency_distribution.png  (E2E latency histogram: incremental vs full)
  - stage_comparison.png      (stage-by-stage bar chart)
  - summary.json              (machine-readable summary)
"""

import sys
import os
import json
import re
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError:
    print("matplotlib/numpy not installed. Install with: pip install matplotlib numpy")
    sys.exit(1)


def parse_log(log_path):
    """Parse lyra scheduler log, extract PERF and E2E entries."""
    records = []
    e2e_records = []
    seen_pods = set()
    seen_e2e = set()

    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            # Parse [PERF] SchedulePod stage timing
            if '[PERF]' in line and 'SchedulePod stage timing' in line:
                rec = {}
                m = re.search(r'"pod":"([^"]+)"', line)
                if m:
                    rec['pod'] = m.group(1)
                m = re.search(r'"snapshot_ns":(\d+)', line)
                if m:
                    rec['snapshot_us'] = int(m.group(1)) / 1000
                m = re.search(r'"filter_ns":(\d+)', line)
                if m:
                    rec['filter_us'] = int(m.group(1)) / 1000
                m = re.search(r'"score_ns":(\d+)', line)
                if m:
                    rec['score_us'] = int(m.group(1)) / 1000
                m = re.search(r'"gpu_alloc_ns":(\d+)', line)
                if m:
                    rec['gpu_alloc_us'] = int(m.group(1)) / 1000
                m = re.search(r'"snapshot_mode":"([^"]+)"', line)
                if m:
                    rec['snapshot_mode'] = m.group(1)
                m = re.search(r'"feasible_nodes":(\d+)', line)
                if m:
                    rec['feasible_nodes'] = int(m.group(1))
                m = re.search(r'"total_nodes":(\d+)', line)
                if m:
                    rec['total_nodes'] = int(m.group(1))
                # Dedup: keep first record per pod
                if rec.get('pod') and rec['pod'] not in seen_pods:
                    seen_pods.add(rec['pod'])
                    records.append(rec)

            # Parse e2e_ns from SchedulingCycle completed log
            if 'SchedulingCycle completed' in line and 'e2e_ns' in line:
                m = re.search(r'"pod":"([^"]+)"', line)
                pod = m.group(1) if m else ''
                m = re.search(r'"e2e_ns":(\d+)', line)
                if m and pod and pod not in seen_e2e:
                    seen_e2e.add(pod)
                    e2e_records.append({
                        'pod': pod,
                        'e2e_us': int(m.group(1)) / 1000,
                        'e2e_ms': int(m.group(1)) / 1_000_000,
                    })

    return records, e2e_records


def stats(values):
    if not values:
        return {'count': 0, 'avg': 0, 'p50': 0, 'p95': 0, 'p99': 0, 'min': 0, 'max': 0}
    arr = sorted(values)
    n = len(arr)
    return {
        'count': n,
        'avg': sum(arr) / n,
        'p50': arr[int(n * 0.50)],
        'p95': arr[int(n * 0.95)] if n > 1 else arr[0],
        'p99': arr[int(n * 0.99)] if n > 1 else arr[0],
        'min': arr[0],
        'max': arr[-1],
    }


def plot_latency_distribution(e2e_incr, e2e_full, output_dir):
    """Plot E2E latency distribution histogram."""
    fig, ax = plt.subplots(figsize=(10, 6))

    all_vals = e2e_incr + e2e_full
    if not all_vals:
        print("No E2E data to plot")
        return

    bins = 30
    if e2e_incr:
        ax.hist(e2e_incr, bins=bins, alpha=0.6, label=f'Incremental (n={len(e2e_incr)})', color='#2196F3')
    if e2e_full:
        ax.hist(e2e_full, bins=bins, alpha=0.6, label=f'Full (n={len(e2e_full)})', color='#FF5722')

    ax.set_xlabel('E2E Scheduling Latency (ms)')
    ax.set_ylabel('Pod Count')
    ax.set_title('End-to-End Scheduling Latency Distribution')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    # Add stats annotation
    if e2e_incr:
        s = stats(e2e_incr)
        ax.axvline(s['avg'], color='#2196F3', linestyle='--', linewidth=1)
    if e2e_full:
        s = stats(e2e_full)
        ax.axvline(s['avg'], color='#FF5722', linestyle='--', linewidth=1)

    plt.tight_layout()
    path = os.path.join(output_dir, 'latency_distribution.png')
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  -> {path}")


def plot_stage_comparison(records_incr, records_full, output_dir):
    """Plot stage-by-stage comparison bar chart."""
    stages = ['snapshot_us', 'filter_us', 'score_us', 'gpu_alloc_us']
    labels = ['Snapshot', 'Filter', 'Score', 'GPU Alloc']

    incr_avgs = []
    full_avgs = []
    for stage in stages:
        incr_vals = [r[stage] for r in records_incr if stage in r]
        full_vals = [r[stage] for r in records_full if stage in r]
        incr_avgs.append(sum(incr_vals) / len(incr_vals) if incr_vals else 0)
        full_avgs.append(sum(full_vals) / len(full_vals) if full_vals else 0)

    fig, ax = plt.subplots(figsize=(10, 6))
    x = np.arange(len(labels))
    width = 0.35

    bars1 = ax.bar(x - width/2, incr_avgs, width, label='Incremental', color='#2196F3')
    bars2 = ax.bar(x + width/2, full_avgs, width, label='Full', color='#FF5722')

    ax.set_ylabel('Average Duration (us)')
    ax.set_title('Scheduling Stage Duration Comparison')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    for bar in bars1:
        if bar.get_height() > 0:
            ax.annotate(f'{bar.get_height():.1f}',
                        xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=8)
    for bar in bars2:
        if bar.get_height() > 0:
            ax.annotate(f'{bar.get_height():.1f}',
                        xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=8)

    plt.tight_layout()
    path = os.path.join(output_dir, 'stage_comparison.png')
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  -> {path}")


def plot_concurrency_trend(result_dirs, output_dir):
    """
    Plot concurrency vs E2E latency and throughput.
    Input: list of c{N}/ subdirectories, each containing scheduler.log and summary.json
    """
    data_points = []

    for d in result_dirs:
        dirname = os.path.basename(d.rstrip('/'))
        # Extract concurrent count: c10, c20, etc.
        concurrent = None
        m = re.match(r'c(\d+)', dirname)
        if m:
            concurrent = int(m.group(1))

        # Try reading summary.json first
        summary_path = os.path.join(d, 'summary.json')
        if os.path.exists(summary_path):
            with open(summary_path, 'r') as f:
                summary = json.load(f)
            if concurrent is None:
                concurrent = summary.get('concurrent', 0)
            data_points.append({
                'concurrent': concurrent,
                'scheduled': summary.get('scheduled', summary.get('dispatched', 0)),
                'first_attempt_success': summary.get('first_attempt_success', summary.get('dispatched', 0)),
                'throughput': summary.get('scheduling_throughput_pods_per_sec', summary.get('throughput', 0)),
                'avg_e2e_ms': summary.get('avg_e2e_ms', 0),
            })
            continue

        # Fallback: parse log
        log_path = os.path.join(d, 'scheduler.log')
        if not os.path.exists(log_path):
            continue
        _, e2e = parse_log(log_path)
        if not e2e:
            continue
        e2e_ms = [r['e2e_ms'] for r in e2e]
        s = stats(e2e_ms)
        avg_e2e = s['avg']
        data_points.append({
            'concurrent': concurrent or 0,
            'dispatched': dispatched,
            'throughput': 1000.0 / avg_e2e if avg_e2e > 0 else 0,
            'avg_e2e_ms': avg_e2e,
            'p50': s['p50'],
            'p95': s['p95'],
        })

    if len(data_points) < 2:
        print(f"Need >= 2 data points for trend plot, got {len(data_points)}")
        return

    data_points.sort(key=lambda x: x['concurrent'])
    concurrents = [d['concurrent'] for d in data_points]
    avg_e2e = [d['avg_e2e_ms'] for d in data_points]
    throughputs = [d['throughput'] for d in data_points]
    scheduled = [d['scheduled'] for d in data_points]
    first_success = [d['first_attempt_success'] for d in data_points]

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    # 1. Latency vs Concurrency
    ax1 = axes[0]
    ax1.plot(concurrents, avg_e2e, 'o-', color='#2196F3', linewidth=2, markersize=8)
    ax1.fill_between(concurrents, [max(0, v * 0.8) for v in avg_e2e],
                     [v * 1.2 for v in avg_e2e], alpha=0.15, color='#2196F3')
    ax1.set_xlabel('Concurrent Pods')
    ax1.set_ylabel('Avg E2E Latency (ms)')
    ax1.set_title('E2E Latency vs Concurrency')
    ax1.grid(alpha=0.3)
    for i, (c, v) in enumerate(zip(concurrents, avg_e2e)):
        ax1.annotate(f'{v:.2f}', (c, v), textcoords="offset points",
                     xytext=(0, 10), ha='center', fontsize=9)

    # 2. Throughput vs Concurrency
    ax2 = axes[1]
    ax2.plot(concurrents, throughputs, 's-', color='#4CAF50', linewidth=2, markersize=8)
    ax2.fill_between(concurrents, [max(0, v * 0.8) for v in throughputs],
                     [v * 1.2 for v in throughputs], alpha=0.15, color='#4CAF50')
    ax2.set_xlabel('Concurrent Pods')
    ax2.set_ylabel('Scheduling Throughput (pods/sec)')
    ax2.set_title('Throughput vs Concurrency')
    ax2.grid(alpha=0.3)
    for i, (c, v) in enumerate(zip(concurrents, throughputs)):
        ax2.annotate(f'{v:.1f}', (c, v), textcoords="offset points",
                     xytext=(0, 10), ha='center', fontsize=9)

    # 3. First-Attempt Success Rate vs Concurrency
    ax3 = axes[2]
    success_rates = [s / c * 100 if c > 0 else 0 for s, c in zip(first_success, concurrents)]
    ax3.bar(range(len(concurrents)), success_rates, color='#FF9800', alpha=0.8)
    ax3.set_xticks(range(len(concurrents)))
    ax3.set_xticklabels([str(c) for c in concurrents])
    ax3.set_xlabel('Concurrent Pods')
    ax3.set_ylabel('First-Attempt Success Rate (%)')
    ax3.set_title('First-Attempt Success Rate vs Concurrency')
    ax3.set_ylim(0, 110)
    ax3.grid(axis='y', alpha=0.3)
    for i, v in enumerate(success_rates):
        ax3.annotate(f'{v:.0f}%', (i, v), textcoords="offset points",
                     xytext=(0, 5), ha='center', fontsize=9)

    plt.tight_layout()
    path = os.path.join(output_dir, 'concurrency_trend.png')
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  -> {path}")


def generate_summary(records_incr, records_full, e2e_incr, e2e_full, dispatched_incr, dispatched_full, output_dir):
    """Generate JSON summary."""
    summary = {
        'incremental': {
            'dispatched': dispatched_incr,
            'total_records': len(records_incr),
            'e2e_ms': stats([r['e2e_ms'] for r in e2e_incr]) if e2e_incr else stats([]),
            'snapshot_us': stats([r['snapshot_us'] for r in records_incr if 'snapshot_us' in r]),
            'filter_us': stats([r['filter_us'] for r in records_incr if 'filter_us' in r]),
            'score_us': stats([r['score_us'] for r in records_incr if 'score_us' in r]),
            'gpu_alloc_us': stats([r['gpu_alloc_us'] for r in records_incr if 'gpu_alloc_us' in r]),
        },
        'full': {
            'dispatched': dispatched_full,
            'total_records': len(records_full),
            'e2e_ms': stats([r['e2e_ms'] for r in e2e_full]) if e2e_full else stats([]),
            'snapshot_us': stats([r['snapshot_us'] for r in records_full if 'snapshot_us' in r]),
            'filter_us': stats([r['filter_us'] for r in records_full if 'filter_us' in r]),
            'score_us': stats([r['score_us'] for r in records_full if 'score_us' in r]),
            'gpu_alloc_us': stats([r['gpu_alloc_us'] for r in records_full if 'gpu_alloc_us' in r]),
        },
    }

    # Snapshot speedup
    incr_snap = summary['incremental']['snapshot_us']['avg']
    full_snap = summary['full']['snapshot_us']['avg']
    if incr_snap > 0 and full_snap > 0:
        summary['snapshot_speedup'] = round(full_snap / incr_snap, 2)

    path = os.path.join(output_dir, 'summary.json')
    with open(path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"  -> {path}")
    return summary


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 plot_perf.py <results_dir> [<results_dir2> ...]")
        print("  Single dir: generate report for one experiment")
        print("  Multiple dirs: generate concurrency trend across experiments")
        sys.exit(1)

    dirs = sys.argv[1:]

    if len(dirs) == 1:
        # Single experiment report
        result_dir = dirs[0]
        if not os.path.isdir(result_dir):
            print(f"Directory not found: {result_dir}")
            sys.exit(1)

        print(f"Generating report for: {os.path.basename(result_dir)}")

        records_incr, e2e_incr = [], []
        records_full, e2e_full = [], []

        incr_log = os.path.join(result_dir, 'incremental.log')
        full_log = os.path.join(result_dir, 'full.log')

        if os.path.exists(incr_log):
            records_incr, e2e_incr = parse_log(incr_log)
            print(f"  Incremental: {len(records_incr)} PERF, {len(e2e_incr)} E2E records")
        if os.path.exists(full_log):
            records_full, e2e_full = parse_log(full_log)
            print(f"  Full: {len(records_full)} PERF, {len(e2e_full)} E2E records")

        e2e_incr_ms = [r['e2e_ms'] for r in e2e_incr]
        e2e_full_ms = [r['e2e_ms'] for r in e2e_full]

        # Count dispatched
        dispatched_incr = count_dispatched(incr_log) if os.path.exists(incr_log) else 0
        dispatched_full = count_dispatched(full_log) if os.path.exists(full_log) else 0

        # Generate plots
        plot_latency_distribution(e2e_incr_ms, e2e_full_ms, result_dir)
        plot_stage_comparison(records_incr, records_full, result_dir)

        # Generate summary
        summary = generate_summary(records_incr, records_full, e2e_incr, e2e_full,
                                   dispatched_incr, dispatched_full, result_dir)

        # Print text summary
        print(f"\n{'='*50}")
        print(f"  E2E Latency Summary")
        print(f"{'='*50}")
        if e2e_incr_ms:
            s = stats(e2e_incr_ms)
            print(f"  Incremental: avg={s['avg']:.2f}ms  p50={s['p50']:.2f}ms  p95={s['p95']:.2f}ms  ({dispatched_incr} dispatched)")
        if e2e_full_ms:
            s = stats(e2e_full_ms)
            print(f"  Full:        avg={s['avg']:.2f}ms  p50={s['p50']:.2f}ms  p95={s['p95']:.2f}ms  ({dispatched_full} dispatched)")
        print(f"{'='*50}")

    else:
        # Concurrency trend across multiple experiments
        print(f"Generating concurrency trend across {len(dirs)} experiments...")
        # Use the parent directory of the first result dir as output location
        parent = os.path.dirname(dirs[0].rstrip('/'))
        plot_concurrency_trend(dirs, parent)


def count_dispatched(log_path):
    count = 0
    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if 'Successfully dispatched' in line and 'test-pod' in line:
                count += 1
    return count


if __name__ == '__main__':
    main()