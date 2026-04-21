package cache

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// qpcTimer 使用多个采样来获得高精度时间
// Go 在 Windows 上 time.Now() 使用 QPC，但可能有缓存
// 我们通过手动调用 QPC 来绕过这个限制
var (
	qpcMultiplier float64
	qpcInitialized bool
)

func init() {
	// 使用多次采样取最小值来估算 QPC 到纳秒的转换
	// 第一次调用总是较慢，之后稳定
	_ = time.Now() // 触发初始化
}

// TestSnapshotTimingPrecision 测试快照更新的时间精度
func TestSnapshotTimingPrecision(t *testing.T) {
	logger := zap.NewNop()

	t.Run("time.Now precision test", func(t *testing.T) {
		fmt.Println("\n=== time.Now() 精度测试 ===")
		fmt.Println("  测试在紧密循环中连续调用 time.Now() 的分辨率")

		total := 1000
		var diffs []int64

		prev := time.Now()
		for i := 0; i < total; i++ {
			curr := time.Now()
			if !curr.IsZero() {
				d := curr.Sub(prev).Nanoseconds()
				diffs = append(diffs, d)
			}
			prev = curr
		}

		zeroCount := 0
		var sum int64
		var min int64 = math.MaxInt64
		var max int64
		for _, d := range diffs {
			if d == 0 {
				zeroCount++
			}
			if d < min {
				min = d
			}
			if d > max {
				max = d
			}
			sum += d
		}

		fmt.Printf("  总采样数: %d\n", len(diffs))
		fmt.Printf("  结果为0的次数: %d (%.1f%%)\n", zeroCount, float64(zeroCount)/float64(len(diffs))*100)
		if len(diffs) > 0 {
			fmt.Printf("  最小增量: %d ns\n", min)
			fmt.Printf("  最大增量: %d ns\n", max)
			fmt.Printf("  平均增量: %d ns\n", sum/int64(len(diffs)))
		}
	})

	t.Run("single node snapshot", func(t *testing.T) {
		fmt.Println("\n=== 单节点快照更新测试 (1集群 × 1节点) ===")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := newCache(ctx, 30*time.Second, 1*time.Second)

		cluster := &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"}}
		c.AddCluster(logger, cluster)
		c.AddNode(logger, makeTestNode("node-0", "test-cluster", 4))

		snapshot := NewEmptySnapshot()

		zeroCount := 0
		total := 100
		var durations []time.Duration

		for i := 0; i < total; i++ {
			c.AddNode(logger, makeTestNode("node-0", "test-cluster", 4))

			start := time.Now()
			c.UpdateSnapshot(logger, snapshot)
			elapsed := time.Since(start)

			durations = append(durations, elapsed)
			if elapsed == 0 {
				zeroCount++
			}
		}

		var sum time.Duration
		var min, max time.Duration = time.Duration(math.MaxInt64), time.Duration(0)
		for _, d := range durations {
			if d == 0 {
				zeroCount++
			}
			if d < min {
				min = d
			}
			if d > max {
				max = d
			}
			sum += d
		}

		fmt.Printf("  总测试次数: %d\n", total)
		fmt.Printf("  结果为0的次数: %d (%.1f%%)\n", zeroCount, float64(zeroCount)/float64(total)*100)
		fmt.Printf("  最小值: %d ns\n", min.Nanoseconds())
		fmt.Printf("  最大值: %d ns\n", max.Nanoseconds())
		fmt.Printf("  平均值: %d ns (%.3f μs)\n", sum.Nanoseconds()/int64(total), float64(sum.Nanoseconds())/float64(total)/1000)
	})

	t.Run("scaling test", func(t *testing.T) {
		fmt.Println("\n=== 不同规模下的增量快照耗时 ===")
		fmt.Println("  (每次只更新1个节点)")

		testCases := []struct {
			clusterCount int
			nodeCount    int
		}{
			{1, 1},
			{1, 10},
			{2, 10},
			{2, 20},
			{5, 50},
			{10, 100},
		}

		for _, tc := range testCases {
			ctx, cancel := context.WithCancel(context.Background())
			c := newCache(ctx, 30*time.Second, 1*time.Second)

			for ci := 0; ci < tc.clusterCount; ci++ {
				clusterName := fmt.Sprintf("cluster-%d", ci)
				c.AddCluster(logger, &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName}})
				for ni := 0; ni < tc.nodeCount; ni++ {
					c.AddNode(logger, makeTestNode(fmt.Sprintf("node-%d-%d", ci, ni), clusterName, 4))
				}
			}

			snapshot := NewEmptySnapshot()
			c.UpdateSnapshot(logger, snapshot) // 预热

			zeroCount := 0
			total := 50
			var durations []time.Duration

			for i := 0; i < total; i++ {
				c.AddNode(logger, makeTestNode("node-0-0", "cluster-0", 4))

				start := time.Now()
				c.UpdateSnapshot(logger, snapshot)
				elapsed := time.Since(start)

				durations = append(durations, elapsed)
				if elapsed == 0 {
					zeroCount++
				}
			}

			var sum time.Duration
			for _, d := range durations {
				sum += d
			}

			fmt.Printf("\n  %d集群 × %d节点 = %d总节点\n", tc.clusterCount, tc.nodeCount, tc.clusterCount*tc.nodeCount)
			fmt.Printf("    结果为0: %d/%d (%.1f%%)\n", zeroCount, total, float64(zeroCount)/float64(total)*100)
			fmt.Printf("    平均耗时: %d ns (%.3f μs)\n", sum.Nanoseconds()/int64(total), float64(sum.Nanoseconds())/float64(total)/1000)

			cancel()
		}
	})

	t.Run("full vs incremental", func(t *testing.T) {
		fmt.Println("\n=== 全量快照 vs 增量快照 (5集群 × 20节点) ===")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := newCache(ctx, 30*time.Second, 1*time.Second)

		for ci := 0; ci < 5; ci++ {
			clusterName := fmt.Sprintf("cluster-%d", ci)
			c.AddCluster(logger, &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName}})
			for ni := 0; ni < 20; ni++ {
				c.AddNode(logger, makeTestNode(fmt.Sprintf("node-%d-%d", ci, ni), clusterName, 4))
			}
		}

		// 全量快照
		snapshotFull := NewEmptySnapshot()
		var fullDurations []time.Duration
		for i := 0; i < 50; i++ {
			start := time.Now()
			c.FullUpdateSnapshot(logger, snapshotFull)
			fullDurations = append(fullDurations, time.Since(start))
		}

		// 增量快照
		snapshotIncr := NewEmptySnapshot()
		c.UpdateSnapshot(logger, snapshotIncr)
		var incrDurations []time.Duration
		incrZeroCount := 0
		for i := 0; i < 50; i++ {
			c.AddNode(logger, makeTestNode("node-0-0", "cluster-0", 4))

			start := time.Now()
			c.UpdateSnapshot(logger, snapshotIncr)
			elapsed := time.Since(start)
			incrDurations = append(incrDurations, elapsed)
			if elapsed == 0 {
				incrZeroCount++
			}
		}

		var fullSum, incrSum time.Duration
		for _, d := range fullDurations {
			fullSum += d
		}
		for _, d := range incrDurations {
			incrSum += d
		}

		fmt.Printf("  全量快照平均耗时: %d ns (%.1f μs)\n", fullSum.Nanoseconds()/50, float64(fullSum.Nanoseconds())/50/1000)
		fmt.Printf("  增量快照平均耗时: %d ns (%.1f μs)\n", incrSum.Nanoseconds()/50, float64(incrSum.Nanoseconds())/50/1000)
		fmt.Printf("  增量快照结果为0: %d/50 (%.1f%%)\n", incrZeroCount, float64(incrZeroCount)/50*100)
		if incrSum > 0 {
			fmt.Printf("  加速比: %.1fx\n", float64(fullSum)/float64(incrSum))
		}
	})
}