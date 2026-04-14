package parallelize

import (
	"context"
	"math"

	"k8s.io/client-go/util/workqueue"
)

// DefaultParallelism 是调度器使用的默认并行度
const DefaultParallelism int = 16

// Parallelizer 持有调度器的并行配置
type Parallelizer struct {
	parallelism int
}

// NewParallelizer 返回一个持有并行度的对象
func NewParallelizer(p int) Parallelizer {
	return Parallelizer{parallelism: p}
}

// chunkSizeFor 计算并行工作时的块大小，以获得良好的 CPU 利用率
func chunkSizeFor(n, parallelism int) int {
	s := int(math.Sqrt(float64(n)))

	if r := n/parallelism + 1; s > r {
		s = r
	} else if s < 1 {
		s = 1
	}
	return s
}

// Until 是对 workqueue.ParallelizeUntil 的封装
// 移除了 operation 参数和 metrics 埋点逻辑，保持调度核心纯净
func (p Parallelizer) Until(ctx context.Context, pieces int, doWorkPiece workqueue.DoWorkPieceFunc) {
	// 直接调用底层的并行处理逻辑，不再包装 metrics 闭包
	workqueue.ParallelizeUntil(
		ctx,
		p.parallelism,
		pieces,
		doWorkPiece,
		workqueue.WithChunkSize(chunkSizeFor(pieces, p.parallelism)),
	)
}
