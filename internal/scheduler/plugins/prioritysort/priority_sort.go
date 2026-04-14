package prioritysort

import (
	"context"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const Name = "PrioritySort"

// PrioritySort 是一个简单的队列排序插件，基于 Pod 优先级和创建时间排序
type PrioritySort struct{}

var _ framework.QueueSortPlugin = &PrioritySort{}

// New 初始化插件
func New(_ context.Context, _ runtime.Object, _ framework.Handle) (framework.Plugin, error) {
	return &PrioritySort{}, nil
}

func (p *PrioritySort) Name() string {
	return Name
}

// getPodPriority 安全地获取 Pod 的优先级
func getPodPriority(pod *corev1.Pod) int32 {
	if pod != nil && pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	return 0
}

// Less 决定了活跃队列中 Pod 的出队顺序
// 返回 true 表示 pInfo1 应该排在 pInfo2 的前面（优先被调度）
func (p *PrioritySort) Less(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
	prio1 := getPodPriority(pInfo1.Pod)
	prio2 := getPodPriority(pInfo2.Pod)

	if prio1 != prio2 {
		// 1. 绝对优先级比较：数值越大，优先级越高，越早出队
		return prio1 > prio2
	}

	// 2. 相同优先级下的公平性保证 (FIFO)
	// Timestamp 记录了 Pod 被加入调度队列的时间。加入越早，越优先。
	return pInfo1.Timestamp.Before(pInfo2.Timestamp)
}