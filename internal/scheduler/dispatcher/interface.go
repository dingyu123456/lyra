package dispatcher

import (
	"context"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	corev1 "k8s.io/api/core/v1"
)

// Dispatcher 定义了下发契约
type Dispatcher interface {
	Dispatch(ctx context.Context, pod *corev1.Pod, result framework.ScheduleResult) error
	TearDown(ctx context.Context, pod *corev1.Pod) error
}
