package karmadabind

import (
	"context"

	"github.com/dingyu123456/lyra/internal/scheduler/dispatcher"
	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const Name = "KarmadaBind"

// KarmadaBind 插件负责将调度决策转化为 Karmada 的分发策略
type KarmadaBind struct {
	dispatcher dispatcher.Dispatcher
}

var _ framework.BindPlugin = &KarmadaBind{}

// New 初始化插件并创建 Dispatcher
func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	// 直接利用 Handle 提供的 Client 构造 Dispatcher
	d := dispatcher.NewDispatcher(h.ClientSet(), h.KarmadaClient(), h.Logger())
	return &KarmadaBind{dispatcher: d}, nil
}

func (k *KarmadaBind) Name() string {
	return Name
}

// Bind 执行真正的策略下发动作
func (k *KarmadaBind) Bind(ctx context.Context, state *framework.CycleState, pod *corev1.Pod, result framework.ScheduleResult) *framework.Status {
	// 调用 Dispatcher 执行联邦策略下发
	// 这里的 Dispatch 会创建 PP 和 OP，从而触发子集群的 Pod 创建
	if err := k.dispatcher.Dispatch(ctx, pod, result); err != nil {
		return framework.AsStatus(err)
	}

	return framework.NewStatus(framework.Success)
}
