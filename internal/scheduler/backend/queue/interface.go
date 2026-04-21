package queue

import (
	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type SchedulingQueue interface {
	Run(logger *zap.Logger)
	Close()

	// --- 核心流转 ---
	Pop(logger *zap.Logger) (*framework.QueuedPodInfo, error)
	Done(podUID types.UID)

	// --- 任务入队与唤醒 (K8s 原生完整版) ---
	Add(logger *zap.Logger, pod *corev1.Pod)

	// Activate moves the given pods to activeQ.
	// If a pod isn't found in unschedulablePods or backoffQ and it's in-flight,
	// the wildcard event is registered so that the pod will be requeued when it comes back.
	// But, if a pod isn't found in unschedulablePods or backoffQ and it's not in-flight (i.e., completely unknown pod),
	// Activate would ignore the pod.
	Activate(logger *zap.Logger, pods map[string]*corev1.Pod)

	AddUnschedulableIfNotPresent(logger *zap.Logger, pod *framework.QueuedPodInfo, podSchedulingCycle int64) error

	SchedulingCycle() int64

	// --- 事件监听与更新 ---
	Update(logger *zap.Logger, oldPod, newPod *corev1.Pod)
	Delete(pod *corev1.Pod)

	//AssignedPodAdded(logger *zap.Logger, pod *corev1.Pod)
	//AssignedPodUpdated(logger *zap.Logger, oldPod, newPod *corev1.Pod, event framework.ClusterEvent)

	// MoveAllToActiveOrBackoffQueue 核心事件驱动唤醒
	MoveAllToActiveOrBackoffQueue(logger *zap.Logger, event framework.ClusterEvent, oldObj, newObj interface{}, preCheck PreEnqueueCheck)

	// --- 调试与 Metrics (极其有助于排障) ---
	GetPod(name, namespace string) (*framework.QueuedPodInfo, bool)
	//PendingPods() ([]*corev1.Pod, string)
	//InFlightPods() []*corev1.Pod
	PodsInActiveQ() []*corev1.Pod

	// --- QueueingHint 配置 ---
	// SetQueueingHintMap 设置事件到 hint 函数的映射（从插件的 EventsToRegister 收集）
	// key: 事件类型, value: 插件名 + hint函数 的列表
	SetQueueingHintMap(hintMap map[framework.ClusterEvent][]framework.ClusterEventWithPluginHint)
}
