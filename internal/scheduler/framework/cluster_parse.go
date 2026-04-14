package framework

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	AnnotationKarmadaNamespace = "work.karmada.io/namespace"
	AnnotationTargetCluster    = "lyra.io/target-cluster"
)

func InjectKarmadaNamespace(pod *corev1.Pod, clusterName string) {
	karmadaNs := fmt.Sprintf("karmada-es-%s", clusterName)
	pod.Annotations[AnnotationKarmadaNamespace] = karmadaNs
}

// GetClusterNameFromPod 负责精准提取 Pod 所属的集群
func GetClusterNameFromPod(pod *corev1.Pod) string {
	// 1. 尝试获取 Karmada 下发的调度决策信息 (适用于预扣阶段和已绑定的 AI 任务)
	if ns, ok := pod.Annotations[AnnotationKarmadaNamespace]; ok && strings.HasPrefix(ns, "karmada-es-") {
		// Karmada 规则：如果 ns 是 karmada-es-cluster1，提取出 cluster1
		return strings.TrimPrefix(ns, "karmada-es-")
	}

	// TODO 这里得梳理梳理，要能讲清楚
	// 2. 尝试获取我们自定义预扣的 Annotation (适用于没有通过karmada下发的pod，比如Calico, kube-proxy)
	// 当pod事件触发，发现GetClusterName（）返回“”，说明该pod不是通过karmada下发的，
	// 因为这个pod的informer是该子集群创建的，所以这个子集群的名字肯定能获取到，先深拷贝pod，然后将集群名字写到注解即可。
	// 这样我们的调度器缓存就可以扣除所有pod的已使用资源（包括不从karmada下发或者不适用lyra调度的pod）
	// AnnotationTargetCluster = "lyra.io/target-cluster"
	if cn, ok := pod.Annotations[AnnotationTargetCluster]; ok {
		return cn
	}
	return "" // 真的找不到，说明架构层面漏了打标
}

// GetClusterNameFromNode 从 Node 的注解中提取其所属的集群名称。
// 依赖：在各子集群的 Node Informer EventHandler 中，必须在调用 Cache 之前完成打标。
func GetClusterNameFromNode(node *corev1.Node) string {
	if cn, ok := node.Annotations[AnnotationTargetCluster]; ok {
		return cn
	}
	return ""
}
