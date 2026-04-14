package dispatcher

import (
	"context"
	"fmt"
	"strings"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// dispatcherImpl 是真实实现
type dispatcherImpl struct {
	kubeClient    kubernetes.Interface
	karmadaClient karmadaclientset.Interface
	logger        *zap.Logger
}

// NewDispatcher 构造函数
func NewDispatcher(kubeClient kubernetes.Interface, karmadaClient karmadaclientset.Interface, logger *zap.Logger) Dispatcher {
	return &dispatcherImpl{
		kubeClient:    kubeClient,
		karmadaClient: karmadaClient,
		logger:        logger.With(zap.String("component", "dispatcher")),
	}
}

// Dispatch 执行联邦下发逻辑：创建 PP 和 OP
func (d *dispatcherImpl) Dispatch(ctx context.Context, pod *corev1.Pod, result framework.ScheduleResult) error {
	// 1. 创建 PropagationPolicy (PP)
	// 作用：将 Pod 和 具有相同 ID 标签的配套资源（如 Service）分发到目标集群
	if err := d.ensurePropagationPolicy(ctx, pod, result.SuggestedCluster); err != nil {
		return fmt.Errorf("ensure propagation policy failed: %w", err)
	}

	// 2. 创建 OverridePolicy (OP)
	// 作用：注入特定的 NodeName 和 GPU UUID 信息
	if err := d.ensureOverridePolicy(ctx, pod, result); err != nil {
		return fmt.Errorf("ensure override policy failed: %w", err)
	}

	d.logger.Info("Successfully dispatched pod and policies",
		zap.String("pod", pod.Name),
		zap.String("cluster", result.SuggestedCluster))
	return nil
}

// TearDown 撤销下发逻辑：清理 PP 和 OP
func (d *dispatcherImpl) TearDown(ctx context.Context, pod *corev1.Pod) error {
	ppName := fmt.Sprintf("%s-pp", pod.Name)
	opName := fmt.Sprintf("%s-op", pod.Name)

	// 使用 Background 确保清理动作不受原 context 取消的影响
	deleteCtx := context.Background()

	_ = d.karmadaClient.PolicyV1alpha1().PropagationPolicies(pod.Namespace).Delete(deleteCtx, ppName, metav1.DeleteOptions{})
	_ = d.karmadaClient.PolicyV1alpha1().OverridePolicies(pod.Namespace).Delete(deleteCtx, opName, metav1.DeleteOptions{})

	d.logger.Info("Cleanup policies for pod", zap.String("pod", pod.Name))
	return nil
}

func (d *dispatcherImpl) ensurePropagationPolicy(ctx context.Context, pod *corev1.Pod, cluster string) error {
	ppName := fmt.Sprintf("%s-pp", pod.Name)

	// 尝试匹配具有相同业务 ID 的所有资源（例如 Service, ConfigMap）
	// 假设我们在提交任务时为配套资源都打上了 lyra-task-id 标签
	taskID := pod.Labels["lyra-task-id"]

	pp := &policyv1alpha1.PropagationPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ppName,
			Namespace: pod.Namespace,
		},
		Spec: policyv1alpha1.PropagationSpec{
			ResourceSelectors: []policyv1alpha1.ResourceSelector{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       pod.Name,
				},
			},
			Placement: policyv1alpha1.Placement{
				ClusterAffinity: &policyv1alpha1.ClusterAffinity{
					ClusterNames: []string{cluster},
				},
			},
		},
	}

	// 如果有任务 ID，增加标签选择器，实现配套资源一并下发
	if taskID != "" {
		pp.Spec.ResourceSelectors = append(pp.Spec.ResourceSelectors, policyv1alpha1.ResourceSelector{
			APIVersion: "v1",
			Kind:       "Service",
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"lyra-task-id": taskID},
			},
		})
	}

	_, err := d.karmadaClient.PolicyV1alpha1().PropagationPolicies(pod.Namespace).Create(ctx, pp, metav1.CreateOptions{})
	return err
}

// internal/scheduler/dispatcher/dispatcher.go

func (d *dispatcherImpl) ensureOverridePolicy(ctx context.Context, pod *corev1.Pod, result framework.ScheduleResult) error {
	opName := fmt.Sprintf("%s-op", pod.Name)

	// 1. 获取要告诉 Hami 的目标 GPU UUID 字符串 (如 "GPU-123,GPU-456")
	gpuTargetUUIDs := framework.FormatHamiVGPUAnnotation(result.SuggestedGPUs)

	// 2. 构造 PlaintextOverriders，只修改下发途中的镜像，不碰原始 Pod
	overriders := []policyv1alpha1.PlaintextOverrider{
		{
			Path:     "/spec/nodeName",
			Operator: policyv1alpha1.OverriderOpAdd,
			Value:    apiextensionsv1.JSON{Raw: []byte(fmt.Sprintf("%q", result.SuggestedNode))},
		},
	}

	// 只有当分配了具体的 GPU 时，才在 OP 中追加 GPU 绑定指令
	if gpuTargetUUIDs != "" {
		// 注意：JSON Patch 中路径的 '/' 必须转义为 '~1'
		gpuAnnotationPath := fmt.Sprintf("/metadata/annotations/%s",
			strings.ReplaceAll(framework.AnnotationUseGPUUUID, "/", "~1"))

		overriders = append(overriders, policyv1alpha1.PlaintextOverrider{
			Path:     gpuAnnotationPath,
			Operator: policyv1alpha1.OverriderOpAdd,
			Value:    apiextensionsv1.JSON{Raw: []byte(fmt.Sprintf("%q", gpuTargetUUIDs))},
		})
	}

	op := &policyv1alpha1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opName,
			Namespace: pod.Namespace,
		},
		Spec: policyv1alpha1.OverrideSpec{
			ResourceSelectors: []policyv1alpha1.ResourceSelector{
				{
					APIVersion: "v1",
					Kind:       "Pod",
					Name:       pod.Name,
				},
			},
			OverrideRules: []policyv1alpha1.RuleWithCluster{
				{
					TargetCluster: &policyv1alpha1.ClusterAffinity{
						ClusterNames: []string{result.SuggestedCluster},
					},
					Overriders: policyv1alpha1.Overriders{
						Plaintext: overriders,
					},
				},
			},
		},
	}

	_, err := d.karmadaClient.PolicyV1alpha1().OverridePolicies(pod.Namespace).Create(ctx, op, metav1.CreateOptions{})
	return err
}
