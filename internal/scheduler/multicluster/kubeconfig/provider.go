package kubeconfig

import (
	"context"
	"fmt"

	"github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
)

const (
	// KEYSecretDataCaBundle 是 Secret 中存储 CA 证书的 Key
	KEYSecretDataCaBundle = "caBundle"
	// KEYSecretDataToken 是 Secret 中存储 ServiceAccount Token 的 Key
	KEYSecretDataToken = "token"
)

type SecretKubeconfigProvider struct {
	k8sClient kubernetes.Interface
	logger    *zap.Logger
}

// NewSecretKubeconfigProvider 创建一个新的基于 Secret 的凭证提供者
func NewSecretKubeconfigProvider(k8sClient kubernetes.Interface, logger *zap.Logger) KubeconfigProvider {
	return &SecretKubeconfigProvider{
		k8sClient: k8sClient,
		logger:    logger.With(zap.String("component", "kubeconfig_provider")),
	}
}

// GetKubeconfig 实现了完整的工作流：获取 Secret -> 解析数据 -> 构建 Config 对象 -> 序列化
func (p *SecretKubeconfigProvider) GetKubeconfig(ctx context.Context, cluster *v1alpha1.Cluster) ([]byte, error) {
	if cluster.Spec.SecretRef == nil {
		return nil, fmt.Errorf("cluster %s has no secretRef defined", cluster.Name)
	}

	// 1. 从控制面获取 Secret 资源
	secret, err := p.getKubeconfigSecret(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get secret [%s/%s] for cluster %s: %w",
			cluster.Spec.SecretRef.Namespace, cluster.Spec.SecretRef.Name, cluster.Name, err)
	}

	// 2. 从 Secret 中解析出关键验证信息
	caBundle, token, err := p.parseClusterAuthFromSecret(secret)
	if err != nil {
		return nil, fmt.Errorf("failed to parse auth data from secret for cluster %s: %w", cluster.Name, err)
	}

	// 3. 构造并序列化 kubeconfig 字节流
	kubeconfig, err := p.buildKubeconfigFromToken(cluster, caBundle, token)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig string for cluster %s: %w", cluster.Name, err)
	}

	return kubeconfig, nil
}

// getKubeconfigSecret 封装了原生的 K8s Client-go 调用
func (p *SecretKubeconfigProvider) getKubeconfigSecret(ctx context.Context, cluster *v1alpha1.Cluster) (*corev1.Secret, error) {
	return p.k8sClient.CoreV1().Secrets(cluster.Spec.SecretRef.Namespace).Get(ctx, cluster.Spec.SecretRef.Name, metav1.GetOptions{})
}

// parseClusterAuthFromSecret 负责校验 Secret 数据结构的正确性
func (p *SecretKubeconfigProvider) parseClusterAuthFromSecret(secret *corev1.Secret) ([]byte, []byte, error) {
	caBundle, caExist := secret.Data[KEYSecretDataCaBundle]
	if !caExist || len(caBundle) == 0 {
		return nil, nil, fmt.Errorf("caBundle is missing or empty in secret %s", secret.Name)
	}

	token, tokenExist := secret.Data[KEYSecretDataToken]
	if !tokenExist || len(token) == 0 {
		return nil, nil, fmt.Errorf("token is missing or empty in secret %s", secret.Name)
	}

	return caBundle, token, nil
}

// buildKubeconfigFromToken 使用内存中的 api.Config 对象动态生成标准的 kubeconfig 文件内容
func (p *SecretKubeconfigProvider) buildKubeconfigFromToken(cluster *v1alpha1.Cluster, caBundle, token []byte) ([]byte, error) {
	config := api.Config{
		APIVersion: "v1",
		Kind:       "Config",
		Clusters: map[string]*api.Cluster{
			cluster.Name: {
				Server:                   cluster.Spec.APIEndpoint,
				CertificateAuthorityData: caBundle,
			},
		},
		AuthInfos: map[string]*api.AuthInfo{
			cluster.Name: {
				Token: string(token),
			},
		},
		Contexts: map[string]*api.Context{
			cluster.Name: {
				Cluster:  cluster.Name,
				AuthInfo: cluster.Name,
			},
		},
		CurrentContext: cluster.Name,
	}

	// 将内存对象序列化为 YAML 字节数组
	return clientcmd.Write(config)
}
