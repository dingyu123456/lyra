package kubeconfig

import (
	"context"

	"github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
)

// KubeconfigProvider 定义了如何获取子集群访问凭证的接口
type KubeconfigProvider interface {
	// GetKubeconfig 根据 Karmada Cluster 资源提取并构造 Kubeconfig 字节流
	GetKubeconfig(ctx context.Context, cluster *v1alpha1.Cluster) ([]byte, error)
}
