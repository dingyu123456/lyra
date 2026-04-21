# KWOK 速查手册

KWOK (Kubernetes WithOut Kubelet) 是一个轻量级 Kubernetes 集群模拟工具，可以在单机上模拟数千个节点和 Pod。

## 一、安装

### Linux (amd64/arm64)
```bash
# 下载最新版本
KWOK_VERSION=$(curl -s https://raw.githubusercontent.com/kubernetes-sigs/kwok/main/VERSION)

# 安装 kwokctl
curl -Lo kwokctl https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/kwokctl-$(go env GOOS)-$(go env GOARCH)
chmod +x kwokctl
sudo mv kwokctl /usr/local/bin/

# 安装 kwok (可选,用于 out-of-cluster 模式)
curl -Lo kwok https://github.com/kubernetes-sigs/kwok/releases/download/${KWOK_VERSION}/kwok-$(go env GOOS)-$(go env GOARCH)
chmod +x kwok
sudo mv kwok /usr/local/bin/
```

### 验证安装
```bash
kwokctl --version
kwok --version
```

---

## 二、集群管理 (kwokctl)

### 创建集群
```bash
# 创建默认集群 (名称: kwok)
kwokctl create cluster

# 创建命名集群
kwokctl create cluster --name=cluster-1

# 指定 Kubernetes 版本
kwokctl create cluster --name=cluster-1 --kube-version=v1.28.0

# 创建集群并设置节点数
kwokctl create cluster --name=cluster-1 --nodes 10

# 使用配置文件创建
kwokctl create cluster --name=cluster-1 --config=kwok.yaml

# 指定运行时 (binary/docker/podman/kind)
kwokctl create cluster --name=cluster-1 --runtime=docker

# 创建带端口映射的集群 (用于 API Server 访问)
kwokctl create cluster --name=cluster-1 --kube-apiserver-port=32765
```

### 查看集群
```bash
# 列出所有集群
kwokctl get clusters

# 查看集群状态
kwokctl get cluster --name=cluster-1
```

### 切换 kubectl 上下文
```bash
# 切换到指定集群
kubectl config use-context kwok-cluster-1

# 查看当前上下文
kubectl config current-context
```

### 导出 kubeconfig
```bash
# 导出指定集群的 kubeconfig
kwokctl get kubeconfig --name=cluster-1 > ~/.kube/config-cluster-1
```

### 删除集群
```bash
# 删除指定集群
kwokctl delete cluster --name=cluster-1

# 删除所有集群
kwokctl delete cluster --all
```

### 启动/停止集群
```bash
# 启动集群
kwokctl start cluster --name=cluster-1

# 停止集群
kwokctl stop cluster --name=cluster-1
```

---

## 三、节点管理

### 创建模拟节点 (通过 kubectl)
```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Node
metadata:
  annotations:
    kwok.x-k8s.io/node: fake
  labels:
    type: kwok
    kubernetes.io/arch: amd64
    kubernetes.io/os: linux
  name: kwok-node-0
spec:
  taints:
  - effect: NoSchedule
    key: kwok.x-k8s.io/node
    value: fake
status:
  allocatable:
    cpu: "32"
    memory: 256Gi
    pods: "110"
  capacity:
    cpu: "32"
    memory: 256Gi
    pods: "110"
  nodeInfo:
    architecture: amd64
    kubeletVersion: fake
    operatingSystem: linux
  phase: Running
EOF
```

### 批量创建节点脚本
```bash
#!/bin/bash
# create_nodes.sh - 创建指定数量的模拟节点

NODE_COUNT=${1:-10}
CLUSTER_LABEL=${2:-kwok}

for i in $(seq 0 $((NODE_COUNT-1))); do
  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Node
metadata:
  annotations:
    kwok.x-k8s.io/node: fake
  labels:
    type: kwok
    cluster: ${CLUSTER_LABEL}
    kubernetes.io/arch: amd64
    kubernetes.io/os: linux
  name: kwok-node-${i}
spec:
  taints:
  - effect: NoSchedule
    key: kwok.x-k8s.io/node
    value: fake
status:
  allocatable:
    cpu: "64"
    memory: 512Gi
    pods: "200"
  capacity:
    cpu: "64"
    memory: 512Gi
    pods: "200"
  nodeInfo:
    architecture: amd64
    kubeletVersion: fake
    operatingSystem: linux
  phase: Running
EOF
done
```

### 创建带 GPU 注解的节点 (模拟 HAMI GPU)
```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Node
metadata:
  annotations:
    kwok.x-k8s.io/node: fake
    # HAMI GPU 注解格式 (需要根据实际 HAMI 格式调整)
    hami.io/node-gpu: "GPU-0,NVIDIA-A100-80GB,80GB,100,10:GPU-1,NVIDIA-A100-80GB,80GB,100,10"
  labels:
    type: kwok
    gpu-node: "true"
  name: gpu-node-0
spec:
  taints:
  - effect: NoSchedule
    key: kwok.x-k8s.io/node
    value: fake
status:
  allocatable:
    cpu: "64"
    memory: 512Gi
    pods: "200"
    nvidia.com/gpu: "8"
  capacity:
    cpu: "64"
    memory: 512Gi
    pods: "200"
    nvidia.com/gpu: "8"
EOF
```

### 查看节点
```bash
kubectl get nodes -o wide
kubectl get nodes -l type=kwok
kubectl describe node kwok-node-0
```

### 删除节点
```bash
kubectl delete node kwok-node-0
kubectl delete nodes -l type=kwok
```

---

## 四、Pod 管理

### 创建模拟 Pod
```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: fake-pod
  namespace: default
spec:
  schedulerName: lyra-scheduler  # 使用 Lyra 调度器
  containers:
  - name: fake-container
    image: fake-image
    resources:
      requests:
        cpu: "1"
        memory: 1Gi
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
  nodeSelector:
    type: kwok
EOF
```

### 批量创建 Pod
```bash
#!/bin/bash
# create_pods.sh - 批量创建测试 Pod

POD_COUNT=${1:-10}
NAMESPACE=${2:-default}

for i in $(seq 1 $POD_COUNT); do
  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: test-pod-${i}
  namespace: ${NAMESPACE}
  labels:
    app: test-pod
    batch: "batch-1"
spec:
  schedulerName: lyra-scheduler
  containers:
  - name: fake-container
    image: fake-image
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
EOF
done
```

### 使用 Deployment 批量创建
```bash
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fake-deployment
  namespace: default
spec:
  replicas: 100
  selector:
    matchLabels:
      app: fake-pod
  template:
    metadata:
      labels:
        app: fake-pod
    spec:
      schedulerName: lyra-scheduler
      containers:
      - name: fake-container
        image: fake-image
        resources:
          requests:
            cpu: "500m"
            memory: "512Mi"
      tolerations:
      - key: "kwok.x-k8s.io/node"
        operator: "Exists"
        effect: "NoSchedule"
      nodeSelector:
        type: kwok
EOF
```

---

## 五、kwok 运行模式

### In-Cluster 模式 (kwokctl 自动管理)
```bash
# kwokctl 创建集群时会自动部署 kwok 组件
kwokctl create cluster --name=cluster-1
# kwok 作为 Pod 运行在集群中,自动管理模拟节点
```

### Out-of-Cluster 模式
```bash
# 直接运行 kwok 进程,连接外部集群
kwok \
  --kubeconfig=~/.kube/config \
  --manage-all-nodes=false \
  --manage-nodes-with-annotation-selector=kwok.x-k8s.io/node=fake \
  --cidr=10.0.0.1/24 \
  --node-ip=10.0.0.1
```

### 管理节点选择模式
```bash
# 管理所有节点
kwok --manage-all-nodes=true

# 只管理带特定 annotation 的节点
kwok --manage-nodes-with-annotation-selector=kwok.x-k8s.io/node=fake

# 只管理带特定 label 的节点
kwok --manage-nodes-with-label-selector=kwok.x-k8s.io/node=fake

# 只管理单个指定节点
kwok --manage-single-node=fake-node-0
```

---

## 六、配置文件

### 默认配置位置
```bash
~/.kwok/kwok.yaml
```

### 配置示例
```yaml
kind: KwokctlConfiguration
apiVersion: config.kwok.x-k8s.io/v1alpha1
options:
  # 集群配置
  kubeApiserverPort: 32764
  kubeVersion: v1.28.0
  runtime: docker

  # 节点模拟配置
  nodeLeaseDurationSeconds: 40

---
kind: KwokConfiguration
apiVersion: config.kwok.x-k8s.io/v1alpha1
options:
  manageAllNodes: false
  manageNodesWithAnnotationSelector: kwok.x-k8s.io/node=fake
  cidr: 10.0.0.1/24
  nodeIP: 10.0.0.1
```

### 使用配置文件
```bash
kwokctl create cluster --config=kwok.yaml
kwok --config=kwok.yaml
```

---

## 七、性能测试相关

### 创建大规模集群
```bash
# 创建集群并预置大量节点
kwokctl create cluster --name=perf-test --nodes=1000

# 或使用 kubectl 批量创建
for i in $(seq 0 999); do
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Node
metadata:
  annotations:
    kwok.x-k8s.io/node: fake
  name: node-$i
status:
  allocatable:
    cpu: "64"
    memory: 256Gi
    pods: "200"
  capacity:
    cpu: "64"
    memory: 256Gi
    pods: "200"
EOF
done
```

### 监控指标
```bash
# 查看集群资源使用
kubectl top nodes
kubectl describe nodes | grep -A 5 "Allocated resources"

# 查看 Pod 状态
kubectl get pods --all-namespaces -o wide
kubectl get pods --field-selector=status.phase=Running
```

---

## 八、故障排查

### 常见问题

1. **节点一直 NotReady**
   ```bash
   # 检查 kwok 是否正常运行
   kubectl get pods -n kube-system | grep kwok

   # 检查节点 annotation
   kubectl get node <node-name> -o yaml | grep annotations -A 5
   ```

2. **Pod 一直 Pending**
   ```bash
   # 检查调度器名称是否正确
   kubectl get pod <pod-name> -o yaml | grep schedulerName

   # 检查节点污点和容忍
   kubectl describe node <node-name> | grep Taints
   kubectl describe pod <pod-name> | grep Tolerations
   ```

3. **清理环境**
   ```bash
   # 删除所有模拟节点
   kubectl delete nodes -l type=kwok

   # 删除所有测试 Pod
   kubectl delete pods --all -n default

   # 完全删除集群
   kwokctl delete cluster --name=cluster-1
   ```

---

## 九、与 Karmada 集成

### 将 KWOK 集群加入 Karmada
```bash
# 1. 导出 KWOK 集群的 kubeconfig
kwokctl get kubeconfig --name=cluster-1 > /tmp/cluster-1.kubeconfig

# 2. 使用 karmadactl 加入集群
karmadactl join cluster-1 --cluster-kubeconfig=/tmp/cluster-1.kubeconfig --kubeconfig=<karmada-kubeconfig>

# 3. 验证加入成功
kubectl --kubeconfig=<karmada-kubeconfig> get clusters
```

### 从 Karmada 移除集群
```bash
# 移除集群
kubectl --kubeconfig=<karmada-kubeconfig> delete cluster cluster-1
```

---

## 十、快速脚本集合

### 一键创建测试集群并加入 Karmada
```bash
#!/bin/bash
# setup_test_cluster.sh

CLUSTER_NAME=$1
KARMADA_KUBECONFIG=$2
NODE_COUNT=${3:-100}

# 创建 KWOK 集群
kwokctl create cluster --name=${CLUSTER_NAME} --nodes=0

# 获取 kubeconfig
kwokctl get kubeconfig --name=${CLUSTER_NAME} > /tmp/${CLUSTER_NAME}.kubeconfig

# 切换上下文
kubectl config use-context kwok-${CLUSTER_NAME}

# 批量创建节点
for i in $(seq 0 $((NODE_COUNT-1))); do
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Node
metadata:
  annotations:
    kwok.x-k8s.io/node: fake
  labels:
    type: kwok
    cluster: ${CLUSTER_NAME}
  name: ${CLUSTER_NAME}-node-${i}
spec:
  taints:
  - effect: NoSchedule
    key: kwok.x-k8s.io/node
    value: fake
status:
  allocatable:
    cpu: "64"
    memory: 512Gi
    pods: "200"
  capacity:
    cpu: "64"
    memory: 512Gi
    pods: "200"
EOF
done

# 加入 Karmada
karmadactl join ${CLUSTER_NAME} --cluster-kubeconfig=/tmp/${CLUSTER_NAME}.kubeconfig --kubeconfig=${KARMADA_KUBECONFIG}

echo "Cluster ${CLUSTER_NAME} created with ${NODE_COUNT} nodes and joined to Karmada"
```

### 一键清理测试环境
```bash
#!/bin/bash
# cleanup_test_cluster.sh

CLUSTER_NAME=$1
KARMADA_KUBECONFIG=$2

# 从 Karmada 移除
kubectl --kubeconfig=${KARMADA_KUBECONFIG} delete cluster ${CLUSTER_NAME} --ignore-not-found

# 删除 KWOK 集群
kwokctl delete cluster --name=${CLUSTER_NAME}

# 清理临时文件
rm -f /tmp/${CLUSTER_NAME}.kubeconfig

echo "Cluster ${CLUSTER_NAME} cleaned up"
```
