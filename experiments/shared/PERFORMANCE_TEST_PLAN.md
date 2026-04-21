# Lyra 调度器性能测试方案

> **注意**: 本文档描述的是初始设计方案。实际脚本已优化，请参考 `experiments/snapshot/README.md` 获取最新用法：
> - `deploy_fast.sh` — 并行部署，2000节点约4.5分钟
> - `cleanup_fast.sh` — 并行清理，20集群约35秒
> - `run_full_perf_test.sh` — 全面测试（含背景事件模拟）
> - `simulate_events_remote.sh` — 模拟集群背景事件

## 一、测试环境

### 1.1 服务器信息

**KWOK 服务器（用于创建虚拟集群和节点）：**

| 用途 | IP | 用户名 | 密码 | 操作系统 | KWOK版本 |
|------|-----|--------|------|---------|----------|
| KWOK服务器1 | 10.10.100.5 | root | cnic123456 | Ubuntu 20.04 | v0.7.0 |
| KWOK服务器2 | 10.10.100.9 | root | cnic123456 | Ubuntu 20.04 | v0.7.0 |

**真实集群服务器（Karmada 成员集群）：**

| 集群名称 | 说明 | GPU | Kubeconfig |
|---------|------|-----|------------|
| karmada | Karmada 控制面部署在此集群，同时它也是成员集群 | 无 | `experiments/shared/kubeconfig/karmada_kubeconfig/config` |
| cluster1 | 真实成员集群 | 3 张 GTX 1660 SUPER | `experiments/shared/kubeconfig/cluster1_kubeconfig/config` |
| cluster2 | 真实成员集群 | 1 张 GPU | `experiments/shared/kubeconfig/cluster2_kubeconfig/config` |

**Kubeconfig 说明：**
- `kubeconfig/karmada-apiserver.config` — Karmada 控制面 kubeconfig，Lyra 调度器通过此配置连接 Karmada APIServer
- `experiments/shared/kubeconfig/` — 三个真实成员集群的 kubeconfig，用于调试和排查问题
- KWOK 虚拟集群（kwok-cluster-0 ~ kwok-cluster-N）的 kubeconfig 存储在远程 KWOK 服务器上，由 deploy.sh 通过 ssh 导出并注入到 Karmada

### 1.2 网络拓扑

```
                         ┌──────────────────────────────────────────┐
                         │           Lyra 调度器 (本地 Windows)      │
                         │   kubeconfig/karmada-apiserver.config     │
                         └──────────────────┬───────────────────────┘
                                            │
                                            ▼
               ┌────────────────────────────────────────────────┐
               │              Karmada 控制面                     │
               │          (部署在 karmada 集群上)                │
               └────────────────────────────────────────────────┘
                                            │
          ┌────────────┬────────────┬───────┴───────────────────────┐
          │            │            │                               │
          ▼            ▼            ▼                               ▼
  ┌──────────────┐ ┌──────────┐ ┌──────────┐         ┌───────────────────────┐
  │   karmada    │ │ cluster1 │ │ cluster2 │         │  KWOK 虚拟集群         │
  │  (真实集群)   │ │(真实集群) │ │(真实集群) │         │  kwok-cluster-0~N     │
  │   无GPU      │ │ 3张GPU   │ │ 1张GPU   │         │  由KWOK服务器创建       │
  └──────────────┘ └──────────┘ └──────────┘         └───────┬───────────────┘
                                                            │
                                              ┌─────────────┼─────────────┐
                                              │             │             │
                                              ▼             ▼             │
                                     ┌──────────────┐ ┌──────────────┐  │
                                     │ KWOK 服务器1  │ │ KWOK 服务器2  │  │
                                     │ 10.10.100.5  │ │ 10.10.100.9  │  │
                                     │ 集群 0 ~ N-1 │ │ 集群 N ~ 2N-1│  │
                                     └──────────────┘ └──────────────┘  │
                                              └─────────────────────────┘
```

---

## 二、测试矩阵

### 2.1 规模档位

| 档位 | 集群数 | 节点数/集群 | 总节点数 | GPU数/节点 | 总GPU数 | Pod并发数 |
|------|--------|------------|---------|-----------|--------|----------|
| 小规模 | 2 | 10 | 20 | 4 | 80 | 50 |
| 中规模 | 4 | 50 | 200 | 4 | 800 | 500 |
| 大规模 | 10 | 100 | 1000 | 4 | 4000 | 1000 |
| 极限规模 | 20 | 100 | 2000 | 4 | 8000 | 2000 |

### 2.2 测试场景

| 场景 | 描述 | 用途 |
|------|------|------|
| 静态场景 | 无背景事件，测试快照基础性能 | 基线对比 |
| 动态场景 | 模拟节点标签更新（1~N events/s） | 模拟真实生产环境 |
| 压力场景 | 高Pod提交速率 + 高背景事件 | 极限性能测试 |

### 2.3 测试维度

| 测试项 | 全量快照 | 增量快照 | 说明 |
|--------|---------|---------|------|
| 快照更新时间 | ✓ | ✓ | 核心对比指标 |
| 克隆节点数 | ✗ | ✓ | 增量效果验证 |
| Filter阶段耗时 | ✓ | ✓ | 并发过滤节点 |
| Score阶段耗时 | ✓ | ✓ | 并发打分 |
| GPU分配耗时 | ✓ | ✓ | Best-Fit装箱 |
| 端到端延迟 | ✓ | ✓ | 从入队到下发 |
| 吞吐量 | ✓ | ✓ | Pods/秒 |

---

## 三、环境准备脚本

### 3.1 在两台服务器上创建KWOK集群

**脚本: `scripts/01_create_kwok_clusters.sh`**

```bash
#!/bin/bash
# 在两台KWOK服务器上创建模拟集群
# 用法: ./01_create_kwok_clusters.sh <server_ip> <start_index> <cluster_count>

SERVER_IP=$1
START_INDEX=$2
CLUSTER_COUNT=$3
SSH_USER="root"
SSH_PASS="cnic123456"

echo "=== 在服务器 ${SERVER_IP} 上创建 ${CLUSTER_COUNT} 个KWOK集群 ==="

for i in $(seq 0 $((CLUSTER_COUNT-1))); do
    CLUSTER_NAME="kwok-cluster-$((START_INDEX + i))"
    echo "创建集群: ${CLUSTER_NAME}"
    
    sshpass -p "${SSH_PASS}" ssh -o StrictHostKeyChecking=no ${SSH_USER}@${SERVER_IP} << EOF
        # 创建集群（不创建默认节点，我们手动创建带GPU的节点）
        kwokctl create cluster --name=${CLUSTER_NAME} --nodes=0 --kube-apiserver-port=$((32000 + START_INDEX + i))
        
        # 等待集群就绪
        sleep 3
        
        # 验证集群
        kwokctl get clusters | grep ${CLUSTER_NAME}
EOF
    
    echo "集群 ${CLUSTER_NAME} 创建完成"
done

echo "=== 服务器 ${SERVER_IP} 集群创建完成 ==="
```

### 3.2 创建带GPU注解的模拟节点

**脚本: `scripts/02_create_gpu_nodes.sh`**

```bash
#!/bin/bash
# 创建带HAMI GPU注解的模拟节点
# 用法: ./02_create_gpu_nodes.sh <server_ip> <cluster_index> <node_count> <gpus_per_node>

SERVER_IP=$1
CLUSTER_INDEX=$2
NODE_COUNT=$3
GPUS_PER_NODE=${4:-4}

SSH_USER="root"
SSH_PASS="cnic123456"
CLUSTER_NAME="kwok-cluster-${CLUSTER_INDEX}"

echo "=== 在集群 ${CLUSTER_NAME} 创建 ${NODE_COUNT} 个GPU节点 ==="

# 生成GPU注解函数
generate_gpu_annotation() {
    local node_idx=$1
    local gpu_count=$2
    local cluster_idx=$3
    local annotation=""
    
    for g in $(seq 0 $((gpu_count-1))); do
        # 生成唯一UUID: GPU-{cluster}-{node}-{gpu}-{random}
        uuid="GPU-c${cluster_idx}n${node_idx}g${g}-$(head -c 8 /dev/urandom | xxd -p)"
        # 格式: GPU-UUID,MaxVGPUs,MemoryTotal(MB),CoreTotal(%),GPU-Type
        # 模拟 NVIDIA A100 80GB: 100 vGPUs, 81920MB显存, 100%算力
        annotation="${annotation}${uuid},100,81920,100,NVIDIA-A100-80GB:"
    done
    echo "${annotation}"
}

# 切换到目标集群的kubeconfig
sshpass -p "${SSH_PASS}" ssh ${SSH_USER}@${SERVER_IP} "kubectl config use-context kwok-${CLUSTER_NAME}"

# 批量创建节点
for n in $(seq 0 $((NODE_COUNT-1))); do
    NODE_NAME="gpu-node-${CLUSTER_INDEX}-${n}"
    GPU_ANNOTATION=$(generate_gpu_annotation $n $GPUS_PER_NODE $CLUSTER_INDEX)
    
    sshpass -p "${SSH_PASS}" ssh ${SSH_USER}@${SERVER_IP} "kubectl apply -f -" << EOF
apiVersion: v1
kind: Node
metadata:
  annotations:
    kwok.x-k8s.io/node: fake
    hami.io/node-nvidia-register: '${GPU_ANNOTATION}'
  labels:
    type: kwok
    gpu-node: "true"
    cluster: kwok-cluster-${CLUSTER_INDEX}
    kubernetes.io/arch: amd64
    kubernetes.io/os: linux
  name: ${NODE_NAME}
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
    nvidia.com/gpu: "${GPUS_PER_NODE}"
  capacity:
    cpu: "64"
    memory: 512Gi
    pods: "200"
    nvidia.com/gpu: "${GPUS_PER_NODE}"
  nodeInfo:
    architecture: amd64
    kubeletVersion: v1.28.0
    operatingSystem: linux
  phase: Running
EOF
    
    if [ $((n % 10)) -eq 0 ]; then
        echo "已创建 $((n+1))/${NODE_COUNT} 个节点"
    fi
done

echo "=== 集群 ${CLUSTER_NAME} 节点创建完成 ==="
```

### 3.3 将KWOK集群加入Karmada

**脚本: `scripts/03_join_karmada.sh`**

```bash
#!/bin/bash
# 将KWOK集群加入Karmada控制面
# 用法: ./03_join_karmada.sh <server_ip> <cluster_index>

SERVER_IP=$1
CLUSTER_INDEX=$2
CLUSTER_NAME="kwok-cluster-${CLUSTER_INDEX}"

SSH_USER="root"
SSH_PASS="cnic123456"
KARMADA_KUBECONFIG="../kubeconfig/karmada-apiserver.config"

echo "=== 将集群 ${CLUSTER_NAME} 加入Karmada ==="

# 1. 从KWOK服务器导出kubeconfig
sshpass -p "${SSH_PASS}" ssh ${SSH_USER}@${SERVER_IP} "kwokctl get kubeconfig --name=${CLUSTER_NAME}" > /tmp/${CLUSTER_NAME}.kubeconfig

# 2. 使用karmadactl加入集群
karmadactl join ${CLUSTER_NAME} \
    --cluster-kubeconfig=/tmp/${CLUSTER_NAME}.kubeconfig \
    --kubeconfig=${KARMADA_KUBECONFIG}

# 3. 验证加入成功
kubectl --kubeconfig=${KARMADA_KUBECONFIG} get cluster ${CLUSTER_NAME}

echo "=== 集群 ${CLUSTER_NAME} 已加入Karmada ==="
```

### 3.4 一键部署脚本

**脚本: `scripts/00_deploy_all.sh`**

```bash
#!/bin/bash
# 一键部署所有KWOK集群并加入Karmada
# 用法: ./00_deploy_all.sh <scale>
# scale: small(2集群) | medium(5集群) | large(10集群) | extreme(20集群)

SCALE=${1:-small}

case $SCALE in
    small)
        CLUSTERS_PER_SERVER=1
        NODES_PER_CLUSTER=10
        ;;
    medium)
        CLUSTERS_PER_SERVER=2
        NODES_PER_CLUSTER=50
        ;;
    large)
        CLUSTERS_PER_SERVER=5
        NODES_PER_CLUSTER=100
        ;;
    extreme)
        CLUSTERS_PER_SERVER=10
        NODES_PER_CLUSTER=100
        ;;
    *)
        echo "Unknown scale: ${SCALE}"
        exit 1
        ;;
esac

SERVER1="10.10.100.5"
SERVER2="10.10.100.9"

echo "=========================================="
echo "部署规模: ${SCALE}"
echo "每台服务器集群数: ${CLUSTERS_PER_SERVER}"
echo "每集群节点数: ${NODES_PER_CLUSTER}"
echo "=========================================="

# Step 1: 创建KWOK集群
echo "[Step 1] 创建KWOK集群..."
./01_create_kwok_clusters.sh ${SERVER1} 0 ${CLUSTERS_PER_SERVER}
./01_create_kwok_clusters.sh ${SERVER2} ${CLUSTERS_PER_SERVER} ${CLUSTERS_PER_SERVER}

# Step 2: 创建GPU节点
echo "[Step 2] 创建GPU节点..."
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    ./02_create_gpu_nodes.sh ${SERVER1} $i ${NODES_PER_CLUSTER} 4
done
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    ./02_create_gpu_nodes.sh ${SERVER2} $((CLUSTERS_PER_SERVER + i)) ${NODES_PER_CLUSTER} 4
done

# Step 3: 加入Karmada
echo "[Step 3] 加入Karmada..."
for i in $(seq 0 $((CLUSTERS_PER_SERVER*2-1))); do
    SERVER=${SERVER1}
    if [ $i -ge ${CLUSTERS_PER_SERVER} ]; then
        SERVER=${SERVER2}
    fi
    ./03_join_karmada.sh ${SERVER} $i
done

echo "=========================================="
echo "部署完成!"
echo "集群数: $((CLUSTERS_PER_SERVER * 2))"
echo "总节点数: $((CLUSTERS_PER_SERVER * 2 * NODES_PER_CLUSTER))"
echo "=========================================="
```

---

## 四、清理脚本

**脚本: `scripts/99_cleanup_all.sh`**

```bash
#!/bin/bash
# 清理所有KWOK集群并从Karmada移除

SCALE=${1:-small}
case $SCALE in
    small) CLUSTERS_PER_SERVER=1 ;;
    medium) CLUSTERS_PER_SERVER=2 ;;
    large) CLUSTERS_PER_SERVER=5 ;;
    extreme) CLUSTERS_PER_SERVER=10 ;;
esac

SERVER1="10.10.100.5"
SERVER2="10.10.100.9"
SSH_USER="root"
SSH_PASS="cnic123456"
KARMADA_KUBECONFIG="../kubeconfig/karmada-apiserver.config"

echo "=== 开始清理环境 ==="

# 1. 从Karmada移除集群
for i in $(seq 0 $((CLUSTERS_PER_SERVER*2-1))); do
    CLUSTER_NAME="kwok-cluster-${i}"
    echo "从Karmada移除: ${CLUSTER_NAME}"
    kubectl --kubeconfig=${KARMADA_KUBECONFIG} delete cluster ${CLUSTER_NAME} --ignore-not-found
done

# 2. 删除服务器1上的KWOK集群
for i in $(seq 0 $((CLUSTERS_PER_SERVER-1))); do
    CLUSTER_NAME="kwok-cluster-${i}"
    echo "删除KWOK集群: ${CLUSTER_NAME} (服务器1)"
    sshpass -p "${SSH_PASS}" ssh ${SSH_USER}@${SERVER1} "kwokctl delete cluster --name=${CLUSTER_NAME}"
done

# 3. 删除服务器2上的KWOK集群
for i in $(seq ${CLUSTERS_PER_SERVER} $((CLUSTERS_PER_SERVER*2-1))); do
    CLUSTER_NAME="kwok-cluster-${i}"
    echo "删除KWOK集群: ${CLUSTER_NAME} (服务器2)"
    sshpass -p "${SSH_PASS}" ssh ${SSH_USER}@${SERVER2} "kwokctl delete cluster --name=${CLUSTER_NAME}"
done

# 4. 清理本地临时文件
rm -f /tmp/kwok-cluster-*.kubeconfig

echo "=== 环境清理完成 ==="
```

---

## 五、性能测试脚本

### 5.1 Pod提交脚本

**脚本: `scripts/submit_pods.sh`**

```bash
#!/bin/bash
# 批量提交Pod到调度器
# 用法: ./submit_pods.sh <pod_count> <namespace>

POD_COUNT=${1:-100}
NAMESPACE=${2:-default}
KARMADA_KUBECONFIG="../kubeconfig/karmada-apiserver.config"

echo "=== 提交 ${POD_COUNT} 个Pod ==="

for i in $(seq 1 ${POD_COUNT}); do
    cat << EOF | kubectl --kubeconfig=${KARMADA_KUBECONFIG} apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: test-pod-${i}
  namespace: ${NAMESPACE}
  labels:
    app: test-pod
    batch: "perf-test"
spec:
  schedulerName: lyra-scheduler
  containers:
  - name: fake-container
    image: fake-image
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
      limits:
        cpu: "500m"
        memory: "512Mi"
        nvidia.com/gpu: "1"
        nvidia.com/gpumem: "8192"
        nvidia.com/gpucores: "50"
  tolerations:
  - key: "kwok.x-k8s.io/node"
    operator: "Exists"
    effect: "NoSchedule"
EOF

    if [ $((i % 100)) -eq 0 ]; then
        echo "已提交 ${i}/${POD_COUNT} 个Pod"
    fi
done

echo "=== Pod提交完成 ==="
```

### 5.2 性能指标收集

**脚本: `scripts/collect_metrics.sh`**

```bash
#!/bin/bash
# 收集调度器性能指标
# 需要在调度器日志中启用 [PERF] 标签

LOG_FILE=${1:-"../monitor.log"}
OUTPUT_FILE=${2:-"metrics_$(date +%Y%m%d_%H%M%S).csv"}

echo "timestamp,stage,duration_us,pod,cluster,node,feasible_nodes,total_nodes" > ${OUTPUT_FILE}

# 解析日志中的性能指标
grep "\[PERF\]" ${LOG_FILE} | while read line; do
    timestamp=$(echo "$line" | grep -oP '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
    pod=$(echo "$line" | grep -oP 'pod=\K[^,]+')
    
    if [[ "$line" == *"snapshot_us"* ]]; then
        duration=$(echo "$line" | grep -oP 'snapshot_us=\K\d+')
        echo "${timestamp},snapshot,${duration},${pod},,,," >> ${OUTPUT_FILE}
    elif [[ "$line" == *"filter_us"* ]]; then
        duration=$(echo "$line" | grep -oP 'filter_us=\K\d+')
        feasible=$(echo "$line" | grep -oP 'feasible_nodes=\K\d+')
        total=$(echo "$line" | grep -oP 'total_nodes=\K\d+')
        echo "${timestamp},filter,${duration},${pod},,,${feasible},${total}" >> ${OUTPUT_FILE}
    elif [[ "$line" == *"score_us"* ]]; then
        duration=$(echo "$line" | grep -oP 'score_us=\K\d+')
        echo "${timestamp},score,${duration},${pod},,,," >> ${OUTPUT_FILE}
    elif [[ "$line" == *"gpu_alloc_us"* ]]; then
        duration=$(echo "$line" | grep -oP 'gpu_alloc_us=\K\d+')
        echo "${timestamp},gpu_alloc,${duration},${pod},,,," >> ${OUTPUT_FILE}
    elif [[ "$line" == *"assume_us"* ]]; then
        duration=$(echo "$line" | grep -oP 'assume_us=\K\d+')
        echo "${timestamp},assume,${duration},${pod},,,," >> ${OUTPUT_FILE}
    fi
done

echo "指标已保存到: ${OUTPUT_FILE}"
```

---

### 5.3 背景事件模拟

**脚本: `scripts/simulate_events_remote.sh`**

在 Pod 调度期间模拟集群节点标签更新，触发增量快照的 Generation 变化，使测试更接近真实生产环境。

```bash
# 用法: bash simulate_events_remote.sh <max_rate> <duration> <cluster_list>
# max_rate: 每秒最大事件数 (实际: 1~max_rate 随机)
# duration: 持续时间秒数
# cluster_list: 空格分隔的集群名列表

# 示例：模拟每秒1~50个事件，持续300秒
bash simulate_events_remote.sh 50 300 kwok-cluster-0 kwok-cluster-1
```

**工作原理：**
1. 每秒随机生成 1~max_rate 个节点更新事件
2. 通过 kubectl annotate 更新节点的 `simulation.alpha/ts` 注解
3. 节点更新触发 Informer 事件，导致 Generation 递增
4. 增量快照需要克隆这些 "脏" 节点，从而验证增量效果

**效果验证：**
- 无背景事件时：`cloned_nodes` ≈ 0~4（仅 AssumePod 的变化）
- 有背景事件时：`cloned_nodes` 随事件速率增加而上升

---

## 六、测试执行流程

### 6.1 快速测试（推荐）

```bash
# 一键运行全面测试（含背景事件模拟）
bash scripts/run_full_perf_test.sh <scale> <pod_rate> <duration_min> <event_rate>

# 示例
bash scripts/run_full_perf_test.sh medium 50 1 30   # 快速验证
bash scripts/run_full_perf_test.sh large 100 5 50    # 标准测试
bash scripts/run_full_perf_test.sh large 200 10 100  # 极限压力
```

### 6.2 手动测试（逐步执行）

**前置条件：**
```bash
# 1. 确保sshpass已安装
sudo apt-get install sshpass  # Linux
# Windows: 使用 Git Bash 或 WSL

# 2. 确保karmadactl已安装
karmadactl version

# 3. 确保Lyra调度器已编译
cd ../ && go build .
```

**执行测试：**
```bash
# Step 1: 部署测试环境
./scripts/deploy_fast.sh small    # 小规模测试

# Step 2: 启动Lyra调度器（使用全量快照）
LYRA_SNAPSHOT_MODE=full ./lyra.exe 2>&1 | tee logs/full_snapshot.log &

# Step 3: 提交测试Pod
./scripts/submit_pods.sh 50

# Step 4: 等待调度完成并收集指标
sleep 60
./scripts/collect_metrics.sh logs/full_snapshot.log metrics/full_small.csv

# Step 5: 停止调度器，切换到增量快照（默认）
pkill lyra
./lyra.exe 2>&1 | tee logs/incremental_snapshot.log &

# Step 6: 清理Pod并重新测试
kubectl --kubeconfig=../kubeconfig/karmada-apiserver.config delete pods -l app=test-pod
sleep 10
./scripts/submit_pods.sh 50
sleep 60
./scripts/collect_metrics.sh logs/incremental_snapshot.log metrics/incr_small.csv

# Step 7: 清理环境
./scripts/cleanup_fast.sh small
```

### 6.3 测试结果汇总

| 规模 | 快照类型 | 快照更新(μs) | Filter(μs) | Score(μs) | GPU分配(μs) | 加速比 |
|------|---------|-------------|-----------|-----------|------------|--------|
| 小规模(20节点) | 增量 | 40.35 | 4.05 | 555.16 | 0.00 | 基准 |
| 小规模(20节点) | 全量 | 522.09 | 56.72 | 552.87 | 2.51 | **12.9x** |
| 中规模(200节点) | 增量 | 38.31 | 44.40 | 6267.13 | 3.46 | 基准 |
| 中规模(200节点) | 全量 | 1292.42 | 44.11 | 6691.27 | 0.88 | **33.7x** |
| 大规模(1000节点) | 增量 | 待测试 | - | - | - | - |
| 大规模(1000节点) | 全量 | 待测试 | - | - | - | - |
| 极限规模(2000节点) | 增量 | 待测试 | - | - | - | - |
| 极限规模(2000节点) | 全量 | 待测试 | - | - | - | - |

**关键结论**：
1. 增量快照在小规模环境加速约13倍，中规模环境加速约34倍
2. 节点越多，增量快照优势越明显
3. Filter/Score阶段不受快照模式影响

---

## 七、代码修改说明

### 7.1 添加全量快照方法

需要在 `internal/scheduler/backend/cache/cache.go` 中添加:

```go
// FullUpdateSnapshot 全量克隆快照（无增量优化）
func (c *cacheImpl) FullUpdateSnapshot(logger *zap.Logger, s *Snapshot) error {
    c.mu.RLock()
    defer c.mu.RUnlock()

    // 清空旧快照
    s.clusterInfoMap = make(map[string]*framework.ClusterInfo)
    s.clusterInfoList = make([]*framework.ClusterInfo, 0, len(c.clusters))

    // 全量克隆所有集群和节点
    for _, ci := range c.clusters {
        clone := &framework.ClusterInfo{
            ClusterName: ci.info.ClusterName,
            Generation:  ci.info.Generation,
            Nodes:       make(map[string]*framework.NodeInfo, len(ci.nodes)),
        }
        if ci.info.Allocatable != nil {
            clone.Allocatable = ci.info.Allocatable.Clone()
        }
        if ci.info.Requested != nil {
            clone.Requested = ci.info.Requested.Clone()
        }
        if ci.info.Cluster() != nil {
            clone.SetCluster(ci.info.Cluster())
        }

        for _, ni := range ci.nodes {
            clone.Nodes[ni.info.NodeName] = ni.info.Snapshot()
        }

        s.clusterInfoMap[clone.ClusterName] = clone
        s.clusterInfoList = append(s.clusterInfoList, clone)
    }

    return nil
}
```

### 7.2 切换快照方法

通过环境变量 `LYRA_SNAPSHOT_MODE` 控制，无需修改代码：

```bash
# 增量快照（默认）
unset LYRA_SNAPSHOT_MODE
./lyra.exe

# 全量快照
export LYRA_SNAPSHOT_MODE=full
./lyra.exe
```

---

## 八、注意事项

1. **HAMI GPU注解格式**: 必须使用 `hami.io/node-nvidia-register` 注解，格式为 `GPU-UUID,MaxVGPUs,MemoryTotal(MB),CoreTotal(%),GPU-Type:...`

2. **UUID唯一性**: 每个GPU的UUID必须全局唯一，脚本中使用 `GPU-c{cluster}n{node}g{gpu}-{random}` 格式

3. **调度器名称**: Pod必须设置 `schedulerName: lyra-scheduler` 才能被Lyra调度

4. **污点容忍**: 模拟节点带有 `kwok.x-k8s.io/node=fake:NoSchedule` 污点，Pod需要添加容忍

5. **网络要求**: 
   - Lyra调度器需要能访问Karmada API Server
   - 两台KWOK服务器之间网络互通

6. **清理顺序**: 先从Karmada移除集群，再删除KWOK集群，避免残留资源

7. **Kubeconfig路径**: 脚本中使用 `../../kubeconfig/karmada-apiserver.config`，请确保此文件存在
