package framework

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// 定义当前文件中使用的 Kubernetes 相关的固定字符串常量
const (
	AnnotationUseGPUType = "nvidia.com/use-gputype"
	ResourceGPU          = "nvidia.com/gpu"
	ResourceGPUMem       = "nvidia.com/gpumem"
	ResourceGPUCores     = "nvidia.com/gpucores"
)

const (
	// AnnotationPodVGPUAllocated 向pod注解中注入预期的GPU分配信息（决策信息），用来预扣GPU、后续与pod实际的gpu分配信息对账
	AnnotationPodVGPUAllocated = "hami.io/vgpu-devices-allocated"
	// AnnotationNodeNvidiaRegister 用于提取节点上的物理GPU容量信息
	AnnotationNodeNvidiaRegister = "hami.io/node-nvidia-register"
)

const (
	// KeyPodGPUReq 这个key是在cycleState中使用的
	KeyPodGPUReq = "lyra.io/pod-gpu-req"
)

const (
	// AnnotationUseGPUUUID 是调度器注入的注解，告知底层 HAMi 真正分配哪些物理卡
	// 注意：在 JSON Patch 中使用此路径时需将 "/" 替换为 "~1"
	AnnotationUseGPUUUID = "nvidia.com/use-gpuuuid"
)

// GPURequirement 记录了 Pod 实际需要的算力清单
type GPURequirement struct {
	NumCards int      // 需要几张物理卡 (nvidia.com/gpu)
	MemReq   int64    // 每张卡需要的显存 (nvidia.com/gpumem)
	CoreReq  int64    // 每张卡需要的算力比例 (nvidia.com/gpucores)
	Types    []string // 指定的显卡型号列表 (nvidia.com/use-gputype)
}

func (req *GPURequirement) Clone() StateData {
	cloneTypes := make([]string, len(req.Types))
	copy(cloneTypes, req.Types)
	stateData := &GPURequirement{
		NumCards: req.NumCards,
		MemReq:   req.MemReq,
		CoreReq:  req.CoreReq,
		Types:    cloneTypes,
	}
	return stateData
}

// ParsedGPUInfo 用于承载解析后的单个物理GPU数据 (供给方 - 节点容量)
type ParsedGPUInfo struct {
	UUID        string
	MaxVGPUs    int64
	MemoryTotal int64
	CoreTotal   int64
	Type        string
}

// ParsedPodGPUAlloc 用于承载解析后的 Pod 实际占用情况 (供给方 - 节点账本扣减依据)
type ParsedPodGPUAlloc struct {
	UUID       string
	MemoryUsed int64
	CoreUsed   int64
	Type       string
}

// ParsePodGPUReqs 从 Pod 的 Limits 和 Annotations 中提取 GPU 需求
func ParsePodGPUReqs(pod *corev1.Pod) GPURequirement {
	req := GPURequirement{}

	// 解析 Annotations 中的卡型强制要求
	if pod.Annotations != nil {
		if gpuTypes, ok := pod.Annotations[AnnotationUseGPUType]; ok && gpuTypes != "" {
			// 支持逗号分隔的多种可选型号，如 "A100,V100"
			for _, t := range strings.Split(gpuTypes, ",") {
				req.Types = append(req.Types, strings.TrimSpace(t))
			}
		}
	}

	// 遍历容器解析 Limits (提取扩展资源)
	for _, container := range pod.Spec.Containers {
		limits := container.Resources.Limits
		if limits == nil {
			continue
		}

		if val, ok := limits[ResourceGPU]; ok {
			req.NumCards += int(val.Value())
		}
		if val, ok := limits[ResourceGPUMem]; ok {
			req.MemReq += val.Value()
		}
		if val, ok := limits[ResourceGPUCores]; ok {
			req.CoreReq += val.Value()
		}
	}

	return req
}

// InjectHamiVGPUAnnotation 根据调度结果和 Pod 需求，组装 Hami 识别的 GPU 分配凭证，并写入 Pod 注解
func InjectHamiVGPUAnnotation(pod *corev1.Pod, allocatedGPUs []string) {
	if len(allocatedGPUs) == 0 {
		return // 如果没有分配 GPU，直接跳过
	}

	// 1. 获取 Pod 原始的 GPU 需求
	req := ParsePodGPUReqs(pod)

	// 2. 确定 GPU 类型 (默认 NVIDIA，防 Hami 报错兜底)
	gpuType := "NVIDIA"
	if len(req.Types) > 0 {
		gpuType = req.Types[0]
	}

	// 3. 拼接 Hami 注解格式
	// 格式: GPU-UUID,Type,Memory,Core:GPU-UUID,Type,Memory,Core:;
	var allocBuilder strings.Builder
	for _, uuid := range allocatedGPUs {
		allocStr := fmt.Sprintf("%s,%s,%d,%d:", uuid, gpuType, req.MemReq, req.CoreReq)
		allocBuilder.WriteString(allocStr)
	}
	allocBuilder.WriteString(";")

	// 4. 安全地写入 Pod 注解
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[AnnotationPodVGPUAllocated] = allocBuilder.String()
}

// ParseNodeHamiAnnotation 解析节点侧真实的物理 GPU 容量
func ParseNodeHamiAnnotation(node *corev1.Node) ([]ParsedGPUInfo, error) {
	val, ok := node.Annotations[AnnotationNodeNvidiaRegister]
	if !ok || val == "" {
		return nil, nil // 没有显卡的普通节点，直接返回空，不是 error
	}

	var results []ParsedGPUInfo
	records := strings.Split(val, ":")

	for _, rec := range records {
		fields := strings.Split(rec, ",")
		// 校验长度防止切片越界 (至少需要UUID,MaxVGPUs,Memory,Core,Type共5个字段)
		if len(fields) < 5 {
			continue
		}

		if !strings.HasPrefix(fields[0], "GPU-") {
			continue
		}

		gpu := ParsedGPUInfo{
			UUID: fields[0],
			Type: fields[4],
		}
		// 忽略转换错误，如果遇到非数字格式则默认为 0
		gpu.MaxVGPUs, _ = strconv.ParseInt(fields[1], 10, 64)
		gpu.MemoryTotal, _ = strconv.ParseInt(fields[2], 10, 64)
		gpu.CoreTotal, _ = strconv.ParseInt(fields[3], 10, 64)

		results = append(results, gpu)
	}
	return results, nil
}

// ParsePodHamiAnnotation 解析 Pod 侧已经被真正分配/扣减的 GPU 算力
func ParsePodHamiAnnotation(pod *corev1.Pod) ([]ParsedPodGPUAlloc, error) {
	val, ok := pod.Annotations[AnnotationPodVGPUAllocated]
	if !ok || val == "" {
		return nil, nil // 没有分配 GPU，直接返回空
	}

	val = strings.TrimSuffix(val, ";")
	records := strings.Split(val, ":")

	var results []ParsedPodGPUAlloc
	for _, rec := range records {
		fields := strings.Split(rec, ",")
		if len(fields) < 4 {
			continue
		}

		alloc := ParsedPodGPUAlloc{
			UUID: fields[0],
			Type: fields[1],
		}
		alloc.MemoryUsed, _ = strconv.ParseInt(fields[2], 10, 64)
		alloc.CoreUsed, _ = strconv.ParseInt(fields[3], 10, 64)

		results = append(results, alloc)
	}
	return results, nil
}

// FormatHamiVGPUAnnotation 仅用于将 UUID 列表转为逗号分隔的字符串，供 OP 组装使用
func FormatHamiVGPUAnnotation(uuids []string) string {
	if len(uuids) == 0 {
		return ""
	}
	return strings.Join(uuids, ",")
}
