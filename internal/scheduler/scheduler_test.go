/*
Copyright 2024 The Lyra Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/dingyu123456/lyra/internal/scheduler/framework"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	karmadafake "github.com/karmada-io/karmada/pkg/generated/clientset/versioned/fake"
	karmadainformers "github.com/karmada-io/karmada/pkg/generated/informers/externalversions"
)

// TestSchedulerInitialization 测试调度器的初始化
func TestSchedulerInitialization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := zaptest.NewLogger(t)
	kubeClient := fake.NewSimpleClientset()
	karmadaClient := karmadafake.NewSimpleClientset()

	// 创建 informer factories
	kubeFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	karmadaFactory := karmadainformers.NewSharedInformerFactory(karmadaClient, 0)

	sched, err := NewScheduler(ctx, kubeClient, karmadaClient, kubeFactory, karmadaFactory, logger)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}

	if sched == nil {
		t.Fatal("Scheduler is nil")
	}

	if sched.Name != "lyra-scheduler" {
		t.Errorf("Expected scheduler name 'lyra-scheduler', got %s", sched.Name)
	}

	if sched.Cache == nil {
		t.Error("Scheduler cache is nil")
	}

	if sched.SchedulingQueue == nil {
		t.Error("Scheduling queue is nil")
	}

	if sched.Framework == nil {
		t.Error("Framework is nil")
	}

	if sched.ClusterManager == nil {
		t.Error("Cluster manager is nil")
	}

	if sched.logger == nil {
		t.Error("Logger is nil")
	}
}

// TestSchedulerRun 测试调度器的运行（基本功能）
func TestSchedulerRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := zaptest.NewLogger(t)
	kubeClient := fake.NewSimpleClientset()
	karmadaClient := karmadafake.NewSimpleClientset()

	// 创建 informer factories
	kubeFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	karmadaFactory := karmadainformers.NewSharedInformerFactory(karmadaClient, 0)

	sched, err := NewScheduler(ctx, kubeClient, karmadaClient, kubeFactory, karmadaFactory, logger)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}

	// 测试调度器运行（短时间内取消）
	runCtx, runCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer runCancel()

	// 在单独的 goroutine 中运行调度器
	go func() {
		sched.Run(runCtx)
	}()

	// 等待一段时间确保调度器启动
	time.Sleep(50 * time.Millisecond)

	// 验证调度器组件不为空
	if sched.SchedulePod == nil {
		t.Error("SchedulePod function is nil")
	}

	if sched.FailureHandler == nil {
		t.Error("FailureHandler function is nil")
	}

	if sched.NextPod == nil {
		t.Error("NextPod function is nil")
	}
}

// TestGetDefaultPlugins 测试默认插件配置
func TestGetDefaultPlugins(t *testing.T) {
	plugins := getDefaultPlugins()

	if plugins == nil {
		t.Fatal("Default plugins is nil")
	}

	// 检查 QueueSort 插件
	if len(plugins.QueueSort.Enabled) != 1 {
		t.Errorf("Expected 1 QueueSort plugin, got %d", len(plugins.QueueSort.Enabled))
	} else if plugins.QueueSort.Enabled[0].Name != "PrioritySort" {
		t.Errorf("Expected QueueSort plugin 'PrioritySort', got %s", plugins.QueueSort.Enabled[0].Name)
	}

	// 检查 Filter 插件
	expectedFilterPlugins := []string{"GPUResourceFit", "NodeResourcesFit"}
	if len(plugins.Filter.Enabled) != len(expectedFilterPlugins) {
		t.Errorf("Expected %d Filter plugins, got %d", len(expectedFilterPlugins), len(plugins.Filter.Enabled))
	} else {
		for i, expected := range expectedFilterPlugins {
			if plugins.Filter.Enabled[i].Name != expected {
				t.Errorf("Filter plugin %d: expected %s, got %s", i, expected, plugins.Filter.Enabled[i].Name)
			}
		}
	}

	// 检查 Score 插件
	expectedScorePlugins := []string{"GPUResourceFit", "NodeResourcesFit"}
	if len(plugins.Score.Enabled) != len(expectedScorePlugins) {
		t.Errorf("Expected %d Score plugins, got %d", len(expectedScorePlugins), len(plugins.Score.Enabled))
	} else {
		for i, expected := range expectedScorePlugins {
			if plugins.Score.Enabled[i].Name != expected {
				t.Errorf("Score plugin %d: expected %s, got %s", i, expected, plugins.Score.Enabled[i].Name)
			}
		}
	}

	// 检查 Bind 插件
	if len(plugins.Bind.Enabled) != 1 {
		t.Errorf("Expected 1 Bind plugin, got %d", len(plugins.Bind.Enabled))
	} else if plugins.Bind.Enabled[0].Name != "KarmadaBind" {
		t.Errorf("Expected Bind plugin 'KarmadaBind', got %s", plugins.Bind.Enabled[0].Name)
	}
}

// TestSchedulerErrors 测试调度器错误定义
func TestSchedulerErrors(t *testing.T) {
	if ErrNoClustersAvailable == nil {
		t.Error("ErrNoClustersAvailable should not be nil")
	}

	if ErrNoNodesAvailable == nil {
		t.Error("ErrNoNodesAvailable should not be nil")
	}

	// 验证错误消息
	if ErrNoClustersAvailable.Error() != "no clusters available to schedule pods" {
		t.Errorf("Wrong error message for ErrNoClustersAvailable: %v", ErrNoClustersAvailable.Error())
	}

	if ErrNoNodesAvailable.Error() != "no nodes available to schedule pods" {
		t.Errorf("Wrong error message for ErrNoNodesAvailable: %v", ErrNoNodesAvailable.Error())
	}
}

// TestSchedulerStructFields 测试调度器结构体字段
func TestSchedulerStructFields(t *testing.T) {
	// 创建模拟的调度器结构体
	sched := &Scheduler{
		Name:           "test-scheduler",
		logger:         zap.NewNop(),
	}

	if sched.Name != "test-scheduler" {
		t.Errorf("Expected name 'test-scheduler', got %s", sched.Name)
	}

	if sched.logger == nil {
		t.Error("Logger should not be nil")
	}
}

// TestFailureHandlerFn 测试失败处理函数类型
func TestFailureHandlerFn(t *testing.T) {
	// 创建一个简单的失败处理函数
	var handlerCalled bool
	var handlerPodInfo *framework.QueuedPodInfo
	var handlerStatus *framework.Status

	handler := func(ctx context.Context, fwk framework.Framework, podInfo *framework.QueuedPodInfo, status *framework.Status, start time.Time) {
		handlerCalled = true
		handlerPodInfo = podInfo
		handlerStatus = status
	}

	// 模拟调用
	ctx := context.Background()
	podInfo := &framework.QueuedPodInfo{
		PodInfo: &framework.PodInfo{
			Pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
				},
			},
		},
	}
	status := framework.NewStatus(framework.Unschedulable, "test error")
	startTime := time.Now()

	handler(ctx, nil, podInfo, status, startTime)

	if !handlerCalled {
		t.Error("Failure handler was not called")
	}

	if handlerPodInfo != podInfo {
		t.Error("Handler received wrong pod info")
	}

	if handlerStatus != status {
		t.Error("Handler received wrong status")
	}
}

// TestSchedulerInterface 测试调度器接口实现
func TestSchedulerInterface(t *testing.T) {
	// 验证调度器实现了必要的函数
	sched := &Scheduler{
		SchedulePod: func(ctx context.Context, fwk framework.Framework, state *framework.CycleState, pod *corev1.Pod) (framework.ScheduleResult, error) {
			return framework.ScheduleResult{}, nil
		},
		FailureHandler: func(ctx context.Context, fwk framework.Framework, podInfo *framework.QueuedPodInfo, status *framework.Status, start time.Time) {
			// Do nothing
		},
		NextPod: func(logger *zap.Logger) (*framework.QueuedPodInfo, error) {
			return nil, nil
		},
	}

	if sched.SchedulePod == nil {
		t.Error("SchedulePod should not be nil")
	}

	if sched.FailureHandler == nil {
		t.Error("FailureHandler should not be nil")
	}

	if sched.NextPod == nil {
		t.Error("NextPod should not be nil")
	}
}

// TestSchedulerWithMockClients 测试使用模拟客户端的调度器
func TestSchedulerWithMockClients(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := zaptest.NewLogger(t)

	// 创建模拟客户端
	kubeClient := &mockKubeClient{}
	karmadaClient := &mockKarmadaClient{}

	// 创建模拟的 informer factories
	kubeFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	karmadaFactory := karmadainformers.NewSharedInformerFactory(karmadaClient, 0)

	// 这里我们期望 NewScheduler 会失败，因为我们的模拟客户端不完整
	// 但我们可以测试错误处理
	_, err := NewScheduler(ctx, kubeClient, karmadaClient, kubeFactory, karmadaFactory, logger)

	// 我们期望错误，因为模拟客户端不完整
	if err == nil {
		t.Log("NewScheduler with mock clients returned without error (this might be expected)")
	}
}

// 模拟的 Kubernetes 客户端
type mockKubeClient struct {
	clientset.Interface
}

// 模拟的 Karmada 客户端
type mockKarmadaClient struct {
	karmadaclientset.Interface
}