# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Lyra is a multi-cluster GPU-aware scheduler built on Karmada for AI training/inference workloads. It extends Kubernetes scheduling with:
- Multi-cluster GPU resource scheduling across federated clusters
- Fine-grained GPU allocation (UUID-level tracking, not just scalar aggregation)
- Karmada-based workload distribution via PropagationPolicy/OverridePolicy
- In-memory cache with optimistic assume mechanism to prevent overselling
- QueueingHint-based intelligent wakeup to reduce invalid retries

See `PROJECT_OVERVIEW.md` for a detailed architectural overview and the three key design highlights (multi-cluster GPU scheduling, cache + assume mechanism, QueueingHint intelligent wakeup).

## Build and Test

```bash
# Build
go build ./...

# Run all tests
go test ./...

# Run tests for specific package
go test -v ./internal/scheduler/framework/...

# Run benchmarks
go test -bench=. ./internal/scheduler/backend/cache/...
go test -bench=. ./internal/scheduler/backend/queue/...
```

## Entry Point

- `cmd/scheduler/scheduler.go` -- main entry, wires config loading, kubeconfig, informers, scheduler
- `scheduler.NewScheduler` in `internal/scheduler/scheduler.go` -- assembles all components (cache, queue, framework, cluster manager, dispatcher)
- `scheduler.Run` starts the main scheduling loop via `wait.UntilWithContext(ctx, sched.ScheduleOne, 0)`

## Architecture

### Two-Layer Connection Model

**Layer 1 - Karmada Control Plane:**
- Listens for Cluster resources and unscheduled Pods
- Dispatches scheduling decisions via Karmada PP/OP

**Layer 2 - Member Clusters:**
- `ClusterAccessManager` dynamically connects to member clusters using their kubeconfigs
- Listens for Node/Pod events to track GPU resources and Pod binding status
- Enables a closed loop: schedule -> assume -> dispatch -> monitor -> reconcile

### Key Components

```
cmd/scheduler/
  scheduler.go              # Binary entry point
internal/scheduler/
  scheduler.go              # NewScheduler, Run, scheduler struct
  schedule_one.go           # ScheduleOne, schedulePod, schedulingCycle, bindingCycle
  eventhandlers.go          # Event handlers for Cluster/Pod/Node
  backend/
    cache/                  # 2D MRU cache with generation-based incremental snapshots
    queue/                  # ActiveQ/BackoffQ/UnschedulablePods three-tier queue
  dispatcher/               # Dispatcher.Dispatch creates Karmada PP/OP
  framework/
    interface.go            # Plugin interfaces (PreFilter, Filter, Score, Reserve, Bind)
    runtime/                # Plugin registry and framework implementation
    types.go                # NodeInfo, ClusterInfo, GPUInfo, Resource, etc.
    gpu_parse.go            # HAMI annotation parsing (ParseNodeHamiAnnotation, ParsePodHamiAnnotation)
  multicluster/
    manager.go              # ClusterAccessManager, dynamic cluster connection
    kubeconfig/             # Kubeconfig secret provider
  plugins/
    gpurender/              # GPU resource filtering and scoring
    noderesources/          # Node resource filtering
    karmadabind/            # Karmada binding (creates PP/OP)
    prioritysort/           # Pod priority queue sorting
pkg/logger/
  context.go                # log.NewContext / log.FromContext for pod-traceable contextual logging
```

### Scheduling Pipeline

Lyra uses **serial scheduling** -- `wait.UntilWithContext` ensures only one Pod is being scheduled at a time. One schedulingCycle completes before the next begins.

1. `ScheduleOne` pops a Pod from the queue via `SchedulingQueue.Pop`
2. `schedulingCycle` (synchronous):
   - `schedulePod` calls `cache.UpdateSnapshot` to clone a snapshot
   - `findNodesThatFitPod` -> PreFilter (cluster pruning) + Filter (node filtering)
   - `prioritizeNodes` -> Score (concurrent scoring across nodes)
   - `allocateGPUsOnNode` -> Best-Fit 2D bin-packing for specific GPU UUIDs
   - `AssumePod` marks resources as assumed in the cache (does NOT update Generation)
3. `bindingCycle` (asynchronous goroutine):
   - `Dispatcher.Dispatch` creates Karmada OP then PP
   - `FinishBinding` starts TTL countdown
   - Sub-cluster Pod events reconcile assumed->confirmed

Snapshot mode is controlled by `LYRA_SNAPSHOT_MODE=full` env var (defaults to incremental).

### GPU Handling

GPU info is parsed from HAMI annotations on Node and Pod objects via `ParseNodeHamiAnnotation` / `ParsePodHamiAnnotation`. Key structures:

```go
type GPUInfo struct {
    UUID, Type string
    AllocatableMem/RequestedMem    // VRAM tracking
    AllocatableCore/RequestedCore  // Compute percentage tracking
    AllocatableVGPU/RequestedVGPU  // vGPU slice tracking
}

type NodeInfo struct {
    NodeName, ClusterName string
    GPUs map[string]*GPUInfo  // Keyed by UUID, not array
}
```

**Important:** `NodeInfo.GPUs` is a `map[string]*GPUInfo`, NOT a slice. Tests must use map literals.

### Cache Snapshot Mechanism

The cache uses a **2D MRU (Most Recently Used) linked list** with generation numbers for incremental snapshots:

- Cluster-level MRU: changed clusters move to head; Node-level MRU: changed nodes move to head within cluster
- Snapshot clones only changed data (O(change volume)) using Generation comparison
- `AssumePod` updates cache state but does NOT increment Generation -- only Informer events do
- `LYRA_SNAPSHOT_MODE=full` forces full snapshot; defaults to incremental

### Framework Plugin Interface

Plugins implement interfaces like `PreFilterPlugin`, `FilterPlugin`, `ScorePlugin`, `BindPlugin`. The `Handle` interface provides:
- `SnapshotSharedLister()` -- cache snapshot
- `ClientSet()` / `KarmadaClient()` -- Kubernetes clients
- `Parallelizer()` -- concurrent execution engine

### Karmada Binding Flow

`KarmadaBind` plugin creates:
1. **OverridePolicy (OP)** first -- injects `schedulerName=default-scheduler`, GPU UUID annotations
2. **PropagationPolicy (PP)** second -- distributes Pod to target cluster

OP must be created before PP because the binding controller syncs Works immediately when PP is created -- if OP doesn't exist yet, Works go to sub-clusters without overrides.

## Key Conventions

- Package `framework` is the common foundation -- no cyclic dependencies allowed
- GPU allocation decision is written to Pod annotations for reconciliation
- `AssumePod` does NOT increment Generation -- only Informer events (NodeAdd/Update/Remove, Pod events) do
- Only ONE scheduler instance should run per Karmada control plane (multiple instances cause PP/OP "already exists" conflicts)
- Dispatcher uses `Create` (not `CreateOrUpdate`) for PP/OP -- conflicts between concurrent scheduler instances are expected if multiple run
- Contextual logging via `pkg/logger`: `log.NewContext(ctx, logger)` embeds context, `log.FromContext(ctx)` retrieves it
- **PP/OP creation order matters**: OP must be created BEFORE PP. See `experiments/troubleshooting/concurrent-scheduling-issue.md` for the full root cause analysis of the race condition this caused.

## Performance Testing

### Scripts

**Shared infrastructure** (`experiments/shared/scripts/`):
```bash
bash experiments/shared/scripts/deploy_fast.sh <small|medium|large|extreme>
bash experiments/shared/scripts/cleanup_fast.sh <small|medium|large|extreme>
bash experiments/shared/scripts/submit_pods.sh <count> [namespace]
```

**Snapshot performance test** (`experiments/snapshot/scripts/`):
```bash
bash experiments/snapshot/scripts/run_full_perf_test.sh <scale> <pod_rate> <duration_min> [event_rate]
# Example: bash experiments/snapshot/scripts/run_full_perf_test.sh large 100 5 80
```

**QueueingHint experiment** (`experiments/queueing_hint/scripts/`):
```bash
bash experiments/queueing_hint/scripts/test_qhint_e2e.sh
bash experiments/queueing_hint/scripts/run_benchmark.sh
```

### Scale Matrix

| Scale | Clusters | Nodes/Cluster | Total Nodes | GPUs/Node | Total GPUs | Test Pods |
|-------|----------|---------------|-------------|-----------|------------|-----------|
| small | 2 | 10 | 20 | 4 | 80 | 50 |
| medium | 4 | 50 | 200 | 4 | 800 | 500 |
| large | 10 | 100 | 1000 | 4 | 4000 | 1000 |
| extreme | 20 | 100 | 2000 | 4 | 8000 | 2000 |

### Test Environment

Uses KWOK clusters on remote servers to simulate multi-cluster GPU scheduling:

**Real clusters (always present):**
- **karmada** -- Karmada control plane is deployed on this cluster; also a member cluster; no GPU
- **cluster1** -- real member cluster with 3x GTX 1660 SUPER GPUs
- **cluster2** -- real member cluster with 1x GPU
- Their kubeconfigs are in `experiments/shared/kubeconfig/` (for debugging; not used by scheduler)

**KWOK virtual clusters (created per test):**
- Server 1 (`10.10.100.5`): hosts clusters 0 to N-1
- Server 2 (`10.10.100.9`): hosts clusters N to 2N-1
- Both directly join Karmada as member clusters (same level as real clusters)
- Kubeconfigs live on remote servers, injected to Karmada by deploy_fast.sh via ssh

**Kubeconfig:**
- `kubeconfig/karmada-apiserver.config` -- Karmada control plane, used by Lyra scheduler
- `experiments/shared/kubeconfig/` -- real member clusters (karmada, cluster1, cluster2), for manual debugging only

### References

- `experiments/shared/PERFORMANCE_TEST_PLAN.md` -- full test plan with scale matrix, environment setup scripts, execution flow
- `experiments/shared/KWOK_CHEATSHEET.md` -- KWOK cluster management reference (create/delete nodes, join Karmada, etc.)
- `experiments/troubleshooting/concurrent-scheduling-issue.md` -- PP/OP race condition root cause analysis and fix verification

## Configuration

Loaded via `config.Load()` from `config.yaml` at startup. Key settings:
- `karmada.kubeconfig` -- path to Karmada apiserver kubeconfig
- `log.level` -- logging level (debug/info/warn/error)
- `log.file_name` -- log output file (e.g. `monitor.log` for perf analysis)
- `log.max_size` / `log.max_backups` / `log.max_age` -- log rotation settings