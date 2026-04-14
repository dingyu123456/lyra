# 简历三句话亮点

1. **架构融合**：设计并实现基于 Karmada 的多集群调度器，融合 Hami GPU 虚拟化实现 GPU 精细化调度，采用插件化调度框架支持 PreFilter/Filter/Score/Reserve/Permit/Bind 全生命周期扩展点，通过 Karmada PropagationPolicy + OverridePolicy 实现跨集群 Pod 分发与 GPU UUID 精准注入。

2. **缓存设计**：设计二维 MRU 链表 + Generation 原子计数器的增量快照缓存，实现集群→节点→GPU 三级资源视图，快照更新仅深拷贝变更节点（稳态 O(K) vs K8s 全量拷贝 O(N)），结合乐观 AssumePod 与 TTL 回滚机制支撑高吞吐调度。

3. **智能唤醒**：实现三级调度队列（ActiveQ + BackoffQ + UnschedulablePods）配合 QueueingHint 回调机制，插件注册细粒度事件匹配函数实现精准唤醒，避免雷群式重调度风暴，指数退避 + 5 分钟超时兜底防止饥饿。
