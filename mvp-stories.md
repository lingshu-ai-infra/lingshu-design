# MVP 实施 Story 清单

> 衍生自 [linshu-ai-infra.md §7.1](linshu-ai-infra.md#7-⚠️-落地实施步骤mvp-到生产)。V2.1 基线已冻结,进入 MVP 6 周开发节奏。

## 0. Story 元信息

| 字段 | 约定 |
|---|---|
| **Story ID** | `STORY-{阶段}-{序号}`(如 `STORY-1-1` = 阶段 1 第 1 个 Story) |
| **格式** | User Story + Gherkin AC + 技术备注 + 依赖 |
| **估时单位** | 人日(1 人日 = 1 名工程师 1 天工作量) |
| **状态机** | `Backlog → Ready → InProgress → InReview → Done` |
| **验收人** | Tech Lead(技术验收)+ PM(范围验收) |
| **依赖管理** | 同阶段 Story 并行无强依赖;跨阶段严格串行 |

## 1. MVP 范围与决策对齐

**范围收窄**(Q1-Q7 已确认):
- 业务:1 个推理类 Op、1 个业务线
- 集群:单集群、≤ 3 个 GPU 节点
- 验证目标:端到端可行性,**不做性能优化**

**MVP 不包含**(明确划界):
- ❌ 多副本调度器选主(ControllerManager 改造)
- ❌ 优先级抢占、同节点亲和
- ❌ 显存硬隔离(MPS/MIG)
- ❌ 大张量 out-of-band 传输(走 inline 即可)
- ❌ RDMA / NAS / 共享内存
- ❌ mTLS / RBAC / 审计日志
- ❌ 多业务隔离

## 2. Story 清单(按 Sprint 分组)

### Sprint 1(Week 1-2):基础通信与算子注册

---

#### STORY-1-1 扩展 cloud-basic PacketType 枚举支持 GPU 业务包

**User Story**:
> 作为 **平台开发工程师**,我需要 在 `cloud-basic` 的 `PacketType` 枚举中追加 11 个 GPU 业务包类型,以便 后续 GPU 业务 Handler 能通过 `interestPackageTypes()` 精确过滤。

**验收标准(AC)**:
```gherkin
Given cloud-basic 模块的 PacketType.java 当前定义
When 开发追加 11 个新枚举值(100-110)
Then 编译通过,现有 12 个枚举值不受影响
And 枚举值无重复、无冲突
And 配套 JavaDoc 注释每个枚举的语义
```

**技术备注**:
- 文件:`ruyuan-cloud-basic/src/main/java/com/xx/cloud/basic/network/enums/PacketType.java`
- 新增值:`GPU_TASK_SUBMIT(100)` ~ `GPU_TASK_STREAM_CHUNK(110)`,共 11 个
- 完整列表见 [linshu-ai-infra.md §12.4.4](linshu-ai-infra.md#1244-新增-packettype-定义扩展现有枚举)

**依赖**:无
**估时**:0.5 人日
**Owner**:Java 后端

---

#### STORY-1-2 定义 GPU 业务 Protobuf 协议

**User Story**:
> 作为 **平台开发工程师**,我需要 定义 `gpu.pool.v1` Protobuf 协议文件,以便 Client / Gateway / Scheduler / Worker 四方有统一的业务消息格式。

**验收标准(AC)**:
```gherkin
Given GPU 业务场景
When 定义 .proto 文件
Then 包含 11 个核心消息体(SubmitTask/QueryTask/CancelTask/WorkerHeartbeat/OpRegister 等)
And Java_package 为 com.xx.cloud.gpu.rpc
And 通过 protobuf-maven-plugin 自动生成 Java 类
And 编译产物在 target/generated-sources 中
```

**技术备注**:
- 新模块:`linshu-gpu-proto`(独立 jar,被 Client/Gateway/Scheduler/Worker 共同依赖)
- 文件:`src/main/proto/gpu_pool.proto`
- 完整字段定义见 [linshu-ai-infra.md §12.1](linshu-ai-infra.md#121-protobuf-协议适配基于现有-netty-通信组件扩展)

**依赖**:STORY-1-1
**估时**:1 人日
**Owner**:Java 后端

---

#### STORY-1-3 Client SDK 核心方法封装

**User Story**:
> 作为 **业务开发工程师**,我希望通过 一行代码提交 GPU 任务,以便 业务侧无需关心 Netty 连接、序列化、重试等细节。

**验收标准(AC)**:
```gherkin
Given Client SDK 引入业务项目
When 业务调用 GpuClient.submit(taskReq)
Then 内部封装:Netty 连接建立 → Protobuf 序列化 → NettyPacket 封装 → NetClient.sendSync()
And 提供 query(taskId) / cancel(taskId) 两个核心方法
And 网络异常时自动重试 3 次,指数退避
And 返回结果含 taskId/status/result/errorCode
And 单测覆盖率 ≥ 80%
```

**技术备注**:
- 文件:`linshu-gpu-client/src/main/java/com/xx/cloud/gpu/client/GpuClient.java`
- 复用 `NetClient` + `RequestPromise`,参考 [§12.0.4 示例代码](linshu-ai-infra.md#1204-复用示例client-sdk-提交任务的核心-6-行代码)
- Maven 坐标:`com.xx.cloud:linshu-gpu-client:0.1.0-SNAPSHOT`

**依赖**:STORY-1-2
**估时**:2 人日
**Owner**:Java 后端

---

#### STORY-1-4 Worker 端算子静态注册机制

**User Story**:
> 作为 **算法工程师**,我希望通过 配置文件声明算子清单,以便 Worker 启动时自动加载模型和注册算子,无需改动代码。

**验收标准(AC)**:
```gherkin
Given application.yml 配置算子清单
When Worker 启动
Then 解析 ops 列表,每个 op 包含 op_id/version/model_path/handler_class/min_gpu_mem_mb/require_compute_cap
And 解析 managed_devices 列表,声明 Worker 管理的 GPU 范围(默认 1 张,MVP 配置)
And 校验 schema_version 与本地缓存版本一致,不一致启动失败
And 加载模型到 managed_devices[0] 的 GPU 内存(支持 PyTorch / ONNX Runtime)
And 启动完成后上报 OpRegisterRequest(WorkerConfig) 到 Scheduler(详见 §12.6 Protobuf 定义)
And 提供 OpRegistry API 支持运行时热查询
And N:M 验证:在 application.yml 中把 managed_devices 改为 2 张,Worker 正常启动且心跳上报 managed_gpu_ids=["gpu-0","gpu-1"]
```

**技术备注**:
- 文件:`linshu-gpu-worker/src/main/resources/application.yml`
- 加载模式:启动期全量预加载(预热池,见 Q7 相关)
- 配置示例(MVP 1:1 + 未来 1:N 都覆盖):
  ```yaml
  gpu:
    managed_devices:        # Worker 管理的 GPU 范围(N:M 灵活,默认 1:1)
      - gpu_id: "gpu-0"
        isolation: "soft"    # soft / mps / mig
        mem_total_mb: 81920
        compute_cap: "9.0"
    topology_hint: "pcie-single"
    heartbeat_interval_sec: 5
  ops:
    - op_id: text_classification
      version: v1
      model_path: /models/bert-base-chinese
      handler_class: com.xx.cloud.gpu.ops.TextClassificationOp
      min_gpu_mem_mb: 4096
      require_compute_cap: ">=7.5"
  ```
- 关键澄清:**Scheduler 只看 worker_id,不看 gpu_id**;Worker 内部用 CUDA_VISIBLE_DEVICES 选物理卡。详见 [§2.2.4 关系表](linshu-ai-infra.md#⚠️-worker-↔-gpu-↔-op-关系表v21-澄清) 与 [§12.6 WorkerConfig Protobuf](linshu-ai-infra.md#126-worker-启动配置协议v21-新增)

**依赖**:STORY-1-2
**估时**:2 人日
**Owner**:Java 后端 + 算法

---

#### STORY-1-5 单元测试基建(编解码 / 心跳 / 重连)

**User Story**:
> 作为 **Tech Lead**,我需要 MVP 通信层的关键路径有单测覆盖,以便 重构时不破坏现有契约。

**验收标准(AC)**:
```gherkin
Given Netty 通信层
When 编写单元测试
Then 覆盖:NettyPacket 编解码往返一致性(100 个随机 packet)
And 覆盖:RequestPromise 同步等待 + 超时自动 markTimeout
And 覆盖:NetClient 连接断开后自动重连(retryTime=-1)
And 覆盖:GPU 业务 Protobuf 11 种消息体序列化/反序列化
And 关键类单测覆盖率 ≥ 80%
And CI pipeline 集成 surefire 报告
```

**技术备注**:
- 使用 JUnit 5 + Mockito + AssertJ
- 集成 `ruyuan-cloud-basic` 的 `NetServerTest` 模式作为基线
- 跑通 `mvn test -pl linshu-gpu-*`

**依赖**:STORY-1-1, STORY-1-2, STORY-1-3, STORY-1-4
**估时**:2 人日
**Owner**:Java 后端

---

### Sprint 2(Week 3-4):调度与任务执行

---

#### STORY-2-1 Gateway 服务端 Handler

**User Story**:
> 作为 **业务 Pod**,我通过 长连接提交任务到 Gateway,以便 享受高吞吐的异步通信能力。

**验收标准(AC)**:
```gherkin
Given Client SDK 通过 Netty 长连接连入
When 提交 SubmitTaskRequest
Then Gateway 的 GpuGatewayChannelHandler 接收并处理
And 业务 Handler interestPackageTypes = {100, 101, 102}(submit/query/cancel)
And 复用 NetServer + BaseChannelInitializer,不重写 Pipeline
And 接收后路由到 Scheduler(本期直接调用本地 Scheduler bean)
And 返回 SubmitTaskResponse 包含 taskId
```

**技术备注**:
- 文件:`linshu-gpu-gateway/src/main/java/com/xx/cloud/gpu/gateway/handler/GpuGatewayChannelHandler.java`
- 继承 `AbstractChannelHandler`,实现 `interestPackageTypes()` + `handlePackage()`
- 处理业务逻辑委派给 `GpuTaskService`(本地 Bean)

**依赖**:STORY-1-2, STORY-1-3
**估时**:2 人日
**Owner**:Java 后端

---

#### STORY-2-2 Scheduler 单实例版 + 资源视图

**User Story**:
> 作为 **调度器**,我需要 维护所有 Worker 的实时资源状态,以便 任务分配时能基于当前负载选最优节点。

**验收标准(AC)**:
```gherkin
Given 3 个 Worker 上报心跳
When Scheduler 接收 WorkerHeartbeatRequest
Then 更新内存 ConcurrentHashMap<workerId, GpuWorkerNode>
And 节点状态包含:mem_usage_rate / util_rate / queue_task_num / running_task_ids
And 15 秒未收到心跳 → 标记节点 UNHEALTHY 并剔除
And 接收心跳时反序列化 running_task_ids 与内存对账,不一致任务标记 ORPHAN
And 资源变更同步落 MySQL 的 gpu_worker_status 表
```

**技术备注**:
- 文件:`linshu-gpu-scheduler/src/main/java/com/xx/cloud/gpu/scheduler/resource/ResourceViewManager.java`
- 借鉴 `cloud-server` 的 `AbstractController.heartbeat()` 模式
- MySQL 表 DDL(对齐 [§12.6 WorkerConfig](linshu-ai-infra.md#126-worker-启动配置协议v21-新增)):
  ```sql
  CREATE TABLE gpu_worker_status (
      worker_id        VARCHAR(64) PRIMARY KEY,
      node_ip          VARCHAR(45),
      managed_gpu_ids  JSON,                       -- Worker 管理的 GPU 列表(N:M 灵活,默认 ["gpu-0"])
      isolation_mode   VARCHAR(16) DEFAULT 'soft', -- soft / mps / mig
      topology_hint    VARCHAR(32),                -- nvlink-domain / pcie-single / cross-node
      supported_ops    JSON,                       -- [{op_id, version, min_gpu_mem_mb, require_compute_cap, require_min_gpus}]
      mem_total_mb     BIGINT,                     -- 聚合所有 managed GPU
      mem_used_mb      BIGINT,
      mem_usage_rate   FLOAT,                      -- 派生字段,mem_used/mem_total
      util_rate        FLOAT,
      queue_task_num   INT,
      running_task_num INT,
      running_task_ids TEXT,
      last_heartbeat_ts BIGINT,
      health           VARCHAR(16) DEFAULT 'HEALTHY'
  );
  ```

**依赖**:STORY-1-2
**估时**:3 人日
**Owner**:Java 后端

---

#### STORY-2-3 负载均衡算法落地(GpuLoadBalancer)

**User Story**:
> 作为 **调度器**,我需要 在多 Worker 中选最闲的一个执行任务,以便 GPU 资源利用率最大化。

**验收标准(AC)**:
```gherkin
Given 任务 need_mem_bytes + client_node_ip
When 调度器调用 gpu_schedule_dispatch()
Then 按规则过滤:剔除 UNHEALTHY 节点 + 队列满节点 + 显存不足节点
And 按综合分数排序:mem_usage_rate×0.5 + util_rate×0.3 + queue_num_norm×0.2
And 同节点亲和优先(client_node_ip 命中则只在该节点 Worker 中选)
And 返回最优节点 + 原子扣减其配额(本地内存锁)
And 无可用节点返回 null,触发排队
```

**技术备注**:
- 直接参考 [§12.5 伪代码](linshu-ai-infra.md#125-负载均衡调度算法伪代码生产可用完整版)实现 Java 版
- 文件:`linshu-gpu-scheduler/src/main/java/com/xx/cloud/gpu/scheduler/loadbalance/GpuLoadBalancer.java`
- 单测覆盖 10+ 场景(同节点优先/无空闲/全满/单节点)

**依赖**:STORY-2-2
**估时**:2 人日
**Owner**:Java 后端

---

#### STORY-2-4 Worker 任务执行引擎(Java 版,MVP 简化)

**User Story**:
> 作为 **Worker**,我需要 接收调度器分发的任务并执行,以便 业务拿到推理结果。

**验收标准(AC)**:
```gherkin
Given Scheduler 下发 GPU_TASK_DISPATCH 包
When Worker GpuWorkerChannelHandler 接收
And 单任务显存 ≤ WorkerConfig 中所有 managed_gpu 的可用显存(否则拒绝并返回错误)
And Worker 根据内部拓扑(同卡 > 同节点 > 跨节点)选择 managed_devices 中的 GPU(CUDA_VISIBLE_DEVICES)
And 提交到 OpRegistry 获取对应 Op handler
And 调用 handler.execute(input, op_param) 执行业务逻辑
And 任务执行结果序列化为 QueryTaskResponse 回传 Gateway
And 异常时回传 FAILED 状态 + 错误信息
And 单任务最长 180s(推理默认,见 Q7)
```

**技术备注**:
- 文件:`linshu-gpu-worker/src/main/java/com/xx/cloud/gpu/worker/handler/GpuWorkerChannelHandler.java`
- MVP 阶段 Worker 也用 Java(后续 Sprint 替换为 Rust,见 [§8.6 Python Op 集成开销](linshu-ai-infra.md#86-⚠️-python-op-集成开销新增))
- 简化处理:不实现 Python Sidecar,直接在 JVM 调 Python 通过 Jep(后续替换)

**依赖**:STORY-1-4, STORY-2-1
**估时**:3 人日
**Owner**:Java 后端 + 算法

---

#### STORY-2-5 端到端联调:1 个推理业务接入

**User Story**:
> 作为 **PM**,我需要 看到端到端跑通一个真实业务,以便 验证 MVP 技术方案可行性。

**验收标准(AC)**:
```gherkin
Given 1 个推理业务(text_classification Op)
When 业务 Pod 调用 GpuClient.submit()
Then Gateway 接收 → Scheduler 分配 → Worker 执行 → 结果回传 全链路 < 200ms
And 业务侧收到正确推理结果
And Prometheus 指标可观测到各阶段耗时
And 全链路日志含 trace_id 可关联
```

**技术备注**:
- 部署:3 Worker 节点 + 1 Gateway(2 副本)+ 1 Scheduler(单实例)+ 1 MySQL
- 业务接入示例:`linshu-gpu-demo` 跑 BERT 中文文本分类
- 验证脚本:`scripts/e2e_test.sh` 提交 100 个请求,统计成功率/P99

**依赖**:STORY-2-1, STORY-2-2, STORY-2-3, STORY-2-4
**估时**:2 人日
**Owner**:全员

---

### Sprint 3(Week 5-6):故障注入与可观测

---

#### STORY-3-1 故障场景演练

**User Story**:
> 作为 **SRE**,我需要 验证系统在常见故障下能自愈,以便 生产前消除稳定性隐患。

**验收标准(AC)**:
```gherkin
Given MVP 集群已部署
When 注入以下故障
Then 覆盖以下场景:
  - Worker 进程崩溃 → 自动剔除 + 任务重试到其他 Worker
  - Worker ↔ Gateway 长连接断开 → 任务标记 ORPHAN + 资源释放
  - 任务执行超时(>180s)→ 强制终止 + 释放显存 + 返回 TIMEOUT
  - MySQL 主从切换 → Scheduler 自动重连 + 内存状态重新加载
  - 1 个 Worker OOM → 仅影响该 Worker,其他 Worker 正常服务
And 每个故障场景跑 5 次,100% 自愈
And 故障注入到完全恢复时间 < 30s
```

**技术备注**:
- 使用 ChaosBlade / Chaos Monkey 注入故障
- 测试环境:独立 staging 集群,避免影响生产
- 演练 Runbook:`docs/chaos-drill-v1.md`

**依赖**:STORY-2-5
**估时**:3 人日
**Owner**:Java 后端 + SRE

---

#### STORY-3-2 Prometheus 基础指标埋点

**User Story**:
> 作为 **SRE**,我需要 关键路径有 RED 指标(请求数/错误数/耗时),以便 实时监控系统健康度。

**验收标准(AC)**:
```gherkin
Given 4 个组件(Gateway/Scheduler/Worker/MySQL)
When 部署 micrometer-registry-prometheus
Then 暴露以下指标:
  - Gateway: gpu_submit_total / gpu_submit_fail_total / gpu_submit_latency_seconds
  - Scheduler: gpu_dispatch_total / gpu_queue_depth / gpu_active_workers
  - Worker: gpu_task_running / gpu_task_completed / gpu_task_failed / gpu_task_timeout
  - JVM: jvm_memory_used_bytes / jvm_gc_pause_seconds
And Prometheus 抓取间隔 15s 成功
And 关键指标(4 个黄金信号)在 Grafana 可视化
```

**技术备注**:
- 引入依赖:`micrometer-registry-prometheus`(复用 spring-boot-starter-actuator)
- 端点:`/actuator/prometheus`
- ServiceMonitor YAML:`k8s/servicemonitor.yaml`

**依赖**:STORY-2-5
**估时**:2 人日
**Owner**:Java 后端 + SRE

---

#### STORY-3-3 Grafana Dashboard 落地

**User Story**:
> 作为 **SRE / 算法负责人**,我需要 一屏看到全链路健康度,以便 快速定位异常。

**验收标准(AC)**:
```gherkin
Given Prometheus 已采集指标
When 配置 Grafana
Then 提供 3 个 Dashboard:
  - Overview: 全局吞吐 / 成功率 / P99 延迟 / Worker 利用率
  - Task Detail: 单任务全链路 trace + 各阶段耗时拆解
  - Resource: GPU 显存/算力使用率 / 任务队列长度 / 节点健康度
And Dashboard 支持时间范围选择 + 维度过滤(biz_code/op_id)
And 关键 SLI 阈值线标注:成功率 < 99% 红线,P99 > 200ms 黄线
```

**技术备注**:
- Grafana 模板导出为 JSON:`grafana/dashboards/*.json`
- 通过 ConfigMap 挂载到 Grafana Pod
- 部署文档:`docs/grafana-deploy.md`

**依赖**:STORY-3-2
**估时**:1.5 人日
**Owner**:SRE

---

#### STORY-3-4 运维手册初版 + 故障 Runbook

**User Story**:
> 作为 **新加入的 SRE**,我需要 完整的运维手册,以便 独立处理日常运维和常见故障。

**验收标准(AC)**:
```gherkin
Given MVP 已部署到生产
When 编写文档
Then 包含 4 个文档:
  1. 部署手册(DEPLOY.md):环境准备 / K8s 部署 / 数据库初始化 / 配置说明
  2. 运维手册(OPERATIONS.md):日常巡检 / 扩容缩容 / 版本升级 / 备份恢复
  3. 故障 Runbook(RUNBOOK.md):10+ 常见故障的排查步骤 + 恢复命令
  4. 监控手册(MONITORING.md):指标说明 / Dashboard 使用 / 告警规则
And 每个 Runbook 含:现象 / 原因 / 排查命令 / 恢复步骤 / 升级预防
And 通过内部 Review,PM + SRE 双签
```

**技术备注**:
- 故障 Runbook 必须覆盖 STORY-3-1 演练的所有场景
- 模板参考 Google SRE Book 第 11 章
- 存放路径:`docs/{DEPLOY,OPERATIONS,RUNBOOK,MONITORING}.md`
- CI 校验:文档中所有命令必须可执行,执行后产生预期输出

**依赖**:STORY-3-1, STORY-3-2, STORY-3-3
**估时**:2 人日
**Owner**:Tech Lead + SRE

---

## 3. Story 总览

| Sprint | Week | Story 列表 | 总估时 | 关键里程碑 |
|---|---|---|---|---|
| Sprint 1 | W1-W2 | 1-1, 1-2, 1-3, 1-4, 1-5 | 7.5 人日 | 通信层联通,单测覆盖 80%+ |
| Sprint 2 | W3-W4 | 2-1, 2-2, 2-3, 2-4, 2-5 | 12 人日 | 端到端跑通 1 个推理业务 |
| Sprint 3 | W5-W6 | 3-1, 3-2, 3-3, 3-4 | 8.5 人日 | 故障自愈 + 可观测 + 文档齐备 |

**总计**:6 周,28 人日(假设 1 名 Java 全栈 + 1 名 SRE + 0.5 名算法兼职 = 2.5 FTE × 6 周 ≈ 15 人日有效工时,需要并行 + 加班消化)

## 4. 跨 Story 依赖矩阵

```
STORY-1-1 ──→ STORY-1-2 ──┬──→ STORY-1-3 ──┐
                          ├──→ STORY-1-4 ──┤
                          │                ├──→ STORY-1-5
                          │                │
                          │                ↓
                          │            STORY-2-1 ──→ STORY-2-4
                          │                ↑            ↓
                          │           STORY-2-2 ──→ STORY-2-3
                          │                             ↓
                          └──────────────────→ STORY-2-5 (E2E 联调)
                                                          ↓
                                              ┌───────────┴───────────┐
                                              ↓                       ↓
                                         STORY-3-1            STORY-3-2
                                         (故障演练)            (指标埋点)
                                              │                       │
                                              └─────→ STORY-3-3 ←─────┘
                                                       (Dashboard)
                                                          ↓
                                                     STORY-3-4
                                                  (运维手册 + Runbook)
```

## 5. 风险与备选

| Story | 风险 | 备选方案 |
|---|---|---|
| STORY-2-4 | Java Worker + Jep 调 Python 性能可能不足 | 提前压测,若 P99 > 500ms 则在 Sprint 3 末尾插入一个 Spike Story 评估 Rust 化收益 |
| STORY-2-5 | 真实业务接入比预期复杂 | 准备一个 mock 业务(纯 echo)做 E2E,真实业务降级为可选 |
| STORY-3-1 | 故障注入可能误伤 staging | 用独立的 namespace + 资源限制隔离,演练前通知所有相关方 |

## 6. Definition of Done(MVP 级别)

每个 Story 完成必须满足:
- [ ] 代码合并到 main 分支,CI 全绿
- [ ] 单测覆盖率达标(关键类 ≥ 80%)
- [ ] 文档同步更新(README / API 文档 / Runbook)
- [ ] Tech Lead Code Review 通过
- [ ] PM 验收通过(Demo + AC 全过)
- [ ] 部署到 staging 环境并稳定运行 24h

## 7. 后续 Sprint 衔接

Sprint 3 结束后,进入第二阶段(4 周)能力完善期,见 [§7.2](linshu-ai-infra.md#72-第二阶段能力完善-4-周)。关键 Story 预告:
- 多副本调度器 + ControllerManager 选主改造
- 优先级抢占 + 同节点亲和
- 显存硬隔离(MPS)
- 大张量 out-of-band 传输
- Python Op Sidecar 改造(Worker 拆分为 Java + Python)

具体 Story 在第二阶段启动前按相同模板细化。
