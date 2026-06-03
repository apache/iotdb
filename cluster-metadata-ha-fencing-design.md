# ConfigNode→DataNode 元数据广播的高可用改造：租约 + Fencing 统一方案

> 状态：待 review
> 关联文档：`table-model-ddl-ha-analysis.md`（表模型 DDL 的根因分析，本文是其推广到全集群的通用方案）
> 适用版本：2.0.x（master）
> 修订记录：
> - v2 — 移除 epoch 代次与 CN 侧 laggards，简化为"两支柱（Lease + Verdict）"。
> - v3 — 移除 `tier_b_fail_closed` / `enable_metadata_lease_fencing` 配置；放行判定改用 `hbAge` 信号。
> - v4 — `T_fence` 默认 20s（与 `failureDetectorFixedThresholdInMs` 对齐）；Tier-A 内拆"加性即时放行 / 破坏类等待"。
> - **v5（本版，依据第二轮 code-grounded review）**：
>   1. **修正 hbAge 安全证明**：现有样本记录的是心跳**发送时刻**（echoed `heartbeatTimestamp`），且 `onError` 用**当前时刻**写 Unknown 样本——都不能用于判 fence。改为 CN 侧新增**专用"最近成功收到心跳响应时刻"**（收到响应时用 CN 本地时钟打点，仅成功更新、绝不被失败样本前移），并显式声明有界延迟 + 心跳连接双向对称假设（§2.3/§2.6）。
>   2. **取消"加性即时跳过"**（评审点 2/4）：它会制造"未 fenced 的 laggard"（Running 但漏广播、又被跳过 → 新 schema 长期不可见）。改为**所有 Tier-A 统一"未 ack 必须 ack 或 已证实 fenced"**。代价：任一 DN 不可达时所有 Tier-A 都等 `T_proceed`（放弃 v4 的加性快路径）。
>   3. **Tier-B 按 加资源 vs 撤资源/降级/控制 再分**（评审点 3）：DROP UDF/Trigger/Plugin、SET SYSTEM STATUS ReadOnly 这类按 Tier-A 强一致处理（陈旧 DN 会继续跑旧资源 / 继续写入，不是良性漂移）。
>   4. **能力位先于一切判定**（评审点 4）：不支持 fencing 的旧 DN 一律 UNSAFE。
>   5. **FENCED-SAFE 条件收紧**（评审点 5）：`Removing` 不等于已 fence；须"已移出路由、不再接受 client"或"DN 显式 ack fence/shutdown"。

## 0. TL;DR

- 范围：**整个 CN→所有 DN 的元数据广播**，穷尽排查出**约 30 个操作**（四种失败语义 HARD-FAIL / RETRY-THEN-FAIL / SOFT / TIMEOUT-ABANDON）。
- 共性：CN 提交元数据后广播"失效/更新"给**所有**已注册 DN；DN 上一批**被 CN 推送维护的本地缓存**被读写关键路径**直接信任**——"活着但分区"的陈旧 DN 会产生脏数据 / 错误结果 / 安全漏洞。
- 现状缺陷：把"DN 真宕机"（缓存随进程消失，安全）与"DN 活着但分区"（缓存仍在，危险）混为一谈而一并失败（牺牲可用性）；同时对 SOFT/TIMEOUT 类又静默放过（留下长期不一致与权限漏洞）。
- 统一方案两根支柱：
  1. **Lease/Fence（DN 侧）**：DN 的 CN-推送缓存只在"持有有效心跳租约"期间可信；`T_fence` 内收不到 CN 心跳即自我隔离（作废缓存 + Tier-A fail-closed），恢复时 **DN 自驱 resync** 后再解除。
  2. **Verdict（CN 侧）**：把 ~20 处散落的"任一失败即 setFailure"收敛到一个统一判定器：未 ack 的 DN **要么 ack，要么已证实 fenced（或已移出路由）**，否则 WAIT/FAIL；据此输出 PROCEED / WAIT / FAIL。
- **不引入 epoch/laggards**（你已确认的取舍）：靠"不跳过未 ack 的 Running DN"+ 修正后的"成功响应接收时刻"信号 + 有界延迟/连接对称假设来保证正确性（§2.3）。代价：任一 DN 不可达时所有 Tier-A DDL 等 `T_proceed≈25s`（全员存活恒零等待）。

---

## 1. 问题范围：到底有哪些操作"这样"

排查口径：CN 侧获取 `getRegisteredDataNodeLocations()`（所有已注册 DN，**不按存活过滤**）→ 广播失效/更新 RPC（异步 `CnToDnInternalServiceAsyncRequestManager.sendAsyncRequestWithRetry`，或同步 `ConfigNodeProcedureEnv.invalidateCache` / `SyncDataNodeClientPool`）→ 根据响应决定成败。

> 异步重试上限 `MAX_RETRY_NUM = 6`（`AsyncRequestManager.java:56`）；DN 只有回 SUCCESS 才从重试集合移除（`AsyncRequestContext` Javadoc L51），不可达 DN 由 `DataNodeTSStatusRPCHandler.onError`（L73-90）写入一个 `EXECUTE_STATEMENT_ERROR` 状态，故调用方 squash 后必见失败 → 触发各自的失败处理。

### 1.1 全量操作清单（按失败语义分组）

**A. HARD-FAIL — 任一 DN 不可达即整体失败 + 回滚（正确性敏感，本提案主目标）**

| 操作 | 类 / 入口 | 广播 RPC | 目标 DN 缓存 |
|---|---|---|---|
| 表模型 DDL（CREATE/ADD/DROP/SET/RENAME/DROP TABLE 等） | `procedure/impl/schema/table/*`（见上一篇） | `UPDATE_TABLE` / `INVALIDATE_TABLE_CACHE` / `INVALIDATE_COLUMN_CACHE` | `DataNodeTableCache` / `TableDeviceSchemaCache` |
| DELETE TIMESERIES | `DeleteTimeSeriesProcedure`（`CLEAN_DATANODE_SCHEMA_CACHE` L126-128，helper L194-221） | `INVALIDATE_MATCHED_SCHEMA_CACHE` | 树 schema cache |
| DELETE DATABASE | `DeleteDatabaseProcedure`（`INVALIDATE_CACHE` L100-109）→ `ConfigNodeProcedureEnv.invalidateCache` L164-221（**同步串行**，Unknown 重试 10×500ms 后 false） | `INVALIDATE_PARTITION_CACHE` + `INVALIDATE_SCHEMA_CACHE` | 分区 cache + 树/表 schema cache |
| DEACTIVATE TEMPLATE | `DeactivateTemplateProcedure`（L183-206） | `INVALIDATE_MATCHED_SCHEMA_CACHE` | 树 schema cache |
| UNSET TEMPLATE | `UnsetTemplateProcedure`（`executeInvalidateCache` L157-181） | `UPDATE_TEMPLATE`（`INVALIDATE_TEMPLATE_SET_INFO`） | `ClusterTemplateManager` |
| SET TEMPLATE（pre/commit/rollback 三处） | `SetTemplateProcedure`（L223-242 / L408-433 / L510-538） | `UPDATE_TEMPLATE`（`ADD_TEMPLATE_PRE_SET_INFO`/`COMMIT_TEMPLATE_SET_INFO`/`INVALIDATE_TEMPLATE_SET_INFO`） | `ClusterTemplateManager` |
| SET / UNSET TTL | `SetTTLProcedure`（`UPDATE_DATANODE_CACHE` L90-129；rollback L222-250） | `SET_TTL` | `DataNodeTTLCache` |
| ALTER LOGICAL VIEW | `AlterLogicalViewProcedure`（两次 `invalidateCache` L100-103、L193；helper L125-146） | `INVALIDATE_MATCHED_SCHEMA_CACHE` | 树 schema/view cache |
| DELETE LOGICAL VIEW | `DeleteLogicalViewProcedure`（L169-189） | `INVALIDATE_MATCHED_SCHEMA_CACHE` | 树 schema/view cache |
| ALTER ENCODING/COMPRESSOR | `AlterEncodingCompressorProcedure`（`CLEAR_CACHE` L134-138，复用 DeleteTimeSeries helper） | `INVALIDATE_MATCHED_SCHEMA_CACHE` | 树 schema cache |
| ALTER TIMESERIES DATATYPE | `AlterTimeSeriesDataTypeProcedure`（`CLEAR_CACHE` L119-133，helper L246-273；**不支持回滚**） | `INVALIDATE_MATCHED_SCHEMA_CACHE` | 树 schema cache |
| DELETE DEVICES（表模型） | `DeleteDevicesProcedure`（L224-245） | `INVALIDATE_MATCHED_TABLE_DEVICE_CACHE` | `TableDeviceSchemaCache` |
| CREATE/DROP FUNCTION（UDF） | `UDFManager.createFunction/dropFunction`（**非 procedure**，L135-138 / L180-183 squash 即返回错误） | `CREATE_FUNCTION` / `DROP_FUNCTION` | `UDFManagementService` |
| CREATE/DROP/ACTIVE TRIGGER | `CreateTriggerProcedure` / `DropTriggerProcedure`（env L553-595） | `*_TRIGGER_INSTANCE` | `TriggerManagementService` |
| CREATE/DROP PIPE PLUGIN | `CreatePipePluginProcedure`（L206-215）/ `DropPipePluginProcedure`（L173-180） | `CREATE_PIPE_PLUGIN` / `DROP_PIPE_PLUGIN` | `PipePluginAgent` |
| SET SPACE / THROTTLE QUOTA | `ClusterQuotaManager`（L88-96 / L196-203，**非 procedure**，squash 即返回） | `SET_SPACE_QUOTA` / `SET_THROTTLE_QUOTA` | DN 配额执行缓存 |
| SET SYSTEM STATUS / LOAD CONFIG / SET CONFIG / START-STOP REPAIR | `NodeManager`（L1044-1196，**非 procedure**） | `SET_SYSTEM_STATUS` / `LOAD_CONFIGURATION` / `SET_CONFIGURATION` / `*_REPAIR_DATA` | DN 运行态/配置 |

**B. RETRY-THEN-FAIL — 重试 1 次后失败（已部分容忍，但仍会失败）**

| 操作 | 类 | 广播 RPC |
|---|---|---|
| CREATE/DROP/ALTER TOPIC、CREATE/DROP CONSUMER GROUP | `AbstractOperateSubscriptionProcedure` 子类（RETRY_THRESHOLD=1） | `TOPIC_PUSH_*` / `CONSUMER_GROUP_PUSH_*` |

**C. SOFT — 仅告警，靠后台 reconcile（已可用，但一致性靠周期任务兜底）**

| 操作 | 类 | 说明 |
|---|---|---|
| CREATE/START/STOP/DROP/ALTER PIPE | `AbstractOperatePipeProcedureV2` 子类 | 失败 warn，靠 `PipeMetaSyncProcedure` 心跳周期 reconcile |
| `extendSchemaTemplate` | `ClusterSchemaManager` L1103-1190（**非 procedure**） | 返回错误状态但不回滚，CN 与 DN 可漂移 |

**D. TIMEOUT-ABANDON — 超时后静默丢弃（隐患最大）**

| 操作 | 类 | 说明 |
|---|---|---|
| GRANT/REVOKE/DROP USER·ROLE/ALTER USER 等权限变更 | `AuthOperationProcedure`（L98-127） | 每 DN 单独同步重试，超 `datanodeTokenTimeoutMS`（默认 **180s**）即**静默移除**该 DN。该 DN 权限缓存**长期陈旧**直到重启或下次相关 auth 操作——**安全正确性漏洞** |

> 排除项（确认不属于本问题）：`CreateCQProcedure`（无 DN 广播，仅写 CN consensus + 本地调度）；区域级 RPC（`CREATE_*_REGION`/`DELETE_REGION`/`CHANGE_REGION_LEADER` 等，只发副本所在 DN，走数据面 quorum，不在本提案范围）。

### 1.2 DN 上"被 CN 推送维护"的缓存全集（即需要被租约/fencing 覆盖的对象）

| 缓存 / 注册表 | 类 | 由哪些 CN RPC 改写 | 失效后回源方式 | 类型 |
|---|---|---|---|---|
| 表 schema | `DataNodeTableCache` | `UPDATE_TABLE`/`INVALIDATE_TABLE_CACHE`/`INVALIDATE_SCHEMA_CACHE` | 回 CN 拉 + 启动 `init(tableInfo)`（`DataNode.java:523`） | 注册表型 |
| 表设备属性/last | `TableDeviceSchemaCache` | `INVALIDATE_TABLE_CACHE`/`INVALIDATE_COLUMN_CACHE`/`INVALIDATE_MATCHED_TABLE_DEVICE_CACHE`/`INVALIDATE_LAST_CACHE` | 懒加载，回 schema region | 懒加载型 |
| 树 schema/last/view | `TreeDeviceSchemaCacheManager` | `INVALIDATE_SCHEMA_CACHE`/`INVALIDATE_MATCHED_SCHEMA_CACHE`/`INVALIDATE_LAST_CACHE` | 懒加载，回 schema region | 懒加载型 |
| 权限 | `AuthorityChecker`/`AuthorityFetcher` | `INVALIDATE_PERMISSION_CACHE`（同步） | 懒加载回 CN；心跳触发 `refreshToken()`（`DataNodeInternalRPCServiceImpl:2273`） | 懒加载型 |
| 模板 | `ClusterTemplateManager` | `UPDATE_TEMPLATE`/`INVALIDATE_SCHEMA_CACHE` | 启动 `updateTemplateSetInfo`（`DataNode.java:497`） | 注册表型 |
| TTL | `DataNodeTTLCache` | `SET_TTL` | 启动 `initTTLInformation`（`DataNode.java:516`） | 注册表型 |
| 分区路由 | `ClusterPartitionFetcher` | `INVALIDATE_PARTITION_CACHE`/`UPDATE_REGION_ROUTE_MAP` | 懒加载回 CN | 懒加载型 |
| UDF | `UDFManagementService` | `CREATE_FUNCTION`/`DROP_FUNCTION` | 启动 `prepareUDFResources`（`DataNode.java:784`，下载 jar） | 注册表型 |
| Trigger | `TriggerManagementService` | `*_TRIGGER_INSTANCE` | 启动 `prepareTriggerResources` | 注册表型 |
| Pipe Plugin | `PipePluginAgent` | `CREATE/DROP_PIPE_PLUGIN` | 启动 `preparePipeResources` | 注册表型 |
| 配额 | DN 配额执行缓存 | `SET_SPACE/THROTTLE_QUOTA` | 无主动回源（只能等下次推送） | 注册表型（需补回源） |

> "懒加载型 vs 注册表型"决定了 fence 恢复时的 resync 方式（§2.5）：懒加载型只需作废、后续按需回源即可；注册表型需在恢复服务前主动重拉。

**关键事实（决定方案可行性，已据 review 核对代码更正）**：
- 心跳为 **CN→DN**，仅 Raft leader 发送（`HeartbeatService.java:128`），默认 **1s** 一次（`heartbeatIntervalInMs=1000`）。
- DN 失联判定：默认 `FixedDetector`，阈值 **20s**（`failureDetectorFixedThresholdInMs=20000`）→ `NodeStatus.Unknown`；可选 Phi-Accrual。`NodeStatus` 仅 `Running/Unknown/Removing/ReadOnly`。
- **⚠ 现有心跳样本时间戳不能直接用于判 fence**：`HeartbeatService.genHeartbeatReq` 写 `heartbeatTimestamp = System.nanoTime()`（**发送时刻**），DN 原样回填，`NodeHeartbeatSample(resp)` 记录的就是这个**发送时刻**（`NodeHeartbeatSample.java:55`）；并且 `DataNodeHeartbeatHandler.onError` 在连接断开时用 **当前时刻** 写一个 `Unknown` 样本（`super(System.nanoTime())`）。→ 直接复用会导致：延迟心跳使 CN 以为 hbAge 已超而 DN 刚续约；失败样本不断把时间戳前移使 hbAge 反而不增长。**故本方案 CN 侧新增专用"最近成功收到心跳响应时刻"，见 §2.3/§2.6。**
- 心跳**当前不带任何"元数据版本"字段**；DN 重启 resync 是**纯 pull**（`storeRuntimeConfigurations` L486-530）。
- 真宕机 DN 重启必走 resync 重建缓存，**不可能用旧缓存服务**；缺的唯一一环就是"**活着但分区**的 DN 没有任何机制主动停用旧缓存"。

---

## 2. 统一机制设计：Lease + Fence（DN 侧）+ Verdict（CN 侧）

### 2.1 一句话模型

> **DN 的"CN-推送缓存"只有在它"持有有效心跳租约"时才可用于服务；租约失效即 fence（作废 + Tier-A fail-closed），fence 恢复时由 DN 自己先 resync 再解除。CN 在确信某未 ack 的 DN"已被租约隔离 / 已移出路由"后才跳过它继续提交，否则等待或失败。**

| 支柱 | 解决的失效场景 | 机制 |
|---|---|---|
| **Lease/Fence（DN 侧）** | DN **分区或宕机**（收不到心跳） | DN 本地计时（DN 自己的 receive-time）：`now - lastCnHeartbeat > T_fence` → fence；恢复时 DN 自驱 resync |
| **Verdict（CN 侧）** | CN 决定能否跳过未 ack 的 DN | 据 CN 侧"最近成功收到响应时刻"算 `hbAge`，结合能力位/路由状态判定 → PROCEED / WAIT / FAIL |

### 2.2 正确性分层（Tier）

- **Tier-A（脏数据 / 错误结果 / 安全）**：表 schema、树 schema、设备属性/last、模板、view、datatype/encoding、TTL、**权限**、分区路由。Fence = **作废 + fail-closed**：隔离期间相关读写/鉴权回 CN 现拉，CN 不可达则**拒绝**（宁可不可用，绝不写脏 / 不返脏 / 不放行越权）。CN 侧未 ack 的 DN 须"ack 或 已证实 fenced"才放行。
- **Tier-B（功能/一致性，非静默数据损坏）**，再按方向细分（评审点 3）：
  - **B-加资源（soft）**：CREATE FUNCTION/TRIGGER/PIPE PLUGIN、CREATE TOPIC/CONSUMER、配额上调等。漏掉 → 依赖该资源的操作自然失败（如 DN 没有该 UDF → 该查询失败），**不 fail-closed**，靠恢复 resync 收敛。
  - **B-撤资源/降级/控制（按 Tier-A 强一致）**：**DROP** FUNCTION/TRIGGER/PIPE PLUGIN（陈旧 DN 会**继续执行**已删资源）、**SET SYSTEM STATUS = ReadOnly**（陈旧 DN 会**继续接受写入**）、disable/控制类。这些不是良性漂移，须与 Tier-A 同等："未 ack 必须 ack 或 已证实 fenced"，必要时 fail-closed（fence 期间该 DN 本就拒服务）。

### 2.3 未 ack DN 的处理：为什么"统一等待/失败"且不引入 epoch/laggards

回应评审点 1/2/4。纯靠 hbAge 时序"推断 DN 已 fence"不够稳健，且"跳过未 ack DN"会制造**未 fenced 的 laggard**（如 DN 漏掉 CREATE TABLE/ADD COLUMN 广播但心跳仍 Running——它不会自我 fence，而 `DataNodeTableCache.getTable` 仅在 pre-update map 命中时才回 CN 拉取，否则直接返回/抛不存在 `DataNodeTableCache.java:329`，导致新 schema 长期不可见）。本版采用更保守、更简单的规则：

1. **取消"加性即时跳过"**：所有 Tier-A 统一走"未 ack 的 DN 必须 ack 或 已证实 fenced（或已移出路由）"。
   - 未 ack 且 **心跳仍新鲜（仍可能在服务）** → UNSAFE → **WAIT/FAIL**，绝不跳过 → 不会留下陈旧 laggard。
   - 未 ack 且 **不可达**（收不到心跳）→ `T_fence` 后自我 fence；CN 据"已证实 fenced"放行；该 DN 恢复时由本地 fence 标志驱动 resync 自愈。
   - 由此 ADD COLUMN 不再需要单独证明"旧 schema 下不带新列的写入语义安全"（也无法对"新增 TAG 列改变设备身份"等情形一概证明）——它和其他 Tier-A 一样必须 ack-或-fenced。
2. **修正 CN 侧"联系"信号**：新增**专用"最近成功收到心跳响应时刻" `lastResp(dn)`**——CN 在**收到成功响应时**用 **CN 本地时钟**打点，**只在成功时更新，绝不被 `onError`/Unknown 样本前移**（与现有 load-cache 样本分离，不复用 §1.2 所述的发送时刻样本）。由因果关系 `dn_renew ≤ lastResp`（DN 先收到请求并续约 → 回响应 → CN 收到），故 `lastResp` 是 DN 续约时刻的**可靠上界**。
3. **不引入 epoch/laggards**：既不跳过未 ack 的活跃 DN，就不存在"Running 但陈旧被放行"需要 epoch 兜底；不可达 DN 由 fence + 恢复 resync 自愈，CN 无需记 laggards。**代价**：放弃 v4 的加性快路径——任一 DN 不可达时所有 Tier-A 都等 `T_proceed`（§2.6）。

**显式前提假设（必须写明）**：上述时序论证依赖
- **(a) 有界延迟**：在途心跳/RPC 在 Δ 内送达或被丢弃；
- **(b) 心跳连接双向对称**：心跳是同一连接上的请求/响应，"DN 收到请求并续约" ⟺ "CN 收到响应"（modulo Δ）。于是"DN 在续约" ⟺ "CN 在持续收到响应" ⟺ "`hbAge` 小" ⟺ "不判 FENCED-SAFE"。
- 在 (a)(b) 下，"单向可达 + 选择性丢失失效广播"（CN→DN 投递心跳却丢弃失效、DN→CN 响应丢失）这一可能让"DN 续约却漏失效、而 CN 误判 fenced"的危险态不会发生。**若要去除该假设**，需引入 epoch/token 正向确认 currency（本版按选择不做，作为未来增强备选）。

### 2.4 CN 侧统一判定器（取代 ~20 处散落的 setFailure）

```
enum Verdict { PROCEED, WAIT, FAIL }

// lastResp(dn): CN 本地时钟记录的"最近一次成功收到该 DN 心跳响应"时刻；
//               仅成功响应更新，绝不被 onError/Unknown 样本前移（§2.3，评审点 1）。
// hbAge(dn)   = now - lastResp(dn)。
// 统一规则：没有"加性跳过"；能力位先判（评审点 4）；FENCED-SAFE 条件收紧（评审点 5）。
Verdict propagate(payload, opts):
    resp = sendAsyncRequestWithRetry(allRegisteredDNs, rpc)
    for dn not ACKED:
        if isRetiredFromRouting(dn) or ackedFenceOrShutdown(dn):
                                                  -> SAFE_GONE   // 已移出路由/不再接受 client，或显式 ack fence/shutdown（评审点 5）
        elif not supportsFencing(dn):             -> UNSAFE      // 旧 DN 不会自我 fence → 回退严格语义（能力位先判，评审点 4）
        elif hbAge(dn) >= T_proceed:              -> FENCED_SAFE // 已证实自我 fence（T_proceed = T_fence + margin）
        else:                                     -> UNSAFE      // 心跳仍新鲜（仍可能服务）或瞬时错误 → 再等
    if all ACKED / SAFE_GONE / FENCED_SAFE:  return PROCEED
    if waited > maxWait:                     return FAIL          // 半坏 DN：心跳新鲜却持续失败 → 失败（保守正确）
    return WAIT                                                   // 循环本状态；hbAge 随时间增长，到阈值转 FENCED_SAFE
```

要点：
- **能力位先判**：不支持 fencing 的 DN 永远不会被判 FENCED-SAFE（评审点 4，解决 v4 自相矛盾）。
- **FENCED-SAFE 仅来自**：`hbAge ≥ T_proceed`（且支持 fencing）**或** 已移出路由/显式 ack——`Removing` 本身不算（评审点 5，`Removing` 仍可能服务 client）。
- **不再有"加性跳过"**：未 ack 的活跃 DN 一律 UNSAFE → WAIT/FAIL（评审点 2）。
- **PROCEED**：所有未 ack 的 DN 都已 ack / SAFE_GONE / FENCED_SAFE 才提交。
- **WAIT**：存在 UNSAFE（hbAge 未到 `T_proceed`，可能仍在服务）。循环等待至其 ack / 变 FENCED_SAFE / 超 `maxWait`。
- **FAIL**：超 `maxWait` 仍有 UNSAFE（典型：心跳通但广播 RPC 持续失败的半坏 DN）→ 失败，维持现状语义。

各 procedure 把"任一失败即 setFailure"替换为"调用 `propagate(...)` 并按 Verdict 驱动状态机"；`DeleteDatabaseProcedure`（同步串行）与 `AuthOperationProcedure`（180s 静默丢弃）统一切到这套——**修复权限 D 类漏洞**：不再静默丢弃，未 ack 的活跃 DN 会 WAIT/FAIL，不可达 DN fence 后权限缓存作废、恢复 resync。

### 2.5 DN 侧改造

1. **记录租约（DN 本地，sound）**：`getDataNodeHeartBeat`（`DataNodeInternalRPCServiceImpl.java:2226`）记录 `lastCnHeartbeatNanos = System.nanoTime()`（DN 自己的 receive-time，与 CN 侧信号无关）。DN 用它判自己是否该 fence——这一侧从来不是问题所在；评审点 1 针对的是 CN 侧的推断信号。
2. **Fence 触发**：采用**惰性检查**（读写/鉴权入口处 `now - lastCnHeartbeatNanos > T_fence` 即视为 fenced），无需后台线程、无并发作废与读者竞争。（已实现：`MetadataLeaseManager.isFenced()`。）
3. **Tier-A fail-closed 注入点**（fenced 期间，**Phase 1 已全部落地、各带 TDD**）：
   - **表 schema**：`DataNodeTableCache.getTableInWrite/getTable` 抛 `INTERNAL_REQUEST_RETRY_ERROR`（推送型缓存、无回源，只能硬失败）。`TableHeaderSchemaValidator` 的所有读都经此路径，故**自动覆盖**，无需单独注入。
   - **树 schema**：`TreeDeviceSchemaCacheManager` 六个读方法统一经 `getDeviceSchemaOrMissWhenFenced(...)`，fenced 时报 **cache miss → 回源到 quorum 支撑的 SchemaRegion**（读穿透型缓存，回源即权威；比硬失败更可用：SchemaRegion 多数派可达即成功，不可达才 fail-closed）。恢复时 `cleanUp` 作废分区期间未复读的旧条目。
   - **权限**：`ClusterAuthorityFetcher.checkCacheAvailable()` 在 `isFenced()` 时丢弃权限缓存并回源 CN（分区时回源失败→拒绝）。补上了原 `refreshToken()` 超时机制的盲区（它仅在"心跳恢复"那刻才标记失效，分区进行中不触发）。
   - **TTL**：`MultiTsFileDeviceIterator.nextDevice()` fenced 时用无穷大 TTL（compaction 不按 TTL 删除），**仅压制 compaction 删除路径**（查询/写入 TTL 行为不变），防止陈旧 TTL 造成不可逆数据删除。
   - **分区缓存**：未覆盖（低风险，已有分区不变更、miss 即回源 CN，分区时自然 fail-closed）；按需再评估。
4. **恢复时 DN 自驱 resync（事件驱动）**：心跳恢复（fenced→active 的那次心跳）触发已注册的 recovery listener（已实现：`MetadataLeaseManager.addLeaseRecoveryListener`，`DataNodeTableCache` 注册 `invalidateAll`）。懒加载型缓存作废后按需回源；注册表型主动重拉（复用启动 resync 路径）。listener 同步执行：解除 fence 前缓存已作废，无窗口。
5. **重启**：已全量 resync（`storeRuntimeConfigurations`），开机即一次完整追平。

### 2.6 时序正确性（lease ordering）

记 `T_hb`=心跳间隔（默认 1s）、`T_fence`=DN 自我隔离阈值、`T_proceed = T_fence + margin`=CN 判 FENCED-SAFE 所需 `hbAge`。

**安全不变式**：CN 提交一次 Tier-A 变更（尤其**不可逆物理删除**）时，对每个未 ack 的 DN，要么它已 ack，要么它**已证实 fenced 或已移出路由**（不再用旧缓存服务）。

**证明（用修正后的信号）**：`lastResp(dn)` 是 CN 收到成功响应的本地时刻，由因果关系 `dn_renew ≤ lastResp`（DN 续约在前、CN 收响应在后）。DN 在 `dn_renew + T_fence` 自我 fence。故当 `hbAge = now - lastResp ≥ T_fence + margin` 时，`now ≥ lastResp + T_fence ≥ dn_renew + T_fence`，DN 必已 fence。取 `T_proceed = T_fence + margin` 即满足。
- 这条之所以成立，关键是用**响应接收时刻**（≥ DN 续约）而非 §1.2 的**发送时刻**（< DN 续约，会让 CN 过早判 fenced）；且 `lastResp` **绝不被失败样本前移**（否则 hbAge 永不增长）。
- **延迟心跳**：被延迟的心跳即便让 DN 晚续约，CN 也只会更晚收到其响应、`lastResp` 更晚 → 不会过早判 FENCED-SAFE。✓
- **leader 切换**：新 leader 无历史 `lastResp`，须把每个 DN 的 `lastResp` 初始化为**取得 leadership 的时刻**，从而至少等 `T_proceed` 才可能判 FENCED-SAFE（覆盖旧 leader 残留在途心跳对 DN 的续约）。✓
- **残余风险**：见 §2.3 的 (a)(b) 假设；"单向可达+选择性丢失"在该假设下被排除。

`margin` 覆盖：①DN fence 检查粒度（惰性检查下≈0，定时检查则 1 周期）②GC/调度抖动 ③`lastResp` 至多 1 个心跳的认知粒度。默认 **≈5s**（`max(5000, 2×T_hb + fence检查间隔)`）。

**为什么 `T_fence` 取 20s**：`T_fence` 决定"健康 DN 多久没收到心跳就自我 fence（fail-closed）"。过小（如 5s）会让健康 DN 偶发 GC/抖动即被误 fence。取 **20s（与 `failureDetectorFixedThresholdInMs` 对齐）** 使"DN 自我 fence 时刻"≈"集群本来就判它 dead 时刻"，不新增误判区间。

**代价（本版明确接受）**：取消加性快路径后，**任一 DN 不可达时，所有 Tier-A DDL 都要等 `T_proceed≈25s`**（全员存活时恒零等待）。这是为"无 epoch、规则统一、不留 laggard"付出的可用性代价。

### 2.7 不可逆删除的特殊编排

涉及物理删除数据/属性的操作（表 DROP COLUMN 的 `EXECUTE_ON_REGIONS`、DELETE TIMESERIES、DELETE DATABASE、DELETE DEVICES）：删除步骤必须排在"判定器 PROCEED（所有未 ack DN 均 ack / SAFE_GONE / FENCED_SAFE）"**之后**。隔离 DN 在恢复 resync 前处于 fence、不接受相关写入，故删除后不会写出"幽灵列/幽灵设备"。

> 数据面可用性（region 多数派）与本提案正交：物理删除经 region consensus，本就需 quorum；本提案只解决"缓存失效广播"这层的可用性。

---

## 3. 各类操作的落地处理

| 类别 | 现状 | 方案后（v5） |
|---|---|---|
| 全部 Tier-A 表/树 schema DDL（CREATE/ADD/DROP/RENAME/SET）、view、datatype/encoding、模板、DELETE TS/DB/DEVICES、TTL | HARD-FAIL | 统一判定器：未 ack 的 DN 须 ack / SAFE_GONE / FENCED_SAFE 才 PROCEED，否则 WAIT/FAIL；不可逆删除编排见 §2.7；隔离 DN 恢复自驱 resync。**无加性快路径**——CREATE/ADD COLUMN 同样等待（避免未 fenced laggard） |
| 权限（grant/revoke/...） | **TIMEOUT-ABANDON（静默漏洞）** | 切判定器：不再 180s 静默丢弃；未 ack 活跃 DN → WAIT/FAIL，不可达 DN fence+恢复 resync；fence 期间鉴权 fail-closed（撤权立即生效）。**修复安全漏洞** |
| **B-加资源**：CREATE FUNCTION/TRIGGER/PIPE PLUGIN、CREATE TOPIC/CONSUMER、配额上调 | HARD-FAIL / RETRY-FAIL | Tier-B-soft：判定器可 PROCEED（缺资源者自然失败）；恢复 resync 重拉 |
| **B-撤资源/降级/控制**：DROP FUNCTION/TRIGGER/PIPE PLUGIN、SET SYSTEM STATUS=ReadOnly、disable/控制 | HARD-FAIL | **按 Tier-A 强一致**：未 ack 须 ack 或 已证实 fenced（陈旧 DN 否则会继续跑已删资源 / 继续写入）。SET ReadOnly 漏达的 DN 必须被等到 ack 或 fence，不可静默放行 |
| Pipe task | SOFT（已 reconcile） | 维持现有周期 reconcile（或并入 fence-恢复 resync） |
| LOAD/SET CONFIGURATION | HARD-FAIL | 视具体项：影响正确性/安全的按强一致；纯性能项可 soft |

---

## 4. 接口与配置改动清单

**Thrift（`iotdb-protocol/thrift-datanode/.../datanode.thrift`）**
- **不需要 epoch 字段。** 仅 `TDataNodeHeartbeatResp` 增 `optional bool supportsMetadataLeaseFencing`（或按 DN 版本号推断），供 CN 滚动升级期判断能否对该 DN 判 FENCED-SAFE（§5）。
- "最近成功收到响应时刻"是 **CN 本地量**，无需协议字段。

**ConfigNode**
- `HeartbeatService` / 心跳成功回调中新增并维护每 DN 的 `lastSuccessfulHeartbeatResponseNanos`（CN 本地时钟，**仅成功响应更新**；与 load-cache 的 `NodeHeartbeatSample` 分离，不被 `onError`/Unknown 前移）；leader 切换时初始化为取得 leadership 的时刻；记录 `supportsFencing`。
- 新增 `ClusterCachePropagator`（§2.4 判定器），接入：`SchemaUtils.preRelease/commitRelease/rollback`、`DeleteTimeSeriesProcedure`/`AlterTimeSeriesDataTypeProcedure` 的 invalidateCache、模板/view/TTL procedure、`ConfigNodeProcedureEnv.invalidateCache`（DeleteDatabase）、`AuthOperationProcedure`、`UDFManager`、trigger/pipe-plugin env、`ClusterQuotaManager`、`NodeManager`(status/config 中的强一致项)。

**DataNode**
- `getDataNodeHeartBeat`：记录 `lastCnHeartbeatNanos`（已实现）；回填 `supportsMetadataLeaseFencing=true`。
- `MetadataLeaseManager`（已实现 `isFenced`/recovery listener）；Tier-A 注入点 fail-closed（`DataNodeTableCache` 已实现，余 `TableHeaderSchemaValidator`/树 schema/`AuthorityChecker`/TTL 待补）。
- 注册表型缓存的"恢复重拉"实现。

**配置（`CommonConfig`）**
- `metadata_lease_fence_ms`（`T_fence`，**唯一主旋钮**；默认 **20000**，与 `failureDetectorFixedThresholdInMs` 对齐）。已实现。
- `T_proceed = T_fence + margin`，`margin` 内部派生（默认 ≈5s）。
- 判定器 WAIT 的最大等待/重试上限。
- **不设** `tier_b_fail_closed` / `enable_metadata_lease_fencing`。

---

## 5. 兼容性（升级后自动生效，无开关/无人工灰度）

- **升级后直接生效，无需开关**。
- **滚动升级期安全靠自动能力检测**：CN 判 FENCED-SAFE 的前提是"该 DN 会自我 fence"。**旧 DN 不会 fence、也不上报能力位**：
  - DN 心跳回填 `supportsMetadataLeaseFencing=true`（或按版本号推断）；CN 记录每 DN 最近上报的能力位。
  - 判定器**能力位先判**：不支持的 DN 一律 UNSAFE → 对它回退**现状严格语义**（任一不可达即失败）。已升级 DN 走新路径、未升级 DN 走旧严格路径，逐 DN 自动切换，正确性不破（此版已与 §2.4 顺序统一，消除 v4 矛盾）。
  - 全部升级完毕后所有 DN 走新路径。
- **回滚**：仅一个自动能力位 + 新增逻辑，回退旧版本即恢复原行为。

---

## 6. 风险与权衡

- **取消加性快路径的延迟代价**：任一 DN 不可达时，**所有 Tier-A DDL（含 CREATE/ADD COLUMN）都等 `T_proceed≈25s`**；全员存活恒零等待。这是为"无 epoch、规则统一、不留 laggard"接受的代价（你已确认）。若日后需要给 CREATE 这类提速，可再评估 epoch 正向确认方案。
- **时序假设（§2.3 (a)(b)）**：依赖有界延迟 + 心跳连接双向对称；"单向可达 + 选择性丢失失效广播"在该假设外不保证安全。需在文档/运维约束中写明；若环境不满足，则需 epoch/token。
- **CN 侧信号必须新增且独立**：绝不能复用记录发送时刻、且被 `onError` 前移的现有样本（否则证明不成立）。leader 切换须重置 `lastResp`。
- **半坏 DN（心跳通、广播 RPC 断）**：保守失败（WAIT 超时 → FAIL）。
- **少数派分区读写不可用**：被隔离 DN 对 Tier-A fail-closed，牺牲少数侧可用性换正确性（CP）。
- **权限 fail-closed 可用性**：撤权立即生效 vs CN 短暂不可达不至全拒，需分别配置宽限。
- **配额无回源**：DN 侧需新增主动拉取，否则恢复 resync 无处落地。

---

## 7. 分阶段实施计划

**Phase 0 — 观测、能力位、CN 侧 lastResp 信号（无行为变更）**
- DN 记录 `lastCnHeartbeatNanos`（已实现）、回填能力位；CN 新增并维护**独立的 `lastSuccessfulHeartbeatResponseNanos`**（仅成功更新、leader 切换重置）与能力位；加监控指标。**不 fence、不放行。**

**Phase 1 — DN 自我 fencing + Tier-A fail-closed（正确性基石）✅ 已完成**
- `MetadataLeaseManager` + 惰性 fence 检查 + recovery 自驱 resync；表 schema、树 schema、权限、TTL(compaction) 的 fail-closed 注入**均已实现并各带 TDD**（见 §2.5.3）。`TableHeaderSchemaValidator` 经表缓存自动覆盖；分区缓存暂缓。此阶段不改 CN 放行逻辑——即使 CN 仍严格，DN 端已能在分区时自保。

**Phase 2 — CN 统一判定器 + Tier-A 放行（兑现可用性）🚧 进行中**
- ✅ `ClusterCachePropagator`（`propagateOnce` 判定 + `propagate` 重试循环；能力位先判、FENCED-SAFE 收紧；8 个单测）。
- ✅ 生命周期挂钩：`notifyLeaderReady` → `onLeadershipAcquired`（重夺 leadership 重置 `lastResp`）、`removeDataNodePersistence` → `removeDataNode`。
- ✅ **首个 procedure 接入（模板）**：`CreateTableProcedure` 的 PRE_RELEASE 改走 `ClusterCachePropagator.propagate`（`SchemaUtils.preUpdateTableReq` + `broadcastTableUpdate` 返回全量响应；旧 `preReleaseTable` 退化为只返回失败的薄封装，其余调用方不受影响）。COMMIT_RELEASE 维持 best-effort warn。
- ✅ **端到端 IT**（`IoTDBTableDDLHAIT`，1C3D）：停 1 DN 后 CREATE TABLE 仍成功；新增 IT 框架 `setMetadataLeaseFenceMs`。
- ⏳ 待办：按模板接入其余 ~19 个 Tier-A procedure（AddTableColumn/DropTableColumn/RenameTable/SetTableProperties、DeleteTimeSeries、AlterTimeSeriesDataType、模板、view、TTL、DeleteDatabase 同步路径）；不可逆删除编排。**统一规则、无加性快路径**。

**Phase 3 — 权限与 Tier-B 收编**
- `AuthOperationProcedure` 改造（修复 180s 静默漏洞）；Tier-B 按 加资源(soft) / 撤资源·降级·控制(强一致) 分别接入；配额回源补齐。

**测试（含 review 要求的专项）**
- 1C3D 停 1 DN：所有 Tier-A 操作在 `T_proceed` 后成功（Phase 2/3 后）。
- **延迟心跳**：心跳在 `T_proceed` 后才送达 DN（DN 晚续约）→ CN 不得在 DN fence 前判 FENCED-SAFE（验证用的是响应接收时刻）。
- **leader 切换**：新 leader 不得凭旧 `lastResp` 过早判 FENCED-SAFE；旧 leader 残留心跳续约的 DN 不被误放行。
- **heartbeat onError 连续刷新**：连接断开持续产生 Unknown 样本时，新增的 `lastResp` **不被前移**，`hbAge` 正常增长。
- **活着但分区**：`T_fence` 后该 DN Tier-A fail-closed、不产生脏数据/脏读/越权；恢复后自驱 resync。
- **未 fenced laggard 回归**：DN 漏掉 CREATE/ADD COLUMN 广播但心跳 Running → 判定器 WAIT/FAIL（不跳过），不得出现"新 schema 在该 DN 长期不可见"。
- **权限**：撤权后分区 DN 拒绝越权。
- **B-撤资源**：DROP UDF/Trigger/Plugin 后陈旧 DN 不得继续执行旧资源；SET ReadOnly 漏达的 DN 不得继续接受写入。
- **不可逆删除并发写**：隔离 DN 不写幽灵列/设备。
- **滚动升级**：半升级态（含旧 DN）回退严格语义，正确性不破。
- 仅运行新增/改动相关 IT。

---

## 8. 附：与上一篇及已落地代码的关系

`table-model-ddl-ha-analysis.md` 是表模型特例与起点；本文推广为覆盖全部 CN→DN 元数据广播的统一框架。

已落地（worktree 分支）：DN 侧 `MetadataLeaseManager`（含 recovery listener）、心跳记录 `lastCnHeartbeatNanos`、`metadata_lease_fence_ms` 配置、heartbeat-age 指标，以及 `DataNodeTableCache` 的 fail-closed + 恢复作废（均 TDD）。这些属于 Phase 0/1 的 DN 侧，**不受本次 review 影响**（评审点 1 针对的是尚未实现的 CN 侧判定信号）。CN 侧判定器（Phase 2）按本 v5 实现：统一规则、能力位先判、独立的响应接收时刻信号、FENCED-SAFE 收紧。
