# 表模型 DDL 操作在任一 DataNode 宕机时失败的根因分析与解决方案

> 状态：待 review
> 作者：（草拟）
> 适用版本：2.0.x（master）

## 0. TL;DR

- **现象**：表模型的 `CREATE TABLE` / `ALTER TABLE`（加列、删列、改属性、改名）/ `DROP TABLE` 等 DDL，在集群中**任意一个 DataNode（DN）不可达**时都会执行失败并回滚。这与"多副本高可用"的预期相悖——按直觉，挂 1 个 DN 不应阻塞元数据变更。
- **根因（正确性）**：这些 DDL 在 ConfigNode（CN）侧以 Procedure 执行，其中有一步会把"缓存失效 / 预发布"RPC **广播给集群里所有已注册的 DN**，并要求**每一个 DN 都返回 SUCCESS** 才继续；只要有一个 DN 不可达（重试 6 次后仍失败），整个 Procedure 就 `setFailure` 并回滚。
- **为什么要这么强（不是 bug，是设计）**：DN 上对表模型有几类**本地缓存**（表 schema 缓存 `DataNodeTableCache`、设备属性/last 值缓存 `TableDeviceSchemaCache`）。写入校验和部分查询**直接读本地缓存、不回 CN 核对**。如果某个 DN 持有过期缓存且没被清理，在**网络分区**下它仍可能用旧 schema 接受写入 / 返回旧值，从而产生**与已提交 schema 不一致的脏数据**（类型错乱、幽灵列、删列后又写入等）。因此当前实现选择了"宁可失败也不放过任何一个 DN"。
- **关键洞察**：真正危险的只有"**DN 活着但与 CN 分区**"这一种情况。**真正宕机的 DN 内存缓存已经没了**，重启时会从 CN 重新拉取全量 schema（`DataNode.java:523` 的 `DataNodeTableCache.init(...)`），不可能用旧缓存服务请求。当前实现把"宕机"和"分区"混为一谈，对"宕机"这种本来安全的情况也一并失败，才造成了可用性损失。
- **解决方向**：给 DN 的表缓存加一个**与 CN 心跳绑定的"租约/fencing"机制**——DN 一旦在 `T_fence` 内收不到 CN 心跳，就**自行作废表缓存并对依赖缓存的表操作 fail-closed**。这样"不可达"就等价于"安全"，CN 侧的 DDL 便可以**跳过已确认隔离/宕机的 DN 继续执行**，从而在挂掉少数 DN 时仍保持 DDL 可用，且不牺牲正确性。

---

## 1. 问题现象

在一个多 DN 集群（例如 1 CN + 3 DN，数据多副本）中，停掉任意 1 个 DN 后执行下列表模型语句，会直接报错失败（而非降级成功）：

- `CREATE TABLE` / `CREATE VIEW`
- `ALTER TABLE ... ADD COLUMN`
- `ALTER TABLE ... DROP COLUMN`
- `ALTER TABLE ... SET PROPERTIES`（如 TTL）
- `ALTER TABLE ... RENAME COLUMN` / `RENAME TABLE`
- `DROP TABLE`
- 对应的 view 变体、`CREATE/DROP DATABASE` 等

报错信息形如 `Pre create table failed` / `pre release add table column failed` / `... must clear the related schema cache` 等。

> 注：树模型的 `DELETE TIMESERIES`、`DELETE DATABASE` 也有**相同**的"所有 DN 必须可达"约束（见 §2.6），本文聚焦表模型，但方案对树模型同样适用。

---

## 2. 根因分析

### 2.1 总体执行链路

表模型 DDL 的执行入口在 CN 的 Procedure 框架，相关类位于：

```
iotdb-core/confignode/src/main/java/org/apache/iotdb/confignode/procedure/impl/schema/table/
├── CreateTableProcedure.java
├── DropTableProcedure.java
├── AddTableColumnProcedure.java
├── DropTableColumnProcedure.java
├── RenameTableColumnProcedure.java
├── RenameTableProcedure.java
├── SetTablePropertiesProcedure.java
├── AlterTableColumnDataTypeProcedure.java
├── AbstractAlterOrDropTableProcedure.java   ← 所有 alter/drop 的基类
└── view/...                                  ← view 变体
```

这些 Procedure 的共同点：**在真正提交元数据变更之前 / 删除数据之前，必须先让所有 DN 把相关本地缓存清掉或进入"待更新"态**。这一步通过向所有 DN 广播 RPC 完成。

### 2.2 关键代码：广播给"所有已注册 DN"，任一失败即整体失败

以"加列"为例，`AddTableColumnProcedure` 的状态机是：

```
COLUMN_CHECK → PRE_RELEASE → ADD_COLUMN → COMMIT_RELEASE
```

`PRE_RELEASE` 步调用基类 `AbstractAlterOrDropTableProcedure.preRelease(env)`，进而调用 `SchemaUtils.preReleaseTable(...)`。后者是整个问题的核心：

```java
// SchemaUtils.java  (≈ L243-262)  preReleaseTable
final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
    configManager.getNodeManager().getRegisteredDataNodeLocations();   // ← 所有已注册 DN
final DataNodeAsyncRequestContext<TUpdateTableReq, TSStatus> clientHandler =
    new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.UPDATE_TABLE, req, dataNodeLocationMap);
CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
return clientHandler.getResponseMap().entrySet().stream()
    .filter(e -> e.getValue().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));   // ← 返回"失败的 DN"
```

- `getRegisteredDataNodeLocations()`（`NodeManager.java` ≈ L688-697）返回**所有曾经注册过的 DN，不区分当前是否存活**。
- `sendAsyncRequestWithRetry` 内部最多重试 `MAX_RETRY_NUM = 6` 次（`AsyncRequestManager.java`）。一个 DN 只有返回 SUCCESS 才会从重试集合移除；不可达的 DN（连接拒绝 / 超时）由 `DataNodeTSStatusRPCHandler.onError` 写入一个**错误 TSStatus**，并**保留在重试集合**里，6 轮耗尽后仍是失败项。

Procedure 拿到非空的 `failedResults` 后：

```java
// AbstractAlterOrDropTableProcedure.java  (≈ L96-101)
if (!failedResults.isEmpty()) {
  // All dataNodes must clear the related schema cache   ← 设计者的注释
  setFailure(new ProcedureException(new MetadataException(...)));
  return;
}
```

`setFailure` 把 Procedure 置为 `FAILED`，ProcedureExecutor 随后触发**回滚**。结果就是：**只要有 1 个 DN 不可达，DDL 失败**。

> 同样的 "All dataNodes must clear the related schema cache / schemaEngine cache" 注释与 `setFailure` 逻辑，硬编码在至少 5 处：`AbstractAlterOrDropTableProcedure`（正向 L96-101 与回滚 L144-148）、`CreateTableProcedure`（L153-169）、`DropTableProcedure`（L141-167）、`DropTableColumnProcedure`（L152-188）。这是一个**全集群一致的硬约束**，不是个别遗漏。

### 2.3 DataNode 上与表模型相关的几类缓存

DDL 之所以要广播失效，是因为 DN 上确实缓存了 schema，且**关键路径直接信任本地缓存**。

| 缓存 | 类 / 字段 | Key | 内容 | 谁来读（危险路径） |
|---|---|---|---|---|
| 表 schema（已提交） | `DataNodeTableCache.databaseTableMap`（L64） | db, table | `TsTable`：列定义、列类别（TAG/ATTRIBUTE/FIELD/TIME）、数据类型、表属性（TTL 等） | **写入校验**：`getTableInWrite`（L316）直接读，不回 CN |
| 表 schema（变更中） | `DataNodeTableCache.preUpdateTableMap`（L67） | db, table | `(TsTable, version)`：DDL 进行中的"待更新"占位 | 查询取 schema 的 `getTable`（L329）遇到它会**回 CN 重新拉取** |
| 设备属性 | `TableDeviceSchemaCache`（dualKeyCache，table 模型 `deviceSchema`） | db, table, deviceID | `Map<String,Binary>`：每个设备的 ATTRIBUTE 列名→值 | 查询属性：`getDeviceAttribute`（L141）直接读，命中即返回，不回 schema region |
| last 值 | `TableDeviceSchemaCache`（table 模型 `lastCache`） | db, table, deviceID, measurement | `TimeValuePair`：每列最后一个点 | `LAST` 查询直接读 |

补充：设备 ATTRIBUTE 的**权威存储**是 schema region 内的 `DeviceAttributeStore`（按 snapshot 持久化），但**查询读属性走的是 `TableDeviceSchemaCache` 缓存**——命中缓存就不回 schema region。这正是属性脏读的来源。

DN 侧接收 CN 广播的 RPC handler 都在 `DataNodeInternalRPCServiceImpl`：

- `updateTable`（L1813）：按子类型分发 `PRE_UPDATE_TABLE` / `ROLLBACK_UPDATE_TABLE` / `COMMIT_UPDATE_TABLE`，驱动 `DataNodeTableCache` 的两阶段协议。
- `invalidateTableCache`（L1842）：整表失效（drop table）。
- `invalidateColumnCache`（L2033）：单列失效（drop column）。
- `deleteColumnData`（L2051）：物理删除列数据（drop column 第三步）。

所有失效 handler 都会先拿 `SchemaLockType.VALIDATE_VS_DELETION_TABLE` 的**写锁**。

### 2.4 两阶段协议与 `VALIDATE_VS_DELETION_TABLE` 锁

DDL 用一个 **pre-release / commit-release / rollback** 的两阶段（对 drop 列是"先失效缓存、再删数据、最后提交"的三阶段）协议来保证跨 DN 的原子性：

- **PRE_RELEASE**：广播 `PRE_UPDATE_TABLE`，每个 DN 把新 `TsTable` 放入 `preUpdateTableMap`，使得**取 schema 的读路径**在变更窗口内回 CN 拉最新版本。**任一 DN 失败 → 整体失败**。
- **（中间）** 在 CN consensus 提交真正的元数据变更。
- **COMMIT_RELEASE**：广播 `COMMIT_UPDATE_TABLE`，DN 把表从 `preUpdateTableMap` 落到 `databaseTableMap`。**失败只告警、不失败 Procedure**（见 §2.5 的关键不对称）。

`VALIDATE_VS_DELETION_TABLE` 锁的协议在 `SchemaLockType.java`（L52-62）写得很清楚：

```
1. 写入 / load TsFile 校验 schema 前，加读锁；
2. 完成后释放读锁；
3. 表相关删除时，作废 device cache 前，加写锁；
4. 完成失效后释放写锁。
```

这把锁只保证**单个 DN 内**"写入校验"与"缓存失效"互斥。"所有 DN 都必须 ack"这一条，则是把这种互斥**提升到集群级**：CN 不提交元数据变更，直到确信每个 DN 都拆掉了旧缓存。

### 2.5 关键不对称：PRE 必须全可达，COMMIT 却允许失败

这是理解问题、也是设计方案的关键线索：

| 步骤 | 一个 DN 不可达时 | 原因 |
|---|---|---|
| PRE_RELEASE / INVALIDATE_CACHE | **整体失败 + 回滚** | 元数据还没变。必须保证"没有任何活着的 DN 还揣着旧缓存跨过这次变更" |
| COMMIT_RELEASE | **仅告警，Procedure 成功** | 元数据已提交。漏掉 commit 的 DN 只是 `preUpdateTableMap` 里留了个占位，其读路径会持续回 CN 拉最新版 → 最终一致、且安全 |

**结论**：系统其实**已经容忍** DN 在 commit 阶段缺席（最终一致）。真正的硬约束只在**变更前的失效/预发布**这一步。而这一步要求"全员可达"的唯一目的，是**防止某个活着的 DN 带着旧缓存跨过变更点**。这恰恰是我们可以用 fencing 来替代的部分。

### 2.6 为什么"必须所有 DN"——三个正确性场景

如果允许某个**活着但分区**的 DN 漏掉失效，可能产生的脏数据：

1. **写路径用过期表 schema（类型/类别错乱）**
   写入校验入口 `TableHeaderSchemaValidator`（`validateInsertNodeMeasurements` L343 / `validateTableHeaderSchema4TsFile` L102）先加 `VALIDATE_VS_DELETION_TABLE` 读锁，然后 `DataNodeTableCache.getTableInWrite(...)`（L123 / L363）**直接读 `databaseTableMap`**。该方法对 FIELD 列的数据类型校验"交给上层"，类型不一致不会在这层拦截。
   设想列 `pressure` 在 CN 上由 `FLOAT` 改成 `DOUBLE`，但分区 DN 仍缓存 `FLOAT`：路由到该 DN 的写入会以 `DOUBLE` 落盘，而该 DN 的列定义却是 `FLOAT`——后续按 `FLOAT` 解码即得到**错误数值**，且**对客户端无任何报错**（静默类型损坏）。若错配发生在 TAG 与 FIELD 之间，物理存储路径完全不同（tag 进 deviceID、field 进 measurement），会产生**正常查询无法触及的结构性脏数据**。

2. **属性缓存脏读**
   查询读属性 `TableDeviceSchemaFetcher.tryGetTableDeviceInCache`（L413-452）→ `cache.getDeviceAttribute`（L424）**纯内存命中即返回**，不回 schema region。`DROP COLUMN`（属性列）经 `invalidateColumnCache` 把该列从每个设备的属性 map 移除；若某分区 DN 漏掉这次失效，它仍会把已删除列的旧值当作有效属性返回——**返回一个 schema 里已不存在的列的值**。

3. **DROP COLUMN：物理删除 + 幽灵数据（最危险）**
   `DropTableColumnProcedure` 状态机：

   ```
   CHECK_AND_INVALIDATE_COLUMN → INVALIDATE_CACHE → EXECUTE_ON_REGIONS → DROP_COLUMN
   ```

   顺序保证是：**先让所有 DN 失效缓存（此后没有新写入能写进该列）→ 再物理删除 TsFile/属性数据 → 最后在 CN 提交删列**。
   若某 DN 漏掉 `INVALIDATE_CACHE` 且系统仍继续：数据被物理删除、CN schema 已删列，但该 DN 缓存里**该列仍存在**；新写入路由到它会**通过校验**并把该列数据写进 WAL/memtable——于是出现"存储里有、schema 里没有"的**幽灵列数据**；查询扫到这些字节会静默跳过或解码报错。属性列情形更糟：脏值落在 schema region 持久化存储里，副本间**持久性数据分叉**，难以自动 reconcile。

> 这三点正是用户所说"DN 上有几种 Cache，不清理则在网络分区时可能产生脏数据"的具体机理。

### 2.7 现状的不合理之处

把上面拼起来，问题的本质是：

> **当前实现用"所有已注册 DN 必须同步 ack 缓存失效"来保证正确性，却没有区分"DN 真宕机"（缓存已随进程消失，本质安全）与"DN 活着但与 CN 分区"（缓存仍在，真正危险）。对前者本可放行，却一并判失败，于是牺牲了 DDL 的高可用。**

证据：

- 一个**真正宕机**的 DN，重启后必然走注册流程，从 CN 的 `runtimeConfiguration.getTableInfo()` 重建 `DataNodeTableCache`（`DataNode.java:523` 的 `init(...)`），**不可能用旧缓存服务任何请求**。它在宕机期间也不服务任何请求。对它而言，"等它 ack 失效"在逻辑上是多余的。
- CN 侧其实**已经知道** DN 是否可达：`DataNodeHeartbeatCache` 通过 Phi-Accrual `failureDetector` 把失联 DN 标为 `Unknown`。树模型的 `DeleteDatabaseProcedure` 走的 `ConfigNodeProcedureEnv.invalidateCache`（L164-221）已经会**检查 NodeStatus、对 `Unknown` 重试 10 次 / 5s** ——说明"失效时参考 NodeStatus"这条路代码里已有先例，只是最终仍是"超时即失败"，没有走到"放行"。

缺的那一环是：**没有任何机制保证一个"活着但分区"的 DN 会主动停止使用旧缓存**。只要补上这一环（DN 自我 fencing），"不可达"就能安全地等价于"已隔离"，CN 就能放心放行。

---

## 3. 解决方案

### 3.1 设计目标

1. **正确性不回退**：任何已提交的表 schema 变更之后，集群中**不存在**任何 DN 用过期缓存接受写入或返回旧值（尤其是 DROP 列物理删除之前，必须保证没有 DN 还能写该列）。
2. **可用性提升**：挂掉**少数** DN（典型：3 副本挂 1）时，表模型 DDL 仍能成功。
3. **复用现有设施**：尽量基于现有心跳 / NodeStatus / 注册 resync，不引入新的重协议。
4. **常态零额外开销**：全员存活时路径与现状一致，无新增等待。

### 3.2 核心思想：把"缓存有效性"绑定到"与 ConfigNode 的租约"

引入一个概念：**DN 的表模型缓存只有在它"持有 CN 租约"期间才可信**。租约就用现有心跳承载——DN 持续收到 CN 心跳即续约；一旦在 `T_fence` 内收不到心跳，租约过期，DN 必须**自我隔离（self-fencing）**。

于是：

- **DN 真宕机** → 进程没了，缓存没了，重启 resync，安全。
- **DN 活着但分区** → `T_fence` 后租约过期，自我隔离（作废表缓存 + 对依赖缓存的表操作 fail-closed），不再可能产生脏数据。
- 两种情况下，"CN 联系不上的 DN"在 `T_fence` 之后都**保证不会用旧缓存服务请求**。CN 据此放行。

### 3.3 组件一：DataNode 自我隔离（self-fencing）——新增

这是方案中**唯一全新的机制**，也是正确性的基石。

1. **记录最后心跳时间**：在 `getDataNodeHeartBeat`（`DataNodeInternalRPCServiceImpl.java:2226`）里记录"最近一次收到 CN 心跳"的单调时钟时间戳（DN 当前**不**记录，但 handler 就在那，改动很小）。
2. **后台 fencing 检查**：DN 起一个轻量定时任务，若 `now - lastHeartbeatFromCN > T_fence`，进入 **FENCED** 态：
   - 作废 `DataNodeTableCache`（`databaseTableMap` + `preUpdateTableMap`）与 `TableDeviceSchemaCache`（属性 + last）。
   - 设 `tableSchemaFenced = true`。
3. **FENCED 态下 fail-closed**：
   - 写入校验（`TableHeaderSchemaValidator`）与取 schema（`getTableInWrite` / `getTable`）在 FENCED 态下**不信任本地缓存**：要么回 CN 现拉，CN 不可达则**直接拒绝该操作**（fail-closed，宁可不可用也不写脏）；要么干脆对表写入/查询返回"schema 暂不可用，请重试"。
   - 属性 / last 缓存查询同理：FENCED 态视为 miss，回源；回不了源则失败。
4. **续约即恢复**：恢复收到 CN 心跳后，**先强制 resync**（组件二）再清除 FENCED 态。

> 失败语义：分区少数侧的客户端在 `T_fence` 后会被 fail-closed 拒绝表读写——这正是 CP 系统对少数派分区的**正确**行为；多数侧（含 CN）保持可用。

### 3.4 组件二：重连后强制 resync——增强现有路径

DN 从 FENCED 恢复（或重启注册）时，在**对外服务表请求之前**必须把缓存与 CN 对齐：

- 复用现有注册 resync：重启路径已通过 `DataNodeTableCache.init(runtimeConfiguration.getTableInfo())`（`DataNode.java:523`）重建缓存。
- 对"未重启、仅心跳恢复"的 FENCED→恢复路径，新增一次**主动全量拉取**（沿用 `getTable` 已有的 `fetchTables`/`ClusterConfigTaskExecutor` 回 CN 的能力）：拉到当前 schema 版本后再清 FENCED。
- 可选优化：在心跳响应里带一个**单调递增的 schema epoch**；DN 比对本地 epoch，落后才触发全量拉取，常态只续约不拉数据。

### 3.5 组件三：ConfigNode DDL 容忍"已隔离/已宕机"的 DN——核心改动

改造 §2.2 那一步"广播失效 + 任一失败即 setFailure"的逻辑。把失效广播的结果分三类处理，而不是一刀切失败：

对每个未返回 SUCCESS 的 DN，查其 `NodeStatus` / 最近成功联系时间：

| DN 情况 | 判定 | 处理 |
|---|---|---|
| `Running`（可达），但 RPC 报错 | 真错误（如 DN 内部异常） | 重试；仍失败则**失败 Procedure**（与现状一致） |
| `Unknown` / 失联，且失联时长 < `T_proceed` | 可能还没自我隔离 | **等待**至 `T_proceed`（或在此期间它恢复并 ack） |
| 失联时长（`hbAge`）≥ `T_proceed` | 保证已自我隔离或已宕机 | **视为安全，放行**；该 DN 恢复时由其**自驱 resync**，CN 无需记录（见通用方案 `cluster-metadata-ha-fencing-design.md` §2.3） |
| 已被移除 / 确认下线 | 不再是集群成员 | 放行 |

放行后：

- DDL 照常提交元数据（并对 DROP 列继续物理删除——此时已保证无活着的旧缓存 DN）。
- 把"该 DN 需要 resync"持久化（或依赖组件二的 DN 自恢复 resync）。该 DN 恢复时被强制对齐后才重新服务（组件二保证它在对齐前处于 FENCED，不会脏读/脏写）。

> 这本质上是把树模型 `ConfigNodeProcedureEnv.invalidateCache`（L164-221）已有的"NodeStatus 感知 + 重试"，从"超时即失败"改成"确认隔离后放行"。

### 3.6 时序与正确性论证（lease ordering）

记号：
- `T_hb`：心跳间隔。
- `T_fence`：DN 自我隔离阈值（收不到心跳超过它就 fence）。
- `T_proceed`：CN 判定"该失联 DN 已安全隔离"所需的失联时长。

**安全不变式**：CN 在提交变更（及 DROP 列物理删除）时，对每个未 ack 的 DN，要么它已 ack，要么它**已经自我隔离**。

**为什么 `T_proceed > T_fence + margin` 即可保证**：
心跳方向是 CN→DN。CN 对某 DN 的"最近一次成功联系"时刻 `t_cn`，在真实时间上**不早于** DN"最近一次收到心跳"的时刻 `t_dn`（DN 先收到、CN 才拿到响应）。DN 在 `t_dn + T_fence` 自我隔离。因此只要 CN 自 `t_cn` 起又过了 `T_fence + margin`（`margin` 覆盖时钟漂移、DN fence 检查周期、网络抖动），就能确信 `now ≥ t_dn + T_fence`，即该 DN **已隔离**。取 `T_proceed = T_fence + margin` 成立。

**与 commit 不对称的呼应**：§2.5 已说明系统本就容忍 commit 阶段缺席（最终一致）。本方案只是把"变更前失效"这步从"全员强同步"放宽为"全员 ack 或 已隔离"，正确性边界没有降低——因为"已隔离"DN 与"已清缓存"DN 对外行为等价（都不会用旧缓存服务）。

**DROP 列的特别说明**：物理删除（`EXECUTE_ON_REGIONS`）必须在"全员 ack 或 已隔离"**之后**才执行。隔离 DN 在恢复 resync 前处于 FENCED、不接受该表写入，故不会在删除后再写出幽灵列。顺序不变，安全。

### 3.7 各操作的处理要点

| 操作 | 现状失败点 | 方案后 |
|---|---|---|
| CREATE TABLE | PRE_RELEASE 广播 `PRE_UPDATE_TABLE` 全员必达 | 失联且已隔离的 DN 放行；它恢复 resync 时自然学到新表 |
| ADD COLUMN / SET PROPERTIES / RENAME | `preRelease` 全员必达 | 同上；隔离 DN 恢复后拉到新 schema |
| DROP COLUMN | `INVALIDATE_CACHE` 全员必达，且卡住后续物理删除 | 隔离 DN 放行后再删数据；隔离 DN 恢复前 FENCED，不会写幽灵列 |
| DROP TABLE | `invalidateTableCache` 全员必达 | 同 DROP COLUMN |
| COMMIT_RELEASE | 本就只告警 | 不变 |

### 3.8 备选方案（讨论用）

**备选 A：写路径 schema 版本号 fencing（更彻底但更侵入）**
给每张表一个在 CN consensus 提交的单调 schema 版本 `V`，并下沉到数据写入路径：每次写入用所用 schema 的 `V` 打戳，region 侧（持有权威已提交 `V`）拒绝**低版本**写入。这样即使某 DN 缓存过期，它的写入也会在 region 层被版本校验拦下，从根上杜绝脏写。
- 优点：不依赖时间/租约推理，纯版本号比较，最严格。
- 缺点：需把表 schema 版本贯穿到数据写入 consensus 路径（当前数据 region 并不知道表 schema 版本），改动面大；对读路径脏读仍需另行处理。
- 建议：作为长期演进选项，可与组件一/三组合（fencing 解决可用性，版本号兜底正确性）。

**备选 B：仅缩小广播范围**
有人可能想"只对受影响 schema region 的副本所在 DN 广播"。**不可行**：表缓存存在于**每个** DN（任何 DN 都可能做查询协调者并缓存任意表 schema、也可能承载该表数据 region），不是只在副本 DN 上。所以无法用 quorum 替代全员。这条排除，正好反衬出组件一（让每个 DN 自我兜底）才是对的方向。

### 3.9 实施计划与涉及文件

**Phase 1：DataNode 自我 fencing（正确性基石，先落地）**
- `DataNodeInternalRPCServiceImpl.getDataNodeHeartBeat`（L2226）：记录 `lastHeartbeatFromCnNanos`。
- 新增 fencing 检查任务 + FENCED 状态（建议挂在 schema engine / `DataNodeTableCache` 附近）。
- `DataNodeTableCache`：FENCED 态下 `getTableInWrite` / `getTable` 不信任本地缓存；提供 `fenceAll()` / `clearFence()`。
- `TableDeviceSchemaCache`：FENCED 态下属性/last 查询按 miss 处理。
- `TableHeaderSchemaValidator`（L102/L343）：FENCED 态写入校验 fail-closed。
- 配置项：`T_fence`（默认 **20s**，与 `failureDetectorFixedThresholdInMs` 对齐，避免误 fence 健康 DN）。表 DDL 中 **CREATE TABLE / ADD COLUMN 等加性操作即时放行**（陈旧 DN 对未知实体天然 fail-closed），只有 DROP/RENAME/SET 等**破坏/语义变更类**在确有 DN 不可达时等 `T_proceed = T_fence + margin ≈ 25s`（`margin≈5s` 内部派生）。分类与论证见通用方案 §2.6/§3。

**Phase 2：DN 恢复 resync**
- 心跳响应增加 `schemaEpoch`（可选优化）；DN 心跳恢复后落后才全量拉取。
- 复用 `DataNode.java:523` 注册 resync；新增"心跳恢复"分支的主动拉取。

**Phase 3：ConfigNode DDL 放行逻辑（兑现可用性）**
- `SchemaUtils.preReleaseTable / commitReleaseTable / rollbackPreRelease`（≈L243-318）：返回结果区分"真失败 / 失联可放行"。
- `AbstractAlterOrDropTableProcedure.preRelease`（L89-110）、`CreateTableProcedure`（L153-169）、`DropTableProcedure`（L141-167）、`DropTableColumnProcedure`（L152-188）：把"任一失败即 setFailure"改为"按 §3.5 表格分类处理"。
- 引入基于 `hbAge` 的判定（参考并改造 `ConfigNodeProcedureEnv.invalidateCache` L164-221），落地"加性即时放行 / 破坏类等 `T_proceed = T_fence + margin`"。
- 隔离 DN 恢复时由其**自驱 resync**，CN 无需记录 laggards（见通用方案 §2.3）。

**Phase 4（可选）：备选 A 的写路径版本号兜底**——长期演进。

**测试**
- 复用 / 扩展现有 IT：在 1C3D 集群停 1 DN，验证 CREATE / ADD / DROP / SET / RENAME / DROP TABLE 均成功。
- 注入"活着但分区"场景（阻断 DN↔CN 心跳但保留 DN↔client）：验证 `T_fence` 后该 DN 对表读写 fail-closed，不产生脏数据；恢复后 resync 正确。
- DROP 列并发写：验证隔离 DN 不会写出幽灵列。
- 仅运行本次新增/改动的 IT，不跑全量。

### 3.10 风险与权衡

- **DDL 延迟**：仅当确有 DN 失联时，CN 需等待至 `T_proceed` 才放行；全员存活时无额外等待。可接受。
- **少数派读不可用**：分区少数侧的 DN 在 `T_fence` 后对表读写 fail-closed，牺牲该侧可用性换正确性——符合 CP 取舍。若未来要给"可容忍轻微陈旧"的读开口子，可在备选 A 的版本号体系下单独放宽，但默认 fail-closed。
- **`T_fence` 取值**：默认 20s（与失败检测阈值对齐，避免误 fence）。破坏类操作的下线等待 ~25s 主要由加性快速路径抵消；全员存活恒零等待。详见通用方案 §2.6。
- **时钟假设**：论证用单调时钟与保守 margin，不依赖跨节点钟同步。

---

## 4. 附：关键文件索引

CN 侧：
- `confignode/.../procedure/impl/schema/SchemaUtils.java` —— `preReleaseTable`/`commitReleaseTable`/`rollbackPreRelease`（≈L243-318）
- `confignode/.../procedure/impl/schema/table/AbstractAlterOrDropTableProcedure.java` —— `preRelease`/`commitRelease`/`rollbackPreRelease`，"All dataNodes must clear..." 注释
- `confignode/.../procedure/impl/schema/table/{CreateTable,DropTable,AddTableColumn,DropTableColumn,SetTableProperties,RenameTableColumn}Procedure.java`
- `confignode/.../procedure/env/ConfigNodeProcedureEnv.java` —— 树模型 `invalidateCache`（L164-221，NodeStatus 重试先例）
- `confignode/.../manager/node/NodeManager.java` —— `getRegisteredDataNodeLocations`（L688-697）
- `confignode/.../manager/load/cache/node/DataNodeHeartbeatCache.java` —— Phi-Accrual 失败检测 → `Unknown`
- `node-commons/.../client/request/AsyncRequestManager.java` —— `MAX_RETRY_NUM = 6`

DN 侧：
- `datanode/.../schemaengine/table/DataNodeTableCache.java` —— 表 schema 缓存 + 两阶段协议（`databaseTableMap` L64 / `preUpdateTableMap` L67 / `getTableInWrite` L316 / `getTable` L329）
- `datanode/.../queryengine/plan/relational/metadata/fetcher/cache/TableDeviceSchemaCache.java` —— 属性 + last 缓存（`getDeviceAttribute` L141 / `invalidate` L614,L676）
- `datanode/.../queryengine/plan/relational/metadata/fetcher/TableDeviceSchemaFetcher.java` —— 属性读缓存路径（`tryGetTableDeviceInCache` L413-452）
- `datanode/.../queryengine/plan/relational/metadata/fetcher/TableHeaderSchemaValidator.java` —— 写入校验入口（L102 / L343）
- `datanode/.../queryengine/plan/analyze/lock/SchemaLockType.java` —— `VALIDATE_VS_DELETION_TABLE`（L52-62）
- `datanode/.../protocol/thrift/impl/DataNodeInternalRPCServiceImpl.java` —— RPC handler：`updateTable` L1813 / `invalidateTableCache` L1842 / `invalidateColumnCache` L2033 / `deleteColumnData` L2051 / `getDataNodeHeartBeat` L2226
- `datanode/.../schemaengine/schemaregion/attribute/DeviceAttributeStore.java` —— 属性权威存储（snapshot 持久化）
- `datanode/.../service/DataNode.java` —— 启动 resync（`DataNodeTableCache.init` L523）
