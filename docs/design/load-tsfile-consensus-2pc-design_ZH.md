<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# LOAD TsFile 走共识协议及基于 2PC 的暂存目录生命周期管理

[English version](load-tsfile-consensus-2pc-design.md)

| 项目 | 内容 |
| --- | --- |
| 状态 | 设计文档（Design Doc） |
| 适用版本 | 2.0.11-SNAPSHOT |
| 参考实现 | 提交 `45946c352c`（LOAD TsFile: replicate pieces through consensus and clean up staged directories） |
| 涉及模块 | `iotdb-core/datanode`（`queryengine/plan/scheduler/load`、`storageengine/load`）、`iotdb-core/consensus` |
| 核心类 | `LoadTsFileScheduler`、`TwoPhaseConsensusLoadStrategy`、`LoadTsFileConsensusNode`、`LoadTsFileManager`、`TsFileWriterManager`、`LoadTsFileProgress`、`LoadTaskRetention`、`LoadTsFileCleaner`、`LoadTsFileSnapshot` |

---

## 一、背景与要解决的问题

旧版 LOAD TsFile 的实现存在以下三个结构性缺陷：

1. **LOAD 独立于共识协议，存在一致性风险。**
   原有设计中，协调节点将序列化后的 `LoadTsFilePieceNode` 通过独立的 RPC（`sendTsFilePieceNode`）逐个扇出（fan-out）给各副本，随后通过 `EXECUTE`/`ROLLBACK` 命令令各节点独立落盘。此链路绕开了既有的共识状态机，副本间的一致性完全依赖协调者保障。一旦节点发生宕机或重启，极易出现"部分副本落后、部分副本超前"的不一致中间态。

2. **暂存数据生命周期缺乏确定性（靠猜测）。**
   暂存文件的清理时机没有可靠依据。清理过早，会导致追赶中的副本无法读回数据；清理过晚，则会导致已结束任务的暂存文件长期占用磁盘资源（磁盘泄漏）。

3. **WAL 存在双份数据拷贝开销。**
   旧方案中，分片的 Payload 会被完整写入 WAL，导致同一份数据在"暂存文件"和"WAL"中各存一份，造成内存与磁盘的双重开销。

**本次设计的核心目标**：

- 将 LOAD 的写入路径全面并入既有共识写入路径；
- 采用**两阶段提交（2PC）**替代旧的 `EXECUTE`/`ROLLBACK` 协议；
- 通过引入由**共识水位（Consensus Watermark）**驱动的确定性回收机制来管理暂存目录。

---

## 二、总体设计方案

### 2.1 分层架构设计

协调者只需向"分区的写节点"提交一次请求，副本即可通过正常的共识日志复制收到命令，与普通写入路径完全一致，彻底摒弃协调者向每个副本单独发起 RPC 的旧模式。

```text
[协调者 DataNode]  LoadTsFileScheduler
        │ (按 needDecodeTsFile 选策略)
        ├── LocalLoadStrategy          (无需解码：整文件直接交由本地 region)
        └── TwoPhaseConsensusLoadStrategy(需解码：切分 → 流式提交 → 两阶段提交)
                 │
                 │ LoadConsensusSubmitter (按正常写路径选写节点：Ratis leader / IoTConsensus 写节点)
                 ▼
[共识层]  LoadTsFileConsensusNode (下发 5 种阶段命令: BEGIN / PIECE / PREPARE / COMMIT / ABORT)
         (经 DataRegionConsensusImpl.write → WAL 复制 → 各副本状态机)
                 ▼
[存储层 DataRegion]  DataExecutionVisitor
        └── writeLoadTsFile{Begin,Piece,Prepare,Commit,Abort}
                 └── LoadTsFileManager (每 DataRegion 一个)
                          └── TsFileWriterManager (每任务一个：管理暂存目录 + 暂存 TsFile + 进度 Progress)
```

### 2.2 两阶段提交（2PC）协议设计

#### 2.2.1 角色与事务边界定义

| 核心概念 | 设计说明 |
| --- | --- |
| 协调者（Coordinator） | 接收 LOAD 语句的 DataNode（`LoadTsFileScheduler` → `TwoPhaseConsensusLoadStrategy`）。 |
| 参与者（Participant） | 被该 TsFile 触及的每个 DataRegion（写节点执行，副本经共识日志复制自动参与）。 |
| 事务 ID | 每 Region 独立一个 `loadId`（`regionLoadIds.computeIfAbsent(regionId, UUID)`），非全局单 ID。 |
| 事务边界 | 1 个源 TsFile × 1 个 DataRegion（同一 TsFile 跨 Region 属于多个独立的 2PC 事务，避免引入跨共识组的全局事务协调器）。 |
| 提交通道 | `LoadConsensusSubmitter` → 分区写节点 → `DataRegionConsensusImpl.write` → WAL 复制 → `DataExecutionVisitor`。 |

#### 2.2.2 阶段协议与服务端语义

服务端**不保留 per-load 的内存事务状态**，协议约束与幂等性由客户端负责。服务端仅按 `loadId` 定位暂存目录并执行命令。

| 命令 | 服务端行为 | 重放 / 重复投递处理 |
| --- | --- | --- |
| `BEGIN` | `DataExecutionVisitor` 直接返回 `OK`（空操作）。暂存 writer 由首个 `PIECE` 惰性创建。保留此阶段仅用于跨版本兼容。 | 天然幂等。 |
| `PIECE` | `writePiece`：按 `(device, 时间分区)` 定位/创建暂存 TsFile，写入 chunk 与 deletion，返回 `PieceRef`，将带引用的节点写 WAL。 | 基于物理偏移幂等（见 2.2.4）。 |
| `PREPARE` | `prepare`：关闭修改文件；校验暂存文件无洞；写 metadata zone 封口、更新 TimeIndex、落 `ProgressIndex`。 | 若已 `isSealed()` 则跳过（幂等）。 |
| `COMMIT` | `loadAll`：先判断 `mustRetain` → 导入 TsFile → 结束任务并写 WAL + separator。 | 若 `loadId` 已结束且 writer 移除 → no-op 成功。 |
| `ABORT` | `deleteAll` → 结束任务并写 WAL + separator（丢弃或交接给保留机制）。 | 同上，no-op 成功。 |
| `PULL` | 仅保留命令定义以兼容旧节点，不再支持实际逻辑。 | — |

> 注：`PREPARE` / `COMMIT` 会携带协调者累计的 `pieceCount`、`totalBytes`、`checksum`，以及每个时间分区的 `ProgressIndex` 序列化字节，确保不丢失 pipe / 订阅的进度语义（避免退化为 `MinimumProgressIndex`）。

#### 2.2.3 协议时序图

```text
Coordinator                                  Region Write Peer (及其副本)
    │                                                │
    │  第一轮：流式暂存分片                              │
    │---- PIECE(0, chunks, chunkLayout) ────────────>│ 惰性创建暂存 writer，按偏移追加 chunk
    │---- PIECE(1, chunks, chunkLayout) ────────────>│ 追加 chunk（支持乱序到达）
    │---- ...                                        │
    │                                                │
    │  第二轮第 1 步：全部 Region 投票 PREPARE           │
    │---- PREPARE(pieceCount, bytes, progress) ─────>│ Region 1：校验无洞、封口暂存 TsFile
    │---- PREPARE(pieceCount, bytes, progress) ─────>│ Region 2：校验无洞、封口暂存 TsFile
    │                                                │
    │  只有全部 PREPARE 成功，才进入第 2 步               │
    │---- COMMIT ───────────────────────────────────>│ Region 1：导入暂存 TsFile（先判 mustRetain）
    │---- COMMIT ───────────────────────────────────>│ Region 2：导入暂存 TsFile
    │                                                │
 任一 PREPARE 失败（事务尚未决定）：                     │
    │---- ABORT ────────────────────────────────────>│ 每个已触达 Region 都丢弃暂存数据
    │                                                │  （任何失败都重试；此时无任何导入）
```

#### 2.2.4 2PC 高可用保障机制（核心三要素）

**机制一：偏移幂等（取代服务端去重表）**

协调者在切分时通过 `ChunkOffsetCalculator` 提前计算物理布局（`ChunkLayout`：chunkGroupHeader 偏移、chunk 偏移、是否组内首 chunk），随 `PIECE` 一起下发。服务端写入前调用 `progress.hasChunkAt(offset)`：

- **已记录**：跳过写入，并复用原引用（有 payload 时按已落位置生成 `ChunkPayloadRef`，无 payload 时直接沿用传入引用）；
- **未记录且无 payload**：抛出异常拒绝（`..._ARRIVED_WITHOUT_ITS_CHUNK_PAYLOAD`），避免静默跳过导致丢数据；`incomingRefs.size() != chunks.size()` 同样拒绝。

**优势**：piece 乱序、网络重试、WAL 重放天然收敛，服务端彻底免除维护去重表的负担（`pieceIndex` 仅用于日志关联与身份标识，不承担顺序校验）。

**机制二：决策先于行动（MustRetain before Import）**

在 `loadAll` 时，严格遵循**先**计算 `retention.mustRetain(searchIndex)`、**再**依据结果导入的顺序。原因：WAL 中仅存 `PIECE` 引用，若直接移动（而非拷贝）暂存文件，追赶中的副本将无法读回内容。决策与导入统一基于同一个 `searchIndex`，避免"决策允许删、导入却执行硬移动"的逻辑分裂。

**机制三：终止记录驱动事务可恢复**

`PREPARE` / `COMMIT` / `ABORT` 均落 WAL（`logLoadNodeToWAL` + `insertSeparatorToWAL`）。在丢弃 progress 文件**之前**，系统会将终止标记（终止算子 + 共识 index）写入 progress 尾部。

- **崩溃重启**：扫描看到终止标记，则执行清理，避免将已结束任务误判为需 resume 的任务；
- **重放命令**：内存中无对应 `loadId`（writer 已移除），直接安全返回 no-op 成功。

#### 2.2.5 提交点与失败容错矩阵

核心原则：**提交点之前全量回滚，提交点之后绝不回滚**（presumed commit）。

| 失败节点 | 触发行为 | 测试用例断言表现 |
| --- | --- | --- |
| Phase 1 失败（切分 / 任一片提交失败） | 所有已触达的 Region 触发**全量 ABORT**（尚不存在已投票的 Region）。 | 全部 ABORT |
| 任一 Region 的 `PREPARE` 失败 | 事务**尚未决定**，因此所有已触达 Region 全部 ABORT；已经 PREPARE 成功的 Region 也**不提交**。 | `PREPARE@1, PREPARE@2, ABORT@1, ABORT@2` |
| `PREPARE` 失败于首个 Region | 同上，全部 ABORT。 | `PREPARE@1, ABORT@1, ABORT@2` |
| 全部 `PREPARE` 成功 | 进入提交轮：逐 Region 发送 `COMMIT`（提交点已过，不可回滚）。 | `PREPARE@1, PREPARE@2, COMMIT@1, COMMIT@2` |
| 某个 Region 的 `COMMIT` 失败 | **不回滚任何 Region**：该 Region 可能已导入，其余 Region 已投赞成票必须继续提交（presumed commit）；返回失败交给 Tablet 兜底。 | `PREPARE@1, PREPARE@2, COMMIT@1, COMMIT@2` |
| `COMMIT` 结果未知（应答丢失） | **重发该命令**，让结果变得可判定：已导入该任务的 Region 答复**成功**，未导入的 Region 答复它真实的失败。否则"跟随一次丢失应答的重试"会把一次到处都提交成功的 LOAD 报成失败。 | `testRepeatedCommitOfTheSameTaskSucceeds` |
| `ABORT` 失败 | 对**任何种类**的失败都重试（3 次 + 退避）：对本 Region 已不持有的任务发 ABORT 会被确认为成功，所以重发无害，而放弃只会把暂存目录留给扫描兜底。重试仍失败仅告警，遗留目录由 Cleaner 回收。 | `testAbortIsRetriedOnFailure` |

> **提交点**是"全部 Region 的 `PREPARE` 都成功"这一刻：在此之前失败 → 全量回滚；在此之后失败 → 绝不回滚，只能继续提交并把失败上报。
>
> 注：为避免无用暂存目录长期占用磁盘（暂存按 `loadId` 命名，不会过期），提交点之前的任何异常都必须显式触发 ABORT；否则后续重放同一批 piece 会撞上"半填的暂存文件"，形成脏状态。

#### 2.2.6 重试策略与共识协议的协作

- **有界重试**：最大重试 3 次（`LOAD_CONSENSUS_SUBMIT_MAX_RETRIES`），退避 `100ms × attempt`。哪些命令可以重发，由"重发在 Region 侧做了什么"决定：
  - `PIECE`：仅瞬时错误（`DISPATCH_ERROR`、`INTERNAL_SERVER_ERROR`、`NO_AVAILABLE_REGION_GROUP`、`EXECUTE_STATEMENT_ERROR`）；靠偏移幂等性，重发本身无代价；
  - `PREPARE`：仅瞬时错误；已经封口的暂存文件会被原样跳过，因此重放不会写第二段元数据区；
  - `COMMIT`：仅瞬时错误；对已经导入过该任务的 Region 会答复成功——这正是"应答丢失"能被判定的原因，而真正的失败仍答复失败；
  - `ABORT`：任何种类的失败都重试，因为对本 Region 已不持有的任务发 ABORT 会被确认为成功；
  - `PIECE` / `PREPARE` 的永久失败立即上抛，由协调者触发 ABORT。
- **事务的路由**：拆分时解析出的副本集按 Region **钉住**，该任务的所有命令（PIECE、`PREPARE`、`COMMIT`、`ABORT`）都发给它，因此两个分片之间发生迁移也不会出现"分片落在一个路由、终态命令发往另一个路由"。瞬时失败是"路由可能已不是持有该 Region 的那个"的信号：此时会**先丢弃本地分区缓存再重新解析**（缓存命中只会回答它已有的路由），同样是有界重试；解析出的路由若**发生变化**，则由整个事务**采纳**，该 Region 之后的所有命令都走新路由。完全解析不到路由时视为没有路由：命令在原路由上重试，由那次尝试的结果决定后续，绝不把一个旧路由悄悄换成空路由。
- **Ratis 共识**：完整命令（含 chunk 数据）走 Ratis Log，各副本独立写自己的暂存目录。
- **IoTConsensus**：写节点 apply 后，向 Follower 仅复制几十字节的 marker-only 条目（`pieceMarker`：含索引、校验和与字节数）。实际数据通过 `ChunkPayloadRef` 在发送时延迟回读（延迟物化）。

### 2.3 切分与流式投递（Phase 1）

- **处理流**：`TsFileSplitter` 产出 `TsFileData`（CHUNK / DELETION）→ `TsFileSplitConsumer` 进行内存缓冲与 `DataPartitionRouter` 路由 → `PieceDispatcher` 派发。
- **内存预算限制**：基于 `MemoryBoundedBuffer`（预算 = `thriftMaxFrameSize >> 2`），受集群级 LOAD cache 同步约束；超载时触发 `PieceDispatcher` 的最大优先（largest-first）淘汰机制。
- **DELETION 语义**：删除操作会被安全复制到所有已缓冲的 piece 中（数据先路由、删除后写入，保障删除语义边界）。
- **进度索引**：切分阶段同时为每个时间分区生成 `ProgressIndex`，供 `PREPARE` / `COMMIT` 携带。

### 2.4 暂存目录结构与无洞续传

- **目录布局**：`<load dir>/<database-region>/<load id>/`（`LoadStagingDirs`），与 DataRegion 的 `sequence`/`unsequence` 数据区布局解耦。
- **Progress 位图管理**：`LoadTsFileProgress` 是只追加（append-only）的进度文件，精准记录 chunk 偏移、chunk header / statistics 元数据、物理数据区间与共识 index。
- **恢复与校验**：重启后从这些记录重建 `TsFilePrecalculatedChunkWriter` 的元数据，并在最后一条完整 chunk 之后续写。完整性只有一处判据 —— `isReady(fileLength)`，由封口（`prepare`）、Cleaner（`isComplete`）与续传共用：记录区间必须**无空洞**地从 TsFile header 覆盖到文件末尾，且文件至少持有这些字节。空洞意味着某个分片没到；把这种文件封口会把零字节当成数据导入，而 Cleaner 会删掉副本还需要回读的字节。
- **迟到的分片**：chunk 按自身内容决定的绝对偏移写入，因此"晚到但计划偏移在前"的分片会写回自己的区间，而不是追加到文件末尾。否则该计划区间会永久留下空洞，"有空洞 = 分片没到"这个判据也就不再成立。
- **进度文件尾部是半条条目**：这正是"分片仍在追加时打了暂存目录快照"会留下的形态（拷贝与追加之间没有互斥）。读取方会丢弃这段碎片、并继续使用它之前的完整条目，而不是把整个暂存文件判为无法归属。
- **无法续传的暂存文件**：其分片会**响亮失败**而不是被静默丢弃——静默丢分片会让副本导入一个缺数据的文件，而且下游无从察觉。

### 2.5 暂存目录确定性回收（Watermark-Driven）

彻底摒弃超时（Timeout）猜测，由系统真实进展驱动生命周期管理：

- **核心链路**：任务结束写入终止标记 → `LoadTaskRetention` 依据 WAL safe-deletion watermark 决定释放时机 → `LoadTsFileCleaner` 执行清理（水位推进即触发，不等轮询）。
- **回收条件**：必须所有副本都越过该 index（`replicasReached` 为真）。对于 `COMMIT`，还需额外满足 `isComplete`（暂存文件无洞且覆盖至末尾），防止副本无法读回 payload；`ABORT` 的任务在水位越过后即可删除（从未被导入）。
- **降级支持**：共识 V2 及一切没有 WAL 水位的协议报告默认水位 `ConsensusReqReader.DEFAULT_SAFELY_DELETED_SEARCH_INDEX`（`Long.MIN_VALUE`，表示"从未上报"）；该值被特殊判定为"没有 follower 需要等待"，因此 `COMMIT`/`ABORT` 执行完即可释放。
- **重启兜底**：系统重启时自动扫描暂存目录并读取尾部终止记录，补删遗留的垃圾目录。

### 2.6 零拷贝与快照隔离

- **WAL 引用零拷贝**：全局仅维护一份 Payload（在暂存文件中）。WAL、复制队列及内存记账均使用 `ChunkPayloadRef` 引用；配套修改了 `IndexedConsensusRequest` / `LogDispatcher` / `SubscriptionQueueRegistry` / `IoTConsensusMemoryManager` 的预留记账逻辑（入队时快照预留量、释放时按同值归还），避免内存漂移。
- **引用按暂存根相对记录**：`PieceRef` 与 `ChunkPayloadRef` 记录的是文件在**所属暂存根内部**的路径，读取方在自己的各个暂存根下解析它。一个 DataNode 可以把任务分布在多个暂存根、两个节点的根名可以不同、恢复出来的任务也可能落在与来源不同的根上；绝对路径在这三种情况下都会失去对文件的描述能力。绝对路径仍然兼容（旧版本写下的引用照旧可读）。
- **快照隔离**：`LoadTsFileSnapshot` 负责 LOAD 暂存树的 snapshot / restore。通用 `SnapshotTaker` / `SnapshotLoader` 显式跳过 LOAD 目录，因此 `.progress` 不会被误认为数据文件；恢复中的副本继承"部分物理状态 + 进度位图"，可以继续一个未完成的 2PC。
  - **哪些协议需要它**：IoTConsensus 的副本靠"以成员身份应用到的日志条目"重建暂存文件，因此中途加入的副本必须通过迁移快照继承暂存状态；暂存目录的快照因此只在 IoTConsensus V1 / V2 下产生，Ratis 自己会把每个分片的 payload 复制到每个副本，跳过。
  - **无锁的一致拷贝**：**先拷进度日志、后拷它描述的暂存文件**，而暂存文件只在尾部增长，因此拷到的日志绝不会引用同批拷贝的文件里没有的字节；拷贝唯一可能抓到的碎片（日志尾部半条条目）由读取方丢弃。若为整段拷贝而阻塞写入方，会在大文件拷贝期间让 Region 停止应用分片。
  - **快照内容**：清单 `load/roots` 记录每个任务的来源暂存根；恢复时"本节点有该根就回该根，没有就落到本节点有的根"。拷贝出的文件只在完整时才以最终名发布（`.copying.` + 原子 move）；枚举失败会让快照整体失败，而不是静默少拷。

### 2.7 配置与失败兜底

- **配置**：新增 `setLoadTsFileDirs`（同步刷新 canonical paths）；`loadTsFileSpiltPartitionMaxSize` 下界校验明确为 `>= 1`（0 值会让所有跨时间分区的 LOAD 全部失败）。
- **失败兜底**：`LoadFallbackHandler` 将失败的 TsFile 转为 Tablet 重试；源文件在整批 LOAD 结束后才物理删除，保证 `deleteAfterLoad` 语义安全。

---

## 三、核心设计思想（Architecture Philosophy）

1. **拥抱既有共识栈，避免重复造轮子**：LOAD 写入与普通写入路径统一，免费继承一致性、故障恢复与追赶语义。
2. **命令自描述 + 无状态服务端**：阶段命令自带充足上下文（总量、校验值、物理布局）。服务端不维护易失状态机，极大增强对重复投递和乱序的健壮性。
3. **物理事实优先于内存状态**：采用"偏移量幂等"取代"去重表"，同一片写两次天然安全；进度文件 append-only，续传与回收都依赖磁盘上的确凿记录。
4. **生命周期由真实水位驱动**：用共识系统真实的 Watermark 判定生死，放弃脆弱的超时或引用计数机制。
5. **删除保守、恢复激进**：删除条件取保守侧（无洞 + 水位推进），恢复取激进侧（能读多少算多少，支持带洞续写）。
6. **回滚需"知止"**：提交点是"全部参与者投赞成票（PREPARE 成功）"这一刻；在此之前全量回滚，在此之后（presumed commit）绝不回滚任何参与者，避免制造一致性黑洞——无法原子的跨 Region 失败交给上层 Tablet 重试。
7. **决策始终先于行动**：`mustRetain` 在导入之前求值并作为参数下传，杜绝判断与执行不一致。
8. **职责单一、关注点隔离**：切分 / 路由 / 缓冲 / 派发 / 提交 / 回滚 / 兜底各成可独立测试的类；清理是 DataNode 级单例；LOAD staging 的快照逻辑不污染通用 DataRegion 快照；LOAD 相关类收敛在 `scheduler.load` 与 `storageengine.load` 两个包内。

---

## 四、兼容、降级与功能全景图

### 4.1 异常降级与兜底策略

- **Tablet 降级重试**：只要 2PC 过程出现跨 Region 不一致或最终失败，`LoadFallbackHandler` 会将失败的 TsFile 自动转为 Tablet 模式（支持 Table / Tree model，分别走 `convertForTableModel` / `convertForTreeModel`）重试，成功则状态置为 `FINISHED`，否则 `FAILED`。源文件在整批加载结束后才物理删除。
- **非严格原子性声明**：该方案是以共识日志为通道、物理偏移做幂等、共识水位定生死的**乐观两阶段提交**。提交轮里某个 Region 未能导入（A 已导入、B 未导入）这种跨 Region 的中间态，由上述 Tablet 重试机制兜底补救；但"提交点之前"绝不会出现部分导入——任一 `PREPARE` 失败即全量回滚。

### 4.2 兼容性与降级

- **协议降级**：共识 V2 及无副本环境自动降级为"立即释放"模式。
- **版本兼容**：明确不再支持旧的 `PULL` 协议；旧节点调用 `sendLoadCommand` 将返回"协议已移除"报错；但依然兼容旧版带 slice 元数据的分片投递（#18627 链路已作适配保留）。
- **本地化策略**：单机 / 本地直接加载触发 `LocalLoadStrategy`，不跨网络。

### 4.3 监控与配置

- **配置校验**：新增 `setLoadTsFileDirs` 并强制 `loadTsFileSpiltPartitionMaxSize >= 1`，避免 0 值导致大面积失败。
- **观测指标**：提供点数指标 `LoadPointCountMetrics` 及多阶段耗时指标（`LoadTsFileCostMetricsSet`：`FIRST_PHASE`、`SECOND_PHASE`、`SCHEDULER_CAST_TABLETS` 等）。

### 4.4 功能清单

**调度与切分**

- 按 `needDecodeTsFile` 分流的两条策略：本地直载（不解码）与两阶段共识载入（解码）；
- TsFile 切分为 CHUNK / DELETION；批量分区查询与 region 路由（含 table model / pipe 的 database 提示）；
- 内存预算控制与 largest-first 淘汰；EOF 时 flush 剩余 piece；
- region 迁移检测（副本集变化即报错 `RegionReplicaSetChangedException`）；
- 失败兜底：TsFile → Tablet 重试，最终状态机 `FINISHED` / `FAILED`。

**2PC 协议**

- 阶段命令 BEGIN / PIECE / PREPARE / COMMIT / ABORT 经共识状态机执行；BEGIN 为空操作、writer 由首个 PIECE 惰性创建；旧 PULL 不再支持；
- 每 Region 一个 `loadId` 的事务划分；PREPARE / COMMIT 携带 `pieceCount` / `totalBytes` / `checksum` / 每时间分区 ProgressIndex；
- 提交给分区写节点：Ratis 走 leader，IoTConsensus 走写节点，本地走 `RegionWriteExecutor`，远端走内部 RPC（`sendBatchPlanNode`）；
- 偏移幂等、PREPARE 的无空洞完整性校验与 `isSealed` 重放跳过、COMMIT / ABORT 对已结束 `loadId` 的 no-op（Region 记住已提交的 task，对重复 COMMIT 答复成功）；
- 两轮提交：先向所有已触达 Region 发 `PREPARE`，全部成功后才向所有 Region 发 `COMMIT`（提交点 = 全部 PREPARE 成功）；
- 失败矩阵（Phase 1 全量 ABORT / 任一 PREPARE 失败即全量 ABORT / COMMIT 失败不回滚已投票的 Region / ABORT 任何失败都重试）；
- `mustRetain` 先于导入的决策顺序（保留时导入拷贝）；
- 按命令区分的有界重试（3 次 / 100ms 退避）：PIECE / PREPARE / COMMIT 仅瞬时错误，ABORT 任何失败；
- 事务路由按 Region 钉住，瞬时失败时先丢弃分区缓存再重新解析；变化的路由被整个事务采纳，解析不到的路由绝不采纳。

**暂存写入**

- 每任务独立目录；每时间分区一个暂存 TsFile；`TsFilePrecalculatedChunkWriter` 直接写入并预置元数据；
- PIECE 写入记录 chunk 物理落点并返回 `PieceRef`；`prepare` 封口、`loadAll` 导入、`close` 丢弃；
- 迟到分片写回自身计划偏移；无法续传的暂存文件让分片响亮失败，而不是被静默丢弃；
- 引用按暂存根相对记录（`PieceRef`、`ChunkPayloadRef`），读取方在自己的根下解析，绝对路径仍兼容；
- chunk 引用（`ChunkPayloadRef`）与"payload 不可用"异常（`ChunkPayloadUnavailableException`）语义。

**进度与续传**

- append-only progress 文件（magic、uuid、每 chunk 的物理区间与元数据、共识 index）；
- 重启恢复：按记录重建 writer 元数据；支持"文件中间有洞"续写；由 `prepare` 与 Cleaner 共用的无空洞 `isReady` 判据；丢弃撕裂的尾部条目后继续使用其之前的条目；终止标记避免误 resume。

**回收与保留**

- 尾部终止记录（算子 + index）；`LoadTaskRetention` 保留目录直至 WAL 水位越过；`LoadTsFileCleaner` 扫描 + 注册表双路径删除；
- `ABORT` 在水位过后即删、`COMMIT` 需额外满足"无洞"；V2 / 无副本立即删；重启补删遗漏目录。

**WAL / 共识 / 内存**

- 新 WAL 条目类型 `LOAD_TSFILE_CONSENSUS_NODE(13)`（含 isUserData 归类、序列化 / 反序列化、`IWALNode.log(memTableId, node)`）；
- WAL 只存引用：下游 V1 sync 转发时用 `LoadPieceConsensusRequest` 按需回读；
- `IWALNode.setSafeDeletedSearchIndexListener` + `getSafelyDeletedSearchIndex()` 水位回调链路；
- 延迟序列化请求的队列内存记账修正（`getQueueReservedMemorySize`、`hasDeferredRequests`）。

**快照**

- LOAD staging 目录的 snapshot / restore / clear；通用快照路径排除 LOAD 目录；进行中任务的暂存文件与 progress 位图随快照一起恢复；
- 暂存目录快照只在 IoTConsensus V1 / V2 下产生；`load/roots` 清单让恢复把每个任务放回来源根；进度日志先于暂存文件拷贝、文件原子发布、枚举失败让快照整体失败。

### 4.5 测试覆盖

- **单元测试**：scheduler、两阶段策略与失败矩阵及路由跟随（`TwoPhaseConsensusLoadStrategyAbortTest` 八个用例）、写点与事务路由（`LoadConsensusSubmitterTest`：Ratis leader 按 nodeId 命中、leader 不在路由内、钉住路由不被刷新、解析前先丢弃缓存、解析不到路由）、共识节点序列化往返（`LoadTsFileConsensusNodeTest`）、manager（`LoadTsFileManagerTest`：乱序缺片让 PREPARE 失败、补齐后成功；撕裂进度条目在恢复时被修复；无 writer 可恢复的暂存文件让分片失败；PREPARE / ABORT / COMMIT 重放；payload 引用边界；暂存路径边界；曾被忽略的"续传任务元数据丢失"用例）、resume（`LoadTsFileResumeTest`）、progress（`LoadTsFileProgressTest`）、切分直写与偏移幂等（`LoadTsFileManagerSplitCoverageTest#testPieceAppliedTwiceIsWrittenOnce`、`#testIdenticalPieceWithPayloadIsNotWrittenTwice`）、暂存快照（`LoadTsFileSnapshotTest`：任务文件被拷贝、分片应用期间拍下的快照仍可恢复、多根恢复含"根更少"的节点、协议守卫）、dispatcher、slice assembler、共识请求（`IndexedConsensusRequestTest`）、订阅队列（`SubscriptionQueueRegistryTest`）。
- **集成测试**：`IoTDBLoadTsFileClusterIT`（集群正常载入、停掉一个 DataNode 后载入两个场景）。运行方式：先 `mvn clean package -pl distribution -am -DskipTests`，再 `mvn verify -DskipUTs -Dit.test=IoTDBLoadTsFileClusterIT -pl integration-test -am -PClusterIT -P with-integration-tests`。
