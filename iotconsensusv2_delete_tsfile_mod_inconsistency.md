# IoTConsensusV2 并发 Delete + TsFile 同步导致副本不一致：证据链与修复说明

## 0. 结论摘要

在本次 benchmark 场景里，IoTConsensusV2 的副本不一致，根因已经收敛为：

1. 本地 IoTV2 `DELETE` 的传播，与 sealed TsFile 上 `.mods` 的物化，不是一个原子动作；
2. `PipeTsFileInsertionEvent` 的第一次 `increaseReferenceCount()` 会把 tsfile/mod 的传输快照 pin 下来；
3. 如果这个第一次 pin 发生在 delete 把对应 `.mods` 写出来之前，那么后续即使 delete 已经成功同步，tsfile 事件也可能仍然带着“无 mod”的旧快照；
4. follower 若先执行 `delete(N)`，后 `seal/load tsfile(N+1)`，这个 tsfile 上本应删除的数据就会“回弹”。

这里要特别区分两件事：

- “event 被创建 / TsFileEpoch 被 extracted”
- “event 第一次被 pin / snapshot 用于传输”

真正缺失同步的是第二件事，不是单纯的“event 创建得早”。

另外，之前分支上做的两类修复仍然是有价值的，但它们不是根因闭环：

- `PipeTsFileInsertionEvent` 动态 refresh mod，可避免把 mod 是否存在永久冻结在构造时；
- pinned mod path 稳定化，可避免 reference tracking 与实际传输文件不一致；
- 更关键的是，第一次 pin 完成后必须冻结 snapshot，后续不能再从 live resource 吸收“未来才出现”的 mod。

但仅靠 refresh 仍然不够，因为 tsfile 可能已经更早被 pin 住；此时再 refresh，也改不了已经固定下来的传输快照。最终根修复需要把“delete 的 sealed-file materialization”与“tsfile 的第一次 pin”串起来。

在把 barrier 补上之后，又进一步发现一个 residual window：

1. realtime 原始 event 在 assigner 第一次 pin 后，`tsFile` 已经变成 pipe 目录里的 hardlink；
2. 但后续 `shallowCopy` 给各个 pipe source 创建副本时，旧代码并没有继承已经 pin 好的 `modFile`；
3. 这些副本仍然会重新读取 live `resource.getExclusiveModFile()`；
4. 如果此时 source mod 已经被 compaction / settle / replace 删除或替换，副本仍然可能丢 mod。

因此最终修复除了 delete barrier 之外，还要保证：

- 首次 snapshot/pin 在资源锁保护下完成；
- 第一次 pin 完成后，事件的 mod snapshot 立即冻结；
- `shallowCopy` 继承已经 pin 好的 mod snapshot 状态，而不是回头读取 live resource。

---

## 1. 测试场景与异常现象

### 1.1 场景

- 共识协议：IoTConsensusV2（默认参数）
- 负载：benchmark 并发读写，过程中 CLI 执行 delete
- 操作：未执行任何节点故障/启停

### 1.2 校验 SQL

```sql
select
  count(s_0),count(s_1000),count(s_2000),count(s_3000),count(s_4000),
  count(s_5000),count(s_6000),count(s_7000),count(s_8000),count(s_9999)
from root.treedb.g_0.** align by device;
```

### 1.3 结果差异（Tree）

证据文件：

- `/Users/pengjunzhi/Code/iotdb-11/bug-report-and-log/v2082rc6_iotv2_query_result/q_all_online_tree.out`
- `/Users/pengjunzhi/Code/iotdb-11/bug-report-and-log/v2082rc6_iotv2_query_result/q_stop_ip2_tree.out`

关键行均为第 58 行：

- all-online：`root.treedb.g_0.nonaligned_12` 的 `count(s_1000)=26960`
- stop-ip2：`root.treedb.g_0.nonaligned_12` 的 `count(s_1000)=27350`

差值为 `+390`，说明某个副本上 `s_1000` 的 delete 没有完整体现在查询结果里。

### 1.4 删除语句确实覆盖问题列

证据文件：

- `/Users/pengjunzhi/Code/iotdb-11/bug-report-and-log/delete-sql.txt`

第 4 行包含：

```text
delete from root.treedb.g_0.nonaligned_12.s_1000 ...
```

---

## 2. 关键证据链

本节只关注同一个问题 tsfile：

- Region：`DataRegion 21`
- tsfile：`1773977715979-129-0-0.tsfile`
- 路径：`root.treedb.g_0.nonaligned_12.s_1000`

### 2.1 Leader（dn_ip2）侧证据

日志：

- `/Users/pengjunzhi/Code/iotdb-11/bug-report-and-log/v2082rc6_iotv2_deletion_logs/dn_ip2/log_datanode_all.log`

片段可用：

```bash
nl -ba /Users/pengjunzhi/Code/iotdb-11/bug-report-and-log/v2082rc6_iotv2_deletion_logs/dn_ip2/log_datanode_all.log | sed -n '19935,19946p'
```

关键日志：

```text
19937 2026-03-20 11:36:02,284 ... TsFileEpoch not found ... creating a new one
19938 2026-03-20 11:36:02,285 ... All data in TsFileEpoch ... was extracted
19939 2026-03-20 11:36:02,293 ... [Deletion] ... nonaligned_12.s_1000 ... written into 19 mod files
19940 2026-03-20 11:36:02,494 ... DeleteNodeTransfer: no.160 event successfully processed!
19945 2026-03-20 11:36:04,356 ... transferred file ... replicate index=161
```

这段日志能证明：

1. `11:36:02.285`，leader 已经把这个 tsfile 对应的 epoch 提取出来；
2. `11:36:02.293`，delete 随后才把 `nonaligned_12.s_1000` 写入 sealed tsfile 的 `.mods`；
3. `11:36:02.494`，delete 事件以 `replicateIndex=160` 处理成功；
4. `11:36:04.356`，对应 tsfile 事件以 `replicateIndex=161` 完成传输。

这正是“leader 侧先出现 tsfile 提取，再出现 deletion 写 mod，再出现 delete 复制成功，最后才完成 tsfile 传输”的完整证据链。

### 2.2 Follower（dn_ip4）侧证据

日志：

- `/Users/pengjunzhi/Code/iotdb-11/bug-report-and-log/v2082rc6_iotv2_deletion_logs/dn_ip4/log_datanode_all.log`

片段可用：

```bash
nl -ba /Users/pengjunzhi/Code/iotdb-11/bug-report-and-log/v2082rc6_iotv2_deletion_logs/dn_ip4/log_datanode_all.log | sed -n '18304,18338p'
```

关键日志：

```text
18306 2026-03-20 11:36:02,488 ... start to receive ... replicateIndex:160
18307 2026-03-20 11:36:02,493 ... [Deletion] ... nonaligned_12.s_1000 ... written into 6 mod files
18308 2026-03-20 11:36:02,493 ... process ... replicateIndex:160 ... successfully

18309 2026-03-20 11:36:02,520 ... start to receive ... replicateIndex:161
18326 2026-03-20 11:36:03,677 ... starting to receive tsFile seal
18328 2026-03-20 11:36:04,352 ... Load tsfile in unsequence list ...
18331 2026-03-20 11:36:04,353 ... TsFile ... successfully loaded ...
18338 2026-03-20 11:36:04,354 ... process ... replicateIndex:161 ... successfully
```

这段日志能证明：

1. follower 先完整执行了 `delete(160)`；
2. 然后才接收并 seal/load 了 `tsfile(161)`；
3. 如果这个 tsfile 传过来时不带 `.mods`，那么 follower 上该 tsfile 的删除效果就不会被补上，最终查询结果会偏大。

---

## 3. 关于 replicateIndex 与 extracted 时序的澄清

之前一个很容易混淆的点是：

- `TsFileEpoch` 被 extracted 的时间
- `replicateIndex` 被赋值的时间
- `PipeTsFileInsertionEvent` 第一次被 pin 的时间

它们不是同一件事。

`replicateIndex` 的赋值发生在 Pipe 分发阶段，而不是 `TsFileEpoch` 被 extracted 的那个瞬间。代码位置：

- `/Users/pengjunzhi/Code/iotdb-11/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/source/dataregion/realtime/assigner/PipeDataRegionAssigner.java`

关键逻辑：

```java
if (DataRegionConsensusImpl.getInstance() instanceof IoTConsensusV2
    && IoTConsensusV2Processor.isShouldReplicate(innerEvent)) {
  innerEvent.setReplicateIndexForIoTV2(
      ReplicateProgressDataNodeManager.assignReplicateIndexForIoTV2(source.getPipeName()));
}
```

所以，leader 日志里“tsfile 更早 extracted”只说明它更早进入了 realtime 提取链路，并不能单独推出“它的 replicateIndex 一定更小或更大”。

本次最终收敛后的判断是：

- follower 日志已经足够证明最终外部执行顺序是 `delete(160)` 在前、`tsfile(161)` 在后；
- leader 日志已经足够证明 `.mods` 是在 tsfile 被 extracted 之后才物化出来；
- 真正决定是否丢 delete 的，是 tsfile 第一次 pin/snapshot 是否早于 delete 的 sealed-file materialization。

也就是说，根因并不是“必须存在 replicateIndex 逆序赋值 bug”，而是“pin 时刻缺少与 delete materialization 的同步”。

---

## 4. 为什么“仅在传输前 refresh mod”仍然不够

分支之前的修复做了两件正确的事：

1. `PipeTsFileInsertionEvent` 不再把 `isWithMod/modFile` 永久冻结在构造时，而是在使用前 refresh；
2. 一旦 mod 已被 pin，就保持 pinned mod path 稳定，不再被后续 refresh 覆盖。

这些修复解决了两类真实问题：

- event 创建后、真正传输前，晚到的 `.mods` 能被看见；
- 避免 reference count 绑定的是一个 mod 文件，而实际传输又换成另一个 mod 文件。

但它仍然不能覆盖下面这个窗口：

1. tsfile event 已经在上游某个环节调用了第一次 `increaseReferenceCount()`；
2. 这次调用把 tsfile 以及当时能看到的 mod 快照 pin 下来；
3. 随后 delete 才去 sealed tsfile 上写 `.mods`；
4. 再晚一点即使 `refreshModFileState()` 能看到新 mod，也不一定还能安全替换已经 pinned 的传输快照。

换句话说：

- refresh 解决的是“看不看得见晚到 mod”
- barrier 解决的是“能不能保证第一次 pin 不早于 delete materialization”

后者才是这次副本不一致的根因闭环。

---

## 5. 真实根因（代码级）

### 5.1 缺失同步的位置

delete 路径里，本地 IoTV2 删除会经历：

1. WAL / delete node 处理；
2. 通过 `PipeInsertionDataNodeListener.listenToDeleteData(...)` 把 delete 事件发布到 Pipe / IoTV2；
3. 再把对应 sealed tsfile 上的 `.mods` 真正写出来。

而 tsfile 事件在另一条链路里，会在第一次 `increaseReferenceCount()` 时 pin 住将要传输的 tsfile/mod。

原来这两条链路之间没有 per-tsfile 的同步机制，所以会出现：

- delete 已经发出；
- 但对应 sealed tsfile 的 `.mods` 还没写完；
- tsfile 事件已经先一步完成了第一次 pin。

### 5.2 这会怎样导致 follower 不一致

当 follower 的外部执行顺序是：

1. `delete(160)` 先执行；
2. `tsfile(161)` 后 seal/load；

如果 `tsfile(161)` 传来的仍然是 delete 物化前 pin 下来的“无 mod 快照”，那么 follower 载入这个 tsfile 后，对应删除就会缺失，表现出来就是 count 偏大。

---

## 6. 修复方案

### 6.1 既有加固：继续保留动态 mod refresh 与 pinned path 稳定化

文件：

- `/Users/pengjunzhi/Code/iotdb-11/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/event/common/tsfile/PipeTsFileInsertionEvent.java`

这部分是分支上已有修复，但最终语义已经收敛为“pin 前可刷新，pin 后冻结”：

1. 在第一次 pin 之前，`refreshModFileState()` 仍然会读取 live `resource.anyModFileExists()`，因此 event 创建后、pin 前晚到的 `.mods` 仍可被观察到；
2. 一旦第一次 pin 完成，`PipeTsFileInsertionEvent` 会立刻冻结 mod snapshot，不再回头读取 live `resource.getExclusiveModFile()`；
3. 已经 pin 好的 `modFile` 路径会保持稳定，不会再被后续 refresh 覆盖。

这一步解决的是三件事：

- pin 前的晚到 mod 可见性；
- pinned mod path 正确性；
- 防止旧 tsfile event 在 pin 之后误吸收未来 delete 产生的 mod。

### 6.2 根修复：给 sealed tsfile 的 delete materialization 增加 barrier

新增文件：

- `/Users/pengjunzhi/Code/iotdb-11/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/consensus/deletion/PipeTsFileDeletionBarrier.java`

核心思路：

1. delete 路径先算出这次本地 IoTV2 delete 会影响哪些 sealed tsfile；
2. 在真正发布 delete 并物化 `.mods` 之前，把这些 tsfile path 注册到 barrier；
3. tsfile event 在第一次 `increaseReferenceCount()` 前，先检查该 tsfile 是否存在 pending deletion；
4. 若存在，则等待 delete 完成 sealed-file `.mods` 物化后再继续 pin/snapshot；
5. delete 完成后释放 barrier。

落地位置：

- `DataRegion.deleteByDevice(...)`
- `DataRegion.deleteByTable(...)`
- `DataRegion.deleteDataDirectly(...)`
- `PipeTsFileInsertionEvent.internallyIncreaseResourceReferenceCount(...)`

对应代码文件：

- `/Users/pengjunzhi/Code/iotdb-11/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/storageengine/dataregion/DataRegion.java`
- `/Users/pengjunzhi/Code/iotdb-11/iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/event/common/tsfile/PipeTsFileInsertionEvent.java`

### 6.3 补齐剩余窗口：首次 snapshot 加锁，shallow copy 继承 frozen snapshot

在补上 barrier 之后，还需要再解决一个很隐蔽的 realtime-only 问题：

1. 原始 realtime `PipeTsFileInsertionEvent` 在 assigner 的第一次 `increaseReferenceCount()` 时，会把 tsfile/mod snapshot 到 common pipe 目录；
2. 随后 assigner 会对 event 做 `shallowCopy`，为每个 pipe source 生成副本；
3. 旧代码里，`shallowCopy` 只继承了已经 pin 好的 `tsFile`，却没有继承“mod snapshot 已冻结”这个状态；
4. 当原始 event 已经 pin 到有 mod 时，副本可能丢掉已 pin 的 `modFile`，重新读取 live `resource.getExclusiveModFile()`；
5. 当原始 event 已经 pin 到“无 mod”时，副本也可能重新看见未来才出现的 mod；
6. 如果此时源 `.mods` 已被 compaction / settle / replace 删除或替换，副本还可能再次丢 mod。

本次补充修复做了两件事：

1. `PipeTsFileInsertionEvent` 的第一次 snapshot/pin 改为在 `TsFileResource.readLock()` 下完成；
2. 若需要 pin live exclusive mod，则在同一阶段持有对应 `ModificationFile.writeLock()` 复制 mod；
3. 第一次 pin 完成后，event 会把“mod snapshot 是否还允许从 live resource refresh”切成 false，从而冻结 snapshot；
4. `shallowCopy` 会直接继承这份 frozen snapshot 状态，包括：
   - 已经 pin 好的 `modFile` 路径；
   - 以及“已经确认当前 snapshot 没有 mod”这个空快照状态；
5. 后续 pipe source 副本再做第一次 pin 时，会基于这份 frozen snapshot 继续复制，而不是回头读 live resource。

这样可以避免：

- 首次 snapshot 过程中，source tsfile 被 compaction/remove 删除；
- 原始 event 已经拿到 pinned snapshot，但 shallow copy 仍然回头依赖 live mod；
- 原始 event 已经 pin 到“无 mod”快照，但 shallow copy 又错误吸收未来 mod；
- source mod 被 merge/replace 后，pipe 副本看不到已经 pin 好的 mod。

### 6.4 为什么这套组合修复才是当前闭环

它把系统收敛到两个都安全的分支：

1. tsfile event 的第一次 pin 早于 delete 注册 barrier

这说明 tsfile 已经先进入复制顺序，它会保持更早的外部顺序；follower 先 load tsfile、后 apply delete，是安全的。

2. delete 先注册 barrier，再发生 tsfile event 的第一次 pin

这时 tsfile event 会等待，直到 delete 完成 sealed-file `.mods` 物化，再把带 mod 的快照 pin 下来；follower 即使先 apply delete、后 load tsfile，也仍然安全。

真正不安全的只有第三种：

- delete 已经开始影响该 tsfile，但 barrier 尚未建立；
- 同时 tsfile 又先完成了第一次 pin。

这正是本次修复消除的窗口。

---

## 7. 回归测试

测试文件：

- `/Users/pengjunzhi/Code/iotdb-11/iotdb-core/datanode/src/test/java/org/apache/iotdb/db/pipe/event/PipeTsFileInsertionEventTest.java`

本次新增：

- `testIncreaseReferenceCountWaitsForPendingDeletionBarrier`
- `testShallowCopyKeepsPinnedModSnapshotAfterSourceModDisappears`
- `testPinnedEventDoesNotAdoptFutureModFile`

覆盖点：

1. 手工为某个 tsfile path 注册 barrier；
2. 在另一个线程里调用 `event.increaseReferenceCount(...)`；
3. 断言它会先阻塞；
4. 阻塞期间创建 mod 文件；
5. 释放 barrier 后，断言事件成功完成 pin，并且能观察到 mod。

分支上原有的 late-created mod 可见性测试也继续保留，因此现在覆盖了三类行为：

- mod 在 event 创建后、pin 前出现
- delete 先占住 barrier，tsfile 的第一次 pin 必须等待
- 原始 event 已经 pin 了 mod snapshot 后，即使 source mod 消失，shallow copy 仍然沿用 pinned snapshot
- 原始 event 已经 pin 到“无 mod”快照后，即使未来真的出现 mod，旧事件及其 shallow copy 也不会再吸收它

---

## 8. 验证结果

执行命令：

```bash
./mvnw -pl iotdb-core/datanode -am \
  -Dtest=org.apache.iotdb.db.pipe.event.PipeTsFileInsertionEventTest,org.apache.iotdb.db.pipe.event.TsFileInsertionEventParserTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false \
  test -DskipITs
```

结果：

- `BUILD SUCCESS`

---

## 9. 仍建议后续单独跟进的问题

`historicalExtractor` 里 historical deletion 没有 assign replicateIndex，这个判断依然成立，而且值得单独修。

它对应的是另一个独立风险：

1. historical tsfile 路径会 assign IoTV2 replicateIndex；
2. historical deletion 路径当前没有补齐 replicateIndex；
3. `IoTConsensusV2Processor` 会过滤 `NO_COMMIT_ID` 事件；
4. 因而 historical deletion 存在被 IoTV2 处理链过滤掉的风险。

但它不是这次 benchmark 事故的主因，因为本次事故里的 deletion 已经明确在 leader/follower 两侧都被成功处理了。

建议另开 issue 跟进：

1. 在 historical deletion 路径补齐 IoTV2 replicateIndex；
2. 增加对应回归测试；
3. 与本次副本不一致修复分开提交，降低回归面。
