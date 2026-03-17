# Review: Replica Consistency Check and Repair Plan

## 主要发现（按严重级别）

- **P0：IBF 解码结果与修复输入不闭环，当前定义下无法直接落地**
  - 方案中 `Key=xxHash64(deviceId||measurement||timestamp)`，但后续流式修复直接使用 `deviceId/measurement/timestamp` 查询原始点；中间缺少“哈希键 -> 原始点位”的反查索引设计。
  - 建议：明确一种可实现路径（例如在参与 IBF 的窗口内构建临时 `key -> rowRef` 映射，或将可逆编码扩展为携带定位信息）。

- **P0：`last_repaired_watermark` 单一水位可能掩盖局部失败，造成永久漏检**
  - 有效范围定义为 `(lastRepairedWatermark, T_safe]`，但失败仅记录在 `failed_partitions` 文本字段；如果本轮部分分区失败但全局水位仍推进，失败区间可能被跳过。
  - 建议：改为“分区级/子范围级 checkpoint”（至少 `partition_id + repaired_to`），或仅在范围内全成功时推进全局水位。

- **P1：XOR 聚合存在可抵消性，存在误判为一致的理论窗口**
  - XOR 作为集合聚合摘要存在信息损失，不同差异组合可能相互抵消，导致误判一致。
  - 建议：至少使用双摘要（如 `xor + sum(mod 2^64)`），或改为真正树形聚合（`parent=H(left||right)`）。

- **P1：`Leader wins` 与弱一致/领导权切换语义可能冲突**
  - 方案中多处采用“Leader 版本覆盖”，但 leader 转移与复制延迟窗口下，若不统一按 `ProgressIndex/term` 做因果裁决，可能覆盖掉更“新”的删除或写入。
  - 建议：将冲突裁决统一定义为“更高 `ProgressIndex` 获胜”，并落实到所有 repair record 的 apply 逻辑。

- **P1：修复应用阶段缺少明确原子提交边界**
  - Procedure 有状态机和 checkpoint，但 streaming repair 的“写入可见性边界”与“重试幂等边界”尚不清晰，存在部分应用后重放副作用风险。
  - 建议：引入 repair session staging + 原子 promote，或最少定义严格幂等键、去重窗口与 WAL 持久化顺序。

- **P2：性能估算偏乐观，可能触发锁竞争**
  - 视图构建若持有 `resourceListLock` 读锁执行重扫描/加载，可能抑制 compaction 写锁并放大尾延迟。
  - 建议：采用“短读锁抓快照 + 锁外重活”，并增加单分区最大处理时长与中断点。

## 测试缺口（建议作为准入门槛）

- 故障注入：`EXECUTE_REPAIR` 中点崩溃、网络抖动、leader 切换，验证无重复/无遗漏/无回退。
- 语义冲突：同 key 上插入/删除并发与乱序，验证 `ProgressIndex` 裁决一致。
- 正确性对照：随机数据集做全量扫描真值对比，量化 false-negative（尤其 XOR 聚合方案）。
- 大分歧退化：分区级缺失/整段缺失，验证快速跳过 IBF 直接全量修复。
- 幂等回放：同一 repair session 重放多次结果不变。

## 结论

方案方向正确、工程化程度高，但建议在上线前优先收敛三点：

1. 打通 IBF 解码到修复输入的可实现闭环；
2. 引入分区级 checkpoint，避免全局水位推进掩盖失败；
3. 将冲突裁决统一为 `ProgressIndex`，替代“Leader wins”口径。
