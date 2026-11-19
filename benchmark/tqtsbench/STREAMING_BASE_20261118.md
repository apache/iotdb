<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# FS 流式基座实现与验证：2026-11-18

本次实现针对宽预算 TQTS 对照中暴露的全量物化、过滤分页失效和重复窗口扫描。改动限于 CLI、受控实验适配器及测试；聚合仍在 CLI 计算，没有把聚合切换为 IoTDB SQL 聚合，也没有修改数据库内核。

## 实现

- `SqlExecutor.scan` / `JdbcSqlExecutor.scan` 提供按行回调及早停。JDBC fetch size 为 1,024；正常结束、早停及异常时关闭 ResultSet 和 Statement。Tree、Table 和虚拟目录均接入该通路；Tree 列名只在每个查询中解析一次，避免逐单元格反复调用路径解析器；旧自定义 provider 的默认方法保留兼容，默认方法本身不保证流式。
- Tree/Table 原始读取使用受控整数时间谓词，将时间范围限制在后端读取阶段；TAG 条件仍在客户端逐行过滤，offset 逐行跳过，达到 limit 即关闭游标。CSV/NDJSON 顺序输出；table 为保持全局列宽使用私有磁盘暂存，不再缓存全部数据行。tail 使用反向扫描，选中页再恢复正序。
- `stats` / `count` 按实体维护计数、时间、首末值、极值和累计值，不保留全部输入行。只累积选定的 FIELD；计数保留原来的列类别规则。Tree 仍扫描设备 union timeline，保证未选 measurement 上的时间戳不会消失，NULL 计数不变。
- 精确 median 不改为近似：仅请求 median（或省略 aggregates 请求全部统计）时启用，每个数值状态最多缓冲 4,096 个值，超过后形成排序段，以最多 32 路分层归并定位中位数。临时数据不向 Agent 暴露，正常/异常退出清理；受控适配器为每次调用建立私有临时目录，外层超时终止后也清理。
- `stats --windows start:end,...` 在每个设备/表的一次原始数据扫描中计算 1..256 个窗口，允许重叠、重复、空窗口和负 epoch，按输入顺序输出。窗口内存随实体、字段及窗口数增长，磁盘用量随需计算 median 的样本量增长；不声称所有工作集都为常量空间。

## 命令契约

```text
stats /root/db/device -m temperature \
  --windows 0:999,1000:1999,500:1499 \
  --aggregates count,min,max,avg,first,last,min_time,max_time,null_count -f ndjson
```

所有窗口都是**包含两个端点的 epoch 区间**，与现有 `--start/--end` 一致；`--windows` 与两者互斥。连续窗口应写为 `[a,b-1]` 和 `[b,c-1]`，不是重复包含 b。输出新增 `window_index`（从零开始）、`window_start`、`window_end`，同时保留 object、field、TAG 身份。空 Tree 窗口输出零计数和 NULL 统计；空 Table 窗口没有实体行。

聚合白名单新增 `first,last,min_time,max_time,null_count`，原有 `count,min,max,sum,avg,median` 保留。`min_time/max_time` 通常是首末非空时间；Table 全 NULL 字段保留既有的实体时间线回退规则。两者均不能替代 argmin/argmax。输入范围超过 int64、倒置窗口、超过 256 个窗口、非法 aggregate 均拒绝。

受控 `iotdb_fs` 对应参数：

```json
{"command":"stats","path":"/root/iotdb_pvdaq/system_1199/metric_2745","measurements":["value"],"windows":[{"start_ms":1559347200000,"end_ms":1560556799999},{"start_ms":1560556800000,"end_ms":1561766399999}],"aggregates":["count","first","last"]}
```

此版本只在一次命令内部复用扫描，不提供跨调用缓存、会话快照或读 epoch。多个窗口共享同一数据游标；Tree 跨设备仍分别查询，不能声称跨设备事务快照。offset 翻页也不保证并发写入下的稳定性。

## 验证

- 331 项 FS/JDBC 单元与兼容性回归通过，覆盖百万行无 median 聚合、20 万条大整数精确 median（跨归并层）、重叠/重复/空/极端时间窗口、Table TAG 身份及全 NULL 时间线、过滤后分页早停、反向页、CSV/NDJSON/table 编码等价、JDBC 异常资源关闭。
- 10 项 Python 受控适配器测试通过；JavaScript/Python 语法检查通过。
- 英文、中文完整 reactor `test-compile` 均通过。首次普通命令被 `.tmp` 中既有的飞书 XML 片段触发格式器错误；复核使用 `-Dxml-format.skip=true`，仅跳过 XML 格式化，未跳过 Java 编译或 Spotless 检查。

### 实验服务器实测

在 `zzy@101.6.42.165` 原有 TQTS 数据上完成 22 次只读命令检查：512 MiB Java heap、128 MiB direct memory、280 秒查询时限、300 秒进程时限。标量聚合与独立 SQL 比对，多窗口与独立扫描比对；NDBC 中位数与 Python 排序中位数比对，Gas 全量中位数检查独立 SQL 秩条件，偶数中位数中点由单元测试补充验证。Tree 三个真实数据包通过；Table 本轮覆盖来自单元测试，未声称完成真实 Table 数据全量验证。

| 检查 | wall time（秒） | 峰值 RSS（MiB） |
|---|---:|---:|
| Gas 全量标量统计（4,208,261 行） | 16.234 | 193.2 |
| Gas 全量精确中位数 | 22.246 | 281.5 |
| Gas 窄区间单行读取 | 0.515 | 120.4 |
| Gas 四窗口（含重复、空窗口） | 0.615 | 168.7 |
| PVDAQ 八窗口 | 0.616 | 172.4 |
| NDBC 单日精确中位数 | 0.464 | 123.8 |

每项完成后私有临时文件剩余数均为 0。PVDAQ 八次独立命令合计 4.174 秒，批处理为 0.616 秒；该对比包含 JVM 启动及连接开销，未隔离缓存影响，不是等预算 Agent 性能实验。

保留的迭代证据：attempt-1 全量 Gas 在服务端默认 60 秒查询时限处终止，未发生 OOM；attempt-2 放宽查询时限后 22 项通过，但标量/中位数分别耗时 270.941/277.331 秒。线程采样显示逐单元格列路径解析占用 CPU，故最终版本缓存每个查询的列标签解析；同时将多窗口包络改为合并后的时间区间并集，Gas 四窗口从该中间版本的 33.410 秒降至 0.615 秒。两项改动同时进入 attempt-3，不能从该次对比单独归因各自收益。

最终 CLI 已部署到 `/data/zzy/iotdb-fs-exp/runtime/iotdb/lib/iotdb-cli-2.0.11-SNAPSHOT.jar`。原 CLI 保存为 `experiments/fs-streaming-base-20261118/iotdb-cli.pre-streaming.jar`；最终 jar SHA-256：`319facda8f059a7d1049fb2b422bbfb5a6639e63257d82b99473aadc4ae0d3fd`。新适配器位于 `experiments/fs-streaming-base-20261118/scripts/`；部署后通过 2 项无模型端到端检查（typed windows、分页、临时目录清理）。旧实验脚本和 traces 未覆盖，复现旧实验必须使用旧 CLI 与其 frozen-source 配对。

完整证据位于服务器 `/data/zzy/iotdb-fs-exp/results/fs-streaming-base-20261118/`，包含三个 attempt 的配置、命令、输出、资源记录及 adapter-smoke；本地镜像为 `.tmp/fs-streaming-base-20261118/`。源文件哈希和源码快照位于服务器 `experiments/fs-streaming-base-20261118/`。本地报告和代码尚未提交 Git。


## 当前边界

这次补齐读取/聚合的执行基座，不等于完成全部 SQL 表达能力或规划中的真实 TsFile 物化。session、argmin/argmax 关联行、跨实体排名、统一快照仍需单独定义和验证。通用文本管道、join/paste、export 等其他调用路径并非全部在本次改造范围内；不能把结构化读取和统计的内存界限外推到所有命令。

受控分页的输出行数有界，但高 offset、TAG 过滤、宽时间范围仍可能扫描大量数据；原始查询会合并重叠/相邻区间并读取区间并集，避免读取窗口之间的空隙。为保持 Tree union timeline，不直接丢弃未选择的序列，因此传输量仍可能较大。精确 median 和 table 对齐输出需要临时磁盘，不能解释成无磁盘成本。

未运行下一轮 LLM benchmark；本轮真实数据回归只验证工程正确性、资源占用和窗口扫描复用，不能据此推导 Agent 准确率或接口优势。
