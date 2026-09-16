<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# IoTDB CLI SQL / filesystem 基准测试报告

## 1. 报告目的

本报告记录当前基准脚本的测试结果，并解释 SQL 模式与 filesystem（fs）模式出现耗时差异的原因。需要先明确测试边界：当前 filesystem 模式也通过 JDBC 执行 IoTDB SQL，脚本测量的是**非交互式启动 `iotdb-cli` 后执行一条命令的完整墙钟时间**，并没有测量大模型从接收自然语言请求到完成操作的总耗时。因此，结果可以说明两种 CLI 命令表面的相对开销，不能直接作为“大模型操作 IoTDB 的效率”结论。

## 2. 测试方法

脚本位于 [`benchmark.py`](./benchmark.py)。每个用例分别用 SQL 命令和 fs 命令访问同一数据库、同一张表；每个样本单独启动一个 CLI 进程。计时范围包括 CLI/JVM 启动、连接和认证、命令解析、服务端执行、结果序列化与终端输出，直到进程退出为止。

测试数据和运行参数来自本次运行的 [`metadata.json`](./results/20260912T144923227568Z/metadata.json)：

| 项目 | 配置 |
| --- | --- |
| IoTDB CLI | 2.0.11-SNAPSHOT，table dialect |
| 服务端 | `127.0.0.1:32867` |
| 数据库/表 | `cli_benchmark_codex.telemetry` |
| 数据规模 | 4 个设备，每个设备 1000 个时间点，共 4000 行 |
| 字段 | `device`、`temperature`、`humidity`、`status` |
| 预热/重复 | 1 次预热，正式执行 3 次 |
| 写入测试 | 未启用 |

覆盖的场景包括点查、时间范围查询、前 N 行、全量扫描、计数、聚合、表结构、表元数据和表列表。比较文件中的 `sql_over_filesystem_speedup` 定义为：

```text
SQL 平均耗时 / fs 平均耗时
```

数值大于 1 表示 fs 平均耗时更短，小于 1 表示 SQL 平均耗时更短。

这些 SQL/fs 命令是按场景设计的对应操作，但当前脚本只校验命令成功和耗时，
没有逐项比较两种模式的结果内容、行顺序和聚合值。因此，结果应理解为接口
调用开销基线；若要做严格的等价性性能比较，还应保存并校验两种模式的规范化
输出。

## 3. 测试结果

原始样本见 [`results.csv`](./results/20260912T144923227568Z/results.csv)，汇总见 [`summary.csv`](./results/20260912T144923227568Z/summary.csv) 和 [`comparison.csv`](./results/20260912T144923227568Z/comparison.csv)。

| 场景 | SQL 均值（ms） | fs 均值（ms） | SQL/fs | 较快模式 |
| --- | ---: | ---: | ---: | --- |
| aggregate | 481.890 | 515.158 | 0.935 | SQL |
| count | 465.240 | 549.276 | 0.847 | SQL |
| full_scan | 616.756 | 548.841 | 1.124 | fs |
| head | 532.634 | 515.856 | 1.033 | fs |
| list_tables | 482.066 | 464.794 | 1.037 | fs |
| metadata | 482.101 | 498.942 | 0.966 | SQL |
| point_lookup | 482.329 | 549.242 | 0.878 | SQL |
| range_scan | 582.145 | 514.871 | 1.131 | fs |
| schema | 515.142 | 515.393 | 1.000 | SQL |

在这次小规模运行中，fs 在全量扫描、前 N 行、范围查询和表列表场景较快；SQL 在点查、计数、聚合、元数据和表结构场景较快。差距并不大，且每个场景只有 3 个正式样本；例如范围查询的 SQL/fs 比为 1.131，表示 SQL 平均耗时约为 fs 的 1.131 倍，而不是 fs 固定快 13%。

由于每个场景只有 3 个正式样本，且每次样本都要启动新的 JVM，结果更适合作为初始基线。正式结论应增加重复次数和数据规模，并明确独立进程、缓存状态等条件，同时报告中位数、P95 或置信区间。

## 4. 为什么 SQL 和 fs 会有差异

filesystem 模式并不是绕过 SQL 的本地文件读取。当前 table provider 通过同一个
JDBC `IoTDBConnection` 执行 SQL；filesystem 命令只是把路径和 Unix 风格参数
翻译成 SQL，再在 CLI 进程中完成部分过滤、截断、统计和格式化。因此，两种模式
共享 IoTDB 的 SQL/存储后端，但命令翻译和客户端后处理不同：

1. **命令解析和执行器不同。** SQL 要经过 SQL 词法/语法解析、逻辑计划和优化；fs 命令要解析虚拟路径、子命令和参数，再映射到 table provider 的读取或元数据操作。
2. **过滤和统计可能在 CLI 中完成。** 例如 table provider 的 `cat --start/--end` 先执行 `SELECT * ... ORDER BY time`，再由 `FsRowReader` 在客户端按时间过滤；`stats` 和部分 `count` 逻辑先读取行，再由 `FsStatistics` 在客户端计算统计值。对应 SQL 用例则把条件或聚合直接交给 IoTDB。
3. **LIMIT 和投影路径不同。** 没有时间过滤时，fs 读取会把 `LIMIT` 转成 SQL 限制；带时间过滤时，为保证客户端过滤，可能先读取更多行。fs 还会先查询 `DESC` 获取列定义，再进行投影和 CSV 格式化。
4. **结果处理量不同。** 两条命令即使返回“相同业务结果”，输出格式、列数、表头、CSV 编码和客户端渲染工作也可能不同；全量扫描尤其容易被输出字节数影响。
5. **元数据路径不同。** `SHOW TABLES`、`DESC`、`meta`、`schema` 的组合查询和客户端整理逻辑不同，因此它们的耗时不能简单按数据扫描速度解释。
6. **启动和环境噪声占比很高。** 每个样本都重新启动 JVM 并重新认证。当前单次操作通常约 0.46–0.62 秒，这会掩盖真正的服务端执行差异；操作系统调度、JIT、连接建立和缓存状态也会造成波动。

本次用例中有几个具体的不对称点：`point_lookup` 的 SQL 带有 `WHERE time = ...`，而 fs 的 `cat --start/--end` 会先执行排序后的 `SELECT *`，再在 Java 代码中筛选时间；`count -f csv` 会先读取行并由 `FsStatistics` 计算列统计，而 SQL 只执行 `COUNT(*)`；`stats` 也会读取所有行后按设备和字段在客户端计算多项统计，而 SQL 用例只返回全表的四个聚合值。这些命令虽然业务意图相近，但并不是相同的数据库工作量。

## 5. 与“大模型直接操作耗时”的关系

当前结果**不包含大模型耗时**。大模型直接操作的端到端时间至少应包括：模型收到用户请求后的理解和规划、生成 SQL 或 fs 命令、工具调用往返、CLI/连接执行、错误重试、结果读取、结果解释以及最终回复生成。脚本只覆盖其中的 CLI/IoTDB 执行部分，而且每个命令都由固定脚本预先写好，绕过了模型决策和生成过程。由于 fs 最终也走 SQL，当前结果不能回答“模型选择 SQL 还是 fs 后哪种任务完成方式更快”，只能回答“这两个命令表面在当前实现下的额外开销有何不同”。

因此，不能用本报告的 SQL/fs 均值推断“哪个接口能让大模型更快完成任务”。例如 fs 命令在一次扫描中更快，但如果模型需要额外探索路径、读取 schema 或重试参数，整体任务仍可能更慢；反之，SQL 可能一次请求就表达完整过滤条件，减少工具调用轮次。

## 6. 建议的 LLM 对比方案

具体设计已整理到 [大模型 SQL/fs 对照设计](../llm-sql-fs-comparison/README.md)，
包含 12 个自然语言任务、工具限制、提示词、计时与正确性标准；该方案尚未执行。

若目标是比较大模型直接操作效率，应在同一模型、同一 IoTDB 实例和同一任务集合上运行两组代理，共用任务说明，仅切换接口能力说明：一组只允许生成 SQL 工具调用，另一组只允许生成 fs 工具调用。每个任务记录以下时间点和计数：

- `t0`：发送用户任务；
- 首个工具调用生成时间、每次工具调用开始/结束时间；
- 最终答案生成时间 `t_end`；
- 总墙钟时间 `t_end - t0`、模型 token 数、工具调用次数、重试次数和任务成功率。

任务应覆盖简单查询、多条件过滤、聚合、schema 探索、需要先发现表再查询的组合任务，并使用固定的正确性校验。报告应同时给出端到端时间和工具执行时间，这样才能区分模型规划开销与 IoTDB 接口开销。对于同一个任务，SQL 和 fs 两组必须使用等价的数据范围和输出要求；不能只比较两条手写命令的运行时间。

## 7. 结论

本次基准在当前实现和小规模数据下观察到 SQL 与 fs 的 CLI 端到端耗时随场景变化，但差异受到 JVM 启动、输出和样本量影响，尚未确认统计显著性。它适合作为 CLI 接口层的初始基线。要回答“大模型直接操作哪种方式更高效”，还需要按第 6 节记录模型请求、工具调用和最终任务完成时间，并以任务成功率和结果正确性作为共同指标。
