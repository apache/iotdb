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

# SQL / filesystem 模型交互链路分析

分析日期：2026-09-16。依据已有原始调用轨迹、公开任务规则、评分器和 CLI 源码进行复核。本次未调用模型重新测试，未修改历史结果和评分代码。

对于当前异常排查、冷链区间判断、楼宇基线比较任务，建议以 SQL 或结构化查询工具承担计算，保留 filesystem 作为浏览和发现入口。实测主要耗时发生在模型与工具的交互过程；接口设计应优先减少不必要的模型往返，并让返回数据直接对应业务问题。

## 1. 比较对象与已有结论修正

两组都使用 `iotdb-cli`，区别是 SQL 访问模式和 filesystem 访问模式。filesystem 也通过 SQL 后端访问 IoTDB。

上轮报告中 A1 filesystem 的 `wrong_answer` 是评分器误判。公开任务将 `peak_temperature_c` 定义为 `number`，模型返回 `92.0` / `90.0`，oracle 使用 Python 整数 `92` / `90`。评分器根据 oracle 的 Python 类型要求答案必须为整数，与公开字段契约不一致。

本次按公开 answer_shape 检查字段类型、字段集合、数组顺序和完整答案：只将声明为 `number` 的合法有限数值按数值比较，时间和计数仍按整数规则检查，布尔值不接受为数字。A1 filesystem 全部字段通过。A1 SQL 的缺测数返回 3，oracle 为 1，仍是业务错误。复核沿用原始答案与计时，不产生新的模型样本。

| 09-15 应用重跑 | SQL | filesystem |
| --- | --- | --- |
| A1 冷却泵 | 83.96 秒；缺测数错误 | 41.11 秒；答案符合公开契约，原评分误判 |
| A2 冷链 | 18.35 秒后上游限流，未完成 | 93.02 秒；答案正确 |
| A3 楼宇 | 53.93 秒；答案正确 | 180 秒超时；只完成表发现与结构读取 |

本轮所有 `stream_retry_count` 均为 0。A2 SQL 是限流错误，A3 fs 是任务总时限耗尽；响应流重试未触发。该轮没有双方都正确完成的配对，不能据此给出成功完成相同任务的平均速度排名。

证据：[公开字段契约](application_tasks.json)、[评分器](fixture.py)、[原始记录](results/application-rerun-with-retry-sol-20260915/trials.jsonl)。

## 2. 慢发生在哪一段

记录可直接拆为：

```text
任务总耗时 = 工具耗时 + 非工具耗时
工具耗时 = 命令校验 + CLI 启动/连接/查询/处理 + 结果包装
非工具耗时 = 模型处理 + 网络 + 服务排队 + Codex 编排等
```

`model_api_ms` 没有独立观测值。因此，非工具耗时不能直接命名为模型思考时间。

### A2：已有的成功配对说明差距来自哪里

使用 09-14 初轮中两侧答案均正确的冷链任务：

| 指标 | SQL | filesystem |
| --- | ---: | ---: |
| 任务总耗时 | 27.804 秒 | 33.651 秒 |
| 工具合计耗时 | 1.553 秒 | 1.548 秒 |
| 非工具耗时 | 26.251 秒 | 32.103 秒 |
| 工具调用次数 | 3 | 3 |
| 完成的模型响应数 | 4 | 4 |
| 最后一次工具返回至最终答案 | 9.574 秒 | 15.046 秒 |
| 全部工具 stdout 字节数 | 2,684 | 1,976 |

fs 总耗时多 5.847 秒，但工具执行少约 4 毫秒。约 5.472 秒差值出现在最后一次数据返回之后，约占总差值的 94%。这组样本中，两侧调用次数相同，fs 输出还更小。

两种模式的实际路径都是“列表 → schema → 读取 → 最终答案”。一个具体差别是输出顺序：SQL 使用 `ORDER BY box_id, time, upload_id`，同箱观测相邻；fs 按 time 与 TAG 排序，不同箱子的观测交错。任务要求按箱合并异常区间，SQL 的结果顺序直接贴合这项操作，fs 需要按箱重新整理。这是可验证的接口改进方向；日志没有进一步区分最后阶段的模型计算、服务等待和网络时间，不能将全部 5.472 秒归因于重排。

证据：[09-14 逐条记录](results/application-smoke-sol-20260914e/trials.csv)、[SQL 第三次工具输出](results/application-smoke-sol-20260914e/trials/APP_A2-COMPLIANCE_01_sql/tools/03/stdout.txt)、[fs 第三次工具输出](results/application-smoke-sol-20260914e/trials/APP_A2-COMPLIANCE_01_filesystem/tools/03/stdout.txt)。

### A3：超时发生在数据发现阶段

09-15 filesystem 轨迹如下，时间相对任务开始：

| 时间 | 动作 | 工具本身耗时 |
| --- | --- | ---: |
| 21.15 秒 | 列出表 | 约 0.57 秒 |
| 65.21 秒 | 读取 floor_assets 结构 | 约 0.52 秒 |
| 127.07 秒 | 读取 floor_baselines 结构 | 约 0.52 秒 |
| 174.47 秒 | 读取 meter_readings 结构 | 约 0.47 秒 |
| 180.00 秒 | 任务超时 | — |

四次工具全部执行成功，只返回了 892 字节元数据，工具总耗时 2.068 秒；非工具耗时为 177.932 秒，占 98.85%。这轮还没有读取业务观测，更没有进入基线对齐或电能重建。

对应 SQL 跳过了 floor_assets 的结构检查，执行两次必要的 DESC 和两次 SELECT，约 34.09 秒完成数据读取，53.93 秒交付正确答案，工具合计 2.536 秒。fs 多检查一张表是探索策略差异；数十秒的调用间隔才是这轮超时的主要组成部分。

证据：[fs 时间线](results/application-rerun-with-retry-sol-20260915/trials/APP_A3-ENERGY_01_filesystem/events.jsonl)、[SQL 时间线](results/application-rerun-with-retry-sol-20260915/trials/APP_A3-ENERGY_01_sql/events.jsonl)。

### 基础任务也表现为随任务变化

最初 240 条任务批次中，在两侧均成功的配对上，K04 最近记录和 K07 布尔状态计数倾向于 fs 更快，K06 分设备聚合与 K08 全局湿度聚合倾向于 SQL 更快。任务等权的 `T_sql/T_fs` 几何比为 0.957，报告的 95% bootstrap 区间为 0.860–1.066，包含 1。

这与“匹配特定操作的接口更有效”一致，未显示稳定的全任务速度优势。此处引用原始批次，后续按失败记录替换的整合视图与其分开。证据：[原始批次报告](results/pilot-sol-20260913-v2/REPORT.md)。

## 3. 同样走 SQL，执行工作量为什么不同

SQL 模式将模型指定的筛选、投影、聚合和排序交给数据库。当前 fs 实现则包含额外的路径解析、对象存在性检查和客户端处理：

| 当前 fs 路径 | 实现行为 | 对效率的影响 |
| --- | --- | --- |
| 带时间或 TAG 过滤的读取 | 先按全列读取，再在 Java 中过滤；过滤存在时取消底层读取 limit | 数据变大时可能扫描和传输远多于最终需要的行 |
| 字段选择 `-m` | 底层仍读取全列，在返回阶段做投影 | 返回给模型的列少，不等于数据库读取的列少 |
| `stats` / CLI `count` | 读表后在 Java 中计算统计 | 相比数据库聚合，增加数据传输和客户端处理 |
| 普通结构化读取 | 取 schema 和读表路径分别检查对象、读取列信息 | 常见路径包含重复 SHOW TABLES 和 DESC |
| 每个工具调用 | SQL/fs 均重新启动 CLI 并建立连接 | 多一次模型调用同时增加启动与交互开销 |

源码位置：

- [FsRowReader.java](../../iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/FsRowReader.java)：read 中的 schema 获取、filtered、requested 和客户端过滤。
- [TableFilesystemSchemaProvider.java](../../iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/TableFilesystemSchemaProvider.java)：stats、countRows、read 中的全表读取与 SQL 生成。
- [FilesystemShell.java](../../iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/FilesystemShell.java)：CLI count 调用 countRows；不能用 provider 中另一种 count 方法的 COUNT(*) 推断 CLI count 已做聚合下推。
- [tool_adapter.py](tool_adapter.py)：每次工具调用独立启动 CLI。

这些是实现上的工作量差别。A1–A3 的小快照下，工具只用了约 1.5–2.5 秒；几十秒到超时级别的任务差距主要出现在工具之外。优化下推可改善数据规模增长后的表现，同时仍需优化模型交互。

## 4. 什么设计更利于模型理解

当前 fs 借用了熟悉的命令名，但模型仍需学习一套数据库专有规则：

- `cat/head` 的 `-f` 表示格式，`tail -f` 表示持续跟随，格式要用 `--format`。
- TAG 可以过滤，数值 FIELD 不能用相同机制过滤；字段选择又只选 FIELD，TIME/TAG 保留。
- `stats` 按全部 TAG 组合分组，而任务常要求按 pump_id、box_id 等业务实体分组。A1/A2 的 upload_id 也是 TAG，全部 TAG 组合不等于业务设备粒度。
- `stats` 没有 AVG，也不接受读取命令的时间/TAG 过滤选项；模型需要合并统计并计算平均值。
- `count` 返回多列统计，每一行重复 row_count；模型必须理解它不是可相加的多份计数。
- NDJSON 中 INT64/TIMESTAMP 是字符串，最终业务 JSON 要求毫秒整数，需另做类型转换。

SQL 用 WHERE、GROUP BY、ORDER BY、AVG 等直接表达这些数据操作。对于按实体筛选、汇总、排序和跨表分析的问题，其表达能力更贴近任务目标。但本轮 A3 SQL 实际仍读取了两张表供模型整理，没有执行 JOIN；一次查询完成更多计算是可采用的能力，不能写成已发生的实测路径。

另一方面，fs 的 ls/tree、schema、head/tail 很适合“有什么数据”“看几条样本”“取最新观测”。简单操作可用短指令完成，模型无需构造复杂 SQL。

本实验只开放 fs 的一组只读命令，禁用管道、通用 shell、本地 Python 和 fs 中的 SQL 入口；它并非完整的 Unix 数据处理环境。两侧被测模型都禁用 skill 加载，接收的是接口提示词，因此当前数据也没有比较 skill 对理解的帮助。

证据：[filesystem 接口说明](prompts/filesystem.md)、[SQL 接口说明](prompts/sql.md)、[共同约束](prompts/common.md)、[客户端配置](codex_client.py)。

## 5. 建议的产品方向和优先级

| 需求 | 建议入口 | 原因 |
| --- | --- | --- |
| 找库、找表、预览、最近记录 | 保留 fs 或结构化 discover/preview | 路径和对象浏览直观，操作集合小 |
| 条件过滤、分组、排序、基线对齐 | SQL 或类型明确的 query/aggregate 工具 | 数据库执行计算，返回贴合任务的结果 |
| 面向模型的统一产品接口 | fs 导航 + 结构化分析参数 + SQL 后端 | 同时保留发现能力和组合查询能力 |

推荐按以下顺序推进；这是设计建议，未作为新接口完成测试：

1. **先校准评估与服务状态。** 评分按公开 schema 判定 number/integer；将业务错误、限流、断流、超时分开。以正确完成任务的耗时判断接口价值。
2. **减少发现阶段往返。** 支持一次返回相关表的精简 schema、列类型和单位；明确对象与字段意义，让模型避免逐表试探。批量发现的收益包括少一次完整模型往返。
3. **让工具执行确定性计算。** 显式提供 filter、group_by、aggregate、order_by、limit，按同一范围语义处理查询与统计；区间合并、去重等提供通用、明确的计算规则。支持 SQL 承担复杂组合，避免把整表计算留给模型。
4. **统一输出和命令语义。** 统一格式选项，返回列定义、数据、行数、截断状态；单位、时区和空值规则稳定一致。按业务实体与时间排序可减少模型重新整理数据。
5. **优化后端执行。** 时间/TAG/FIELD 过滤、投影和聚合下推 SQL；复用元数据与连接；大结果分页或流式处理。与交互轮数一起衡量，避免只优化毫秒级解析。

若近期必须在现有两种模式中选一种用于 A1–A3 这类复杂分析，优先 SQL。若继续建设面向模型的产品，推荐保留 fs 的浏览入口，并加上通用结构化查询能力。此方向基于能力与任务匹配，不预设新接口一定更快。

## 6. 下一轮如何验证设计判断

分别回答两个问题：现有完整工作流哪个更有效，以及仅改变接口表达会产生什么差别。

- 完整工作流对比：相同快照、任务、模型、预算和重试规则；交错随机执行 SQL/fs，保留每次尝试，分别报告正确率、成功配对耗时、超时与服务错误。
- 接口表达对比：让 SQL 与结构化接口支持相同操作，并使用相同底层 SQL 和规范化输出，固定字段与顺序。这样再比较命令构造正确率、调用次数和端到端耗时，才更接近模型理解差异。
- 输出顺序对比：专门比较按 time 排序与按实体/time 排序的 A2 数据，其他条件保持一致，验证整理负担的假设。
- 实现对比：保留同一 fs 命令，比较 SQL 下推与客户端处理，在不同数据规模下记录数据库查询数、读取行数/字节数和工具耗时。

现有结论可落实为：减少模型往返、让数据操作直接表达业务意图、把确定性计算交给执行器。这比单纯把 SQL 换成看起来像文件操作的语法更值得优先验证。
