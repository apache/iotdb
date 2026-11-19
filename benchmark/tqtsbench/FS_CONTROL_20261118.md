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

# TQTS-Bench FS 对照与链路瓶颈：2026-11-18

本轮目的：在与 SQL pilot 相同的八题上充分放宽 FS 资源，检查两侧瓶颈是否重合。实验使用当前 Tree FS 实现，不代表规划中的虚拟 TsFile 目录或后续混合接口。模型运行期间没有修改 FS 实现或模型侧适配器。

## 协议与比较边界

两组均使用 `deepseek-official / deepseek-flash`、同一 IoTDB 2.0.11-SNAPSHOT 实例、相同的八题和公共上下文。初始输入只给题目、包根路径等基本信息；schema 与文档需自行发现。FS 只暴露 `dataset_context` 和 `iotdb_fs`，SQL 只暴露 `dataset_context` 和 `iotdb_sql`。不允许读取源码、本地文件、任意 shell 或外部计算工具。

| 条件 | SQL pilot | FS control |
|---|---:|---:|
| 每题 wall time 上限 | 240 s | 900 s |
| 准入工具调用 | 30 | 120 |
| 单次模型生成 token 上限 | 8,192 | 32,768 |
| 单次数据库/FS 操作上限 | 45 s | 180 s |
| 工具返回字符上限 | 24,000 | 40,000 |
| FS Java 堆 / direct memory | 不适用 | 8 GiB / 512 MiB |

FS 题目和原生命令均串行执行。服务器总内存 32 GiB；8 GiB 是单个 CLI 的堆限制，不是服务器可用内存。单页最多 2,000 行，额外读取一行判定 `next_offset`。生成上限不等于整题累计 token 上限。FS 具备完整受控参数 help、NDJSON 分页；SQL pilot 未加载额外 SQL skills。这些均是已知不对称条件，所以本轮只用于诊断，不能归因性地声称某种接口更快或更准。

适配器支持 18 种原生命令：`help, ls, find, tree, schema, meta, stats, count, cat, head, tail, stat, file, grep, cut, paste, join, wc`。每次只执行一个受控命令，没有任意管道、写文件或 SQL 逃逸；没有新增 session、window、ranking 算子。其能力范围也限制了本轮结论。

首次 smoke 发现 DSH 存储配置覆盖插件的 maxTokens，实际仍为 8,192。使用实验专用 DSH 配置后再次 smoke 确认 32,768，随后才执行八题；两个 smoke 均排除在指标外。共享凭据和配置未修改。

## 逐题结果

| 题目 | SQL / FS wall (s) | SQL / FS token | SQL / FS 尝试次数 | FS 文档 / FS 命令 | 结果复核 |
|---|---:|---:|---:|---:|---|
| C-MAPSS 013 | 66.7 / 43.6 | 174,234 / 73,218 | 31 / 23 | 3 / 20 | 主答案一致；SQL 未完成 |
| C-MAPSS 030 | 98.1 / 63.2 | 263,402 / 257,264 | 32 / 18 | 6 / 12 | 两组主答案一致 |
| Gas 001 | 26.5 / 514.6 | 33,370 / 531,815 | 11 / 31 | 5 / 26 | 两组主答案一致；FS 5 次 OOM 后恢复 |
| Gas 021 | 77.5 / 900.1 | 214,354 / 179,439 | 31 / 40 | 5 / 35 | FS 超时；SQL 丢失实验粒度 |
| NDBC 005 | 33.0 / 282.8 | 42,918 / 176,581 | 16 / 56 | 8 / 36 | 两组主答案一致 |
| NDBC 028 | 137.5 / 138.2 | 271,356 / 109,036 | 25 / 21 | 10 / 11 | 两组均有 gap 边界偏差 |
| PVDAQ 006 | 149.5 / 900.1 | 269,109 / 974,373 | 32 / 94 | 14 / 80 | FS 超时；SQL 生成预算耗尽 |
| PVDAQ 040 | 91.5 / 806.3 | 373,131 / 509,317 | 32 / 84 | 4 / 80 | FS 主答案一致；SQL 未完成跨流选择 |

八题均完成一次尝试，FS 六题正常结束、两题达到 900 秒上限；没有达到 120 次准入调用上限或 32,768 单次生成上限。逐项复核主答案：**FS 5/8 与原始参考一致，SQL 3/8**。FS 另有一题语义不一致、两题无最终答案。这是人工主答案复核，不是官方分数；资源和帮助不对称，不能据此声称 FS 准确率优势。没有将超时任务的未交付思考当成答案。

- FS wall time 合计 **3,648.8 秒（60.8 分钟）**，中位数 **398.7 秒**；SQL 为 680.4 秒和 84.5 秒。正常完成的 FS 任务中位数 210.5 秒；将超时题剔除会产生选择偏差，故主表保留全部任务。
- FS 累计 token **2,811,043**，其中非缓存输入 **260,299**、缓存输入 **2,420,736**、输出 **130,008**，reasoning **78,261** 已包含在输出中。输入加权缓存命中比例 **90.29%**。总 token 中位数 **218,351.5**；SQL 总量 1,641,874，中位数 238,878。长尾使总量与中位数的方向不同，不能只选一个指标。
- FS 工具尝试 **367** 次，进入 runner **355** 次：公共上下文 **55**、FS **300**。另 **12** 次因缺少 command 被 DSH 拒绝。FS 目录/结构/元数据类调用 **30/300（10%）**；其余主要是读取和计算，而非目录 DFS。
- 已记录 **26** 次堆 OOM、**9** 次适配器参数错误、**1** 次原生命令 180 秒截止；另 **2** 次调用在整题截止时无完整 result。汇总中的 38 个 runner 错误/未完成记录包含这些项，不能再重复加总。输出截断 **4** 次。传输层 retry **0**，模型请求 **110** 次。
- 模型 API 区间合计 **700.0 秒**，工具区间 **2,899.2 秒**；SQL 分别为 554.3 秒、115.0 秒。FS 已匹配工具区间约为任务 wall 合计的 **79.5%**，SQL 模型区间约为 **81.5%**。这是记录区间的描述，包含调用开销，并非数据库内部算子耗时或精确关键路径分解；两个截止任务还有未结束工具区间。
- GNU time 记录的 CLI 峰值 RSS 达约 **8.30 GiB**，高于堆上限是因为进程还有堆外内存。OOM 发生在配置的 8 GiB Java 堆下；不声称更多物理内存完全无效，而是当前算法的无界物化需要先处理。

PVDAQ 040 是本轮值得保留的正例：FS 完成七个流 × 八个完整窗口的 56 次统计，选出与 gold 相同的 `metric_2745`、2019-06-15 起始窗口、27.0 → 47.566667 °C。总共 **71 次 stats**（包括其他范围和 12 次移窗检查），其中一次较宽父目录统计达到 180 秒限制，整题耗时 806.3 秒。这说明现有命令可以通过组合得到该答案，也说明组合的重复扫描成本很高；额外检查少数移窗不能证明全局滑动窗口最优。

最终回答的命令文本和 evidence ID 有转写、虚构标签或非严格 JSON 的问题；本轮评的是主答案，与可机器重放的输出契约分开。原始 request/response 才是执行事实。

## 两侧瓶颈是否相同

**部分重合，但当前 FS 额外受到明显的读取与计算能力限制。** 目录结构探索不是主要调用来源；高调用任务集中在已经定位到目标数据之后。

| 瓶颈 | trace 证据 | 解释边界 |
|---|---|---|
| 同一错误假设下无进展试错 | SQL C-MAPSS 013 以 measurement 作为 FROM 路径，19 次空结果；FS Gas 021 反复缩小窗口/字段仍触发全量扫描和堆溢出 | 共同的是恢复失败；具体原因分别是寻址和物化范围，不能只统计完全相同的命令文本 |
| 时间边界与结果粒度 | NDBC 028 两组均把 gap 限制为两端在同一小时内；参考以 gap 样本时间归属输出 HH:00、HH:50 | 两组都返回 10 分钟，不代表选点等价。应审计题面与参考边界，不能为某组改写评分 |
| 已有数据后的手工计算 | C-MAPSS 030 两组均从端点计算变化率；FS NDBC 028 仍长篇展开小时结果 | FS 命令没有自动消除后续变换和答案构造成本 |
| 当前 FS 的全量物化 | Gas 001 五次、Gas 021 二十一次堆溢出；缩窄时间窗口也没有缩窄后端读取 | 是当前实现可定位的执行瓶颈，不能外推为 FS 接口必然劣势 |
| 极值缺少关联行选择 | Gas 001 使用 13 次 grep 恢复；NDBC 005 用 31 次 stats 逐层缩小年月周日后读取目标行 | `min_time/max_time` 是首末非空时间，不能替代 argmin/argmax；数值条件用 regex 搜索不适合一般数值推理 |
| 会话/窗口缺少直接计算 | PVDAQ 006 共 94 次调用，其中 56 次 stats；窗口变化后每次仍约 10 秒，900 秒截止 | SQL 已取得 session 均值后在交付阶段耗尽生成预算；FS 尚在寻找间隔，失败阶段不同 |
| 参数契约与批量错误 | NDBC 005 一轮中 12 个 stats 请求都缺少必填 command，在适配器前被拒绝 | 是本轮通用参数工具的交互问题，不是数据库执行错误；也不是 12 次收到反馈后仍重犯 |

源码与部署版本哈希一致。`TreeFilesystemSchemaProvider.collectStatistics` 先执行 `SELECT * FROM device ORDER BY time ASC` 并物化整个设备，再应用过滤；measurement 叶子上的 stats 也走这个流程。`FsRowReader.read` 在有时间过滤时把请求行数设为无限，再在内存中筛选和投影。因此，**输出分页不等于扫描分页**，`limit=5` 或很窄的 epoch 范围仍可能全量读取。

对应源码：`iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/provider/TreeFilesystemSchemaProvider.java` 与 `iotdb-client/cli/src/main/java/org/apache/iotdb/cli/fs/FsRowReader.java`。前者 SHA-256 为 `d8b1c275d4acf1c565398056025b045cc22d6e6dfceb70a58d2789bae2e9e4a4`，后者为 `e57487299140cec7127644a04ddb564cb0329ff61b2a06a53ff805b9cdf0ae65`。

Gas 021 的 21 次 OOM 并非全是逐次收到反馈后的重试：其中同一模型步骤预先发出了 10 个窗口统计。应区分批量计划失败与反馈后恢复失败。PVDAQ 006 的 56 个 stats 也包含相同月份先 count、后完整 stats 的重复读取。可合并的窗口扫描和中间结果复用是可识别的优化候选。

## 下一轮优先级

1. **先消除执行层的无界物化。** 保持结果语义，改用流式聚合、有界读取与尽早安全过滤/投影；暴露有效扫描范围、是否重复全量扫描、OOM 后可行恢复方式。设备 union timeline 和 NULL 计数需要单独等价验证，不能直接删掉 `SELECT *` 就假设语义不变。本轮不要求引入混合 SQL 下推，也没有实施该优化。
2. **补齐可组合的数值运算。** 优先验证极值关联行选择、一次扫描的多窗口统计、gap/session，以及带实体键的端点差分和排名。适配器参数按命令区分，避免 stat/stats/tail 参数混淆；自动附证据引用，减少模型重建 provenance 的成本。
3. **把 trace 现象变成演化目标。** 按有效后端扫描签名识别重复扫描；区分批量失败/反馈后重试；标记可融合窗口、数据已就绪后的推理与答案展开、粒度/边界丢失。每项优化分别检查答案等价性、扫描行数/字节、峰值内存、调用数和模型 token。
4. **再做资源匹配的对照。** 当前八题作为验证集，固定同等语义资料、输出约定与预算，比较 SQL、FS 和候选混合接口。增加独立任务族与重复运行后才能评价泛化优势；本轮结果不能当作正式准确率或性能排名。

## 证据与限制

服务器：`/data/zzy/iotdb-fs-exp/results/tqts-fs-control-20261118/`。
本地：`/Users/alan671/Documents/iotdb/.tmp/tqts-fs-control-20261118/`。

- `protocol.json`、`selection.json`、`public-context/`：冻结协议、同题选择与公共上下文。
- `trials/<id>/dsh-session/**/session.v3.jsonl`：完整会话、usage、工具事件及终止信息。
- `trials/<id>/tool-output/`：原始输入/输出、受控命令及 GNU time 的 CPU/峰值 RSS；截止时尚未结束的调用可能没有 result，保留原始部分输出。
- `summary.json`、`answer-review.json`：逐题汇总与主答案复核。复用 SQL pilot 的原始 gold 运行结果，FS 运行期间未并发执行离线参考查询。
- `resource-monitor.jsonl`：五秒采样的内存和实验进程资源，从 Gas 001 阶段开始；前两题仍有逐调用 GNU time 数据。只记录资源值，不保存进程命令行。
- `frozen-source/` 与 `evidence.tar.gz`：实际执行代码与不含凭据的归档。`private-dsh-home` 整个目录必须排除，不能打包整个结果根目录。

这是固定八题、每题一次的探索性诊断。答案由本次执行者根据 gold 输出复核，未经独立盲审；主答案一致不等于所有解释或命令转写都准确。被取消的生成可能没有完整 usage，超时任务的已匹配工具区间也不含未结束调用，因此这些成本记录可能低估。API 传输重试与语义恢复尝试单独统计。
