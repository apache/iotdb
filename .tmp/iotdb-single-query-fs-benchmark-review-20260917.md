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

# 单问题时序查询：SQL 与 FS 分解实验候选复核

检索与文件核验日期：2026-09-17。本文收窄并替代上一版方案中的长程 Agent 工作流主线。
范围是同一个自然语言问题、相同数据和答案：生成一条复杂 SQL，与生成可组合的 FS 操作序列比较。
本次只完成题源和实现静态核验；未导入数据、执行参考查询或运行模型实验。

## 核验结果与推荐顺序

| 题源 | 真正的时序内容 | 已核验的候选 | 使用建议 |
|---|---|---|---|
| NLQTSBench / Sonar-TS | 河流、ETTm1、服务器监控序列 | 当前 tasks.json 共 1,153 题；Level 1 共 274 题 | 主候选：独立发布的时序自然语言问题与答案；SQL 需另行实现和验证 |
| Spider 2.0 Lite | NOAA 气象观测，以及 GA4/GA360 事件序列 | bq031 移动平均与滞后；bq011 时间窗口用户差集 | 主候选：现成问题与复杂参考 SQL；先处理方言和数据导出 |
| EHRSQL 2024 | 不规则生命体征、化验及临床事件 | 测试集机械筛得 175 道有时间标注且访问 chartevents/labevents 的题目，25 个原始问题模板 | 外部领域验证；有 NL、SQL、答案文件和公开 demo 数据 |
| TSBS IoT | 卡车速度、状态、载荷、油耗序列 | driving session、daily driving duration、breakdown frequency 等官方 SQL 模板 | IoT 主场景和规模实验；需补 NL，不能声称原生 Text-to-SQL 数据集 |
| TSM-Bench | D-LONG / D-MULTI 监测序列 | Q1–Q7：选择、过滤、聚合、降采样、升采样、跨序列平均、相关性 | 算子与执行性能补充，复杂问题多样性不足 |
| BIRD Mini-Dev | financial 交易；debit_card_specializing 月度消费 | 两个数据库分别有 32、30 题；不是全部都属于时序问题 | 辅助通用性验证，逐题筛选，不把日期字段当成时序分析 |
| DataAgentBench | stockmarket / stockindex | 官方分别 5、3 个问题；股票题 1 查询指定公司某年最高复权收盘价 | 确有金融时序，但量小、跨库设置不作为主实验 |

对于 Spider 1.0、LiveSQLBench、BIRD-Interact、DSBench、InfiAgent-DABench，本次没有确立可直接引用的官方专门时序 split。
这不等于它们没有任何时序题；它们不应再作为已经核实的主要时序题源推荐。
TSAQA 等序列 QA 数据确实含时序，但侧重分类、形态、选择题或变换辨认，通常不提供单条复杂 SQL oracle，故不适合作为本次主评测。

## 1. NLQTSBench：最贴近自然语言时序检索

官方：[代码](https://github.com/Atlamtiz/Sonar-TS)、[题目](https://github.com/Atlamtiz/Sonar-TS/blob/main/nlqtsbench/tasks.json)、[论文](https://arxiv.org/abs/2602.17001)。
本次仓库树版本为 `99866c43cd7ceb91a929de745e9f49cea92962e5`。

当前文件中的 Level 1 计数：

| 子类 | 题数 | 在本实验中的作用 |
|---|---:|---|
| Global Aggregation | 72 | 简单查询对照；包含 median 等需核验方言支持的算子 |
| Temporal Localization | 72 | 极值时间定位、排序对照 |
| Interval Discovery | 72 | 连续区间识别，适合考察分解 |
| Sliding Window | 58 | 滑动窗口统计及最优窗口选择，适合考察分解 |
| 合计 | 274 | SQL 可移植性审查前的完整候选池 |

其中连续区间与滑动窗口共 130 题，是优先审核的复杂查询候选；不是保证 130 题均能在当前 IoTDB 和 FS 工具集上执行。
这 274 题的 source 分布为 causal_rivers 234、ettm1 29、smd_machine_1_1 11。
实际样例的中文语义概述：

- `L1_T3_Interval_Discovery_00340`：指定日期范围内，通道 71 连续高于 0.01 的最长区间。
- `L1_T4_Sliding_Window_00516`：2023 年通道 71 极差最大的 21 天窗口。

文件直接提供 ground_truth、eval_metric 和 CSV 路径；本次未发现可直接当作 SQL oracle 的字段。
因此保留官方题目和答案，独立实现一条 IoTDB SQL 和参考计算程序，核对二者一致。
L2–L4 涉及形态、语义异常、报告，不放入主评测，避免评价目标转向时序算法或 Agent 系统。
早期 arXiv v1 为 831 题、L1 为 191 题，不能与当前 1,153/274 的文件计数混用，也不能直接引用旧版本整体分数作对照。

## 2. Spider 2.0：已有单条复杂 SQL 的强证据

官方：[Lite 题目](https://github.com/xlang-ai/Spider2/blob/main/spider2-lite/spider2-lite.jsonl)。
本次仓库树版本 `cafb867313aab4e674652054198f383cf4018943`；当前 Lite 文件 547 题。

- `bq031` / `noaa_data`：Rochester 气象序列的单位转换、8 日移动平均，再与前 1–8 日均值比较。
  [参考 SQL](https://github.com/xlang-ai/Spider2/blob/main/spider2-lite/evaluation_suite/gold/sql/bq031.sql) 使用 CTE、窗口 AVG、LAG，适合将确定的查询算子分解为中间步骤。
- `bq011` / `ga4`：指定结束时刻的 7 日活跃用户，排除最近 2 日仍活跃用户，输出去重人数。
  [参考 SQL](https://github.com/xlang-ai/Spider2/blob/main/spider2-lite/evaluation_suite/gold/sql/bq011.sql) 提供窗口过滤、集合差与计数的完整候选。
- `bq001` / `ga360`：首次访问到首次交易的时间差及交易设备，可进一步检验事件排序与实体关联。

这些是从正式 benchmark 按公开规则选出的题目，并非官方命名的 temporal split。
NOAA 更接近传感器时序；GA4 属于带时间的事件流，应分别报告。
BigQuery 方言、UNNEST、按日期分表和云端数据访问需要适配。若展平数组或合并表，两个接口必须使用同一结果，且原题的多重集和实体关系语义必须保留。
不要只给 FS 预连接视图、却要求 SQL 自行连接。

## 3. EHRSQL：既有时序问题、既有 SQL、明确筛选字段

官方：[2024 仓库](https://github.com/glee4810/ehrsql-2024)、[测试标注](https://github.com/glee4810/ehrsql-2024/blob/master/data/mimic_iv/test/annotated.json)。
本次树版本 `f9e1aa02160d39e3f8df52bf5c69c5cf2e472499`。

机械计数规则：query 不为字符串 null；val_dict.time_placeholder 非空；SQL 引用 chartevents 或 labevents。
测试集 1,167 题，934 题有 SQL；853 题有非空时间标注；生命体征表 77 题，化验表 98 题，合计 175 题，涉及 25 个原始问题模板。
这只是可复核候选集，不能称为官方发布的 175 题时序子集，也不能把 25 个模板的参数变体视为 175 个独立查询家族。

- `c814a79881c8be3a4809fdb9`：某患者 2100 年每月最低体重；原 SQL 有多层实体筛选和月聚合。
- `eb20ba71a6257201e3668689`：某患者第一次体重测量值；原 SQL 有嵌套实体筛选与首时间点检索。
- `9cdee4aaa2811035370a1c86`：某患者从指定月份开始的每日平均体重。

2024 使用公开 MIMIC-IV demo，仓库有 preprocessing，以及各 split 的 answer.json；它并不要求一开始就获取完整临床数据库。
必须固定相对时间基准，保留实体主键，检查重复时间点、缺失值与官方预处理，避免将记录导入 IoTDB 时因相同 time/TAG 覆盖而改变答案。
不包含医疗决策或不可回答题；这里只测可执行数值查询。

## 4. TSBS 与 TSM-Bench：从正式数据库 workload 继承任务

[TSBS 官方 IoT SQL](https://github.com/timescale/tsbs/blob/master/cmd/tsbs_generate_queries/databases/timescaledb/iot.go) 已核查，树版本 `8323e59c74027b108f4ad5ec5d3e498b0101a02e`。
`avg-daily-driving-session` 使用时间分桶、LAG/LEAD、状态转换和按天平均，是真正的一条复杂 SQL。
`long-driving-sessions` 采用 10 分钟桶内平均速度判定后计数；不能把描述自由改写为另一套严格连续驾驶定义。
优先按源代码确定语义，再补充不泄漏执行步骤的自然语言描述。
预先固定全部可执行模板或按算子分层纳入，不能只挑 FS 有利模板。

[TSM-Bench](https://github.com/eXascaleInfolab/TSM-Bench) 的 [TimescaleDB queries.sql](https://github.com/eXascaleInfolab/TSM-Bench/blob/main/systems/timescaledb/queries.sql) 有 7 个模板。
Q4 时间降采样、Q5 gapfill/interpolation、Q6 同时刻跨序列平均、Q7 correlation 可做算子覆盖；模板数较少，主要用于解释执行开销和规模变化。
两者都有正式 SQL 工作负载，但不是现成 NL→SQL 数据集，新增自然语言应标记为 adaptation。

## 5. BIRD、DataAgentBench 的定位及 gold 审核

[BIRD 官方 Mini-Dev 数据](https://huggingface.co/datasets/birdsql/bird_mini_dev) 本次文件为 500 题。
debit_card_specializing 30 题、financial 32 题是数据库总数，不是时序题数。
可复用候选有：1480 月度消费峰值；1482 分组年度消费变化；116 指定日期之间的账户余额变化；145 低于年度平均的交易。
数据含 question、evidence、SQL，可以保留原问题而不自造 workload。
但已发现题 1526 的 question 金额 634.8 与 SQL 的 Price=1513.12 不一致，evidence 日期也与问题不一致；应隔离审核，不默默修改后混入原 benchmark 分数。

[DataAgentBench](https://github.com/ucbepic/DataAgentBench) 的 stockmarket 有 5 题、stockindex 有 3 题。
[stockmarket/query1](https://github.com/ucbepic/DataAgentBench/blob/main/query_stockmarket/query1/query.json) 是公司某年最大复权收盘价问题，确认含真实时序检索语义。
其跨数据库布局会引入额外变量，且任务少，适合作补充案例。

## 公平对照与可归因实验

一条用户问题、一条最终 SQL、一次 LLM 生成、一次数据库执行是四种不同约束，必须分别写清。
建议先以如下四组组成一个 2×2 实验；每题只接受一个用户问题，不引入长期业务流程。

| 组别 | 模型输出/执行方式 | 要回答的问题 |
|---|---|---|
| SQL-one-pass | 一次模型生成，可先规划，最终一条 SQL；不见执行反馈 | 常规复杂 SQL 对照 |
| FS-one-pass | 一次模型生成完整 FS 操作序列/脚本；中间数据可由执行器传递，模型不见反馈 | 同推理轮次下，组合表达是否更易正确生成 |
| SQL-feedback | 有限轮工具反馈，可拆成数条 SQL 及组合中间结果 | 分步执行本身的收益 |
| FS-feedback | 相同预算、相同反馈机制的 FS 分步执行 | FS 接口相对 SQL 分步执行的额外收益 |

one-pass 两组使用同模型与同 token 上限；feedback 两组使用同 token、wall-time、工具调用上限，报告实际消耗。
one-pass 与 feedback 跨组不能因“都只有一个用户问题”就声称计算预算相同；额外绘制成功率—token/时间曲线。
主比较保留 SQL-one-pass 与 FS-feedback，同时以上消融排除额外反馈预算的解释。
如果 FS 脚本执行器尚不存在，只实施已有模式，并明确缺失 one-pass 消融，不能偷偷预写每题的程序。

有论文依据的补充 candidate：[DIN-SQL](https://arxiv.org/abs/2304.11015)，用同一模型重跑其分解/自校正方法。
它仍然解决一个 NL→SQL 问题，适合作为强 SQL 方法对照；“借鉴 DIN-SQL 的提示”必须与完整复现区分。
Sonar-TS 可作扩展相关系统，但其特征索引与 Python 算子会改变计算能力，不作为隔离 CLI 接口效果的唯一 baseline。
不能把不同数据版本、不同模型的论文分数直接填入本实验表。

## 当前实现对可执行性的约束

- 当前 benchmark/llm-sql-fs-comparison/prompts/filesystem.md 禁止 pipe、复合命令、通用 shell；stats 不支持时间/TAG 过滤，也不直接提供滑动窗口、区间划分、JOIN。
- 若使用 FS + awk/jq/Python 做组合，明确命名该配置，允许 SQL-feedback 使用同一计算环境；禁止绕过 CLI 直接连接数据库和读取 gold。
- 自定义组合库只能提供通用算子，不能按题目 ID 调用参考答案或封装一个完整的最长区间/特定题目求解器；领域算子扩展单独测。
- 优先提供相同 schema、路径映射、单位、业务 evidence；主实验不把找库、读文档、写报告计入核心任务。
- FsRowReader.read 在有时间/TAG 过滤时请求全部行后本地过滤；TableFilesystemSchemaProvider 的 stats/countRows 也读取全部行。扩大数据规模后，FS 的执行成本可能主导总耗时，不能预设其更快。
- 对所有候选先做表达能力表：原生支持、方言等价改写、需通用组合、需新算子、不支持。固定规则后冻结题单。
- 主性能表用双方均可表达的共同集合，另外报告相对于完整候选池的支持率；不支持和未验证不能消失在分母中。

## 数据、oracle、指标与统计协议

1. 保留 source/version/task_id/original question/original SQL-or-answer。原题只做记录在案的方言/格式适配，不往题面添加 FS 分解步骤。
2. 在原数据库跑官方 SQL，导入后跑等价 IoTDB SQL，逐题核对完整结果。NLQTSBench 无现成 gold SQL 的题，使用官方答案加独立参考程序校验；参考程序不开放给被测模型。
3. SQL/FS 使用同一不可变 IoTDB 数据副本。展开字段、预连接、虚拟目录、索引等若只服务某一组，作为独立消融；计算建设成本。
4. 时间边界、时区、NULL、重复行、多重集、并列极值、窗口缺失采样、ROWS 与时间窗口必须显式定义。发现原题与 gold 不一致时发布隔离清单和原因。
5. 主指标：结果正确率（按题型确定集合/序列/数值容差）、预算内成功率、端到端耗时、实际 token/费用、工具调用与修正次数。
6. 同时报客户端耗时、服务器查询耗时、读出行数/字节及 FS 中间数据量。不要将所有非工具时间等同于模型推理时间。
7. NLQTSBench 保留官方 soft score 用于来源内比较，但另报严格正确率；不能把接近正确的区间 IoU 当成完全正确 SQL 结果。
8. 总体时间指标包含失败/超时；双方均成功样本的配对加速比仅为辅助指标。缓存冷/热分开，交错随机执行各组。
9. 同一题重复 3 次以估计模型波动；按题和模板/源序列分组统计，避免参数变体造成伪独立样本；报告配对差值和 bootstrap 置信区间。
10. 推荐先对 NLQTSBench L1 274 题、EHR 175 候选与 Spider/TSBS 模板做全量支持审核；然后以预先固定的分层规则抽取约 30 题验证 harness。题量是拟议计划，本次未运行。
11. 主实验优先保留全部通过预定规则的候选；在 TSBS 上另做设备数、时间跨度、选择率规模实验。不要将几百条同模板参数实例宣传为几百种复杂查询。

## 可复核产物

源文件下载在 `.tmp/sql-fs-benchmark-sources/`。
`candidate-audit.json` 记录仓库树 SHA、源文件 SHA-256、筛选定义、274 个 NLQTSBench L1 ID、175 个 EHRSQL 候选 ID 及 Spider 样例。
文件核验只确认候选来源及数量，没有证明当前 IoTDB 的 SQL/FS 已能正确执行这些题。
