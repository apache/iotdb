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

# IoTDB SQL / FS Agent 数据获取实验设计

调研日期：2026-09-17。代码核对：`2bf1146ea0`。本文是实验建议，不是新实验结果；本轮没有调用被测模型、修改实验脚本或启动数据库实验。

建议将主问题定义为：**在相同数据、任务和资源预算下，Agent 使用哪种接口更可靠、更快、更节省成本地交付正确数据？** 不以“生成 SQL 是否正确”作为唯一目标，也不以 CLI 启动耗时代表 Agent 性能。

这里将现代数据获取具体化为：寻找数据源、理解 schema 与业务文档、筛选/排序/汇总、多表或多测点组合、处理缺测与重复、交付可验证结果。预测模型训练和开放式报告写作先不纳入主实验。

## 1. 可以借鉴的公开 benchmark

以下事实来自作者项目、官方仓库或论文。版本可能变化，正式实验应固定下载版本、commit 和数据哈希。下列优先级是针对本项目的建议。

| Benchmark | 已核实的特点 | 对本实验的价值 | 使用限制与建议 |
| --- | --- | --- | --- |
| [Spider 1.0](https://yale-lily.github.io/spider) | 跨数据库 schema 的 Text-to-SQL；训练/测试数据库分离 | 基础检索、连接、聚合；检验跨 schema 泛化 | 适合基础任务来源，不足以单独评价长流程 Agent |
| [BIRD / Mini-Dev](https://github.com/bird-bench/mini_dev) | 真实数据库内容、外部 evidence；提供 EX 和效率评价，Mini-Dev 的 R-VES 针对查询运行效率 | 引入脏数据、业务知识和“正确后再谈效率”的思路 | VES/R-VES 不等于包含模型交互的端到端耗时；版本/方言需固定 |
| [Spider 2.0](https://spider2-sql.github.io/) | 初始框架含 632 个企业工作流问题，涉及大 schema、文档和多步查询；当前有 Lite/Snow/DBT 等不同设置；Agent 设置要求交付 CSV | 最适合借鉴“发现 → 理解 → 获取 → 交付”的任务组织 | BigQuery/Snowflake 等特性不能机械迁移到 IoTDB；迁移后必须标注为改编任务 |
| [LiveSQLBench](https://github.com/bird-bench/livesqlbench) | Base-Lite 270 题，Base-Full v1 600 题，Large-v1 480 题；包含知识库、可执行测试和 Query/Management 分类 | 只读 Query 子集、业务文档依赖、可执行验证、大 schema 探索 | 首轮排除 Management；Large 主要强调 schema/上下文复杂度，不能当作亿级行数性能实验 |
| [LiveSQLBench-CLI](https://github.com/bird-bench/livesqlbench/blob/main/LiveSQLBench-CLI/README.md) | 基于 Harbor 将任务包装成终端 Agent 环境，使用 PostgreSQL，并支持 Codex 等客户端 | 最接近“CLI 驱动 Agent”的运行环境；可借鉴隔离、轨迹和 verifier 组织 | 不是现成的 IoTDB SQL/FS 比较器；仍需替换数据库、两种接口和答案验证 |
| [BIRD-Interact](https://bird-interact.github.io/) | 支持被动会话和主动 Agent 交互；Full 600 题，结合知识库、用户模拟器和测试用例 | 多轮澄清、错误恢复、逐步发现规则 | 主实验先用明确需求；用户模拟器会增加随机性，放入独立扩展实验 |
| [DataAgentBench](https://ucbepic.github.io/DataAgentBench/) | 12 个数据集、54 个问题、9 个领域、4 种 DBMS；复杂连接键、非结构化信息和领域知识；官方要求多次运行并提供轨迹 | 更贴近最终数据问题的回答，而非只比较 SQL 文本；借鉴按数据集汇总与重复实验 | 跨 DBMS 联邦能力超出单 IoTDB 接口比较，首轮只迁移适配子集 |
| [DSBench](https://github.com/LiqiangJing/DSBench) | 真实数据分析和建模任务，来源包括 ModelOff/Kaggle | 为多表业务分析与数据交付设计提供补充 | 建模、图像和通用 Python 能力易掩盖访问接口的影响，非主评测集 |
| [TSBS](https://github.com/timescale/tsbs) / [IoT-benchmark](https://github.com/thulab/iot-benchmark) | 时序数据生成、查询和规模测试；TSBS 包含最新位置、低燃料、长驾驶区间等 IoT 查询 | 生成可扩展数据和具体时序检索任务 | 不是 Agent benchmark；需要另外添加自然语言任务、Agent 环境与答案验证 |

建议组合：以 Spider 2.0 的工作流形式设计任务，以 BIRD/LiveSQLBench 的业务知识和执行验证为参考，以 TSBS/IoT-benchmark 提供 IoT 数据与规模维度。保留一部分公开任务的可追溯改编，避免完全自造简单任务；不把改编后的得分称为原 benchmark 官方分数。

[Distilled Test Suites](https://github.com/taoyds/test-suite-sql-eval) 提醒我们：错误查询可能在单个数据库实例上恰好返回正确答案。因此不能只比较 SQL 字符串，也不能仅靠一个快照上的结果相等证明语义正确。

## 2. 必须区分的实验问题

| 层次 | 实验 | 固定内容 | 可以支持的结论 |
| --- | --- | --- | --- |
| A：产品效果，主实验 | 同一个 Agent 分别使用实际 SQL CLI 和 FS CLI 完成相同任务 | 模型/推理配置、快照、任务、预算、辅助工具 | 当前实现作为数据获取入口的成功率、时延和成本 |
| B：接口表达，诊断实验 | 在共同能力子集上比较 SQL/FS 表达；让执行路径和规范化返回尽可能一致 | 相同底层操作/SQL、结果范围、格式、排序、分页 | 更接近接口表达、文档和命令构造的影响 |
| C：执行成本，诊断实验 | 不调用 LLM，运行人工确认正确的命令计划与代表性 Agent 计划 | 数据、查询范围、输出要求、缓存条件 | CLI/JDBC/查询/传输/客户端处理开销 |

B 中若要实现新的等价适配器，必须单独标明它是实验性控制条件，不能将结果当成现有产品实测。对于现阶段无法匹配底层执行的任务，明确仍含实现差异，而不声称已识别纯语法效应。

主对照为 `SQL-native` 与 `FS-native`。FS 原生组禁止其 `sql` 逃生入口，否则它可能实质上通过 SQL 完成任务。`FS+SQL` 可以作为独立 hybrid 扩展组。对数据发现任务再增加 `FS without virtual views` / `FS with virtual views` 消融；两侧必须拥有同等业务元数据，不能只给虚拟目录组额外的人类语义标注。

辅助计算分成两种独立设置：只使用数据库 CLI；或双方均可使用相同的受限 Python/文本处理环境。后者只能处理各自接口已经获取的结果，不得通过额外客户端或原始数据文件绕过指定入口。FS 管道要么作为原生产品能力明确开放，要么将实验名称标明为受限命令子集；不能笼统称为完整 FS CLI。

## 3. 任务与数据

先为每个任务定义输出契约和参考解，再写两侧提示词。按业务目标抽样，避免分别为某接口设计有利题目。

| 任务层次 | 例子 | 需要区分的条件 |
| --- | --- | --- |
| 发现与定位 | 找到某站点的温度数据、相同测点在不同设备的位置 | 已知 schema / 未知 schema；干扰库表；命名别名；标签与属性 |
| 简单获取 | 取最新 20 条记录，读取某设备某时间段的指定列 | 时间边界、升降序、NULL、并列时间、无结果 |
| 过滤与汇总 | 找超阈值设备，分设备求平均和缺测数量 | 数值 FIELD/TAG 条件、COUNT 行/非空值、分组粒度 |
| 组合获取 | 关联资产表与读数表，按基线筛选异常楼层 | join、实体键、时间对齐、单位、重复记录 |
| 时序事件 | 连续超温超过 15 分钟，提取完整异常区间 | 采样不齐、缺测、区间端点、去重规则 |
| 多步业务交付 | 冷却泵排查、冷链合规、楼宇能耗对比 | 先发现后获取；多个数据对象；结构化结论及证据 |

任务集建议由“公开任务改编”和“IoT 原生任务”组成，分别报告结果。现有 A1/A2/A3 可作为种子场景，不能只重复这三个问题来替代任务覆盖。

Table 与 Tree 分轨：关系型公开数据优先适配 Table，Tree 使用真正的设备/测点/标签任务。一个逻辑任务在两种数据模型中重复出现时，统计上仍属于同一个任务族，不是两条完全独立证据。

迁移公开数据时，保留业务语义和重复行语义；明确 IoTDB 时间列、TAG 组合、行标识的映射，避免相同 time/TAG 写入造成覆盖。先验证源数据库 gold 结果与 IoTDB 参考解在迁移快照上一致。记录不支持、排除、重写的任务及理由；公共可表达子集用于接口对比，全业务集合另报告覆盖率，不在看到结果后删除 FS 不擅长的题。

独立改变三类难度，不把它们混成“大数据”一个变量：

- 数据行数：例如 `10^4 / 10^6 / 10^7`，初期先在固定命令实验中验证资源可承受性。
- 元数据规模：例如 10/100/1000 个候选对象，控制目标数据大小，观测发现耗时与 token。
- 结果大小与选择率：例如返回 10/100/1000 行，以及不同范围选择率；同时固定正确答案所需信息。

增加时间边界、空结果、NULL、重复、负值、乱序和少量字段/表重命名的隐藏变体。调整数据值要同步计算新 oracle，而不只是反复跑完全相同的固定答案。开发集与测试集按数据库、业务场景或任务模板划分，防止只改时间和表名造成任务泄漏。

## 4. 正确性与指标

最终输出统一为 JSON 或明确 schema 的 CSV/Parquet 结果，按实际内容评分。中间 CLI 输出在产品组保留原样；在接口表达控制组再统一格式。

答案验证必须处理字段集合、数据类型、NULL、重复行的重数、任务要求的排序、时间精度/时区及显式数值容差。`number` 按数值契约比较 `92` 与 `92.0`，`integer` 与布尔值按各自规则检查。空结果不能自动视为正确。对于允许任意行序的任务比较 multiset，而不是丢掉重复的 set。

Oracle 使用独立的确定性代码或经过交叉验证的参考查询，不直接复用被测 FS 的过滤/统计实现。对关键模板使用多个隐藏快照检验同一语义；LLM judge 只作为开放式描述的辅助，不决定数据正确性主分数。保留可观察的工具调用、返回数据和最终答案即可，不需要保存模型私有思维链。

主指标建议：

1. **Success@budget**：在任务时限、token/调用上限内正确交付的比例。多次独立 trial 估计单次成功概率，不取 best-of-k。
2. **成功交付曲线**：在 30/60/120/300 秒内正确结束的任务比例；所有已安排任务进入分母。快但错误不计成功。
3. **配对成功时延**：仅在同一任务、快照和重复中双方都正确时，报告时延比和配对差异。必须同时给配对覆盖率和“仅 SQL 成功/仅 FS 成功/双方失败”四格表；没有共同成功配对时不报速度排名。
4. **成本效率**：输入/输出/缓存 token 与实际计费成本；`所有 trial 的成本总和 / 成功次数`，包括失败开销，零成功时记为不可估计。不同模型 tokenizer 的 token 数不直接混成一个均值。
5. **交互与执行工作量**：模型轮数、工具尝试数、有效命令/原子操作数、错误与修复次数、底层 SQL 数、元数据查询数、返回行/字节、截断和分页次数。

补充成功样本时延 P50；P95 只对样本量足够的分层报告。每题 5 次不足以给出稳定的逐题 P95。

总时延起点为向 Agent 发出任务，终点为最终答案提交；离线评分耗时单列。串行运行时可按互斥区间拆分：

`T_task = T_tool + T_non_tool`

`T_tool` 再区分校验、进程/JDBC 建连、数据库查询、客户端处理/输出包装；DB 查询耗时与 CLI 耗时通常是包含关系，不能直接相加。没有细粒度观测的分项保持 null，不能用差值伪造。

`T_non_tool` 包含模型、网络、服务排队和 Agent 编排，不能叫“纯推理时间”。若允许并发，使用时间区间并集/关键路径，不能将并发请求时长简单累加为墙钟时间。

## 5. 配对设计与控制条件

- 同一模型版本、推理档位、Agent scaffold、上下文策略、任务说明和快照；SQL/FS 只替换访问接口与必要文档。
- 接口说明要求信息充分、同等质量，并记录提示词与帮助调用 token；不人为截短某一侧文档来强行等长。
- 每对任务随机 SQL→FS 或 FS→SQL，并使顺序平衡；穿插任务，避免全天先跑 SQL 再跑 FS。每个 trial 使用独立会话，不共享探索结果。
- 主实验使用一致的数据库缓存条件和 CLI 生命周期。当前“一次工具调用一个进程”属于产品测量的一部分；连接复用和持久 FS 会话放入单独消融。不要只对一侧免除 JVM 启动。
- 首轮并发设为 1，固定服务端资源；高并发吞吐另测，避免服务限流混入接口比较。
- 先以 300 秒/30 次工具尝试为候选预算进行校准，再在试跑后冻结；记录每次工具内部有多少语句/操作，避免“一次 SQL 聚合”等价于“一次任意 FS 管道”的误解。token 硬上限只有能被运行时可靠执行时才声称受控。
- 无法控制的 provider 缓存、采样 seed、请求起止时间等必须记录为限制。主运行中固定重试政策，重试消耗纳入原任务预算。
- 分别标记业务错误、语法错误、输出截断、超时、限流/断流等。主表报告用户实际成功率；剔除基础设施问题的敏感性分析另列且预先定义。若重跑，重跑完整配对并保留原尝试，不能择优替换。
- 统计按模型、任务类别、Tree/Table、数据规模分别汇总，再按预设权重给宏平均。用保持配对的分层/聚类 bootstrap 给出成功率差、时延比和成本差的 95% 区间；同模板变体和重复运行不能被当成独立任务。样本量按 pilot 中的配对差异和目标效应调整。

## 6. 当前仓库的可复用部分与缺口

本节来自本地代码，不是公开 benchmark 的结论。

| 位置 | 已有能力 | 下一轮要做什么 |
| --- | --- | --- |
| `benchmark/cli-fs-comparison/` | 固定命令端到端 CLI 计时 | 增加完整结果等价验证、行数/对象数规模梯度、DB 查询数；用作 C 层 |
| `benchmark/llm-sql-fs-comparison/run_benchmark.py` | 12 个基础/发现任务，配对随机顺序、快照校验、重复运行 | 参数化模型/推理配置/预算，加入更多任务族和 Tree 轨道 |
| `run_application_benchmark.py` | A1/A2/A3 真实 Agent 多步场景、失败分类 | 扩大业务覆盖，冻结评分器和重试版本，加入隐藏变体 |
| `fixture.py:compare_answer` | 独立答案和严格递归比较 | 目前仍按 oracle 的 Python int 类型强制 int；应改为按公开 answer_shape 验证数值类型，再比较内容，并补回归用例 |
| `tool_adapter.py` | 限制 SQL/FS 命令、只读访问、进程计时、原始输出和截断标志 | 现仅允许指定数据库绝对路径，排除了 `/.virtual`；新增 Tree 和虚拟路径规则，按解析后的实际资源限定范围；区分受限/完整命令设置 |
| `codex_client.py` | 实际 Agent 会话、工具事件和 usage | 接口没有可靠 API 起始时间时继续保持 model_api_ms=null；核验真实模型、可执行预算和缓存信息 |
| `analyze.py` | 成功配对、token、错误分类与统计 | 补 Success@time、失败计入的成本、能力覆盖率、按任务族分层置信区间 |

已核对的实现差异：`FsRowReader.read()` 在有时间或 TAG 过滤时可先无限量读取，再在 Java 中筛选；列投影也在客户端处理。`TableFilesystemSchemaProvider.stats()/countRows()` 读取数据后计算统计。这意味着“最终给 Agent 返回的字节少”不等于“数据库少读/少传数据”。主实验应测量现状，再以固定命令与下推消融解释原因，不能事先将结果解释为 FS 语法优劣。

已有 `INTERFACE_EFFICIENCY_ANALYSIS.md` 记录了应用任务数值类型误判，以及工具外等待占比较高等现象；下一轮应优先修正评分契约、记录可靠时间边界。其历史样本不能视为本轮新接口的结果。

## 7. 建议的执行顺序与规模

1. **校准阶段**：审计 oracle、测试数据、适配器允许的操作和输出截断；对每题准备两侧正确参考计划。共同能力题要求双方参考解可过，业务覆盖题保留能力缺口标签。固定运行快照和文档版本。
2. **Pilot**：24 个独立任务 × 2 个接口 × 1 个模型 × 3 次重复 = 144 个 trial。覆盖上述 6 类任务；用于发现评分/环境问题和估算方差，不作为最终优势结论。
3. **主实验**：建议从 120 个任务实例（如 80 Table + 40 Tree）× 2 个接口 × 3 个模型配置 × 5 次重复 = 3600 个 trial 起步。具体样本量由 pilot 决定；模型配置至少涵盖不同能力/推理档位，按模型分别报告，不把模型差异归因于接口。
4. **消融与规模实验**：只在代表性子集增加 virtual on/off、schema 已知/未知、同等后处理、输出格式/排序、冷启动/复用。行数和元数据规模先由固定命令测量，再选关键点跑 Agent，避免将全部因素做成昂贵的全组合。

每个 trial 的最小记录建议包含：`task_id/family/fixture_hash/model_version/reasoning_config/arm/repeat/order/status/answer_correct/answer_hash/task_wall_ms/tool_wall_ms/non_tool_wall_ms/token_usage/tool_attempts/atomic_operations/db_queries/result_bytes/truncated/retries/source_commit`。无法直接观测的指标用 null，不能把零作为缺失值。

最终交付应包括：任务与数据版本、运行配置、完整 trial 表、可观察调用轨迹、确定性 verifier、任务分类成功率图、正确交付时间曲线、成功配对时延/成本图、规模曲线和失败原因分布。预先指定成功率为首要指标，避免从多个指标中只挑 FS 或 SQL 最有利的一项。

待验证的假设是：FS 的对象浏览可能降低发现成本；SQL 的过滤、连接、聚合可能减少返回数据和模型后处理；虚拟目录的收益可能主要出现在元数据密集任务。它们应通过上述分层实验被支持或推翻，不应写成预设结论。
