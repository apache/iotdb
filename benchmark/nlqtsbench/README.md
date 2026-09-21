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

# NLQTSBench 选题筛选器与迁移计划

后续的数据导入、TsFile 准备和确定性答案核验见 [MIGRATION.md](MIGRATION.md)。
完整执行结果见 [EQUIVALENCE.md](EQUIVALENCE.md)，逐题证据索引见
[equivalence-summary.json](equivalence-summary.json)。
以下记录保留第一阶段静态筛选的状态；`execution=not_verified` 等字段描述筛选器
本身的输出，实际执行证据由迁移报告单独记录，不覆盖历史筛选结果。

本目录实现只读的第一阶段筛选器：检查数据可导入性、SQL/fs 表达能力以及
模型/方言/评分所需改动。官方名称是 **NLQTSBench**。
实验对象是一个自然语言问题对应的一条复杂 SQL 或一组 FS 操作；不增加长程业务任务。

筛选器不下载数据，不执行 Sonar 代码，不连接数据库，不调用模型，也不修改源题目。
`candidate` 表示进入迁移审核的候选，**不表示已经可运行**。
`pass_static` 仅表示 CSV 通过保守的格式检查，不能代替实际导入和答案核对。
本阶段所有题的 `execution=not_verified`、`experiment_ready=false`；没有运行验证就不放行。

## 使用

在 IoTDB 仓库根目录运行，Python 3.11+，仅使用标准库：

```bash
python3 benchmark/nlqtsbench/select_tasks.py \
  --sonar-root /absolute/path/Sonar-TS-iotdb \
  --output .tmp/nlqtsbench-selection/run-001
```

默认读取 `SONAR/nlqtsbench/tasks.json` 和 `SONAR/nlqtsbench/ts_data/`。
CSV 保存在其他目录时，用 `--data-root /path/containing/ts_data`。
输出目录必须不存在，防止覆盖历史证据。数据到齐后用新的目录重新检查。
能力规则针对下述已检查的源码版本；更换 CLI、方言或适配器时应复核规则，输出中的
源码 SHA-256 用于检测差异，不会自动推断新版本能力。

输出文件：

| 文件 | 内容 | 是否可给被测模型 |
| --- | --- | --- |
| `selection.json` | 全题清单、保留/排除原因、三个筛选维度、源版本及文件哈希 | 否，含内部语义审核信息 |
| `selection.csv` | 便于人工审核的逐题表 | 否 |
| `public-candidates.json` | 候选原题、task_id、原始数组下标 | 可作为之后提示词的输入；不是运行器任务配置 |

未把 `meta.args`、`ground_truth`、`answer`、`predict_perfect.json`、Sonar 的
解题技能或预计算特征表放入公开题目文件。正式评测的 schema 和通用 CLI 帮助仍需补齐。

## 已核验的版本与本次结果

2026-09-20 检查：

- Sonar-TS-iotdb：`99866c43cd7ceb91a929de745e9f49cea92962e5`，本地分支 `feat/iotdb`。
  当前 `TaskDatabase` 仍是 SQLite 实现；分支名不代表已完成 IoTDB 迁移。
- IoTDB：`2bf1146ea058071a1def9ed3e6a2323487131a7a`，`fs/inner-view`。
- 共 1,153 题；274 道 L1 纳入候选，879 道 L2–L4 按实验范围排除。
- 本地没有原始 CSV：274 道候选的数据状态均为 `missing`，不能声称数据已可导入。
- 不需要按题目成功率挑选。全部 L1 保留在审核分母中，不支持、待修改、数据缺失分别统计。

| L1 子类 | 题数 | SQL 计划 | 原生 FS 与现有实验限制 |
| --- | ---: | --- | --- |
| Global Aggregation | 72 | 54 题用 MIN/MAX/AVG/极差；18 题 median 单独审核 | 54 题仅在完整文件位于公开时间范围内时可使用每 channel 的 stats；median 需组合计算 |
| Temporal Localization | 72 | 值筛选/极值、排序、取时间；核对 crossing 与 tie | stats 的 min_time 是数据范围端点，不能当作极值发生时间；缺 FIELD 条件与 arg-extremum |
| Interval Discovery | 72 | LAG、累计分组、区间长度排名等单语句改写 | 缺连续段划分和最长段选择 |
| Sliding Window | 58 | 窗口聚合与最优窗口选择；15 题 variance | 缺滚动计算、完整窗口约束和窗口排名 |

按拟议 SQL 实现路径，现有 adapter 有 105 题需扩展（18 median、72 interval、15 variance）；
126 题具备基本语法表面，43 题需逐条确认窗口 SQL 能否通过 adapter。
这不意味着不存在其他 SQL 写法，也不代表语法表面通过的题已经正确执行。
FS 有 220 题标为需组合，54 题需确认整文件范围。
“需组合”描述确定性执行算子缺口；没有断言模型读少量原始行后一定无法推理答案。
但正式实验不能把模型阅读大量数据后的心算当成数据库算子支持。

## 维度一：数据库是否可导入

Sonar 的输入是每题一个 CSV；原加载器转成每题独立的 SQLite `raw_data`：
`timestamp TEXT PRIMARY KEY` 加若干数值通道列。它将时间截断到秒，并将非数值转为空值。
生成的 `yearly_feature`、`monthly_feature`、`daily_feature` 是 Sonar 的额外索引，
主实验先不引入，以免一侧获得预计算答案线索。

建议使用 IoTDB **TABLE** 长表，逻辑映射为：

| 原始字段/概念 | IoTDB 映射 | 约束 |
| --- | --- | --- |
| timestamp | 内置 TIME 列 `time` | 显式时区与精度；禁止依赖本机时区 |
| 每题独立 CSV | 每题独立测试库/表，并记录 task_id | 不将不同题的合成片段按 channel/time 合并覆盖 |
| 通道列名，例如 `147` | `channel_id STRING TAG` | 避免纯数字测点名的标识符引用问题 |
| 数值 | `value DOUBLE FIELD` | 保留 NULL；导入后核对精度与完整数据 |

优先每题独立测试库，沿用现有适配器的库级隔离。若同库共享表，仅靠 task_id TAG
不足以满足现有权限边界，需要增加行级工具约束，不在第一阶段默默改成共享库。
双方共用完全相同的长表。该模型映射是拟议方案，尚未执行 DDL/导入。

筛选器已检查：CSV 是否存在、路径是否越界、表头是否唯一且含 timestamp/目标通道、
行宽、空文件、ISO 时间、原加载器秒级键重复、数值/NULL/Inf、顺序与采样间隔、
数据是否全部处于公开的整年/整月范围。完整扫描，不用前 N 行冒充全量验证。

状态约定：

- `missing`：数据未取得，未知可导入性，不是“不支持 IoTDB”。
- `reject`：例如重复源秒级主键、坏行或缺目标列；不静默去重或改数据。
- `review`：例如时间精度、时区偏移、非数值转 NULL、缺测/不规则采样，需明确转换合同。
- `pass_static`：格式层通过，仍待导入后 count、内容哈希、时间边界及 NULL 数量核验。

检查器当前只接受秒到微秒的 ISO 时间；其他合法时间格式可能进入人工审核，
这是保守预检范围，不是数据库能力上限。禁止对 `meta.args.time` 字符串使用 eval。

## 维度二：SQL 与 FS 是否覆盖功能

必须分开三层：IoTDB 引擎能力、CLI 命令能力、benchmark 适配器允许的能力。

源码中的 TABLE window registry 包含 LAG/LEAD/ROW_NUMBER，grammar 有 ROWS/RANGE，
并有窗口函数 IT；聚合 registry 包含 VAR_SAMP 与 PERCENTILE。
因此不应把复杂时序查询直接标成“IoTDB 不支持”。但注册函数不能证明具体语义、
窗口组合或与标准答案等价，median 尤其不能直接换成 APPROX_PERCENTILE。

现有 `tool_adapter.py` 只接受 SELECT/SHOW/DESC 入口，函数白名单不含 LAG、ROW_NUMBER、
PERCENTILE、VAR_SAMP，WITH 起始语句也不被接受。完整 IoTDB SQL 能运行，仍可能被实验框架拒绝。
扩展时保留只读账号、库隔离、禁止任意函数/外部访问；不能简单取消全部验证。

FS 的 cat/head/tail 支持时间、TAG 与投影，stats 支持每 TAG 组合统计，
但 stats 不接受时间/TAG 条件，读取选项也不支持数值 FIELD 谓词。
FsRowReader 当前先读取再做部分过滤；stats/countRows 读取全部数据。
需要记录实际读行数/字节，不能用更短的 FS 命令预测它更快。

建议保留两种明确的能力配置：

1. **现有原生 FS**：只调用当前命令；只能在可直接覆盖的小范围任务上作为对照。
2. **FS + 通用组合**：提供通用 filter/sort/group/rolling 等能力；SQL 分步组获得相同计算环境。
   这是后续待实现配置，当前筛选器标为 `not_implemented`，不能当作已有工具。

不提供以题目 ID 或完整题型命名的求解函数。不得通过 FS 的 sql 子命令绕回整条 SQL，
也不得允许脚本直接连接数据库或读取本地参考 CSV/gold。

## 维度三：模型、方言与评分改动

1. 固定 TABLE 模型；不把 Tree 的 GROUP BY SESSION/CONDITION 或 ALIGN BY DEVICE 混入。
   树模型若要研究，作为后续独立实验，不能只给 FS 特殊分段能力。
2. SQLite timestamp 文本比较改成明确的 TIME 边界；FS --start/--end 是包含端点，
   整月/整年 SQL 推荐半开区间，转为 FS 端点时按固定精度处理。
3. 63 道 L1 的私有 args 使用包含时分秒的 Timestamp 区间，而题面仅显示日期。
   不能把隐藏参数直接给模型；先重算公开题意与原 gold 的一致性，必要时隔离题目，
   或另建双方一致的澄清题版本，并发布原题/修订题两个结果。
4. 滑动窗口技能代码用采样间隔中位数换算 N，再做 N 行 rolling；其端点长度可能是 K 天减一个步长。
   技能是当前系统求解方法，不是 gold 生成器，仍需独立重算确认。缺测、完整窗口、
   方差 ddof、NULL、并列最优都需要单独定义，不能直接将 N 行窗口换成 K 天 RANGE。
5. 阈值定位题的 first rise/last fall 需核对是“第一个满足阈值的点”还是“真正穿越事件”。
6. 原 evaluator 使用原题数组的整数下标，而任务 id 是字符串。筛选器同时保留二者；
   后续评分直接按 task_id 关联并调用 score_one，或显式映射回原始下标，禁止筛选后重编号。
7. 保留原 rel_acc/hit/iou 用于来源内比较，另报严格结果正确率；不能将非零 IoU 算成完全成功。
8. 不复制 Sonar cold-start/技能/feature index 到主实验提示词。独立 oracle 与公开工具帮助分开。

## 与已有基础实验衔接

参考 [现有总计划](https://github.com/apache/iotdb/blob/fs/inner-view/benchmark/README.md)
及本地 [基础任务运行器](../llm-sql-fs-comparison/README.md)。
本次本地没有总 README，已读取远端版本；没有为了文档同步而切换或合并分支。

可复用：Codex 接入、工具计时、配对随机化、只读账号、隔离输出与错误分类。
不能直接复用：固定 fixture、K/A 任务 oracle、按整数推断类型的评分逻辑，以及限制过窄的 SQL/fs adapter。
新增 NLQTSBench 运行入口，保持原基础实验不变；本次尚未实现该运行入口。

后续顺序：

1. 获取带版本/hash 的 CSV，运行本筛选器，发布 274 道题的完整状态表。
2. 实现导入器与数据一致性核验，不去重、不按标准答案裁剪数据。
3. 从每个操作家族按固定 ID 顺序取探针，先验证 SQL/FS 的确定性程序与独立 oracle，
   不调用模型；探针覆盖 median、极值、阈值、连续段、均值/极差/方差窗口、NULL 与缺测。
4. 解决语义审核项，记录所有修订与排除，再冻结正式题单；通过执行探针和逐题
   oracle 核对后，才由下一阶段生成真正的实验 ready manifest。
5. 接入模型实验，对同一问题设置 SQL 单条、SQL 分步和 FS 分步；需要时增加一次生成
   完整 FS 序列的消融。至少报告预算内正确率、端到端时间、token、调用数、读出数据量。

当前静态筛选结果不能提前声称 274 题全部可覆盖，也不能因数据缺失声称 NLQTSBench 不适用。

## 本地验证

```bash
python3 -m unittest discover -s benchmark/nlqtsbench -p 'test_*.py' -v
python3 -m black --check benchmark/nlqtsbench/*.py
git diff --check
```

测试覆盖导入覆盖风险、脏数据、路径/符号链接边界、时间范围泄漏、接口能力区分、
公开题目与 oracle 隔离，以及筛选后的原始下标保持。没有启动数据库或运行 LLM。
