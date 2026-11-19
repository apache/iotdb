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


# TQTS-Bench 新版 FS 同题试跑：2026-11-19

**结论：端到端解题时间显著下降，主要来自工具执行等待减少；LLM 本身的推理/输出负担和主答案正确数没有同步改善。** 本轮是固定八道验证题、每题一次的历史前后对照，不是随机配对实验，也不是对 SQL 的公平性能排名。

## 固定条件与干预

复用 2026-11-18 FS control 的八题顺序、公共上下文、只读数据库账号与私有 DeepSeek 配置。题目、公共上下文和 settings 文件逐字节一致。模型为 `deepseek-official/deepseek-flash`；每题 900 秒、120 次工具调用，每次生成 32,768 token，单个 FS 进程 180 秒、8 GiB Java heap、512 MiB direct memory，返回上限 40,000 字符。实际请求的生成上限及工具白名单经 smoke 和八题 trace 核验。

Agent 只使用 `dataset_context` 和 `iotdb_fs`，自行发现 schema 和文档；没有预载 gold、SQL、源码或外部计算工具。初始题目提示不变，新功能仅通过工具参数及 help 提供。旧 FS 使用之前保存的历史结果，不重新执行；本轮没有同时跑离线 gold 查询。

干预是昨天完成的整套基座及受控接口：流式读取/聚合、时间范围限定、早停分页、精确 median 外排、多窗口单扫描、列路径解析缓存，以及新增聚合参数。原始时间谓词由 IoTDB 执行，聚合仍在 CLI；不单独归因每项优化。运行期间没有修改代码、模型配置或工具协议。CLI SHA-256 为 `319facda8f059a7d1049fb2b422bbfb5a6639e63257d82b99473aadc4ae0d3fd`。

## 总体结果

| 指标 | 原 FS | 新 FS | 变化 |
|---|---:|---:|---:|
| 八题 wall 合计（秒） | 3,648.8 | 947.2 | -74.0% |
| wall 中位数（秒） | 398.7 | 106.5 | -73.3% |
| 已记录工具区间合计（秒） | 2,899.2 | 269.5 | -90.7% |
| 模型 API 区间合计（秒） | 700.0 | 665.3 | -5.0% |
| 总 token（含缓存输入） | 2,811,043 | 2,598,071 | -7.6% |
| 非缓存输入 token | 260,299 | 228,545 | -12.2% |
| 缓存输入 token | 2,420,736 | 2,240,512 | -7.4% |
| 输出 token | 130,008 | 129,014 | -0.8% |
| 其中 reasoning token | 78,261 | 82,859 | +5.9% |
| 工具尝试次数 | 367 | 172 | -53.1% |
| FS 原生命令调用 | 300 | 125 | −58.3% |
| 堆 OOM 次数 | 26 | 0 | 消除本轮 OOM |
| 900 秒截止题数 | 2 | 0 | 全部正常结束 |
| 主答案与参考一致 | 5/8 | 5/8 | 未提高 |
| CLI 峰值 RSS | 8.30 GiB | 1.43 GiB | 仍高于统计状态本身 |

总 wall 从 **60.8 分钟降至 15.8 分钟（−74.0%）**。模型区间约占新 wall 的 70.2%，原来为 19.2%；工具区间由约 79.5% 降为 28.5%。区间求和包含调用开销，并非数据库内部算子时间或精确关键路径分解；原来两个超时题的未结束区间也未完整计入。

总 token 只下降 7.6%，输出 token 基本不变，reasoning token 反而增加 5.9%。reasoning 已包含在输出中，不能重复加总；总 token 包含缓存输入，不能直接等同于付费 token 或金额。输入加权缓存命中率由 90.29% 变为 90.74%。传输层 retry 两轮均为 0。

作为辅助核对，两轮主答案均正确的相同五题，wall 合计从 1,710.5 秒降至 514.7 秒（−69.9%）；因此耗时下降并非仅来自更快交付错误答案。该子集不能代替全部八题结果。

## 逐题结果

| 题目 | 原/新 wall（秒） | 原/新总 token | 原/新工具尝试 | 新窗口调用 | 主答案复核 |
|---|---:|---:|---:|---:|---|
| cmapss 013 | 43.6 / 112.0 | 73,218 / 97,834 | 23 / 17 | 2 | 与参考一致 |
| cmapss 030 | 63.2 / 101.1 | 257,264 / 124,330 | 18 / 15 | 2 | 与参考一致 |
| gas_dynamic 001 | 514.6 / 126.3 | 531,815 / 258,787 | 31 / 23 | 11 | 与参考一致 |
| gas_dynamic 021 | 900.1 / 67.5 | 179,439 / 65,008 | 40 / 13 | 4 | 与参考语义不一致 |
| ndbc 005 | 282.8 / 50.4 | 176,581 / 124,045 | 56 / 15 | 4 | 与参考一致 |
| ndbc 028 | 138.2 / 66.9 | 109,036 / 88,867 | 21 / 11 | 1 | 与参考语义不一致 |
| pvdaq 006 | 900.1 / 298.1 | 974,373 / 1,280,017 | 94 / 43 | 9 | 与参考语义不一致 |
| pvdaq 040 | 806.3 / 124.9 | 509,317 / 559,183 | 84 / 35 | 14 | 与参考一致 |

八题都自然采用了多窗口功能：共 **47 次窗口调用、829 个请求窗口**，包括重复验证及不同网格，不代表 829 个不重复窗口。原 `stats` 调用 202 次，新版 60 次；对应已完成 runner 区间合计由 2,161.0 秒降为 117.0 秒。无额外人工提示窗口算法，也没有运行时修改 Agent 计划。

## 成本和失败机制

1. **扫描和重复调用确实减少。** Gas 001 从 514.6 秒降至 126.3 秒，去掉此前 OOM 后的恢复搜索，改用 11 次四分窗口缩小极值范围；答案时间 5,020,520 ms 与 sensor_02=−4.39 正确。NDBC 005 的 `stats` 从 31 次降至 5 次，以四个年/月/日/小时批次定位 1675516800000、wtmp=20.3，wall 从 282.8 秒降至 50.4 秒。PVDAQ 040 的 `stats` 从 71 次降至 15 次，wall 从 806.3 秒降至 124.9 秒，主结果仍为 metric_2745、2019-06-15 窗口、27 → 47.566667 °C。

2. **更少的统计调用不保证每题更快。** 两道 C-MAPSS 均答对、调用更少，但 wall 都增加。新 trace 对 `/root/iotdb_cmapss/train` 的宽范围 `tree` 分别耗时 40.3/42.5 秒且结果截断；013 另有两次 `find` 合计 16.3 秒。它们是新增探索路径的成本，不能算作统计执行回退，也说明目录发现仍需有界化。

3. **Gas 021 消除了执行失败，却保留了粒度错误。** 原来 21 次 OOM 后 900 秒截止，新版 67.5 秒完成。模型把两个实验的均值混合后排名 3720000、3600000、4080000；gold 按“实验 × 窗口”排名，三个结果均来自 ethylene_methane，顺序为 4080000、3600000、3720000。工具返回保留 object/window 身份，信息在模型后续组合中丢失；这是 SQL pilot 也出现过的语义问题。

4. **NDBC 028 的错误仍是 gap 边界。** 新版读取了前一天的末尾观测，却仍主动排除跨小时 gap，选择 HH:10/HH:50；gold 按 gap 的后端观测归属窗口，选择 HH:00/HH:50。值都为 10 分钟并不代表答案等价。

5. **PVDAQ 006 暴露了缺乏完整性证明的搜索。** 新版 298.1 秒结束，其中模型 260.9 秒、工具 35.4 秒；token 从 974,373 增至 1,280,017。模型用非空的宽窗口及部分低密度日/小时抽查，推断不存在超过 4 小时的间隔，给出一个 session。gold 实际有两个 session：`1385764560000..1407053520000`，均值 60.00006511738033；`1407070260000..1595797200102`，均值 59.90763809908739。遗漏间隔为 16,740,000 ms（4 小时 39 分）。大窗口非空不能证明窗口内部没有长 gap；加速让错误计划更快完成，未补齐 session 能力。

6. **剩余资源与接口问题没有被隐藏。** 一次 tail 请求带了不支持的 measurements，被受控适配器拒绝；没有进程超时、未完成工具结果或 DSH 参数层拒绝。三次截断来自两次宽树输出和一次 grep。最高 RSS 1.43 GiB 来自 session 题的 grep；wc、stat 也仍保留较重的旧文本路径，不应把统计流式保证扩展到所有 FS 命令。

## 答案判定范围

人工复核最终主答案，复用原始 gold 输出，非官方执行准确率或独立盲审。两个旧超时题本次均返回了错误答案，因此“正常结束 8/8”不能解释为正确率提高。PVDAQ 040 主答案一致，但新网格末尾延至 10 月 4 日，而 gold 的总体范围截至 9 月 30 日；本次未改变获胜窗口，仍需记录为范围偏差。最终回答中的 `final_commands` 有额外 purpose、窗口缩写或字符串转写，`evidence_call_ids` 也有自造标签；它们不能视为通过重放验证的接口输出，实际 request/response 才是执行事实。

## 下一轮建议

优先补齐可完整验证的组合运算：带前驱观测/跨窗口边界规则的 gap/session 汇总，返回关联字段的 argmin/argmax，以及保留实体键的端点差值与排名。前两者分别针对不完整搜索和多次定位，后者防止模型手工组合时丢失实验粒度。宽目录发现应提供有界展开、明确截断/续读及更窄的搜索操作。

把试跑目标从“工具是否够快”转向“是否能用完整证据完成变换和正确交付”。后续以本轮 trace 为验证输入，分别改动并检查输出等价性；固定版本后重复运行以估计方差，再扩展独立任务族。不要仅为改善这八题的主答案而加入 gold 特定规则。本轮未自动继续改接口或补跑模型。

## 证据与复现

服务器：`/data/zzy/iotdb-fs-exp/results/tqts-fs-streaming-20261119/`；本地：`.tmp/tqts-fs-streaming-20261119/`。

- `protocol.json`、`selection.json`、`public-context/`、`integrity-check.json`：版本、预算、输入一致性和运行前后哈希核验。
- `trials/<id>/dsh-session/**/session.v3.jsonl`、`tool-output/`：完整模型事件、token、原始命令与结果、GNU time 资源；`resource-monitor.jsonl` 提供全程采样。
- `summary.json`、`comparison.json`、`answer-review.json`：逐题指标、前后对照与人工答案复核。
- `frozen-source/`、`evidence.tar.gz`：执行源码与不含私有配置/凭据目录的证据包。新旧原始 traces 均保留。

分析入口为 `analyze_fs_pilot.py` 和 `compare_fs_pilots.py`，均只读取已保存结果。模型连通性 smoke 不计入八题指标。没有重跑 SQL，也没有重复采样，因此无法给出显著性、排除 provider/缓存/宿主机变化，或声称胜过 SQL。
