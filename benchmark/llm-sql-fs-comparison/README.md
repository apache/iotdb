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

# 大模型操作 IoTDB：SQL / filesystem 基准

新环境部署、失败重跑、报告整合和分支交接见 [统一操作手册](../README.md)。

通过本地 Codex 客户端调用 `gpt-5.6-sol`，比较同一模型使用 SQL 和 filesystem
接口完成数据库任务的正确性与端到端耗时。两种接口共用 IoTDB SQL 后端。
本地客户端的接入、隔离和计时说明见 [LOCAL_CODEX.md](LOCAL_CODEX.md)。

## 代码与任务

| 文件 | 用途 |
| --- | --- |
| `run_benchmark.py`、`prepare.py` | 基础任务执行与数据准备 |
| `run_application_benchmark.py`、`prepare_application.py` | 应用场景执行与数据准备 |
| `codex_client.py` | Codex 会话、工具回调、流断开重试及事件记录 |
| `dsh_client.py` | DSH 独立 trial、受控基线审计、canonical session 解析及指标采集 |
| `dsh-baseline.patch.yml`、`dsh-controlled-tool/` | 关闭 DSH 默认工具并只注册指定 IoTDB 接口 |
| `dsh_tool_runner.py` | 在隔离环境中校验并执行一条 SQL 或 FS 命令 |
| `tool_adapter.py` | SQL/fs 命令校验、CLI 调用与耗时记录 |
| `fixture.py`、`application_fixture.py` | 确定性数据与独立答案计算 |
| `tasks.json`、`application_tasks.json` | 任务目标、公开规则和答案结构 |
| `prompts/` | 运行时加载的共同说明和接口提示词 |
| `restricted-model-catalog.json` | 限制额外工具的模型目录 |
| `review_evidence.py`、`analyze.py` | 基础任务证据审核与报告 |
| `merge_application_results.py` | 应用场景结果汇总 |
| `test_*.py` | 数据、评分、适配器与客户端测试 |

基础任务包含 8 个已知表结构任务和 4 个发现任务，覆盖点查、范围查询、最近记录、
计数、聚合和表结构发现。应用任务包含 A1 冷却泵异常排查、A2 冷链温控合规、
A3 楼宇能耗异常；具体时间窗口、去重、排序和数据质量规则以任务 JSON 为准。

## 环境

- Python 3.11 或以上，以及可用的本地 `codex`。
- 已启动的 IoTDB table 模型实例，以及支持 filesystem 模式的匹配 CLI。
- Codex 已配置模型供应商和认证；模型请求发送至该供应商。
- 数据准备使用管理员账号创建专用测试库和只读账号。管理员密码从
  `IOTDB_ADMIN_PASSWORD` 读取，未设置时使用测试实例默认值。

在本目录运行下列命令。将 CLI 路径和端口替换为实际值。
连接文件包含认证信息，保存在忽略的 `results/` 下，文件权限为 0600。

## DSH trial adapter

服务器上的 DeepSeek Harness 可通过 adapter 执行单个受控 trial。SQL 组只注册
`iotdb_sql`，filesystem 组只注册 `iotdb_fs`：

```bash
python3 dsh_client.py \
  --dsh /data_01/iotdb-fs-exp/bin/dsh-exp \
  --patch /data_01/iotdb-fs-exp/config/agent.patch.yml \
  --workspace /data_01/iotdb-fs-exp/runtime/agent-workspace \
  --prompt-file /absolute/path/to/prompt.txt \
  --output /data_01/iotdb-fs-exp/results/trials/sql-example \
  --baseline sql \
  --database nlqts_001

python3 dsh_client.py \
  --dsh /data_01/iotdb-fs-exp/bin/dsh-exp \
  --patch /data_01/iotdb-fs-exp/config/agent.patch.yml \
  --workspace /data_01/iotdb-fs-exp/runtime/agent-workspace \
  --prompt-file /absolute/path/to/prompt.txt \
  --output /data_01/iotdb-fs-exp/results/trials/fs-example \
  --baseline filesystem \
  --database nlqts_001 \
  --filesystem-path /nlqts_001/raw_data.csv
```

受控补丁关闭 Bash、文件读写/搜索、Web、skill、subagent、workflow、todo、goal、
plan-mode 等默认插件。工具进程不经过 shell，并只继承运行 CLI 所需的环境变量；模型
API 凭据不会传给工具进程。SQL 校验器只接受选定数据库内的一条只读
`SELECT`/`SHOW TABLES`/`DESCRIBE`。FS 工具将对象路径固定在 trial 配置中，模型只能
选择枚举只读操作并填写带类型的 epoch 毫秒时间范围、measurement、分页和 TAG
过滤参数。`cat`、`head` 和 `tail` 返回有界结构化页面；`cat`/`head` 通过
`next_offset` 连续读取，原始 CLI 输出仅保留在审计目录。`help` 由 adapter 返回完整
接口说明，不启动 CLI。模型可见结果受 40,000-byte 上限约束，不包含审计或 spill 路径。
使用 `--fs-output-mode raw` 可运行 typed-raw 消融：保留固定路径与类型化参数，但让
数据读取返回原始 CSV 并恢复 DSH 的默认 spill 行为；默认 `page` 使用结构化分页。
使用 `--fs-output-mode compact` 可运行 typed-page-compact 消融：分页字段与预算保持
不变，时间戳和选定的数值 measurement 使用 JSON number，TAG 仍使用 string，NULL
使用 JSON null。

adapter 为每个 trial 覆盖独立的未压缩 session persistence 目录，避免通过全局目录
mtime 猜测会话归属。`dsh_result.json` 包含 wall time、LLM time、TTFT、decode time、
token/cache 用量、工具调用/错误和 provider retry。原始证据保存在
`dsh-session/**/session.v3.jsonl`；`events-summary.jsonl` 仅保留事件元数据和用量，
不复制提示词、工具参数、工具输出或 reasoning 正文。每个受控 trial 还会从所有
`request/header` 审计工具清单；缺少清单、暴露额外工具或调用额外工具都会把 trial
标记为 `infrastructure_error`。未指定 `--baseline` 的调用不会获得上述隔离保证，不能
用于正式 SQL/FS 对比实验。

## 基础任务

```bash
python3 prepare.py --cli /absolute/path/sbin/start-cli.sh --port 32867 --output results/setup
python3 run_benchmark.py --connections results/setup/connections.private.json --output results/pilot --repeats 10
python3 review_evidence.py results/pilot
python3 analyze.py results/pilot
```

默认执行 12 个任务 × 10 对 × 2 种接口，共 240 条记录。
使用 `--tasks K01 --repeats 1` 可检查单个任务；`--seed` 控制执行计划的随机化。
每次运行使用新的输出目录。

## 应用场景

```bash
python3 prepare_application.py --cli /absolute/path/sbin/start-cli.sh --port 32867 --output results/setup-application
python3 run_application_benchmark.py --connections results/setup-application/connections.private.json --output results/application --repeats 1 --seed 20260915 --stream-retries 2
```

准备脚本验证表结构、行数、内容哈希和只读权限。运行器检查快照与当前 fixture
的哈希是否一致；修改 fixture 后重新准备快照。默认执行三个场景的 SQL/fs 配对，
可用 `--tasks A1-TRIAGE A3-ENERGY` 选择场景。

流断开默认最多重试 2 次，每次使用新会话，重试与退避计入同一任务的 180 秒预算。
`--stream-retries 0` 关闭应用运行器的重试。限流、其他 API 错误、错误答案和任务超时
不属于流断开重试条件。

## 输出与计时

每次运行保存以下记录：

- `run.json`、`schedule.csv`：配置与任务执行顺序。
- `trials/<id>/`：提示词、事件、原始工具输出和最终结果；重试记录在其 `retries/` 下。
- `trials.jsonl`、`trials.csv`：状态、答案判定、耗时、调用数和重试信息。
- `comparison.csv`、`REPORT.md`：任务汇总与测试报告。

`task_wall_ms` 包含模型生成命令、多轮工具交互和最终回答。
`tool_wall_ms` 包括命令校验、CLI 执行与结果包装；
`cli_process_ms` 是其中的 CLI 部分。
`non_tool_wall_ms` 包含模型、网络、排队和 Codex 编排。
本地接口没有独立的 API 请求计时，`model_api_ms` 为 null。

DSH adapter 的 `task_wall_ms` 从子进程启动计至退出；`model_api_ms` 与 DSH
`sessionStats.llmMs` 的定义一致，从 `step/start` 计至 `assistant/message`，包含同一步骤
中的 provider retry 等待。`tool_wall_ms` 是匹配的 `tool/call` 至 `tool/result` 区间之和；
`cache_hit_ratio` 是 cache-read token 占全部 prompt-side token traffic 的比例。

基础任务通过 `review_evidence.py` 审核观察证据后再生成报告。
应用任务由运行器直接比较完整 oracle。成功、答案错误、超时和基础设施错误分别记录。
比较完成速度时使用两侧都正确完成的配对；不同重试策略和数据快照分别统计。

## 本地验证与报告

```bash
python3 -m unittest discover -s . -p 'test_*.py' -v
```

固定命令的 CLI 基准位于 [../cli-fs-comparison](../cli-fs-comparison/README.md)。
已有测试结果保存在 `results/`；接口链路复核见
[INTERFACE_EFFICIENCY_ANALYSIS.md](INTERFACE_EFFICIENCY_ANALYSIS.md)。

公开时序查询的迁移预检见 [NLQTSBench 筛选器与计划](../nlqtsbench/README.md)。
该工具仅生成静态候选清单，尚未接入本目录的模型运行器。
