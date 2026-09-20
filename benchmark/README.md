<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# SQL / filesystem 对比测试操作手册

本文是新环境、新会话的统一入口，覆盖获取代码、部署、执行测试、重跑、生成报告和提交更新。
仓库为 `apache/iotdb`，工作分支为 `fs/inner-view`。命令按 Linux/Bash 编写。

## 1. 测什么、代码在哪里

| 测试 | 解决的问题 | 入口 | 默认规模 |
| --- | --- | --- | --- |
| 固定 CLI 命令 | 等价查询经 SQL/fs 接口执行的耗时 | [cli-fs-comparison](cli-fs-comparison/README.md) | 9 个只读场景，两种模式各重复 5 次 |
| 大模型基础任务 | 同一模型理解任务、调用数据库、给出正确答案的耗时 | [llm-sql-fs-comparison](llm-sql-fs-comparison/README.md) | 12 个任务 × 10 对 × 2 种模式＝240 条 |
| 大模型应用场景 | 模型能否完成完整业务问题 | 同目录的 `run_application_benchmark.py` | 3 个场景 × 1 对 × 2 种模式＝6 条 |

SQL/fs 都由 `iotdb-cli` 访问 IoTDB，fs 内部也生成 SQL。大模型实验通过本机
`codex app-server` 调用 `gpt-5.6-sol`，模型推理由所配置的供应商提供。
两组共用任务、数据快照和只读权限，分别提供 SQL/fs 提示词；被测模型禁用 skill 加载。

基础任务包含 K01–K08（已知表结构）和 D01–D04（数据发现）。应用任务如下：

| ID | 场景 | 必须完成的业务处理 |
| --- | --- | --- |
| `A1-TRIAGE` | 冷却泵异常排查 | 运行状态过滤、去重、缺测检查、异常区间合并与优先级排序 |
| `A2-COMPLIANCE` | 冷链温控合规 | 时区转换、高低温与离线区间判断、运输箱复核清单 |
| `A3-ENERGY` | 楼宇能耗异常 | 同楼层同小时基线对齐、异常排名、累计电能回退重建 |

公开规则和答案结构见 [tasks.json](llm-sql-fs-comparison/tasks.json) 与
[application_tasks.json](llm-sql-fs-comparison/application_tasks.json)；数据及独立答案由
`fixture.py`、`application_fixture.py` 生成。

## 2. 获取分支和准备环境

新机器获取代码：

```bash
git clone --branch fs/inner-view https://github.com/apache/iotdb.git
cd iotdb
export BENCH_REPO="$PWD"
export BENCH_TAG="$(date -u +%Y%m%dT%H%M%SZ)"
```

已有仓库时，在工作区改动已提交后同步分支：

```bash
git status --short
git fetch origin
git switch fs/inner-view
git merge --ff-only origin/fs/inner-view
export BENCH_REPO="$PWD"
export BENCH_TAG="$(date -u +%Y%m%dT%H%M%SZ)"
```

环境要求：JDK 17、Maven 3.6 或以上、Python 3.11 或以上、Bash、unzip，以及本地
Codex。Python 执行器和单元测试使用标准库；Black 仅用于开发时格式检查。
按你的供应商方式安装并认证支持 `gpt-5.6-sol` 的 Codex；运行器读取
`${CODEX_HOME:-$HOME/.codex}/config.toml` 和本地认证。若供应商配置 `env_key`，
对应环境变量也需在运行前设置。

```bash
java -version
mvn -version
python3 --version
codex --version
codex app-server --help
```

客户端配置最初按 `codex-cli 0.153.4` 验证；更换 Codex 版本或供应商后先执行第 5 节
单任务测试，确认模型可用、配额足够、动态工具协议可用。供应商认证和模型权限需要在新机器配置。

## 3. 构建并启动专用 IoTDB

从本分支构建包含 fs 功能的完整分发包，Maven 会下载依赖及适用平台的 Thrift 编译器：

```bash
cd "$BENCH_REPO"
mvn clean package -pl distribution -am -DskipTests
export BENCH_RUNTIME="$BENCH_REPO/benchmark/llm-sql-fs-comparison/results/runtime-$BENCH_TAG"
mkdir -p "$BENCH_RUNTIME"
unzip -q distribution/target/apache-iotdb-2.0.11-SNAPSHOT-all-bin.zip -d "$BENCH_RUNTIME"
export BENCH_IOTDB="$BENCH_RUNTIME/apache-iotdb-2.0.11-SNAPSHOT-all-bin"
export BENCH_CLI="$BENCH_IOTDB/sbin/start-cli.sh"
export BENCH_PORT=6667
bash "$BENCH_IOTDB/sbin/start-standalone.sh"
```

文件名中的版本来自当前项目版本；版本升级后以 `distribution/target/` 中实际 ZIP 名为准。
默认端口需空闲。启动完成后，执行下列 SQL/fs 检查，确认连接和命令输出正常。
如果已有匹配本分支 CLI 的测试实例，直接设置 `BENCH_CLI`、`BENCH_PORT`，跳过启动步骤。

```bash
bash "$BENCH_CLI" -h 127.0.0.1 -p "$BENCH_PORT" -u root -pw root \
  -sql_dialect table -e 'SHOW DATABASES'
bash "$BENCH_CLI" -h 127.0.0.1 -p "$BENCH_PORT" -u root -pw root \
  -sql_dialect table --access_mode filesystem -e 'help'
```

以上使用新实例默认密码。管理员密码已修改时，将检查命令中的密码替换，并在准备数据前设置：

```bash
read -r -s -p 'IoTDB 管理员密码: ' IOTDB_ADMIN_PASSWORD
export IOTDB_ADMIN_PASSWORD
```

两个大模型数据准备脚本固定使用 `127.0.0.1` 和管理员 `root`，接受 `--cli`、`--port`、
`--output`；在数据库所在主机执行最直接。它们创建新的专用库和随机密码的只读账号，
连接信息保存在权限为 0600 的 `connections.private.json`。

## 4. 固定 CLI 对比

```bash
cd "$BENCH_REPO/benchmark/cli-fs-comparison"
cp config.env.example config.env
${EDITOR:-vi} config.env
```

将 `CLI_BIN` 设置为 `BENCH_CLI` 对应的绝对路径，`PORT` 设置为测试端口，填写实际
`USERNAME`、`PASSWORD`。配置文件中的 `CLI_BIN` 应填写实际值；`DATABASE` 使用专用测试库名。
默认是 4 个设备、每设备 1000 点，预热 2 次、正式重复 5 次。

```bash
set -a
source ./config.env
set +a
python3 benchmark.py --prepare
./run_compare.sh
```

使用 `./run_sql.sh`、`./run_fs.sh` 可分别运行。需要重建同名测试库时使用
`python3 benchmark.py --prepare --reset`，它会删除配置中的数据库。
写入测试通过 `INCLUDE_WRITE=true` 开启，写入独立的 `WRITE_TABLE`。

每次运行输出到 `results/<UTC run id>/`，包括 `results.csv`、`summary.csv`、
`comparison.csv` 和 `metadata.json`。已有实测解释见 [CLI 测试报告](cli-fs-comparison/REPORT.md)。

## 5. 大模型基础任务：先冒烟、再全量

同一终端保留第 2、3 节的 `BENCH_*` 变量。每次准备或运行都使用尚不存在的输出目录：

```bash
cd "$BENCH_REPO/benchmark/llm-sql-fs-comparison"
export BENCH_SETUP="results/setup-$BENCH_TAG"
export BENCH_SMOKE="results/smoke-$BENCH_TAG"
export BENCH_FULL="results/full-$BENCH_TAG"
python3 prepare.py --cli "$BENCH_CLI" --port "$BENCH_PORT" --output "$BENCH_SETUP"
python3 run_benchmark.py --connections "$BENCH_SETUP/connections.private.json" \
  --output "$BENCH_SMOKE" --tasks K01 --repeats 1
python3 review_evidence.py "$BENCH_SMOKE"
python3 analyze.py "$BENCH_SMOKE"
```

检查冒烟目录中的 `REPORT.md`，确认 SQL/fs 均完成；然后执行全部 240 条任务：

```bash
python3 run_benchmark.py --connections "$BENCH_SETUP/connections.private.json" \
  --output "$BENCH_FULL" --repeats 10 --seed 20260913
python3 review_evidence.py "$BENCH_FULL"
python3 analyze.py "$BENCH_FULL"
```

准备脚本生成六种表角色置换；每个配对共用一个快照，并验证完整内容哈希。
`review_evidence.py` 在本地核对模型实际观察到的数据，不发起模型调用。
`analyze.py` 生成 `REPORT.md`、`trials.csv` 和 `comparison.csv`。
`evidence_pending` 表示答案匹配、证据还待核验，不能直接归为成功。

## 6. 大模型应用场景

```bash
cd "$BENCH_REPO/benchmark/llm-sql-fs-comparison"
export BENCH_APP_SETUP="results/setup-application-$BENCH_TAG"
export BENCH_APP="results/application-$BENCH_TAG"
python3 prepare_application.py --cli "$BENCH_CLI" --port "$BENCH_PORT" \
  --output "$BENCH_APP_SETUP"
python3 run_application_benchmark.py \
  --connections "$BENCH_APP_SETUP/connections.private.json" \
  --output "$BENCH_APP" --repeats 1 --seed 20260915 --stream-retries 2
```

运行结束自动输出 `REPORT.md`、`trials.csv`、`comparison.csv`。用
`--tasks A1-TRIAGE A3-ENERGY` 选择场景，用 `--repeats` 增加配对数量。
每轮保持相同数据快照、模型、预算和重试配置。修改 fixture 后重新准备数据。

## 7. 模型报错和失败重跑

默认每个任务总预算 180 秒，单次 CLI 最多 30 秒；模型响应流提前断开时最多重试 2 次，
每次建立新会话，共用任务总预算。应用运行器支持 `--stream-retries 0` 关闭断流重试；
基础运行器使用客户端默认值，没有同名命令行参数。

| 状态/错误 | 处理方法 |
| --- | --- |
| `stream disconnected before completion` | 客户端自动重试；仍失败时按下面步骤重跑 |
| `timecho token quota exhausted before model request`、限流、其他模型服务错误 | 恢复配额或服务后，在新输出目录重跑 |
| 基础 `infra_error` / 应用 `infrastructure_error` | 查看该条 `error`、`evaluation_reason`，保留失败记录 |
| 基础 `task_timeout` / 应用 `timeout` | 检查工具轨迹和调用间隔；总时限耗尽不触发断流重试 |
| `wrong_answer` / `invalid_answer` | 按公开规则核对答案和评分；与模型服务报错分开统计 |
| `protocol_invalid` / `tool_policy_violation` | 先修正工具隔离或协议，再新开测试轮次 |

基础任务可从原计划选出所有含 `infra_error` 的配对，重跑配对双方以保留完整对比。
在基础任务目录执行，`BENCH_FULL`、`BENCH_SETUP` 指向原运行及原快照：

```bash
python3 - <<'PY'
import json
import os
from pathlib import Path

run = Path(os.environ['BENCH_FULL'])
rows = [json.loads(line) for line in (run / 'trials.jsonl').read_text().splitlines() if line.strip()]
failed = {row['pair_id'] for row in rows if row['status'] == 'infra_error'}
plan = json.loads((run / 'schedule.json').read_text())
selected = [pair for pair in plan if pair['pair_id'] in failed]
(run / 'retry-schedule.json').write_text(json.dumps(selected, indent=2) + '\n')
print(f'需重跑 {len(selected)} 对；为 0 时跳过重跑命令。')
PY
```

有待重跑配对时执行：

```bash
export BENCH_RETRY="results/retry-$(date -u +%Y%m%dT%H%M%SZ)"
python3 run_benchmark.py --connections "$BENCH_SETUP/connections.private.json" \
  --output "$BENCH_RETRY" --schedule-file "$BENCH_FULL/retry-schedule.json" \
  --replacement-source "$BENCH_FULL/trials.jsonl"
python3 review_evidence.py "$BENCH_RETRY"
python3 analyze.py "$BENCH_RETRY"
```

`--schedule-file` 控制实际执行条目；`--replacement-source` 只记录 `replacement_of`，
不会自动筛选、覆盖或合并历史结果。重跑沿用原 `fixture_index` 和连接文件。
若只重跑失败一侧，可将所选计划的 `arms` 筛成该侧；速度比较优先采用同轮双方成功的配对。

应用场景按任务重跑，下面以 A1、A3 为例，替换为本轮实际失败的任务 ID：

```bash
export BENCH_APP_RETRY="results/application-retry-$(date -u +%Y%m%dT%H%M%SZ)"
python3 run_application_benchmark.py \
  --connections "$BENCH_APP_SETUP/connections.private.json" \
  --output "$BENCH_APP_RETRY" --tasks A1-TRIAGE A3-ENERGY \
  --repeats 1 --seed 20260915 --stream-retries 2
```

应用运行器会重跑所选场景的 SQL/fs 双方，不接受 `--schedule-file` 或 `--replacement-source`。

## 8. 报告整合与结果解释

| 文件/目录 | 用途 |
| --- | --- |
| `run.json` | 模型、版本、种子、数据与运行配置 |
| `schedule.json` / `schedule.csv` | 基础任务的原始配对计划 |
| `trials.jsonl` | 每次任务状态、计时和答案 |
| `trials/<trial_id>/result.json` | 单条任务完整结果 |
| `trials/<trial_id>/tools/` | 模型命令、CLI 原始输出及调用计时 |
| `trials/<trial_id>/events.jsonl`、`retries/` | 模型事件与断流重试轨迹 |
| `evidence_review.json` | 基础任务证据审核决定 |
| `REPORT.md`、`trials.csv`、`comparison.csv` | 可读报告和汇总表 |

主要指标：`task_wall_ms` 是完整任务耗时，`tool_wall_ms` 是工具耗时，
`cli_process_ms` 是其中的 CLI 执行时间。`non_tool_wall_ms` 包含模型、网络、排队和编排；
`model_api_ms` 未单独测量。固定 CLI 的计时还包含 JVM 启动和连接。

统一报告应同时列出全部尝试的状态数量、各任务正确率、双方均成功的配对耗时、工具调用数，
并按运行轮次区分。基础分析器只接收单个运行目录，不能将重复 `trial_id` 直接拼接后分析。
如需要“重跑后最终状态”视图，应明确每条采用的 `source_run` 和替换关系，同时保留原始失败；
配对速度采用同轮结果，跨轮替换后的状态表单独展示。

应用场景可导出多轮尝试明细：

```bash
python3 merge_application_results.py \
  --output "results/application-merged-$(date -u +%Y%m%dT%H%M%SZ)" \
  "$BENCH_APP" "$BENCH_APP_RETRY"
```

此脚本为历史三场景报告编写：`trials.jsonl`、`trials.csv` 保留所有输入尝试，
`comparison.csv` 计算每侧成功数和成功耗时。其 `paired_success_attempts` 按运行目录计数，
多次重复时应按 `(source_run, pair_id)` 重新核对真实配对数；`REPORT.md` 中存在固定的
历史快照、断流和耗时解释，发布新报告时须替换成当轮事实。单轮报告由应用运行器直接生成。
当前应用报告的偶数样本“中位数”取排序后的上中位值，正式多轮报告需按普通中位数重新计算。

已确认的评分问题：A1 `peak_temperature_c` 的公开类型是 `number`，当前评分器按 oracle
整数类型判定，会拒绝数值相等的 `92.0` 等答案。出现此类失败时按公开结构复核并在报告中
记录修正依据。详见 [接口效率分析及评分复核](llm-sql-fs-comparison/INTERFACE_EFFICIENCY_ANALYSIS.md)。

现有结论：基础原始 240 条批次没有显示稳定的全任务速度优势；复杂应用任务更适合 SQL 表达
筛选、分组和排序，fs 适合浏览、发现和预览。各轮的错误、数据量和模型服务状态需一并解释。
该分析及 [CLI 实测报告](cli-fs-comparison/REPORT.md) 随分支保存；它们引用的原始 `results/`
属于本地运行产物，新机器通过本手册重新生成。连接密码、Codex 认证、数据库目录和原始日志
不随 Git 分支迁移；对外保存报告时导出已检查的独立报告文件。

## 9. 后续修改、验证、合并和推送

只读的本地测试不需要数据库或模型服务：

```bash
cd "$BENCH_REPO"
python3 -m unittest discover -s benchmark/llm-sql-fs-comparison -p 'test_*.py' -v
python3 -m py_compile benchmark/cli-fs-comparison/benchmark.py benchmark/llm-sql-fs-comparison/*.py
bash -n benchmark/cli-fs-comparison/run_compare.sh
bash -n benchmark/cli-fs-comparison/run_sql.sh
bash -n benchmark/cli-fs-comparison/run_fs.sh
git diff --check
```

修改 Python 时按项目现有风格格式化；修改 CLI Java 时执行相关格式、编译和单元测试。
仓库根目录与各模块 `AGENTS.md` 给出具体要求。此前 `ad5f0bbf1a` 合并时验证了
37 个 Python 测试、66 个子测试和 387 个 CLI 测试；这是该提交的历史验证记录。

```bash
python3 -m black --check benchmark/cli-fs-comparison/benchmark.py benchmark/llm-sql-fs-comparison/*.py
mvn -pl iotdb-client/cli -am test \
  '-Dtest=org/apache/iotdb/cli/**/*Test,org/apache/iotdb/tool/*Test' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

这里用 `-am` 编译同分支依赖模块，并只选择 CLI 相关测试；新环境无需先把依赖安装到本地 Maven 仓库。

此前整理为文档清理、固定 CLI 基准、Codex 框架、应用场景、说明与报告五个语义提交：
`ed9981fae8`、`96e12f6fdf`、`b0154e826a`、`2ffb6b1d7f`、`7f31fcf94a`；
随后用 `ad5f0bbf1a` 合并 master。后续按每个功能分别暂存、检查并提交：

```bash
git status --short
git add -- path/to/changed-file
git diff --cached --check
git diff --cached
git commit -m 'docs(benchmark): describe the concrete change'
```

在工作区干净时合并最新主线，然后复核受影响模块，再推送功能分支：

```bash
git fetch origin master fs/inner-view
git merge origin/fs/inner-view
git merge --no-ff origin/master
# 处理可能的冲突，并运行受影响模块的检查后继续。
git status --short
git push -u origin fs/inner-view
```

推送需要远程写权限；HTTPS 克隆可使用本机已配置的 Git 凭据，或将 origin 改为你有权限的
SSH 地址。`results/`、本地配置和缓存保持在 `.gitignore` 中。

## 10. 新会话交接文本

将下面这段连同本机仓库路径交给新会话即可继续：

> 请在 IoTDB 的 `fs/inner-view` 分支继续工作。先读取仓库 AGENTS.md 和
> benchmark/README.md，检查分支与工作区状态。目标是对比本地 Codex gpt-5.6-sol
> 通过 SQL/fs 完成相同任务的正确率和端到端耗时。先确认测试实例、CLI 路径、端口和
> Codex 认证，再准备快照并执行单任务冒烟；通过后执行基础 240 条和应用 A1–A3。
> 流断开最多重试两次；其他模型服务错误恢复后按原快照重跑并保留原始记录。
> 最终提供一份包含所有尝试、重跑来源、正确率及成功配对耗时的测试报告，核对已知评分
> 和历史汇总脚本的问题。代码按语义提交；需要同步主线时合并 origin/master，完成验证后
> 推送 fs/inner-view。原始日志和连接认证保留在本地忽略目录中。
