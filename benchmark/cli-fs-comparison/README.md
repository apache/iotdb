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

# IoTDB CLI SQL / filesystem 对比基准

新环境部署、三类测试和分支交接见 [统一操作手册](../README.md)。

本目录附有一次实测报告：[REPORT.md](./REPORT.md)。报告中的耗时是固定命令的
CLI 端到端耗时，不包含大模型的理解、规划、命令生成和最终回复时间；若要比较
大模型直接操作效率，应按报告第 6 节记录完整任务链路。

完整任务基准的运行代码、自然语言任务集及两组提示词位于
[大模型 SQL/fs 基准](../llm-sql-fs-comparison/README.md)，支持本地 Codex 执行、
独立答案校验和测试报告生成。

这个目录用于比较同一批 IoTDB 表数据通过 `iotdb-cli` 的 SQL 模式和
filesystem（`fs`）模式执行时的端到端耗时。filesystem 模式在当前实现中仍然
通过 JDBC 执行 IoTDB SQL；它提供的是 Unix 风格命令表面，并在 CLI 内完成路径
翻译、结果过滤/统计和格式化。测试对象是当前分支提供的
`--access_mode filesystem`，默认使用 table dialect。

每个样本都会单独启动一次 `start-cli.sh`，并计时从启动 CLI 到命令完成的
wall-clock 时间。因此结果包含 JVM 启动、登录、命令解析、JDBC 请求和输出
渲染成本，适合回答“脚本直接调用 CLI 哪种方式更快”。它不等同于同一交互
会话中重复执行命令的纯服务端延迟；如果要研究后者，应另行写一个长连接
客户端基准。

## 快速开始

先保持 IoTDB 服务运行，不要在测试过程中重启或清理服务端数据目录：

```bash
cp config.env.example config.env
# 修改 CLI_BIN，例如：
# CLI_BIN=/work/iotdb/sbin/start-cli.sh
${EDITOR:-vi} config.env
```

首次准备独立数据库时可以加 `--reset`。这个选项会删除配置中的数据库，
只应对专用测试库使用：

```bash
set -a; . ./config.env; set +a
python3 benchmark.py --prepare --reset
./run_compare.sh
```

不希望删除数据库时，使用 `python3 benchmark.py --prepare`；如果表中已有
相同时间戳，IoTDB 的写入语义可能覆盖已有点，正式比较前应保证数据集干净。

只测一个访问模式时：

```bash
./run_sql.sh       # SQL mode
./run_fs.sh        # filesystem mode
```

也可以把配置文件作为脚本的第一个参数：

```bash
./run_compare.sh /path/to/my-config.env
```

## 测试内容

`benchmark.py --prepare` 创建数据库和两张表，并按设备、时间戳生成确定性数据。
默认数据量为 4 个设备、每设备 1000 点；所有参数均可在 `config.env` 中调整。

| 场景 | SQL mode | filesystem mode |
| --- | --- | --- |
| 点查 | `SELECT ... WHERE time = ...` | `cat --start ... --end ...` |
| 时间范围 | `SELECT ... WHERE time >= ... AND time <= ...` | `cat --start ... --end ...` |
| 前 N 行 | `SELECT ... ORDER BY time LIMIT N` | `head -n N` |
| 扫描 | `SELECT ... ORDER BY time` | `cat` |
| 计数 | `SELECT COUNT(*)` | `count` |
| 聚合 | `COUNT/AVG/MIN/MAX` | `stats` |
| 表结构 | `DESC ... DETAILS` | `schema` |
| 表元数据 | `SHOW TABLES DETAILS` | `meta` |
| 表列表 | `SHOW TABLES` | `ls -f csv` |

两种模式使用相同的数据库、表、时间范围和返回行数。filesystem 路径使用
`/<database>/<table>.csv`，这是当前 table provider 的虚拟路径，并不是本地
文件。

默认不测写入。若要增加写入用例，在配置中设置：

```bash
INCLUDE_WRITE=true
```

然后重新准备数据库。写入用例用 SQL `INSERT` 对比 filesystem `tee -a`，脚本
会自动为 fs 调用增加 `--fs_write_mode enabled`。写入会追加到 `WRITE_TABLE`，
不会修改只读查询表。

## 输出与结果解释

每次运行会在 `OUTPUT_DIR/<UTC run id>/` 生成：

- `results.csv`：每个场景、模式和重复次数的原始耗时（毫秒）、退出码和输出字节数；
- `summary.csv`：均值、中位数、P95、P99 和按样本计算的操作/秒；
- `comparison.csv`：同时运行两种模式时的均值、SQL/FS 加速比和较快模式；
- `metadata.json`：数据规模、预热次数、重复次数、端点和本机信息。

`WARMUP` 用于降低首次运行的类加载影响，`REPEAT` 用于形成可比较的样本。
建议先用默认的小数据集确认命令可用，再逐步增大 `POINTS_PER_DEVICE`、
`QUERY_LIMIT` 和 `REPEAT`。比较时应同时记录 IoTDB 版本、JDK、机器负载、
服务端配置和是否冷缓存；不要只比较单次最小值。

脚本不会启动、停止或删除 IoTDB 服务，也不会执行 `kill -9`。如果 CLI 返回
非零退出码或超时，当前运行立即失败，并把最后的诊断信息打印到 stderr。

## 手工运行等价命令

```bash
# SQL mode
./sbin/start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root \
  -sql_dialect table -e 'SELECT * FROM cli_benchmark.telemetry LIMIT 10'

# filesystem mode
./sbin/start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root \
  -sql_dialect table --access_mode filesystem \
  -e 'head -n 10 -f csv /cli_benchmark/telemetry.csv'
```
