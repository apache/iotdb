<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# TsFile-Cli 与 IoTDB-cli fs 命令对照

本文记录 `fs/inner-view` 分支当前 filesystem CLI 与 `tsfile-cli` 的命令级差异。TsFile-Cli
读取本地 `.tsfile`；IoTDB-cli fs 通过 JDBC 读取远程 IoTDB 的虚拟目录和对象。两者的启动
参数、交互提示符和数据来源因此继续保留差异。`write` 是本地文件操作，按 TsFile-Cli
的方式从 CSV 创建新 `.tsfile`，不通过 JDBC 写入远程表。

## 逐命令对照

| 命令 | TsFile-Cli | IoTDB-cli fs | 当前关系 |
| --- | --- | --- | --- |
| `ls` | 必须提供 `.tsfile`；支持 `-f table\|ndjson\|csv`；输出 `model,object` | 可省略路径；支持 `-a/-l/-R`；输出虚拟目录或文件名 | 数据对象和目录语义保留差异 |
| `schema` | 支持 `-d/-t`、重复 `-m`、`-f`；输出文件中的 schema | 按 table/tree provider 查询 IoTDB schema；支持裸表路径和 `.csv` 路径；默认制表符行输出 | 语义适配远程 IoTDB，参数面仍不同 |
| `meta` | 文件级 `size_bytes,format_version,model`，支持 `-f` | 查询表或 timeseries 的 IoTDB 元数据；支持裸表路径和 `.csv` 路径 | 元数据语义不同 |
| `stats` | 支持 `-d/-t`、重复 `-m`、`-f`；输出 field/tag 统计 | 按 IoTDB 聚合查询输出统计；当前只接受路径，默认制表符行输出 | 统计来源和参数面不同 |
| `count` | 支持 `-d/-t`、重复 `-m`、`-f`；输出逻辑行、实体、列和空值计数 | 按 IoTDB 聚合查询输出计数；当前只接受路径，默认制表符行输出 | 统计字段和参数面不同 |
| `sketch` | 输出 TsFile 物理布局文本，可写入 `-o` | 未实现 | IoTDB-cli 缺少该本地文件诊断命令 |
| `head` | 读取一个对象；默认 10 行；支持 scope、投影、时间范围、offset、tag filter 和格式 | 支持同名读取参数；路径指向 IoTDB 虚拟对象，文本 sidecar 保留文本读取行为 | 参数已基本对齐，查询后处理仍由 IoTDB shell 完成 |
| `cat` | 读取一个对象；默认不限制行数；支持 scope、投影、时间范围、offset、tag filter 和格式 | 可省略路径或连续读取多个路径；支持同名读取参数 | 多路径交互语义保留，查询后处理仍由 IoTDB shell 完成 |
| `export` | 按 `table\|ndjson\|csv` 导出到单文件或目录 | 未实现 | IoTDB-cli 缺少导出命令 |
| `write` | 按 schema 声明从 CSV/stdin 创建新的本地 table-model TsFile；支持按类型设置编码和压缩 | 相同参数及本地文件语义，支持严格 CSV 校验、失败清理和 `-v` 诊断；需要启用写模式 | 已实现；Java 写入器不支持 `LZO`、`BLOB + DICTIONARY`，这些配置明确报参数错误；边界差异见下文 |
| `wc -c` | 无同名命令 | 统计虚拟可读文件的 UTF-8 字节数，输出 `<bytes> <path>` | IoTDB-cli 的 Unix 兼容扩展 |
| `tail` | 无此命令 | 读取最后若干行或行数据，支持读取过滤参数 | IoTDB-cli 的 Unix 兼容扩展 |
| `pwd` | 无此命令 | 输出当前虚拟工作目录 | IoTDB-cli 的 Unix 兼容扩展 |
| `cd` | 无此命令 | 切换虚拟工作目录 | IoTDB-cli 的 Unix 兼容扩展 |
| `ll` | 无此命令 | `ls -l` 别名 | IoTDB-cli 的 Unix 兼容扩展 |
| `stat` | 无此命令 | 输出虚拟对象路径、类型和 provider 元数据 | IoTDB-cli 的 Unix 兼容扩展 |
| `grep` | 无此命令 | 对可读文本或数据行执行字面子串匹配 | IoTDB-cli 的 Unix 兼容扩展 |
| `find` | 无此命令 | 按名称递归查找虚拟目录项 | IoTDB-cli 的 Unix 兼容扩展 |
| `less` / `more` | 无此命令 | 非交互分页器；按默认限制输出可读内容 | IoTDB-cli 的 Unix 兼容扩展 |
| `file` | 无此命令 | 输出虚拟对象的 Unix 类型 | IoTDB-cli 的 Unix 兼容扩展 |
| `mkdir` / `rmdir` | 无此命令 | 写模式下创建或删除 table-model 数据库 | IoTDB-cli 的远程写入扩展 |
| `rm` | 无此命令 | 写模式下删除表或数据库 | IoTDB-cli 的远程写入扩展 |
| `mv` | 无此命令 | 写模式下重命名同一数据库中的表 | IoTDB-cli 的远程写入扩展 |
| `cp` | 无此命令 | 命令入口保留，但当前 table provider 返回“不支持的写操作” | IoTDB-cli 的预留写入扩展，当前不可用 |
| `cut` | 无此命令 | 按 Unix 分隔符和字段编号投影可读文本 | IoTDB-cli 的 Unix 兼容扩展 |
| `paste` | 无此命令 | 合并多个可读文件的对应行 | IoTDB-cli 的 Unix 兼容扩展 |
| `join` | 无此命令 | 按字段执行两个可读文件的 inner join | IoTDB-cli 的 Unix 兼容扩展 |
| `tee -a` | 无此命令 | 写模式下从 stdin 或交互输入向表 CSV 追加数据 | IoTDB-cli 的远程写入扩展 |
| `tree` | 无此命令 | 递归显示虚拟目录项 | IoTDB-cli 的 Unix 兼容扩展 |
| `sql` | 无此命令 | 在 filesystem 会话中保留 SQL 语句入口，执行仍受 fs 模式限制 | IoTDB-cli 的会话扩展 |
| `help` | 使用 `--help` 或 `<command> --help` | 使用 `help`、`help <command>` 和 `--help` | 帮助入口不同 |
| `exit` / `quit` | 进程命令，不属于 TsFile 文件操作命令 | 结束 fs 交互会话 | 会话控制命令 |

## 已明确的取舍

- `du` 已删除，不作为 IoTDB-cli fs 命令保留。
- `wc -c` 保留为文本兼容命令；它统计 UTF-8 字节数，不等同于 TsFile 的 `count` 或 `stats`。
- `.schema` 虚拟 sidecar 已删除；table 模式的 `.meta` sidecar 当前仍保留兼容行为，另有独立
  `meta` 命令。
- `count` 和 `stats` 是结构化统计命令，不能解释为真实磁盘空间统计。

## write 用法与行为

```text
write --table <name> (--tag <name> STRING)* (--field <name> <type>)+
      [--encoding <type> <encoding>] [--compression <type> <compression>]
      (-i/--input <input.csv> | --stdin) -o/--output <out.tsfile> [-v/--verbose]
```

例如，`input.csv` 内容为：

```csv
time,site,temperature
0,north,20.5
1,north,21.0
```

在 IoTDB 分发包目录执行：

```sh
./sbin/start-cli.sh --access_mode filesystem --fs_write_mode enabled \
  -e 'write --table sensors --tag site STRING --field temperature DOUBLE -i input.csv -o output.tsfile'
```

此批量命令在连接数据库前执行，无需服务端。`-i -` 与 `--stdin` 等价。输入、输出是
客户端本地路径，相对路径基于进程工作目录，与 fs 会话中的虚拟 `cd` 目录无关。
交互会话也支持 `write`；`--stdin` 读取 CSV 到 EOF，`-i` 读取指定本地文件。

- `--table` 可缩写为 `-t`；TAG 只能声明为 `STRING`，至少声明一个 FIELD，不推断类型。
- FIELD 类型支持 `BOOLEAN`、`INT32`、`INT64`、`FLOAT`、`DOUBLE`、`STRING`、`TEXT`、
  `TIMESTAMP`、`DATE`、`BLOB`。类型、编码和压缩名称使用大写规范名称。
- CSV 表头必须包含 `time` 和所有声明的列，不允许多列、缺列或重复列，顺序可不同。
  名称中的 ASCII 大写字母转为小写。Java 写入器会进一步改写部分非 ASCII 大写名称
  （例如 `Ä`）；为了保留声明的名称，当前对此类表名和列名明确报参数错误，中文等
  不发生大小写转换的 UTF-8 名称可正常使用。
- 未加引号的 `\N` 表示空值；字符串空单元格保留为空串；`"\N"` 是字面字符串。
  支持 CSV 引号、双引号转义、嵌入换行，以及文件开头的 UTF-8 BOM。
- `time` 使用严格十进制 int64，每个 TAG 组合内必须严格递增，不同设备可交错或使用
  相同时间戳。`DATE` 使用合法的 `YYYY-MM-DD`；BLOB 将输入文本编码为 UTF-8 字节。
- 输出目标必须不存在；失败时清理本次临时文件，已有文件保持原样。仅含表头的 CSV
  可以创建零行文件。原子发布要求目标文件系统支持硬链接。
- 成功时 stdout 静默，`-v` 向 stderr 输出行数和每列编码、压缩配置。默认编码与
  TsFile-Cli 一致：整数、日期和时间戳为 `TS_2DIFF`，浮点为 `GORILLA`，其余为 `PLAIN`；
  默认压缩为 `LZ4`。当前 Java 写入器不支持 `LZO` 压缩和 `BLOB + DICTIONARY` 编码。
- 退出码为 `0` 成功、`1` 参数错误、`2` CSV/输入错误、`3` 输出/运行错误。

CSV 字段解析复用 Commons CSV，拒绝结束引号后的非法尾随字符；C++ 当前实现对此较宽松。
日期按真实日历校验，接受合法的 1899 年日期；C++ 当前日期转换器会错误拒绝该年份。
这两处未复制 C++ 的边界缺陷。

`tee -a` 仍用于向远程 IoTDB 已有表追加数据；`write` 生成的本地文件不会自动加载进数据库。
