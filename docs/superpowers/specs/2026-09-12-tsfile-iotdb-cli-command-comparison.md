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
| `ls` | 必须提供 `.tsfile`；支持 `-f table\|ndjson\|csv`；输出 `model,object` | 列虚拟目录；支持 `-a/-l/-R` 和显式 `-f` 结构化输出 | 对象模型保留差异；不伪造权限、所有者和磁盘大小 |
| `schema` | 支持 `-d/-t`、重复 `-m`、`-f`；不指定 scope 时遍历文件对象 | 支持对应参数，查询虚拟路径指定的对象，输出 `model,object,column,category,data_type,encoding,compression` | 虚拟目录不等于多对象文件；表模型不公开编码和压缩时输出空值，树模型返回真实 schema |
| `meta` | 文件级 `size_bytes,format_version,model`，支持 `-f` | 支持 `-f`，查询远程表或 timeseries 元数据 | 对象不同，不能将远程表冒充单个物理文件 |
| `stats` | 按设备、FIELD 和数据类型输出统计，不指定 scope 时遍历文件对象 | 对路径选定对象按 TAG 组合分组扫描真实数据，支持 `-d/-t/-m/-f`，按类型处理聚合 | 输出采用对应类型规则；来源标注 `scan`，不声称使用文件物理统计 |
| `count` | 输出逻辑行、实体、列、空值计数及时间边界 | 真实去重设备计数、非空/空值计数、类别和时间边界；支持 `-d/-t/-m/-f` | 表实体数去重；树实体数按 TsFile 输出空值 |
| `sketch` | 当前本地 C++ 实现输出 Sketch 首尾、文件路径和模型，可写入 `-o` | 验证客户端本地 TsFile，输出相同内容；支持 `-o/--force`，可离线运行 | 按当前源码对齐，不将现有简化输出描述为完整物理布局 |
| `head` | 默认 10 个数据行；支持 scope、投影、时间、offset、TAG 过滤和格式 | 裸表及 `.csv` 走同一结构化读取流程；表头不占行数 | 无效对象、FIELD、TAG、正则表达式报错 |
| `cat` | 默认全部匹配行；支持与 head 相同的读取参数 | 默认不限行；支持多个路径；保留 TIME 和所有 TAG，按 schema 输出类型 | 多路径为扩展；空结果仍输出 CSV/table 表头 |
| `export` | 按 `table\|ndjson\|csv` 导出到单文件或目录 | 与 cat 共用查询和输出；支持重复对象选择、单文件 `-o`、`--force`、新目录及 `_manifest.json` | 本地输出，默认防覆盖；失败清单标记未完成 |
| `write` | 按 schema 声明从 CSV/stdin 创建新的本地 table-model TsFile；支持按类型设置编码和压缩 | 相同参数及本地文件语义，支持严格 CSV 校验、失败清理和 `-v` 诊断；需要启用写模式 | 已实现；Java 写入器不支持 `LZO`、`BLOB + DICTIONARY`，这些配置明确报参数错误；边界差异见下文 |
| `wc -c` | 无同名命令 | 统计规范 CSV 文本或 stdin 的 UTF-8 字节数；支持多个输入及总计 | 不增加 `wc -l`，不代表磁盘大小 |
| `tail` | 无此命令 | 支持 `-n/-c/+N/-f`；独立的 `-f` 命令轮询跟随，结构化读取使用 `--format` | `-f` 不支持管道或重定向；`-f/-c/+N` 不能与结构化查询选项组合 |
| `pwd` | 无此命令 | 输出当前虚拟工作目录 | IoTDB-cli 的 Unix 兼容扩展 |
| `cd` | 无此命令 | 无参数回到虚拟 HOME `/`，`cd -` 返回上一目录 | HOME 为虚拟根，非客户端用户主目录 |
| `ll` | 无此命令 | `ls -l` 别名 | IoTDB-cli 的 Unix 兼容扩展 |
| `stat` | 无此命令 | 输出虚拟对象路径、类型、可读取内容的字节数和 provider 元数据 | 字节数是生成文本大小，不是磁盘占用 |
| `grep` | 无同名命令 | 全量匹配，支持基本正则及 `-F/-E/-i/-v/-n`，无匹配返回 1 | 使用 Java 正则实现常用 BRE/ERE；不承诺完整 POSIX 正则语法及 locale 行为，不提供 `-c/-l/-q` |
| `find` | 无同名命令 | `-name` 支持通配模式，另支持 `-type/-maxdepth` | 常用功能子集，不实现 `-exec` |
| `less` / `more` | 无同名命令 | 交互终端使用 JLine 分页、搜索；批量输出完整文本 | 无 20 行截断 |
| `file` | 无同名命令 | 输出目录、CSV 数据或 CSV 元数据类型 | 对象是生成文本，不进行宿主系统文件 magic 检测 |
| `mkdir` / `rmdir` | 无同名命令 | 创建数据库；`-p` 容忍已存在目录；rmdir 拒绝非空数据库；支持多路径 | 数据库层级固定；Unix 权限模式明确不支持 |
| `rm` | 无同名命令 | 删除表或数据库，支持多路径、`-r/-f/-i` | 数据库对象操作，非任意目录递归 |
| `mv` | 无同名命令 | 同库重命名、跨库复制成功后删源；支持覆盖、`-n/-i` 和多源目标目录 | 树模型写入仍不支持 |
| `cp` | 无同名命令 | 复制表结构和数据；支持目标数据库、覆盖、`-n/-i` 和多源 | 失败清理新目标；树模型写入仍不支持 |
| `cut` | 无同名命令 | 全量输入，支持 `-f/-b/-c/-s`、开放范围及空字段 | 按文本分隔符处理，不作为 CSV 字段解析器 |
| `paste` | 无同名命令 | 全量对应行合并，支持 `-d/-s` 和 stdin | 无 20 行截断 |
| `join` | 无同名命令 | 全量连接，验证输入排序，支持 `-t/-1/-2/-a/-v/-e/-o` | 使用 Java 字符串顺序；CSV 表头也参与排序；`-a/-v` 各接受一次，不支持任意 Unix 选项集合 |
| `tee` | 无同名命令 | stdin 到 EOF，原样回显；默认替换目标表内容，`-a` 追加，支持多个目标 | 远程目标须有 schema；交互不再使用 `:wq/:q!` |
| `tree` | 无同名命令 | ASCII 分支、根目录和目录/文件计数，支持深度限制 | 常用扩展，本身非 POSIX 标准命令 |
| `sql` | 无同名命令 | 执行服务端 SQL；只读模式限制为保守识别的读取语句，其他语句要求写开关 | 需要连接；不再只解析后报“不支持” |
| `help` | 支持 `-h/--help` 和 `<command> -h/--help` | 支持这些帮助参数，另有 `help <command>` | 增加便捷入口 |
| `exit` / `quit` | 进程命令，不属于 TsFile 文件操作命令 | `exit [status]` 退出，quit 为别名 | 会话控制 |

## 已明确的取舍

- `du` 已删除，不作为 IoTDB-cli fs 命令保留。
- `wc -c` 保留为文本兼容命令；它统计 UTF-8 字节数，不等同于 TsFile 的 `count` 或 `stats`。
- `.schema` 虚拟 sidecar 已删除；table 模式的 `.meta` sidecar 当前仍保留兼容行为，另有独立
  `meta` 命令。
- `count` 和 `stats` 是结构化统计命令，不能解释为真实磁盘空间统计。

## 修复后的行为边界

- fs 会话支持基础管道 `|`、本地文本重定向 `<`、`>`、`>>` 和虚拟路径通配展开。引号内或
  反斜线转义的通配符保持字面含义；同一参数内未引用的通配符仍可展开，重复参数分别处理。
  单引号中的正则反斜线原样保留。这是面向数据库的命令会话，不实现完整 POSIX shell 语言。
- 管道按阶段完整读取再执行下一阶段，不是流式管道。`tail -f` 与管道、输入或输出重定向
  的组合明确报错，避免无限缓存和无法进入后续命令。普通 `tail` 可用于管道。
- 重定向目标只接受一个路径；输入重定向位于首个阶段，输出重定向位于最后阶段。
  语法错误、未闭合的引号和空目标均报参数错误。`sql` 后的文本整体交给 SQL 解析器，
  其中的比较符和字符串不当作 fs 管道或重定向。
- 文本命令的虚拟数据文件统一采用 UTF-8 CSV 表示；`wc -c` 与相同对象的 CSV 字节表示一致。
  `cat/head` 读取 stdin 时保留原始换行和未换行的末行，不额外增加字节；`.meta` 保留原始
  CSV 文本，可用行数/偏移截取，拒绝数据列、设备和时间过滤；纯文本 `tail` 拒绝 `--offset`。
  数据读取和统计目前沿用 provider 的列表结果接口，会在客户端保存结果，不是无界流式处理。
- `schema/stats/count` 的省略路径默认值是当前虚拟目录，但目录本身不必是可查询对象；
  表模型应指定 `/database/table.csv`、裸表路径，或在数据库路径上使用 `-t table`。
  这与 TsFile-Cli 在一个文件内自动遍历所有对象的行为仍有差异。
- `tail` 未设置查询选项时按完整 CSV 文本处理，表头与其他文本行同样参与计数；带
  `--format/-d/-t/-m/--start/--end/--offset/--tag-filter` 时按结构化数据行处理。
  `grep/cut/paste/join` 是文本工具，不理解 CSV 引号或多行记录的逻辑边界。
- `stats` 按类型处理：BOOLEAN 的 sum 为 true 数量；INT64/TIMESTAMP 不输出可能丢精度的
  sum；STRING/TEXT/DATE/BLOB 按 TsFile 对应类型的统计能力返回值或空值。
- 远程覆盖采用临时表填充、旧表备份、重命名发布及失败回滚。IoTDB 没有用于这些操作的
  多语句事务，并发读取可能观察到短暂的名称切换；跨库移动也不是跨库事务。
- 数据库虚拟对象没有 Unix inode、用户/组、文件权限及文件修改时间。未知属性不得伪造；
  `stat` 展示的是虚拟内容和服务端可提供的元数据。
- `write` 的 Java 库能力边界继续以下文为准：不引入不兼容编码实现，也不复制 C++ 的已知
  CSV/日期缺陷。所有不支持的配置必须明确失败，不能静默降级。

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

## 本轮验证

- CLI 模块完整测试：387 项，失败 0、错误 0、跳过 0；默认 Spotless 和 Checkstyle 检查通过。
- 中文资源编译通过；新增读取、统计、变更回滚、文本工具、引号转义、管道和本地文件用例。
- 尚未连接真实 IoTDB 服务端执行集成测试；远程 SQL 和失败回滚由 provider/JDBC 测试覆盖。
