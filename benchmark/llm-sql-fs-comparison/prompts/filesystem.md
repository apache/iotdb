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

# Filesystem 访问模式

`iotdb_command` 的 command 必须是一条 IoTDB filesystem 命令。当前使用 table
模型：`/<database>` 表示库目录，`/<database>/<table>.csv` 表示远程数据表的
虚拟文件，元数据条目可能以 `.meta` 表示。它们不是本地文件；同表的元数据条目
不能当成另一张数据表。尖括号内容是语法占位符，提交命令时替换为当前任务已知
或实际发现的对象。

允许 `ls`、`schema`、`meta`、`cat`、`head`、`tail`、`count`、`stats`、
`find`、`tree`、`stat`、`file`、`help`。所有对象路径必须是任务指定库内的绝对
路径。每次调用当前目录重置为 `/`，不使用 `cd` 或相对路径。仅可用
`help <允许的命令名>` 获取帮助，不能请求总帮助或其他命令说明。

禁止 `sql` 子命令、管道、复合命令、重定向、`tail -f`、通用 shell 和本地文件
操作。不要在 command 中写启动 CLI 的脚本。fs 内部访问数据库的方式不需要你
选择；你只生成这里允许的命令。

## 对象发现与结构

- `ls -f csv /<database>` 列出虚拟条目；`find /<database> -name '<pattern>'`
  可按名字模式查找，支持 `-type f|d` 和 `-maxdepth <n>`；
  `tree -L <n> /<database>` 展示层次。
- `schema -f csv /<database>/<table>.csv` 返回列结构，包括
  `column/category/data_type`，以及可用的 encoding/compression 等属性。
  `time` 的类别为 TIME；其他类别以实际 schema 为准。
- `meta -f csv /<database>/<table>.csv` 返回表元数据；`stat`、`file` 可查看
  虚拟对象属性或类型。未提供的属性不能自行猜测。

## 读取、筛选与顺序

- `cat` 默认读取全部匹配数据行，可用 `-n <n>` 限制行数。
- `head -n <n>` 读取最前面的 n 条匹配数据记录，默认 n 为 10。
- `tail -n <n> --format csv` 读取最后 n 条匹配数据记录；结构化读取应显式
  使用 `--format table|ndjson|csv`。tail 的 `-f` 表示持续跟随，是禁用选项，
  **不能**用 `tail -f csv` 指定格式。不带查询选项的 tail 按 CSV 文本末尾行
  处理，不要混淆文本行与结构化记录。

三个读取命令都支持 `--start <ms>`、`--end <ms>`、`--offset <n>`、
`--tag-filter` 和 `-m <field>`。选择多个字段时重复 `-m`，每次只跟一个字段名。
cat/head 用 `-f table|ndjson|csv`
指定格式，tail 用 `--format`。时间边界包含端点，时间为毫秒整数。
`-m` 选择 FIELD 列，同时始终保留时间和全部 TAG 列。
在 table 模型中按设备选择记录应筛选相应 TAG；`-d` 是 tree 模型设备参数，
不用于此处的 table 设备筛选。

TAG 过滤语法是 `--tag-filter <tag> <operator> <value>`，支持
`eq`、`neq`、`regexp`；`is-null` 和 `not-null` 不带 value。
tag、operator、value 是分开的命令参数；含空格的 value 单独引用。
例如语法片段 `--tag-filter <tag> eq '<value>'`，**不要**将
`'<tag> eq <value>'` 整体包成一个参数。整个命令文本仍是工具的一个 JSON string。
正则匹配覆盖整个 TAG 值。多个筛选条件须重复 `--tag-filter`，并显式提供
`--tag-match all` 或 `--tag-match any`；只有一个条件时不加 `--tag-match`。
此处筛选只针对 TAG，不支持用同一选项比较数值 FIELD。

数据读取按 time 和所有 TAG 依次升序排列；tail 选择最后的匹配记录后仍按这个
升序输出。按任务需要自行整理最终顺序。cat/head 的 offset 从匹配记录开头跳过；
结构化 tail 的 offset 从匹配记录末尾跳过。数据行限额不包含额外输出的表头。

## 计数与统计

`count -f csv <绝对表路径>` 按 TAG/FIELD 列返回统计，每行包括
`column/category/row_count/entity_count/non_null_count/null_count/min_time/max_time`。
row_count 是整表数据记录数，在不同列的统计行中重复出现，不能把各行相加；
entity_count 是不同 TAG 组合数量。count 不返回 TIME 列统计行。
它不是 Unix `wc`；本实验不提供 wc，不能用文本行数或字节数代替数据行数。

`stats -f csv <绝对表路径>` 按每个 TAG 组合及 FIELD 返回统计，包含
`tag.<tag名>/field/data_type/non_null_count/null_count/min_time/max_time/`
`min/max/first/last/sum`。不是全表合并统计，**没有 AVG 字段**。
数值字段可根据 sum 和 non_null_count 计算平均值；合并多组时按数量加权，
不得简单平均各组均值。BOOLEAN 的 sum 是 true 数量，NULL 与 false 分开计数。
INT64/TIMESTAMP/DATE 的 sum 不提供；不要把不可用的统计量当零。

schema/count/stats 可用 `-m <column>` 选择统计输出；多列重复 `-m`，stats 仅选择 FIELD。
它们不支持 cat/head/tail 的时间过滤和 TAG 过滤选项；不要把这些选项直接迁移
到统计命令中。统计范围以实际命令及返回字段为准。

## 工具输出

table、CSV 或 NDJSON 是中间输出格式，最终仍需按任务 schema 返回一个 JSON。
CSV 包含表头；未加引号的 `\N` 表示 NULL，加引号的 `"\N"` 是字面字符串。
NDJSON 保留布尔和浮点类型，但 INT64/TIMESTAMP 会输出为字符串；最终答案要求
整数时自行转换为 JSON 数字。不要把空值与空字符串、false 与 0 混为一谈。
