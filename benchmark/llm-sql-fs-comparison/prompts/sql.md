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

# SQL 访问模式

`iotdb_command` 的 command 必须是一条 IoTDB **table dialect** 只读 SQL。
允许 `SELECT`、`SHOW TABLES [DETAILS] FROM <database>`、
`DESC <database>.<table> [DETAILS]` 或对应的 `DESCRIBE`。
尖括号内容是语法占位符，提交命令时替换为当前任务已知或实际发现的对象。
末尾分号可省略；一次不能提交多条语句。

每个表引用都使用完整的 `<database>.<table>`，仅访问任务指定库。每次调用不保留
数据库选择状态，不使用 `USE`。不要调用 filesystem 命令。禁止写入、导出、
外部函数、跨库子查询和其他未列出的 SQL 语句。

## 对象发现与结构

- `SHOW TABLES FROM <database>` 返回该库的数据表；`DETAILS` 提供额外表属性。
- `DESC <database>.<table>` 返回列名、数据类型、列类别等结构信息；`DETAILS`
  可补充编码、压缩等可用属性。不存在或未提供的属性不能自行猜测。
- table 模型包含时间列、TAG 列、FIELD 列等类别。时间列名是 `time`；TAG
  组合标识实体。查询应根据当前任务公开或查询获得的 schema 选择列。

## 读取、筛选与顺序

`SELECT <expressions> FROM <database>.<table>` 可读取指定列或表达式。
可使用 `WHERE`、比较运算、`AND/OR`、`IN (...)`、`IS NULL/IS NOT NULL`，
以及 `ORDER BY ... ASC/DESC`、`LIMIT`、`OFFSET`。

时间条件使用毫秒整数，闭区间使用包含端点的条件。字符串字面量使用单引号，
其中的单引号用两个单引号表示。布尔值为 `true` 或 `false`。
不写 `ORDER BY` 时不要假定返回顺序；仅按时间排序时，同时间的多条记录可能
仍需按 TAG 排序。根据任务指定的顺序选择稳定排序，分页时保持一致。

## 聚合与分组

可使用 `COUNT(*)`、`COUNT(<column>)`、`COUNT(DISTINCT <column>)`、
`MIN/MAX/SUM/AVG`、`GROUP BY` 以及条件表达式 `CASE WHEN ... THEN ... ELSE ... END`。
可给列或表达式加别名，便于识别返回值。分组列与聚合范围须符合任务要求。

`COUNT(*)` 是行数；`COUNT(column)` 只计算该列非空值。数值聚合通常忽略 NULL，
NULL 不等于零，也不能用 `= NULL` 筛选。布尔字段不要直接当数值字段求和；
根据所需语义使用筛选或条件表达式。涉及平均值时，应使用对应非空值数量，
不得将不等大小分组的平均值简单平均。

## 工具输出

CLI 返回原生表格、列标题和可能的执行说明；先识别实际数据行。
时间已配置为毫秒显示。依据列类型将结果转换为任务指定 JSON，保留 NULL 与空
字符串、false 与 0 的区别；不要把客户端显示的数字字符串原样当成最终数值类型。
