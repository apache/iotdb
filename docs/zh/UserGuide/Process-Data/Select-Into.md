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

# 查询写回（SELECT INTO）

`SELECT INTO` 语句用于将查询结果写入一系列指定的时间序列中。

应用场景如下：
- **实现 IoTDB 内部 ETL**：对原始数据进行 ETL 处理后写入新序列。
- **查询结果存储**：将查询结果进行持久化存储，起到类似物化视图的作用。
- **非对齐序列转对齐序列**：对齐序列从0.13版本开始支持，可以通过该功能将历史的非对齐序列重新写成对齐序列。

## 语法定义

### 整体描述

```sql
selectIntoStatement
    : SELECT
        resultColumn [, resultColumn] ...
        INTO intoItem [, intoItem] ...
        FROM prefixPath [, prefixPath] ...
        [WHERE whereCondition]
        [GROUP BY groupByTimeClause, groupByLevelClause]
        [FILL {PREVIOUS | LINEAR | constant}]
        [LIMIT rowLimit OFFSET rowOffset]
        [ALIGN BY DEVICE]
    ;

intoItem
    : [ALIGNED] intoDevicePath '(' intoMeasurementName [',' intoMeasurementName]* ')'
    ;
```

### `INTO` 子句

`INTO` 子句由若干个 `intoItem` 构成。

每个 `intoItem` 由一个目标设备路径和一个包含若干目标物理量名的列表组成（与 `INSERT` 语句中的 `INTO` 子句写法类似）。

其中每个目标物理量名与目标设备路径组成一个目标序列，一个 `intoItem` 包含若干目标序列。例如：`root.sg_copy.d1(s1, s2)` 指定了两条目标序列 `root.sg_copy.d1.s1` 和 `root.sg_copy.d1.s2`。

`INTO` 子句指定的目标序列要能够与查询结果集的列一一对应。具体规则如下：

- **按时间对齐**（默认）：全部 `intoItem` 包含的目标序列数量要与查询结果集的列数（除时间列外）一致，且按照表头从左到右的顺序一一对应。
- **按设备对齐**（使用 `ALIGN BY DEVICE`）：全部 `intoItem` 中指定的目标设备数和查询的设备数（即 `FROM` 子句中路径模式匹配的设备数）一致，且按照结果集设备的输出顺序一一对应。
  为每个目标设备指定的目标物理量数量要与查询结果集的列数（除时间和设备列外）一致，且按照表头从左到右的顺序一一对应。

下面通过示例进一步说明：

- **示例 1**（按时间对齐）：

- **示例 2**（按时间对齐）：

- **示例 3**（按设备对齐）：

- **示例 4**（按设备对齐）：

### 使用变量占位符

特别地，可以使用变量占位符描述目标序列与查询序列之间的对应规律，简化语句书写。目前支持以下两种变量占位符：
 
- 后缀复制符 `::`：复制查询设备后缀（或物理量），表示从该层开始一直到设备的最后一层（或物理量），目标设备的节点名（或物理量名）与查询的设备对应的节点名（或物理量名）相同。
- 单层节点匹配符 `${i}`：表示目标序列当前层节点名与查询序列的第`i`层节点名相同。比如，对于路径`root.sg1.d1.s1`而言，`${1}`表示`sg1`，`${2}`表示`d1`，`${3}`表示`s1`。

注意：变量占位符**只能描述序列与序列之间的对应关系**，不能用于函数、表达式等。

在使用变量占位符时，`intoItem`与查询结果集列的对应关系不能存在歧义，具体情况分类讨论如下：

- **按时间对齐**（默认）
    > 注：如果查询中包含聚合、表达式计算，此时查询结果中的列无法与某个序列对应，因此目标设备和目标物理量都不能使用变量占位符。 
- **按设备对齐**（使用 `ALIGN BY DEVICE`）
    > 注：如果查询中包含聚合、表达式计算，此时查询结果中的列无法与某个物理量对应，因此目标物理量不能使用变量占位符。

### 指定目标序列为对齐序列

通过 `ALIGNED` 关键词可以指定写入的目标设备为对齐写入，每个 `intoItem` 可以独立设置。

### 不支持使用的查询子句

- `SLIMIT`、`SOFFSET`：查询出来的列不确定，功能不清晰，因此不支持。
- `LAST`、`GROUP BY TAGS`、`DISABLE ALIGN`：表结构和写入结构不一致，因此不支持。

### 其他要注意的点

- 对于一般的聚合查询，时间戳是无意义的，约定使用 0 来存储。
- 当目标序列存在时，需要保证源序列和目标时间序列的数据类型、压缩和编码方式、是否属于对齐设备等元数据信息一致。
- 当目标序列不存在时，系统将自动创建目标序列（包括存储组）。
- 当查询的序列不存在或查询的序列不存在数据，则不会自动创建目标序列。

## 应用举例

### 查询结果存储
将查询结果进行持久化存储，起到类似物化视图的作用。

### 非对齐序列转对齐序列
对齐序列从 0.13 版本开始支持，可以通过该功能将历史的非对齐序列重新写成对齐序列。

**注意：** 建议配合使用 `LIMIT & OFFSET` 子句或 `WHERE` 子句（时间过滤条件）对数据进行分批，防止单次操作的数据量过大。

## 相关用户权限

用户必须有下列权限才能正常执行查询写回语句：

* 所有 `SELECT` 子句中源序列的 `READ_TIMESERIES` 权限。
* 所有 `INTO` 子句中目标序列 `INSERT_TIMESERIES` 权限。

更多用户权限相关的内容，请参考[权限管理语句](../Administration-Management/Administration.md)。

## 相关配置参数

* `select_into_insert_tablet_plan_row_limit`

  | 参数名 | select_into_insert_tablet_plan_row_limit |
  | ---- | ---- | 
  | 描述 | 写入过程中每一批 `Tablet` 的最大行数 | 
  | 类型 | int32 | 
  | 默认值 | 10000 | 
  | 改后生效方式 | 重启后生效 | 
