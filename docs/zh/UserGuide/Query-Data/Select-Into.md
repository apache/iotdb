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

## 查询写回（SELECT INTO）

`SELECT INTO` 语句用于将查询结果写入一系列指定的时间序列中。

应用场景如下：
- **实现 IoTDB 内部 ETL**：对原始数据进行 ETL 处理后写入新序列。
- **查询结果存储**：将查询结果进行持久化存储，起到类似物化视图的作用。
- **非对齐序列转对齐序列**：对齐序列从0.13版本开始支持，可以通过该功能将非对齐序列的数据写入新的对齐序列中。

### 语法定义

#### 整体描述

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

#### `INTO` 子句

`INTO` 子句由若干个 `intoItem` 构成。

每个 `intoItem` 由一个目标设备路径和一个包含若干目标物理量名的列表组成（与 `INSERT` 语句中的 `INTO` 子句写法类似）。

其中每个目标物理量名与目标设备路径组成一个目标序列，一个 `intoItem` 包含若干目标序列。例如：`root.sg_copy.d1(s1, s2)` 指定了两条目标序列 `root.sg_copy.d1.s1` 和 `root.sg_copy.d1.s2`。

`INTO` 子句指定的目标序列要能够与查询结果集的列一一对应。具体规则如下：

- **按时间对齐**（默认）：全部 `intoItem` 包含的目标序列数量要与查询结果集的列数（除时间列外）一致，且按照表头从左到右的顺序一一对应。
- **按设备对齐**（使用 `ALIGN BY DEVICE`）：全部 `intoItem` 中指定的目标设备数和查询的设备数（即 `FROM` 子句中路径模式匹配的设备数）一致，且按照结果集设备的输出顺序一一对应。
  为每个目标设备指定的目标物理量数量要与查询结果集的列数（除时间和设备列外）一致，且按照表头从左到右的顺序一一对应。

下面通过示例进一步说明：

- **示例 1**（按时间对齐）
```shell
IoTDB> select s1, s2 into root.sg_copy.d1(t1), root.sg_copy.d2(t1, t2), root.sg_copy.d1(t2) from root.sg.d1, root.sg.d2;
+--------------+-------------------+--------+
| source column|  target timeseries| written|
+--------------+-------------------+--------+
| root.sg.d1.s1| root.sg_copy.d1.t1|    8000|
+--------------+-------------------+--------+
| root.sg.d2.s1| root.sg_copy.d2.t1|   10000|
+--------------+-------------------+--------+
| root.sg.d1.s2| root.sg_copy.d2.t2|   12000|
+--------------+-------------------+--------+
| root.sg.d2.s2| root.sg_copy.d1.t2|   10000|
+--------------+-------------------+--------+
Total line number = 4
It costs 0.725s
```

该语句将 `root.sg` database 下四条序列的查询结果写入到 `root.sg_copy` database 下指定的四条序列中。注意，`root.sg_copy.d2(t1, t2)` 也可以写做 `root.sg_copy.d2(t1), root.sg_copy.d2(t2)`。

可以看到，`INTO` 子句的写法非常灵活，只要满足组合出的目标序列没有重复，且与查询结果列一一对应即可。

> `CLI` 展示的结果集中，各列的含义如下：
> - `source column` 列表示查询结果的列名。
> - `target timeseries` 表示对应列写入的目标序列。
> - `written` 表示预期写入的数据量。

- **示例 2**（按时间对齐）
```shell
IoTDB> select count(s1 + s2), last_value(s2) into root.agg.count(s1_add_s2), root.agg.last_value(s2) from root.sg.d1 group by ([0, 100), 10ms);
+--------------------------------------+-------------------------+--------+
|                         source column|        target timeseries| written|
+--------------------------------------+-------------------------+--------+
|  count(root.sg.d1.s1 + root.sg.d1.s2)| root.agg.count.s1_add_s2|      10|
+--------------------------------------+-------------------------+--------+
|             last_value(root.sg.d1.s2)|   root.agg.last_value.s2|      10|
+--------------------------------------+-------------------------+--------+
Total line number = 2
It costs 0.375s
```

该语句将聚合查询的结果存储到指定序列中。

- **示例 3**（按设备对齐）
```shell
IoTDB> select s1, s2 into root.sg_copy.d1(t1, t2), root.sg_copy.d2(t1, t2) from root.sg.d1, root.sg.d2 align by device;
+--------------+--------------+-------------------+--------+
| source device| source column|  target timeseries| written|
+--------------+--------------+-------------------+--------+
|    root.sg.d1|            s1| root.sg_copy.d1.t1|    8000|
+--------------+--------------+-------------------+--------+
|    root.sg.d1|            s2| root.sg_copy.d1.t2|   11000|
+--------------+--------------+-------------------+--------+
|    root.sg.d2|            s1| root.sg_copy.d2.t1|   12000|
+--------------+--------------+-------------------+--------+
|    root.sg.d2|            s2| root.sg_copy.d2.t2|    9000|
+--------------+--------------+-------------------+--------+
Total line number = 4
It costs 0.625s
```

该语句同样是将 `root.sg` database 下四条序列的查询结果写入到 `root.sg_copy` database 下指定的四条序列中。但在按设备对齐中，`intoItem` 的数量必须和查询的设备数量一致，每个查询设备对应一个 `intoItem`。

> 按设备对齐查询时，`CLI` 展示的结果集多出一列 `source device` 列表示查询的设备。

- **示例 4**（按设备对齐）
```shell
IoTDB> select s1 + s2 into root.expr.add(d1s1_d1s2), root.expr.add(d2s1_d2s2) from root.sg.d1, root.sg.d2 align by device;
+--------------+--------------+------------------------+--------+
| source device| source column|       target timeseries| written|
+--------------+--------------+------------------------+--------+
|    root.sg.d1|       s1 + s2| root.expr.add.d1s1_d1s2|   10000|
+--------------+--------------+------------------------+--------+
|    root.sg.d2|       s1 + s2| root.expr.add.d2s1_d2s2|   10000|
+--------------+--------------+------------------------+--------+
Total line number = 2
It costs 0.532s
```

该语句将表达式计算的结果存储到指定序列中。

#### 使用变量占位符

特别地，可以使用变量占位符描述目标序列与查询序列之间的对应规律，简化语句书写。目前支持以下两种变量占位符：
 
- 后缀复制符 `::`：复制查询设备后缀（或物理量），表示从该层开始一直到设备的最后一层（或物理量），目标设备的节点名（或物理量名）与查询的设备对应的节点名（或物理量名）相同。
- 单层节点匹配符 `${i}`：表示目标序列当前层节点名与查询序列的第`i`层节点名相同。比如，对于路径`root.sg1.d1.s1`而言，`${1}`表示`sg1`，`${2}`表示`d1`，`${3}`表示`s1`。

在使用变量占位符时，`intoItem`与查询结果集列的对应关系不能存在歧义，具体情况分类讨论如下：

##### 按时间对齐（默认）

> 注：变量占位符**只能描述序列与序列之间的对应关系**，如果查询中包含聚合、表达式计算，此时查询结果中的列无法与某个序列对应，因此目标设备和目标物理量都不能使用变量占位符。 

###### （1）目标设备不使用变量占位符 & 目标物理量列表使用变量占位符
  
**限制：**
   1. 每个 `intoItem` 中，物理量列表的长度必须为 1。<br>（如果长度可以大于1，例如 `root.sg1.d1(::, s1)`，无法确定具体哪些列与`::`匹配）
   2. `intoItem` 数量为 1，或与查询结果集列数一致。<br>（在每个目标物理量列表长度均为 1 的情况下，若 `intoItem` 只有 1 个，此时表示全部查询序列写入相同设备；若 `intoItem` 数量与查询序列一致，则表示为每个查询序列指定一个目标设备；若 `intoItem` 大于 1 小于查询序列数，此时无法与查询序列一一对应）

**匹配方法：** 每个查询序列指定目标设备，而目标物理量根据变量占位符生成。

**示例：**

```sql
select s1, s2
into root.sg_copy.d1(::), root.sg_copy.d2(s1), root.sg_copy.d1(${3}), root.sg_copy.d2(::)
from root.sg.d1, root.sg.d2;
```
该语句等价于：
```sql
select s1, s2
into root.sg_copy.d1(s1), root.sg_copy.d2(s1), root.sg_copy.d1(s2), root.sg_copy.d2(s2)
from root.sg.d1, root.sg.d2;
```
可以看到，在这种情况下，语句并不能得到很好地简化。

###### （2）目标设备使用变量占位符 & 目标物理量列表不使用变量占位符

**限制：** 全部 `intoItem` 中目标物理量的数量与查询结果集列数一致。

**匹配方式：** 为每个查询序列指定了目标物理量，目标设备根据对应目标物理量所在 `intoItem` 的目标设备占位符生成。

**示例：**
```sql
select d1.s1, d1.s2, d2.s3, d3.s4 
into ::(s1_1, s2_2), root.sg.d2_2(s3_3), root.${2}_copy.::(s4)
from root.sg;
```

###### （3）目标设备使用变量占位符 & 目标物理量列表使用变量占位符

**限制：** `intoItem` 只有一个且物理量列表的长度为 1。

**匹配方式：** 每个查询序列根据变量占位符可以得到一个目标序列。

**示例：**
```sql
select * into root.sg_bk.::(::) from root.sg.**;
```
将 `root.sg` 下全部序列的查询结果写到 `root.sg_bk`，设备名后缀和物理量名保持不变。

##### 按设备对齐（使用 `ALIGN BY DEVICE`）

> 注：变量占位符**只能描述序列与序列之间的对应关系**，如果查询中包含聚合、表达式计算，此时查询结果中的列无法与某个物理量对应，因此目标物理量不能使用变量占位符。

###### （1）目标设备不使用变量占位符 & 目标物理量列表使用变量占位符

**限制：** 每个 `intoItem` 中，如果物理量列表使用了变量占位符，则列表的长度必须为 1。

**匹配方法：** 每个查询序列指定目标设备，而目标物理量根据变量占位符生成。

**示例：**
```sql
select s1, s2, s3, s4
into root.backup_sg.d1(s1, s2, s3, s4), root.backup_sg.d2(::), root.sg.d3(backup_${4})
from root.sg.d1, root.sg.d2, root.sg.d3
align by device;
```

###### （2）目标设备使用变量占位符 & 目标物理量列表不使用变量占位符

**限制：** `intoItem` 只有一个。（如果出现多个带占位符的 `intoItem`，我们将无法得知每个 `intoItem` 需要匹配哪几个源设备）

**匹配方式：** 每个查询设备根据变量占位符得到一个目标设备，每个设备下结果集各列写入的目标物理量由目标物理量列表指定。

**示例：**
```sql
select avg(s1), sum(s2) + sum(s3), count(s4)
into root.agg_${2}.::(avg_s1, sum_s2_add_s3, count_s4)
from root.**
align by device;
```

###### （3）目标设备使用变量占位符 & 目标物理量列表使用变量占位符

**限制：** `intoItem` 只有一个且物理量列表的长度为 1。

**匹配方式：** 每个查询序列根据变量占位符可以得到一个目标序列。

**示例：**
```sql
select * into ::(backup_${4}) from root.sg.** align by device;
```
将 `root.sg` 下每条序列的查询结果写到相同设备下，物理量名前加`backup_`。

#### 指定目标序列为对齐序列

通过 `ALIGNED` 关键词可以指定写入的目标设备为对齐写入，每个 `intoItem` 可以独立设置。

**示例：**
```sql
select s1, s2 into root.sg_copy.d1(t1, t2), aligned root.sg_copy.d2(t1, t2) from root.sg.d1, root.sg.d2 align by device;
```
该语句指定了 `root.sg_copy.d1` 是非对齐设备，`root.sg_copy.d2`是对齐设备。

#### 不支持使用的查询子句

- `SLIMIT`、`SOFFSET`：查询出来的列不确定，功能不清晰，因此不支持。
- `LAST`查询、`GROUP BY TAGS`、`DISABLE ALIGN`：表结构和写入结构不一致，因此不支持。

#### 其他要注意的点

- 对于一般的聚合查询，时间戳是无意义的，约定使用 0 来存储。
- 当目标序列存在时，需要保证源序列和目标时间序列的数据类型兼容。关于数据类型的兼容性，查看文档 [数据类型](../Data-Concept/Data-Type.md#数据类型兼容性)。
- 当目标序列不存在时，系统将自动创建目标序列（包括 database）。
- 当查询的序列不存在或查询的序列不存在数据，则不会自动创建目标序列。

### 应用举例
  
#### 实现 IoTDB 内部 ETL
对原始数据进行 ETL 处理后写入新序列。
```shell
IOTDB > SELECT preprocess_udf(s1, s2) INTO ::(preprocessed_s1, preprocessed_s2) FROM root.sg.* ALIGN BY DEIVCE;
+--------------+-------------------+---------------------------+--------+
| source device|      source column|          target timeseries| written|
+--------------+-------------------+---------------------------+--------+
|    root.sg.d1| preprocess_udf(s1)| root.sg.d1.preprocessed_s1|    8000|
+--------------+-------------------+---------------------------+--------+
|    root.sg.d1| preprocess_udf(s2)| root.sg.d1.preprocessed_s2|   10000|
+--------------+-------------------+---------------------------+--------+
|    root.sg.d2| preprocess_udf(s1)| root.sg.d2.preprocessed_s1|   11000|
+--------------+-------------------+---------------------------+--------+
|    root.sg.d2| preprocess_udf(s2)| root.sg.d2.preprocessed_s2|    9000|
+--------------+-------------------+---------------------------+--------+
```
以上语句使用自定义函数对数据进行预处理，将预处理后的结果持久化存储到新序列中。

#### 查询结果存储
将查询结果进行持久化存储，起到类似物化视图的作用。
```shell
IOTDB > SELECT count(s1), last_value(s1) INTO root.sg.agg_${2}(count_s1, last_value_s1) FROM root.sg1.d1 GROUP BY ([0, 10000), 10ms);
+--------------------------+-----------------------------+--------+
|             source column|            target timeseries| written|
+--------------------------+-----------------------------+--------+
|      count(root.sg.d1.s1)|      root.sg.agg_d1.count_s1|    1000|
+--------------------------+-----------------------------+--------+
| last_value(root.sg.d1.s2)| root.sg.agg_d1.last_value_s2|    1000|
+--------------------------+-----------------------------+--------+
Total line number = 2
It costs 0.115s
```
以上语句将降采样查询的结果持久化存储到新序列中。

#### 非对齐序列转对齐序列
对齐序列从 0.13 版本开始支持，可以通过该功能将非对齐序列的数据写入新的对齐序列中。

**注意：** 建议配合使用 `LIMIT & OFFSET` 子句或 `WHERE` 子句（时间过滤条件）对数据进行分批，防止单次操作的数据量过大。

```shell
IOTDB > SELECT s1, s2 INTO ALIGNED root.sg1.aligned_d(s1, s2) FROM root.sg1.non_aligned_d WHERE time >= 0 and time < 10000;
+--------------------------+----------------------+--------+
|             source column|     target timeseries| written|
+--------------------------+----------------------+--------+
| root.sg1.non_aligned_d.s1| root.sg1.aligned_d.s1|   10000|
+--------------------------+----------------------+--------+
| root.sg1.non_aligned_d.s2| root.sg1.aligned_d.s2|   10000|
+--------------------------+----------------------+--------+
Total line number = 2
It costs 0.375s
```
以上语句将一组非对齐的序列的数据迁移到一组对齐序列。

### 相关用户权限

用户必须有下列权限才能正常执行查询写回语句：

* 所有 `SELECT` 子句中源序列的 `READ_TIMESERIES` 权限。
* 所有 `INTO` 子句中目标序列 `INSERT_TIMESERIES` 权限。

更多用户权限相关的内容，请参考[权限管理语句](../Administration-Management/Administration.md)。

### 相关配置参数

* `select_into_insert_tablet_plan_row_limit`

  | 参数名 | select_into_insert_tablet_plan_row_limit |
  | ---- | ---- | 
  | 描述 | 写入过程中每一批 `Tablet` 的最大行数 | 
  | 类型 | int32 | 
  | 默认值 | 10000 | 
  | 改后生效方式 | 重启后生效 | 
