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

# 概述

在 IoTDB 中，使用 `SELECT` 语句从一条或多条时间序列中查询数据。

## 语法定义

```sql
SELECT [LAST] selectExpr [, selectExpr] ...
    [INTO intoItem [, intoItem] ...]
    FROM prefixPath [, prefixPath] ...
    [WHERE whereCondition]
    [GROUP BY {
        ([startTime, endTime), interval [, slidingStep]) |
        LEVEL = levelNum [, levelNum] ... |
        TAGS(tagKey [, tagKey] ... )
    }]
    [HAVING havingCondition]
    [ORDER BY sortKey {ASC | DESC}]
    [FILL ({PREVIOUS | LINEAR | constant})]
    [SLIMIT seriesLimit] [SOFFSET seriesOffset]
    [LIMIT rowLimit] [OFFSET rowOffset]
    [ALIGN BY {TIME | DEVICE}]
```

## 语法说明

### `SELECT` 子句

- `SELECT` 子句指定查询的输出，由若干个 `selectExpr` 组成。
- 每个 `selectExpr` 定义查询结果中的一列或多列，它是一个由时间序列路径后缀、常量、函数和运算符组成的表达式。
- 支持使用`AS`为查询结果集中的列指定别名。
- 在 `SELECT` 子句中使用 `LAST` 关键词可以指定查询为最新点查询，详细说明及示例见文档 [最新点查询](./Last-Query.md) 。
- 详细说明及示例见文档 [选择表达式](./Select-Expression.md) 。

### `INTO` 子句

- `SELECT INTO` 用于将查询结果写入一系列指定的时间序列中。`INTO` 子句指定了查询结果写入的目标时间序列。
- 详细说明及示例见文档 [SELECT INTO（查询写回）](Select-Into.md) 。

### `FROM` 子句

- `FROM` 子句包含要查询的一个或多个时间序列的路径前缀，支持使用通配符。
- 在执行查询时，会将 `FROM` 子句中的路径前缀和 `SELECT` 子句中的后缀进行拼接得到完整的查询目标序列。

### `WHERE` 子句

- `WHERE` 子句指定了对数据行的筛选条件，由一个 `whereCondition` 组成。
- `whereCondition` 是一个逻辑表达式，对于要选择的每一行，其计算结果为真。如果没有 `WHERE` 子句，将选择所有行。
- 在 `whereCondition` 中，可以使用除聚合函数之外的任何 IOTDB 支持的函数和运算符。
- 详细说明及示例见文档 [查询过滤条件](./Where-Condition.md) 。

### `GROUP BY` 子句

- `GROUP BY` 子句指定对序列进行分段或分组聚合的方式。
- 分段聚合是指按照时间维度，针对同时间序列中不同数据点之间的时间关系，对数据在行的方向进行分段，每个段得到一个聚合值。目前仅支持**按时间区间分段**，未来将支持更多分段方式。
- 分组聚合是指针对不同时间序列，在时间序列的潜在业务属性上分组，每个组包含若干条时间序列，每个组得到一个聚合值。支持**按路径层级分组**和**按序列标签分组**两种分组方式。
- 分段聚合和分组聚合可以混合使用。
- 详细说明及示例见文档 [分段分组聚合](./Group-By.md) 。

### `HAVING` 子句

- `HAVING` 子句指定了对聚合结果的筛选条件，由一个 `havingCondition` 组成。
- `havingCondition` 是一个逻辑表达式，对于要选择的聚合结果，其计算结果为真。如果没有 `HAVING` 子句，将选择所有聚合结果。
- `HAVING` 要和聚合函数以及 `GROUP BY` 子句一起使用。
- 详细说明及示例见文档 [聚合结果过滤](./Having-Condition.md) 。

### `ORDER BY` 子句

- `ORDER BY` 子句用于指定结果集的排序方式。
- 按时间对齐模式下：默认按照时间戳大小升序排列，可以通过 `ORDER BY TIME DESC` 指定结果集按照时间戳大小降序排列。
- 按设备对齐模式下：先按照设备排列，每个设备内部按照时间戳大小升序排列，暂不支持使用 `ORDER BY` 子句。

[comment]: <> (- 按设备对齐模式下：默认按照设备名的字典序升序排列，每个设备内部按照时间戳大小升序排列，可以通过 `ORDER BY` 子句调整设备列和时间列的排序优先级。)

[comment]: <> (- 详细说明及示例见文档 [结果集排序]&#40;./Order-By.md&#41; 。)

### `FILL` 子句

- `FILL` 子句用于指定数据缺失情况下的填充模式，允许用户按照特定的方法对任何查询的结果集填充空值。
- 详细说明及示例见文档 [结果集补空值](./Fill.md) 。

### `SLIMIT` 和 `SOFFSET` 子句

- `SLIMIT` 指定查询结果的列数，`SOFFSET` 指定查询结果显示的起始列位置。`SLIMIT` 和 `SOFFSET` 仅用于控制值列，对时间列和设备列无效。
- 关于查询结果分页，详细说明及示例见文档 [结果集分页](./Pagination.md) 。

### `LIMIT` 和 `OFFSET` 子句

- `LIMIT` 指定查询结果的行数，`OFFSET` 指定查询结果显示的起始行位置。
- 关于查询结果分页，详细说明及示例见文档 [结果集分页](./Pagination.md) 。

### `ALIGN BY` 子句

- 查询结果集默认**按时间对齐**，包含一列时间列和若干个值列，每一行数据各列的时间戳相同。
- 除按时间对齐之外，还支持**按设备对齐**，查询结果集包含一列时间列、一列设备列和若干个值列。
- 详细说明及示例见文档 [查询对齐模式](./Align-By.md) 。

## SQL 示例

### 示例1：根据一个时间区间选择一列数据

SQL 语句为：

```sql
select temperature from root.ln.wf01.wt01 where time < 2017-11-01T00:08:00.000
```

其含义为：

被选择的设备为 ln 集团 wf01 子站 wt01 设备；被选择的时间序列为温度传感器（temperature）；该语句要求选择出该设备在 “2017-11-01T00:08:00.000” 时间点以前的所有温度传感器的值。

该 SQL 语句的执行结果如下：

```
+-----------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.temperature|
+-----------------------------+-----------------------------+
|2017-11-01T00:00:00.000+08:00|                        25.96|
|2017-11-01T00:01:00.000+08:00|                        24.36|
|2017-11-01T00:02:00.000+08:00|                        20.09|
|2017-11-01T00:03:00.000+08:00|                        20.18|
|2017-11-01T00:04:00.000+08:00|                        21.13|
|2017-11-01T00:05:00.000+08:00|                        22.72|
|2017-11-01T00:06:00.000+08:00|                        20.71|
|2017-11-01T00:07:00.000+08:00|                        21.45|
+-----------------------------+-----------------------------+
Total line number = 8
It costs 0.026s
```

### 示例2：根据一个时间区间选择多列数据

SQL 语句为：

```sql
select status, temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000;
```

其含义为：

被选择的设备为 ln 集团 wf01 子站 wt01 设备；被选择的时间序列为供电状态（status）和温度传感器（temperature）；该语句要求选择出 “2017-11-01T00:05:00.000” 至 “2017-11-01T00:12:00.000” 之间的所选时间序列的值。

该 SQL 语句的执行结果如下：

```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
|2017-11-01T00:08:00.000+08:00|                   false|                        22.58|
|2017-11-01T00:09:00.000+08:00|                   false|                        20.98|
|2017-11-01T00:10:00.000+08:00|                    true|                        25.52|
|2017-11-01T00:11:00.000+08:00|                   false|                        22.91|
+-----------------------------+------------------------+-----------------------------+
Total line number = 6
It costs 0.018s
```

### 示例3：按照多个时间区间选择同一设备的多列数据

IoTDB 支持在一次查询中指定多个时间区间条件，用户可以根据需求随意组合时间区间条件。例如，

SQL 语句为：

```sql
select status, temperature from root.ln.wf01.wt01 where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```

其含义为：

被选择的设备为 ln 集团 wf01 子站 wt01 设备；被选择的时间序列为“供电状态（status）”和“温度传感器（temperature）”；该语句指定了两个不同的时间区间，分别为“2017-11-01T00:05:00.000 至 2017-11-01T00:12:00.000”和“2017-11-01T16:35:00.000 至 2017-11-01T16:37:00.000”；该语句要求选择出满足任一时间区间的被选时间序列的值。

该 SQL 语句的执行结果如下：

```
+-----------------------------+------------------------+-----------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------------+------------------------+-----------------------------+
|2017-11-01T00:06:00.000+08:00|                   false|                        20.71|
|2017-11-01T00:07:00.000+08:00|                   false|                        21.45|
|2017-11-01T00:08:00.000+08:00|                   false|                        22.58|
|2017-11-01T00:09:00.000+08:00|                   false|                        20.98|
|2017-11-01T00:10:00.000+08:00|                    true|                        25.52|
|2017-11-01T00:11:00.000+08:00|                   false|                        22.91|
|2017-11-01T16:35:00.000+08:00|                    true|                        23.44|
|2017-11-01T16:36:00.000+08:00|                   false|                        21.98|
|2017-11-01T16:37:00.000+08:00|                   false|                        21.93|
+-----------------------------+------------------------+-----------------------------+
Total line number = 9
It costs 0.018s
```

### 示例4：按照多个时间区间选择不同设备的多列数据

该系统支持在一次查询中选择任意列的数据，也就是说，被选择的列可以来源于不同的设备。例如，SQL 语句为：

```sql
select wf01.wt01.status, wf02.wt02.hardware from root.ln where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```

其含义为：

被选择的时间序列为 “ln 集团 wf01 子站 wt01 设备的供电状态” 以及 “ln 集团 wf02 子站 wt02 设备的硬件版本”；该语句指定了两个时间区间，分别为 “2017-11-01T00:05:00.000 至 2017-11-01T00:12:00.000” 和 “2017-11-01T16:35:00.000 至 2017-11-01T16:37:00.000”；该语句要求选择出满足任意时间区间的被选时间序列的值。

该 SQL 语句的执行结果如下：

```
+-----------------------------+------------------------+--------------------------+
|                         Time|root.ln.wf01.wt01.status|root.ln.wf02.wt02.hardware|
+-----------------------------+------------------------+--------------------------+
|2017-11-01T00:06:00.000+08:00|                   false|                        v1|
|2017-11-01T00:07:00.000+08:00|                   false|                        v1|
|2017-11-01T00:08:00.000+08:00|                   false|                        v1|
|2017-11-01T00:09:00.000+08:00|                   false|                        v1|
|2017-11-01T00:10:00.000+08:00|                    true|                        v2|
|2017-11-01T00:11:00.000+08:00|                   false|                        v1|
|2017-11-01T16:35:00.000+08:00|                    true|                        v2|
|2017-11-01T16:36:00.000+08:00|                   false|                        v1|
|2017-11-01T16:37:00.000+08:00|                   false|                        v1|
+-----------------------------+------------------------+--------------------------+
Total line number = 9
It costs 0.014s
```

### 示例5：根据时间降序返回结果集

IoTDB 支持 `order by time` 语句，用于对结果按照时间进行降序展示。例如，SQL 语句为：

```sql
select * from root.ln.** where time > 1 order by time desc limit 10;
```

语句执行的结果为：

```
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
|                         Time|root.ln.wf02.wt02.hardware|root.ln.wf02.wt02.status|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
|2017-11-07T23:59:00.000+08:00|                        v1|                   false|                        21.07|                   false|
|2017-11-07T23:58:00.000+08:00|                        v1|                   false|                        22.93|                   false|
|2017-11-07T23:57:00.000+08:00|                        v2|                    true|                        24.39|                    true|
|2017-11-07T23:56:00.000+08:00|                        v2|                    true|                        24.44|                    true|
|2017-11-07T23:55:00.000+08:00|                        v2|                    true|                         25.9|                    true|
|2017-11-07T23:54:00.000+08:00|                        v1|                   false|                        22.52|                   false|
|2017-11-07T23:53:00.000+08:00|                        v2|                    true|                        24.58|                    true|
|2017-11-07T23:52:00.000+08:00|                        v1|                   false|                        20.18|                   false|
|2017-11-07T23:51:00.000+08:00|                        v1|                   false|                        22.24|                   false|
|2017-11-07T23:50:00.000+08:00|                        v2|                    true|                         23.7|                    true|
+-----------------------------+--------------------------+------------------------+-----------------------------+------------------------+
Total line number = 10
It costs 0.016s
```

## 使用方式

数据查询语句支持在 SQL 命令行终端、JDBC、JAVA / C++ / Python / Go 等编程语言 API、RESTful API 中使用。

- 在 SQL 命令行终端中执行查询语句：启动 SQL 命令行终端，直接输入查询语句执行即可，详见 [SQL 命令行终端](../QuickStart/Command-Line-Interface.md)。

- 在 JDBC 中执行查询语句，详见 [JDBC](../API/Programming-JDBC.md) 。

- 在 JAVA / C++ / Python / Go 等编程语言 API 中执行查询语句，详见应用编程接口一章相应文档。接口原型如下：

  ```java
  SessionDataSet executeQueryStatement(String sql);
  ```

- 在 RESTful API 中使用，详见 [HTTP API](../API/RestService.md) 。
