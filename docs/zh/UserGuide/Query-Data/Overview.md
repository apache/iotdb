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
# 数据查询
## 概述

### 语法定义

在 IoTDB 中，使用 `SELECT` 语句从一条或多条时间序列中查询数据。 下面是 `SELECT` 语句的语法定义：

```sql
[TRACING?] SELECT 
	[LAST?] selectExpr (, selectExpr)*
	<fromClause> FROM prefixPath (, prefixPath)*
	<whereClause?> WHERE queryFilter
	<orderByTimeClause?> ORDER BY TIME [ASC | DESC]
	<paginationClause?> [LIMIT | SLIMIT] INT [OFFSET | SOFFSET] INT
	<groupByLevelClause?> GROUP BY LEVEL = INT
	<groupByTimeClause?> GROUP BY ([startTime, endTime), slidingStep)
	<fillClause?> FILL ([PREVIOUS, beforeRange | LINEAR, beforeRange, afterRange | constant])
	<withoutNullClause?> WITHOUT NULL [ANY | ALL]
	<alignClause?> [ALIGN BY DEVICE | DISABLE ALIGN]
```

常用的子句如下：

- 每个 `selectExpr` 对应查询结果的一列，支持时间序列后缀、时间序列生成函数（包括用户自定义函数）、聚合函数、数字常量、算数运算表达式。每个 `SELECT` 语句至少应该包含一个 `selectExpr` 。关于 `selectExpr`，详见 [选择表达式](./Select-Expression.md) 。
- `fromClause` 包含要查询的一个或多个时间序列的前缀。
- `whereClause`（可选）指定了查询的筛选条件 `queryFilter`。`queryFilter` 是一个逻辑表达式，查询结果返回计算结果为真的数据点。如果没有指定 `whereClaue`，则返回序列中所有数据点。关于 `queryFilter`，详见 [查询过滤条件](./Query-Filter.md) 。
- 查询结果默认按照时间戳大小升序排列，可以通过 `ORDER BY TIME DESC` 指定结果集按照时间戳大小降序排列。
- 当查询结果数据量很大时，可以使用 `LIMIT/SLIMIT` 及 `OFFSET/SOFFSET` 对结果集进行分页，详见 [查询结果分页](./Pagination.md) 。
- 查询结果集默认按照时间戳进行对齐，即以时间序列为列，每一行数据各列的时间戳相同。其他结果集对齐方式详见 [查询结果对齐格式](./Result-Format.md) 。

### 基本示例

#### 根据一个时间区间选择一列数据

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

#### 根据一个时间区间选择多列数据

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

#### 按照多个时间区间选择同一设备的多列数据

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

#### 按照多个时间区间选择不同设备的多列数据

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

#### 根据时间降序返回结果集

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

### 使用方式

数据查询语句支持在 SQL 命令行终端、JDBC、JAVA / C++ / Python / Go 等编程语言 API、RESTful API 中使用。

- 在 SQL 命令行终端中执行查询语句：启动 SQL 命令行终端，直接输入查询语句执行即可，详见 [SQL 命令行终端](../QuickStart/Command-Line-Interface.md)。

- 在 JDBC 中执行查询语句，详见 [JDBC](../API/Programming-JDBC.md) 。

- 在 JAVA / C++ / Python / Go 等编程语言 API 中执行查询语句，详见应用编程接口一章相关文档。接口原型如下：

  ```java
  SessionDataSet executeQueryStatement(String sql)
  ```

- 在 RESTful API 中使用，详见 [HTTP API](../API/RestService.md) 。
