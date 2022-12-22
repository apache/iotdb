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


# 写入数据

IoTDB 为用户提供多种插入实时数据的方式，例如在 [Cli/Shell 工具](../QuickStart/Command-Line-Interface.md) 中直接输入插入数据的 INSERT 语句，或使用 Java API（标准 [Java JDBC](../API/Programming-JDBC.md) 接口）单条或批量执行插入数据的 INSERT 语句。

本节主要为您介绍实时数据接入的 INSERT 语句在场景中的实际使用示例，有关 INSERT SQL 语句的详细语法请参见本文 [INSERT 语句](../Reference/SQL-Reference.md) 节。

注：写入重复时间戳的数据则原时间戳数据被覆盖，可视为更新数据。

## 使用 INSERT 语句

使用 INSERT 语句可以向指定的已经创建的一条或多条时间序列中插入数据。对于每一条数据，均由一个时间戳类型的时间戳和一个数值或布尔值、字符串类型的传感器采集值组成。

在本节的场景实例下，以其中的两个时间序列`root.ln.wf02.wt02.status`和`root.ln.wf02.wt02.hardware`为例 ，它们的数据类型分别为 BOOLEAN 和 TEXT。

单列数据插入示例代码如下：

```sql
IoTDB > insert into root.ln.wf02.wt02(timestamp,status) values(1,true)
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1, 'v1')
```

以上示例代码将长整型的 timestamp 以及值为 true 的数据插入到时间序列`root.ln.wf02.wt02.status`中和将长整型的 timestamp 以及值为”v1”的数据插入到时间序列`root.ln.wf02.wt02.hardware`中。执行成功后会返回执行时间，代表数据插入已完成。 

> 注意：在 IoTDB 中，TEXT 类型的数据单双引号都可以来表示，上面的插入语句是用的是双引号表示 TEXT 类型数据，下面的示例将使用单引号表示 TEXT 类型数据。

INSERT 语句还可以支持在同一个时间点下多列数据的插入，同时向 2 时间点插入上述两个时间序列的值，多列数据插入示例代码如下：

```sql
IoTDB > insert into root.ln.wf02.wt02(timestamp, status, hardware) values (2, false, 'v2')
```

此外，INSERT 语句支持一次性插入多行数据，同时向 2 个不同时间点插入上述时间序列的值，示例代码如下：

```sql
IoTDB > insert into root.ln.wf02.wt02(timestamp, status, hardware) VALUES (3, false, 'v3'),(4, true, 'v4')
```

插入数据后我们可以使用 SELECT 语句简单查询已插入的数据。

```sql
IoTDB > select * from root.ln.wf02.wt02 where time < 5
```

结果如图所示。由查询结果可以看出，单列、多列数据的插入操作正确执行。

```
+-----------------------------+--------------------------+------------------------+
|                         Time|root.ln.wf02.wt02.hardware|root.ln.wf02.wt02.status|
+-----------------------------+--------------------------+------------------------+
|1970-01-01T08:00:00.001+08:00|                        v1|                    true|
|1970-01-01T08:00:00.002+08:00|                        v2|                   false|
|1970-01-01T08:00:00.003+08:00|                        v3|                   false|
|1970-01-01T08:00:00.004+08:00|                        v4|                    true|
+-----------------------------+--------------------------+------------------------+
Total line number = 4
It costs 0.004s
```

此外，我们可以省略 timestamp 列，此时系统将使用当前的系统时间作为该数据点的时间戳，示例代码如下：
```sql
IoTDB > insert into root.ln.wf02.wt02(status, hardware) values (false, 'v2')
```
**注意：** 当一次插入多行数据时必须指定时间戳。

## 向对齐时间序列插入数据

向对齐时间序列插入数据只需在SQL中增加`ALIGNED`关键词，其他类似。

示例代码如下：

```sql
IoTDB > create aligned timeseries root.sg1.d1(s1 INT32, s2 DOUBLE)
IoTDB > insert into root.sg1.d1(time, s1, s2) aligned values(1, 1, 1)
IoTDB > insert into root.sg1.d1(time, s1, s2) aligned values(2, 2, 2), (3, 3, 3)
IoTDB > select * from root.sg1.d1
```

结果如图所示。由查询结果可以看出，数据的插入操作正确执行。

```
+-----------------------------+--------------+--------------+
|                         Time|root.sg1.d1.s1|root.sg1.d1.s2|
+-----------------------------+--------------+--------------+
|1970-01-01T08:00:00.001+08:00|             1|           1.0|
|1970-01-01T08:00:00.002+08:00|             2|           2.0|
|1970-01-01T08:00:00.003+08:00|             3|           3.0|
+-----------------------------+--------------+--------------+
Total line number = 3
It costs 0.004s
```
