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
## CLI写入数据

IoTDB 为用户提供多种插入实时数据的方式，例如在 [Cli/Shell 工具](../QuickStart/Command-Line-Interface.md) 中直接输入插入数据的 INSERT 语句，或使用 Java API（标准 [Java JDBC](../API/Programming-JDBC.md) 接口）单条或批量执行插入数据的 INSERT 语句。

本节主要为您介绍实时数据接入的 INSERT 语句在场景中的实际使用示例，有关 INSERT SQL 语句的详细语法请参见本文 [INSERT 语句](../Reference/SQL-Reference.md) 节。

注：写入重复时间戳的数据则原时间戳数据被覆盖，可视为更新数据。

### 使用 INSERT 语句

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

### 向对齐时间序列插入数据

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

## 原生接口写入
原生接口 （Session） 是目前IoTDB使用最广泛的系列接口，包含多种写入接口，适配不同的数据采集场景，性能高效且支持多语言。

### 多语言接口写入
* ### Java
    使用Java接口写入之前，你需要先建立连接，参考 [Java原生接口](../API/Programming-Java-Native-API.md)。
    之后通过 [ JAVA 数据操作接口（DML）](../API/Programming-Java-Native-API.md#数据写入)写入。

* ### Python
    参考 [ Python 数据操作接口（DML）](../API/Programming-Python-Native-API.md#数据写入)

* ### C++ 
    参考 [ C++ 数据操作接口（DML）](../API/Programming-Cpp-Native-API.md)

* ### Go
    参考 [Go 原生接口](../API/Programming-Go-Native-API.md)

## REST API写入

参考 [insertTablet (v1)](../API/RestServiceV1.md#inserttablet) or [insertTablet (v2)](../API/RestServiceV2.md#inserttablet)

示例如下：
```JSON
{
      "timestamps": [
            1,
            2,
            3
      ],
      "measurements": [
            "temperature",
            "status"
      ],
      "data_types": [
            "FLOAT",
            "BOOLEAN"
      ],
      "values": [
            [
                  1.1,
                  2.2,
                  3.3
            ],
            [
                  false,
                  true,
                  true
            ]
      ],
      "is_aligned": false,
      "device": "root.ln.wf01.wt01"
}
```

## MQTT写入

参考 [内置 MQTT 服务](../API/Programming-MQTT.md#内置-mqtt-服务)

## 批量数据导入

针对于不同场景，IoTDB 为用户提供多种批量导入数据的操作方式，本章节向大家介绍最为常用的两种方式为 CSV文本形式的导入 和 TsFile文件形式的导入。

### TsFile批量导入

TsFile 是在 IoTDB 中使用的时间序列的文件格式，您可以通过CLI等工具直接将存有时间序列的一个或多个 TsFile 文件导入到另外一个正在运行的IoTDB实例中。具体操作方式请参考[TsFile 导入工具](../Maintenance-Tools/Load-Tsfile.md)，[TsFile 导出工具](../Maintenance-Tools/TsFile-Load-Export-Tool.md)。

### CSV批量导入

CSV 是以纯文本形式存储表格数据，您可以在CSV文件中写入多条格式化的数据，并批量的将这些数据导入到 IoTDB 中，在导入数据之前，建议在IoTDB中创建好对应的元数据信息。如果忘记创建元数据也不要担心，IoTDB 可以自动将CSV中数据推断为其对应的数据类型，前提是你每一列的数据类型必须唯一。除单个文件外，此工具还支持以文件夹的形式导入多个 CSV 文件，并且支持设置如时间精度等优化参数。具体操作方式请参考 [CSV 导入导出工具](../Maintenance-Tools/CSV-Tool.md)。

# 删除数据

用户使用 [DELETE 语句](../Reference/SQL-Reference.md) 可以删除指定的时间序列中符合时间删除条件的数据。在删除数据时，用户可以选择需要删除的一个或多个时间序列、时间序列的前缀、时间序列带、*路径对某一个时间区间内的数据进行删除。

在 JAVA 编程环境中，您可以使用 JDBC API 单条或批量执行 DELETE 语句。

## 单传感器时间序列值删除

以测控 ln 集团为例，存在这样的使用场景：

wf02 子站的 wt02 设备在 2017-11-01 16:26:00 之前的供电状态出现多段错误，且无法分析其正确数据，错误数据影响了与其他设备的关联分析。此时，需要将此时间段前的数据删除。进行此操作的 SQL 语句为：

```sql
delete from root.ln.wf02.wt02.status where time<=2017-11-01T16:26:00;
```

如果我们仅仅想要删除 2017 年内的在 2017-11-01 16:26:00 之前的数据，可以使用以下 SQL:
```sql
delete from root.ln.wf02.wt02.status where time>=2017-01-01T00:00:00 and time<=2017-11-01T16:26:00;
```

IoTDB 支持删除一个时间序列任何一个时间范围内的所有时序点，用户可以使用以下 SQL 语句指定需要删除的时间范围：
```sql
delete from root.ln.wf02.wt02.status where time < 10
delete from root.ln.wf02.wt02.status where time <= 10
delete from root.ln.wf02.wt02.status where time < 20 and time > 10
delete from root.ln.wf02.wt02.status where time <= 20 and time >= 10
delete from root.ln.wf02.wt02.status where time > 20
delete from root.ln.wf02.wt02.status where time >= 20
delete from root.ln.wf02.wt02.status where time = 20
```

需要注意，当前的删除语句不支持 where 子句后的时间范围为多个由 OR 连接成的时间区间。如下删除语句将会解析出错：
```
delete from root.ln.wf02.wt02.status where time > 4 or time < 0
Msg: 303: Check metadata error: For delete statement, where clause can only contain atomic
expressions like : time > XXX, time <= XXX, or two atomic expressions connected by 'AND'
```

如果 delete 语句中未指定 where 子句，则会删除时间序列中的所有数据。
```sql
delete from root.ln.wf02.wt02.status
```

## 多传感器时间序列值删除    

当 ln 集团 wf02 子站的 wt02 设备在 2017-11-01 16:26:00 之前的供电状态和设备硬件版本都需要删除，此时可以使用含义更广的 [路径模式（Path Pattern）](../Basic-Concept/Data-Model-and-Terminology.md) 进行删除操作，进行此操作的 SQL 语句为：


```sql
delete from root.ln.wf02.wt02.* where time <= 2017-11-01T16:26:00;
```

需要注意的是，当删除的路径不存在时，IoTDB 不会提示路径不存在，而是显示执行成功，因为 SQL 是一种声明式的编程方式，除非是语法错误、权限不足等，否则都不认为是错误，如下所示。

```sql
IoTDB> delete from root.ln.wf03.wt02.status where time < now()
Msg: The statement is executed successfully.
```

## 删除时间分区 （实验性功能）
您可以通过如下语句来删除某一个 database 下的指定时间分区：

```sql
DELETE PARTITION root.ln 0,1,2
```

上例中的 0,1,2 为待删除时间分区的 id，您可以通过查看 IoTDB 的数据文件夹找到它，或者可以通过计算`timestamp / partitionInterval`（向下取整）,
手动地将一个时间戳转换为对应的 id，其中的`partitionInterval`可以在 IoTDB 的配置文件中找到（如果您使用的版本支持时间分区）。

请注意该功能目前只是实验性的，如果您不是开发者，使用时请务必谨慎。

