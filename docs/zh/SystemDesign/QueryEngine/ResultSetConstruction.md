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

# 查询结果集构造

## 内容简介

本文主要介绍查询结果集的构造原理，包括三部分：表头构造、生成非重复的结果集、还原完整结果集。

## 表头构造

接下来介绍第一部分，包括原始数据查询（RawDataQuery）、按设备对齐查询（AlignByDeviceQuery）、最新数据查询（LastQuery）三种查询的结果集表头构造方法。如降频聚合查询、空值填充查询等查询将作为子查询在该三类查询中讲解。

### 原始数据查询

原始数据查询的结果集表头构造逻辑主要在 `getWideQueryHeaders()` 方法中。

- org.apache.iotdb.db.service.TSServiceImpl.getWideQueryHeaders

对于每个结果集表头的构造，需要提供列名及该列对应的数据类型。

- 普通原始数据查询（包括空值填充查询）只需要从物理查询计划中取得**未去重**的时间序列路径，该时间序列路径即作为列名，并使用该路径取得时间序列对应的数据类型，即可生成结果集表头。

- 而如果原始数据查询中包含聚合函数（包括一般聚合查询和降频聚合查询），将忽略时间列并使用**聚合函数和时间序列路径共同构成列名**，且取得数据类型时将以聚合函数的类型为准，如 `root.sg.d1.s1` 是 FLOAT 类型，而 `count(root.sg.d1.s1)` 应为 LONG 类型。

接下来将举例说明：

假设查询中出现的所有时间序列均存在，则以下两个查询生成的结果集表头分别为：

SQL1：`SELECT s1, s1, s2 FROM root.sg.d1;`  ->

| Time | root.sg.d1.s1 | root.sg.d1.s1 | root.sg.d1.s2 |
| ---- | ------------- | ------------- | ------------- |
|      |               |               |               |

SQL2：`SELECT count(s1), max_time(s1) FROM root.sg.d1;` ->

| count(root.sg.d1.s1) | max_time(root.sg.d1.s1) |
| -------------------- | ----------------------- |
|                      |                         |

### 按设备对齐查询

原始数据查询的结果集表头构造逻辑主要在 `getAlignByDeviceQueryHeaders()` 方法中。

- org.apache.iotdb.db.service.TSServiceImpl.getAlignByDeviceQueryHeaders

按设备对齐查询的结果集构造依赖于物理查询计划中生成的**未去重**的度量（Measurements）列表。在此作简单介绍，度量列表是由 SELECT 子句中的后缀路径（包括通配符）生成的列表，其中共有三种类型，分别为常量（Constant）、存在的时间序列（Exist）以及不存在的时间序列（NonExist）。详细可以参考 [Align by device query](../DataQuery/AlignByDeviceQuery.md)

由于按设备对齐查询采用关系表结构，因此首先在表头中加入设备列，其对应的数据类型为文本类型。

然后取得度量列表，将每个度量作为列名，并根据度量类型选择相应的数据类型。如果是常量或不存在的时间序列类型，则直接设数据类型为文本类型；如果是存在的时间序列，则根据物理查询计划中存储的 `measurementDataTypeMap` 取得该度量对应的数据类型。

注意如果是聚合查询，此处度量列表中的度量会包含聚合函数，因此可以一并处理。

接下来举例说明：

假设目前共有 `root.sg.d1.s1`, `root.sg.d1.s2` 两条时间序列，则以下查询语句生成的表头为：

SQL：`SELECT '111', s1, s2, *, s5 FROM root.sg.d1 ALIGN BY DEVICE;`

-> 度量列表 ['111', s1, s2, s1, s2, s5]

-> 表头

| Time | Device | 111 | s1  | s2  | s1  | s2  | s5  |
| ---- | ------ | --- | --- | --- | --- | --- | --- |
|      |        |     |     |     |     |     |     |

### 最新数据查询

最新数据查询的结果集表头构造逻辑主要在静态方法 `LAST_RESP` 中。

- org.apache.iotdb.db.service.StaticResps.LAST_RESP

最新数据查询计算出需要查询的时间序列具有最大时间戳的结果并以时间（Time）、时间序列（timeseries）、值（value）、数据类型（dataType）四列进行展示。

接下来举例说明：

假设目前共有 `root.sg.d1.s1`, `root.sg.d1.s2` 两条时间序列，则以下查询语句生成的表头为：

SQL：`SELECT last s1, s2 FROM root.sg.d1;`

| Time | timeseries    | value | dataType |
| ---- | ------------- | ----- |----- |
| ...  | root.sg.d1.s1 | ...   |...   |
| ...  | root.sg.d1.s2 | ...   |...   |

## 生成非重复结果集

与表头构造不同，我们不需要在执行物理查询计划时查询重复的数据，例如对于查询 `SELECT s1, s1 FROM root.sg.d1`，我们只需要查询一次时间序列 `root.sg.d1.s1` 的值即可。因此在表头构造完成后，我们需要在服务器端先生成非重复的结果集。

除按设备对齐查询外，**原始数据查询、聚合查询、最新数据查询** 等查询的去重逻辑均在 `deduplicate()` 方法中。

- org.apache.iotdb.db.qp.strategy.PhysicalGenerator.deduplicate()

去重逻辑比较简单：首先从查询计划中取得未去重的路径，然后在遍历时创建一个 Set 集合用于去重即可。

值得注意的是：在原始数据查询和聚合查询去重前，先**将查询的时间序列路径按设备进行排序**，目的是为了查询时减少 I/O 和反序列化操作，加快查询速度。此处额外计算出一个数据结构 `pathToIndex` 用于记录排序后每个路径在查询中的位置。

由于最新数据查询只需要计算一组数据，因此不需要对路径进行排序，其 `pathToIndex` 为 null。

**按设备对齐查询**的去重逻辑在其结果集的 `hasNextWithoutConstraint()` 方法中。

- org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet.hasNextWithoutConstraint()

由于按设备对齐查询需要按设备依次组织其查询计划，每个设备查询的路径未必相同，且允许包含常量列以及不存在的时间序列，因此不能简单地与其他查询一起去重。去重时**不仅需要去除重复查询的时间序列路径，还需要去除查询中出现的常量列以及当前设备中不存在的时间序列**。实现方法可以参考 [Align by device query](../DataQuery/AlignByDeviceQuery.md).

在查询计划中的去重路径构造完成后，即可调用 IoTDB 的查询执行器来执行查询，并得到去重后的结果集。

## 还原完整结果集

以上构造表头和生成非重复结果集均在服务器端完成，然后返回给客户端。客户端根据原始的表头信息对非重复的结果集进行还原后，展示给用户。为了区分两个结果集，下面分别称之为**查询结果集**和**最终结果集**。

为了方便讲解，首先给出一个简单的示例：

SQL: `SELECT s2, s1, s2 FROM root.sg.d1;`

通过上面的步骤已经计算出表头中的列名列表 `columnNameList` 为（时间戳将之后计算）：

`[root.sg.d1.s2, root.sg.d1.s1, root.sg.d1.s2].`

时间序列路径在查询中的位置 `pathToIndex` 为（按设备排过序）：

`root.sg.d1.s1 -> 0, root.sg.d1.s2 -> 1;`

则查询结果集为：

| Time | root.sg.d1.s1 | root.sg.d1.s2 |
| ---- | ------------- | ------------- |
|      |               |               |

为了还原最终结果集，需要构造一个列名到其在查询结果集中位置的映射集 `columnOrdinalMap`，方便从查询结果集中取出某一列对应的结果，该部分逻辑在新建最终结果集 `IoTDBQueryResultSet` 的构造函数内完成。

- org.apache.iotdb.jdbc.AbstractIoTDBResultSet.AbstractIoTDBResultSet()

为了构造最终结果集中的元数据信息，需要构造完整的列名列表，由于上面给出的 `columnNameList` 中不包含时间戳，因此，如果需要打印时间戳则在表头中加入 `Time` 列构成完整的表头。

示例中为：

| Time | root.sg.d1.s2 | root.sg.d1.s1 | root.sg.d1.s2 |
| ---- | ------------- | ------------- | ------------- |
|      |               |               |               |

接下来计算 `columnOrdinalMap` ，首先判断是否需要打印时间戳，如果需要则将时间戳记录为第一列。

然后遍历表头中的列名列表，并检查 `columnNameIndex` 是否被初始化，该字段来源于去重时计算的 `pathToIndex`，记录了每个时间序列路径在查询中的位置。如果被初始化，则其中的位置 + 2 即为在结果集中的位置；若未被初始化，则按遍历顺序记录位置即可，该顺序与服务器端去重后的查询顺序保持一致。

示例中 `columnOrdinalMap` 应为：

`Time -> 1, root.sg.d1.s2 -> 3, root.sg.d1.s1 -> 2`

接下来遍历完整的表头信息，然后根据该映射集填充完整的结果集即可，其逻辑在 `cacheResult()` 方法中。

- org.apache.iotdb.cli.AbstractCli.cacheResult()

例如最终结果集表头中第二列为 `root.sg.d1.s2`，则从查询结果集中取出第三列的结果作为其值。重复该过程，填满每一行的结果，直到查询结果集中没有下一结果或达到最大输出行数。
