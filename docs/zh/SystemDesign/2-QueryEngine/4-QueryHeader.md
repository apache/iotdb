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

# 查询结果集表头构造

## 内容简介

本文主要介绍原始数据查询（RawDataQuery）、按设备对齐查询（AlignByDeviceQuery）、最新数据查询（LastQuery）三种查询的结果集表头构造方法。如降频聚合查询、空值填充查询等查询将作为子查询在该三类查询中讲解。

## 原始数据查询

原始数据查询的结果集表头构造逻辑主要在 `getWideQueryHeaders()` 方法中。

- org.apache.iotdb.db.service.TSServiceImpl.getWideQueryHeaders

对于每个结果集表头的构造，需要提供列名及该列对应的数据类型。

- 普通原始数据查询（包括空值填充查询）只需要从物理查询计划中取得**未去重**的时间序列路径，该时间序列路径即作为列名，并使用该路径取得时间序列对应的数据类型，即可生成结果集表头。

- 而如果原始数据查询中包含聚合函数（包括一般聚合查询和降频聚合查询），将忽略时间列并使用**聚合函数和时间序列路径共同构成列名**，且取得数据类型时将以聚合函数的类型为准，如 `root.sg.d1.s1` 是 FLOAT 类型，而 `count(root.sg.d1.s1)` 应为 INT 类型。

接下来将举例说明：

假设查询中出现的所有时间序列均存在，则以下两个查询生成的结果集表头分别为：

SQL1：`SELECT s1, s2 FROM root.sg.d1;`  ->

| Time | root.sg.d1.s1 | root.sg.d1.s2 |
| ---- | ------------- | ------------- |
|      |               |               |

SQL2：`SELECT count(s1), max_time(s1) FROM root.sg.d1;` ->

| count(root.sg.d1.s1) | max_time(root.sg.d1.s2) |
| -------------------- | ----------------------- |
|                      |                         |

## 按设备对齐查询

原始数据查询的结果集表头构造逻辑主要在 `getAlignByDeviceQueryHeaders()` 方法中。

- org.apache.iotdb.db.service.TSServiceImpl.getAlignByDeviceQueryHeaders

按设备对齐查询的结果集构造依赖于物理查询计划中生成的**未去重**的度量（Measurements）列表。在此作简单介绍，度量列表是由 SELECT 子句中的后缀路径（包括通配符）生成的列表，其中共有三种类型，分别为常量（Constant）、存在的时间序列（Exist）以及不存在的时间序列（NonExist）。详细可以参考 [Align by device query](/SystemDesign/5-DataQuery/8-AlignByDeviceQuery.html)

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

## 最新数据查询

最新数据查询的结果集表头构造逻辑主要在静态方法 `LAST_RESP` 中。

- org.apache.iotdb.db.service.StaticResps.LAST_RESP

最新数据查询计算出需要查询的时间序列具有最大时间戳的结果并以时间（Time）、时间序列（timeseries）及值（value）三列进行展示。

接下来举例说明：

假设目前共有 `root.sg.d1.s1`, `root.sg.d1.s2` 两条时间序列，则以下查询语句生成的表头为：

SQL：`SELECT last s1, s2 FROM root.sg.d1;`

| Time | timeseries    | value |
| ---- | ------------- | ----- |
| ...  | root.sg.d1.s1 | ...   |
| ...  | root.sg.d1.s2 | ...   |
