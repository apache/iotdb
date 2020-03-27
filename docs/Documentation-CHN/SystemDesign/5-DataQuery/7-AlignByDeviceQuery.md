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

# 按设备对齐查询

AlignByDevicePlan 即按设备对齐查询对应的表结构为：

| Time | Device | sensor1 | sensor2 | sensor3 | ... |
| ---- | ------ | ------- | ------- | ------- | --- |
|      |        |         |         |         |     |

## 设计原理

按设备对齐查询其实现原理主要是计算出查询中每个设备对应的测点和过滤条件，然后将查询按设备分别进行，最后将结果集拼装并返回。

### AlignByDevicePlan 中重要字段含义

首先解释一下 AlignByDevicePlan 中一些重要字段的含义：
- `List<String> measurements`：查询中出现的 measurement 列表。
- `Map<Path, TSDataType> dataTypeMapping`: 该变量继承自基类 QueryPlan，其主要作用是在计算每个设备的执行路径时，提供此次查询的 paths 对应的数据类型。
- `Map<String, Set<String>> deviceToMeasurementsMap`, `Map<String, IExpression> deviceToFilterMap`: 这两个字段分别用来存储设备对应的测点和过滤条件。
- `Map<String, TSDataType> measurementDataTypeMap`：AlignByDevicePlan 要求不同设备的同名 sensor 数据类型一致，该字段是一个 `measurementName -> dataType` 的 Map 结构，用来验证同名 sensor 的数据类型一致性。如 `root.sg.d1.s1` 和 `root.sg.d2.s1` 应该是同一数据类型。
- `enum MeasurementType`：记录三种 measurement 类型。在任何设备中都不存在的 measurement 为 `NonExist` 类型；有单引号或双引号的 measurement 为 `Constant` 类型；存在的 measurement 为 `Exist` 类型。
- `Map<String, MeasurementType> measurementTypeMap`: 该字段是一个 `measureName -> measurementType` 的 Map 结构，用来记录查询中所有 measurement 的类型。
- groupByPlan, fillQueryPlan, aggregationPlan：为了避免冗余，这三个执行计划被设定为 RawDataQueryPlan 的子类，而在 AlignByDevicePlan 中被设置为变量。如果查询计划属于这三个计划中的一种，则该字段会被赋值并保存。

在进行具体实现过程的讲解前，先给出一个覆盖较为完整的例子，下面的解释过程中将结合该示例进行说明。

```sql
SELECT s1, "1", *, s2, s5 FROM root.sg.d1, root.sg.* WHERE time = 1 AND s1 < 25 ALIGN BY DEVICE
```

其中，系统中的时间序列为：

- root.sg.d1.s1
- root.sg.d1.s2
- root.sg.d2.s1

存储组 `root.sg` 共包含两个设备 d1 和 d2，其中 d1 有两个传感器 s1 和 s2，d2 只有传感器 s1，相同传感器 s1 的数据类型相同。

下面将按具体过程进行分别解释：

### 逻辑计划生成

- org.apache.iotdb.db.qp.Planner

与原始数据查询不同，按设备对齐查询并不在此阶段进行 SELECT 语句和 WHERE 语句中后缀路径的拼接，而将在后续生成物理计划时，计算出每个设备对应的映射值和过滤条件。因此，按设备对齐在此阶段所做的工作只包括对 WHERE 语句中过滤条件的优化。

对过滤条件的优化主要包括三部分：去非、转化析取范式、合并同路径过滤条件。对应的优化器分别为：RemoveNotOptimizer, DnfFilterOptimizer, MergeSingleFilterOptimizer。该部分逻辑可参考：[Planner](/#/SystemDesign/progress/chap2/sec2).

### 物理计划生成

- org.apache.iotdb.db.qp.strategy.PhysicalGenerator

生成逻辑计划后，将调用 PhysicalGenerator 类中的 `transformToPhysicalPlan()` 方法将该逻辑计划转化为物理计划。对于按设备对齐查询，该方法的主要逻辑实现在 `transformQuery()` 方法中。

**该阶段所做的主要工作为生成查询对应的** `AlignByDevicePlan`，**填充其中的变量信息。**

首先解释一下 `transformQuery()` 方法中一些重要字段的含义(与 AlignByDevicePlan 中重复的字段见上文)：

- prefixPaths, suffixPaths：前者为 FROM 子句中的前缀路径，示例中为 `[root.sg.d1, root.sg.*]`; 后者为 SELECT 子句中的后缀路径，示例中为 `[s1, "1", *, s2, s5]`.
- devices：对前缀路径去通配符和设备去重后得到的设备列表，示例中为 `[root.sg.d1, root.sg.d2]`。
- measurementSetOfGivenSuffix：中间变量，记录某一 suffix 对应的 measurement，示例中，对于后缀 \*, `measurementSetOfGivenSuffix = {s1,s2}`，对于后缀 s1, `measurementSetOfGivenSuffix = {s1}`;

接下来介绍 AlignByDevicePlan 的计算过程：

1. 检查查询类型是否为 groupByPlan, fillQueryPlan, aggregationPlan 这三类查询中的一种，如果是则对相应的变量进行赋值，并更改 `AlignByDevicePlan` 的查询类型。
2. 遍历 SELECT 后缀路径，对每一个后缀路径设置一个中间变量为 `measurementSetOfGivenSuffix`，用来记录该后缀路径对应的所有 measurement。如果后缀路径以单引号或双引号开头，则直接在 `measurements` 中增加该值，并记录其类型为 `Constant` 类型。
3. 否则将设备列表与该后缀路径拼接，得到完整的路径，如果拼接后的路径不存在，需要进一步判断该 measurement 是否在其它设备中存在，如果都没有则暂时识别为 `NonExist`，如果后续出现设备存在该 measurement，则覆盖 `NonExist` 值为 `Exist`。
4. 如果拼接后路径存在，则证明 measurement 是 `Exist` 类型，需要检验数据类型的一致性，不满足返回错误信息，满足则记录下该 Measurement，对 `measurementSetOfGivenSuffix`, `deviceToMeasurementsMap` 等进行更新。
5. 在一层 suffix 循环结束后，将该层循环中出现的 `measurementSetOfGivenSuffix` 加入 `measurements` 中。在整个循环结束后，将循环中得到的变量信息赋值到 AlignByDevicePlan 中。此处得到的 measurements 列表是未经过去重的，在生成 `ColumnHeader` 时将进行去重。
6. 最后调用 `concatFilterByDevice()` 方法计算 `deviceToFilterMap`，得到将每个设备分别拼接后对应的 Filter 信息。

```java
Map<String, IExpression> concatFilterByDevice(List<String> devices,
      FilterOperator operator)
输入：去重后的 devices 列表和未拼接的 FilterOperator
输入：经过拼接后的 deviceToFilterMap，记录了每个设备对应的 Filter 信息
```

`concatFilterByDevice()` 方法的主要处理逻辑在 `concatFilterPath()` 中：

`concatFilterPath()` 方法遍历未拼接的 FilterOperator 二叉树，判断节点是否为叶子节点，如果是，则取该叶子结点的路径，如果路径以 time 或 root 开头则不做处理，否则将设备名与节点路径进行拼接后返回；如果不是，则对该节点的所有子节点进行迭代处理。示例中，设备1过滤条件拼接后的结果为 `time = 1 AND root.sg.d1.s1 < 25`，设备2为 `time = 1 AND root.sg.d2.s1 < 25`。

下面用示例总结一下通过该阶段计算得到的变量信息：

- measurement 列表 `measurements`：`[s1, "1", s1, s2, s2, s5]`
- measurement 类型 `measurementTypeMap`：
  -  `s1 -> Exist`
  -  `s2 -> Exist`
  -  `"1" -> Constant`
  -  `s5 -> NonExist`
- 每个设备的测点 `deviceToMeasurementsMap`:
  -  `root.sg.d1 -> s1, s2`
  -  `root.sg.d2 -> s1`
- 每个设备的过滤条件 `deviceToFilterMap`：
  -  `root.sg.d1 -> time = 1 AND root.sg.d1.s1 < 25`
  -  `root.sg.d2 -> time = 1 AND root.sg.d2.s1 < 25`

### 构造表头 (ColumnHeader)

- org.apache.iotdb.db.service.TSServiceImpl

在生成物理计划后，则可以执行 TSServiceImpl 中的 executeQueryStatement() 方法生成结果集并返回，其中第一步是构造表头。

按设备对齐查询在调用 `TSServiceImpl.getQueryColumnHeaders()` 方法后，根据查询类型进入 `TSServiceImpl.getAlignByDeviceQueryHeaders()` 来构造表头。

`getAlignByDeviceQueryHeaders()` 方法声明如下：

```java
private void getAlignByDeviceQueryHeaders(
      AlignByDevicePlan plan, List<String> respColumns, List<String> columnTypes)
输入：当前执行的物理计划 AlignByDevicePlan 和需要输出的列名 respColumns 以及其对应的数据类型 columnTypes
输出：计算得到的列名 respColumns 和数据类型 columnTypes
```

其具体实现逻辑如下：

1. 首先加入 `Device` 列，其数据类型为 `TEXT`；
2. 遍历未去重的 measurements 列表，判断当前遍历 measurement 的类型，如果是 `Exist` 类型则从 `measurementTypeMap` 中取得其类型；其余两种类型设其类型为 `TEXT`，然后将 measurement 及其类型加入表头数据结构中。
3. 根据中间变量 `deduplicatedMeasurements` 对 measurements 进行去重。

最终得到的 Header 为：

| Time | Device | s1  | 1   | s1  | s2  | s2  | s5  |
| ---- | ------ | --- | --- | --- | --- | --- | --- |
|      |        |     |     |     |     |     |     |

去重后的 `measurements` 为 `[s1, "1", s2, s5]`。

### 结果集生成

生成 ColumnHeader 后，最后一步为生成结果集填充结果并返回。

#### 结果集创建

- org.apache.iotdb.db.service.TSServiceImpl

该阶段需要调用 `TSServiceImpl.createQueryDataSet()` 创建一个新的结果集，这部分实现逻辑较为简单，对于 AlignByDeviceQuery 而言，只需要新建一个 `AlignByDeviceDataSet` 即可，在构造函数中将把 AlignByDevicePlan 中的参数赋值到新建的结果集中。

#### 结果集填充

- org.apache.iotdb.db.utils.QueryDataSetUtils

接下来需要填充结果，AlignByDeviceQuery 将调用 `TSServiceImpl.fillRpcReturnData()` 方法，然后根据查询类型进入 `QueryDataSetUtils.convertQueryDataSetByFetchSize()` 方法.

`convertQueryDataSetByFetchSize()` 方法中获取结果的重要方法为 QueryDataSet 的 `hasNext()` 方法。

`hasNext()` 方法的主要逻辑如下：

1. 判断是否有规定行偏移量 `rowOffset`，如果有则跳过需要偏移的行数；如果结果总行数少于规定的偏移量，则返回 false。
2. 判断是否有规定行数限制 `rowLimit`，如果有则比较当前输出行数，当前输出行数大于行数限制则返回 false。
3. 进入 `AlignByDeviceDataSet.hasNextWithoutConstraint()` 方法

<br>

- org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet

首先解释一下结果集中重要字段的含义：

- `deviceIterator`：按设备对齐查询本质上是计算出每个设备对应的映射值和过滤条件，然后将查询按设备分别进行，该字段即为设备的迭代器，每次查询获取一个设备进行。
- `currentDataSet`：该字段代表了本次对某设备查询所获得的结果集。

`hasNextWithoutConstraint()` 方法所做的工作主要是判断当前结果集是否有下一结果，没有则获取下一设备，计算该设备执行查询需要的路径、数据类型及过滤条件，然后按其查询类型执行具体的查询计划后获得结果集，直至没有设备可进行查询。

其具体实现逻辑如下：

1. 首先判断当前结果集是否被初始化且有下一个结果，如果是则直接返回 true，即当前可以调用 `next()` 方法获取下一个 `RowRecord`；否则设置结果集未被初始化进入步骤2.
2. 迭代 `deviceIterator` 获取本次执行需要的设备，之后 `deviceToMeasurementsMap` 中取得该设备对应的测点，得到 `executeColumns`.
3. 拼接当前设备名与 measurements，计算当前设备的查询路径、数据类型及过滤条件，得到对应的字段分别为 `executePaths`, `tsDataTypes`, `expression`，如果是聚合查询，则还需要计算 `executeAggregations`。
4. 判断当前子查询类型为 GroupByQuery, AggregationQuery, FillQuery 或 RawDataQuery 进行对应的查询并返回结果集，实现逻辑可参考[原始数据查询](/#/SystemDesign/progress/chap5/sec3)，[聚合查询](/#/SystemDesign/progress/chap5/sec4)，[降采样查询](/#/SystemDesign/progress/chap5/sec5)。

通过 `hasNextWithoutConstraint()` 方法初始化结果集并确保有下一结果后，则可调用 `QueryDataSet.next()` 方法获取下一个 `RowRecord`.

`next()` 方法主要实现逻辑为 `AlignByDeviceDataSet.nextWithoutConstraint()` 方法。

`nextWithoutConstraint()` 方法所做的工作主要是**将单个设备查询所得到的按时间对齐的结果集形式变换为按设备对齐的结果集形式**，并返回变换后的 `RowRecord`。

其具体实现逻辑如下：

1. 首先从结果集中取得下一个按时间对齐的 `originRowRecord`。
2. 新建一个添加了时间戳的 `RowRecord`，向其中加入设备列，先根据 `executeColumns` 与得到的结果建立一个由 `measurementName -> Field` 的 Map 结构 `currentColumnMap`.
3. 之后只需要遍历去重后的 `measurements` 列表，判断其类型，如果类型为 `Exist` 则根据 measurementName 从 `currentColumnMap` 中取得其对应的结果，如果没有则设为 `null`；如果是 `NonExist`类型，则直接设为 `null`; 如果是 `Constant` 类型，则将 `measureName` 作为该列的值。

再根据变换后的 `RowRecord` 写入输出数据流后，即可将结果集返回。
