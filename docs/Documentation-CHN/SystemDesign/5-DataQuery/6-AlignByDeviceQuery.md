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

按设备对齐查询其实现原理主要是计算出查询中每个设备对应的映射值和过滤条件，然后将查询按设备分别进行，最后将结果集拼装并返回。

### AlignByDevicePlan 中重要字段含义

首先解释一下 AlignByDevicePlan 中一些重要字段的含义：

- dataTypeMapping: 该变量继承自基类 QueryPlan，其主要作用是在计算每个设备的执行路径时，提供此次查询的 paths 对应的数据类型。在按设备对齐查询中，基类中的 paths 字段除了有验证的额外作用外，和 dataTypes 字段主要都是为这个字段服务。
- deviceToMeasurementsMap, deviceToFilterMap: 这两个字段分别用来存储设备对应的映射值和过滤条件。
- dataTypeConsistencyChecker：该字段主要用来验证不同设备的同名 sensor 的数据类型是否一致。如 `root.sg.d1.s1` 和 `root.sg.d2.s1` 应该是同一数据类型。
- groupByPlan, fillQueryPlan, aggregationPlan：为了避免冗余，这三个执行计划被设定为 RawDataQueryPlan 的子类，而在 AlignByDevicePlan 中被设置为变量。如果查询计划属于这三个计划中的一种，则该字段会被赋值并保存。

在进行具体实现过程的讲解前，先给出一个覆盖较为完整的例子，下面的解释过程中将结合该示例进行说明。

```sql
SELECT s1, "1", *, s2, s5 FROM root.sg.d1, root.sg.* WHERE time = 1 AND s1 < 25 ALIGN BY DEVICE
```

其中，存储组 `root.sg` 共包含两个设备 d1 和 d2，其中 d1 有两个传感器 s1 和 s2，d2 只有传感器 s1，相同传感器 s1 的数据类型相同。

下面将按具体过程进行分别解释：

### 逻辑计划生成

- org.apache.iotdb.db.qp.Planner

与原始数据查询不同，按设备对齐查询并不在此阶段进行 SELECT 语句和 WHERE 语句中后缀路径的拼接，而将在后续生成物理计划时，计算出每个设备对应的映射值和过滤条件。因此，按设备对齐在此阶段所做的工作只包括对 WHERE 语句中过滤条件的优化。

对过滤条件的优化主要包括三部分：去非、转化析取范式、合并同路径过滤条件。对应的优化器分别为：RemoveNotOptimizer, DnfFilterOptimizer, MergeSingleFilterOptimizer。该部分逻辑可参考：[Planner](/#/SystemDesign/progress/chap2/sec2).

### 物理计划生成

- org.apache.iotdb.db.qp.strategy.PhysicalGenerator

生成逻辑计划后，将调用 PhysicalGenerator 类中的 `transformToPhysicalPlan()` 方法将该逻辑计划转化为物理计划。对于按设备对齐查询，该方法的主要逻辑实现在 `transformQuery()` 方法中。

首先解释一下 `transformQuery()` 方法中一些重要字段的含义：

- prefixPaths, suffixPaths：前者为 FROM 子句中的前缀路径，示例中为 `root.sg.d1`, `root.sg.*`; 后者为 SELECT 子句中的后缀路径，示例中为 `s1`, `"1"`, `*`, `s2`, `s5`。
- devices：对 prefixPaths 进行去星和设备去重后得到的设备列表，示例中为 `[root.sg.d1, root.sg.d2]`。
- measurementSetOfGivenSuffix：记录该 suffix 对应的 measurements，示例中，对于后缀 *, `measurementSetOfGivenSuffix = {s1,s2}`，对于后缀 s1, `measurementSetOfGivenSuffix = {s1}`;

Measurement 共有三种类型，常量 Measurement，不存在的 Measurement 以及存在且非常量的 Measurement，下面将结合具体字段进行解释。

- `loc`：标记当前 Measurement 在 SELECT 后缀路径中的位置，如示例中常量`1`的位置为 1，而 `s5` 的位置为 5.
- `measurements`：存储实际存在且非常量的 Measurement，示例中为 `[s1,s1,s2,s2]`;
- `nonExistMeasurement`：存储不存在的 Measurement，注意其定义位置在第一层 suffixPaths 循环，且为 Set 类型，目的是不对同一个 suffix 下的重复 Measurement 添加重复记录，但可以对多次出现的同名 suffix 增加记录。其将在一个 suffix 循环结束后将 Set 集合内的元素一起添加到 AlignByDevicePlan 中; 示例中不存在的 Measurement 为 `s5`。
- `constMeasurement`：存储常量 Measurement。示例中为 `"1"`。

该阶段的主要工作包括：

1. 计算 AlignByDevicePlan 中所需要的重要参数值，包括 measurements, deviceToMeasurementsMap, deviceToFilterMap, dataTypeConsistencyChecker, notExistMeasurements, constMeasurements等。
2. 如果查询计划是 GroupByPlan, FillQueryPlan 或 AggregationPlan，则对 AlignByDevicePlan 对应的字段变量赋值，并更新查询计划的操作类型。
3. 将 WHERE 语句中的过滤条件与 FROM 语句中的前缀路径进行拼接，并将 Filter 转化为物理计划可以执行的 IExpression，保存为 deviceToFilterMap。

接下来介绍 AlignByDevicePlan 的转化过程：

1. 首先检查查询类型是否为 GroupByPlan, FillQueryPlan 或 AggregationPlan，如果是则先对 AlignByDevicePlan 中的变量进行赋值，并修改其对应的查询类型。以 GroupByPlan 为例，通过调用 `setGroupByPlan()` 方法对 AlignByDevicePlan 中的 GroupByPlan 进行赋值，并调用`setOperatorType(OperatorType.GROUPBY);` 将查询类型设置为降频聚合查询。
2. 接下来获取前缀路径 `prefixPaths` 及后缀路径 `suffixPaths`，以及包含的聚合类型。在拿到前缀路径时，直接调用 `removeStarsInDeviceWithUnique()` 方法对前缀路径进行解析并去重，最终得到非重复的设备列表 `devices`。
3. 然后遍历后缀路径 `suffixPaths` 和设备列表 `devices` 进行 Path 拼接，并对三种不同类型的 Measurement 进行处理，同时设置变量 loc 标记每个 Measurement 对应的位置。其中，如果后缀路径以单引号或双引号开头，则将其存储为常量 constMeasurement；如果拼接后的路径不存在，则将其存储为 nonExistMeasurement；否则视为正常 Measurement 进入步骤 4。特别的，对于一个设备中存在而另一个设备中不存在的 Measurement，示例中为 `s2`，会将其视为存在的 Measurement 存储到 `measurements` 而非 `nonExistMeasurement`.
4. 通过拼接 Path 得到 actualPaths 进行遍历。首先需要检验数据类型的一致性，不满足返回错误信息，满足则记录下该 Measurement，对 `measurementSetOfGivenSuffix`, `deviceToMeasurementsMap` 等进行更新。
5. 循环结束后，将循环中得到的变量信息赋值到 AlignByDevicePlan 中。
6. 最后调用 `concatFilterByDevice()` 方法计算 deviceToFilterMap，得到将每个设备分别拼接后对应的 Filter 信息。

```java
Map<String, IExpression> concatFilterByDevice(List<String> devices,
      FilterOperator operator)
输入：去重后的 devices 列表和未拼接的 FilterOperator
输入：经过拼接后的 deviceToFilterMap，记录了每个设备对应的 Filter 信息
```

`concatFilterByDevice()` 方法的主要处理逻辑在 `concatFilterPath()` 中：

`concatFilterPath()` 方法遍历未拼接的 FilterOperator 二叉树，判断节点是否为叶子节点，如果是，则取该叶子结点的路径，如果路径以 time 或 root 开头则不做处理，否则将设备名与节点路径进行拼接后返回；如果不是，则对该节点的所有子节点进行迭代处理。示例中，设备1过滤条件拼接后的结果为 `time = 1 AND root.sg.d1.s1 < 25`，设备2为 `time = 1 AND root.sg.d2.s1 < 25`。

### 输出列名 (ColumnHeader) 生成

- org.apache.iotdb.db.service.TSServiceImpl

在生成物理计划后，则可以执行 TSServiceImpl 中的 executeQueryStatement() 方法生成结果集并返回，其中第一步是生成输出列名。

按设备对齐查询在调用 `TSServiceImpl.getQueryColumnHeaders()` 方法后，根据查询类型进入 `TSServiceImpl.getAlignByDeviceQueryHeaders()` 来取得输出列名。

`getAlignByDeviceQueryHeaders()` 方法声明如下：

```java
private void getAlignByDeviceQueryHeaders(
      AlignByDevicePlan plan, List<String> respColumns, List<String> columnTypes)
输入：当前执行的物理计划 AlignByDevicePlan 和需要输出的列名 respColumns 以及其对应的数据类型 columnTypes
输出：计算得到的列名 respColumns 和数据类型 columnTypes
```

首先解释一下方法中所设置的指针变量的具体含义：

- `loc`: 代表从 SELECT 语句中得到的所有 measurements 的当前遍历位置。
- `notExistMeasurementsLoc`, `constMeasurementsLoc`, `resLoc`：分别代表了 AlignByDevicePlan 中 `notExistMeasurements`, `constMeasurements`, `measurements` 三个数组当前遍历到的位置。
- `shiftLoc`：代表了因为去重需要对 `positionOfNotExistMeasurements` 和 `positionOfConstMeasurements` 中存储的位置进行移动的大小。

其具体实现逻辑如下：

1. 首先加入 "Device" 列，其数据类型为 TEXT；
2. 遍历 SELECT 语句中的所有 measurements，判断当前遍历 Measurement 的类型，进行相应的处理。
3. 因为三种类型 measurements 的处理较为相似，因此仅以不存在的 Measurement 为例进行说明：如果当前遍历位置与 `positionOfNotExistMeasurements` 中存储的位置相吻合，则代表当前 Measurement 类型为不存在。首先检查是否因为去重需要改动存储的位置，如果需要则设新值为 `loc - shiftLoc`，然后获取当前 Measurement 值赋值给 `column`，移动指针的位置并设置 `isNonExist` 为 true。示例中，因为通配符 \* 导致 s1 和 s2 重复，则 `shiftLoc` 为 2，后续不存在的 Measurement `s5` 的位置需要从 5 提前到 3.
4. 将当前遍历得到的 `column` 及其数据类型添加进返回数组 respColumns 和 columnTypes 后，进入去重处理。
5. 如果当前遍历的 `column` 不存在，判断该 `column` 类型为不存在或常量则仅将其数据类型加入 `deduplicatedColumnsType`，为正常 Measurement 则同时将其加入 `deduplicatedMeasurementColumns` 中；如果是重复 `column`，则更新 `shiftLoc` 的值，如果是不存在或常量 Measurement，同时需要将重复值从存储数组中删除，并更新指针位置。
6. 遍历完成后将去重后的 measurements 及其 dataTypes 赋值，然后返回。

最终得到的 Header 为

| Time | Device | s1  | 1   | s1  | s2  | s2  | s5  |
| ---- | ------ | --- | --- | --- | --- | --- | --- |
|      |        |     |     |     |     |     |     |

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
- `currentColumnMapRelation`：该字段是本次查询的设备需要查询的 measurements 在总 measurements 中位置的映射。如，`root.sg.d2` 需要查询的 measurements 为 `{s1}`，而去重后总的 measurements 为 `{s1,s2}`，则此次查询 `currentColumnMapRelation` 为 `[0,-1]`。

`hasNextWithoutConstraint()` 方法所做的工作主要是判断当前结果集是否有下一结果，没有则获取下一设备，计算该设备执行查询需要的路径、数据类型及过滤条件，然后按其查询类型执行具体的查询计划后获得结果集，直至没有设备可进行查询。

其具体实现逻辑如下：

1. 首先判断当前结果集是否被初始化且有下一个结果，如果是则直接返回 true，即当前可以调用 `next()` 方法获取下一个 `RowRecord`；否则设置结果集未被初始化进入步骤2.
2. 迭代 `deviceIterator` 获取本次执行需要的设备，对比当前设备的 measurements 与去重后的总 measurements 计算 `currentColumnMapRelation`。
3. 拼接当前设备名与 measurements，计算当前设备的查询路径、数据类型及过滤条件，得到对应的字段分别为 `executePaths`, `tsDataTypes`, `expression`，如果是聚合查询，则还需要计算 `executeAggregations`。
4. 判断当前子查询类型为 GroupByQuery, AggregationQuery, FillQuery 或 RawDataQuery 进行对应的查询并返回结果集，实现逻辑可参考[原始数据查询](/#/SystemDesign/progress/chap5/sec3)，[聚合查询](/#/SystemDesign/progress/chap5/sec4)，[降采样查询](/#/SystemDesign/progress/chap5/sec5)。

通过 `hasNextWithoutConstraint()` 方法初始化结果集并确保有下一结果后，则可调用 `QueryDataSet.next()` 方法获取下一个 `RowRecord`.

`next()` 方法主要实现逻辑为 `AlignByDeviceDataSet.nextWithoutConstraint()` 方法。

`nextWithoutConstraint()` 方法所做的工作主要是**将单个设备查询所得到的按时间对齐的结果集形式变换为按设备对齐的结果集形式**，并返回变换后的 `RowRecord`。

其具体实现逻辑如下：

1. 首先从结果集中取得下一个按时间对齐的 `originRowRecord`。
2. 新建一个添加了时间戳的 `RowRecord`，向其中加入设备列，并根据 `currentColumnMapRelation` 中储存的映射关系将 `originRowRecord` 中的 measurements 顺序重新排列，如果不存在则设为 null。
3. 接下来处理 notExistMeasurements 和 constMeasurements，处理逻辑与上述 **输出列名生成** 阶段中的 `getAlignByDeviceQueryHeaders()` 方法较为相似，可参考该方法的逻辑说明，此处不再赘述。

在根据变换后的 `RowRecord` 写入输出数据流后，即可将结果集返回。
