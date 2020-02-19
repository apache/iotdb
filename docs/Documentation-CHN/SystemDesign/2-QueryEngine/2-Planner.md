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

# 执行计划生成器 Planner

* org.apache.iotdb.db.qp.Planner

将 SQL 解析出的语法树转化成逻辑计划，逻辑优化，物理计划。

## 相关概念

* 命题
    
    在数学逻辑中，为了表达现实世界中的事件时，需要将事件抽象为**命题**。在数据库的查询语句中，我们可以将选择条件抽象为命题，即 s<sub>1</sub> > 10 就是一个命题。命题总有一个“值”，该值要么是“真”，要么是“假”。命题可以由其他命题通过**联结词**进行连接构成。在查询语句中，这样的连接词包括否定(&not;)，合取(&and;)，析取(&or;)。不包含任何联结词的命题为**原子命题**，至少包含一个联结词的命题称作**复合命题**。

* 合取范式

    一个命题公式称为合取范式，当且仅当它具有以下形式：
    <center>A<sub>1</sub>&and;A<sub>2</sub>&and;...A<sub>n</sub>, n &ge; 1</center>
    其中A<sub>1</sub>, A<sub>2</sub>, ..., A<sub>n</sub>都由命题变元或其否定所组成的析取式。

* 析取范式

    一个命题公式称为析取范式，当且仅当它具有以下形式：
    <center>A<sub>1</sub>&or;A<sub>2</sub>&or;...A<sub>n</sub>, n &ge; 1</center>
    其中A<sub>1</sub>, A<sub>2</sub>, ..., A<sub>n</sub>都由命题变元或其否定所组成的合取式。

## SQL 解析

SQL 解析采用 Antlr4

* server/src/main/antlr4/org/apache/iotdb/db/qp/strategy/SqlBase.g4

mvn clean compile 之后生成代码位置：server/target/generated-sources/antlr4

## 逻辑计划生成器

* org.apache.iotdb.db.qp.strategy.LogicalGenerator

## 逻辑计划优化器

目前有四种逻辑计划优化器

* org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer

	路径优化器，将 SQL 中的查询路径进行拼接，与 MManager 进行交互去掉通配符，进行路径检查。

* org.apache.iotdb.db.qp.strategy.optimizer.RemoveNotOptimizer

	谓词去非优化器，将谓词逻辑中的非操作符去掉。

* org.apache.iotdb.db.qp.strategy.optimizer.DnfFilterOptimizer

	将谓词转化为析取范式。

* org.apache.iotdb.db.qp.strategy.optimizer.MergeSingleFilterOptimizer

	将相同路径的谓词逻辑合并。

### ConcatPathOptimizer

ConcatPathOptimizer 使用其中的 transform() 方法将给定查询中 FROM 子句中的前缀路径与 SELECT 子句， WHERE 子句中的后缀路径进行拼接。该方法的申明如下：

    Operator transform(Operator operator)
    输入：待转化的 SFWOperator 
    输出：路径经过连接处理后的 SFWOperator


该方法的主要步骤如下：

1. 取出 FROM 子句和 SELECT 子句中的路径，前者称为前缀路径，后者称为后缀路径。进入步骤2.
2. 判断该查询是否包含 ALIGN BY DEVICE 子句。如果不包含，则可进入步骤3；否则，进入步骤4.
3. 则调用 concatSelect() 方法将前缀路径和后缀路径合并补全（包括带 * 的路径），形成全路径。如果查询中包含 SLIMIT 子句，则对 SELECT 子句中的路径做相应的筛选。进入步骤5.
4. 对每一条 FROM 子句中的路径分析其设备名，如果不为空则抛出异常。进入步骤5.
5. 对 WHERE 子句过滤条件中包含的后缀路径做补全。完成后，SELECT 子句和 WHERE 子句中的路径都是完整的路径。

### RemoveNotOptimizer

RemoveNotOptimizer 类中的 removeNot() 和 reverseFilter() 方法共同实现了删去 NOT 关键字的功能。removeNot() 方法的申明如下：

    FilterOperator removeNot(FilterOperator filter)
    输入：待优化的可能含有 NOT 关键字的谓词
    输出：优化后，不包含 NOT 关键字的谓词

该方法的输入为待优化的谓词，并且递归地优化该谓词中的每个子部分。将输入的谓词看作一棵二叉树，其中叶子节点是 BasicFunctionOperator （如 s0 > 10），非叶子节点是“与”、“或”、“非”操作符。 方法的逻辑如下：

1. 判断该节点是否为叶子节点。如果是叶子节点则返回该节点。如果不是叶子节点则进入步骤2.
2. 判断关系类型。如果是“与”、“或”关系，则进入步骤3；如果是“非”关系，则进入步骤4；如果不是“与”、“或”、“非”关系，则抛出异常。
3. 此时的“与”、“或”关系一定包含2个孩子节点。对左右孩子递归调用 removeNot() 方法，去除左右子树中的 NOT 关键词，再将该节点返回。
4. 对该节点调用 reverseFilter() 方法，取反，并且去除 NOT 关键字。

reverseFilter() 方法的申明如下：

    FilterOperator reverseFilter(FilterOperator filter)
    输入：待取反的节点
    输出：该节点取反后的结果

reverseFilter() 方法的逻辑如下：

1. 判断节点是否为叶子节点，如果是，则调用该节点自身的 reverseFunc() 方法取反。其中，reverseFunc() 方法只用设定的 Map 来对关键字取反，如将 < 映射为 >= ,将 AND 映射为 OR。如果不是叶子节点，则进入步骤2.
2. 如果不是叶子节点，分两种情况。如果是“与”、“或”关系，则进入步骤3；如果是“非”关系则进入步骤4；如果既不是“与”、“或”关系，也不是“非”关系，则抛出异常。
3. 对该节点的左右子树递归调用 reverseFilter() 方法，得到两个取反后的子树，并将该节点返回。
4. 对孩子节点调用 removeNot() 方法，并将去除NOT关键字的孩子节点返回。

removeNot() 和 reverseFilter() 之间存在一定的耦合关系。removeNot() 访问非 NOT 关系的节点，确保不含有 NOT 关键字；reverseFilter() 访问 NOT 关系节点以及子节点，并进行取反转换，删去 NOT 关键字。

### DnfFilterOptimizer

DNF 是 Disjuctive Normal Form 的缩写，即析取范式。DnfFilterOptimizer 中的 optimize() 方法来依靠 getDnf() 来实现，对过滤条件进行优化，将过滤条件转化为析取范式的形式。该方法的申明如下：

    FilterOperator getDnf(FilterOperator filter)
    输入：待优化的 FilterOperator
    输出：优化后的 FilterOperator，以析取范式为形式

该方法的步骤如下：

1. 如果当前节点是叶子节点，则返回该节点
2. 如果当前节点表示关系“或”，则
    1. 获取当前节点所有的子节点（子节点数目为2），并对左右子节点分别递归地调用 getDnf() 方法，使得左右字节点都为析取范式。
    2. 对该节点的左子节点和右子节点分别分析。如果子节点是叶子节点或表示关系“与”，则保持不变；否则，说明该子节点的关系为“或”，则将子节点的所有子节点也添加到当前节点的子节点中。即 (OR (OR A, B), C) 转换为 (OR A, B, C)。
    3. 返回当前节点。
3. 如果当前节点表示关系“与”，则
    1. 获取当前节点所有的子节点（子节点数目为2），并对左右子节点分别递归地调用 getDnf() 方法，使得左右字节点都为析取范式。    
    2. 对该节点的左子节点和右子节点分别分析。    
        1. 如果左右子节点都不是“或”关系，则说明左右子节点要么是叶子节点，要么是关系“与”，如果是叶子节点，则保持不变，如果是关系“与”，则将该子节点的子节点添加到当前节点的孩子节点中，如 (AND A, (AND B, C)) 就转化为 (AND A, B, C)。
        2. 如果左右子节点包含为两个“或”关系，将其中一个子节点的两个子节点标记为 A 和 B ，另一子节点的两个子节点标记为 C 和 D，则将 {A, B} 与 {C, D} 两个集合中的元素两两组合，(A, C), (A, D), (B, C), (B, D), 对每一组组合调用 mergeToConjunction() 方法生成合取式作为当前节点的子节点，并将当前节点设为关系“或”，即，把 (AND (OR A, B), (OR C, D)) 转换为 (OR (AND A, C), (AND A, D), (AND B, C), (AND B, D))。
        3. 如果左右子节点包含一个“或”关系，一个“与”关系或叶子节点。将“或”子节点的两个子节点标记为 X, Y，将另一子节点标记为 Z，则只要将 {X, Y} 与 {Z} 集合种的元素两两组合，对 (X, Z), (Y,Z) 调用 mergeToConjunctive() 方法生成合取式作为当前节点的子节点，并将当前节点设为关系“或”，即，把 (AND (OR X, Y), Z) 转换为 (OR (AND X, Z), (AND Y, Z))。
    3. 返回当前节点。


对于以上提到的 mergeToConjunctive() 方法，申明如下：

    FilterOperator mergeToConjunction(FilterOperator operator1, FilterOperator operator2)
    输入：两个子 FilterOperator
    输出：合并后的合取式

该方法生成关系“与”，并将输入的两个 FilterOperator 作为其两个子节点。


### MergeSingleFilterOptimizer


MergeSingleFilterOptimizer 类主要通过 mergeSamePathFilter() 方法对节点进行合并。该方法的申明如下：

    Path mergeSamePathFilter(FilterOperator filter)
    输入：待转换（合并）的 Filter
    输出：处理后（合并）的 Filter

该方法的步骤如下：

1. 如果该节点是叶子节点，则直接返回节点中包含的路径，进入步骤2.
2. 对当前节点依次递归地使用 mergeSamePathFilter() 方法访问子节点并获取子节点所表示的路径，如果子节点路径不同，则将当前节点所表示的路径设为 null，进入步骤3；否则，设为子节点的公共路径，并返回该路径。
3. 如果子节点均为 BasicFunctionOperator，则对子节点按照路径进行排序。进入步骤4.
4. 对当前节点调用 mergeSingleFilters() 方法获得当前节点内子节点表示的路径非 null 的最大序号。该操作会将子节点中路径相同的节点合并，同时保留路径非 null 子节点。
5. 将剩余的路径为 null 的节点依次设为当前节点的子节点。

mergeSingleFilter() 方法将子节点中路径相同的节点进行合并。具体实现上，将生成新的 FilterOperator 作为子节点，并将操作符类型与当前节点设为相同，再将那些路径相同的子节点设为新节点的子节点，最后返回子节点中非 null 的最大序号。例如，下图表示合并同路径过滤条件的过程（圆形表示非叶子节点，长方形表示叶子节点。冒号前表示节点的类别或过滤条件，冒号后表示当前节点存储的路径）：

![image](https://user-images.githubusercontent.com/36235611/74820835-a5eb6780-533d-11ea-8173-6591e69fab5a.jpg)

### 举例说明

假设输入的 SQL 语句为 <center>SELECT * FROM root.v0.d0 WHERE (NOT time<200) AND (s1 < 10 OR s2 > 50 OR s1 > 20)</center>

则生成的选择条件树状结构如下：

![image](https://user-images.githubusercontent.com/36235611/74821007-e8ad3f80-533d-11ea-9b5e-7565cf2d4fc3.jpg)

## 物理计划生成器

* org.apache.iotdb.db.qp.strategy.PhysicalGenerator

### TransformToPhysicalPlan()

PhysicalGenerator 类中的 transformToPhysicalPlan() 方法将 Planner 中生成的逻辑计划转化为物理计划。其声明如下：

```java
PhysicalPlan transformToPhysicalPlan(Operator operator)
输入：Planner 中经过优化的逻辑计划 operator
输出：通过解析 operator 生成的物理计划
```

transformToPhysicalPlan() 方法的逻辑较为简单，即通过 operator.getType() 方法判断逻辑计划的类型，然后创建相对应的物理计划。

其中只有 Query 类型需要额外调用 transformQuery() 方法进行进一步转化，其余类型均可以直接创建物理计划。

#### TransformQuery()

transformQuery() 方法主要分为两部分，一部分是原始数据查询 RawDataQueryPlan 的转化（按时间对齐），另一部分是按设备对齐 AlignByDevicePlan 的转化。

RawDataQueryPlan 有三个子类，分别为 GroupByPlan、FillQueryPlan 和 AggregationPlan。在分开处理 RawDataQueryPlan 和 AlignByDevicePlan 前，首先处理这三个子类，将需要的参数保存供之后使用。

以 GroupByPlan 为例，其中调用了 setInterval()、setSlidingStep()、setStartTime()、setEndTime() 方法存储了 GroupByPlan 的时间间隔、步长、开始时间、结束时间等参数。

##### AlignByDevicePlan 转化

首先解释一下 AlignByDevicePlan 中一些重要字段的作用。

- prefixPaths：FROM 子句中的前缀路径，如 root.*.*, root.sg.d1;
- devices：对 prefixPaths 进行去星和设备去重后得到的设备列表;
- suffixPaths：SELECT 子句中的后缀路径，如 s0, temperature, *;
- measurements：存储实际存在且非常量的 Measurement，在 measurementSetOfGivenSuffix 所举示例中，measurements = [s1,s2,s3,s1];
- deviceToMeasurementsMap：存储每个设备对应的 measurements;
- dataTypeConsistencyChecker：检验不同设备的同名 Measurement 的数据类型一致性，如 root.sg1.d1.s0 为 INT32 类型而 root.sg2.d3.s0 为 FLOAT 类型则不满足一致性;
- loc：标记当前 Measurement 在 SELECT 后缀路径中的位置;
- nonExistMeasurement：存储不存在的 Measurement，注意其定义位置在第一层 suffixPaths 循环，且为 Set 类型，目的是不对同一个 suffix 下的重复 Measurement 添加重复记录，但可以对多次出现的同名 suffix 增加记录。其将在一个 suffix 循环结束后将 Set 集合内的元素一起添加到 AlignByDevicePlan 中;
- measurementSetOfGivenSuffix：记录该 suffix 对应的 measurements，如 select *,s1 from root.sg.d0, root.sg.d1，对于后缀 *, measurementSetOfGivenSuffix = {s1,s2,s3}，对于后缀 s1, measurementSetOfGivenSuffix = {s1};

接下来介绍 AlignByDevicePlan 的转化过程：

为了避免冗余，AlignByDevicePlan 将 GroupByPlan 等三个子类查询设置为其中的变量而非其子类，因此如果查询类型是三种子类查询，则需要先对 AlignByDevicePlan 中的变量进行赋值，并修改其对应的查询类型。

同样以 GroupByPlan 为例，通过调用 setGroupByPlan() 方法对 AlignByDevicePlan 中的 GroupByPlan 进行赋值，并调用 setOperatorType(OperatorType.GROUPBY); 将查询类型设置为降频聚合查询。

接下来对 AlignByDevicePlan 的处理主要包括：

1. 首先获取 FROM 子句中的前缀路径，SELECT 子句中的后缀路径，以及包含的聚合类型。在拿到前缀路径 prefixPaths 时，直接调用 removeStarsInDeviceWithUnique() 方法对前缀路径进行解析并去重，最终得到非重复的设备列表 devices。
2. 然后遍历后缀路径和设备列表进行 Path 拼接，并对三种不同类型的 Measurement 进行处理，同时设置变量 loc 标记每个 Measurement 对应的位置。其中，如果后缀路径以单引号或双引号开头，则将其存储为常量 constMeasurement；如果拼接后的路径不存在，则将其存储为 nonExistMeasurement；否则视为正常 Measurement 进入步骤3。
3. 通过拼接 Path 得到 actualPaths 进行遍历。首先需要检验数据类型的一致性，不满足返回错误信息，满足则记录下该 Measurement，对 measurementSetOfGivenSuffix, deviceToMeasurementsMap 等进行更新。
4. 循环结束后，将循环中得到的变量信息赋值到 AlignByDevicePlan 中。
5. 最后调用 concatFilterByDevice() 方法计算 deviceToFilterMap，得到将每个设备分别拼接后对应的 Filter 信息。

```java
Map<String, IExpression> concatFilterByDevice(List<String> devices,
      FilterOperator operator)
输入：去重后的 devices 列表和未拼接的 FilterOperator
输入：经过拼接后的 deviceToFilterMap，记录了每个设备对应的 Filter 信息
```

concatFilterByDevice() 方法的主要处理逻辑在 concatFilterPath() 中：

concatFilterPath() 方法遍历未拼接的 FilterOperator 二叉树，判断节点是否为叶子节点，如果是，则取该叶子结点的路径，如果路径以 time 或 root 开头则不做处理，否则将设备名与节点路径进行拼接后返回；如果不是，则对该节点的所有子节点进行迭代处理。

##### RawDataQueryPlan 转化

RawDataQueryPlan 的转化相对较为简单，因为拼接过程已经在 ConcatPathOptimizer 中完成，因此对 RawDataQueryPlan 的处理只需要将信息从 queryOperator 中取出并赋值即可。

需要特殊说明的是 generateDataTypes() 方法和 deduplicate() 方法。

generateDataTypes() 方法生成 paths 对应的数据类型，并存储为 dataTypeMap 记录每个 Path 对应的数据类型。deduplicate() 方法对 paths 进行去重，生成去重后的 deduplicatedPaths 和 deduplicatedDataTypes。

其中，generateDataTypes() 对 AlignByDevicePlan 和 RawDataQueryPlan 均作了处理，而 deduplicate() 只对 RawDataQueryPlan 作了处理，是因为 RawDataQueryPlan 依赖于去重后的路径和数据类型进行执行，而 AlignByDevicePlan 按设备分别进行执行，每个设备执行所需的路径和数据类型均在结果集 AlignByDeviceDataSet 的 hasNextWithoutConstraint() 方法中计算产生，因此不需要对整个的 paths 进行去重，generateDataTypes() 生成的 dataTypeMap 也是为了计算时使用。
