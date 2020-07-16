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

# TsFile 读流程

本章节介绍 TsFile 的读取流程，内容总体上分为两部分，对过滤条件和时间表达式的介绍，以及对查询流程的详细介绍。

* 1 过滤条件和查询表达式
    * 1.1 Filter
    * 1.2 Expression表达式
        * 1.2.1 SingleSeriesExpression 表达式
        * 1.2.2 GlobalTimeExpression 表达式
        * 1.2.3 IExpression 表达式
        * 1.2.4 可执行表达式
        * 1.2.5 IExpression 转化为可执行表达式的优化算法
* 2 TsFile 查询执行过程
    * 2.1 设计原理
    * 2.2 三大查询组件
        * 2.2.1 FileSeriesReader 组件
        * 2.2.2 FileSeriesReaderByTimestamp 组件
        * 2.2.3 TimeGeneratorImpl 组件
    * 2.3 归并查询
    * 2.4 连接查询
    * 2.5 查询入口
    * 2.6 相关代码介绍

## 1 过滤条件和查询表达式

本章节首先介绍 Tsfile 文件读取时需要用到的过滤条件和查询表达式的相关定义；其次介绍如何将用户输入的过滤条件转化为系统可以执行的查询条件。

### 1.1 Filter

Filter 表示基本的过滤条件。用户可以在时间戳上、或某一列的值上给出具体的过滤条件。将时间戳和列值的过滤条件加以区分后，设 t 表示某一时间戳常量，Filter 有以下12种基本类型，在实现上是继承关系。

Filter|类型|含义|示例
----|----|---|------
TimeEq|时间过滤条件|时间戳等于某个值|TimeEq(t)，表示时间戳等于 t 
TimeGt|时间过滤条件|时间戳大于某个值|TimeGt(t)，表示时间戳大 t
TimeGtEq|时间过滤条件|时间戳大于等于某个值|TimeGtEq(t)，表示时间戳大于等 t
TimeLt|时间过滤条件|时间戳小于某个值|TimeLt(t)，表示时间戳小 t
TimeLtEq|时间过滤条件|时间戳小于等于某个值|TimeLtEq(t)，表示时间戳小于等 t
TimeNotEq|时间过滤条件|时间戳不等于某个值|TimeNotEq(t)，表示时间戳不等 t
ValueEq|值过滤条件|该列数值等于某个值|ValueEq(2147483649)，表示该列数值等于2147483649
ValueGt|值过滤条件|该列数值大于某个值|ValueGt(100.5)，表示该列数值大于100.5
ValueGtEq|值过滤条件|该列数值大于等于某个值|ValueGtEq(2)，表示该列数值大于等于2
ValueLt|值过滤条件|该列数值小于某个值|ValueLt("string")，表示该列数值字典序小于"string"
ValueLtEq|值过滤条件|该列数值小于等于某个值|ValueLtEq(-100)，表示该列数值小于等于-100
ValueNotEq|值过滤条件|该列数值不等于某个值|ValueNotEq(true)，表示该列数值的值不能为true

Filter 可以由一个或两个子 Filter 组成。如果 Filter 由单一 Filter 构成，则称之为一元过滤条件，及 UnaryFilter 。若包含两个子 Filter，则称之为二元过滤条件，及 BinaryFilter。在二元过滤条件中，两个子 Filter 之间通过逻辑关系“与”、“或”进行连接，前者称为 AndFilter，后者称为 OrFilter。AndFilter 和 OrFilter 都是二元过滤条件。UnaryFilter 和 BinaryFilter 都是 Filter。

下面给出一些 AndFilter 和 OrFilter 的示例，其中“&&”表示关系“与”，“||”表示关系“或”。

1. AndFilter(TimeGt(100), TimeLt(200)) 表示“timestamp > 100 && timestamp < 200”
2. AndFilter (TimeGt(100), ValueGt(0.5)) 表示“timestamp > 100 && value > 0.5”
3. AndFilter (AndFilter (TimeGt(100), TimeLt(200)), ValueGt(0.5)) 表示“(timestamp > 100 && timestamp < 200) && value > 0.5”
4. OrFilter(TimeGt(100), ValueGt(0.5)) 表示“timestamp > 100 || value > 0.5”
5. OrFilter (AndFilter(TimeGt(100), TimeLt(200)), ValueGt(0.5)) 表示“(timestamp > 100 && timestamp < 200) || value > 0.5”

下面，给出“Filter”、“AndFilter”和“OrFilter”的形式化定义：

    Filter := Basic Filter | AndFilter | OrFilter
    AndFilter := Filter && Filter
    OrFilter := Filter || Filter

为了便于表示，下面给出 Basic Filter、AndFilter 和 OrFilter 的符号化表示方法，其中 t 表示数据类型为 INT64 的变量；v表示数据类型为 BOOLEAN、INT32、INT64、FLOAT、DOUBLE 或 BINARY 的变量。

<style> table th:nth-of-type(2) { width: 150px; } </style>
名称|符号化表示方法|示例
----|------------|------
TimeEq| time == t| time == 14152176545，表示 timestamp 等于 14152176545 
TimeGt| time > t| time > 14152176545，表示 timestamp 大于 14152176545
TimeGtEq| time >= t| time >= 14152176545，表示 timestamp 大于等于 14152176545
TimeLt| time < t| time < 14152176545，表示 timestamp 小于 14152176545
TimeLtEq| time <= t| time <= 14152176545，表示 timestamp 小于等于 14152176545
TimeNotEq| time != t| time != 14152176545，表示 timestamp 等于 14152176545
ValueEq| value == v| value == 10，表示 value 等于 10
ValueGt| value > v| value > 100.5，表示 value 大于 100.5
ValueGtEq| value >= v| value >= 2，表示 value 大于等于 2
ValueLt| value < v| value < “string”，表示 value [1e小于“string”
ValueLtEq| value <= v| value <= -100，表示 value 小于等于-100
ValueNotEq| value != v| value != true，表示 value 的值不能为true
AndFilter| \<Filter> && \<Filter>| 1. value > 100 && value < 200,表示 value大于100且小于200； <br>2. (value >= 100 && value <= 200) && time > 14152176545,表示“value 大于等于100 且 value 小于等于200” 且 “时间戳大于 14152176545”
OrFilter| \<Filter> &#124;&#124; \<Filter>| 1. value > 100 &#124;&#124; time >  14152176545，表示value大于100或时间戳大于14152176545；<br>2. (value > 100 && value < 200)&#124;&#124; time > 14152176545，表示“value大于100且value小于200”或“时间戳大于14152176545”

### 1.2 Expression表达式

当一个过滤条件作用到一个时间序列上，就成为一个表达式。例如，“数值大于10” 是一个过滤条件；而 “序列 d1.s1 的数值大于10” 就是一条表达式。特殊地，对时间的过滤条件也是一个表达式，称为 GlobalTimeExpression。以下章节将对表达式进行展开介绍。

#### 1.2.1 SingleSeriesExpression表达式

SingleSeriesExpression 表示针对某一指定时间序列的过滤条件，一个 SingleSeriesExpression 包含一个 Path 和一个 Filter。Path 表示该时间序列的路径；Filter 即为2.1章节中介绍的 Filter，表示相应的过滤条件。

SingleSeriesExpression 的结构如下：

    SingleSeriesExpression
        Path: 该 SingleSeriesExpression 指定的时间序列的路径
        Filter：过滤条件

在一次查询中，一个 SingleSeriesExpression 表示该时间序列的数据点必须满足 Filter所表示的过滤条件。下面给出 SingleSeriesExpression 的示例及对应的表示方法。

例1. 

    SingleSeriesExpression
        Path: "d1.s1"
        Filter: AndFilter(ValueGt(100), ValueLt(200))

该 SingleSeriesExpression 表示"d1.s1"这一时间序列必须满足条件“值大于100且值小于200”。

其符号化的表达方式为：SingleSeriesExpression(“d1.s1”, value > 100 && value < 200)

---------------------------
例2. 
    
    SingleSeriesExpression
        Path：“d1.s1”
        Filter：AndFilter(AndFilter(ValueGt(100), ValueLt(200)), TimeGt(14152176545))
    
该 SingleSeriesExpression 表示"d1.s1"这一时间序列必须满足条件“值大于100且小于200且时间戳大于14152176545”。
    
其符号化表达方式为：SingleSeriesExpression(“d1.s1”, (value > 100 && value < 200) && time > 14152176545)

#### 1.2.2 GlobalTimeExpression 表达式
GlobalTimeExpression 表示全局的时间过滤条件，一个 GlobalTimeExpression 包含一个 Filter，且该 Filter 中包含的子 Filter 必须全为时间过滤条件。在一次查询中，一个 GlobalTimeExpression 表示查询返回数据点必须满足该表达式中 Filter 所表示的过滤条件。GlobalTimeExpression 的结构如下：


    GlobalTimeExpression
        Filter: 由一个或多个时间过滤条件组成的 Filter。
        此处的Filter形式化定义如下：
            Filter := TimeFilter | AndExpression | OrExpression
            AndExpression := Filter && Filter
            OrExpression := Filter || Filter

下面给出 GlobalTimeExpression 的一些例子，均采用符号化表示方法。
1. GlobalTimeExpression(time > 14152176545 && time < 14152176645)表示所有被选择的列的时间戳必须满足“大于14152176545且小于14152176645”
2. GlobalTimeExpression((time > 100 && time < 200) || (time > 400 && time < 500))表示所有被选择列的时间戳必须满足“大于100且小于200”或“大于400且小于500”

#### 1.2.3 IExpression 表达式
IExpression 为查询过滤条件。一个 IExpression 可以是一个 SingleSeriesExpression 或者一个 GlobalTimeExpression，这种情况下，IExpression 也称为一元表达式，即 UnaryExpression。一个 IExpression 也可以由两个 IExpression 通过逻辑关系“与”、“或”进行连接得到 “AndExpression” 或 “OrExpression” 二元表达式，即 BinaryExpression。

下面给出 IExpression 的形式化定义。

    IExpression := SingleSeriesExpression | GlobalTimeExpression | AndExpression | OrExpression
    AndExpression := IExpression && IExpression
    OrExpression := IExpression || IExpression

我们采用一种类似于树形结构的表示方法来表示 IExpression，其中 SingleSeriesExpression 和 GlobalTimeExpression 均采用上文中介绍的符号化表示方法。下面给出示例。

1. 只包含一个 SingleSeriesExpression 的 IExpression：
   
        IExpression(SingleSeriesExpression(“d1.s1”, value > 100 && value < 200))

2. 只包含一个 GlobalTimeExpression 的 IExpression：

        IExpression(GlobalTimeExpression (time > 14152176545 && time < 14152176645))
3. 包含多个 SingleSeriesExpression 的 IExpression：

        IExpression(
            AndExpression
                SingleSeriesExpression(“d1.s1”, (value > 100 && value < 200) || time > 14152176645)
                SingleSeriesExpression(“d1.s2”, value > 0.5 && value < 1.5)
        )

    **解释**：该 IExpression 为一个 AndExpression，其中要求"d1.s1"和"d1.s2"必须同时满足其对应的 Filter。

4. 同时包含 SingleSeriesExpression 和 GlobalTimeExpression 的 IExpression

        IExpression(
            AndExpression
                AndExpression
                    SingleSeriesExpression(“d1.s1”, (value > 100 && value < 200) || time > 14152176645)
                    SingleSeriesExpression(“d1.s2”, value > 0.5 && value < 1.5)
                GlobalTimeExpression(time > 14152176545 && time < 14152176645)
        )

    **解释**：该 IExpression 为一个 AndExpression，其要求"d1.s1"和"d1.s2"必须同时满足其对应的 Filter，且时间列必须满足 GlobalTimeExpression 定义的 Filter 条件。


#### 1.2.4 可执行表达式

便于理解执行过程，定义可执行表达式的概念。可执行表达式是带有一定限制条件的 IExpression。用户输入的查询条件或构造的 IExpression 将经过特定的优化算法（该算法将在后面章节中介绍）转化为可执行表达式。满足下面任意条件的 IExpression 即为可执行表达式。

* 1. IExpression 为单一的 GlobalTimeExpression
* 2. IExpression 为单一的 SingleSeriesExpression
* 3. IExpression 为 AndExpression，且叶子节点均为 SingleSeriesExpression
* 4. IExpression 为 OrExpression，且叶子节点均为 SingleSeriesExpression

可执行表达式的形式化定义为：

    executable expression := SingleSeriesExpression| GlobalTimeExpression | AndExpression | OrExpression
    AndExpression := < ExpressionUNIT > && < ExpressionUNIT >
    OrExpression := < ExpressionUNIT > || < ExpressionUNIT >
    ExpressionUNIT := SingleSeriesExpression | AndExpression | OrExpression

下面给出 一些可执行表达式和非可执行表达式的示例：

例1：

    IExpression(SingleSeriesExpression(“d1.s1”, value > 100 && value < 200))

是否为可执行表达式：是

**解释**：该 IExpression 为一个 SingleSeriesExpression，满足条件2

----------------------------------
例2：

    IExpression(GlobalTimeExpression (time > 14152176545 && time < 14152176645))

是否为可执行表达式：是

**解释**：该 IExpression 为一个 GlobalTimeExpression，满足条件1

-----------------------
例3：

    IExpression(
        AndExpression
            GlobalTimeExpression (time > 14152176545)
            GlobalTimeExpression (time < 14152176645)
    )

是否为可执行表达式：否

**解释**：该 IExpression 为一个 AndExpression，但其中包含了 GlobalTimeExpression，不满足条件3

--------------------------

例4：

    IExpression(
        OrExpression
            AndExpression
                SingleSeriesExpression(“d1.s1”, (value > 100 && value < 200) || time > 14152176645)
                SingleSeriesExpression(“d1.s2”, value > 0.5 && value < 1.5)
        SingleSeriesExpression(“d1.s3”, value > “test” && value < “test100”)
    )

是否为可执行表达式：是

**解释**：该 IExpression 作为一个 OrExpression，其中叶子结点都是 SingleSeriesExpression，满足条件4.

----------------------------

例5：

    IExpression(
        AndExpression        
            AndExpression
                SingleSeriesExpression(“d1.s1”, (value > 100 && value < 200) || time > 14152176645)
                SingleSeriesExpression(“d1.s2”, value > 0.5 && value < 1.5)
            GlobalTimeExpression(time > 14152176545 && time < 14152176645)
    )

是否为可执行表达式：否

**解释**：该 IExpression 为一个 AndExpression，但其叶子结点中包含了 GlobalTimeExpression，不满足条件3

#### 1.2.5 IExpression转化为可执行表达式的优化算法

本章节介绍将 IExpression 转化为一个可执行表达式的算法。

如果一个 IExpression 不是一个可执行表达式，那么它一定是一个 AndExpression 或者 OrExpression，且该 IExpression 既包含了 GlobalTimeExpression 又包含了 SingleSeriesExpression。根据前面章节的定义，我们知道 AndExpression 和 OrExpression 均由两个 IExpression 构成，即

    AndExpression := <IExpression> AND <IExpression>
    OrExpression := <IExpression> OR <IExpression>

令左右两侧的表达式分别为 LeftIExpression 和 RightIExpression，即

    AndExpression := <LeftIExpression> AND <RightIExpression>
    OrExpression := <LeftIExpression> OR <RightIExpression>

下面给出算法定义：

    IExpression optimize(IExpression expression, List<Path> selectedSeries)

    输入：待转换的 IExpression 表达式，需要投影的时间序列
    输出：转换后的 IExpression，即可执行表达式

在介绍优化算法的具体步骤之前，我们首先介绍表达式、过滤条件合并基本的方法。这些方法将在 optimize() 方法中使用。

* MergeFilter: 合并两个 Filter。该方法接受三个参数，分别为：

        Filter1：第一个待合并的 Filter
        Filter2：第二个待合并的 Filter
        Relation：两个待合并 Filter 之间的关系（ relation 的取值为 AND 或 OR）

    则，该方法执行的策略为

        if relation == AND:
            return AndFilter(Filter1, Filter2)
        else if relation == OR:
            return OrFilter(Filter1, Filter2)

    算法实现是，使用 FilterFactory 类中的 AndFilter and(Filter left, Filter right) 和 OrFilter or(Filter left, Filter right)方法进行实现。
    
* combineTwoGlobalTimeExpression: 将两个 GlobalTimeExpression 合并为一个 GlobalTimeExpression。
  
  该方法接受三个输入参数，方法的定义为：

        GlobalTimeExpression combineTwoGlobalTimeExpression(
            GlobalTimeExpression leftGlobalTimeExpression,
            GlobalTimeExpression rightGlobalTimeExpression,
            ExpressionType type)

        输入参数1：leftGlobalTimeExpression，左侧表达式
        输入参数2：rightGlobalTimeExpression，右侧表达式
        输入参数3：type，表达式二元运算类型，为“AND”或“OR”

        输出：GlobalTimeExpression，最终合并后的表达式
    
    该方法分为两个步骤：
    1. 设 leftGlobalTimeExpression 的 Filter 为 filter1；rightGlobalTimeExpression 的 Filter 为 filter2，通过 MergeFilter 方法将其合并为一个新的Filter，设为 filter3。
    2. 创建一个新的 GlobalTimeExpression，并将 filter3 作为其 Filter，返回该 GlobalTimeExpression。

    下面给出一个合并两个 GlobalTimeExpression 的例子。


    三个参数分别为：

        leftGlobalTimeExpression：GlobalTimeExpression(Filter: time > 100 && time < 200)
        rightGlobalTimeExpression: GlobalTimeExpression(Filter: time > 300 && time < 400)
        type: OR

    则，合并后的结果为

        GlobalTimeExpression(Filter: (time > 100 && time < 200) || (time > 300 && time < 400))

* handleOneGlobalExpression: 将 GlobalTimeExpression 和 IExpression 合并为一个可执行表达式。该方法返回的可执行表达式仅由 SingleSeriesExpression 组成。方法的定义如下：

        IExpression handleOneGlobalTimeExpression(
            GlobalTimeExpression globalTimeExpression,
            IExpression expression,
            List<Path> selectedSeries, 
            ExpressionType relation)

        输入参数1：GlobalTimeExpression
        输入参数2：IExpression
        输入参数3：被投影的时间序列
        输入参数4：两个待合并的表达式之间的关系，relation 的取值为 AND 或 OR

        输出：合并后的 IExpression，即为可执行表达式。

    该方法首先调用 optimize() 方法，将输入的第二个参数 IExpression 转化为可执行表达式（从 optimize() 方法上看为递归调用），然后再分为两种情况进行合并。

    *情况一*：GlobalTimeExpression 和优化后的 IExpression 的关系为 AND。这种情况下，记 GlobalTimeExpression 的 Filter 为 tFilter，则只需要 tFilter 合并到 IExpression 的每个 SingleSeriesExpression 的 Filter 中即可。void addTimeFilterToQueryFilter(Filter timeFilter, IExpression expression)为具体实现方法。例如：

    设要将如下 GlobaLTimeFilter 和 IExpression 合并，

        1. GlobaLTimeFilter(tFilter)
        2. IExpression
                AndExpression
                    OrExpression
                        SingleSeriesExpression(“path1”, filter1)
                        SingleSeriesExpression(“path2”, filter2)
                    SingleSeriesExpression(“path3”, filter3)

    则合并后的结果为

        IExpression
            AndExpression
                OrExpression
                    SingleSeriesExpression(“path1”, AndFilter(filter1, tFilter))
                    SingleSeriesExpression(“path2”, AndFilter(filter2, tFilter))
                SingleSeriesExpression(“path3”, AndFilter(filter3, tFilter))

    *情况二*：GlobalTimeExpression 和 IExpression 的关系为 OR。该情况下的合并步骤如下：
    1. 得到该查询所要投影的所有时间序列，其为一个 Path 的集合，以一个包含三个投影时间序列的查询为例，记所有要投影的列为 PathList{path1, path2, path3}。
    2. 记 GlobalTimeExpression 的 Filter 为 tFilter，调用 pushGlobalTimeFilterToAllSeries() 方法为每个 Path 创建一个对应的 SingleSeriesExpression，且每个 SingleSeriesExpression 的 Filter 值均为 tFilter；将所有新创建的 SingleSeriesExpression 用 OR 运算符进行连接，得到一个 OrExpression，记其为 orExpression。
    3. 调用 mergeSecondTreeToFirstTree 方法将 IExpression 中的节点与步骤二得到的 orExpression 中的节点进行合并，返回合并后的表达式。


    例如，将如下 GlobaLTimeFilter 和 IExpression 按照关系 OR 进行合并，设该查询的被投影列为 PathList{path1, path2, path3}

        1. GlobaLTimeFilter(tFilter)
        2. IExpression
                AndExpression
                    SingleSeriesExpression(“path1”, filter1)
                    SingleSeriesExpression(“path2”, filter2)

    则合并后的结果为

        IExpression
            OrExpression
                AndExpression
                    SingleSeriesExpression(“path1”, filter1)
                    SingleSeriesExpression(“path2”, filter2)
                OrExpression
                    OrExpression
                        SingleSeriesExpression(“path1”, tFilter)
                        SingleSeriesExpression(“path2”, tFilter)
                    SingleSeriesExpression(“path3”, tFilter)

* MergeIExpression: 将两个 IExpression 合并为一个可执行表达式。该方法接受三个参数，分别为

        IExpression1：待合并的第一个 IExpression
        IExpression2：待合并的第二个 IExpression
        relation：两个待合并的 IExpression 的关系（Relation 的取值为 AND 或 OR）

    该方法的执行策略为：

        if relation == AND:
            return AndExpression(IExpression1, IExpression2)
        else if relation == OR:
            return OrExpression(IExpression1, IExpression2)

使用以上四种基本的过滤条件、表达式合并方法，optimize() 算法的执行步骤如下：
1. 如果 IExpression 为一元表达式，即单一的 SingleSeriesExpression 或单一的 GlobalTimeExpression，则直接将其返回；否则，执行步骤二
2. 算法达到该步骤，说明 IExpression 为 AndExpression 或 OrExpression。
   
   a. 如果LeftIExpression和RightIExpression均为GlobalTimeExpression，则执行combineTwoGlobalTimeExpression方法，并返回对应的结果。

   b. 如果 LeftIExpression 为 GlobalTimeExpression，而 RightIExpression 不是GlobalTimeExpression，则调用 handleOneGlobalTimeExpressionr() 方法进行合并。

   c. 如果 LeftIExpression 不是 GlobalTimeExpression，而 RightIExpression 是 GlobalTimeExpression，则调用 handleOneGlobalTimeExpressionr()方法进行合并。

   d. 如果 LeftIExpression 和 RightIExpression 均不是 GlobalTimeExpression，则对 LeftIExpression 递归调用 optimize() 方法得到左可执行表达式；对 RightIExpression 递归调用 optimize() 方法得到右可执行表达式。使用 MergeIExpression 方法，根据 type 的值将左可执行表达式和右可执行表达式合并为一个 IExpression。

## 2 TsFile 查询执行过程

### 2.1 设计原理

TsFile 文件层查询接口只包含原始数据查询，根据是否包含值过滤条件，可以将查询分为两类“无过滤条件或仅包含时间过滤条件查询”和“包含值过滤条件的查询”

为了执行以上两类查询，有两套查询流程

* 归并查询

	生成多个 reader，按照 time 对齐，返回结果集。

* 连接查询

	根据查询条件生成满足过滤条件的时间戳，通过满足条件的时间戳查询投影列的值，返回结果集。

### 2.2 三大查询组件

#### 2.2.1 FileSeriesReader 组件
org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader

**功能**：该组件用于查询一个文件中单个时间序列满足过滤条件的数据点。根据给定的查询路径和被查询的文件，按照时间戳递增的顺序查询出该时间序列在文件中的所有数据点。其中过滤条件可为空。

**实现**：该组件首先获取给定的路径查询出所有 Chunk 的信息，然后按照起始时间戳从小到大的顺序遍历每个 Chunk，并从中读取出满足条件的数据点。

#### 2.2.2 FileSeriesReaderByTimestamp 组件

org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp

**功能**：该组件主要用于查询一个文件中单个时间序列在指定时间戳上的数据点。

**实现**：该组件提供一个接口，getValueInTimestamp(long timestamp)，通过接口依次传入递增的时间戳，返回时间序列上与该时间戳相对应的数据点。如果满足该时间戳的数据点不存在，则返回 null。

#### 2.2.3 TsFileTimeGenerator 组件
org.apache.iotdb.tsfile.read.query.timegenerator.TsFileTimeGenerator

**功能**：根据“选择条件”，计算出满足该“选择条件”的时间戳，先将“选择条件”转化为一棵二叉树，然后递归地计算满足“选择条件”的时间戳。主要用于连接查询。

一个可执行的过滤条件由一个或多个 SingleSeriesExpression 构成，且 SingleSeriesExpression 之间具有相应的与或关系。所以，一个可执行的过滤条件可以转为一棵表示“查询条件”的二叉树，二叉树的叶子节点（ LeafNode ）为 FileSeriesReader，中间节点为 AndNode 或 OrNode。特殊地，当可执行的过滤条件仅由一个 SingleSeriesExpression 构成时，该二叉树仅包含一个节点。得到由“选择条件”转化后的二叉树后，便可以计算“满足该选择条件”的时间戳。
该组件提供两个基本的功能：

1. 判断是否还有下一个满足“选择条件”的时间戳

2. 返回下一个满足“选择条件”的时间戳


### 2.3 归并查询
org.apache.iotdb.tsfile.read.query.dataset.DataSetWithoutTimeGenerator

设当查询 n 个时间序列，为每个时间序列构建一个 FileSeriesReader，如果有 GlobalTimeExpression，则将其中的 Filter 传入 FileSeriesReader。

根据所有的 FileSeriesReader 生成一个 DataSetWithoutTimeGenerator，由于每个 FileSeriesReader 会按照时间戳从小到大的顺序迭代地返回数据点，所以可以采用“多路归并”对所有 FileSeriesReader 的结果进行按时间戳对齐。

数据合并的步骤为：

（1） 创建一个最小堆，堆里面存储“时间戳”，该堆将按照每个时间戳的大小进行组织。

（2） 初始化堆，依次访问每一个 FileSeriesReader，如果该 FileSeriesReader 中还有数据点，则获取数据点的时间戳并放入堆中。此时每个时间序列最多有1个时间戳被放入到堆中，即该序列最小的时间戳。

（3） 如果堆的 size > 0，获取堆顶的时间戳，记为t，并将其在堆中删除，进入步骤（4）；如果堆的 size 等于0，则跳到步骤（5），结束数据合并过程。

（4） 创建新的 RowRecord。依次遍历每一条时间序列。在处理其中一条时间序列时，如果该序列没有更多的数据点，则将该列标记为 null 并添加在 RowRecord 中；否则，判断最小的时间戳是否与 t 相同，若不相同，则将该列标记为 null 并添加在 RowRecord 中。若相同，将该数据点添加在 RowRecord 中，同时判断该时间序列是否有新的数据点，若存在，则将下一个时间戳 t' 添加在堆中，并将 t' 设为当前时间序列的最小时间戳。最后，返回步骤（3）。

（5） 结束数据合并过程。

### 2.4 连接查询

org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator

连接查询生成满足“选择条件”的时间戳、查询被投影列在对应时间戳下的数据点、合成 RowRecord。主要流程如下：

（1）	根据 QueryExpression，初始化时间戳计算模块 TimeGeneratorImpl

（2）	为每个被投影的时间序列创建 FileSeriesReaderByTimestamp

（3）	如果“时间戳计算模块”中还有下一个时间戳，则计算出下一个时间戳 t ，进入步骤（4）；否则，结束查询。

（4）	根据 t，在每个时间序列上使用 FileSeriesReaderByTimestamp 组件获取在时间戳 t 下的数据点；如果在该时间戳下没有对应的数据点，则用 null 表示。

（5）	将步骤（4）中得到的所有数据点合并成一个 RowRecord，此时得到一条查询结果，返回步骤（3）计算下一个查询结果。


### 2.5 查询入口

 org.apache.iotdb.tsfile.read.query.executor.TsFileExecutor

TsFileExecutor 接收一个 QueryExpression ，执行该查询并返回相应的 QueryDataSet。基本工作流程如下：

（1）接收一个 QueryExpression

（2）如果无过滤条件，执行归并查询。如果该 QueryExpression 包含 Filter（过滤条件），则通过 ExpressionOptimizer 对该 QueryExpression 的 Filter 进行优化。如果是 GlobalTimeExpression，执行归并查询。如果包含值过滤，交给 ExecutorWithTimeGenerator 执行连接查询。

（3） 生成对应的 QueryDataSet，迭代地生成 RowRecord，将查询结果返回。



### 2.6 相关代码介绍

* Chunk：一段时间序列的内存结构，可供 IChunkReader 进行读取。

* ChunkMetaData：记录对应 Chunk 在文件中的偏移量及数据类型和编码方式，便于对 Chunk 进行读取。

* IMetadataQuerier：一个 TsFile 的元数据加载器。可以加载整个文件的元数据和一个序列的所有 ChunkMetaData。

* IChunkLoader： IChunkLoader 为 Chunk 的加载器，主要功能为，给定一个 ChunkMetaData，返回对应的 Chunk。

* IChunkReader：对一个 Chunk 中的数据进行读取，其接收一个 Chunk，根据其中 ChunkHeader 中的相关信息，对该 Chunk 进行解析。其提供两套接口：

	* hasNextSatisfiedPage & nextPageData：迭代式的返回一个一个的 Page
	* getPageReaderList：一次返回所有 PageReader

* IPageReader：对一个 Page 中的数据进行读取，提供两个基本的接口：

	* getAllSatisfiedPageData()：一次返回所有满足条件的值
	* getStatistics()：返回 Page 的统计信息

* QueryExpression

	QueryExpression 为查询表达式，包含投影的时间序列和过滤条件。

* QueryDataSet

	一次查询所返回的结果，具有相同时间戳的数据点合并为一个 RowRecord。 QueryDataSet 提供两个基本的功能：

	* 判断是否还有下一个 RowRecord
	* 返回下一个 RowRecord




