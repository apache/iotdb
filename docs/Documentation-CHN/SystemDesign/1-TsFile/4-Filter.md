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

# 过滤条件和查询表达式

本章节首先介绍 Tsfile 文件读取时需要用到的过滤条件和查询表达式的相关定义；其次介绍如何将用户输入的过滤条件转化为系统可以执行的查询条件。

* 1 Filter
* 2 Expression表达式
    * 2.1 SingleSeriesExpression 表达式
    * 2.2 GlobalTimeExpression 表达式
    * 2.3 IExpression 表达式
    * 2.4 可执行表达式
    * 2.5 IExpression 转化为可执行表达式的优化算法

## 1 Filter

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

## 2 Expression表达式

当一个过滤条件作用到一个时间序列上，就成为一个表达式。例如，“数值大于10” 是一个过滤条件；而 “序列 d1.s1 的数值大于10” 就是一条表达式。特殊地，对时间的过滤条件也是一个表达式，称为 GlobalTimeExpression。以下章节将对表达式进行展开介绍。

### 2.1 SingleSeriesExpression表达式

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

### 2.2 GlobalTimeExpression 表达式
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

### 2.3 IExpression 表达式
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


### 2.4 可执行表达式

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

### 2.5 IExpression转化为可执行表达式的优化算法

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
    2. 记 GlobalTimeExpression 的 Filter 为 tFilter，调用 pushGlobalTimeFilterToAllSeries() 方法为每个 Path 创建一个对应的 SingleSeriesExpression，且每个 SingleSeriesExpression 的 Filter 值均为 tFilter；将所有新创建的 SingleSeriesExpression 用 OR 运算符进行连接，得到一个 OrExpression，记其为 orExpression
    3. 将步骤二得到的 orExpression 与 IExpression 按照关系 OR 进行合并，得到最终的结果。


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
