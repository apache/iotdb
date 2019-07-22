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

# 第3章 IoTDB操作指南

## 数据查询
### 时间切片查询

本章节主要介绍时间切片查询的相关示例，主要使用的是[IoTDB SELECT语句](/#/Documents/latest/chap5/sec1)。同时，您也可以使用[Java JDBC](/#/Documents/latest/chap6/sec1)标准接口来执行相关的查询语句。

#### 根据一个时间区间选择一列数据

SQL语句为：

```
select temperature from root.ln.wf01.wt01 where time < 2017-11-01T00:08:00.000
```
其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为温度传感器（temperature）；该语句要求选择出该设备在“2017-11-01T00:08:00.000”（此处可以使用多种时间格式，详情可参看[2.1节](/#/Documents/latest/chap2/sec1)）时间点以前的所有温度传感器的值。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/23614968/61280074-da1c0a00-a7e9-11e9-8eb8-3809428043a8.png"></center>

#### 根据一个时间区间选择多列数据

SQL语句为：

```
select status, temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000;
```
其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为供电状态（status）和温度传感器（temperature）；该语句要求选择出“2017-11-01T00:05:00.000”至“2017-11-01T00:12:00.000”之间的所选时间序列的值。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/23614968/61280328-40a12800-a7ea-11e9-85b9-3b8db67673a3.png"></center>

#### 按照多个时间区间选择同一设备的多列数据

IoTDB支持在一次查询中指定多个时间区间条件，用户可以根据需求随意组合时间区间条件。例如，

SQL语句为：

```
select status,temperature from root.ln.wf01.wt01 where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```
其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为“供电状态（status）”和“温度传感器（temperature）”；该语句指定了两个不同的时间区间，分别为“2017-11-01T00:05:00.000至2017-11-01T00:12:00.000”和“2017-11-01T16:35:00.000至2017-11-01T16:37:00.000”；该语句要求选择出满足任一时间区间的被选时间序列的值。

该SQL语句的执行结果如下：
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/23614968/61280449-780fd480-a7ea-11e9-8ed0-70fa9dfda80f.png"></center>


#### 按照多个时间区间选择不同设备的多列数据

该系统支持在一次查询中选择任意列的数据，也就是说，被选择的列可以来源于不同的设备。例如，SQL语句为：

```
select wf01.wt01.status,wf02.wt02.hardware from root.ln where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```
其含义为：

被选择的时间序列为“ln集团wf01子站wt01设备的供电状态”以及“ln集团wf02子站wt02设备的硬件版本”；该语句指定了两个时间区间，分别为“2017-11-01T00:05:00.000至2017-11-01T00:12:00.000”和“2017-11-01T16:35:00.000至2017-11-01T16:37:00.000”；该语句要求选择出满足任意时间区间的被选时间序列的值。

该SQL语句的执行结果如下：
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577450-dcfe0800-1ef4-11e9-9399-4ba2b2b7fb73.jpg"></center>

### 降频聚合查询

本章节主要介绍降频聚合查询的相关示例，主要使用的是IoTDB SELECT语句的[GROUP BY子句](/#/Documents/latest/chap5/sec1)，该子句是IoTDB中用于根据用户给定划分条件对结果集进行划分，并对已划分的结果集进行聚合计算的语句。IoTDB支持根据时间间隔对结果集进行划分，默认结果按照时间升序排列。同时，您也可以使用[Java JDBC](/#/Documents/latest/chap6/sec1)标准接口来执行相关的查询语句。

GROUP BY语句为用户提供三类指定参数：

* 参数1：划分时间轴的时间间隔参数
* 参数2：时间轴划分原点参数（可选参数）
* 参数3：时间轴显示时间窗参数（一个或多个）


三类参数的实际含义如下图3.2所示。其中时间轴划分原点参数为可选参数，下面我们将给出指定划分原点、不指定划分原点、指定时间过滤条件三种较为典型的降频聚合的例子。

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577465-e8513380-1ef4-11e9-84c6-d0690f2a8113.jpg">

**图3.2 三类参数实际含义**</center>

#### 不指定时间轴划分原点的降频聚合

SQL语句为：

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by (1d, [2017-11-01T00:00:00, 2017-11-07T23:00:00]);
```
其含义为：

由于用户没有指定时间轴划分原点参数，GROUP BY语句将默认以1970年1月1日0点（+0时区）为划分原点。

以上GROUP BY语句的第一个参数是划分时间轴的时间间隔，以该参数（1d）为时间间隔，默认原点为划分原点，将时间轴划分为若干个连续的区间，这些区间为[0,1d]，[1d,2d]，[2d,3d]…。

以上GROUP BY语句的第二个参数是显示时间段参数，该参数决定了最终展示的结果范围为[2017-11-01T00:00:00, 2017-11-07T23:00:00]。

之后系统会将和where子句中的时间及值过滤条件与GROUP BY语句的第二个参数一同作为数据的过滤条件，得到满足过滤条件要求的数据（在此例子中为[2017-11-01T00:00:00, 2017-11-07T23:00:00]范围内的数据），将这些数据映射到前面的已经切分好的时间轴中（在此例子中，从2017-11-01T00:00:00开始至2017-11-07T23:00:00结束，每隔1天的时间段中均有映射进来的数据）。

由于要显示的结果范围中每个时间段均有数据，该SQL语句的执行结果如图：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577537-277f8480-1ef5-11e9-9b0f-c477f3b71acb.jpg"></center>

#### 指定时间轴划分原点的降频聚合

SQL语句为：

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by (1d, 2017-11-03 00:00:00, [2017-11-01 00:00:00, 2017-11-07 23:00:00]);
```

其含义为：

由于用户指定了时间轴划分原点参数（第二个参数）为2017-11-03 00:00:00，GROUP BY语句将默认以2017年11月03日0点（系统默认时区）为划分原点。

以上GROUP BY语句的第一个参数是划分时间轴的时间间隔，以该参数（1d）为时间间隔，默认原点为划分原点，将时间轴划分为若干个连续的区间，这些区间为[2017-11-02T00:00:00, 2017-11-03T00:00:00], [2017-11-03T00:00:00, 2017-11-04T00:00:00]等。

以上GROUP BY语句的第三个参数是显示时间段参数，该参数决定了最终展示的结果范围为[2017-11-01T00:00:00, 2017-11-07T23:00:00]。

之后系统会将和where子句中的时间及值过滤条件与GROUP BY语句的第三个参数一同作为数据的过滤条件，得到满足过滤条件要求的数据（在此例子中为[2017-11-01T00:00:00, 2017-11-07T23:00:00]范围内的数据），将这些数据映射到前面的已经切分好的时间轴中（在此例子中，从2017-11-01T00:00:00开始至2017-11-07T23:00:00结束，每隔1天的时间段中均有映射进来的数据）。

由于要显示的结果范围中每个时间段均有数据，该SQL语句的执行结果如图：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577563-3a925480-1ef5-11e9-88da-2d7e3eb4c951.jpg"></center>

#### 指定时间过滤条件的降频聚合

SQL语句为：

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 where time > 2017-11-03T06:00:00 and temperature > 20 group by(1h, [2017-11-03T00:00:00, 2017-11-03T23:00:00]);
```
其含义为：

由于用户没有指定时间轴划分原点参数，GROUP BY语句将默认以1970年1月1日0点（+0时区）为划分原点。

以上GROUP BY语句的第一个参数是划分时间轴的时间间隔，以该参数（1h）为时间间隔，默认原点为划分原点，将时间轴划分为若干个连续的区间，这些区间为[2017-11-03T00:00:00, 2017-11-03T01:00:00], [2017-11-03T01:00:00, 2017-11-03T02:00:00]等。

以上GROUP BY语句的第二个参数是显示时间段参数，该参数决定了最终展示的结果范围为[2017-11-03T00:00:00, 2017-11-03T23:00:00]。

之后系统会将和where子句中的时间及值过滤条件与GROUP BY语句的第二个参数一同作为数据的过滤条件，得到满足过滤条件要求的数据（在此例子中为[2017-11-03T00:06:00, 2017-11-07T23:00:00]范围内，且root.ln.wf01.wt01.temperature > 20的数据），将这些数据映射到前面的已经切分好的时间轴中（在此例子中，从2017-11-03T00:06:00开始至2017-11-03T23:00:00结束，每隔1小时的时间段中均有映射进来的数据）。

由于要显示的结果范围中[2017-11-03T00:00:00, 2017-11-03T00:06:00]时段没有数据，该段区间的聚合结果会显示null，其余时间段均有数据，该SQL语句的执行结果如图：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577582-441bbc80-1ef5-11e9-8b54-3ad1f586bbc4.jpg"></center>

需要注意的是，GROUP BY语句中SELECT后面的路径必须全部为聚合函数，否则系统会给出相应的错误提示。如图所示。

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/61517091-fbbf0080-aa38-11e9-8623-cdadf1ccf5d6.png"></center>

### 查询结果自动补值

在IoTDB实际使用中，做时间序列的查询操作时，会出现在某些时刻数值为空值的情况，这样的情况会影响使用者进行进一步的分析。为了更好的反映数据的变化程度，用户希望能够对缺失值进行自动填补，因此，IoTDB系统引入了自动补值（Fill）功能。

自动补值功能是指在针对单列或多列的时间序列查询中，根据用户的指定方法以及有效时间范围填充空值，若查询的点有值则自动补值功能不生效。

> 注：当前0.8.0版本中IoTDB为用户提供使用前一个数值填充（Previous）和使用线性拟合填充（Linear）两种方法。且填充仅可用在对某一个时间点进行查询传感器数值结果为空的情况。

#### 填充方法
* Previous方法

当查询时间戳的值为空值时，用查询时间戳的前一个时间戳的值进行补值。 形式化的Previous自动补值方法如下所示：
```
select <path> from <prefixPath> where time = <T> fill(<data_type>[previous, <before_range>], …)
```

其中各个参数含义如下：

<center>**表格3-4 Previous方法参数列表**

|参数名|含义|
|:---|:---|
|path, prefixPath|查询路径，必选字段。|
|T|查询时间戳（该查询时间戳只能指定一个），必选字段。|
|data\_type|填充方法作用的数据类型。可选值为：int32, int64, float, double, boolean, text。可选字段。|
|before\_range|表示Previous填充方法的有效时间范围。当 [T-before\_range, T]范围内有数值时，Previous方法才能进行填充。当未指定before\_range的值时，before\_range为默认值T。可选字段。|
</center>

在此我们给出一个使用Previous方法填充空值的示例，SQL语句如下所示：

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) 
```
该语句的含义为：

由于2017-11-01T16:37:50.000时刻，时间序列`root.sgcc.wf03.wt01.temperature`结果为空值，系统采用2017-11-01T16:37:50.000时刻的前一个时间戳（且该时间戳在[2017-11-01T16:36:50.000, 2017-11-01T16:37:50.000]时间范围内）的值进行补值并显示。

在本文的[样例数据集](/#/Documents/latest/chap3/sec1)上，该语句的执行结果如图所示：
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577616-67df0280-1ef5-11e9-9dff-2eb8342074eb.jpg"></center>

值得说明的是，如果在填充指定的有效时间内没有数值，则系统不会进行填充，返回为空，如图所示：
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577679-9f4daf00-1ef5-11e9-8d8b-06a58de6efc1.jpg"></center>

* Linear方法

当查询时间戳的值为空值时，用查询时间戳的前一个时间戳的值和后一个时间戳的值进行线性补值。 形式化的Linear自动补值方法如下所示：
```
select <path> from <prefixPath> where time = <T> fill(<data_type>[linear, <before_range>, <after_range>]…)
```
其中各个参数含义如下：

<center>**表格3-5 Linear方法参数列表**

|参数名|含义|
|:---|:---|
|path, prefixPath|查询路径，必选字段。|
|T|查询时间戳（该查询时间戳只能指定一个），必选字段。|
|data\_type|填充方法作用的数据类型。可选值为：int32, int64, float, double, boolean, text。可选字段。|
|before\_range, after\_range|表示Linear填充方法的有效时间范围。当 [T-before\_range, T+after\_range]范围内有数值时，Linear方法才能进行填充。当未指定before\_range的值时，before\_range为默认值T。可选字段。|
|before\_range|表示Linear填充方法的前置有效时间范围。当 [T-before\_range, T+after\_range]范围内前后都有数值时，Linear方法才能进行填充。当before\_range及after\_range未显式指定时，before\_range与after\_range皆默认为无穷大。|
</center>

在此我们给出一个使用Linear方法填充空值的示例，SQL语句如下所示：

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float [linear, 1m, 1m])
```
该语句的含义为：

由于2017-11-01T16:37:50.000时刻，时间序列`root.sgcc.wf03.wt01.temperature`结果为空值，系统采用2017-11-01T16:37:50.000时刻的前一个时间戳（且该时间戳在[2017-11-01T16:36:50.000, 2017-11-01T16:37:50.000]时间范围内）2017-11-01T16:37:00.000及其值21.927326、2017-11-01T16:37:50.000时刻的后一个时间戳（且该时间戳在[2017-11-01T16:37:50.000, 2017-11-01T16:38:50.000]时间范围内）2017-11-01T16:39:00.000及其值25.311783进行线性计算得到结果为21.927326 + (25.311783-21.927326)/60s*50s = 24.747707。

在本文的[样例数据集](/#/Documents/latest/chap3/sec1)上，该语句的执行结果如图所示：
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577727-d4f29800-1ef5-11e9-8ff3-3bb519da3993.jpg"></center>

#### 数据类型与填充方法对应关系
数据类型及支持的填充方式如表格3-6所示：

<center>**表格3-6 数据类型及支持的填充方式**

|数据类型|支持的填充方式|
|:---|:---|
|boolean|previous|
|int32|previous, linear|
|int64|previous, linear|
|float|previous, linear|
|double|previous, linear|
|text|previous|
</center>

需要注意的是，对数据类型不支持的fill方式，IoTDB系统会给出错误提示，如下图：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577741-e340b400-1ef5-11e9-9238-a4eaf498ab84.jpg"></center>

在不指定填充方式时，各类型有其自己默认的填充方式以及参数，对应关系如表格3-7：

<center>**Table 3-7 各种数据类型的默认Fill方式**

|数据类型|默认Fill方式|
|:---|:---|
|boolean|previous, 0|
|int32|linear, 0, 0|
|int64|linear, 0, 0|
|float|linear, 0, 0|
|double|linear, 0, 0|
|text|previous, 0|
</center>

> 注意: 0.8.0版本中Fill语句内至少指定一种填充类型。

### 查询结果的分页控制
为方便用户在对IoTDB进行查询时更好的进行结果阅读，IoTDB为用户提供了[LIMIT/SLIMIT](/#/Documents/latest/chap5/sec1)子句以及[OFFSET/SOFFSET](/#/Documents/latest/chap5/sec1)子句。使用LIMIT和SLIMIT子句可以允许用户对查询结果的行数和列数进行控制，使用OFFSET和SOFFSET子句可以允许用户设定结果展示的起始位置。

值得说明的是，LIMIT/SLIMIT子句以及OFFSET/SOFFSET子句均不改变查询的实际执行过程，仅对查询返回的结果进行约束。

本章节主要介绍查询结果分页控制的相关示例。同时你也可以使用[Java JDBC](/#/Documents/latest/chap6/sec1)标准接口来执行相关的查询语句。

#### 查询结果的行数控制

通过使用LIMIT和OFFSET子句，用户可以对查询结果进行与行有关的控制。我们将通过以下几个例子来示范如何使用LIMIT和OFFSET子句对查询结果的行数进行控制。

* 例1：基本的LIMIT 子句

SQL语句为：

```
select status, temperature from root.ln.wf01.wt01 limit 10
```
其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为“供电状态（status）”和“温度传感器（temperature）”；该语句要求返回查询结果的前10行。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577752-efc50c80-1ef5-11e9-9071-da2bbd8b9bdd.jpg"></center>


* 例2：带OFFSET的LIMIT子句

SQL语句为：

```
select status, temperature from root.ln.wf01.wt01 limit 5 offset 3
```
其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为“供电状态（status）”和“温度传感器（temperature）”；该语句要求返回查询结果的查询结果 的第3行到第7行（首行为第0行）。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577773-08352700-1ef6-11e9-883f-8d353bef2bdc.jpg"></center>

* 例3：与WHERE子句结合的LIMIT子句

SQL语句为：

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time< 2017-11-01T00:12:00.000 limit 2 offset 3
```
其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为“供电状态（status）”和“温度传感器（temperature）”；该语句要求选择出“2017-11-01T00:05:00.000”至“2017-11-01T00:12:00.000”之间的所选时间序列值的查询结果的第3行到第4行（首行为第0行）。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577789-15521600-1ef6-11e9-86ca-d7b2c947367f.jpg"></center>

* 例4：与GROUP BY子句结合的LIMIT子句

SQL语句为：

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by (1d,[2017-11-01T00:00:00, 2017-11-07T23:00:00]) limit 5 offset 3
```
其含义为：

返回查询结果的第3行到第7行（首行为第0行）。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577796-1e42e780-1ef6-11e9-8987-be443000a77e.jpg"></center>

值得说明的是，由于当前FILL子句仅能对某时间点的时间序列缺失值进行填充，FILL子句的执行结果为一行，因此LIMIT和OFFSET不允许和FILL子句结合使用，否则会提示错误。例如执行如下SQL语句：

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) limit 10
```

该SQL语句将进行无法执行，并给出相应的错误提示，提示如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/61517266-6e2fe080-aa39-11e9-8015-154a8e8ace30.png"></center>

#### 查询结果的列数控制

SLIMIT子句与LIMIT的子句用法相同，可以被用于与WHERE子句、GROUP BY等子句组合，也可以与FILL子句组合，我们将通过以下几个例子来示范如何使用SLIMIT和SOFFSET子句对查询结果的行数进行控制。

通过下面几个例子来示范如何使用SLIMIT子句对查询结果的列数进行控制。

* 例1：基本的SLIMIT子句

SQL语句为：

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1
```
其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为该设备下的第0列“供电状态（status）”；该语句要求选择出“2017-11-01T00:05:00.000”至“2017-11-01T00:12:00.000”之间的所选时间序列值的查询结果。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577813-30bd2100-1ef6-11e9-94ef-dbeb450cf319.jpg"></center>

* 例2：带SOFFSET的SLIMIT子句

SQL语句为：

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 1
```
其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为该设备下的第1列“温度传感器（temperature）”（首列为第0列）；该语句要求选择出“2017-11-01T00:05:00.000”至“2017-11-01T00:12:00.000”之间的所选时间序列值的查询结果。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577827-39adf280-1ef6-11e9-81b5-876769607cd2.jpg"></center>

* 例3：与GROUP BY子句结合

SQL语句为：

```
select max_value(*) from root.ln.wf01.wt01 group by (1d, [2017-11-01T00:00:00, 2017-11-07T23:00:00]) slimit 1 soffset 1
```

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577840-44688780-1ef6-11e9-8abc-04ae78efa85b.jpg"></center>

* 例4：与FILL子句结合

SQL语句为：

```
select * from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) slimit 1 soffset 1
```
该FILL子句的具体含义请参见本文第4.4.4.1.1节。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577855-4d595900-1ef6-11e9-8541-a4accd714b75.jpg"></center>

值得说明的是，在使用过程中，SLIMIT子句只能和带星路径或者前缀路径查询结合使用，与仅含完整路径查询结合使用会提示错误。例如执行如下SQL语句：

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1
```

该SQL语句将进行无法执行，并给出相应的错误提示，提示如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577867-577b5780-1ef6-11e9-978c-e02c1294bcc5.jpg"></center>

#### 查询结果的行列控制

除查询结果的行或列控制，IoTDB允许用户同时对查询结果进行行列控制。下面示范一个完整的带有LIMIT子句和SLIMIT子句的例子。

SQL语句为：

```
select * from root.ln.wf01.wt01 limit 10 offset 100 slimit 2 soffset 0
```
其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为该设备下的第0列到第1列（首列为第0列）；该语句要求返回所选时间序列值的查询结果的第100行到109行（首行为第0行）。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577879-64984680-1ef6-11e9-9d7b-57dd60fab60e.jpg"></center>

#### 错误情况的处理

当LIMIT/SLIMIT的参数N/SN超出结果集大小时，IoTDB将正常返回全部结果。例如如下SQL语句的执行结果仅有6行，我们通过limit语句选取其前100行：

```
select status,temperature from root.ln.wf01.wt01 
where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 
limit 100
```
该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51578187-ad9cca80-1ef7-11e9-897a-83e66a0f3d94.jpg"></center>

当LIMIT/SLIMIT的参数N/SN超出允许的最大值（N/SN为int32类型）时，当参数超过阈值时，系统会提示相应错误。例如执行如下SQL语句：

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 1234567890123456789
```
该SQL语句将进行无法执行，并给出相应的错误提示，错误提示如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/61517469-e696a180-aa39-11e9-8ca5-42ea991d520e.png"></center>

当LIMIT/SLIMIT的参数N/SN不为正整数时，系统会给出相应的错误提示，例如执行如下错误语句：

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 13.1
```

该SQL语句将进行无法执行，并给出相应的错误提示，错误提示如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/61518094-68d39580-aa3b-11e9-993c-fc73c27540f7.png"></center>

当OFFSET的参数OffsetValue超出结果集大小时，返回结果将为空。例如执行如下SQL语句：

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 2 offset 6
```

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51578227-c60ce500-1ef7-11e9-98eb-175beb8d4086.jpg"></center>

当SOFFSET的参数SOffsetValue超出可选时间序列范围时，提示错误信息，例如执行如下SQL语句：

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 2
```
该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51578237-cd33f300-1ef7-11e9-9aef-2a717c56ab54.jpg"></center>
