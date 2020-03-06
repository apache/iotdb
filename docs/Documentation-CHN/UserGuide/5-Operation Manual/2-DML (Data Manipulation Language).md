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

# 第5章 IoTDB操作指南

## DML (数据操作语言)

## 数据接入

IoTDB为用户提供多种插入实时数据的方式，例如在[Cli/Shell工具](/#/Documents/progress/chap4/sec1)中直接输入插入数据的INSERT语句，或使用Java API（标准[Java JDBC](/#/Documents/progress/chap4/sec2)接口）单条或批量执行插入数据的INSERT语句。

本节主要为您介绍实时数据接入的INSERT语句在场景中的实际使用示例，有关INSERT SQL语句的详细语法请参见本文[INSERT语句](/#/Documents/progress/chap5/sec4)节。

### 使用INSERT语句

使用INSERT语句可以向指定的已经创建的一条或多条时间序列中插入数据。对于每一条数据，均由一个时间戳类型的时间戳和一个数值或布尔值、字符串类型的传感器采集值组成。

在本节的场景实例下，以其中的两个时间序列`root.ln.wf02.wt02.status`和`root.ln.wf02.wt02.hardware`为例 ，它们的数据类型分别为BOOLEAN和TEXT。

单列数据插入示例代码如下：

```
IoTDB > insert into root.ln.wf02.wt02(timestamp,status) values(1,true)
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1, "v1")
```

以上示例代码将长整型的timestamp以及值为true的数据插入到时间序列`root.ln.wf02.wt02.status`中和将长整型的timestamp以及值为”v1”的数据插入到时间序列`root.ln.wf02.wt02.hardware`中。执行成功后会返回执行时间，代表数据插入已完成。 

> 注意：在IoTDB中，TEXT类型的数据单双引号都可以来表示,上面的插入语句是用的是双引号表示TEXT类型数据，下面的示例将使用单引号表示TEXT类型数据。

INSERT语句还可以支持在同一个时间点下多列数据的插入，同时向2时间点插入上述两个时间序列的值，多列数据插入示例代码如下：

```
IoTDB > insert into root.ln.wf02.wt02(timestamp, status, hardware) VALUES (2, false, 'v2')
```

插入数据后我们可以使用SELECT语句简单查询已插入的数据。

```
IoTDB > select * from root.ln.wf02 where time < 3
```

结果如图所示。由查询结果可以看出，单列、多列数据的插入操作正确执行。
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51605021-c2ee1500-1f48-11e9-8f6b-ba9b48875a41.png"></center>

### INSERT语句的错误处理

若用户向一个不存在的时间序列中插入数据，例如执行以下命令：

```
IoTDB > insert into root.ln.wf02.wt02(timestamp, temperature) values(1,"v1")
```

由于`root.ln.wf02.wt02. temperature`时间序列不存在，系统将会返回以下ERROR告知该Timeseries路径不存在：

```
Msg: The resultDataType or encoding or compression of the last node temperature is conflicting in the storage group root.ln
```

若用户插入的数据类型与该Timeseries对应的数据类型不一致，例如执行以下命令：

```
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1,100)
```

系统将会返回以下ERROR告知数据类型有误：

```
error: The TEXT data type should be covered by " or '
```

## 数据查询

### 时间切片查询

本节主要介绍时间切片查询的相关示例，主要使用的是[IoTDB SELECT语句](/#/Documents/progress/chap5/sec4)。同时，您也可以使用[Java JDBC](/#/Documents/progress/chap4/sec2)标准接口来执行相关的查询语句。

#### 根据一个时间区间选择一列数据

SQL语句为：

```
select temperature from root.ln.wf01.wt01 where time < 2017-11-01T00:08:00.000
```

其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为温度传感器（temperature）；该语句要求选择出该设备在“2017-11-01T00:08:00.000”（此处可以使用多种时间格式，详情可参看[2.1节](/#/Documents/progress/chap2/sec1)）时间点以前的所有温度传感器的值。

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

#### 其他结果返回形式

IoTDB支持另外两种结果返回形式: 按设备时间对齐 'align by device' 和 时序不对齐 'disable align'.

'align by device' 对齐方式下，设备ID会单独作为一列出现。在 select 子句中写了多少列，最终结果就会有该列数+2 （时间列和设备名字列）。SQL形如:

```
select s1,s2 from root.sg1.* GROUP BY DEVICE
```

更多语法请参照 SQL REFERENCE.

'disable align' 意味着每条时序就有3列存在。更多语法请参照 SQL REFERENCE.


### 降频聚合查询

本章节主要介绍降频聚合查询的相关示例，
主要使用的是IoTDB SELECT语句的[GROUP BY子句](/#/Documents/progress/chap5/sec4)，
该子句是IoTDB中用于根据用户给定划分条件对结果集进行划分，并对已划分的结果集进行聚合计算的语句。
IoTDB支持根据时间间隔和自定义的滑动步长（默认值与时间间隔相同，自定义的值必须大于等于时间间隔）对结果集进行划分，默认结果按照时间升序排列。
同时，您也可以使用Java JDBC标准接口来执行相关的查询语句。

Group By 语句不支持 limit 和 offset。

GROUP BY语句为用户提供三类指定参数：

* 参数1：时间轴显示时间窗参数
* 参数2：划分时间轴的时间间隔参数（必须为正数）
* 参数3：滑动步长（可选参数，默认值与时间间隔相同，自定义的值必须大于等于时间间隔）

三类参数的实际含义已经在图5.2中指出，这三类参数里，第三个参数是可选的。
接下来，我们将给出三种典型的降频聚合查询的例子：
滑动步长未指定，
指定滑动步长，
带值过滤条件。

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69109512-f808bc80-0ab2-11ea-9e4d-b2b2f58fb474.png">

**图 5.2 三类参数的实际含义**</center>

#### 未指定滑动步长的降频聚合查询

对应的SQL语句是:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00],1d);
```
这条查询的含义是:

由于用户没有指定滑动步长，滑动步长将会被默认设置为跟时间间隔参数相同，也就是`1d`。

上面这个例子的第一个参数是显示窗口参数，决定了最终的显示范围是[2017-11-01T00:00:00, 2017-11-07T23:00:00]。

上面这个例子的第二个参数是划分时间轴的时间间隔参数，将`1d`当作划分间隔，显示窗口参数的起始时间当作分割原点，时间轴即被划分为连续的时间间隔：[0,1d), [1d, 2d), [2d, 3d)等等。

然后系统将会用WHERE子句中的时间和值过滤条件以及GROUP BY语句中的第一个参数作为数据的联合过滤条件，获得满足所有过滤条件的数据（在这个例子里是在[2017-11-01T00:00:00, 2017-11-07 T23:00:00]这个时间范围的数据），并把这些数据映射到之前分割好的时间轴中（这个例子里是从2017-11-01T00:00:00到2017-11-07T23:00:00:00的每一天）

每个时间间隔窗口内都有数据，SQL执行后的结果集如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116068-eed51b00-0ac5-11ea-9731-b5a45c5cd224.png"></center>

#### 指定滑动步长的降频聚合查询

对应的SQL语句是:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01 00:00:00, 2017-11-07 23:00:00], 3h, 1d);
```

这条查询的含义是:

由于用户指定了滑动步长为`1d`，GROUP BY语句执行时将会每次把时间间隔往后移动一天的步长，而不是默认的3小时。

也就意味着，我们想要取从2017-11-01到2017-11-07每一天的凌晨0点到凌晨3点的数据。

上面这个例子的第一个参数是显示窗口参数，决定了最终的显示范围是[2017-11-01T00:00:00, 2017-11-07T23:00:00]。

上面这个例子的第二个参数是划分时间轴的时间间隔参数，将`3h`当作划分间隔，显示窗口参数的起始时间当作分割原点，时间轴即被划分为连续的时间间隔：[2017-11-01T00:00:00, 2017-11-01T03:00:00), [2017-11-02T00:00:00, 2017-11-02T03:00:00), [2017-11-03T00:00:00, 2017-11-03T03:00:00)等等。

上面这个例子的第三个参数是每次时间间隔的滑动步长。

然后系统将会用WHERE子句中的时间和值过滤条件以及GROUP BY语句中的第一个参数作为数据的联合过滤条件，获得满足所有过滤条件的数据（在这个例子里是在[2017-11-01T00:00:00, 2017-11-07 T23:00:00]这个时间范围的数据），并把这些数据映射到之前分割好的时间轴中（这个例子里是从2017-11-01T00:00:00到2017-11-07T23:00:00:00的每一天的凌晨0点到凌晨3点）

每个时间间隔窗口内都有数据，SQL执行后的结果集如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116083-f85e8300-0ac5-11ea-84f1-59d934eee96e.png"></center>

#### 带值过滤条件的降频聚合查询

对应的SQL语句是:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 where time > 2017-11-01T01:00:00 and temperature > 20 group by([2017-11-01T00:00:00, 2017-11-07T23:00:00], 3h, 1d);
```

这条查询的含义是:

由于用户指定了滑动步长为`1d`，GROUP BY语句执行时将会每次把时间间隔往后移动一天的步长，而不是默认的3小时。

也就意味着，我们想要取从2017-11-01到2017-11-07每一天的凌晨0点到凌晨3点的数据。

上面这个例子的第一个参数是显示窗口参数，决定了最终的显示范围是[2017-11-01T00:00:00, 2017-11-07T23:00:00]。

上面这个例子的第二个参数是划分时间轴的时间间隔参数，将`3h`当作划分间隔，显示窗口参数的起始时间当作分割原点，时间轴即被划分为连续的时间间隔：[2017-11-01T00:00:00, 2017-11-01T03:00:00), [2017-11-02T00:00:00, 2017-11-02T03:00:00), [2017-11-03T00:00:00, 2017-11-03T03:00:00)等等。

上面这个例子的第三个参数是每次时间间隔的滑动步长。

然后系统将会用WHERE子句中的时间和值过滤条件以及GROUP BY语句中的第一个参数作为数据的联合过滤条件，获得满足所有过滤条件的数据（在这个例子里是在[2017-11-01T00:00:00, 2017-11-07 T23:00:00]这个时间范围的并且满足root.ln.wf01.wt01.temperature > 20的数据），并把这些数据映射到之前分割好的时间轴中（这个例子里是从2017-11-01T00:00:00到2017-11-07T23:00:00:00的每一天的凌晨0点到凌晨3点）

每个时间间隔窗口内都有数据，SQL执行后的结果集如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116088-001e2780-0ac6-11ea-9a01-dc45271d1dad.png"></center>

GROUP BY的SELECT子句里的查询路径必须是聚合函数，否则系统将会抛出如下对应的错误。

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116099-0b715300-0ac6-11ea-8074-84e04797b8c7.png"></center>

### 最近时间戳数据查询

对应的SQL语句是：

```
select last <Path> [COMMA <Path>]* from < PrefixPath > [COMMA < PrefixPath >]* <DISABLE ALIGN>
```
其含义是：

查询时间序列prefixPath.path中最近时间戳的数据

下面的例子中查询时间序列root.ln.wf01.wt01.status最近时间戳的数据:
```
select last status from root.ln.wf01.wt01 disable align
```
结果集为以下的形式返回：
```
| Time | Path                    | Value |
| ---  | ----------------------- | ----- |
|  5   | root.ln.wf01.wt01.status| 100   |
```

假设root.ln.wf01.wt01中包含多列数据，如id, status, temperature，下面的例子将会把这几列数据在最近时间戳的记录同时返回：
```
select last id, status, temperature from root.ln.wf01 disable align

| Time | Path                         | Value |
| ---  | ---------------------------- | ----- |
|  5   | root.ln.wf01.wt01.id         | 10    |
|  7   | root.ln.wf01.wt01.status     | true  |
|  9   | root.ln.wf01.wt01.temperature| 35.7  |
```


## 数据维护

### 数据删除

用户使用[DELETE语句](/#/Documents/progress/chap5/sec4)可以删除指定的时间序列中符合时间删除条件的数据。在删除数据时，用户可以选择需要删除的一个或多个时间序列、时间序列的前缀、时间序列带\*路径对某时间之前的数据进行删除（当前版本暂不支持删除某一闭时间区间范围内的数据）。

在JAVA编程环境中，您可以使用JDBC API单条或批量执行DELETE语句。

#### 单传感器时间序列值删除

以测控ln集团为例，存在这样的使用场景：

wf02子站的wt02设备在2017-11-01 16:26:00之前的供电状态出现多段错误，且无法分析其正确数据，错误数据影响了与其他设备的关联分析。此时，需要将此时间段前的数据删除。进行此操作的SQL语句为：

```
delete from root.ln.wf02.wt02.status where time<=2017-11-01T16:26:00;
```

#### 多传感器时间序列值删除    

当ln集团wf02子站的wt02设备在2017-11-01 16:26:00之前的供电状态和设备硬件版本都需要删除，此时可以使用含义更广的[前缀路径或带`*`路径](/#/Documents/progress/chap2/sec1)进行删除操作，进行此操作的SQL语句为：

```
delete from root.ln.wf02.wt02 where time <= 2017-11-01T16:26:00;
```

或

```
delete from root.ln.wf02.wt02.* where time <= 2017-11-01T16:26:00;
```

需要注意的是，当删除的路径不存在时，IoTDB会提示路径不存在，无法删除数据，如下所示。

```
IoTDB> delete from root.ln.wf03.wt02.status where time < now()
Msg: TimeSeries does not exist and its data cannot be deleted
```
