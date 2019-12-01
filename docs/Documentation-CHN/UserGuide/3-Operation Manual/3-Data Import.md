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

## 数据接入
### 历史数据导入

0.8.2版本中暂不支持此功能。

### 实时数据接入

IoTDB为用户提供多种插入实时数据的方式，例如在[Cli/Shell工具](/#/Tools/Cli)中直接输入插入数据的[INSERT语句](/#/Documents/0.8.2/chap5/sec1)，或使用Java API（标准[Java JDBC](/#/Documents/0.8.2/chap6/sec1)接口）单条或批量执行插入数据的[INSERT语句](/#/Documents/0.8.2/chap5/sec1)。

本节主要为您介绍实时数据接入的[INSERT语句](/#/Documents/0.8.2/chap5/sec1)在场景中的实际使用示例，有关INSERT SQL语句的详细语法请参见本文[INSERT语句](/#/Documents/0.8.2/chap5/sec1)节。

#### 使用INSERT语句
使用[INSERT语句](/#/Documents/0.8.2/chap5/sec1)可以向指定的已经创建的一条或多条时间序列中插入数据。对于每一条数据，均由一个时间戳类型的[时间戳](/#/Documents/0.8.2/chap2/sec1)和一个[数值类型](/#/Documents/0.8.2/chap2/sec2)的传感器采集值组成。

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
Msg: Current deviceId[root.ln.wf02.wt02] does not contains measurement:temperature
```
若用户插入的数据类型与该Timeseries对应的数据类型不一致，例如执行以下命令：
```
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1,100)
```
系统将会返回以下ERROR告知数据类型有误：
```
error: The TEXT data type should be covered by " or '
```
