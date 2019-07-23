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

## 数据维护

<!-- > 

### 数据更新

用户使用[UPDATE语句](/#/Documents/latest/chap5/sec1)可以更新指定的时间序列中一段时间的数据。在更新数据时，用户可以选择需要更新的一个时间序列（0.8.0版本暂不支持多个时间序列的更新）并指定更新某个时间点或时间段的数据（0.8.0版本必须有时间过滤条件）。

在JAVA编程环境中，您可以使用[JDBC API](/#/Documents/latest/chap6/sec1)单条或批量执行UPDATE语句。

#### 单传感器时间序列值更新

以测控ln集团wf02子站wt02设备供电状态为例，存在这样的使用场景：

当数据接入并分析后，发现从2017-11-01 15:54:00到2017-11-01 16:00:00内的供电状态为true，但实际供电状态存在异常。需要将这段时间状态更新为false。进行此操作的SQL语句为：

```
update root.ln.wf02 SET wt02.status = false where time <=2017-11-01T16:00:00 and time >= 2017-11-01T15:54:00
```
需要注意的是，当更新数据类型与实际类型不符时，IoTDB会给出相应的错误提示：
```
IoTDB> update root.ln.wf02 set wt02.status = 1205 where time < now()
error: The BOOLEAN data type should be true/TRUE or false/FALSE
```
当更新的列不存在时，IoTDB给出没有存在的路径的错误提示：
```
IoTDB> update root.ln.wf02 set wt02.sta = false where time < now()
Msg: do not select any existing series
```
-->

### 数据删除

用户使用[DELETE语句](/#/Documents/latest/chap5/sec1)可以删除指定的时间序列中符合时间删除条件的数据。在删除数据时，用户可以选择需要删除的一个或多个时间序列、时间序列的前缀、时间序列带*路径对某时间之前的数据进行删除（0.8.0版本暂不支持删除某一闭时间区间范围内的数据）。

在JAVA编程环境中，您可以使用[JDBC API](/#/Documents/latest/chap6/sec1)单条或批量执行UPDATE语句。

#### 单传感器时间序列值删除

以测控ln集团为例，存在这样的使用场景：

wf02子站的wt02设备在2017-11-01 16:26:00之前的供电状态出现多段错误，且无法分析其正确数据，错误数据影响了与其他设备的关联分析。此时，需要将此时间段前的数据删除。进行此操作的SQL语句为：

```
delete from root.ln.wf02.wt02.status where time<=2017-11-01T16:26:00;
```

#### 多传感器时间序列值删除	

当ln集团wf02子站的wt02设备在2017-11-01 16:26:00之前的供电状态和设备硬件版本都需要删除，此时可以使用含义更广的[前缀路径或带`*`路径](/#/Documents/latest/chap2/sec1)进行删除操作，进行此操作的SQL语句为：

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
