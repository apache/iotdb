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

# 第4章 系统部署与管理

## 系统监控

当前用户可以使用Java的JConsole工具对正在运行的IoTDB进程进行系统状态监控，或使用IoTDB为用户开放的接口查看数据统计量。

### 系统状态监控

进入Jconsole监控页面后，首先看到的是IoTDB各类运行情况的概览。在这里，您可以看到[堆内存信息、线程信息、类信息以及服务器的CPU使用情况](https://docs.oracle.com/javase/7/docs/technotes/guides/management/jconsole.html)。

### 数据统计监控

本模块是IoTDB为用户提供的对其中存储数据信息的数据统计监控方式，我们会在系统中为您记录各个模块的数据统计信息，并将其汇总存入数据库中。当前0.8.0版本的IoTDB提供IoTDB写入数据的统计功能。

用户可以选择开启或关闭数据统计监控功能（您可以设定配置文件中的`enable_stat_monitor`项，详细信息参见[第4.2节](/#/Documents/0.8.0/chap4/sec2)）。

#### 写入数据统计

系统目前对写入数据的统计可分为两大模块： 全局（Global） 写入数据统计和存储组（Storage Group） 写入数据统计。 全局统计量记录了所有写入数据的点数、请求数统计，存储组统计量对某一个存储组的写入数据进行了统计，系统默认设定每 5 秒 （若需更改统计频率，您可以设定配置文件中的`back_loop_period_in_second`项，详细信息参见本文[4.2节](/#/Documents/0.8.0/chap4/sec2)） 将统计量写入 IoTDB 中，并以系统指定的命名方式存储。系统刷新或者重启后， IoTDB 不对统计量做恢复处理，统计量从零值重新开始计算。

为了避免统计信息占用过多空间，我们为统计信息加入定期清除无效数据的机制。系统将每隔一段时间删除无效数据。用户可以通过设置删除机制触发频率（`stat_monitor_retain_interval_in_second`项，默认为600s，详细信息参见本文[4.2节](/#/Documents/0.8.0/chap4/sec2)）配置删除数据的频率，通过设置有效数据的期限（`stat_monitor_detect_freq_in_second`项，默认为600s，详细信息参见本文[4.2节](/#/Documents/0.8.0/chap4/sec2)）设置有效数据的范围，即距离清除操作触发时间为`stat_monitor_detect_freq_in_second`以内的数据为有效数据。为了保证系统的稳定，不允许频繁地删除统计量，因此如果配置参数的时间小于默认值，系统不采用配置参数而使用默认参数。

注：当前 0.8.0 版本统计的写入数据统计信息会同时统计用户写入的数据与系统内部监控数据。

写入数据统计项列表：

* TOTAL_POINTS (全局)

|名字| TOTAL\_POINTS |
|:---:|:---|
|描述| 写入总点数|
|时间序列名称| root.stats.write.global.TOTAL\_POINTS |
|服务器重启后是否重置| 是 |
|例子| select TOTAL_POINTS from root.stats.write.global|

* TOTAL\_REQ\_SUCCESS (全局)

|名字| TOTAL\_REQ\_SUCCESS |
|:---:|:---|
|描述| 写入请求成功次数|
|时间序列名称| root.stats.write.global.TOTAL\_REQ\_SUCCESS |
|服务器重启后是否重置| 是 |
|例子| select TOTAL\_REQ\_SUCCESS from root.stats.write.global|

* TOTAL\_REQ\_FAIL (全局)

|名字| TOTAL\_REQ\_FAIL |
|:---:|:---|
|描述| 写入请求失败次数|
|时间序列名称| root.stats.write.global.TOTAL\_REQ\_FAIL |
|服务器重启后是否重置| 是 |
|例子| select TOTAL\_REQ\_FAIL from root.stats.write.global|


* TOTAL\_POINTS\_FAIL (全局)

|名字| TOTAL\_POINTS\_FAIL |
|:---:|:---|
|描述| 写入点数数百次数|
|时间序列名称| root.stats.write.global.TOTAL\_POINTS\_FAIL |
|服务器重启后是否重置| 是 |
|例子| select TOTAL\_POINTS\_FAIL from root.stats.write.global|


* TOTAL\_POINTS\_SUCCESS (全局)

|名字| TOTAL\_POINTS\_SUCCESS |
|:---:|:---|
|描述| 写入点数成功次数|
|时间序列名称| root.stats.write.global.TOTAL\_POINTS\_SUCCESS |
|服务器重启后是否重置| 是 |
|例子| select TOTAL\_POINTS\_SUCCESS from root.stats.write.global|

* TOTAL\_REQ\_SUCCESS (STORAGE GROUP)

|名字| TOTAL\_REQ\_SUCCESS |
|:---:|:---|
|描述| 写入存储组成功次数|
|时间序列名称| root.stats.write.\<storage\_group\_name\>.TOTAL\_REQ\_SUCCESS |
|服务器重启后是否重置| 是 |
|例子| select TOTAL\_REQ\_SUCCESS from root.stats.write.\<storage\_group\_name\>|

* TOTAL\_REQ\_FAIL (STORAGE GROUP)

|名字| TOTAL\_REQ\_FAIL |
|:---:|:---|
|描述| 写入某个Storage group的请求失败次数|
|时间序列名称| root.stats.write.\<storage\_group\_name\>.TOTAL\_REQ\_FAIL |
|服务器重启后是否重置| 是 |
|例子| select TOTAL\_REQ\_FAIL from root.stats.write.\<storage\_group\_name\>|


* TOTAL\_POINTS\_SUCCESS (STORAGE GROUP)

|名字| TOTAL\_POINTS\_SUCCESS |
|:---:|:---|
|描述| 写入某个Storage group成功的点数|
|时间序列名称| root.stats.write.\<storage\_group\_name\>.TOTAL\_POINTS\_SUCCESS |
|服务器重启后是否重置| 是 |
|例子| select TOTAL\_POINTS\_SUCCESS from root.stats.write.\<storage\_group\_name\>|


* TOTAL\_POINTS\_FAIL (STORAGE GROUP)

|名字| TOTAL\_POINTS\_FAIL |
|:---:|:---|
|描述| 写入某个Storage group失败的点数|
|时间序列名称| root.stats.write.\<storage\_group\_name\>.TOTAL\_POINTS\_FAIL |
|服务器重启后是否重置| 是 |
|例子| select TOTAL\_POINTS\_FAIL from root.stats.write.\<storage\_group\_name\>|

> 其中，\<storage\_group\_name\> 为所需进行数据统计的存储组名称，存储组中的“.”使用“_”代替。例如：名为'root.a.b'的存储组命名为：'root\_a\_b'。

下面为您展示两个具体的例子。用户可以通过`SELECT`语句查询自己所需要的写入数据统计项。（查询方法与普通的时间序列查询方式一致）

我们以查询全局统计量总写入成功数（`TOTAL_POINTS_SUCCES`）为例，用IoTDB SELECT语句查询它的值。SQL语句如下：

```
select TOTAL_POINTS_SUCCESS from root.stats.write.global
```

我们以查询存储组root.ln的统计量总写入成功数（`TOTAL_POINTS_SUCCESS`）为例，用IoTDB SELECT语句查询它的值。SQL语句如下：

```
select TOTAL_POINTS_SUCCESS from root.stats.write.root_ln
```

若您需要查询当前系统的写入统计信息，您可以使用`MAX_VALUE()`聚合函数进行查询，SQL语句如下：
```
select MAX_VALUE(TOTAL_POINTS_SUCCESS) from root.stats.write.root_ln
```
