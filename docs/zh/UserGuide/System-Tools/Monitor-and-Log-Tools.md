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

## 监控工具

### 系统监控

当前用户可以使用 Java 的 JConsole 工具对正在运行的 IoTDB 进程进行系统状态监控，或使用 IoTDB 为用户开放的接口查看数据统计量。

#### 系统状态监控

进入 Jconsole 监控页面后，首先看到的是 IoTDB 各类运行情况的概览。在这里，您可以看到堆内存信息、线程信息、类信息以及服务器的 CPU 使用情况。

#### JMX MBean 监控

通过使用 JConsole 工具并与 JMX 连接，您可以查看一些系统统计信息和参数。
本节描述如何使用 JConsole 的 "Mbean" 选项卡来监视 IoTDB 的一些系统配置、写入数据统计等等。 连接到 JMX 后，您可以通过 "MBeans" 标签找到名为 "org.apache.iotdb.service" 的 "MBean"，如下图所示。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/112428223-ce7e3600-8d75-11eb-8e50-04f04571925b.png"> <br>

Monitor 下有几个属性，包括数据文件目录，写入数据统计信息以及某些系统参数的值。 通过双击与属性对应的值，它还可以显示该属性的折线图。有关 Monitor 属性的具体介绍，请参见以下部分。

 * MBean 监视器属性列表

- SystemDirectory

| 名称 | SystemDirectory        |
| :--: | :--------------------- |
| 描述 | 数据系统文件的绝对目录 |
| 类型 | String                 |

- DataSizeInByte

| 名称 | DataSizeInByte   |
| :--: | :--------------- |
| 描述 | 数据文件的总大小 |
| 单元 | Byte             |
| 类型 | Long             |

- EnableStatMonitor

| 名称 | EnableStatMonitor    |
| ---- | -------------------- |
| 描述 | 系统监控模块是否开启 |
| 类型 | Boolean              |

- WriteAheadLogStatus

| 名称 | WriteAheadLogStatus                       |
| :--: | :---------------------------------------- |
| 描述 | 预写日志（WAL）的状态。 True 表示启用 WAL。 |
| 类型 | Boolean                                   |

- GlobalTotalPointsNum

| 名称 | GlobalTotalPointsNum                       |
| :--: | :---------------------------------------- |
| 描述 | 总点数 |
| 类型 |     Long                               |

- GlobalReqSuccessNum
  
| 名称 | GlobalReqSuccessNum                       |
| :--: | :---------------------------------------- |
| 描述 |  成功的请求数 |
| 类型 |     Long                               |

- GlobalReqFailNum
  
| 名称 | GlobalReqFailNum                       |
| :--: | :---------------------------------------- |
| 描述 | 失败的请求数 |
| 类型 |      Long                              |

### 数据统计监控

本模块对数据写入操作提供一些统计信息的监控，包括：

- IoTDB 中数据文件的大小 （以字节为单位） ，数据写入的总点数
- 写入操作成功和失败的次数

#### 开启/关闭监控

用户可以选择开启或关闭数据统计监控功能（您可以设定配置文件中的`enable_stat_monitor`项，详细信息参见 [附录 1：配置参数](http://iotdb.apache.org/zh/UserGuide/Master/Appendix/Config-Manual.html)）。

### 统计数据存储

默认情况下，统计数据只储存在内存中，可以使用 Jconsole 进行访问。

统计数据还支持以时间序列的格式写入磁盘，可以通过设置配置文件中的`enable_monitor_series_write`开启。开启后，可以使用 `select` 命令来对这些统计数据进行查询。

> 请注意：
>
> 如果 `enable_monitor_series_write=true`, 当 IoTDB 重启时，之前的统计数据会被重新载入内存。
> 如果 `enable_monitor_series_write=false`, IoTDB 关闭时内存中所有的统计信息会被清空。

#### 写入数据统计

系统目前对写入数据的统计可分为两大模块： 全局（Global） 写入数据统计和存储组（Storage Group） 写入数据统计。全局统计量记录了所有写入数据的点数、请求数统计，存储组统计量对每一个存储组的写入数据进行了统计。

系统设定监控模块的采集粒度为**每个数据文件刷入磁盘时更新一次统计信息**，因此数据精度可能和实际有所出入。如需获取准确信息，**请先调用 flush 方法后再查询**。

写入数据统计项列表（括号内为统计量支持的范围）：

* TOTAL_POINTS （全局，存储组）

|     名字     | TOTAL\_POINTS                                                |
| :----------: | :----------------------------------------------------------- |
|     描述     | 写入总点数                                                   |
| 时间序列名称 | root.stats."global".TOTAL\_POINTS <br />root.stats."{storageGroupName}".TOTAL\_POINTS<br />其中 storageGroupName 是将存储组中的 `.` 替换成 `#` 得到<br />例如存储组 `root.sg` 的写入总点数记录在  root.stats."root#sg".TOTAL\_POINTS |

* TOTAL\_REQ\_SUCCESS （全局）

|         名字         | TOTAL\_REQ\_SUCCESS                                     |
|:--------------------:|:------------------------------------------------------- |
|         描述         | 写入请求成功次数                                        |
|     时间序列名称     | root.stats."global".TOTAL\_REQ\_SUCCESS             |

* TOTAL\_REQ\_FAIL （全局）

|         名字         | TOTAL\_REQ\_FAIL                                     |
|:--------------------:|:---------------------------------------------------- |
|         描述         | 写入请求失败次数                                     |
|     时间序列名称     | root.stats."global".TOTAL\_REQ\_FAIL             |

以上属性同样支持在 Jconsole 中进行可视化显示。对于每个存储组的统计信息，为了避免存储组过多造成显示混乱，用户可以在 Monitor Mbean 下的操作方法中输入存储组名进行相应的统计信息查询。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/34242296/92922942-34a20c00-f469-11ea-8dc2-8229d454583c.png"> <br>

#### 示例

下面为您展示两个具体的例子。用户可以通过`SELECT`语句查询自己所需要的写入数据统计项。（查询方法与普通的时间序列查询方式一致）

我们以查询全局统计量总点数（`TOTAL_POINTS`）为例，用 IoTDB SELECT 语句查询它的值。SQL 语句如下：

```sql
select TOTAL_POINTS from root.stats."global"
```

我们以查询存储组 root.ln 的统计量总点数（`TOTAL_POINTS`）为例，用 IoTDB SELECT 语句查询它的值。SQL 语句如下：

```sql
select TOTAL_POINTS from root.stats."root.ln"
```

若您需要查询当前系统的最新信息，您可以使用最新数据查询，SQL 语句如下：

```sql
flush
select last TOTAL_POINTS from root.stats."global"
```

### 性能监控

#### 介绍

性能监控模块用来监控 IOTDB 每一个操作的耗时，以便用户更好的了解数据库的整体性能。此模块会统计每一种操作的平均耗时，以及耗时在一定时间区间内（1ms，4ms，16ms，64ms，256ms，1024ms，以上）的操作的比例。输出文件在 log_measure.log 中。输出样例如下：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/60937461-14296f80-a303-11e9-9602-a7bed624bfb3.png">

#### 配置参数

配置文件位置：conf/iotdb-engine.properties

<center>

**表 -配置参数以及描述项**

|参数|默认值|描述|
|:---|:---|:---|
|enable\_performance\_stat|false|是否开启性能监控模块|
|performance\_stat\_display\_interval|60000|打印统计结果的时间延迟，以毫秒为单位|
|performance_stat_memory_in_kb|20|性能监控模块使用的内存阈值，单位为 KB|
</center>

#### 利用 JMX MBean 动态调节参数

通过端口 31999 连接 jconsole，并在上方菜单项中选择`MBean`. 展开侧边框并选择 `org.apache.iotdb.db.cost.statistic`. 将会得到如下图所示结果：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/112426751-721a1700-8d73-11eb-871c-db2e9c13cf99.png">

**属性**

1. EnableStat：是否开启性能监控模块，如果被设置为 true，则性能监控模块会记录每个操作的耗时并打印结果。这个参数不能直接通过 jconsole 直接更改，但可通过下方的函数来进行动态设置。
2. DisplayIntervalInMs：相邻两次打印结果的时间间隔。这个参数可以直接设置，但它要等性能监控模块重启才会生效。重启性能监控模块可以通过先调用 stopStatistic() 然后调用 startContinuousStatistics() 或者直接调用 startOneTimeStatistics() 实现。
3. OperationSwitch：这个属性用来展示针对每一种操作是否开启了监控统计，map 的键为操作的名字，值为是否针对这种操作开启性能监控。这个参数不能直接通过 jconsole 直接更改，但可通过下方的 'changeOperationSwitch()'函数来进行动态设置。

**操作**

1. startStatistics：开启性能监控。
2. startContinuousPrintStatistics：开启性能监控并以‘DisplayIntervalInMs’的时间间隔打印统计结果。 
3. startPrintStatisticsOnce：开启性能监控并以‘DisplayIntervalInMs’的时间延迟打印一次统计结果。 
4. stopPrintStatistic：关闭打印性能监控的统计结果。
5. stopStatistic：关闭性能监控。
6. clearStatisticalState(): 清除以统计的结果，从新开始统计。
7. changeOperationSwitch(String operationName, Boolean operationState): 设置是否针对每一种不同的操作开启监控。参数‘operationName 是操作的名称，在 OperationSwitch 属性中展示了所有操作的名称。参数 ‘operationState’是操作的状态，打开或者关闭。如果状态设置成功则此函数会返回 true，否则返回 false。

### 自定义操作类型监控其他区域

**增加操作项**

在 org.apache.iotdb.db.cost.statistic.Operation 类中增加一个枚举项来表示新增的操作。

**在监控区域增加监控代码**

在监控开始区域增加计时代码：

    long t0 = System. currentTimeMillis();

在监控结束区域增加记录代码：

    Measurement.INSTANCE.addOperationLatency(Operation, t0);

### cache 命中率统计

#### 概述

为了提高查询性能，IOTDB 对 ChunkMetaData 和 TsFileMetaData 进行了缓存。用户可以通过 debug 级别的日志以及 MXBean 两种方式来查看缓存的命中率，并根据缓存命中率以及系统内存来调节缓存所使用的内存大小。使用 MXBean 查看缓存命中率的方法为：
1. 通过端口 31999 连接 jconsole，并在上方菜单项中选择‘MBean’. 
2. 展开侧边框并选择 'org.apache.iotdb.db.service'. 将会得到如下图所示结果：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/112426760-73e3da80-8d73-11eb-9a8f-9232d1f2033b.png">

## 系统日志

IoTDB 支持用户通过修改日志配置文件的方式对 IoTDB 系统日志（如日志输出级别等）进行配置，系统日志配置文件默认位置在$IOTDB_HOME/conf 文件夹下，默认的日志配置文件名为 logback.xml。用户可以通过增加或更改其中的 xml 树型节点参数对系统运行日志的相关配置进行修改。详细配置说明参看本文日志文件配置说明。

同时，为了方便在系统运行过程中运维人员对系统的调试，我们为系统运维人员提供了动态修改日志配置的 JMX 接口，能够在系统不重启的前提下实时对系统的 Log 模块进行配置。详细使用方法参看动态系统日志配置说明）。

### 动态系统日志配置说明

#### 连接 JMX

本节以 Jconsole 为例介绍连接 JMX 并进入动态系统日志配置模块的方法。启动 Jconsole 控制页面，在新建连接处建立与 IoTDB Server 的 JMX 连接（可以选择本地进程或给定 IoTDB 的 IP 及 PORT 进行远程连接，IoTDB 的 JMX 服务默认运行端口为 31999），如下图使用远程进程连接 Localhost 下运行在 31999 端口的 IoTDB JMX 服务。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577195-f94d7500-1ef3-11e9-999a-b4f67055d80e.png">

连接到 JMX 后，您可以通过 MBean 选项卡找到名为`ch.qos.logback.classic`的`MBean`，如下图所示。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577204-fe122900-1ef3-11e9-9e89-2eb1d46e24b8.png">

在`ch.qos.logback.classic`的 MBean 操作（Operations）选项中，可以看到当前动态系统日志配置支持的 6 种接口，您可以通过使用相应的方法，来执行相应的操作，操作页面如图。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577216-09fdeb00-1ef4-11e9-9005-542ad7d9e9e0.png">

#### 动态系统日志接口说明

* reloadDefaultConfiguration 接口

该方法为重新加载默认的 logback 配置文件，用户可以先对默认的配置文件进行修改，然后调用该方法将修改后的配置文件重新加载到系统中，使其生效。

* reloadByFileName 接口

该方法为加载一个指定路径的 logback 配置文件，并使其生效。该方法接受一个名为 p1 的 String 类型的参数，该参数为需要指定加载的配置文件路径。

* getLoggerEffectiveLevel 接口

该方法为获取指定 Logger 当前生效的日志级别。该方法接受一个名为 p1 的 String 类型的参数，该参数为指定 Logger 的名称。该方法返回指定 Logger 当前生效的日志级别。

* getLoggerLevel 接口

该方法为获取指定 Logger 的日志级别。该方法接受一个名为 p1 的 String 类型的参数，该参数为指定 Logger 的名称。该方法返回指定 Logger 的日志级别。

需要注意的是，该方法与`getLoggerEffectiveLevel`方法的区别在于，该方法返回的是指定 Logger 在配置文件中被设定的日志级别，如果用户没有对该 Logger 进行日志级别的设定，则返回空。按照 Logback 的日志级别继承机制，如果一个 Logger 没有被显示地设定日志级别，其将会从其最近的祖先继承日志级别的设定。这时，调用`getLoggerEffectiveLevel`方法将返回该 Logger 生效的日志级别；而调用本节所述方法，将返回空。

* setLoggerLevel 接口

该方法为设置指定 Logger 的日志级别。该方法接受一个名为 p1 的 String 类型的参数和一个名为 p2 的 String 类型的参数，分别指定 Logger 的名称和目标的日志等级。
