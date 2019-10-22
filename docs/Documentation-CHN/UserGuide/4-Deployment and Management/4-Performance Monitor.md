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
## 性能监控
### 引言

性能监控模块用来监控IOTDB每一个操作的耗时，以便用户更好的了解数据库的整体性能。此模块会统计每一种操作的平均耗时，以及耗时在一定时间区间内（1ms，4ms，16ms，64ms，256ms，1024ms，以上）的操作的比例。输出文件在log_measure.log中。输出样例如下：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/60937461-14296f80-a303-11e9-9602-a7bed624bfb3.png">
 
### 配置参数

配置文件位置：conf/iotdb-engine.properties

<center>**表 -配置参数以及描述项**

|参数|默认值|描述|
|:---|:---|:---|
|enable\_performance\_stat|false|是否开启性能监控模块|
|performance\_stat\_display\_interval|60000|打印统计结果的时间延迟，以毫秒为单位|
|performance_stat_memory_in_kb|20|性能监控模块使用的内存阈值，单位为KB|
</center>
 
### 利用JMX MBean动态调节参数

通过端口31999连接jconsole，并在上方菜单项中选择‘MBean’. 展开侧边框并选择 'org.apache.iotdb.db.cost.statistic'. 将会得到如下图所示结果：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/60937484-30c5a780-a303-11e9-8e92-04c413df2088.png">
 
**属性**

1. EnableStat：是否开启性能监控模块，如果被设置为true，则性能监控模块会记录每个操作的耗时并打印结果。这个参数不能直接通过jconsole直接更改，但可通过下方的函数来进行动态设置。
2. DisplayIntervalInMs：相邻两次打印结果的时间间隔。这个参数可以直接设置，但它要等性能监控模块重启才会生效。重启性能监控模块可以通过先调用 stopStatistic()然后调用startContinuousStatistics()或者直接调用 startOneTimeStatistics()实现。
3. OperationSwitch：这个属性用来展示针对每一种操作是否开启了监控统计，map的键为操作的名字，值为是否针对这种操作开启性能监控。这个参数不能直接通过jconsole直接更改，但可通过下方的 'changeOperationSwitch()'函数来进行动态设置。

**操作**

1. startContinuousStatistics：开启性能监控并以‘DisplayIntervalInMs’的时间间隔打印统计结果。 
2. startOneTimeStatistics：开启性能监控并以‘DisplayIntervalInMs’的时间延迟打印一次统计结果。 
3. stopStatistic：关闭性能监控。
4. clearStatisticalState(): 清除以统计的结果，从新开始统计。
5. changeOperationSwitch(String operationName, Boolean operationState):设置是否针对每一种不同的操作开启监控。参数‘operationName是操作的名称，在OperationSwitch属性中展示了所有操作的名称。参数 ‘operationState’是操作的状态，打开或者关闭。如果状态设置成功则此函数会返回true，否则返回false。
 
### 自定义操作类型监控其他区域

**增加操作项**

在org.apache.iotdb.db.cost.statistic.Operation类中增加一个枚举项来表示新增的操作.

**在监控区域增加监控代码**

在监控开始区域增加计时代码:

	long t0 = System. currentTimeMillis();


在监控结束区域增加记录代码: 

	Measurement.INSTANCE.addOperationLatency(Operation, t0);

## cache命中率统计
### 概述

为了提高查询性能，IOTDB对ChunkMetaData和TsFileMetaData进行了缓存。用户可以通过debug级别的日志以及MXBean两种方式来查看缓存的命中率，并根据缓存命中率以及系统内存来调节缓存所使用的内存大小。使用MXBean查看缓存命中率的方法为：
1. 通过端口31999连接jconsole，并在上方菜单项中选择‘MBean’. 
2. 展开侧边框并选择 'org.apache.iotdb.db.service'. 将会得到如下图所示结果：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/65687623-404fc380-e09c-11e9-83c3-3c7c63a5b0be.jpeg">