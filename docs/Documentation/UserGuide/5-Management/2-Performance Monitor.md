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

# Chapter 5: Management
## Performance Monitor
### Introduction

In order to grasp the performance of iotdb, we add this module to count the time-consuming of each operation. This module can statistic the avg time-consuming of each operation and the proportion of each operation fall into a time range. The output is in log_measure.log file. A output example is in below.  

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/60937461-14296f80-a303-11e9-9602-a7bed624bfb3.png">
 
### Configuration parameter

location：conf/iotdb-engine.properties

<center>**Table -parameter and description**

|Parameter|Default Value|Description|
|:---|:---|:---|
|enable\_performance\_stat|false|Is stat performance of sub-module enable.|
|performance\_stat\_display\_interval|60000|The interval of display statistic result in ms.|
|performance_stat_memory_in_kb|20|The memory used for performance_stat in kb.|
</center>
 
### JMX MBean

Connect to jconsole with port 31999，and choose ‘MBean’in menu bar. Expand the sidebar and choose 'org.apache.iotdb.db.cost.statistic'. You can Find：
 
<img style="width:100%; max-width:600px; max-height:200px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/60937484-30c5a780-a303-11e9-8e92-04c413df2088.png">

**Attribute**

1. EnableStat：Whether the statistics are enable or not, if it is true, the module records the time-consuming of each operation and prints the results; It can not be set dynamically but changed by function in below.

2. DisplayIntervalInMs：The interval between print results. It can be set dynamically, but will take effect after restart.( First call stopStatistic(), then call startContinuousStatistics() or startOneTimeStatistics()）
3. OperationSwitch：It's a map to indicate whether stat the operation, the key is operation name and the value is stat state. This parameter cannot be changed directly, it's change by operation 'changeOperationSwitch()'. 

**Operation**

1. startContinuousStatistics： Start the statistics and output at interval of ‘DisplayIntervalInMs’.
2. startOneTimeStatistics：Start the statistics and output in delay of ‘DisplayIntervalInMs’.
3. stopStatistic：Stop the statistics.
4. clearStatisticalState(): clear current stat result, reset statistical result.
5. changeOperationSwitch(String operationName, Boolean operationState):set whether to monitor operation status. The param 'operationName' is the name of operation, defined in attribute operationSwitch. The param operationState is the state of operation. If state-switch successful the function will return true, else return false.
 
### Adding Custom Monitoring Items for developer of IOTDB

**Add Operation**

Add an enumeration in org.apache.iotdb.db.cost.statistic.Operation.

**Add Timing Code in Monitoring Area**

Add timing code in the monitoring start area:

	long t0 = System. currentTimeMillis();


Add timing code in the monitoring stop area: 

	Measurement.INSTANCE.addOperationLatency(Operation, t0);


## Cache Hit Ratio Statistics
### Overview

To improve query performance, IOTDB caches ChunkMetaData and TsFileMetaData. Users can view the cache hit rate through debug level log and MXBean, and adjust the memory occupied by the cache according to the cache hit rate and system memory. The method of using MXBean to view cache hit ratio is as follows:
1. Connect to jconsole with port 31999 and select 'MBean' in the menu item above.
2. Expand the sidebar and select 'org.apache.iotdb.db.service'. You will get the results shown in the following figure:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/65687623-404fc380-e09c-11e9-83c3-3c7c63a5b0be.jpeg">
