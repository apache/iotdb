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

# Monitor and Log Tools

## System Monitor

Currently, IoTDB provides users Java's JConsole tool to monitor system status or use IoTDB's open API to check data status.

### System Status Monitoring

After starting JConsole tool and connecting to IoTDB server, a basic look at IoTDB system status(CPU Occupation, in-memory information, etc.) is provided. See [official documentation](https://docs.oracle.com/javase/7/docs/technotes/guides/management/jconsole.html) for more information.

#### JMX MBean Monitoring
By using JConsole tool and connecting with JMX you are provided with some system statistics and parameters.

This section describes how to use the JConsole ```Mbean```tab of jconsole to monitor some system configurations of IoTDB, the statistics of writing, and so on. After connecting to JMX, you can find the "MBean" of "org.apache.iotdb.service", as shown in the figure below.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/34242296/92922876-16d4a700-f469-11ea-874d-dcf58d5bb1b3.png"> <br>

There are several attributes under monitor, including data file directory, the statistics of writing and the values of some system parameters. It can also display a line chart of the property by double clicking the value corresponding to the property. For a detailed description of monitor attributes, see the following sections.

##### MBean Monitor Attributes List

* SystemDirectory

|Name| SystemDirectory |
|:---:|:---|
|Description| The absolute directory of data system file. |
|Type| String |

* DataSizeInByte

|Name| DataSizeInByte |
|:---:|:---|
|Description| The total size of data file.|
|Unit| Byte |
|Type| Long |

* EnableStatMonitor

|Name| EnableStatMonitor |
|:---:|:---|
|Description| If the monitor module is open |
|Type| Boolean |

### Data Monitoring

This module is for providing some statistics info about the writing operations:

- the data size (in bytes) in IoTDB, the number of data points in IoTDB;
- how many operations are successful or failed executed.

#### Enable/disable the module

Users can choose to enable or disable the feature of data statistics monitoring (set the `enable_stat_monitor` item in the configuration file).

#### Statistics Data Storing

By default, the statistics data is only saved in memory and can be accessed using Jconsole.

The data can also be written as some time series on disk. To enable it, set `enable_monitor_series_write=true` in the configuration file. If so, using `select` statement in IoTDB-cli can query these time series.

> Note:
> if `enable_monitor_series_write=true`, when IoTDB is restarted, the previous statistics data will be recovered into memory.
> if `enable_monitor_series_write=false`, IoTDB will forget all statistics data after the instance is restarted.

#### Writing Data Monitor

At present, the monitor system can be divided into two modules: global writing statistics and storage group writing statistics. The global statistics records the number of total points and requests, and the storage group statistics counts the write data of each storage group.

The system sets the collection granularity of the monitoring module to **update the statistical information once one data file is flushed into the disk **, so the data accuracy may be different from the actual situation. To obtain accurate information, **Please call the `flush` method before querying **. 

Here are the writing data statistics (the range supported is shown in brackets):

* TOTAL_POINTS (GLOBAL)

|Name| TOTAL\_POINTS |
|:---:|:---|
|Description| Calculate the total number of global writing points. |
|Timeseries Name| root.stats.{"global" \|"storageGroupName"}.TOTAL\_POINTS |

* TOTAL\_REQ\_SUCCESS (GLOBAL)

|Name| TOTAL\_REQ\_SUCCESS |
|:---:|:---|
|Description| Calculate the number of global successful requests. |
|Timeseries Name|  root.stats."global".TOTAL\_REQ\_SUCCESS |

* TOTAL\_REQ\_FAIL (GLOBAL)

|Name| TOTAL\_REQ\_FAIL |
|:---:|:---|
|Description| Calculate the number of global failed requests. |
|Timeseries Name| root.stats."global".TOTAL\_REQ\_FAIL |

The above attributes also support visualization in JConsole. For the statistical information of each storage group, in order to avoid the display confusion caused by too many storage groups, the user can input the storage group name in the operation method under monitor MBean to query the corresponding statistical information.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/34242296/92922942-34a20c00-f469-11ea-8dc2-8229d454583c.png">

##### Example

Here we give some examples of using writing data statistics.

To know the total number of global writing points, use `select` clause to query it's value. The query statement is:

```sql
select TOTAL_POINTS from root.stats."global"
```

To know the total number of global writing points of root.ln (storage group), the query statement is:

```sql
select TOTAL_POINTS from root.stats."root.ln"
```

To know the latest statistics of the current system, you can use the latest data query. Here is the query statement:

```sql
flush
select last TOTAL_POINTS from root.stats."global"
```

## Performance Monitor

### Introduction

To grasp the performance of iotdb, this module is added to count the time-consumption of each operation. This module can compute the statistics of the avg time-consuming of each operation and the proportion of each operation whose time consumption falls into a time range. The output is in log_measure.log file. An output example is below.  

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

1. EnableStat：Whether the statistics are enabled or not, if it is true, the module records the time-consuming of each operation and prints the results; It is non-editable but can be changed by the function below.

2. DisplayIntervalInMs：The interval between print results. The changes will not take effect instantly. To make the changes effective, you should call startContinuousStatistics() or startOneTimeStatistics().
3. OperationSwitch：It's a map to indicate whether the statistics of one kind of operation should be computed, the key is operation name and the value is true means the statistics of the operation are enabled, otherwise disabled. This parameter cannot be changed directly, it's changed by operation 'changeOperationSwitch()'. 

**Operation**

1. startContinuousStatistics： Start the statistics and output at interval of ‘DisplayIntervalInMs’.
2. startOneTimeStatistics：Start the statistics and output in delay of ‘DisplayIntervalInMs’.
3. stopStatistic：Stop the statistics.
4. clearStatisticalState(): clear current stat result, reset statistical result.
5. changeOperationSwitch(String operationName, Boolean operationState):set whether to monitor a kind of operation. The param 'operationName' is the name of operation, defined in attribute operationSwitch. The param operationState is whether to enable the statistics or not. If the state is switched successfully, the function will return true, else return false.

### Adding Custom Monitoring Items for contributors of IOTDB

**Add Operation**

Add an enumeration in org.apache.iotdb.db.cost.statistic.Operation.

**Add Timing Code in Monitoring Area**

Add timing code in the monitoring start area:

    long t0 = System. currentTimeMillis();

Add timing code in the monitoring stop area: 

    Measurement.INSTANCE.addOperationLatency(Operation, t0);

## Cache Hit Ratio Statistics

### Overview

To improve query performance, IOTDB caches ChunkMetaData and TsFileMetaData. Users can view the cache hit ratio through debug level log and MXBean, and adjust the memory occupied by the cache according to the cache hit ratio and system memory. The method of using MXBean to view cache hit ratio is as follows:
1. Connect to jconsole with port 31999 and select 'MBean' in the menu item above.
2. Expand the sidebar and select 'org.apache.iotdb.db.service'. You will get the results shown in the following figure:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/65687623-404fc380-e09c-11e9-83c3-3c7c63a5b0be.jpeg">
## System log

IoTDB allows users to configure IoTDB system logs (such as log output level) by modifying the log configuration file. The default location of the system log configuration file is in \$IOTDB_HOME/conf folder. 

The default log configuration file is named logback.xml. The user can modify the configuration of the system running log by adding or changing the xml tree node parameters. It should be noted that the configuration of the system log using the log configuration file does not take effect immediately after the modification, instead, it will take effect after restarting the system. The usage of logback.xml is just as usual.

At the same time, in order to facilitate the debugging of the system by the developers and DBAs, we provide several JMX interfaces to dynamically modify the log configuration, and configure the Log module of the system in real time without restarting the system.

### Dynamic System Log Configuration

#### Connect JMX

Here we use JConsole to connect with JMX. 

Start the JConsole, establish a new JMX connection with the IoTDB Server (you can select the local process or input the IP and PORT for remote connection, the default operation port of the IoTDB JMX service is 31999). Fig 4.1 shows the connection GUI of JConsole.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577195-f94d7500-1ef3-11e9-999a-b4f67055d80e.png">

After connected, click `MBean` and find `ch.qos.logback.classic.default.ch.qos.logback.classic.jmx.JMXConfigurator`(As shown in fig 4.2).
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577204-fe122900-1ef3-11e9-9e89-2eb1d46e24b8.png">

In the JMXConfigurator Window, there are 6 operations provided, as shown in fig 4.3. You can use these interfaces to perform operation.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577216-09fdeb00-1ef4-11e9-9005-542ad7d9e9e0.png">

#### Interface Instruction

* reloadDefaultConfiguration

This method is to reload the default logback configuration file. The user can modify the default configuration file first, and then call this method to reload the modified configuration file into the system to take effect.

* reloadByFileName

This method loads a logback configuration file with the specified path and name, and then makes it take effect. This method accepts a parameter of type String named p1, which is the path to the configuration file that needs to be specified for loading.

* getLoggerEffectiveLevel

This method is to obtain the current log level of the specified Logger. This method accepts a String type parameter named p1, which is the name of the specified Logger. This method returns the log level currently in effect for the specified Logger.

* getLoggerLevel

This method is to obtain the log level of the specified Logger. This method accepts a String type parameter named p1, which is the name of the specified Logger. This method returns the log level of the specified Logger.
It should be noted that the difference between this method and the `getLoggerEffectiveLevel` method is that the method returns the log level that the specified Logger is set in the configuration file. If the user does not set the log level for the Logger, then return empty. According to Logger's log-level inheritance mechanism, a Logger's level is not explicitly set, it will inherit the log level settings from its nearest ancestor. At this point, calling the `getLoggerEffectiveLevel` method will return the log level in which the Logger is in effect; calling `getLoggerLevel` will return null.
