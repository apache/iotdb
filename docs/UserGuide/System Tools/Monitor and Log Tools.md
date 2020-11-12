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
This section describes how to use the JConsole ```Mbean``` tab to monitor the number of files opened by the IoTDB service process, the size of the data file, and so on. Once connected to JMX, you can find the ```MBean``` named ```org.apache.iotdb.service``` through the ```MBeans``` tab, as shown in the following Figure.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/20263106/53316064-54aec080-3901-11e9-9a49-76563ac09192.png">

There are several attributes under Monitor, including the numbers of files opened in different folders, the data file size statistics and the values of some system parameters. By double-clicking the value corresponding to an attribute it also displays a line chart of that attribute. Currently, all the opened file count statistics are only supported on ```MacOS``` and most ```Linux``` distro except ```CentOS```. For the OS not supported these statistics returns ```-2```. See the following section for specific introduction of the Monitor attributes.

##### MBean Monitor Attributes List

* DataSizeInByte

|Name| DataSizeInByte |
|:---:|:---|
|Description| The total size of data file.|
|Unit| Byte |
|Type| Long |

* FileNodeNum

|Name| FileNodeNum |
|:---:|:---|
|Description| The count number of FileNode. (Currently not supported)|
|Type| Long |

* OverflowCacheSize

|Name| OverflowCacheSize |
|:---:|:---|
|Description| The size of out-of-order data cache. (Currently not supported)|
|Unit| Byte |
|Type| Long |

* BufferWriteCacheSize

|Name| BufferWriteCacheSize |
|:---:|:---|
|Description| The size of BufferWriter cache. (Currently not supported)|
|Unit| Byte |
|Type| Long |

* BaseDirectory

|Name| BaseDirectory |
|:---:|:---|
|Description| The absolute directory of data file. |
|Type| String |

* WriteAheadLogStatus

|Name| WriteAheadLogStatus |
|:---:|:---|
|Description| The status of write-ahead-log (WAL). ```True``` means WAL is enabled. |
|Type| Boolean |

* TotalOpenFileNum

|Name| TotalOpenFileNum |
|:---:|:---|
|Description| All the opened file number of IoTDB server process. |
|Type| Int |

* DeltaOpenFileNum

|Name| DeltaOpenFileNum |
|:---:|:---|
|Description| The opened TsFile file number of IoTDB server process. |
|Default Directory| /data/data/settled |
|Type| Int |

* WalOpenFileNum

|Name| WalOpenFileNum |
|:---:|:---|
|Description| The opened write-ahead-log file number of IoTDB server process. |
|Default Directory| /data/wal |
|Type| Int |

* MetadataOpenFileNum

|Name| MetadataOpenFileNum |
|:---:|:---|
|Description| The opened meta-data file number of IoTDB server process. |
|Default Directory| /data/system/schema |
|Type| Int |

* DigestOpenFileNum

|Name| DigestOpenFileNum |
|:---:|:---|
|Description| The opened info file number of IoTDB server process. |
|Default Directory| /data/system/info |
|Type| Int |

* SocketOpenFileNum

|Name| SocketOpenFileNum |
|:---:|:---|
|Description| The Socket link (TCP or UDP) number of the operation system. |
|Type| Int |

* MergePeriodInSecond

|Name| MergePeriodInSecond |
|:---:|:---|
|Description| The interval at which the IoTDB service process periodically triggers the merge process. |
|Unit| Second |
|Type| Long |

* ClosePeriodInSecond

|Name| ClosePeriodInSecond |
|:---:|:---|
|Description| The interval at which the IoTDB service process periodically flushes memory data to disk. |
|Unit| Second |
|Type| Long |

### Data Status Monitoring

This module is the statistical monitoring method provided by IoTDB for users to store data information. The statistical data are recorded in the system and stored in the database. The current 0.8.0 version of IoTDB provides statistics for writing data.

The user can choose to enable or disable the data statistics monitoring function (set the `enable_stat_monitor` item in the configuration file).

#### Writing Data Monitor

The current statistics of writing data by the system can be divided into two major modules: **Global Writing Data Statistics** and **Storage Group Writing Data Statistics**. **Global Writing Data Statistics** records the point number written by the user and the number of requests. **Storage Group Writing Data Statistics** records data of a certain storage group. 

The system defaults to collect data every 5 seconds, and writes the statistics to the IoTDB and stores them in a system-specified locate. (If you need to change the statistic frequency, you can set The `back_loop_period_in_second entry` in the configuration file, see Section [Engine Layer](../Server/Single%20Node%20Setup.md) for details). After the system is refreshed or restarted, IoTDB does not recover the statistics, and the statistics data will restart from zero.

To avoid the excessive use of statistical information, a mechanism is set to periodically clear invalid data for statistical information. The system deletes invalid data at regular intervals. The user set the trigger frequency (`stat_monitor_retain_interval_in_second`, default is 600s, see section [Engine Layer](../Server/Single%20Node%20Setup.md) for details) to set the frequency of deleting data. By setting the valid data duration (`stat_monitor_detect_freq_in_second entry`, the default is 600s, see section [Engine Layer](../Server/Single%20Node%20Setup.md) for details) to set the time period of valid data, that is, the data within the time of the clear operation trigger time is stat_monitor_detect_freq_in_second is valid data. In order to ensure the stability of the system, it is not allowed to delete the statistics frequently. Therefore, if the configuration parameter time is less than the default value (600s), the system will abort the configuration parameter and use the default parameter.

It's convenient for you to use `select` clause to get the writing data statistics the same as other timeseries.

Here are the writing data statistics:

* TOTAL_POINTS (GLOABAL)

|Name| TOTAL\_POINTS |
|:---:|:---|
|Description| Calculate the global writing points number.|
|Type| Writing data statistics |
|Timeseries Name| root.stats.write.global.TOTAL\_POINTS |
|Reset After Restarting System| yes |
|Example| select TOTAL_POINTS from root.stats.write.global|

* TOTAL\_REQ\_SUCCESS (GLOABAL)

|Name| TOTAL\_REQ\_SUCCESS |
|:---:|:---|
|Description| Calculate the global successful requests number.|
|Type| Writing data statistics |
|Timeseries Name| root.stats.write.global.TOTAL\_REQ\_SUCCESS |
|Reset After Restarting System| yes |
|Example| select TOTAL\_REQ\_SUCCESS from root.stats.write.global|

* TOTAL\_REQ\_FAIL (GLOABAL)

|Name| TOTAL\_REQ\_FAIL |
|:---:|:---|
|Description| Calculate the global failed requests number.|
|Type| Writing data statistics |
|Timeseries Name| root.stats.write.global.TOTAL\_REQ\_FAIL |
|Reset After Restarting System| yes |
|Example| select TOTAL\_REQ\_FAIL from root.stats.write.global|


* TOTAL\_POINTS\_FAIL (GLOABAL)

|Name| TOTAL\_POINTS\_FAIL |
|:---:|:---|
|Description| Calculate the global failed writing points number.|
|Type| Writing data statistics |
|Timeseries Name| root.stats.write.global.TOTAL\_POINTS\_FAIL |
|Reset After Restarting System| yes |
|Example| select TOTAL\_POINTS\_FAIL from root.stats.write.global|


* TOTAL\_POINTS\_SUCCESS (GLOABAL)

|Name| TOTAL\_POINTS\_SUCCESS |
|:---:|:---|
|Description| Calculate the c.|
|Type| Writing data statistics |
|Timeseries Name| root.stats.write.global.TOTAL\_POINTS\_SUCCESS |
|Reset After Restarting System| yes |
|Example| select TOTAL\_POINTS\_SUCCESS from root.stats.write.global|

* TOTAL\_REQ\_SUCCESS (STORAGE GROUP)

|Name| TOTAL\_REQ\_SUCCESS |
|:---:|:---|
|Description| Calculate the successful requests number for specific storage group|
|Type| Writing data statistics |
|Timeseries Name| root.stats.write.\<storage\_group\_name\>.TOTAL\_REQ\_SUCCESS |
|Reset After Restarting System| yes |
|Example| select TOTAL\_REQ\_SUCCESS from root.stats.write.\<storage\_group\_name\>|

* TOTAL\_REQ\_FAIL (STORAGE GROUP)

|Name| TOTAL\_REQ\_FAIL |
|:---:|:---|
|Description| Calculate the fail requests number for specific storage group|
|Type| Writing data statistics |
|Timeseries Name| root.stats.write.\<storage\_group\_name\>.TOTAL\_REQ\_FAIL |
|Reset After Restarting System| yes |
|Example| select TOTAL\_REQ\_FAIL from root.stats.write.\<storage\_group\_name\>|


* TOTAL\_POINTS\_SUCCESS (STORAGE GROUP)

|Name| TOTAL\_POINTS\_SUCCESS |
|:---:|:---|
|Description| Calculate the successful writing points number for specific storage group.|
|Type| Writing data statistics |
|Timeseries Name| root.stats.write.\<storage\_group\_name\>.TOTAL\_POINTS\_SUCCESS |
|Reset After Restarting System| yes |
|Example| select TOTAL\_POINTS\_SUCCESS from root.stats.write.\<storage\_group\_name\>|


* TOTAL\_POINTS\_FAIL (STORAGE GROUP)

|Name| TOTAL\_POINTS\_FAIL |
|:---:|:---|
|Description| Calculate the fail writing points number for specific storage group.|
|Type| Writing data statistics |
|Timeseries Name| root.stats.write.\<storage\_group\_name\>.TOTAL\_POINTS\_FAIL |
|Reset After Restarting System| yes |
|Example| select TOTAL\_POINTS\_FAIL from root.stats.write.\<storage\_group\_name\>|

> Note: 
> 
> \<storage\_group\_name\> should be replaced by real storage group name, and the '.' in storage group need to be replaced by '_'. For example, the storage group name is 'root.a.b', when using in the statistics, it will change to 'root\_a\_b'

##### Example

Here we give some example of using writing data statistics.

To know the global successful writing points number, use `select` clause to query it's value. The query statement is:

```
select TOTAL_POINTS_SUCCESS from root.stats.write.global
```

To know the successfule writing points number of root.ln (storage group), the query statement is:

```
select TOTAL_POINTS_SUCCESS from root.stats.write.root_ln
```

To know the current timeseries point in the system, use `MAX_VALUE` function to query. Here is the query statement:

```
select MAX_VALUE(TOTAL_POINTS_SUCCESS) from root.stats.write.root_ln
```

#### File Size Monitor

Sometimes we are concerned about how the data file size of IoTDB changes, maybe to help calculate how much disk space is left or the data ingestion speed. The File Size Monitor provides several statistics to show how different types of file-sizes change. 

The file size monitor defaults to collect file size data every 5 seconds using the same shared parameter ```back_loop_period_in_second```, 

Unlike Writing Data Monitor, currently File Size Monitor does not delete statistic data at regular intervals. 

You can also use `select` clause to get the file size statistics like other time series.

Here are the file size statistics:

* DATA

|Name| DATA |
|:---:|:---|
|Description| Calculate the sum of all the files's sizes under the data directory (```data/data``` by default) in byte.|
|Type| File size statistics |
|Timeseries Name| root.stats.file\_size.DATA |
|Reset After Restarting System| No |
|Example| select DATA from root.stats.file\_size.DATA|

* SETTLED

|Name| SETTLED |
|:---:|:---|
|Description| Calculate the sum of all the ```TsFile``` size (under ```data/data/settled``` by default) in byte. If there are multiple ```TsFile``` directories like ```{data/data/settled1, data/data/settled2}```, this statistic is the sum of their size.|
|Type| File size statistics |
|Timeseries Name| root.stats.file\_size.SETTLED |
|Reset After Restarting System| No |
|Example| select SETTLED from root.stats.file\_size.SETTLED|

* OVERFLOW

|Name| OVERFLOW |
|:---:|:---|
|Description| Calculate the sum of all the ```out-of-order data file``` size (under ```data/data/unsequence``` by default) in byte.|
|Type| File size statistics |
|Timeseries Name| root.stats.file\_size.OVERFLOW |
|Reset After Restarting System| No |
|Example| select OVERFLOW from root.stats.file\_size.OVERFLOW|


* WAL

|Name| WAL |
|:---:|:---|
|Description| Calculate the sum of all the ```Write-Ahead-Log file``` size (under ```data/wal``` by default) in byte.|
|Type| File size statistics |
|Timeseries Name| root.stats.file\_size.WAL |
|Reset After Restarting System| No |
|Example| select WAL from root.stats.file\_size.WAL|


* INFO

|Name| INFO|
|:---:|:---|
|Description| Calculate the sum of all the ```.restore```, etc. file size (under ```data/system/info```) in byte.|
|Type| File size statistics |
|Timeseries Name| root.stats.file\_size.INFO |
|Reset After Restarting System| No |
|Example| select INFO from root.stats.file\_size.INFO|

* SCHEMA

|Name| SCHEMA |
|:---:|:---|
|Description| Calculate the sum of all the ```metadata file``` size (under ```data/system/metadata```) in byte.|
|Type| File size statistics |
|Timeseries Name| root.stats.file\_size.SCHEMA |
|Reset After Restarting System| No |
|Example| select SCHEMA from root.stats.file\_size.SCHEMA|

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
