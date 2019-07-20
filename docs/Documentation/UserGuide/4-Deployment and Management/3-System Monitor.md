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

# Chapter 4: Deployment and Management

## System Monitor

Currently, IoTDB provides users to use Java's JConsole tool to monitor system status or use IoTDB's open API to check data status.

### System Status Monitoring

After starting JConsole tool and connecting to IoTDB server, you will have a basic look at IoTDB system status(CPU Occupation, in-memory information, etc.). See [official documentation](https://docs.oracle.com/javase/7/docs/technotes/guides/management/jconsole.html) for more informations.

#### JMX MBean Monitoring
By using JConsole tool and connecting with JMX you can see some system statistics and parameters.
This section describes how to use the JConsole ```Mbean``` tab to monitor the number of files opened by the IoTDB service process, the size of the data file, and so on. Once connected to JMX, you can find the ```MBean``` named ```org.apache.iotdb.service``` through the ```MBeans``` tab, as shown in the following Figure.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/20263106/53316064-54aec080-3901-11e9-9a49-76563ac09192.png">

There are several attributes under Monitor, including the numbers of files opened in different folders, the data file size statistics and the values of some system parameters. By double-clicking the value corresponding to an attribute it can also display a line chart of that attribute. In particular, all the opened file count statistics are currently only supported on ```MacOS``` and most ```Linux``` distro except ```CentOS```. For the OS not supported these statistics will return ```-2```. See the following section for specific introduction of the Monitor attributes.

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

This module is the statistical monitoring method provided by IoTDB for users to store data information. We will record the statistical data in the system and store it in the database. The current 0.9.0 version of IoTDB provides statistics for writing data.

The user can choose to enable or disable the data statistics monitoring function (set the `enable_stat_monitor` item in the configuration file, see [Engine Layer](/#/Documents/latest/chap4/sec2) for details).

#### Writing Data Monitor

The current statistics of writing data by the system can be divided into two major modules: **Global Writing Data Statistics** and **Storage Group Writing Data Statistics**. **Global Writing Data Statistics** records the point number written by the user and the number of requests. **Storage Group Writing Data Statistics** records data of a certain storage group. 

The system defaults to collect data every 5 seconds, and writes the statistics to the IoTDB and stores them in a system-specified locate. (If you need to change the statistic frequency, you can set The `back_loop_period_in_second entry` in the configuration file, see Section [Engine Layer](/#/Documents/latest/chap4/sec2) for details). After the system is refreshed or restarted, IoTDB does not recover the statistics, and the statistics data will restart from zero.

In order to avoid the excessive use of statistical information, we add a mechanism to periodically clear invalid data for statistical information. The system will delete invalid data at regular intervals. The user can set the trigger frequency (`stat_monitor_retain_interval_in_second`, default is 600s, see section [Engine Layer](/#/Documents/latest/chap4/sec2) for details) to set the frequency of deleting data. By setting the valid data duration (`stat_monitor_detect_freq_in_second entry`, the default is 600s, see section [Engine Layer](/#/Documents/latest/chap4/sec2) for details) to set the time period of valid data, that is, the data within the time of the clear operation trigger time is stat_monitor_detect_freq_in_second is valid data. In order to ensure the stability of the system, it is not allowed to delete the statistics frequently. Therefore, if the configuration parameter time is less than the default value (600s), the system will abort the configuration parameter and uses the default parameter.

It's convenient for you to use `select` clause to get the writing data statistics the same as other timeseires.

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

If you want to know the global successful writing points number, you can use `select` clause to query it's value. The query statement is like this:

```
select TOTAL_POINTS_SUCCESS from root.stats.write.global
```

If you want to know the successfule writing points number of root.ln (storage group), here is the query statement:

```
select TOTAL_POINTS_SUCCESS from root.stats.write.root_ln
```

If you want to know the current timeseries point in the system, you can use `MAX_VALUE` function to query. Here is the query statement:

```
select MAX_VALUE(TOTAL_POINTS_SUCCESS) from root.stats.write.root_ln
```

#### File Size Monitor

Sometimes we are concerned about how the data file size of IoTDB is changing, maybe to help calculate how much disk space is left or the data ingestion speed. The File Size Monitor provides several statistics to show how different types of file-sizes change. 

The file size monitor defaults to collect file size data every 5 seconds using the same shared parameter ```back_loop_period_in_second```, 

Unlike Writing Data Monitor, currently File Size Monitor will not delete statistic data at regular intervals. 

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
