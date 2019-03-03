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

<!-- TOC -->

- [Chapter4: Deployment and Management](#chapter4-deployment-and-management)
    - [Deployment](#deployment)
        - [Prerequisites](#prerequisites)
        - [Installation from  binary files](#installation-from--binary-files)
        - [Installation from source code](#installation-from-source-code)
        - [Installation from Docker (dockerfile)](#installation-by-docker-dockerfile)
    - [Configuration](#configuration)
        - [IoTDB Environment Configuration File](#iotdb-environment-configuration-file)
        - [IoTDB System Configuration File](#iotdb-system-configuration-file)
            - [File Layer](#file-layer)
            - [Engine Layer](#engine-layer)
    - [System Monitor](#system-monitor)
        - [System Status Monitoring](#system-status-monitoring)
            - [JMX MBean Monitoring](#jmx-mbean-monitoring)
                - [MBean Monitor Attributes List](#mbean-monitor-attributes-list)
        - [Data Status Monitoring](#data-status-monitoring)
            - [Writing Data Monitor](#writing-data-monitor)
                - [Example](#example)
    - [System log](#system-log)
        - [Dynamic System Log Configuration](#dynamic-system-log-configuration)
            - [Connect JMX](#connect-jmx)
            - [Interface Instruction](#interface-instruction)
    - [Data Management](#data-management)
        - [Data Files](#data-files)
        - [System Files](#system-files)
        - [Pre-write Log Files](#pre-write-log-files)
        - [Example of Setting Data storage Directory](#example-of-setting-data-storage-directory)

<!-- /TOC -->
# Chapter4: Deployment and Management

## Deployment

IoTDB provides you two installation methods, you can refer to the following suggestions, choose one of them:

* Installation from binary files. Download the binary files from the official website. This is the recommended method, in which you will get a binary released package which is out-of-the-box.
* Installation from source code. If you need to modify the code yourself, you can use this method.

### Prerequisites

To install and use IoTDB, you need to have:

1. Java >= 1.8 (Please make sure the environment path has been set)
2. Maven >= 3.0 (If you want to compile and install IoTDB from source code)
3. TsFile >= 0.7.0 (TsFile Github page: [https://github.com/thulab/tsfile](https://github.com/thulab/tsfile))
4. IoTDB-JDBC >= 0.7.0 (IoTDB-JDBC Github page: [https://github.com/thulab/iotdb-jdbc](https://github.com/thulab/iotdb-jdbc))

TODO: TsFile and IoTDB-JDBC dependencies will be removed after the project reconstruct.

### Installation from  binary files

IoTDB provides you binary files which contains all the necessary components for the IoTDB system to run. You can get them on our website [http://tsfile.org/download](http://tsfile.org/download). 

```
NOTE:
iotdb-<version>.tar.gz # For Linux or MacOS
iotdb-<version>.zip # For Windows
```

After downloading, you can extract the IoTDB tarball using the following operations:

```
Shell > uzip iotdb-<version>.zip # For Windows
Shell > tar -zxf iotdb-<version>.tar.gz # For Linux or MacOS
```

The IoTDB project will be at the subfolder named iotdb. The folder will include the following contents:

```
iotdb/     <-- root path
|
+- bin/       <-- script files
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
|
+- LICENSE    <-- LICENSE
```

### Installation from source code

Use git to get IoTDB source code:

```
Shell > git clone https://github.com/thulab/iotdb.git
```

If you are not the first time that building IoTDB, remember deleting the following files:

```
rm -rf iotdb/data/
rm -rf iotdb/lib/
```

Then under the IoTDB path, you can build IoTDB using Maven:

```
mvn clean package -Dmaven.test.skip=true
```

If successful, you will see the the following text in the terminal:

```
[INFO] BUILD SUCCESS
```
Otherwise, you may need to check the error statements and fix the problems.

After build, the IoTDB project will be at the subfolder named iotdb. The folder will include the following contents:


```
iotdb/     <-- root path
|
+- bin/       <-- script files
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
|
+- LICENSE    <-- LICENSE
```

### Installation by Docker (Dockerfile)

You can build and run a IoTDB docker image by following the guide of [Deployment by Docker](#build-and-use-iotdb-by-dockerfile)


## Configuration


Before starting to use IoTDB, you need to config the configuration files first. For your convenience, we have already set the default config in the files.

In total, we provide users three kinds of configurations module: 

* environment configuration file (iotdb-env.bat, iotdb-env.sh). The default configuration file for the environment configuration item. Users can configure the relevant system configuration items of JAVA-JVM in the file.
* system configuration file (tsfile-format.properties, iotdb-engine.properties). 
	* tsfile-format.properties: The default configuration file for the IoTDB file layer configuration item. Users can configure the information about the TsFile, such as the data size written to the disk per time(group\_size\_in_byte). 
	* iotdb-engine.properties: The default configuration file for the IoTDB engine layer configuration item. Users can configure the IoTDB engine related parameters in the file, such as JDBC service listening port (rpc\_port), overflow data storage directory (overflow\_data\_dir), etc.
* log configuration file (logback.xml)

The configuration files of the three configuration items are located in the IoTDB installation directory: $IOTDB_HOME/conf folder.

### IoTDB Environment Configuration File

The environment configuration file is mainly used to configure the Java environment related parameters when IoTDB Server is running, such as JVM related configuration. This part of the configuration is passed to the JVM when the IoTDB Server starts. Users can view the contents of the environment configuration file by viewing the iotdb-env.sh (or iotdb-env.bat) file.

The detail of each variables are as follows:

* JMX\_LOCAL

|Name|JMX\_LOCAL|
|:---:|:---|
|Description|JMX monitoring mode, configured as yes to allow only local monitoring, no to allow remote monitoring|
|Type|Enum String: "yes", "no"|
|Default|yes|
|Effective|After restart system|


* JMX\_PORT

|Name|JMX\_PORT|
|:---:|:---|
|Description|JMX listening port. Please confirm that the port is not a system reserved port and is not occupied|
|Type|Short Int: [0,65535]|
|Default|31999|
|Effective|After restart system|

* MAX\_HEAP\_SIZE

|Name|MAX\_HEAP\_SIZE|
|:---:|:---|
|Description|The maximum heap memory size that IoTDB can use at startup.|
|Type|String|
|Default| On Linux or MacOS, the default is one quarter of the memory. On Windows, the default value for 32-bit systems is 512M, and the default for 64-bit systems is 2G.|
|Effective|After restart system|

* HEAP\_NEWSIZE

|Name|HEAP\_NEWSIZE|
|:---:|:---|
|Description|The minimum heap memory size that IoTDB can use at startup.|
|Type|String|
|Default| On Linux or MacOS, the default is min{cores * 100M, one quarter of MAX\_HEAP\_SIZE}. On Windows, the default value for 32-bit systems is 512M, and the default for 64-bit systems is 2G.|
|Effective|After restart system|

### IoTDB System Configuration File

#### File Layer

* compressor

|Name|compressor|
|:---:|:---|
|Description|Data compression method|
|Type|Enum String : “UNCOMPRESSED”, “SNAPPY”|
|Default| UNCOMPRESSED |
|Effective|Immediately|

* group\_size\_in\_byte

|Name|group\_size\_in\_byte|
|:---:|:---|
|Description|The data size written to the disk per time|
|Type|Int32|
|Default| 134217728 |
|Effective|Immediately|

* max\_number\_of\_points\_in\_page

|Name| max\_number\_of\_points\_in\_page |
|:---:|:---|
|Description|The maximum number of data points (timestamps - valued groups) contained in a page|
|Type|Int32|
|Default| 1048576 |
|Effective|Immediately|

* max\_string\_length

|Name| max\_string\_length |
|:---:|:---|
|Description|The maximum length of a single string (number of character)|
|Type|Int32|
|Default| 128 |
|Effective|Immediately|

* page\_size\_in\_byte

|Name| page\_size\_in\_byte |
|:---:|:---|
|Description|The maximum size of a single page written in memory when each column in memory is written (in bytes)|
|Type|Int32|
|Default| 134217728 |
|Effective|Immediately|

* time\_series\_data\_type

|Name| time\_series\_data\_type |
|:---:|:---|
|Description|Timestamp data type|
|Type|Enum String: "INT32", "INT64"|
|Default| Int64 |
|Effective|Immediately|

* time\_series\_encoder

|Name| time\_series\_data\_type |
|:---:|:---|
|Description| TimeSeries encoding type|
|Type|Enum String: “TS_2DIFF”,“PLAIN”,“RLE”|
|Default| TS_2DIFF |
|Effective|Immediately|

* float_precision

|Name| float_precision |
|:---:|:---|
|Description| The precision of the floating point number.(The number of digits after the decimal point) |
|Type|Int32|
|Default| The default is 2 digits. Note: The 32-bit floating point number has a decimal precision of 7 bits, and the 64-bit floating point number has a decimal precision of 15 bits. If the setting is out of the range, it will have no practical significance. |
|Effective|Immediately|

#### Engine Layer

* back\_loop\_period

|Name| back\_loop\_period |
|:---:|:---|
|Description| The frequency at which the system statistic module triggers(in seconds). |
|Type|Int32|
|Default| 10 |
|Effective|After restart system|

* data\_dir

|Name| data\_dir |
|:---:|:---|
|Description| The IoTDB data path.By default, it is stored in the data directory at the same level as the bin directory. It is recommended to use an absolute path. |
|Type|String|
|Default| data |
|Effective|After restart system|

* enable_wal

|Name| enable_wal |
|:---:|:---|
|Description| Whether to enable the pre-write log. The default value is true(enabled), and false means closed. |
|Type|Bool|
|Default| true |
|Effective|After restart system|

* fetch_size

|Name| fetch_size |
|:---:|:---|
|Description| The amount of data read each time in batches(the number of data strips, that is, the number of different time stamps.) |
|Type|Int32|
|Default| 10000 |
|Effective|After restart system|

* flush\_wal\_period\_in\_ms

|Name| flush\_wal\_period\_in\_ms |
|:---:|:---|
|Description| The period during which the log is periodically flushed to disk(in milliseconds) |
|Type|Int32|
|Default| 10 |
|Effective|After restart system|

* flush\_wal\_threshold

|Name| flush\_wal\_threshold |
|:---:|:---|
|Description| After the WAL reaches this value, it is flushed to disk, and it is possible to lose at most flush_wal_threshold operations. |
|Type|Int32|
|Default| 10000 |
|Effective|After restart system|

* max\_opened\_folder

|Name| max\_opened\_folder |
|:---:|:---|
|Description| The maximum number of folders opened at the same time. When the value becomes larger, the memory usage increases, the IO random read and write becomes less, and the file partition (ie, group) is more neat; the smaller the value, the less memory is occupied, the IO random read and write becomes more, and the file block size is insufficient. |
|Type|Int32|
|Default| 100 |
|Effective|After restart system|

* merge\_concurrent\_threads

|Name| merge\_concurrent\_threads |
|:---:|:---|
|Description| THe max threads which can be used when overflow data is merged. The larger it is, the more IO and CPU cost. The smaller the value, the more the disk is occupied when the overflow data is too large, the reading will be slower. |
|Type|Int32|
|Default| 10 |
|Effective|After restart system|

* mult\_dir\_strategy

|Name| mult\_dir\_strategy |
|:---:|:---|
|Description| IoTDB's strategy for selecting directories for TsFile in tsfile_dir. You can use a simple class name or a full name of the class. The system provides the following three strategies: <br>1. SequenceStrategy: IoTDB selects the directory from tsfile\_dir in order, traverses all the directories in tsfile\_dir in turn, and keeps counting;<br>2. MaxDiskUsableSpaceFirstStrategy: IoTDB first selects the directory with the largest free disk space in tsfile\_dir;<br>3. MinFolderOccupiedSpaceFirstStrategy: IoTDB prefers the directory with the least space used in tsfile\_dir;<br>4. <UserDfineStrategyPackage> (user-defined policy)<br>You can complete a user-defined policy in the following ways:<br>1. Inherit the cn.edu.tsinghua.iotdb.conf.directories.strategy.DirectoryStrategy class and implement its own Strategy method;<br>2. Fill in the configuration class with the full class name of the implemented class (package name plus class name, UserDfineStrategyPackage);<br>3. Add the jar file to the project. |
|Type|String|
|Default| MaxDiskUsableSpaceFirstStrategy |
|Effective|After restart system|

* period\_time\_for\_flush\_in\_second

|Name| period\_time\_for\_flush\_in\_second |
|:---:|:---|
|Description| The interval period IoTDB closes files(in seconds). At every set time, the system will automatically flush the data in the memory to the disk and seal all the files that are currently open.|
|Type|Int32|
|Default| 3600 |
|Effective|After restart system|

* period\_time\_for\_merge\_in\_second

|Name| period\_time\_for\_merge\_in\_second |
|:---:|:---|
|Description| IoTDB has two parts of data in memory at runtime: overflow and bufferwrite. The system will automatically merge the two parts of data at regular intervals. This is the merge interval(in seconds).|
|Type|Int32|
|Default| 7200 |
|Effective|After restart system|

* rpc_address

|Name| rpc_address |
|:---:|:---|
|Description| The jdbc service listens on the address.|
|Type|String|
|Default| "0.0.0.0" |
|Effective|After restart system|

* rpc_port

|Name| rpc_port |
|:---:|:---|
|Description| The jdbc service listens on the port. Please confirm that the port is not a system reserved port and is not occupied.|
|Type|Short Int : [0,65535]|
|Default| 6667 |
|Effective|After restart system|

* tsfile_dir

|Name| tsfile_dir |
|:---:|:---|
|Description| The storage path of TsFile. By default, it is stored in three folders under data directory(soldled1, settled2, and settled3). See the [mult\_dir\_strategy](chapter4,multdirstrategy) configuration item for data distribution strategy. The starting directory of the relative path is related to the operating system. It is recommended to use an absolute path. If the path does not exist, the system will automatically create it.|
|Type|String[]|
|Default| settled1, settled2, settled3 |
|Effective|After restart system|

* wal\_cleanup\_threshold

|Name| wal\_cleanup\_threshold |
|:---:|:---|
|Description| When the total number of logs in the file and in memory reaches this value, all logs are compressed and the useless log is removed. The default is 500000. If this value is too large, it will cause a short write pause. If it is too small, it will increase IO and CPU consumption. |
|Type|Int32|
|Default| 500000 |
|Effective|After restart system|

* sys\_dir

|Name| sys\_dir |
|:---:|:---|
|Description| IoTDB metadata storage path.(By default it is in the data directory at the same level as the bin directory. The starting directory of the relative path is related to the operating system. It is recommended to use an absolute path. |
|Type|String|
|Default| system |
|Effective|After restart system|

* time_zone

|Name| time_zone |
|:---:|:---|
|Description| The time zone in which the server is located, the default is Beijing time (+8) |
|Type|Time Zone String|
|Default| +08:00 |
|Effective|After restart system|

* enable\_stat\_monitor

|Name| enable\_stat\_monitor |
|:---:|:---|
|Description| Whether to enable background statistics|
|Type| Boolean |
|Default| true |
|Effective|After restart system|


* mem\_threshold\_warning

|Name| mem\_threshold\_warning |
|:---:|:---|
|Description| A percentage value, which is multiplied by the maximum heap memory assigned by the IoTDB runtime to get a threshold. When the IoTDB uses memory beyond the threshold, it will trigger the operation of writing the current in-memory data to the disk and releasing the corresponding memory. By default, IoTDB runtime can use 80% of the maximum heap memory. If the value is configured to exceed 1, the configuration item will not take effect. If the value is less than or equal to 0, then the default value is used.|
|Type| Float |
|Default| 0.8 |
|Effective|After restart system|


* mem\_threshold\_dangerous

|Name| mem\_threshold\_dangerous |
|:---:|:---|
|Description| A percentage value, which is multiplied by the maximum heap memory allocated by the IoTDB runtime to get a threshold. When the IoTDB uses memory beyond the threshold, it will trigger the operation of writing the current in-memory data to the disk and releasing the corresponding memory. At the same time, the write operation will be blocked. By default, the IoTDB runtime can use 90% of the maximum heap memory. If the value is configured to exceed 1, the configuration item will not take effect. If the value is less than or equal to 0, then the default value is used.|
|Type| Float |
|Default| 0.9 |
|Effective|After restart system|

* mem\_monitor\_interval

|Name| mem\_monitor\_interval |
|:---:|:---|
|Description| The time interval IoTDB system checks the current memory usage. If the threshold calculated according to mem_threshold_warning or mem_threshold_dangerous is exceeded, the corresponding operation will be triggered. The unit is milliseconds and the default is 1000 milliseconds.|
|Type| Int64 |
|Default| 1000 |
|Effective|After restart system|

* bufferwrite\_meta\_size\_threshold

|Name| bufferwrite\_meta\_size\_threshold |
|:---:|:---|
|Description| When the metadata size of the TsFile saved in the memory exceeds the threshold, the metadata is saved at the end of the TsFile, and then the file is closed and the memory space occupied by the metadata is released. The unit is byte and the default value is 200M.|
|Type| Int64 |
|Default| 209715200 |
|Effective|After restart system|

* bufferwrite\_file\_size\_threshold

|Name| bufferwrite\_meta\_size\_threshold |
|:---:|:---|
|Description| When a TsFile size on the disk exceeds this threshold, the TsFile is closed and open a new TsFile to accept data writes. The unit is byte and the default value is 2G.|
|Type| Int64 |
|Default| 2147483648 |
|Effective|After restart system|


* overflow\_meta\_size\_threshold

|Name| overflow\_meta\_size\_threshold |
|:---:|:---|
|Description| When the size of the Overflow metadata stored in the memory exceeds the threshold, the metadata is saved at the end of the TsFile, then the file is closed and the memory space occupied by the metadata is released. The unit is byte and the default value is 200M.|
|Type| Int64 |
|Default| 209715200 |
|Effective|After restart system|

* overflow\_file\_size\_threshold

|Name| overflow\_file\_size\_threshold |
|:---:|:---|
|Description| When an Overflow file size on the disk exceeds this threshold, the Overflow file is closed. And open a new Overflow file to accept data writes. The unit is byte, the default value is 2G|
|Type| Int64 |
|Default| 2147483648 |
|Effective|After restart system|

* concurrent\_flush\_thread

|Name| concurrent\_flush\_thread |
|:---:|:---|
|Description| The thread number used to perform the operation when IoTDB writes data in memory to disk. If the value is less than or equal to 0, then the number of CPU cores installed on the machine is used. The default is 0.|
|Type| Int32 |
|Default| 0 |
|Effective|After restart system|


* stat\_monitor\_detect\_freq\_sec

|Name| concurrent\_flush\_thread |
|:---:|:---|
|Description| The time interval which the system check whether the current record statistic time range exceeds stat_monitor_retain_interval every time (in seconds) and perform regular cleaning|
|Type| Int32 |
|Default|600 |
|Effective|After restart system|


* stat\_monitor\_retain\_interval\_sec

|Name| stat\_monitor\_retain\_interval\_sec |
|:---:|:---|
|Description| The retention time of system statistics data(in seconds). Statistics data over the retention time range will be cleaned regularly.|
|Type| Int32 |
|Default|600 |
|Effective|After restart system|

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

* OverflowOpenFileNum

|Name| OverflowOpenFileNum |
|:---:|:---|
|Description| The opened out-of-order data file number of IoTDB server process. |
|Default Directory| /data/data/overflow |
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

This module is the statistical monitoring method provided by IoTDB for users to store data information. We will record the statistical data in the system and store it in the database. The current 0.7.0 version of IoTDB provides statistics for writing data.

The user can choose to enable or disable the data statistics monitoring function (set the `enable_stat_monitor` item in the configuration file, see [Engine Layer](chapter4,enginelayer) for details).

#### Writing Data Monitor

The current statistics of writing data by the system can be divided into two major modules: **Global Writing Data Statistics** and **Storage Group Writing Data Statistics**. **Global Writing Data Statistics** records the point number written by the user and the number of requests. **Storage Group Writing Data Statistics** records data of a certain storage group. 

The system defaults to collect data every 5 seconds, and writes the statistics to the IoTDB and stores them in a system-specified locate. (If you need to change the statistic frequency, you can set The `back_loop_period_sec entry` in the configuration file, see Section [Engine Layer](chapter4,enginelayer) for details). After the system is refreshed or restarted, IoTDB does not recover the statistics, and the statistics data will restart from zero.

In order to avoid the excessive use of statistical information, we add a mechanism to periodically clear invalid data for statistical information. The system will delete invalid data at regular intervals. The user can set the trigger frequency (`stat_monitor_retain_interval_sec`, default is 600s, see section [Engine Layer](chapter4,enginelayer) for details) to set the frequency of deleting data. By setting the valid data duration (`stat_monitor_detect_freq_sec entry`, the default is 600s, see section [Engine Layer](chapter4,enginelayer) for details) to set the time period of valid data, that is, the data within the time of the clear operation trigger time is stat_monitor_detect_freq_sec is valid data. In order to ensure the stability of the system, it is not allowed to delete the statistics frequently. Therefore, if the configuration parameter time is less than the default value (600s), the system will abort the configuration parameter and uses the default parameter.

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

## System log

IoTDB allows users to configure IoTDB system logs (such as log output level) by modifying the log configuration file. The default location of the system log configuration file is in \$IOTDB_HOME/conf folder. 

The default log configuration file is named logback.xml. The user can modify the configuration of the system running log by adding or changing the xml tree node parameters. It should be noted that the configuration of the system log using the log configuration file does not take effect immediately after the modification, instead, it will take effect after restarting the system. The usage of logback.xml is just as usual.

At the same time, in order to facilitate the debugging of the system by the developers and DBAs, we provide several JMX interface to dynamically modify the log configuration, and configure the Log module of the system in real time without restarting the system. For detailed usage, see [Dynamic System Log Configuration](Chap4dynamicsystemlog) section.

### Dynamic System Log Configuration

#### Connect JMX

Here we use JConsole to connect with JMX. 

Start the JConsole, establish a new JMX connection with the IoTDB Server (you can select the local process or input the IP and PORT for remote connection, the default operation port of the IoTDB JMX service is 31999). Fig 4.1 shows the connection GUI of JConsole.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577195-f94d7500-1ef3-11e9-999a-b4f67055d80e.png">

After connected, click `MBean` and find `ch.qos.logback.classic.default.ch.qos.logback.classic.jmx.JMXConfigurator`(As shown in fig 4.2).
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577204-fe122900-1ef3-11e9-9e89-2eb1d46e24b8.png">

In the JMXConfigurator Window, there are 6 operation provided for you, as shown in fig 4.3. You can use there interface to perform operation.

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
It should be noted that the difference between this method and the `getLoggerEffectiveLevel` method is that the method returns the log level that the specified Logger is set in the configuration file. If the user does not set the log level for the Logger. , then return empty. According to Logre's log-level inheritance mechanism, if a Logger is not displayed to set the log level, it will inherit the log level settings from its nearest ancestor. At this point, calling the `getLoggerEffectiveLevel` method will return the log level in which the Logger is in effect; calling the methods described in this section will return null.

## Data Management

In IoTDB, there are many kinds of data needed to be storage. In this section, we will introduce IoTDB's data storage strategy in order to give you an intuitive understanding of IoTDB's data management.

The data that IoTDB stores is divided into three categories, namely data files, system files, and pre-write log files.

### Data Files

Data files store all the data that the user wrote to IoTDB, which contains TsFile and other files. TsFile storage directory can be configured with the `tsfile_dir` configuration item (see [file layer](Chap4filelayer) for details). Other files can be configured through [data_dir](chap4datadir) configuration item (see [Engine Layer](chapter4,enginelayer) for details).

In order to better support users' storage requirements such as disk space expansion, IoTDB supports multiple file directorys storage methods for TsFile storage configuration. Users can set multiple storage paths as data storage locations( see [tsfile_dir](Chap4,tsfiledir) configuration item), and you can specify or customize the directory selection policy (see [mult_dir_strategy](chapter4,enginelayer) configuration item for details).

### System Files

System files include restore files and schema files, which store metadata information of data in IoTDB. It can be configured through the `sys_dir` configuration item (see [System Layer](chapter4,systemlayer) for details).

### Pre-write Log Files

Pre-write log files store WAL files. It can be configured through the `wal_dir` configuration item (see [System Layer](chapter4,systemlayer) for details).

### Example of Setting Data storage Directory

For a clearer understanding of configuring the data storage directory, we will give an excample in this section.

All data directory paths involved in storage directory setting are: data_dir, tsfile_dir, mult_dir_strategy, sys_dir, and wal_dir, which refer to data files, stroage strategy, system files, and pre-write log files. You can choose to configure the items you'd like to change, otherwise, you can use the system default configuration item without any operation.

Here we give an example of a user who configures all five configurations mentioned above. The configuration items are as follow:

```
data_dir = D:\\iotdb\\data\\data  
tsfile_dir = E:\\iotdb\\data\\data1, data\\data2, F:\\data3  mult_dir_strategy = MaxDiskUsableSpaceFirstStrategy sys_dir = data\\system wal_dir = data

```
After setting the configuration, the system will:

* Save all data files except TsFile in D:\\iotdb\\data\\data
* Save TsFile in E:\\iotdb\\data\\data1, $IOTDB_HOME\\data\\data2 and F:\\data3. And the choosing strategy is `MaxDiskUsableSpaceFirstStrategy`, that is every time data writes to the disk, the system will automatically select a directory with the largest remaining disk space to write data.
* Save system data in $IOTDB_HOME\\data\\system
* Save WAL data in $IOTDB_HOME\\data

> Note:
> 
> If you change directory names in tsfile_dir, the newer name and the older name should be one-to-one correspondence. Also, the files in the older directory needs to be moved to the newer directory. 
> 
> If you add some directorys in tsfile_dir, IoTDB will add the path automatically. Nothing needs to do by your own. 

For example, modify the tsfile_dir to:

```
tsfile_dir = D:\\data4, E:\\data5, F:\\data6
```

You need to move files in E:\iotdb\data\data1 to D:\data4, move files in %IOTDB_HOME%\data\data2 to E:\data5, move files in F:\data3 to F:\data6. In this way, the system will operation normally.




## Build and use IoTDB by Dockerfile
Now a Dockerfile has been written at ROOT/docker/Dockerfile on the branch enable_docker_image.

1. You can build a docker image by: 
```
$ docker build -t iotdb:base git://github.com/apache/incubator-iotdb#master:docker
```
Or:
```
$ git clone https://github.com/apache/incubator-iotdb
$ cd incubator-iotdb
$ cd docker
$ docker build -t iotdb:base .
```
Once the docker image has been built locally (the tag is iotdb:base in this example), you are almost done!

2. create docker volume for data files and logs:
```
$ docker volume create mydata
$ docker volume create mylogs
```
3. run a docker container:
```shell
$ docker run -p 6667:6667 -v mydata:/iotdb/data -v mylogs:/iotdb/logs -d iotdb:base /iotdb/bin/start-server.sh
```
If success, you can run `docker ps`, and get something like the following:
```
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                               NAMES
2a68b6944cb5        iotdb:base          "/iotdb/bin/start-se…"   4 minutes ago       Up 5 minutes        0.0.0.0:6667->6667/tcp              laughing_meitner
```
You can use the above command to get the container ID: 
```
$ docker container ls
```
suppose the ID is <C_ID>.

And get the docker IP by:
```
$ docker inspect --format='{{.NetworkSettings.IPAddress}}' <C_ID>
```
suppose the IP is <C_IP>.

4. If you just want to have a try by using iotdb-cli, you can:
```
$ docker exec -it /bin/bash  <C_ID>
$ (now you have enter the container): /cli/bin/start-client.sh -h localhost -p 6667 -u root -pw root
```

Or,  run a new docker container as the client:
```
$ docker run -it iotdb:base /cli/bin/start-client.sh -h <C_IP> -p 6667 -u root -pw root
```
Or,  if you have a iotdb-cli locally (e.g., you have compiled the source code by `mvn package`), and suppose your work_dir is cli/bin, then you can just run:
```
$ start-client.sh -h localhost -p 6667 -u root -pw root
```
5. If you want to write codes to insert data and query data, please add the following dependence:
```xml
        <dependency>
            <groupId>org.apache.iotdb</groupId>
            <artifactId>iotdb-jdbc</artifactId>
            <version>0.8.0-SNAPSHOT</version>
        </dependency>
```
Some example about how to use IoTDB with IoTDB-JDBC can be found at: https://github.com/apache/incubator-iotdb/tree/master/jdbc/src/test/java/org/apache/iotdb/jdbc/demo

(Notice that because we have not published Apache IoTDB version 0.8.0 now, you have to compile the source code by `mvn install -DskipTests` to install the dependence into your local maven repository)

6. Now enjoy it!
