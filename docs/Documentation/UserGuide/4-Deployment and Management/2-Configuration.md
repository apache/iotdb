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

## Configuration


Before starting to use IoTDB, you need to config the configuration files first. For your convenience, we have already set the default config in the files.

In total, we provide users three kinds of configurations module: 

* environment configuration file (`iotdb-env.bat`, `iotdb-env.sh`). The default configuration file for the environment configuration item. Users can configure the relevant system configuration items of JAVA-JVM in the file.
* system configuration file (`tsfile-format.properties`, `iotdb-engine.properties`). 
	* `tsfile-format.properties`: The default configuration file for the IoTDB file layer configuration item. Users can configure the information about the TsFile, such as the data size written to the disk per time(`group_size_in_byte`). 
	* `iotdb-engine.properties`: The default configuration file for the IoTDB engine layer configuration item. Users can configure the IoTDB engine related parameters in the file, such as JDBC service listening port (`rpc_port`), unsequence data storage directory (`unsequence_data_dir`), etc.
* log configuration file (`logback.xml`)

The configuration files of the three configuration items are located in the IoTDB installation directory: `$IOTDB_HOME/conf` folder.

### IoTDB Environment Configuration File

The environment configuration file is mainly used to configure the Java environment related parameters when IoTDB Server is running, such as JVM related configuration. This part of the configuration is passed to the JVM when the IoTDB Server starts. Users can view the contents of the environment configuration file by viewing the `iotdb-env.sh` (or `iotdb-env.bat`) file.

The detail of each variables are as follows:

* LOCAL\_JMX

|Name|LOCAL\_JMX|
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

* page\_size\_in\_byte

|Name| page\_size\_in\_byte |
|:---:|:---|
|Description|The maximum size of a single page written in memory when each column in memory is written (in bytes)|
|Type|Int32|
|Default| 65536 |
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

* time\_series\_data\_type

|Name| time\_series\_data\_type |
|:---:|:---|
|Description|Timestamp data type|
|Type|Enum String: "INT32", "INT64"|
|Default| Int64 |
|Effective|Immediately|

* time\_encoder

|Name| time\_encoder |
|:---:|:---|
|Description| Encoding type of time column|
|Type|Enum String: “TS_2DIFF”,“PLAIN”,“RLE”|
|Default| TS_2DIFF |
|Effective|Immediately|

* value\_encoder

|Name| value\_encoder |
|:---:|:---|
|Description| Encoding type of value column|
|Type|Enum String: “TS_2DIFF”,“PLAIN”,“RLE”|
|Default| PLAIN |
|Effective|Immediately|

* float_precision

|Name| float_precision |
|:---:|:---|
|Description| The precision of the floating point number.(The number of digits after the decimal point) |
|Type|Int32|
|Default| The default is 2 digits. Note: The 32-bit floating point number has a decimal precision of 7 bits, and the 64-bit floating point number has a decimal precision of 15 bits. If the setting is out of the range, it will have no practical significance. |
|Effective|Immediately|

#### Engine Layer

* rpc\_address

|Name| rpc\_address |
|:---:|:---|
|Description| The jdbc service listens on the address.|
|Type|String|
|Default| "0.0.0.0" |
|Effective|After restart system|

* rpc\_port

|Name| rpc\_port |
|:---:|:---|
|Description| The jdbc service listens on the port. Please confirm that the port is not a system reserved port and is not occupied.|
|Type|Short Int : [0,65535]|
|Default| 6667 |
|Effective|After restart system|

* time\_zone

|Name| time\_zone |
|:---:|:---|
|Description| The time zone in which the server is located, the default is Beijing time (+8) |
|Type|Time Zone String|
|Default| +08:00 |
|Effective|After restart system|

* base\_dir

|Name| base\_dir |
|:---:|:---|
|Description| The IoTDB system folder. It is recommended to use an absolute path. |
|Type|String|
|Default| data |
|Effective|After restart system|

* data\_dirs

|Name| data\_dirs |
|:---:|:---|
|Description| The directories of data files. Multiple directories are separated by comma. See the [multi\_dir\_strategy](/#/Documents/0.8.1/chap4/sec2) configuration item for data distribution strategy. The starting directory of the relative path is related to the operating system. It is recommended to use an absolute path. If the path does not exist, the system will automatically create it.|
|Type|String[]|
|Default| data/data |
|Effective|After restart system|

* wal\_dir

|Name| wal\_dir |
|:---:|:---|
|Description| Write Ahead Log storage path. It is recommended to use an absolute path. |
|Type|String|
|Default| data/wal |
|Effective|After restart system|

* enable\_wal

|Name| enable\_wal |
|:---:|:---|
|Description| Whether to enable the pre-write log. The default value is true(enabled), and false means closed. |
|Type|Bool|
|Default| true |
|Effective|After restart system|

* multi\_dir\_strategy

|Name| multi\_dir\_strategy |
|:---:|:---|
|Description| IoTDB's strategy for selecting directories for TsFile in tsfile_dir. You can use a simple class name or a full name of the class. The system provides the following three strategies: <br>1. SequenceStrategy: IoTDB selects the directory from tsfile\_dir in order, traverses all the directories in tsfile\_dir in turn, and keeps counting;<br>2. MaxDiskUsableSpaceFirstStrategy: IoTDB first selects the directory with the largest free disk space in tsfile\_dir;<br>3. MinFolderOccupiedSpaceFirstStrategy: IoTDB prefers the directory with the least space used in tsfile\_dir;<br>4. <UserDfineStrategyPackage> (user-defined policy)<br>You can complete a user-defined policy in the following ways:<br>1. Inherit the cn.edu.tsinghua.iotdb.conf.directories.strategy.DirectoryStrategy class and implement its own Strategy method;<br>2. Fill in the configuration class with the full class name of the implemented class (package name plus class name, UserDfineStrategyPackage);<br>3. Add the jar file to the project. |
|Type|String|
|Default| MaxDiskUsableSpaceFirstStrategy |
|Effective|After restart system|

* tsfile\_size\_threshold

|Name| tsfile\_size\_threshold |
|:---:|:---|
|Description| When a TsFile size on the disk exceeds this threshold, the TsFile is closed and open a new TsFile to accept data writes. The unit is byte and the default value is 2G.|
|Type| Int64 |
|Default| 536870912 |
|Effective|After restart system|

* flush\_wal\_threshold

|Name| flush\_wal\_threshold |
|:---:|:---|
|Description| After the WAL reaches this value, it is flushed to disk, and it is possible to lose at most flush_wal_threshold operations. |
|Type|Int32|
|Default| 10000 |
|Effective|After restart system|

* force\_wal\_period\_in\_ms

|Name| force\_wal\_period\_in\_ms |
|:---:|:---|
|Description| The period during which the log is periodically forced to flush to disk(in milliseconds) |
|Type|Int32|
|Default| 10 |
|Effective|After restart system|

* fetch\_size

|Name| fetch\_size |
|:---:|:---|
|Description| The amount of data read each time in batch (the number of data strips, that is, the number of different timestamps.) |
|Type|Int32|
|Default| 10000 |
|Effective|After restart system|

* merge\_concurrent\_threads

|Name| merge\_concurrent\_threads |
|:---:|:---|
|Description| THe max threads which can be used when unsequence data is merged. The larger it is, the more IO and CPU cost. The smaller the value, the more the disk is occupied when the unsequence data is too large, the reading will be slower. |
|Type|Int32|
|Default| 0 |
|Effective|After restart system|

* enable\_stat\_monitor

|Name| enable\_stat\_monitor |
|:---:|:---|
|Description| Whether to enable background statistics|
|Type| Boolean |
|Default| false |
|Effective|After restart system|

* back\_loop\_period_in_second

|Name| back\_loop\_period\_in\_second |
|:---:|:---|
|Description| The frequency at which the system statistic module triggers(in seconds). |
|Type|Int32|
|Default| 5 |
|Effective|After restart system|

* concurrent\_flush\_thread

|Name| concurrent\_flush\_thread |
|:---:|:---|
|Description| The thread number used to perform the operation when IoTDB writes data in memory to disk. If the value is less than or equal to 0, then the number of CPU cores installed on the machine is used. The default is 0.|
|Type| Int32 |
|Default| 0 |
|Effective|After restart system|

* stat\_monitor\_detect\_freq\_in\_second

|Name| stat\_monitor\_detect\_freq\_in\_second |
|:---:|:---|
|Description| The time interval which the system check whether the current record statistic time range exceeds stat_monitor_retain_interval every time (in seconds) and perform regular cleaning|
|Type| Int32 |
|Default|600 |
|Effective|After restart system|

* stat\_monitor\_retain\_interval\_in\_second

|Name| stat\_monitor\_retain\_interval\_in\_second |
|:---:|:---|
|Description| The retention time of system statistics data(in seconds). Statistics data over the retention time range will be cleaned regularly.|
|Type| Int32 |
|Default|600 |
|Effective|After restart system|
