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

# Appendix 1: Configuration Parameters


Before starting to use IoTDB, you need to config the configuration files first. For your convenience, we have already set the default config in the files.

In total, we provide users three kinds of configurations module: 

* environment configuration file (`iotdb-env.bat`, `iotdb-env.sh`). The default configuration file for the environment configuration item. Users can configure the relevant system configuration items of JAVA-JVM in the file.
* system configuration file (`iotdb-engine.properties`). 
	* `iotdb-engine.properties`: The default configuration file for the IoTDB engine layer configuration item. Users can configure the IoTDB engine related parameters in the file, such as JDBC service listening port (`rpc_port`), unsequence data storage directory (`unsequence_data_dir`), etc. What's more, Users can configure the information about the TsFile, such as the data size written to the disk per time(`group_size_in_byte`). 
  
* log configuration file (`logback.xml`)

The configuration files of the three configuration items are located in the IoTDB installation directory: `$IOTDB_HOME/conf` folder.

## Hot Modification Configuration

For the convenience of users, IoTDB server provides users with hot modification function, that is, modifying some configuration parameters in `iotdb engine. Properties` during the system operation and applying them to the system immediately. 
In the parameters described below, these parameters whose way of `Effective` is `trigger` support hot modification.

Trigger way: The client sends the command `load configuration` to the IoTDB server. See Chapter 4 for the usage of the client.

## IoTDB Environment Configuration File

The environment configuration file is mainly used to configure the Java environment related parameters when IoTDB Server is running, such as JVM related configuration. This part of the configuration is passed to the JVM when the IoTDB Server starts. Users can view the contents of the environment configuration file by viewing the `iotdb-env.sh` (or `iotdb-env.bat`) file.

The detail of each variables are as follows:

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

* JMX\_LOCAL

|Name|JMX\_LOCAL|
|:---:|:---|
|Description|JMX monitoring mode, configured as yes to allow only local monitoring, no to allow remote monitoring|
|Type|Enum String: "true", "false"|
|Default|true|
|Effective|After restart system|


* JMX\_PORT

|Name|JMX\_PORT|
|:---:|:---|
|Description|JMX listening port. Please confirm that the port is not a system reserved port and is not occupied|
|Type|Short Int: [0,65535]|
|Default|31999|
|Effective|After restart system|

* JMX\_IP

|Name|JMX\_IP|
|:---:|:---|
|Description|JMX listening address. Only take effect if JMX\_LOCAL=false. 0.0.0.0 is never allowed|
|Type|String|
|Default|127.0.0.1|
|Effective|After restart system|

## JMX Authorization

We **STRONGLY RECOMMENDED** you CHANGE the PASSWORD for the JMX remote connection.

The user and passwords are in ${IOTDB\_CONF}/conf/jmx.password.

The permission definitions are in ${IOTDB\_CONF}/conf/jmx.access.

## IoTDB System Configuration File

### File Layer

* compressor

|Name|compressor|
|:---:|:---|
|Description|Data compression method|
|Type|Enum String : “UNCOMPRESSED”, “SNAPPY”|
|Default| UNCOMPRESSED |
|Effective|Trigger|

* group\_size\_in\_byte

|Name|group\_size\_in\_byte|
|:---:|:---|
|Description|The data size written to the disk per time|
|Type|Int32|
|Default| 134217728 |
|Effective|Trigger|

* page\_size\_in\_byte

|Name| page\_size\_in\_byte |
|:---:|:---|
|Description|The maximum size of a single page written in memory when each column in memory is written (in bytes)|
|Type|Int32|
|Default| 65536 |
|Effective|Trigger|

* max\_number\_of\_points\_in\_page

|Name| max\_number\_of\_points\_in\_page |
|:---:|:---|
|Description|The maximum number of data points (timestamps - valued groups) contained in a page|
|Type|Int32|
|Default| 1048576 |
|Effective|Trigger|

* max\_degree\_of\_index\_node

|Name| max\_degree\_of\_index\_node |
|:---:|:---|
|Description|The maximum degree of the metadata index tree (that is, the max number of each node's children)|
|Type|Int32|
|Default| 256 |
|Effective|Only allowed to be modified in first start up|

* max\_string\_length

|Name| max\_string\_length |
|:---:|:---|
|Description|The maximum length of a single string (number of character)|
|Type|Int32|
|Default| 128 |
|Effective|Trigger|

* time\_series\_data\_type

|Name| time\_series\_data\_type |
|:---:|:---|
|Description|Timestamp data type|
|Type|Enum String: "INT32", "INT64"|
|Default| Int64 |
|Effective|Trigger|

* time\_encoder

|Name| time\_encoder |
|:---:|:---|
|Description| Encoding type of time column|
|Type|Enum String: “TS_2DIFF”,“PLAIN”,“RLE”|
|Default| TS_2DIFF |
|Effective|Trigger|

* value\_encoder

|Name| value\_encoder |
|:---:|:---|
|Description| Encoding type of value column|
|Type|Enum String: “TS_2DIFF”,“PLAIN”,“RLE”|
|Default| PLAIN |
|Effective|Trigger|

* float_precision

|Name| float_precision |
|:---:|:---|
|Description| The precision of the floating point number.(The number of digits after the decimal point) |
|Type|Int32|
|Default| The default is 2 digits. Note: The 32-bit floating point number has a decimal precision of 7 bits, and the 64-bit floating point number has a decimal precision of 15 bits. If the setting is out of the range, it will have no practical significance. |
|Effective|Trigger|


* bloomFilterErrorRate

|Name| bloomFilterErrorRate |
|:---:|:---|
|Description| The false positive rate of bloom filter in each TsFile. Bloom filter checks whether a given time series is in the tsfile before loading metadata. This can improve the performance of loading metadata and skip the tsfile that doesn't contain specified time series. If you want to learn more about its mechanism, you can refer to: [wiki page of bloom filter](https://en.wikipedia.org/wiki/Bloom_filter).|
|Type|float, (0, 1)|
|Default| 0.05 |
|Effective|After restart system|



### Engine Layer

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

* rpc\_thrift\_compression\_enable

|Name| rpc\_thrift\_compression\_enable |
|:---:|:---|
|Description| Whether enable thrift's compression (using GZIP).|
|Type|Boolean|
|Default| false |
|Effective|After restart system|

* rpc\_advanced\_compression\_enable

|Name| rpc\_advanced\_compression\_enable |
|:---:|:---|
|Description| Whether enable thrift's advanced compression.|
|Type|Boolean|
|Default| false |
|Effective|After restart system|


* time\_zone

|Name| time\_zone |
|:---:|:---|
|Description| The time zone in which the server is located, the default is Beijing time (+8) |
|Type|Time Zone String|
|Default| +08:00 |
|Effective|Trigger|

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
|Description| The directories of data files. Multiple directories are separated by comma. The starting directory of the relative path is related to the operating system. It is recommended to use an absolute path. If the path does not exist, the system will automatically create it.|
|Type|String[]|
|Default| data/data |
|Effective|Trigger|

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
|Effective|Trigger|

* enable\_mem\_control

|Name| enable\_mem\_control |
|:---:|:---|
|Description| enable memory control to avoid OOM|
|Type|Bool|
|Default| true |
|Effective|After restart system|

* memtable\_size\_threshold

|Name| memtable\_size\_threshold |
|:---:|:---|
|Description| max memtable size|
|Type|Long|
|Default| 1073741824 |
|Effective| when enable\_mem\_control is false & After restart system|

* avg\_series\_point\_number\_threshold

|Name| avg\_series\_point\_number\_threshold |
|:---:|:---|
|Description| max average number of point of each series in memtable|
|Type|Int32|
|Default| 10000 |
|Effective|After restart system|

* tsfile\_size\_threshold

|Name| tsfile\_size\_threshold |
|:---:|:---|
|Description| max tsfile size|
|Type|Long|
|Default| 536870912 |
|Effective| After restart system|

* enable\_partition

|Name| enable\_partition |
|:---:|:---|
|Description| Whether enable time partition for data, if disabled, all data belongs to partition 0 |
|Type|Bool|
|Default| false |
|Effective|Only allowed to be modified in first start up|

* partition\_interval

|Name| partition\_interval |
|:---:|:---|
|Description| Time range for dividing storage group, time series data will be divided into groups by this time range |
|Type|Int64|
|Default| 604800 |
|Effective|Only allowed to be modified in first start up|


* concurrent\_writing\_time\_partition

|Name| concurrent\_writing\_time\_partition |
|:---:|:---|
|Description| This config decides how many time partitions in a storage group can be inserted concurrently </br> For example, your partitionInterval is 86400 and you want to insert data in 5 different days, |
|Type|Int32|
|Default| 1 |
|Effective|After restart system|

* multi\_dir\_strategy

|Name| multi\_dir\_strategy |
|:---:|:---|
|Description| IoTDB's strategy for selecting directories for TsFile in tsfile_dir. You can use a simple class name or a full name of the class. The system provides the following three strategies: <br>1. SequenceStrategy: IoTDB selects the directory from tsfile\_dir in order, traverses all the directories in tsfile\_dir in turn, and keeps counting;<br>2. MaxDiskUsableSpaceFirstStrategy: IoTDB first selects the directory with the largest free disk space in tsfile\_dir;<br>3. MinFolderOccupiedSpaceFirstStrategy: IoTDB prefers the directory with the least space used in tsfile\_dir;<br>4. UserDfineStrategyPackage (user-defined policy)<br>You can complete a user-defined policy in the following ways:<br>1. Inherit the cn.edu.tsinghua.iotdb.conf.directories.strategy.DirectoryStrategy class and implement its own Strategy method;<br>2. Fill in the configuration class with the full class name of the implemented class (package name plus class name, UserDfineStrategyPackage);<br>3. Add the jar file to the project. |
|Type|String|
|Default| MaxDiskUsableSpaceFirstStrategy |
|Effective|Trigger|

* tsfile\_size\_threshold

|Name| tsfile\_size\_threshold |
|:---:|:---|
|Description| When a TsFile size on the disk exceeds this threshold, the TsFile is closed and open a new TsFile to accept data writes. The unit is byte and the default value is 2G.|
|Type| Int64 |
|Default| 536870912 |
|Effective|After restart system|

* tag\_attribute\_total\_size

|Name| tag\_attribute\_total\_size |
|:---:|:---|
|Description| The maximum persistence size of tags and attributes of each time series.|
|Type| Int32 |
|Default| 700 |
|Effective|Only allowed to be modified in first start up|

* enable\_partial\_insert

|Name| enable\_partial\_insert |
|:---:|:---|
|Description| Whether continue to write other measurements if some measurements are failed in one insertion.|
|Type| Bool |
|Default| true |
|Effective|After restart system|

* mtree\_snapshot\_interval

|Name| mtree\_snapshot\_interval |
|:---:|:---|
|Description| The least interval line numbers of mlog.txt when creating a checkpoint and saving snapshot of MTree. Unit: line numbers|
|Type| Int32 |
|Default| 100000 |
|Effective|After restart system|

* flush\_wal\_threshold

|Name| flush\_wal\_threshold |
|:---:|:---|
|Description| After the WAL reaches this value, it is flushed to disk, and it is possible to lose at most flush_wal_threshold operations. |
|Type|Int32|
|Default| 10000 |
|Effective|Trigger|

* force\_wal\_period\_in\_ms

|Name| force\_wal\_period\_in\_ms |
|:---:|:---|
|Description| The period during which the log is periodically forced to flush to disk(in milliseconds) |
|Type|Int32|
|Default| 10 |
|Effective|Trigger|

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

* concurrent\_flush\_thread

|Name| concurrent\_flush\_thread |
|:---:|:---|
|Description| The thread number used to perform the operation when IoTDB writes data in memory to disk. If the value is less than or equal to 0, then the number of CPU cores installed on the machine is used. The default is 0.|
|Type| Int32 |
|Default| 0 |
|Effective|After restart system|

* tsfile\_storage\_fs

|Name| tsfile\_storage\_fs |
|:---:|:---|
|Description| The storage file system of Tsfile and related data files. Currently LOCAL file system and HDFS are supported.|
|Type| String |
|Default|LOCAL |
|Effective|Only allowed to be modified in first start up|

* core\_site\_path

|Name| core\_site\_path |
|:---:|:---|
|Description| Absolute file path of core-site.xml if Tsfile and related data files are stored in HDFS.|
|Type| String |
|Default|/etc/hadoop/conf/core-site.xml |
|Effective|After restart system|

* hdfs\_site\_path

|Name| hdfs\_site\_path |
|:---:|:---|
|Description| Absolute file path of hdfs-site.xml if Tsfile and related data files are stored in HDFS.|
|Type| String |
|Default|/etc/hadoop/conf/hdfs-site.xml |
|Effective|After restart system|

* hdfs\_ip

|Name| hdfs\_ip |
|:---:|:---|
|Description| IP of HDFS if Tsfile and related data files are stored in HDFS. **If there are more than one hdfs\_ip in configuration, Hadoop HA is used.**|
|Type| String |
|Default|localhost |
|Effective|After restart system|

* hdfs\_port

|Name| hdfs\_port |
|:---:|:---|
|Description| Port of HDFS if Tsfile and related data files are stored in HDFS|
|Type| String |
|Default|9000 |
|Effective|After restart system|

* dfs\_nameservices

|Name| hdfs\_nameservices |
|:---:|:---|
|Description| Nameservices of HDFS HA if using Hadoop HA|
|Type| String |
|Default|hdfsnamespace |
|Effective|After restart system|

* dfs\_ha\_namenodes

|Name| hdfs\_ha\_namenodes |
|:---:|:---|
|Description| Namenodes under DFS nameservices of HDFS HA if using Hadoop HA|
|Type| String |
|Default|nn1,nn2 |
|Effective|After restart system|

* dfs\_ha\_automatic\_failover\_enabled

|Name| dfs\_ha\_automatic\_failover\_enabled |
|:---:|:---|
|Description| Whether using automatic failover if using Hadoop HA|
|Type| Boolean |
|Default|true |
|Effective|After restart system|

* dfs\_client\_failover\_proxy\_provider

|Name| dfs\_client\_failover\_proxy\_provider |
|:---:|:---|
|Description| Proxy provider if using Hadoop HA and enabling automatic failover|
|Type| String |
|Default|org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider |
|Effective|After restart system|


* hdfs\_use\_kerberos

|Name| hdfs\_use\_kerberos |
|:---:|:---|
|Description| Whether use kerberos to authenticate hdfs|
|Type| String |
|Default|false |
|Effective|After restart system|

* kerberos\_keytab\_file_path

|Name| kerberos\_keytab\_file_path |
|:---:|:---|
|Description| Full path of kerberos keytab file|
|Type| String |
|Default|/path |
|Effective|After restart system|

* kerberos\_principal

|Name| kerberos\_principal |
|:---:|:---|
|Description| Kerberos pricipal|
|Type| String |
|Default|your principal |
|Effective|After restart system|


* authorizer\_provider\_class

|Name| authorizer\_provider\_class |
|:---:|:---|
|Description| the class name of the authorization service|
|Type| String |
|Default|org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer |
|Effective|After restart system|
|Other available values| org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer |

* openID\_url

|Name| openID\_url |
|:---:|:---|
|Description| the openID server if OpenIdAuthorizer is enabled|
|Type| String (a http url) |
|Default|no |
|Effective|After restart system|

* thrift\_max\_frame\_size

|Name| thrift\_max\_frame\_size |
|:---:|:---|
|Description| the max bytes in a RPC request/response|
|Type| long |
|Default| 67108864 (should >= 8 * 1024 * 1024) |
|Effective|After restart system|


## Automatic Schema Creation and Type Inference

* enable\_auto\_create\_schema

|Name| enable\_auto\_create\_schema |
|:---:|:---|
|Description| whether auto create the time series when a non-existed time series data comes|
|Type| true or false |
|Default|true |
|Effective|After restart system|

* default\_storage\_group\_level

|Name| default\_storage\_group\_level |
|:---:|:---|
|Description| Storage group level when creating schema automatically is enabled. For example, if we receives a data point from root.sg0.d1.s2, we will set root.sg0 as the storage group if storage group level is 1. (root is level 0)|
|Type| integer |
|Default|1 |
|Effective|After restart system|

* boolean\_string\_infer\_type

|Name| boolean\_string\_infer\_type |
|:---:|:---|
|Description| To which type the values "true" and "false" should be reslved|
|Type| BOOLEAN or TEXT |
|Default|BOOLEAN |
|Effective|After restart system|

* integer\_string\_infer\_type

|Name| integer\_string\_infer\_type |
|:---:|:---|
|Description| To which type an integer string like "67" in a query should be resolved|
|Type| INT32, INT64, DOUBLE, FLOAT or TEXT |
|Default|DOUBLE |
|Effective|After restart system|

* nan\_string\_infer\_type

|Name| nan\_string\_infer\_type |
|:---:|:---|
|Description| To which type the value NaN in a query should be resolved|
|Type| DOUBLE, FLOAT or TEXT |
|Default|FLOAT |
|Effective|After restart system|

* floating\_string\_infer\_type

|Name| floating\_string\_infer\_type |
|:---:|:---|
|Description| To which type a floating number string like "6.7" in a query should be resolved|
|Type| DOUBLE, FLOAT or TEXT |
|Default|FLOAT |
|Effective|After restart system|

* enable\_partition

|Name| enable\_partition |
|:---:|:---|
|Description| whether enable data partition. If disabled, all data belongs to partition 0|
|Type| BOOLEAN |
|Default|false |
|Effective|After restart system|

* partition\_interval

|Name| partition\_interval |
|:---:|:---|
|Description| time range for partitioning data inside each storage group, the unit is second|
|Type| LONG |
|Default| 604800 |
|Effective|After restart system|

## Enable GC log
GC log is off by default.
For performance tuning, you may want to collect the GC info. 

To enable GC log, just add a parameter "printgc" when you start the server.

```bash
nohup sbin/start-server.sh printgc >/dev/null 2>&1 &
```
Or
```bash
sbin\start-server.bat printgc
```

GC log is stored at `IOTDB_HOME/logs/gc.log`.
There will be at most 10 gc.log.* files and each one can reach to 10MB.

