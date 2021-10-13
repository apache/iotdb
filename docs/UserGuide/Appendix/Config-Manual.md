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

Trigger way: The client sends the command `load configuration` to the IoTDB server. See [Command Line Interface(CLI)](https://iotdb.apache.org/UserGuide/Master/CLI/Command-Line-Interface.html) for the usage of the client.

## IoTDB Environment Configuration File

The environment configuration file is mainly used to configure the Java environment related parameters when IoTDB Server is running, such as JVM related configuration. This part of the configuration is passed to the JVM when the IoTDB Server starts. Users can view the contents of the environment configuration file by viewing the `iotdb-env.sh` (or `iotdb-env.bat`) file.

The detail of each variables are as follows:

* MAX\_HEAP\_SIZE

|Name|MAX\_HEAP\_SIZE|
|:---:|:---|
|Description|The maximum heap memory size that IoTDB can use at startup.|
|Type|String|
|Default|On Linux or MacOS, the default is one quarter of the memory. On Windows, the default value for 32-bit systems is 512M, and the default for 64-bit systems is 2G.|
|Effective|After restart system|

* HEAP\_NEWSIZE

|Name|HEAP\_NEWSIZE|
|:---:|:---|
|Description|The minimum heap memory size that IoTDB can use at startup.|
|Type|String|
|Default|On Linux or MacOS, the default is min{cores * 100M, one quarter of MAX\_HEAP\_SIZE}. On Windows, the default value for 32-bit systems is 512M, and the default for 64-bit systems is 2G.|
|Effective|After restart system|

* JMX\_LOCAL

|Name|JMX\_LOCAL|
|:---:|:---|
|Description|JMX monitoring mode, configured as yes to allow only local monitoring, no to allow remote monitoring.|
|Values|Enum String: "true", "false"|
|Default|true|
|Effective|After restart system|

* JMX\_PORT

|Name|JMX\_PORT|
|:---:|:---|
|Description|JMX listening port. Please confirm that the port is not a system reserved port and is not occupied.|
|Values|Short Int: [0,65535]|
|Default|31999|
|Effective|After restart system|

* JMX\_IP

|Name|JMX\_IP|
|:---:|:---|
|Description|JMX listening address. Only take effect if JMX\_LOCAL=false. 0.0.0.0 is never allowed.|
|Type|String|
|Default|127.0.0.1|
|Effective|After restart system|

## JMX Authorization

We **STRONGLY RECOMMENDED** you CHANGE the PASSWORD for the JMX remote connection.

The user and passwords are in ${IOTDB\_CONF}/conf/jmx.password.

The permission definitions are in ${IOTDB\_CONF}/conf/jmx.access.

## IoTDB System Configuration File

### RPC Configuration

* rpc\_address

|Name| rpc\_address |
|:---:|:---|
|Description|The jdbc service listens on the address.|
|Type|String|
|Default|"0.0.0.0"|
|Effective|After restart system|

* rpc\_port

|Name| rpc\_port |
|:---:|:---|
|Description|The jdbc service listens on the port. Please confirm that the port is not a system reserved port and is not occupied.|
|Values|Short Int : [0,65535]|
|Default|6667|
|Effective|After restart system|

* rpc\_thrift\_compression\_enable

|Name| rpc\_thrift\_compression\_enable |
|:---:|:---|
|Description|Whether enable thrift's compression (using GZIP).|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* rpc\_advanced\_compression\_enable

|Name| rpc\_advanced\_compression\_enable |
|:---:|:---|
|Description|Whether enable thrift's advanced compression.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* rpc\_max\_concurrent\_client\_num

|Name| rpc\_max\_concurrent\_client\_num |
|:---:|:---|
|Description|The maximum number of client connections.|
|Values|Int32 [0,65535]|
|Default|65535|
|Effective|After restart system|

* thrift\_max\_frame\_size

|Name| thrift\_max\_frame\_size |
|:---:|:---|
|Description|The max bytes in a RPC request/response.|
|Type|Long|
|Default|67108864 (should >= 8 * 1024 * 1024)|
|Effective|After restart system|

* thrift\_init\_buffer\_size

|Name| thrift\_init\_buffer\_size |
|:---:|:---|
|Description|Initialize buffer size.|
|Type|Long|
|Default|1024|
|Effective|After restart system|

### Write Ahead Log Configuration

* enable\_wal

|Name| enable\_wal |
|:---:|:---|
|Description|Whether to enable the pre-write log. The default value is true(enabled), and false means closed.|
|Type|Boolean|
|Default|true|
|Effective|Trigger|

* enable\_discard\_out\_of\_order\_data

|Name| enable\_discard\_out\_of\_order\_data |
|:---:|:---|
|Description|Whether to discard out-of-order data, the default value is false, which means it is closed.|
|Type|Boolean|
|Default|false|
|Effective|Trigger|

* flush\_wal\_threshold

|Name| flush\_wal\_threshold |
|:---:|:---|
|Description|When a certain amount of insert ahead log is reached, it will be flushed to disk,it is possible to lose at most flush_wal_threshold operations.|
|Type|Int32|
|Default|10000|
|Effective|Trigger|

* force\_wal\_period\_in\_ms

|Name| force\_wal\_period\_in\_ms |
|:---:|:---|
|Description|The cycle when insert ahead log is periodically forced to be written to disk(in milliseconds),if force_wal_period_in_ms = 0 ,it means force insert ahead log to be written to disk after each refreshment and may slow down the ingestion on slow disk.|
|Type|Int32|
|Default|100|
|Effective|Trigger|

### Directory Configuration

* system\_dir

|Name| system\_dir |
|:---:|:---|
|Description|If this property is unset, system will save the data in the default relative path directory under the IoTDB folder(i.e., %IOTDB_HOME%/data/system).|
|Type|String|
|Default|data/system (Windows：data\\system)|
|Effective|Trigger|

* data\_dirs

|Name| data\_dirs |
|:---:|:---|
|Description|If this property is unset, system will save the data in the default relative path directory under the IoTDB folder(i.e., %IOTDB_HOME%/data/data).|
|Type|String|
|Default|data/data (Windows：data\\data)|
|Effective|Trigger|

* multi\_dir\_strategy

|Name| multi\_dir\_strategy |
|:---:|:---|
|Description|The strategy is used to choose a directory from tsfile_dir for the system to store a new tsfile.System provides three strategies to choose from, or user can create his own strategy by extending org.apache.iotdb.db.conf.directories.strategy.DirectoryStrategy.The info of the three strategies are as follows: <br>1. SequenceStrategy: the system will choose the directory in sequence;<br>2. MaxDiskUsableSpaceFirstStrategy: the system will choose the directory whose disk has the maximum space;<br>3. MinFolderOccupiedSpaceFirstStrategy: the system will choose the directory whose folder has the minimum occupied space;<br>4. RandomOnDiskUsableSpaceStrategy: the system will randomly choose the directory based on usable space of disks. The more usable space, the greater the chance of being chosen;<br>Set SequenceStrategy,MaxDiskUsableSpaceFirstStrategy and MinFolderOccupiedSpaceFirstStrategy to apply the corresponding strategy.<br>If this property is unset, system will use MaxDiskUsableSpaceFirstStrategy as default strategy.<br>For this property, fully-qualified class name (include package name) and simple class name are both acceptable.|
|Type|String|
|Default|MaxDiskUsableSpaceFirstStrategy|
|Effective|Trigger|

* wal\_dir

|Name| wal\_dir |
|:---:|:---|
|Description|Write Ahead Log storage path.<br>It is recommended to use an absolute path.|
|Type|String|
|Default|data/wal (Windows：data\\wal)|
|Effective|After restart system|

* tsfile\_storage\_fs

|Name| tsfile\_storage\_fs |
|:---:|:---|
|Description|TSFile storage file system.<br>Currently, Tsfile are supported to be stored in LOCAL file system or HDFS.|
|Type|String|
|Default|LOCAL|
|Effective|Only allowed to be modified in first start up|

* core\_site\_path

|Name| core\_site\_path |
|:---:|:---|
|Description|If using HDFS, the absolute file path of Hadoop core-site.xml should be configured.|
|Type|String|
|Default|/etc/hadoop/conf/core-site.xml |
|Effective|After restart system|

* hdfs\_site\_path

|Name| hdfs\_site\_path |
|:---:|:---|
|Description|If using HDFS, the absolute file path of Hadoop hdfs-site.xml should be configured.|
|Type|String|
|Default|/etc/hadoop/conf/hdfs-site.xml|
|Effective|After restart system|

* hdfs\_ip

|Name| hdfs\_ip |
|:---:|:---|
|Description|If using HDFS, hadoop ip can be configured.<br>If there are more than one hdfs_ip, Hadoop HA is used.|
|Type|String|
|Default|localhost|
|Effective|After restart system|

* hdfs\_port

|Name| hdfs\_port |
|:---:|:---|
|Description|If using HDFS, hadoop port can be configured.|
|Type|String|
|Default|9000 |
|Effective|After restart system|

* dfs\_nameservices

|Name| hdfs\_nameservices |
|:---:|:---|
|Description|If there are more than one hdfs_ip, Hadoop HA is used.<br>Below are configuration for HA.<br>If using Hadoop HA, nameservices of hdfs can be configured.|
|Type|String|
|Default|hdfsnamespace|
|Effective|After restart system|

* dfs\_ha\_namenodes

|Name| dfs\_ha\_namenodes |
|:---:|:---|
|Description|If using Hadoop HA, namenodes under dfs nameservices can be configured.|
|Type|String|
|Default|nn1,nn2 |
|Effective|After restart system|

* dfs\_ha\_automatic\_failover\_enabled

|Name| dfs\_ha\_automatic\_failover\_enabled |
|:---:|:---|
|Description|If using Hadoop HA, automatic failover can be enabled or disabled.|
|Type|Boolean|
|Default|true|
|Effective|After restart system|

* dfs\_client\_failover\_proxy\_provider

|Name| dfs\_client\_failover\_proxy\_provider |
|:---:|:---|
|Description|If using Hadoop HA and enabling automatic failover, the proxy provider can be configured.|
|Type|String|
|Default|org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider|
|Effective|After restart system|

* hdfs\_use\_kerberos

|Name| hdfs\_use\_kerberos |
|:---:|:---|
|Description|If using kerberos to authenticate hdfs, this should be true.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* kerberos\_keytab\_file_path

|Name| kerberos\_keytab\_file_path |
|:---:|:---|
|Description|Full path of kerberos keytab file.|
|Type|String|
|Default|/path|
|Effective|After restart system|

* kerberos\_principal

|Name| kerberos\_principal |
|:---:|:---|
|Description|Kerberos pricipal|
|Type|String|
|Default|your principal|
|Effective|After restart system|

### Storage Engine Configuration

* timestamp\_precision

|Name| timestamp\_precision |
|:---:|:---|
|Description|Set timestamp precision as "ms", "us" or "ns".|
|Type|String|
|Default|ms|
|Effective|Trigger|

* default\_ttl

|Name| default\_ttl |
|:---:|:---|
|Description|Data retention time, the data before now()-default_ttl will be discarded, the unit is ms.|
|Type|Long|
|Default|36000000|
|Effective|After restart system|

* wal\_buffer\_size

|Name| wal\_buffer\_size |
|:---:|:---|
|Description|The size of the log buffer in each log node (in bytes).|
|Type|Int32|
|Default|16777216|
|Effective|Trigger|

* unseq\_tsfile\_size

|Name| unseq\_tsfile\_size |
|:---:|:---|
|Description|The size of unsequence TsFile's file size (in byte).|
|Type|Int32|
|Default|1|
|Effective|After restart system|

* seq\_tsfile\_size

|Name| seq\_tsfile\_size |
|:---:|:---|
|Description|The size of sequence TsFile's file size (in byte).|
|Type|Int32|
|Default|1|
|Effective|After restart system|

* mlog\_buffer\_size

|Name| mlog\_buffer\_size |
|:---:|:---|
|Description|The size of mlog buffer in each metadata operation plan(in byte).|
|Type|Int32|
|Default|1048576|
|Effective|Trigger|

* memtable\_size\_threshold

|Name| memtable\_size\_threshold |
|:---:|:---|
|Description|The memTable's size threshold,default threshold is 1 GB.|
|Type|Long|
|Default|1073741824|
|Effective|when enable\_mem\_control is false & After restart system|

* enable\_timed\_flush\_seq\_memtable

|Name| enable\_timed\_flush\_seq\_memtable |
|:---:|:---|
|Description|Whether to timed flush sequence tsfiles' memtables.|
|Type|Boolean|
|Default|false|
|Effective|Trigger|

* seq\_memtable\_flush\_interval\_in\_ms

|Name| seq\_memtable\_flush\_interval\_in\_ms |
|:---:|:---|
|Description|If a memTable's created time is older than current time minus this, the memtable will be flushed to disk.|
|Type|Int32|
|Default|3600000|
|Effective|Trigger|

* seq\_memtable\_flush\_check\_interval\_in\_ms

|Name| seq\_memtable\_flush\_check\_interval\_in\_ms |
|:---:|:---|
|Description|The interval to check whether sequence memtables need flushing.|
|Type|Int32|
|Default|600000|
|Effective|Trigger|

* enable\_timed\_flush\_unseq\_memtable

|Name| enable\_timed\_flush\_unseq\_memtable |
|:---:|:---|
|Description|Whether to enable timed flush unsequence memtable|
|Type|Boolean|
|Default|true|
|Effective|Trigger|

* unseq\_memtable\_flush\_interval\_in\_ms

|Name| unseq\_memtable\_flush\_interval\_in\_ms |
|:---:|:---|
|Description|If a memTable's created time is older than current time minus this, the memtable will be flushed to disk.|
|Type|Int32|
|Default|3600000|
|Effective| Trigger |

* unseq\_memtable\_flush\_check\_interval\_in\_ms

|Name| unseq\_memtable\_flush\_check\_interval\_in\_ms |
|:---:|:---|
|Description|The interval to check whether unsequence memtables need flushing.|
|Type|Int32|
|Default|600000|
|Effective|Trigger|

* enable\_timed\_close\_tsfile

|Name| enable\_timed\_close\_tsfile |
|:---:|:---|
|Description|Whether to timed close tsfiles.|
|Type|Boolean|
|Default|true|
|Effective|Trigger|

* close\_tsfile\_interval\_after\_flushing\_in\_ms

|Name| close\_tsfile\_interval\_after\_flushing\_in\_ms |
|:---:|:---|
|Description|If a TsfileProcessor's last working memtable flush time is older than current time minus this and its working memtable is null, the TsfileProcessor will be closed.|
|Type|Int32|
|Default|3600000|
|Effective|Trigger|

* close\_tsfile\_check\_interval\_in\_ms

|Name| close\_tsfile\_check\_interval\_in\_ms |
|:---:|:---|
|Description|The interval to check whether tsfiles need closing.|
|Type|Int32|
|Default|600000|
|Effective| Trigger |

* avg\_series\_point\_number\_threshold

|Name| avg\_series\_point\_number\_threshold |
|:---:|:---|
|Description|When the average point number of timeseries in memtable exceeds this, the memtable is flushed to disk. The default threshold is 10000.|
|Type|Int32|
|Default|10000|
|Effective|After restart system|

* concurrent\_flush\_thread

|Name| concurrent\_flush\_thread |
|:---:|:---|
|Description|How many threads can concurrently flush.<br>When <= 0, use CPU core number.|
|Type|Int32|
|Default|0|
|Effective|After restart system|

* concurrent\_query\_thread

|Name| concurrent\_query\_thread |
|:---:|:---|
|Description|How many threads can concurrently query.<br>When <= 0, use CPU core number.|
|Type|Int32|
|Default|0|
|Effective|After restart system|

* chunk\_buffer\_pool\_enable

|Name| chunk\_buffer\_pool\_enable |
|:---:|:---|
|Description|Whether take over the memory management by IoTDB rather than JVM when serializing memtable as bytes in memory.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* batch\_size

|Name| batch\_size |
|:---:|:---|
|Description|The amount of data iterate each time in server (the number of data strips, that is, the number of different timestamps.)|
|Type|Int32|
|Default|100000|
|Effective|After restart system|

* tag\_attribute\_total\_size

|Name| tag\_attribute\_total\_size |
|:---:|:---|
|Description|Max size for tag and attribute of one time series,the unit is byte.|
|Type|Int32|
|Default|700|
|Effective|Only allowed to be modified in first start up|

* enable\_partial\_insert

|Name| enable\_partial\_insert |
|:---:|:---|
|Description|In one insert (one device, one timestamp, multiple measurements),if enable partial insert, one measurement failure will not impact other measurements.|
|Type|Boolean|
|Default|true|
|Effective|After restart system|

* enable\_mtree\_snapshot

|Name| enable\_mtree\_snapshot |
|:---:|:---|
|Description|Whether to enable MTree snapshot. Default false from 0.11.0 on.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* mtree\_snapshot\_interval

|Name| mtree\_snapshot\_interval |
|:---:|:---|
|Description|The least interval line numbers of mlog.txt when creating a checkpoint and saving snapshot of MTree. Unit: line numbers|
|Type|Int32|
|Default|100000|
|Effective|After restart system|

* mtree\_snapshot\_threshold\_time

|Name| mtree\_snapshot\_threshold\_time |
|:---:|:---|
|Description|Threshold interval time of MTree modification,default 1 hour(3600 seconds).<br>If the last modification time is less than this threshold, MTree snapshot will not be created,and only take effect when enable_mtree_snapshot=true.|
|Type|Int32|
|Default|3600|
|Effective|After restart system|

* virtual\_storage\_group\_num

|Name| virtual\_storage\_group\_num |
|:---:|:---|
|Description|Number of virtual storage groups per user-defined storage group.|
|Type|Int64|
|Default|1|
|Effective|Only allowed to be modified in first start up|

* time\_index\_level

|Name| time\_index\_level |
|:---:|:---|
|Description|Level of TimeIndex, which records the start time and end time of TsFileResource.<br>Currently,DEVICE_TIME_INDEX and FILE_TIME_INDEX are supported, and could not be changed after first set.|
|Type|String|
|Default|DEVICE_TIME_INDEX|
|Effective|Trigger|

### Memory Control Configuration

* enable\_mem\_control

|Name| enable\_mem\_control |
|:---:|:---|
|Description|Whether to enable memory control.|
|Type|Boolean|
|Default|true|
|Effective|After restart system|

* write\_read\_schema\_free\_memory\_proportion

|Name| write\_read\_schema\_free\_memory\_proportion |
|:---:|:---|
|Description|Memory Allocation Ratio: Write, Read, Schema and Free Memory.<br>The parameter form is a : b : c : d, where a, b, c and d are integers,for example: 1:1:1:1 , 6:2:1:1.<br>If you have high level of writing pressure and low level of reading pressure, please adjust it to for example 6:1:1:2|
|Type|String|
|Default|4:3:1:2|
|Effective|After restart system|

* primitive\_array\_size

|Name| primitive\_array\_size |
|:---:|:---|
|Description|Primitive array size (length of each array) in array pool.|
|Type|Int32|
|Default|32|
|Effective|After restart system|

* flush\_proportion

|Name| flush\_proportion |
|:---:|:---|
|Description|Ratio of write memory for invoking flush disk, 0.4 by default.|
|Type|Float|
|Default|0.4|
|Effective|After restart system|

* buffered\_arrays\_memory\_proportion

|Name| buffered\_arrays\_memory\_proportion |
|:---:|:---|
|Description|Ratio of write memory allocated for buffered arrays, 0.6 by default.|
|Type|Float|
|Default|0.6|
|Effective|After restart system|

* reject\_proportion

|Name| reject\_proportion |
|:---:|:---|
|Description|Ratio of write memory for rejecting insertion, 0.8 by default.|
|Type|Float|
|Default|0.8|
|Effective|After restart system|

* storage\_group\_report\_threshold

|Name| storage\_group\_report\_threshold |
|:---:|:---|
|Description|If memory (in byte) of storage group increased more than this threshold, report to system. The default value is 16MB.|
|Type|Int32|
|Default|16777216|
|Effective|After restart system|

* max\_deduplicatedp\_path\_num

|Name| max\_deduplicatedp\_path\_num |
|:---:|:---|
|Description|Allowed max numbers of deduplicated path in one query.|
|Type|Int32|
|Default|1000|
|Effective|After restart system|

* check\_period\_when\_insert\_blocked

|Name| check\_period\_when\_insert\_blocked |
|:---:|:---|
|Description|When an inserting is rejected, waiting period (in ms) to check system again, 50 by default.|
|Type|Int32|
|Default|50|
|Effective|After restart system|

* max\_waiting\_time\_when\_insert\_blocked

|Name| max\_waiting\_time\_when\_insert\_blocked |
|:---:|:---|
|Description|When the waiting time (in ms) of an inserting exceeds this, throw an exception. 10000 by default.|
|Type|Int32|
|Default|10000|
|Effective|After restart system|

* estimated\_series\_size

|Name| estimated\_series\_size |
|:---:|:---|
|Description|Estimated metadata size (in byte) of one timeseries in Mtree.|
|Type|Int32|
|Default|300|
|Effective|After restart system|

* io\_task\_queue\_size\_for\_flushing

|Name| io\_task\_queue\_size\_for\_flushing |
|:---:|:---|
|Description|Size of ioTaskQueue. The default value is 10.|
|Type|Int32|
|Default|10|
|Effective|After restart system|

### Upgrade Configurations

* upgrade\_thread\_num

|Name| upgrade\_thread\_num |
|:---:|:---|
|Description|When there exists old version(0.9.x/v1) data, how many thread will be set up to perform upgrade tasks, 1 by default.|
|Type|Int32|
|Default|1|
|Effective|After restart system|

### Query Configurations

* default\_fill\_interval

|Name| default\_fill\_interval |
|:---:|:---|
|Description|The default time period that used in fill query, -1 by default means infinite past time, in ms.|
|Type|Int32|
|Default|-1|
|Effective|After restart system|

### Merge Configurations

* compaction\_strategy

|Name| compaction\_strategy |
|:---:|:---|
|Description|LEVEL_COMPACTION by default, can be set to NO_COMPACTION according to requirments.|
|Type|String|
|Default|LEVEL_COMPACTION|
|Effective|After restart system|

* enable\_unseq\_compaction

|Name| enable\_unseq\_compaction |
|:---:|:---|
|Description|Works when the compaction_strategy is LEVEL_COMPACTION.<br>Whether to merge unseq files into seq files or not.|
|Type|Boolean|
|Default|true|
|Effective|After restart system|

* compaction\_interval

|Name| compaction\_interval |
|:---:|:---|
|Description|Start compaction task at this delay, unit is ms.|
|Type|Int32|
|Default|30000|
|Effective|Only LEVEL_COMPACTION,after restart system|

* seq\_file\_num\_in\_each\_level

|Name| seq\_file\_num\_in\_each\_level |
|:---:|:---|
|Description|Works when the compaction_strategy is LEVEL_COMPACTION.<br>The max seq file num of each level.|
|Type|Int32|
|Default|6|
|Effective|After restart system|

* seq\_level\_num

|Name| seq\_level\_num |
|:---:|:---|
|Description|Works when the compaction_strategy is LEVEL_COMPACTION.<br>The max num of seq level.|
|Type|Int32|
|Default|3|
|Effective|After restart system|

* unseq\_file\_num\_in\_each\_level

|Name| unseq\_file\_num\_in\_each\_level |
|:---:|:---|
|Description|Works when the compaction_strategy is LEVEL_COMPACTION.<br>The max unseq file num of each level.|
|Type|Int32|
|Default|10|
|Effective|After restart system|

* unseq\_level\_num

|Name| unseq\_level\_num |
|:---:|:---|
|Description|Works when the compaction_strategy is LEVEL_COMPACTION.<br>The max num of unseq level.|
|Type|Int32|
|Default|1|
|Effective|After restart system|

* max\_select\_unseq\_file\_num\_in\_each\_unseq\_compaction

|Name| max\_select\_unseq\_file\_num\_in\_each\_unseq\_compaction |
|:---:|:---|
|Description|Works when the compaction_strategy is LEVEL_COMPACTION.<br>The max open file num in each unseq compaction task.|
|Type|Int32|
|Default|2000|
|Effective|After restart system|

* merge\_chunk\_point\_number

|Name| merge\_chunk\_point\_number |
|:---:|:---|
|Description|Works when the compaction_strategy is LEVEL_COMPACTION.<br>When the average point number of chunks in the target file reaches this, merge the file to the top level.|
|Type|Int32|
|Default|100000|
|Effective|After restart system|

* merge\_page\_point\_number

|Name| merge\_chunk\_point\_number |
|:---:|:---|
|Description|Works when the compaction_strategy is LEVEL_COMPACTION.<br>When point number of a page reaches this, use "append merge" instead of "deserialize merge".|
|Type|Int32|
|Default|100|
|Effective|After restart system|

* merge\_chunk\_subthread\_num

|Name| merge\_chunk\_subthread\_num |
|:---:|:---|
|Description|How many threads will be set up to perform unseq merge chunk sub-tasks, 4 by default.Set to 1 when less than or equal to 0.|
|Type|Int32|
|Default|4|
|Effective|After restart system|

* merge\_fileSelection\_time\_budget

|Name| merge\_fileSelection\_time\_budget |
|:---:|:---|
|Description|If one merge file selection runs for more than this time, it will be ended and its current selection will be used as final selection. Unit: millis.|
|Type|Int32|
|Default|30000|
|Effective|After restart system|

* merge\_memory\_budget

|Name| merge\_memory\_budget |
|:---:|:---|
|Description|How much memory may be used in ONE merge task (in byte), 10% of maximum JVM memory by default.<br>This is only a rough estimation, starting from a relatively small value to avoid OOM.|
|Type|Int32|
|Default|2147483648|
|Effective|After restart system|

* continue\_merge\_after\_reboot

|Name| continue\_merge\_after\_reboot |
|:---:|:---|
|Description|When set to true, if some crashed merges are detected during system rebooting, such merges will be continued, otherwise, the unfinished parts of such merges will not be continued while the finished parts still remains as they are.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* force\_full\_merge

|Name| force\_full\_merge |
|:---:|:---|
|Description|When set to true, all unseq merges becomes full merge (the whole SeqFiles are re-written despite how much they are overflowed).<br>This may increase merge overhead depending on how much the SeqFiles are overflowed.|
|Type|Boolean|
|Default|true|
|Effective|After restart system|

* compaction\_thread\_num

|Name| compaction\_thread\_num |
|:---:|:---|
|Description|How many threads will be set up to perform compaction, 10 by default.Set to 1 when less than or equal to 0.|
|Type|Int32|
|Default|10|
|Effective|After restart system|

* merge\_write\_throughput\_mb\_per\_sec

|Name| merge\_write\_throughput\_mb\_per\_sec |
|:---:|:---|
|Description|The limit of write throughput merge can reach per second.|
|Type|Int32|
|Default|8|
|Effective|After restart system|

* query\_timeout\_threshold

|Name| query\_timeout\_threshold |
|:---:|:---|
|Description|The max executing time of query. unit: ms.|
|Type|Int32|
|Default|60000|
|Effective|After restart system|

### Metadata Cache Configuration

* meta\_data\_cache\_enable

|Name| meta\_data\_cache\_enable |
|:---:|:---|
|Description|Whether to cache meta data(ChunkMetadata and TimeSeriesMetadata) or not.|
|Type|Boolean|
|Default|true|
|Effective|After restart system|

* chunk\_timeseriesmeta\_free\_memory\_proportion

|Name| chunk\_timeseriesmeta\_free\_memory\_proportion |
|:---:|:---|
|Description|Read memory Allocation Ratio: ChunkCache, TimeSeriesMetadataCache, memory used for constructing QueryDataSet and Free Memory Used in Query.<br>The parameter form is a : b : c : d, where a, b, c and d are integers. For example: 1:1:1:1 , 1:2:3:4.|
|Type|String|
|Default|1:2:3:4|
|Effective|After restart system|

* metadata\_node\_cache\_size

|Name| metadata\_node\_cache\_size |
|:---:|:---|
|Description|Cache size for MManager.<br>This cache is used to improve insert speed where all path check and TSDataType will be cached in MManager with corresponding Path.|
|Type|Int32|
|Default|300000|
|Effective|After restart system|

### LAST Cache Configuration

* enable\_last\_cache

|Name| enable\_last\_cache |
|:---:|:---|
|Description|Whether to enable LAST cache.|
|Type|Boolean|
|Default|true|
|Effective|After restart system|

### Statistics Monitor configuration

* enable\_stat\_monitor

|Name| enable\_stat\_monitor |
|:---:|:---|
|Description|Set enable_stat_monitor true(or false) to enable(or disable) the StatMonitor that stores statistics info.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* enable\_monitor\_series\_write

|Name| enable\_monitor\_series\_write |
|:---:|:---|
|Description|Set enable_monitor_series_write true (or false) to enable (or disable) the writing monitor time series.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

### WAL Direct Buffer Pool Configuration

* wal\_pool\_trim\_interval\_ms

|Name| wal\_pool\_trim\_interval\_ms |
|:---:|:---|
|Description|The interval to trim the wal pool.|
|Type|Int32|
|Default|10000|
|Effective|After restart system|

* max\_wal\_bytebuffer\_num\_for\_each\_partition

|Name| max\_wal\_bytebuffer\_num\_for\_each\_partition |
|:---:|:---|
|Description|The max number of wal bytebuffer can be allocated for each time partition, if there is no unseq data you can set it to 4,it should be an even number.|
|Type|Int32|
|Default|6|
|Effective|After restart system|

### External sort Configuration

* enable\_external\_sort

|Name| enable\_external\_sort |
|:---:|:---|
|Description|Is external sort enable.|
|Type|Boolean|
|Default|true|
|Effective|After restart system|

* external\_sort\_threshold

|Name| external\_sort\_threshold |
|:---:|:---|
|Description|The maximum number of simultaneous chunk reading for a single time series.<br>If the num of simultaneous chunk reading is greater than external_sort_threshold, external sorting is used.|
|Type|Int32|
|Default|1000|
|Effective|After restart system|

### Sync Server Configuration

* is\_sync\_enable

|Name| is\_sync\_enable |
|:---:|:---|
|Description|Whether to open the sync_server_port for receiving data from sync client, the default is closed.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* sync\_server\_port

|Name| sync\_server\_port |
|:---:|:---|
|Description|Sync server port to listen.|
|Type|Int32|
|Default|5555|
|Effective|After restart system|

* ip\_white\_list

|Name| ip\_white\_list |
|:---:|:---|
|Description|White IP list of Sync client.Please use the form of network segment to present the range of IP, for example: 192.168.0.0/16.|
|Type|String|
|Default|0.0.0.0/0|
|Effective|After restart system|

### performance statistic configuration

* enable\_performance\_stat

|Name| enable\_performance\_stat |
|:---:|:---|
|Description|Is stat performance of sub-module enable.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* performance\_stat\_display\_interval

|Name| performance\_stat\_display\_interval |
|:---:|:---|
|Description|The interval of display statistic result in ms.|
|Type|Int32|
|Default|60000|
|Effective|After restart system|

* performance\_stat\_display\_interval

|Name| performance\_stat\_display\_interval |
|:---:|:---|
|Description|The interval of display statistic result in ms.|
|Type|Int32|
|Default|60000|
|Effective|After restart system|

* performance\_stat\_memory\_in\_kb

|Name| performance\_stat\_memory\_in\_kb |
|:---:|:---|
|Description|The memory used for performance_stat in kb.|
|Type|Int32|
|Default|20|
|Effective|After restart system|

* enable\_performance\_tracing

|Name| enable\_performance\_tracing |
|:---:|:---|
|Description|Is performance tracing enable.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* tracing\_dir

|Name| tracing\_dir |
|:---:|:---|
|Description|Uncomment following fields to configure the tracing root directory.|
|Type|String|
|Default|data/tracing(Windows：data\tracing)|
|Effective|After restart system|

### Configurations for watermark module

* watermark\_module\_opened

|Name| watermark\_module\_opened |
|:---:|:---|
|Description|Whether to enable the watermark embedding function.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* watermark\_secret\_key

|Name| watermark\_secret\_key |
|:---:|:---|
|Description|Watermark embedding function key.|
|Type|String|
|Default|IoTDB * 2019@Beijing|
|Effective|After restart system|

* watermark\_bit\_string

|Name| watermark\_bit\_string |
|:---:|:---|
|Description|Watermark bit string.|
|Type|Int32|
|Default|100101110100|
|Effective|After restart system|

* watermark\_method

|Name| watermark\_method |
|:---:|:---|
|Description|Watermark embedding method.|
|Type|String|
|Default|GroupBasedLSBMethod(embed_row_cycle=2,embed_lsb_num=5)|
|Effective|After restart system|

### Configurations for creating schema automatically

* enable\_auto\_auto\_create\_schema

|Name| enable\_auto\_auto\_create\_schema |
|:---:|:---|
|Description|Whether creating schema automatically is enabled.|
|Type|Boolean|
|Default|true|
|Effective|After restart system|

* default\_storage\_group\_level

|Name| default\_storage\_group\_level |
|:---:|:---|
|Description|Storage group level when creating schema automatically is enabled.|
|Type|Int32|
|Default|1|
|Effective|After restart system|

* boolean\_string\_infer\_type

|Name| boolean\_string\_infer\_type |
|:---:|:---|
|Description|Register time series as which type when receiving boolean string "true" or "false"|
|Values|BOOLEAN or TEXT|
|Default|BOOLEAN|
|Effective|After restart system|

* integer\_string\_infer\_type

|Name| integer\_string\_infer\_type |
|:---:|:---|
|Description|Register time series as which type when receiving an integer string "67".|
|Values|INT32, INT64, DOUBLE, FLOAT or TEXT|
|Default|FLOAT|
|Effective|After restart system|

* long\_string\_infer\_type

|Name| long\_string\_infer\_type |
|:---:|:---|
|Description|Register time series as which type when receiving an integer string and using float may lose precision num > 2 ^ 24.|
|Values|DOUBLE, FLOAT or TEXT|
|Default|DOUBLE|
|Effective|After restart system|

* floating\_string\_infer\_type

|Name| floating\_string\_infer\_type |
|:---:|:---|
|Description|Register time series as which type when receiving a floating number string "6.7"|
|Values|DOUBLE, FLOAT or TEXT|
|Default|FLOAT |
|Effective|After restart system|

* nan\_string\_infer\_type

|Name| nan\_string\_infer\_type |
|:---:|:---|
|Description|Register time series as which type when receiving the Literal NaN.<br>Values can be DOUBLE, FLOAT or TEXT|
|Values|DOUBLE, FLOAT or TEXT|
|Default|DOUBLE|
|Effective|After restart system|

* default\_boolean\_encoding

|Name| default\_boolean\_encoding |
|:---:|:---|
|Description|BOOLEAN encoding when creating schema automatically is enabled.|
|Values|PLAIN, RLE|
|Default|RLE|
|Effective|After restart system|

* default\_int32\_encoding

|Name| default\_int32\_encoding |
|:---:|:---|
|Description|INT32 encoding when creating schema automatically is enabled|
|Values|PLAIN, RLE, TS_2DIFF, REGULAR, GORILLA|
|Default|RLE|
|Effective|After restart system|

* default\_int64\_encoding

|Name| default\_int64\_encoding |
|:---:|:---|
|Description|INT64 encoding when creating schema automatically is enabled.|
|Values|PLAIN, RLE, TS_2DIFF, REGULAR, GORILLA|
|Default|RLE|
|Effective|After restart system|

* default\_float\_encoding

|Name| default\_float\_encoding |
|:---:|:---|
|Description|FLOAT encoding when creating schema automatically is enabled.|
|Values|PLAIN, RLE, TS_2DIFF, GORILLA|
|Default|GORILLA|
|Effective|After restart system|

* default\_double\_encoding

|Name| default\_double\_encoding |
|:---:|:---|
|Description|DOUBLE encoding when creating schema automatically is enabled.|
|Values|PLAIN, RLE, TS_2DIFF, GORILLA|
|Default|GORILLA|
|Effective|After restart system|

* default\_text\_encoding

|Name| default\_text\_encoding |
|:---:|:---|
|Description|TEXT encoding when creating schema automatically is enabled.|
|Values| PLAIN |
|Default|PLAIN|
|Effective|After restart system|

### Configurations for tsfile-format

* group\_size\_in\_byte

|Name|group\_size\_in\_byte|
|:---:|:---|
|Description|The data size written to the disk per time.|
|Type|Int32|
|Default|134217728|
|Effective|Trigger|

* page\_size\_in\_byte

|Name| page\_size\_in\_byte |
|:---:|:---|
|Description|The memory size for each series writer to pack page, default value is 64KB.|
|Type|Int32|
|Default|65536|
|Effective|Trigger|

* max\_number\_of\_points\_in\_page

|Name| max\_number\_of\_points\_in\_page |
|:---:|:---|
|Description|The maximum number of data points in a page, default 1024 * 1024.|
|Type|Int32|
|Default|1048576|
|Effective|Trigger|

* time\_series\_data\_type

|Name| time\_series\_data\_type |
|:---:|:---|
|Description|Data type configuration.Data type for input timestamp, supports INT32 or INT64.|
|Type|String|
|Default|INT64|
|Effective|Trigger|

* max\_string\_length

|Name| max\_string\_length |
|:---:|:---|
|Description|Max size limitation of input string.|
|Type|Int32|
|Default|128|
|Effective|Trigger|

* float_precision

|Name| float_precision |
|:---:|:---|
|Description|Floating-point precision.|
|Type|Int32|
|Default|2|
|Effective|Trigger|

* time\_encoder

|Name| time\_encoder |
|:---:|:---|
|Description|Encoder configuration.Encoder of time series, supports TS_2DIFF, PLAIN and RLE(run-length encoding), REGULAR and default value is TS_2DIFF.|
|Values|Enum String: “TS_2DIFF”,“PLAIN”,“RLE”|
|Default|TS_2DIFF|
|Effective|Trigger|

* value\_encoder

|Name| value\_encoder |
|:---:|:---|
|Description|Encoder of value series. default value is PLAIN.|
|Values|Enum String: “TS_2DIFF”,“PLAIN”,“RLE”|
|Default|PLAIN|
|Effective|Trigger|

* compressor

|Name| compressor |
|:---:|:---|
|Description|Compression configuration.<br>Data compression method, supports UNCOMPRESSED, SNAPPY or LZ4. Default value is SNAPPY.|
|Values|Enum String : “UNCOMPRESSED”, “SNAPPY”|
|Default|SNAPPY|
|Effective|Trigger|

* max\_degree\_of\_index\_node

|Name| max\_degree\_of\_index\_node |
|:---:|:---|
|Description|Maximum degree of a metadataIndex node, default value is 256.|
|Type|Int32|
|Default|256|
|Effective|Only allowed to be modified in first start up|

* frequency\_interval\_in\_minute

|Name| frequency\_interval\_in\_minute |
|:---:|:---|
|Description|Time interval in minute for calculating query frequency.|
|Type|Int32|
|Default|1|
|Effective|Trigger|

* slow\_query\_threshold

|Name| slow\_query\_threshold |
|:---:|:---|
|Description|Time cost(ms) threshold for slow query.|
|Type|Int32|
|Default|5000|
|Effective|Trigger|

### MQTT Broker Configuration

* enable\_mqtt\_service

|Name| enable\_mqtt\_service |
|:---:|:---|
|Description|Whether to enable the mqtt service.|
|Type|Boolean|
|Default|false|
|Effective|Trigger|

* mqtt\_host

|Name| mqtt\_host |
|:---:|:---|
|Description|The mqtt service binding host.|
|Type|String|
|Default|0.0.0.0|
|Effective|Trigger|

* mqtt\_port

|Name| mqtt\_port |
|:---:|:---|
|Description|The mqtt service binding port.|
|Type|Int32|
|Default|1883|
|Effective|Trigger|

* mqtt\_handler\_pool\_size

|Name| mqtt\_handler\_pool\_size |
|:---:|:---|
|Description|The handler pool size for handing the mqtt messages.|
|Type|Int32|
|Default|1|
|Effective|Trigger|

* mqtt\_payload\_formatter

|Name| mqtt\_payload\_formatter |
|:---:|:---|
|Description|The mqtt message payload formatter.|
|Type|String|
|Default|json|
|Effective|Trigger|

* mqtt\_max\_message\_size

|Name| mqtt\_max\_message\_size |
|:---:|:---|
|Description|Max length of mqtt message in byte|
|Type|Int32|
|Default|1048576|
|Effective|Trigger|

### Authorization Configuration

* authorizer\_provider\_class

|Name| authorizer\_provider\_class |
|:---:|:---|
|Description|Which class to serve for authorization.<br>By default, it is LocalFileAuthorizer.<br>Another choice is org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer|
|Type| String |
|Default|org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer |
|Effective|After restart system|
|Other available values| org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer |

* openID\_url

|Name| openID\_url |
|:---:|:---|
|Description|If OpenIdAuthorizer is enabled, then openID_url must be set.|
|Type|String (a http url)|
|Default|no|
|Effective|After restart system|

### UDF Query Configuration

* udf\_initial\_byte\_array\_length\_for\_memory\_control

|Name| udf\_initial\_byte\_array\_length\_for\_memory\_control |
|:---:|:---|
|Description|Used to estimate the memory usage of text fields in a UDF query.<br>It is recommended to set this value to be slightly larger than the average length of all text records.|
|Type|Int32|
|Default|48|
|Effective|After restart system|

* udf\_memory\_budget\_in\_mb

|Name| udf\_memory\_budget\_in\_mb |
|:---:|:---|
|Description|How much memory may be used in ONE UDF query (in MB).<br>The upper limit is 20% of allocated memory for read.|
|Type|Float|
|Default|30.0|
|Effective|After restart system|

* udf\_reader\_transformer\_collector\_memory\_proportion

|Name| udf\_reader\_transformer\_collector\_memory\_proportion |
|:---:|:---|
|Description|UDF memory allocation ratio.<br>The parameter form is a : b : c, where a, b, and c are integers.|
|Type|String|
|Default|1:1:1|
|Effective|After restart system|

* udf\_root\_dir

|Name| udf\_root\_dir |
|:---:|:---|
|Description|Uncomment following fields to configure the udf root directory.|
|Type|String|
|Default|ext/udf (Windows:ext\\udf)|
|Effective|After restart system|

* index\_root\_dir

|Name| index\_root\_dir |
|:---:|:---|
|Description|Uncomment following fields to configure the index root directory.|
|Type|String|
|Default|data/index (Windows:data\\index)|
|Effective|After restart system|

* enable\_index

|Name| enable\_index |
|:---:|:---|
|Description|Is index enable.|
|Type|Boolean|
|Default|false|
|Effective|After restart system|

* concurrent\_index\_build\_thread

|Name| concurrent\_index\_build\_thread |
|:---:|:---|
|Description|How many threads can concurrently build index. <br>When <= 0, use CPU core number.|
|Type|Int32|
|Default|0|
|Effective|After restart system|

* default\_index\_window\_range

|Name| default\_index\_window\_range |
|:---:|:---|
|Description|The default size of sliding window used for the subsequence matching in index framework.|
|Type|Int32|
|Default|10|
|Effective|After restart system|

* index\_buffer\_size

|Name| index\_buffer\_size |
|:---:|:---|
|Description|Buffer parameter for index processor.|
|Type|Int32|
|Default|134217728|
|Effective|After restart system|

* enable\_partition

|Name| enable\_partition |
|:---:|:---|
|Description|Whether enable data partition. If disabled, all data belongs to partition 0.|
|Type|Boolean|
|Default|false|
|Effective|Only allowed to be modified in first start up|

* partition\_interval

|Name| partition\_interval |
|:---:|:---|
|Description|Time range for partitioning data inside each storage group, the unit is second.|
|Type|Int64|
|Default|604800|
|Effective|Only allowed to be modified in first start up|

* concurrent\_writing\_time\_partition

|Name| concurrent\_writing\_time\_partition |
|:---:|:---|
|Description|The maximum number of time partitions that can be written at the same time, the default is 500 partitions.|
|Type|Int32|
|Default|500|
|Effective|After restart system|

* admin\_name

|Name| admin\_name |
|:---:|:---|
|Description|Admin username, default is root.|
|Type|String|
|Default|root|
|Effective|Only allowed to be modified in first start up|

* admin\_password

|Name| admin\_password |
|:---:|:---|
|Description|Admin password, default is root.|
|Type|String|
|Default|root|
|Effective|Only allowed to be modified in first start up|

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

