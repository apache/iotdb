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

# Common Configuration

IoTDB common files for ConfigNode and DataNode are under `conf`.

* `iotdb-common.properties`：IoTDB common configurations.

## Configuration File

### Replication Configuration

* config\_node\_consensus\_protocol\_class

|    Name     | config\_node\_consensus\_protocol\_class                               |
| :---------: | :--------------------------------------------------------------------- |
| Description | Consensus protocol of ConfigNode replicas, only support RatisConsensus |
|    Type     | String                                                                 |
|   Default   | org.apache.iotdb.consensus.ratis.RatisConsensus                        |
|  Effective  | Only allowed to be modified in first start up                          |

* schema\_replication\_factor

|Name| schema\_replication\_factor |
|:---:|:---|
|Description| Schema replication num|
|Type| int32 |
|Default| 1 |
|Effective|After restarting system|

* schema\_region\_consensus\_protocol\_class

|    Name     | schema\_region\_consensus\_protocol\_class                                                                                                   |
| :---------: | :------------------------------------------------------------------------------------------------------------------------------------------- |
| Description | Consensus protocol of schema replicas, SimpleConsensus could only be used in 1 replica，larger than 1 replicas could only use RatisConsensus |  |
|    Type     | String                                                                                                                                       |
|   Default   | org.apache.iotdb.consensus.simple.SimpleConsensus                                                                                            |
|  Effective  | Only allowed to be modified in first start up                                                                                                |

* data\_replication\_factor

|Name| data\_replication\_factor |
|:---:|:---|
|Description| Data replication num|
|Type| int32 |
|Default| 1 |
|Effective|After restarting system|

* data\_region\_consensus\_protocol\_class

|    Name     | data\_region\_consensus\_protocol\_class                                                                                                                      |
| :---------: | :------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Description | Consensus protocol of data replicas, SimpleConsensus could only be used in 1 replica，larger than 1 replicas could use MultiLeaderConsensus or RatisConsensus |
|    Type     | String                                                                                                                                                        |
|   Default   | org.apache.iotdb.consensus.simple.SimpleConsensus                                                                                                             |
|  Effective  | Only allowed to be modified in first start up                                                                                                                 |

### Partition (Load balancing) Configuration

* series\_partition\_slot\_num

|Name| series\_partition\_slot\_num |
|:---:|:---|
|Description| Slot num of series partition |
|Type| int32 |
|Default| 10000 |
|Effective|Only allowed to be modified in first start up|

* series\_partition\_executor\_class

|    Name     | series\_partition\_executor\_class                                |
| :---------: | :---------------------------------------------------------------- |
| Description | Series partition hash function                                    |
|    Type     | String                                                            |
|   Default   | org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor |
|  Effective  | Only allowed to be modified in first start up                     |

* region\_allocate\_strategy

|    Name     | region\_allocate\_strategy                                                                               |
| :---------: | :------------------------------------------------------------------------------------------------------- |
| Description | Region allocate strategy, COPY_SET is suitable for large clusters, GREEDY is suitable for small clusters |
|    Type     | String                                                                                                   |
|   Default   | GREEDY                                                                                                   |
|  Effective  | After restarting system                                                                                  |

### Cluster Management

* time\_partition\_interval

|    Name     | time\_partition\_interval                                     |
| :---------: | :------------------------------------------------------------ |
| Description | Time partition interval of data when ConfigNode allocate data |
|    Type     | Long                                                          |
|    Unit     | ms                                                            |
|   Default   | 604800000                                                     |
|  Effective  | Only allowed to be modified in first start up                 |

* heartbeat\_interval\_in\_ms

|    Name     | heartbeat\_interval\_in\_ms             |
| :---------: | :-------------------------------------- |
| Description | Heartbeat interval in the cluster nodes |
|    Type     | Long                                    |
|    Unit     | ms                                      |
|   Default   | 1000                                    |
|  Effective  | After restarting system                 |

* disk\_space\_warning\_threshold

|    Name     | disk\_space\_warning\_threshold |
| :---------: | :------------------------------ |
| Description | Disk remaining threshold        |
|    Type     | double(percentage)              |
|   Default   | 0.05                            |
|  Effective  | After restarting system         |

### Memory Control Configuration

* enable\_mem\_control

|Name| enable\_mem\_control |
|:---:|:---|
|Description| enable memory control to avoid OOM|
|Type|Boolean|
|Default| true |
|Effective|After restarting system|

* storage\_query_schema_consensus_free_memory_proportion

|Name| storage\_query\_schema\_consensus\_free\_memory\_proportion                                                  |
|:---:|:-------------------------------------------------------------------------------------------------------------|
|Description| Memory Allocation Ratio: StorageEngine, QueryEngine, SchemaEngine, Consensus and Free Memory                 |
|Type| Ratio                                                                                                        |
|Default| 3:3:1:1:2                                                                                                    |
|Effective| After restarting system                                                                                      |

* schema\_memory\_allocate\_proportion

|Name| schema\_memory\_allocate\_proportion                                                    |
|:---:|:----------------------------------------------------------------------------------------|
|Description| Schema Memory Allocation Ratio: SchemaRegion, SchemaCache, PartitionCache and LastCache |
|Type| Ratio                                                                                   |
|Default| 5:3:1:1                                                                                 |
|Effective| After restarting system                                                                 |

* storage\_engine\_memory\_proportion

|Name| storage\_engine\_memory\_proportion |
|:---:|:------------------------------------|
|Description| Memory allocation ratio in StorageEngine: Write, Compaction                       |
|Type| Ratio                               |
|Default| 8:2                                 |
|Effective| After restarting system                              |

* write\_memory\_proportion

|Name| write\_memory\_proportion                            |
|:---:|:----------------------------------------------------------------|
|Description| Memory allocation ratio in writing: Memtable, TimePartitionInfo |
|Type| Ratio                                                           |
|Default| 19:1                                                       |
|Effective| After restarting system                                                          |

* concurrent\_writing\_time\_partition

|Name| concurrent\_writing\_time\_partition |
|:---:|:---|
|Description| This config decides how many time partitions in a database can be inserted concurrently </br> For example, your partitionInterval is 86400 and you want to insert data in 5 different days, |
|Type|int32|
|Default| 1 |
|Effective|After restarting system|

* primitive\_array\_size

|Name| primitive\_array\_size |
|:---:|:---|
|Description| primitive array size (length of each array) in array pool|
|Type| Int32 |
|Default| 32 |
|Effective|After restart system|

* chunk\_metadata\_size\_proportion

|Name| chunk\_metadata\_size\_proportion   |
|:---:|:------------------------------------|
|Description| size proportion for chunk metadata maintains in memory when writing tsfile |
|Type| Double                              |
|Default| 0.1                                 |
|Effective|After restart system|

* flush\_proportion

|    Name     | flush\_proportion                                                                                                                                                          |
|:-----------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Description | Ratio of write memory for invoking flush disk, 0.4 by default If you have extremely high write load (like batch=1000), it can be set lower than the default value like 0.2 |
|    Type     | Double                                                                                                                                                                     |
|   Default   | 0.4                                                                                                                                                                        |
|  Effective  | After restart system                                                                                                                                                       |

* buffered\_arrays\_memory\_proportion

|Name| buffered\_arrays\_memory\_proportion |
|:---:|:---|
|Description| Ratio of write memory allocated for buffered arrays |
|Type| Double |
|Default| 0.6 |
|Effective|After restart system|

* reject\_proportion

|Name| reject\_proportion |
|:---:|:---|
|Description| Ratio of write memory for rejecting insertion |
|Type| Double |
|Default| 0.8 |
|Effective|After restart system|

* write\_memory\_variation\_report\_proportion

|    Name     | write\_memory\_variation\_report\_proportion                 |
| :---------: | :----------------------------------------------------------- |
| Description | if memory cost of data region increased more than proportion of allocated memory for write, report to system |
|    Type     | Double                                                       |
|   Default   | 0.001                                                        |
|  Effective  | After restarting system                                      |

* check\_period\_when\_insert\_blocked

|Name| check\_period\_when\_insert\_blocked                                        |
|:---:|:----------------------------------------------------------------------------|
|Description| when an inserting is rejected, waiting period (in ms) to check system again |
|Type| Int32                                                                       |
|Default| 50                                                                          |
|Effective| After restart system                                                        |

* io\_task\_queue\_size\_for\_flushing

|Name| io\_task\_queue\_size\_for\_flushing          |
|:---:|:----------------------------------------------|
|Description| size of ioTaskQueue. The default value is 10  |
|Type| Int32                                         |
|Default| 10                                            |
|Effective| After restart system                          |

* enable\_query\_memory\_estimation

|Name| enable\_query\_memory\_estimation |
|:---:|:----------------------------------|
|Description| If true, we will estimate each query's possible memory footprint before executing it and deny it if its estimated memory exceeds current free memory |
|Type| bool                              |
|Default| true                              |
|Effective|Trigger|

* partition\_cache\_size

|Name| partition\_cache\_size |
|:---:|:---|
|Description| The max num of partition info record cached on DataNode. |
|Type| Int32 |
|Default| 1000 |
|Effective|After restarting system|

### Schema Engine Configuration

* mlog\_buffer\_size

|Name| mlog\_buffer\_size |
|:---:|:---|
|Description| size of log buffer in each metadata operation plan(in byte) |
|Type|int32|
|Default| 1048576 |
|Effective|After restart system|

* sync\_mlog\_period\_in\_ms

|    Name     | sync\_mlog\_period\_in\_ms                                                                                                                                                                         |
| :---------: | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Description | The cycle when metadata log is periodically forced to be written to disk(in milliseconds). If force_mlog_period_in_ms = 0 it means force metadata log to be written to disk after each refreshment |
|    Type     | Int64                                                                                                                                                                                              |
|   Default   | 100                                                                                                                                                                                                |
|  Effective  | After restarting system                                                                                                                                                                               |

* tag\_attribute\_flush\_interval

|Name| tag\_attribute\_flush\_interval                                                                                                                                                                                                                |
|:---:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|Description| interval num for tag and attribute records when force flushing to disk. When a certain amount of tag and attribute records is reached, they will be force flushed to disk. It is possible to lose at most tag_attribute_flush_interval records |
|Type| int32                                                                                                                                                                                                                                          |
|Default| 1000                                                                                                                                                                                                                                           |
|Effective| Only allowed to be modified in first start up                                                                                                                                                                                                  |

* tag\_attribute\_total\_size

|Name| tag\_attribute\_total\_size |
|:---:|:---|
|Description| The maximum persistence size of tags and attributes of each time series.|
|Type| int32 |
|Default| 700 |
|Effective|Only allowed to be modified in first start up|

* schema\_region\_device\_node\_cache\_size

|Name| schema\_region\_device\_node\_cache\_size |
|:---:|:--------------------------------|
|Description| The max num of device node, used for speeding up device query, cached in schemaRegion.      |
|Type| Int32                           |
|Default| 10000                          |
|Effective|After restarting system|

* max\_measurement\_num\_of\_internal\_request

|Name| max\_measurement\_num\_of\_internal\_request |
|:---:|:--------------------------------|
|Description| When there's too many measurements in one create timeseries plan, the plan will be split to several sub plan, with measurement num no more than this param.|
|Type| Int32                           |
|Default| 10000                          |
|Effective|After restarting system|

### Configurations for creating schema automatically

* enable\_auto\_create\_schema

|    Name     | enable\_auto\_create\_schema                                                  |
| :---------: | :---------------------------------------------------------------------------- |
| Description | whether auto create the time series when a non-existed time series data comes |
|    Type     | true or false                                                                 |
|   Default   | true                                                                          |
|  Effective  | After restarting system                                                       |

* default\_storage\_group\_level

|    Name     | default\_storage\_group\_level                                                                                                                                                                             |
| :---------: | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Description | Database level when creating schema automatically is enabled. For example, if we receives a data point from root.sg0.d1.s2, we will set root.sg0 as the database if database level is 1. (root is level 0) |
|    Type     | integer                                                                                                                                                                                                    |
|   Default   | 1                                                                                                                                                                                                          |
|  Effective  | After restarting system                                                                                                                                                                                    |

* boolean\_string\_infer\_type

|    Name     | boolean\_string\_infer\_type                                  |
| :---------: | :------------------------------------------------------------ |
| Description | To which type the values "true" and "false" should be reslved |
|    Type     | BOOLEAN or TEXT                                               |
|   Default   | BOOLEAN                                                       |
|  Effective  | After restarting system                                       |

* integer\_string\_infer\_type

|    Name     | integer\_string\_infer\_type                                            |
| :---------: | :---------------------------------------------------------------------- |
| Description | To which type an integer string like "67" in a query should be resolved |
|    Type     | INT32, INT64, DOUBLE, FLOAT or TEXT                                     |
|   Default   | DOUBLE                                                                  |
|  Effective  | After restarting system                                                 |

* floating\_string\_infer\_type

|    Name     | floating\_string\_infer\_type                                                   |
| :---------: | :------------------------------------------------------------------------------ |
| Description | To which type a floating number string like "6.7" in a query should be resolved |
|    Type     | DOUBLE, FLOAT or TEXT                                                           |
|   Default   | FLOAT                                                                           |
|  Effective  | After restarting system                                                         |

* nan\_string\_infer\_type

|    Name     | nan\_string\_infer\_type                                  |
| :---------: | :-------------------------------------------------------- |
| Description | To which type the value NaN in a query should be resolved |
|    Type     | DOUBLE, FLOAT or TEXT                                     |
|   Default   | FLOAT                                                     |
|  Effective  | After restarting system                                   |

### Query Configurations

* read\_consistency\_level

|    Name     | mpp\_data\_exchange\_core\_pool\_size        |
|:-----------:|:---------------------------------------------|
| Description | The read consistency level, </br>1. strong(Default, read from the leader replica) </br>2. weak(Read from a random replica) |
|    Type     | string                                          |
|   Default   | strong                                           |
|  Effective  | After restarting system                      |

* meta\_data\_cache\_enable

|Name| meta\_data\_cache\_enable |
|:---:|:---|
|Description| Whether to cache meta data(BloomFilter, ChunkMetadata and TimeSeriesMetadata) or not.|
|Type|Boolean|
|Default| true |
|Effective| After restarting system|

* chunk\_timeseriesmeta\_free\_memory\_proportion

|Name| chunk\_timeseriesmeta\_free\_memory\_proportion                                                                                                                           |
|:---:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|Description| Read memory Allocation Ratio: BloomFilterCache : ChunkCache : TimeSeriesMetadataCache : Coordinator : Operators : DataExchange : timeIndex in TsFileResourceList : others. |
|Default| 1 : 100 : 200 : 300 : 400   |
|Effective| After restarting system |

* enable\_last\_cache

|Name| enable\_last\_cache |
|:---:|:---|
|Description| Whether to enable LAST cache. |
|Type| Boolean |
|Default| true |
|Effective|After restarting system|

* max\_deduplicated\_path\_num

|Name| max\_deduplicated\_path\_num |
|:---:|:---|
|Description| allowed max numbers of deduplicated path in one query. |
|Type| Int32 |
|Default| 1000 |
|Effective|After restarting system|

* mpp\_data\_exchange\_core\_pool\_size

|    Name     | mpp\_data\_exchange\_core\_pool\_size        |
|:-----------:|:---------------------------------------------|
| Description | Core size of ThreadPool of MPP data exchange |
|    Type     | int32                                          |
|   Default   | 10                                           |
|  Effective  | After restarting system                      |

* mpp\_data\_exchange\_max\_pool\_size

|    Name     | mpp\_data\_exchange\_max\_pool\_size        |
| :---------: | :------------------------------------------ |
| Description | Max size of ThreadPool of MPP data exchange |
|    Type     | int32                                         |
|   Default   | 10                                          |
|  Effective  | After restarting system                     |

* mpp\_data\_exchange\_keep\_alive\_time\_in\_ms

|Name| mpp\_data\_exchange\_keep\_alive\_time\_in\_ms |
|:---:|:---|
|Description| Max waiting time for MPP data exchange |
|Type| long |
|Default| 1000 |
|Effective|After restarting system|

* driver\_task\_execution\_time\_slice\_in\_ms

|    Name     | driver\_task\_execution\_time\_slice\_in\_ms |
| :---------: | :------------------------------------------- |
| Description | Maximum execution time of a DriverTask       |
|    Type     | int32                                          |
|   Default   | 100                                          |
|  Effective  | After restarting system                      |

* max\_tsblock\_size\_in\_bytes

|    Name     | max\_tsblock\_size\_in\_bytes |
| :---------: | :---------------------------- |
| Description | Maximum capacity of a TsBlock |
|    Type     | int32                           |
|   Default   | 1024 * 1024 (1 MB)            |
|  Effective  | After restarting system       |

* max\_tsblock\_line\_numbers

|    Name     | max\_tsblock\_line\_numbers                 |
| :---------: | :------------------------------------------ |
| Description | Maximum number of lines in a single TsBlock |
|    Type     | int32                                         |
|   Default   | 1000                                        |
|  Effective  | After restarting system                     |

* slow\_query\_threshold

|Name| slow\_query\_threshold |
|:---:|:---|
|Description| Time cost(ms) threshold for slow query. |
|Type| Int32 |
|Default| 5000 |
|Effective|Trigger|

* query\_timeout\_threshold

|Name| query\_timeout\_threshold |
|:---:|:---|
|Description| The max executing time of query. unit: ms |
|Type| Int32 |
|Default| 60000 |
|Effective| After restarting system|

* max\_allowed\_concurrent\_queries

|Name| max\_allowed\_concurrent\_queries |
|:---:|:---|
|Description| The maximum allowed concurrently executing queries. |
|Type| Int32 |
|Default| 1000 |
|Effective|After restarting system|

* query\_thread\_count

|Name| query\_thread\_count                                                                                            |
|:---:|:---------------------------------------------------------------------------------------------------------------------|
|Description| How many threads can concurrently execute query statement. When <= 0, use CPU core number. |
|Type| Int32                                                               |
|Default | CPU core number                                                    |
|Effective| After restarting system |

* batch\_size

|Name| batch\_size |
|:---:|:---|
|Description| The amount of data iterate each time in server (the number of data strips, that is, the number of different timestamps.) |
|Type| Int32 |
|Default| 100000 |
|Effective|After restarting system|

### Storage Engine Configuration

* default\_ttl\_in\_ms

|    Name     | default\_ttl\_in\_ms                   |
| :---------: | :------------------------------------- |
| Description | Default ttl when each database created |
|    Type     | Long                                   |
|   Default   | Infinity                               |
|  Effective  | After restarting system                |

* memtable\_size\_threshold

|    Name     | memtable\_size\_threshold                                    |
| :---------: | :----------------------------------------------------------- |
| Description | max memtable size                                            |
|    Type     | Long                                                         |
|   Default   | 1073741824                                                   |
|  Effective  | when enable\_mem\_control is false & After restarting system |

* write\_memory\_variation\_report\_proportion

|    Name     | write\_memory\_variation\_report\_proportion                                                                 |
| :---------: | :----------------------------------------------------------------------------------------------------------- |
| Description | if memory cost of data region increased more than proportion of allocated memory for write, report to system |
|    Type     | Double                                                                                                       |
|   Default   | 0.001                                                                                                        |
|  Effective  | After restarting system                                                                                      |

* enable\_timed\_flush\_seq\_memtable

|Name| enable\_timed\_flush\_seq\_memtable |
|:---:|:---|
|Description| whether to enable timed flush sequence memtable |
|Type|Boolean|
|Default| false |
|Effective| Trigger |

* seq\_memtable\_flush\_interval\_in\_ms

|Name| seq\_memtable\_flush\_interval\_in\_ms |
|:---:|:---|
|Description| if a memTable's created time is older than current time minus this, the memtable will be flushed to disk |
|Type|int32|
|Default| 3600000 |
|Effective| Trigger |

* seq\_memtable\_flush\_check\_interval\_in\_ms

|Name| seq\_memtable\_flush\_check\_interval\_in\_ms |
|:---:|:---|
|Description| the interval to check whether sequence memtables need flushing |
|Type|int32|
|Default| 600000 |
|Effective| Trigger |

* enable\_timed\_flush\_unseq\_memtable

|Name| enable\_timed\_flush\_unseq\_memtable |
|:---:|:---|
|Description| whether to enable timed flush unsequence memtable |
|Type|Boolean|
|Default| false |
|Effective| Trigger |

* unseq\_memtable\_flush\_interval\_in\_ms

|Name| unseq\_memtable\_flush\_interval\_in\_ms |
|:---:|:---|
|Description| if a memTable's created time is older than current time minus this, the memtable will be flushed to disk |
|Type|int32|
|Default| 3600000 |
|Effective| Trigger |

* unseq\_memtable\_flush\_check\_interval\_in\_ms

|Name| unseq\_memtable\_flush\_check\_interval\_in\_ms |
|:---:|:---|
|Description| the interval to check whether unsequence memtables need flushing |
|Type|int32|
|Default| 600000 |
|Effective| Trigger |

* avg\_series\_point\_number\_threshold

|Name| avg\_series\_point\_number\_threshold                  |
|:---:|:-------------------------------------------------------|
|Description| max average number of point of each series in memtable |
|Type| int32                                                  |
|Default| 100000                                                 |
|Effective| After restarting system                                |

* flush\_thread\_count

|Name| flush\_thread\_count |
|:---:|:---|
|Description| The thread number used to perform the operation when IoTDB writes data in memory to disk. If the value is less than or equal to 0, then the number of CPU cores installed on the machine is used. The default is 0.|
|Type| int32 |
|Default| 0 |
|Effective|After restarting system|

* enable\_partial\_insert

|Name| enable\_partial\_insert |
|:---:|:---|
|Description| Whether continue to write other measurements if some measurements are failed in one insertion.|
|Type| Boolean |
|Default| true |
|Effective|After restarting system|

* insert\_multi\_tablet\_enable\_multithreading\_column\_threshold

|    Name     | insert\_multi\_tablet\_enable\_multithreading\_column\_threshold                                     |
| :---------: | :--------------------------------------------------------------------------------------------- |
| Description | When the insert plan column count reaches the specified threshold, multi-threading is enabled. |
|    Type     | int32                                                        |
|   Default   | 10                                                           |
|  Effective  | After restarting system                                      |

### Compaction Configurations

* enable\_seq\_space\_compaction

|    Name     | enable\_seq\_space\_compaction               |
| :---------: | :------------------------------------------- |
| Description | enable the compaction between sequence files |
|    Type     | Boolean                                      |
|   Default   | true                                         |
|  Effective  | After restart system                         |

* enable\_unseq\_space\_compaction

|    Name     | enable\_unseq\_space\_compaction               |
| :---------: | :--------------------------------------------- |
| Description | enable the compaction between unsequence files |
|    Type     | Boolean                                        |
|   Default   | false                                          |
|  Effective  | After restart system                           |

* enable\_cross\_space\_compaction

|    Name     | enable\_cross\_space\_compaction                                  |
| :---------: | :---------------------------------------------------------------- |
| Description | enable the compaction between sequence files and unsequence files |
|    Type     | Boolean                                                           |
|   Default   | true                                                              |
|  Effective  | After restart system                                              |

* cross\_compaction\_strategy

|    Name     | cross\_compaction\_strategy        |
| :---------: | :--------------------------------- |
| Description | strategy of cross space compaction |
|    Type     | String                             |
|   Default   | rewrite\_compaction                |
|  Effective  | After restart system               |

* inner\_compaction\_strategy

|    Name     | inner\_compaction\_strategy        |
| :---------: | :--------------------------------- |
| Description | strategy of inner space compaction |
|    Type     | String                             |
|   Default   | size\_tiered\_compaction           |
|  Effective  | After restart system               |

* compaction\_priority

|    Name     | compaction\_priority                                                                                                                                                                                                                                                                     |
| :---------: | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Description | Priority of compaction task. When it is BALANCE, system executes all types of compaction equally; when it is INNER\_CROSS, system takes precedence over executing inner space compaction task; when it is CROSS\_INNER, system takes precedence over executing cross space compaction task |
|    Type     | String                                                                                                                                                                                                                                                                                   |
|   Default   | BALANCE                                                                                                                                                                                                                                                                                  |
|  Effective  | After restart system                                                                                                                                                                                                                                                                     |

* target\_compaction\_file\_size

|    Name     | target\_compaction\_file\_size               |
| :---------: | :------------------------------------------- |
| Description | The target file is in inner space compaction |
|    Type     | Int64                                        |
|   Default   | 1073741824                                   |
|  Effective  | After restart system                         |

* target\_chunk\_size

|    Name     | target\_chunk\_size                |
| :---------: | :--------------------------------- |
| Description | The target size of compacted chunk |
|    Type     | Int64                              |
|   Default   | 1048576                            |
|  Effective  | After restart system               |

* target\_chunk\_point\_num

|Name| target\_chunk\_point\_num |
|:---:|:---|
|Description| The target point number of compacted chunk |
|Type| int32 |
|Default| 100000 |
|Effective|After restart system|

* chunk\_size\_lower\_bound\_in\_compaction

|    Name     | chunk\_size\_lower\_bound\_in\_compaction                                               |
| :---------: | :-------------------------------------------------------------------------------------- |
| Description | A source chunk will be deserialized in compaction when its size is less than this value |
|    Type     | Int64                                                                                   |
|   Default   | 128                                                                                     |
|  Effective  | After restart system                                                                    |

* chunk\_point\_num\_lower\_bound\_in\_compaction

|Name| chunk\_size\_lower\_bound\_in\_compaction |
|:---:|:---|
|Description| A source chunk will be deserialized in compaction when its point num is less than this value |
|Type| int32 |
|Default| 100 |
|Effective|After restart system|

* max\_inner\_compaction\_candidate\_file\_num

|Name| max\_inner\_compaction\_candidate\_file\_num |
|:---:|:---|
|Description| The max num of files encounter in inner space compaction |
|Type| int32 |
|Default| 30 |
|Effective|After restart system|

* max\_cross\_compaction\_file\_num

|Name| max\_cross\_compaction\_candidate\_file\_num |
|:---:|:---|
|Description| The max num of files encounter in cross space compaction |
|Type| int32 |
|Default| 1000 |
|Effective|After restart system|

* cross\_compaction\_file\_selection\_time\_budget

|Name| cross\_compaction\_file\_selection\_time\_budget |
|:---:|:---|
|Description| Time budget for cross space compaction file selection |
|Type| int32 |
|Default| 30000 |
|Effective|After restart system|

* cross\_compaction\_memory\_budget

|Name| cross\_compaction\_memory\_budget |
|:---:|:---|
|Description| Memory budget for a cross space compaction |
|Type| int32 |
|Default| 2147483648 |
|Effective|After restart system|

* compaction\_thread\_count

|Name| compaction\_thread\_count |
|:---:|:---|
|Description| thread num to execute compaction |
|Type| int32 |
|Default| 10 |
|Effective|After restart system|

* compaction\_schedule\_interval\_in\_ms

|    Name     | compaction\_schedule\_interval\_in\_ms |
| :---------: | :------------------------------------- |
| Description | interval of scheduling compaction      |
|    Type     | Int64                                  |
|   Default   | 60000                                  |
|  Effective  | After restart system                   |

* compaction\_submission\_interval\_in\_ms

|    Name     | compaction\_submission\_interval\_in\_ms |
| :---------: | :--------------------------------------- |
| Description | interval of submitting compaction task   |
|    Type     | Int64                                    |
|   Default   | 60000                                    |
|  Effective  | After restart system                     |

* compaction\_write\_throughput\_mb\_per\_sec

|Name| compaction\_write\_throughput\_mb\_per\_sec |
|:---:|:---|
|Description| The write rate of all compaction tasks in MB/s |
|Type| int32 |
|Default| 16 |
|Effective|After restart system|

### Write Ahead Log Configuration

### TsFile Configurations

* group\_size\_in\_byte

|Name|group\_size\_in\_byte|
|:---:|:---|
|Description|The data size written to the disk per time|
|Type|int32|
|Default| 134217728 |
|Effective|Trigger|

* page\_size\_in\_byte

|Name| page\_size\_in\_byte |
|:---:|:---|
|Description|The maximum size of a single page written in memory when each column in memory is written (in bytes)|
|Type|int32|
|Default| 65536 |
|Effective|Trigger|

* max\_number\_of\_points\_in\_page

|Name| max\_number\_of\_points\_in\_page |
|:---:|:---|
|Description|The maximum number of data points (timestamps - valued groups) contained in a page|
|Type|int32|
|Default| 1048576 |
|Effective|Trigger|

* max\_degree\_of\_index\_node

|Name| max\_degree\_of\_index\_node |
|:---:|:---|
|Description|The maximum degree of the metadata index tree (that is, the max number of each node's children)|
|Type|int32|
|Default| 256 |
|Effective|Only allowed to be modified in first start up|

* max\_string\_length

|Name| max\_string\_length |
|:---:|:---|
|Description|The maximum length of a single string (number of character)|
|Type|int32|
|Default| 128 |
|Effective|Trigger|

* time\_encoder

|    Name     | time\_encoder                         |
| :---------: | :------------------------------------ |
| Description | Encoding type of time column          |
|    Type     | Enum String: “TS_2DIFF”,“PLAIN”,“RLE” |
|   Default   | TS_2DIFF                              |
|  Effective  | Trigger                               |

* value\_encoder

|    Name     | value\_encoder                        |
| :---------: | :------------------------------------ |
| Description | Encoding type of value column         |
|    Type     | Enum String: “TS_2DIFF”,“PLAIN”,“RLE” |
|   Default   | PLAIN                                 |
|  Effective  | Trigger                               |

* float\_precision

|Name| float\_precision |
|:---:|:---|
|Description| The precision of the floating point number.(The number of digits after the decimal point) |
|Type|int32|
|Default| The default is 2 digits. Note: The 32-bit floating point number has a decimal precision of 7 bits, and the 64-bit floating point number has a decimal precision of 15 bits. If the setting is out of the range, it will have no practical significance. |
|Effective|Trigger|

* compressor

|    Name     | compressor                                    |
| :---------: | :-------------------------------------------- |
| Description | Data compression method                       |
|    Type     | Enum String : “UNCOMPRESSED”, “SNAPPY”, "LZ4" |
|   Default   | SNAPPY                                        |
|  Effective  | Trigger                                       |

* bloomFilterErrorRate

|    Name     | bloomFilterErrorRate                                                                                                                                                                                                                                                                                                                                                                                             |
| :---------: | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Description | The false positive rate of bloom filter in each TsFile. Bloom filter checks whether a given time series is in the tsfile before loading metadata. This can improve the performance of loading metadata and skip the tsfile that doesn't contain specified time series. If you want to learn more about its mechanism, you can refer to: [wiki page of bloom filter](https://en.wikipedia.org/wiki/Bloom_filter). |
|    Type     | float, (0, 1)                                                                                                                                                                                                                                                                                                                                                                                                    |
|   Default   | 0.05                                                                                                                                                                                                                                                                                                                                                                                                             |
|  Effective  | After restarting system                                                                                                                                                                                                                                                                                                                                                                                          |

* freq\_snr

|    Name     | freq\_snr                                        |
| :---------: | :---------------------------------------------- |
| Description | Signal-noise-ratio (SNR) of lossy FREQ encoding |
|    Type     | Double                                          |
|   Default   | 40.0                                            |
|  Effective  | Trigger                                         |

* freq\_block\_size

|Name| freq\_block\_size |
|:---:|:---|
|Description| Block size of FREQ encoding. In other words, the number of data points in a time-frequency transformation. To speed up the encoding, it is recommended to be the power of 2. |
|Type|int32|
|Default| 1024 |
|Effective|Trigger|

### Authorization Configuration

* authorizer\_provider\_class

|          Name          | authorizer\_provider\_class                             |
| :--------------------: | :------------------------------------------------------ |
|      Description       | the class name of the authorization service             |
|          Type          | String                                                  |
|        Default         | org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer |
|       Effective        | After restarting system                                 |
| Other available values | org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer    |

* openID\_url

|    Name     | openID\_url                                      |
| :---------: | :----------------------------------------------- |
| Description | the openID server if OpenIdAuthorizer is enabled |
|    Type     | String (a http url)                              |
|   Default   | no                                               |
|  Effective  | After restarting system                          |

* admin\_name

|    Name     | admin\_name                                   |
| :---------: | :-------------------------------------------- |
| Description | The username of admin                         |
|    Type     | String                                        |
|   Default   | root                                          |
|  Effective  | Only allowed to be modified in first start up |

* admin\_password

|    Name     | admin\_password                               |
| :---------: | :-------------------------------------------- |
| Description | The password of admin                         |
|    Type     | String                                        |
|   Default   | root                                          |
|  Effective  | Only allowed to be modified in first start up |

* iotdb\_server\_encrypt\_decrypt\_provider

|    Name     | iotdb\_server\_encrypt\_decrypt\_provider                      |
| :---------: | :------------------------------------------------------------- |
| Description | The Class for user password encryption                         |
|    Type     | String                                                         |
|   Default   | org.apache.iotdb.commons.security.encrypt.MessageDigestEncrypt |
|  Effective  | Only allowed to be modified in first start up                  |

* iotdb\_server\_encrypt\_decrypt\_provider\_parameter

|    Name     | iotdb\_server\_encrypt\_decrypt\_provider\_parameter             |
| :---------: | :--------------------------------------------------------------- |
| Description | Parameters used to initialize the user password encryption class |
|    Type     | String                                                           |
|   Default   | 空                                                               |
|  Effective  | After restarting system                                          |

* author\_cache\_size

|    Name     | author\_cache\_size         |
| :---------: | :-------------------------- |
| Description | Cache size of user and role |
|    Type     | int32                       |
|   Default   | 1000                        |
|  Effective  | After restarting system     |

* author\_cache\_expire\_time

|    Name     | author\_cache\_expire\_time                       |
| :---------: | :------------------------------------------------ |
| Description | Cache expire time of user and role, Unit: minutes |
|    Type     | int32                                             |
|   Default   | 30                                                |
|  Effective  | After restarting system                           |

### UDF Configuration

* udf\_initial\_byte\_array\_length\_for\_memory\_control

|    Name     | udf\_initial\_byte\_array\_length\_for\_memory\_control                                                                                                          |
| :---------: | :--------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Description | Used to estimate the memory usage of text fields in a UDF query. It is recommended to set this value to be slightly larger than the average length of all texts. |
|    Type     | int32                                                        |
|   Default   | 48                                                           |
|  Effective  | After restarting system                                      |

* udf\_memory\_budget\_in\_mb

|    Name     | udf\_memory\_budget\_in\_mb                                                                                |
| :---------: | :--------------------------------------------------------------------------------------------------------- |
| Description | How much memory may be used in ONE UDF query (in MB). The upper limit is 20% of allocated memory for read. |
|    Type     | Float                                                                                                      |
|   Default   | 30.0                                                                                                       |
|  Effective  | After restarting system                                                                                    |

* udf\_reader\_transformer\_collector\_memory\_proportion

|    Name     | udf\_reader\_transformer\_collector\_memory\_proportion                                                                             |
| :---------: | :---------------------------------------------------------------------------------------------------------------------------------- |
| Description | UDF memory allocation ratio for reader, transformer and collector. The parameter form is a : b : c, where a, b, and c are integers. |
|    Type     | String                                                                                                                              |
|   Default   | 1:1:1                                                                                                                               |
|  Effective  | After restarting system                                                                                                             |

* udf\_root\_dir

|    Name     | udf\_root\_dir            |
| :---------: | :------------------------ |
| Description | Root directory of UDF     |
|    Type     | String                    |
|   Default   | ext/udf(Windows:ext\\udf) |
|  Effective  | After restarting system   |

* udf\_lib\_dir

|    Name     | udf\_lib\_dir                |
| :---------: | :--------------------------- |
| Description | UDF log and jar file dir     |
|    Type     | String                       |
|   Default   | ext/udf（Windows：ext\\udf） |
|  Effective  | After restarting system      |

### Trigger Configuration

- concurrent\_window\_evaluation\_thread

|    Name     | concurrent\_window\_evaluation\_thread                                                          |
| :---------: | :------------------------------------------------------------------------------------------- |
| Description | How many threads can be used for evaluating sliding windows. When <= 0, use CPU core number. |
|    Type     | int32                                                        |
|   Default   | The number of CPU cores                                      |
|  Effective  | After restarting system                                      |

- max\_pending\_window\_evaluation\_tasks

|    Name     | max\_pending\_window\_evaluation\_tasks                                                                                 |
| :---------: | :------------------------------------------------------------------------------------------------------------------ |
| Description | Maximum number of window evaluation tasks that can be pending for execution. When <= 0, the value is 64 by default. |
|    Type     | int32                                                        |
|   Default   | 64                                                           |
|  Effective  | After restarting system                                      |

### SELECT-INTO

* select\_into\_insert\_tablet\_plan\_row\_limit

|    Name     | select\_into\_insert\_tablet\_plan\_row\_limit                                                                                            |
| :---------: | :---------------------------------------------------------------------------------------------------------------------------------- |
| Description | The maximum number of rows that can be processed in insert-tablet-plan when executing select-into statements. When <= 0, use 10000. |
|    Type     | int32                                                        |
|   Default   | 10000                                                        |
|  Effective  | Trigger                                                      |

### Continuous Query

* continuous\_query\_execution\_thread

|    Name     | continuous\_query\_execution\_thread                             |
| :---------: | :------------------------------------------------------------ |
| Description | How many threads will be set up to perform continuous queries |
|    Type     | int32                                                        |
|   Default   | max(1, the / 2)                                              |
|  Effective  | After restarting system                                      |

* max\_pending\_continuous\_query\_tasks

|    Name     | max\_pending\_continuous\_query\_tasks                                         |
| :---------: | :------------------------------------------------------------------------- |
| Description | Maximum number of continuous query tasks that can be pending for execution |
|    Type     | int32                                                        |
|   Default   | 64                                                           |
|  Effective  | After restarting system                                      |

* continuous\_query\_min\_every\_interval

|    Name     | continuous\_query\_min\_every\_interval                 |
| :---------: | :-------------------------------------------------- |
| Description | Minimum every interval to perform continuous query. |
|    Type     | duration                                            |
|   Default   | 1s                                                  |
|  Effective  | After restarting system                             |

### PIPE Configuration

### RatisConsensus Configuration

### Procedure Configuration

* procedure\_core\_worker\_thread\_count

|    Name     | procedure\_core\_worker\_thread\_count |
| :---------: | :--------------------------------- |
| Description | The number of worker thread count  |
|    Type     | int32                                |
|   Default   | 4                                  |
|  Effective  | After restarting system            |

* procedure\_completed\_clean\_interval

|    Name     | procedure\_completed\_clean\_interval                   |
| :---------: | :--------------------------------------------------- |
| Description | Time interval of completed procedure cleaner work in |
|    Type     | int32                                                  |
|    Unit     | second                                               |
|   Default   | 30                                                   |
|  Effective  | After restarting system                              |

* procedure\_completed\_evict\_ttl

|    Name     | procedure\_completed\_evict\_ttl  |
| :---------: | :----------------------------- |
| Description | The ttl of completed procedure |
|    Type     | int32                            |
|    Unit     | second                         |
|   Default   | 800                            |
|  Effective  | After restarting system        |


### MQTT Broker Configuration
* enable\_mqtt\_service

|    Name     | enable\_mqtt\_service。              |
|:-----------:|:------------------------------------|
| Description | Whether to enable the MQTT service  |
|    Type     | Boolean                             |
|   Default   | False                               |
|  Effective  | Trigger                             |

* mqtt\_host

|    Name     | mqtt\_host                                   |
|:-----------:|:---------------------------------------------|
| Description | The host to which the MQTT service is bound  |
|    Type     | String                                       |
|   Default   | 0.0.0.0                                      |
|   Effective    | Trigger                                      |

* mqtt\_port

|    Name     | mqtt\_port                                  |
|:-----------:|:--------------------------------------------|
| Description | The port to which the MQTT service is bound |
|    Type     | int32                                       |
|   Default   | 1883                                        |
|   Effective    | Trigger                                     |

* mqtt\_handler\_pool\_size

|Name| mqtt\_handler\_pool\_size                                   |
|:---:|:------------------------------------------------------------|
|Description| The size of the handler pool used to process MQTT messages  |
|Type| int32                                                       |
|Default| 1                                                           |
|Effective| Trigger                                                     |

* mqtt\_payload\_formatter

|    Name     | mqtt\_payload\_formatter       |
|:-----------:|:-------------------------------|
| Description | MQTT message payload formatter |
|    Type     | String                         |
|   Default   | JSON                           |
|   Effective    | Trigger                        |

* mqtt\_max\_message\_size

|  Name  | mqtt\_max\_message\_size                 |
|:------:|:-----------------------------------------|
|   Description   | Maximum length of MQTT message in bytes  |
|   Type   | int32                                    |
|  Default   | 1048576                                  |
| Effective | Trigger                                  |


### REST Service Configuration

* enable\_rest\_service

|Name| enable\_rest\_service                   |
|:---:|:--------------------------------------|
|Description| Whether to enable the Rest service    |
|Type| Boolean                               |
|Default| false                                 |
|Effective| After restarting system               |

* rest\_service\_port

|Name| rest\_service\_port |
|:---:|:------------------|
|Description| The Rest service listens to the port number      |
|Type| int32             |
|Default| 18080             |
|Effective| After restarting system              |

* enable\_swagger

|Name| enable\_swagger         |
|:---:|:-----------------------|
|Description| Whether to enable swagger to display rest interface information |
|Type| Boolean                |
|Default| false                  |
|Effective| After restarting system                   |

* rest\_query\_default\_row\_size\_limit

|Name| rest\_query\_default\_row\_size\_limit                                                         |
|:---:|:------------------------------------------------------------------------------------------|
|Description| The maximum number of rows in a result set that can be returned by a query                |
|Type| int32                                                                                     |
|Default| 10000                                                                                     |
|Effective| After restarting system                                                                   |

* cache\_expire

|Name| cache\_expire                                            |
|:---:|:--------------------------------------------------------|
|Description| Expiration time for caching customer login information  |
|Type| int32                                                   |
|Default| 28800                                                   |
|Effective| After restarting system                                 |

* cache\_max\_num

|Name| cache\_max\_num |
|:---:|:--------------|
|Description| The maximum number of users stored in the cache  |
|Type| int32         |
|Default| 100           |
|Effective| After restarting system          |

* cache\_init\_num

|Name| cache\_init\_num |
|:---:|:---------------|
|Description| Initial cache capacity        |
|Type| int32          |
|Default| 10             |
|Effective| After restarting system           |

* enable\_https

|Name| cache\_init\_num           |
|:---:|:-------------------------|
|Description| REST Service Specifies whether to enable SSL configuration |
|Type| Boolean                  |
|Default| false                    |
|Effective| After restarting system                     |

* key\_store\_path

|Name| key\_store\_path |
|:---:|:---------------|
|Description| keyStore path (optional)  |
|Type| String         |
|Default| ""          |
|Effective| After restarting system           |

* key\_store\_pwd

|Name| key\_store\_pwd |
|:---:|:---------------|
|Description| keyStore Password (optional) |
|Type| String         |
|Default| ""          |
|Effective| After restarting system           |

* trust\_store\_path

|Name| trust\_store\_path |
|:---:|:---------------|
|Description| keyStore Password (optional) |
|Type| String         |
|Default| ""          |
|Effective| After restarting system           |

* trust\_store\_pwd

|Name| trust\_store\_pwd                  |
|:---:|:---------------------------------|
|Description| trustStore Password (Optional)   |
|Type| String                           |
|Default| ""                               |
|Effective| After restarting system          |

* idle\_timeout

|Name| idle\_timeout  |
|:---:|:--------------|
|Description| SSL timeout duration, expressed in seconds |
|Type| int32         |
|Default| 5000          |
|Effective| After restarting system          |

### InfluxDB RPC Service Configuration

* enable\_influxdb\_rpc\_service

|    Name     | enable\_influxdb\_rpc\_service            |
| :---------: | :------------------------------------- |
| Description | Whether to enable InfluxDB RPC service |
|    Type     | Boolean                                |
|   Default   | true                                   |
|  Effective  | After restarting system                |

* influxdb\_rpc\_port

|    Name     | influxdb\_rpc\_port                     |
| :---------: | :------------------------------------ |
| Description | The port used by InfluxDB RPC service |
|    Type     | int32                                 |
|   Default   | 8086                                  |
|  Effective  | After restarting system               |

