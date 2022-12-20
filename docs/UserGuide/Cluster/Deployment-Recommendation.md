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

# IoTDB Deployment Recommendation
## Backgrounds

System Abilities
- Performance: writing and reading performance, compression ratio
- Extensibility: system has the ability to manage data with multiple nodes, and is essentially that data can be managed by partitions
- High availability(HA): system has the ability to tolerate the nodes disconnected, and is essentially that the data has replicas
- Consistency：when data is with multiple copies, whether the replicas are consistent, and is essentially that the system treats the whole database as a single node

Abbreviations
- C: ConfigNode
- D: DataNode
- nCmD：cluster with n ConfigNodes and m DataNodes

## Deployment mode

|                  mode                   | Performance    | Extensibility | HA     | Consistency |
|:---------------------------------------:|:---------------|:--------------|:-------|:------------|
|       Lightweight standalone mode       | Extremely High | None          | None   | High        |
|   Scalable standalone mode (default)    | High           | High          | Medium | High        |
|      High performance cluster mode      | High           | High          | High   | Medium      |
|     Strong consistency cluster mode     | Medium         | High          | High   | High        | 


|                 Config                 | Lightweight standalone mode | Scalable single node mode | High performance mode | strong consistency cluster mode |
|:--------------------------------------:|:----------------------------|:--------------------------|:----------------------|:--------------------------------|
|           ConfigNode number            | 1                           | ≥1 (odd number)           | ≥1 (odd number)       | ≥1 (odd number)                 |
|            DataNode number             | 1                           | ≥1                        | ≥3                    | ≥3                              |
|       schema_replication_factor        | 1                           | 1                         | 3                     | 3                               |
|        data_replication_factor         | 1                           | 1                         | 2                     | 3                               |
|  config_node_consensus_protocol_class  | Simple                      | Ratis                     | Ratis                 | Ratis                           |
| schema_region_consensus_protocol_class | Simple                      | Ratis                     | Ratis                 | Ratis                           |
|  data_region_consensus_protocol_class  | Simple                      | IoT                       | IoT                   | Ratis                           |


## Deployment Recommendation

### Upgrade from v0.13 to v1.0

Scenario:
Already has some data under v0.13, hope to upgrade to v1.0.

Options:
1. Upgrade to 1C1D standalone mode, allocate 2GB memory to ConfigNode, allocate same memory size with v0.13 to DataNode.
2. Upgrade to 3C3D cluster mode, allocate 2GB memory to ConfigNode, allocate same memory size with v0.13 to DataNode.

Configuration modification:

- Do not point v1.0 data directory to v0.13 data directory
- region_group_extension_strategy=COSTOM
- data_region_group_per_database
    - for 3C3D cluster mode: Cluster CPU total core num / data_replication_factor
    - for 1C1D standalone mode: use virtual_storage_group_num in v0.13

Data migration:
After modifying the configuration, use load-tsfile tool to load the TsFiles of v0.13 to v1.0.

### Use v1.0 directly

**Recommend to use 1 Database only**

#### Memory estimation

##### Use active series number to estimate memory size

Cluster DataNode total heap size(GB) = active series number / 100000 * data_replication_factor

Heap size of each DataNode (GB) = Cluster DataNode total heap size / DataNode number

> Example: use 3C3D to manage 1 million timeseries, use 3 data replicas
> - Cluster DataNode total heap size: 1,000,000 / 100,000 * 3 = 30G
> - 每Heap size of each DataNode: 30 / 3 = 10G

##### Use total series number to estimate memory size

Cluster DataNode total heap size（B） = 20 * (180 + 2 * average character num of the series full path) * total series number * schema_replication_factor

Heap size of each DataNode = Cluster DataNode total heap size / DataNode number

> Example: use 3C3D to manage 1 million timeseries, use 3 schema replicas, series name such as root.sg_1.d_10.s_100(20 chars)
> - Cluster DataNode total heap size: 20 * (180 + 2 * 20) * 1,000,000 * 3 = 13.2 GB
> - Heap size of each DataNode: 13.2 GB / 3 = 4.4 GB

#### Disk estimation

IoTDB storage size = data storage size + schema storage size + temp storage size

##### Data storage size

Series number * Sampling frequency * Data point size * Storage duration * data_replication_factor /  10 (compression ratio)

| Data Type \ Data point size | Timestamp (Byte) | Value (Byte) | Total (Byte) |
|:---------------------------:|:-----------------|:-------------|:-------------|
|           Boolean           | 8                | 1            | 9            |
|        INT32 / FLOAT        | 8                | 4            | 12           |
|       INT64）/ DOUBLE        | 8                | 8            | 16           |
|            TEXT             | 8                | Assuming a   | 8+a          | 


> Example: 1000 devices, 100 sensors for one device, 100,000 series total, INT32 data type, 1Hz sampling frequency, 1 year storage duration, 3 replicas, compression ratio is 10
> Data storage size = 1000 * 100 * 12 * 86400 * 365 * 3 / 10 = 11T

##### Schema storage size

One series uses the path character byte size + 20 bytes.
If the series has tag, add the tag character byte size.

##### Temp storage size

Temp storage size = WAL storage size  + Consensus storage size + Compaction temp storage size

1. WAL

max wal storage size = memtable memory size ÷ wal_min_effective_info_ratio
- memtable memory size is decided by storage_query_schema_consensus_free_memory_proportion, storage_engine_memory_proportion and write_memory_proportion
- wal_min_effective_info_ratio is decided by wal_min_effective_info_ratio configuration

> Example: allocate 16G memory for DataNode, config is as below:
>  storage_query_schema_consensus_free_memory_proportion=3:3:1:1:2
>  storage_engine_memory_proportion=8:2
>  write_memory_proportion=19:1
>  wal_min_effective_info_ratio=0.1
>  max wal storage size = 16 * (3 / 10) * (8 / 10) * (19 / 20)  ÷ 0.1 = 36.48G

2. Consensus

Ratis consensus

When using ratis consensus protocol, we need extra storage for Raft Log, which will be deleted after the state machine takes snapshot.
We can adjust `trigger_snapshot_threshold` to control the maximum Raft Log disk usage.


Raft Log disk size in each Region = average * trigger_snapshot_threshold

The total Raft Log storage space is proportional to the data replica number

> Example: DataRegion, 20kB data for one request, data_region_trigger_snapshot_threshold = 400,000, then max Raft Log disk size = 20K * 400,000 = 8G.
Raft Log increases from 0 to 8GB, and then turns to 0 after snapshot. Average size will be 4GB.
When replica number is 3, max Raft log size will be 3 * 8G = 24G.
 
What's more, we can configure data_region_ratis_log_max_size to limit max log size of a single DataRegion.
By default, data_region_ratis_log_max_size=20G, which guarantees that Raft Log size would not exceed 20G.

3. Compaction

- Inner space compaction
  Disk space for temporary files = Total Disk space of origin files

  > Example: 10 origin files, 100MB for each file
  > Disk space for temporary files = 10 * 100 = 1000M


- Outer space compaction
  The overlap of out-of-order data = overlapped data amount  / total out-of-order data amount

  Disk space for temporary file = Total ordered Disk space of origin files + Total out-of-order disk space of origin files *（1 - overlap）
  > Example: 10 ordered files, 10 out-of-order files, 100M for each ordered file, 50M for each out-of-order file, half of data is overlapped with sequence file
  > The overlap of out-of-order data = 25M/50M * 100% = 50%
  > Disk space for temporary files = 10 * 100 + 10 * 50 * 50% = 1250M


