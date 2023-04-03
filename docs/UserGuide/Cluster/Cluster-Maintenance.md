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

# Cluster Information Query Command

## Show Variables

Currently, IoTDB supports showing key parameters of the cluster:
```
SHOW VARIABLES
```

Eg:
```
IoTDB> show variables
+----------------------------------+-----------------------------------------------------------------+
|                          Variable|                                                            Value|
+----------------------------------+-----------------------------------------------------------------+
|                       ClusterName|                                                   defaultCluster|
|             DataReplicationFactor|                                                                1|
|           SchemaReplicationFactor|                                                                1|
|  DataRegionConsensusProtocolClass|                      org.apache.iotdb.consensus.iot.IoTConsensus|
|SchemaRegionConsensusProtocolClass|                  org.apache.iotdb.consensus.ratis.RatisConsensus|
|  ConfigNodeConsensusProtocolClass|                  org.apache.iotdb.consensus.ratis.RatisConsensus|
|             TimePartitionInterval|                                                        604800000|
|                    DefaultTTL(ms)|                                              9223372036854775807|
|              ReadConsistencyLevel|                                                           strong|
|           SchemaRegionPerDataNode|                                                              1.0|
|            DataRegionPerProcessor|                                                              1.0|
|           LeastDataRegionGroupNum|                                                                5|
|                     SeriesSlotNum|                                                            10000|
|           SeriesSlotExecutorClass|org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor|
|         DiskSpaceWarningThreshold|                                                             0.05|
+----------------------------------+-----------------------------------------------------------------+
Total line number = 15
It costs 0.225s
```

**Notice:** Ensure that all key parameters displayed in this SQL are consist on each node in the same cluster

## Show ConfigNode information

Currently, IoTDB supports showing ConfigNode information by the following SQL:
```
SHOW CONFIGNODES
```

Eg:
```
IoTDB> show confignodes
+------+-------+---------------+------------+--------+
|NodeID| Status|InternalAddress|InternalPort|    Role|
+------+-------+---------------+------------+--------+
|     0|Running|      127.0.0.1|       10710|  Leader|
|     1|Running|      127.0.0.1|       10711|Follower|
|     2|Running|      127.0.0.1|       10712|Follower|
+------+-------+---------------+------------+--------+
Total line number = 3
It costs 0.030s
```

### ConfigNode status definition
The ConfigNode statuses are defined as follows:

- **Running**: The ConfigNode is running properly.
- **Unknown**: The ConfigNode doesn't report heartbeat properly.

## Show DataNode information

Currently, IoTDB supports showing DataNode information by the following SQL:
```
SHOW DATANODES
```

Eg:
```
IoTDB> create timeseries root.sg.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.sg.d2.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.ln.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> show datanodes
+------+-------+----------+-------+-------------+---------------+
|NodeID| Status|RpcAddress|RpcPort|DataRegionNum|SchemaRegionNum|
+------+-------+----------+-------+-------------+---------------+
|     1|Running| 127.0.0.1|   6667|            0|              1|
|     2|Running| 127.0.0.1|   6668|            0|              1|
+------+-------+----------+-------+-------------+---------------+
Total line number = 2
It costs 0.007s

IoTDB> insert into root.ln.d1(timestamp,s1) values(1,true)
Msg: The statement is executed successfully.
IoTDB> show datanodes
+------+-------+----------+-------+-------------+---------------+
|NodeID| Status|RpcAddress|RpcPort|DataRegionNum|SchemaRegionNum|
+------+-------+----------+-------+-------------+---------------+
|     1|Running| 127.0.0.1|   6667|            1|              1|
|     2|Running| 127.0.0.1|   6668|            0|              1|
+------+-------+----------+-------+-------------+---------------+
Total line number = 2
It costs 0.006s
```

### DataNode status definition
The state machine of DataNode is shown in the figure below:
<img style="width:100%; max-width:500px; max-height:500px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Cluster/DataNode-StateMachine-EN.jpg?raw=true">

The DataNode statuses are defined as follows:

- **Running**: The DataNode is running properly and is readable and writable.
- **Unknown**: The DataNode doesn't report heartbeat properly, the ConfigNode considers the DataNode as unreadable and un-writable.
- **Removing**: The DataNode is being removed from the cluster and is unreadable and un-writable.
- **ReadOnly**: The remaining disk space of DataNode is lower than disk_warning_threshold(default is 5%), the DataNode is readable but un-writable and cannot synchronize data.

## Show all Node information

Currently, IoTDB supports show the information of all Nodes by the following SQL:
```
SHOW CLUSTER
```

Eg:
```
IoTDB> show cluster
+------+----------+-------+---------------+------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|
+------+----------+-------+---------------+------------+
|     0|ConfigNode|Running|      127.0.0.1|       10710|
|     1|ConfigNode|Running|      127.0.0.1|       10711|
|     2|ConfigNode|Running|      127.0.0.1|       10712|
|     3|  DataNode|Running|      127.0.0.1|       10730|
|     4|  DataNode|Running|      127.0.0.1|       10731|
|     5|  DataNode|Running|      127.0.0.1|       10732|
+------+----------+-------+---------------+------------+
Total line number = 6
It costs 0.011s
```

After a node is stopped, its status will change, as shown below:
```
IoTDB> show cluster
+------+----------+-------+---------------+------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|
+------+----------+-------+---------------+------------+
|     0|ConfigNode|Running|      127.0.0.1|       10710|
|     1|ConfigNode|Unknown|      127.0.0.1|       10711|
|     2|ConfigNode|Running|      127.0.0.1|       10712|
|     3|  DataNode|Running|      127.0.0.1|       10730|
|     4|  DataNode|Running|      127.0.0.1|       10731|
|     5|  DataNode|Running|      127.0.0.1|       10732|
+------+----------+-------+---------------+------------+
Total line number = 6
It costs 0.012s
```

Show the details of all nodes:
```
SHOW CLUSTER DETAILS
```

Eg:
```
IoTDB> show cluster details
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|ConfigConsensusPort|RpcAddress|RpcPort|MppPort|SchemaConsensusPort|DataConsensusPort|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|     0|ConfigNode|Running|      127.0.0.1|       10710|              10720|          |       |       |                   |                 |
|     1|ConfigNode|Running|      127.0.0.1|       10711|              10721|          |       |       |                   |                 |
|     2|ConfigNode|Running|      127.0.0.1|       10712|              10722|          |       |       |                   |                 |
|     3|  DataNode|Running|      127.0.0.1|       10730|                   | 127.0.0.1|   6667|  10740|              10750|            10760|
|     4|  DataNode|Running|      127.0.0.1|       10731|                   | 127.0.0.1|   6668|  10741|              10751|            10761|
|     5|  DataNode|Running|      127.0.0.1|       10732|                   | 127.0.0.1|   6669|  10742|              10752|            10762|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
Total line number = 6
It costs 0.340s
```

## Show Region information

The cluster uses a SchemaRegion/DataRegion as a unit for schema/data replication and data management. 
The Region status and distribution is helpful for system operation and maintenance testing, as shown in the following scenarios:

- Check which DataNodes are allocated to each Region in the cluster and whether they are balanced.
- Check the partitions allocated to each Region in the cluster and whether they are balanced.
- Check which DataNodes are allocated by the leaders of each RegionGroup in the cluster and whether they are balanced.

Currently, IoTDB supports show Region information by the following SQL:

- `SHOW REGIONS`: Show distribution of all Regions.
- `SHOW SCHEMA REGIONS`: Show distribution of all SchemaRegions.
- `SHOW DATA REGIONS`: Show distribution of all DataRegions.
- `SHOW (DATA|SCHEMA)? REGIONS OF DATABASE <sg1,sg2,...>`: Show Region distribution of specified StorageGroups.

Show distribution of all Regions:
```
IoTDB> show regions
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       0|  DataRegion|Running|root.sg1|          1|        1|         1| 127.0.0.1|   6667|Follower|
|       0|  DataRegion|Running|root.sg1|          1|        1|         2| 127.0.0.1|   6668|  Leader|
|       0|  DataRegion|Running|root.sg1|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         2| 127.0.0.1|   6668|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         3| 127.0.0.1|   6669|  Leader|
|       2|  DataRegion|Running|root.sg2|          1|        1|         1| 127.0.0.1|   6667|  Leader|
|       2|  DataRegion|Running|root.sg2|          1|        1|         2| 127.0.0.1|   6668|Follower|
|       2|  DataRegion|Running|root.sg2|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         2| 127.0.0.1|   6668|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         3| 127.0.0.1|   6669|Follower|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 12
It costs 0.165s
```

Show the distribution of SchemaRegions or DataRegions:
```
IoTDB> show data regions
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       0|  DataRegion|Running|root.sg1|          1|        1|         1| 127.0.0.1|   6667|Follower|
|       0|  DataRegion|Running|root.sg1|          1|        1|         2| 127.0.0.1|   6668|  Leader|
|       0|  DataRegion|Running|root.sg1|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       2|  DataRegion|Running|root.sg2|          1|        1|         1| 127.0.0.1|   6667|  Leader|
|       2|  DataRegion|Running|root.sg2|          1|        1|         2| 127.0.0.1|   6668|Follower|
|       2|  DataRegion|Running|root.sg2|          1|        1|         3| 127.0.0.1|   6669|Follower|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 6
It costs 0.011s

IoTDB> show schema regions
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       1|SchemaRegion|Running|root.sg1|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         2| 127.0.0.1|   6668|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         3| 127.0.0.1|   6669|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         2| 127.0.0.1|   6668|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         3| 127.0.0.1|   6669|Follower|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 6
It costs 0.012s
```

Show Region distribution of specified StorageGroups:
```
IoTDB> show regions of database root.sg1
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+-- -----+-----------+---------+----------+----------+-------+--------+
|       0|  DataRegion|Running|root.sg1|          1|        1|         1| 127.0.0.1|   6667|Follower|
|       0|  DataRegion|Running|root.sg1|          1|        1|         2| 127.0.0.1|   6668|  Leader|
|       0|  DataRegion|Running|root.sg1|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         2| 127.0.0.1|   6668|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         3| 127.0.0.1|   6669|  Leader|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 6
It costs 0.007s

IoTDB> show regions of database root.sg1, root.sg2
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       0|  DataRegion|Running|root.sg1|          1|        1|         1| 127.0.0.1|   6667|Follower|
|       0|  DataRegion|Running|root.sg1|          1|        1|         2| 127.0.0.1|   6668|  Leader|
|       0|  DataRegion|Running|root.sg1|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         2| 127.0.0.1|   6668|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         3| 127.0.0.1|   6669|  Leader|
|       2|  DataRegion|Running|root.sg2|          1|        1|         1| 127.0.0.1|   6667|  Leader|
|       2|  DataRegion|Running|root.sg2|          1|        1|         2| 127.0.0.1|   6668|Follower|
|       2|  DataRegion|Running|root.sg2|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         2| 127.0.0.1|   6668|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         3| 127.0.0.1|   6669|Follower|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 12
It costs 0.009s

IoTDB> show data regions of database root.sg1, root.sg2
+--------+----------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|      Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+----------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       0|DataRegion|Running|root.sg1|          1|        1|         1| 127.0.0.1|   6667|Follower|
|       0|DataRegion|Running|root.sg1|          1|        1|         2| 127.0.0.1|   6668|  Leader|
|       0|DataRegion|Running|root.sg1|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       2|DataRegion|Running|root.sg2|          1|        1|         1| 127.0.0.1|   6667|  Leader|
|       2|DataRegion|Running|root.sg2|          1|        1|         2| 127.0.0.1|   6668|Follower|
|       2|DataRegion|Running|root.sg2|          1|        1|         3| 127.0.0.1|   6669|Follower|
+--------+----------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 6
It costs 0.007s

IoTDB> show schema regions of database root.sg1, root.sg2
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       1|SchemaRegion|Running|root.sg1|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         2| 127.0.0.1|   6668|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         3| 127.0.0.1|   6669|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         2| 127.0.0.1|   6668|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         3| 127.0.0.1|   6669|Follower|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 6
It costs 0.009s
```

### Region status definition
Region inherits the status of the DataNode where the Region resides. And Region states are defined as follows:

- **Running**: The DataNode where the Region resides is running properly, the Region is readable and writable.
- **Unknown**: The DataNode where the Region resides doesn't report heartbeat properly, the ConfigNode considers the Region is unreadable and un-writable.
- **Removing**: The DataNode where the Region resides is being removed from the cluster, the Region is unreadable and un-writable.
- **ReadOnly**: The available disk space of the DataNode where the Region resides is lower than the disk_warning_threshold(5% by default). The Region is readable but un-writable and cannot synchronize data.

## Show cluster slots information

The cluster uses partitions for schema and data arrangement, the partition is defined as follows:

- `SchemaPartition`: series slot
- `DataPartition`: <series slot, time slot>

The cluster slots information can be shown by the following SQLs:

### Show the DataRegion where a DataPartition resides

Show the DataRegion where a DataPartition(or all DataPartitions under a same series slot) resides:
- `SHOW DATA REGIONID OF root.sg WHERE SERIESSLOTID=s0 (AND TIMESLOTID=t0)`

Eg:
```
IoTDB> show data regionid of root.sg where seriesslotid=5286 and timeslotid=0
+--------+
|RegionId|
+--------+
|       1|
+--------+
Total line number = 1
It costs 0.006s

IoTDB> show data regionid of root.sg where seriesslotid=5286
+--------+
|RegionId|
+--------+
|       1|
|       2|
+--------+
Total line number = 2
It costs 0.006s
```

### Show the SchemaRegion where a SchemaPartition resides

Show the SchemaRegion where a SchemaPartition resides:
- `SHOW SCHEMA REGIONID OF root.sg WHERE SERIESSLOTID=s0`

Eg:
```
IoTDB> show schema regionid of root.sg where seriesslotid=5286
+--------+
|RegionId|
+--------+
|       0|
+--------+
Total line number = 1
It costs 0.007s
```

### Show time slots of a series slot

Show the time slots under a particular series slot.
- `SHOW TIMESLOTID OF root.sg WHERE SERIESLOTID=s0 (AND STARTTIME=t1) (AND ENDTIME=t2)`

Eg:
```
IoTDB> show timeslotid of root.sg where seriesslotid=5286
+----------+
|TimeSlotId|
+----------+
|         0|
|      1000|
+----------+
Total line number = 1
It costs 0.007s
```

### Show Database's series slots

Show the data/schema/all series slots related to a database:
- `SHOW (DATA|SCHEMA)? SERIESSLOTID OF root.sg`

Eg:
```
IoTDB> show data seriesslotid of root.sg
+------------+
|SeriesSlotId|
+------------+
|        5286|
+------------+
Total line number = 1
It costs 0.007s

IoTDB> show schema seriesslotid of root.sg
+------------+
|SeriesSlotId|
+------------+
|        5286|
+------------+
Total line number = 1
It costs 0.006s

IoTDB> show seriesslotid of root.sg
+------------+
|SeriesSlotId|
+------------+
|        5286|
+------------+
Total line number = 1
It costs 0.006s
```