<!--

```
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
```

-->

## Basic Concepts of IoTDB Cluster

Apache IoTDB Cluster contains two types of nodes: ConfigNode and DataNode, each is a process that could be deployed independently.

An illustration of the cluster architecture：

<img style="width:100%; max-width:500px; max-height:400px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Cluster/Architecture.png?raw=true">

ConfigNode is the control node of the cluster, which manages the cluster's node status, partition information, etc. All ConfigNodes in the cluster form a highly available group, which is fully replicated.

Notice：The replication factor of ConfigNode is all ConfigNodes that has joined the Cluster. Over half of the ConfigNodes is Running could the cluster work.

DataNode stores the data and schema of the cluster, which manages multiple data regions and schema regions. Data is a time-value pair, and schema is the path and data type of each time series.

Client could only connect to the DataNode for operation.

### Concepts

| Concept           | Type                             | Description                                                                                                                                 |
|:------------------|:---------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------|
| ConfigNode        | node role                        | Configuration node, which manages cluster node information and partition information, monitors cluster status and controls load balancing   |
| DataNode          | node role                        | Data node, which manages data and meta data                                                                                                 |
| Database          | meta data                        | Database, data are isolated physically from different databases                                                                             |
| DeviceId          | device id                        | The full path from root to the penultimate level in the metadata tree represents a device id                                                |
| SeriesSlot        | schema partition                 | Each database contains many SeriesSlots, the partition key being DeviceId                                                                   |
| SchemaRegion      | schema region                    | A collection of multiple SeriesSlots                                                                                                        |
| SchemaRegionGroup | logical concept                  | The number of SchemaRegions contained in group is the number of schema replicas, it manages the same schema data, and back up each other    |
| SeriesTimeSlot    | data partition                   | The data of a time interval of SeriesSlot, a SeriesSlot contains multiple SeriesTimeSlots, the partition key being timestamp                |
| DataRegion        | data region                      | A collection of multiple SeriesTimeSlots                                                                                                    |
| DataRegionGroup   | logical concept                  | The number of DataRegions contained in group is the number of data replicas, it manages the same data, and back up each other               |

## Characteristics of Cluster

* Native Cluster Architecture
    * All modules are designed for cluster.
    * Standalone is a special form of Cluster.
* High Scalability
    * Support adding nodes in a few seconds without data migration.
* Massive Parallel Processing Architecture
    * Adopt the MPP architecture and volcano module for data processing, which have high extensibility.
* Configurable Consensus Protocol
    * We could adopt different consensus protocol for data replicas and schema replicas.
* Extensible Partition Strategy
    * The cluster adopts the lookup table for data and schema partitions, which is flexible to extend.
* Built-in Metric Framework
    * Monitor the status of each node in cluster.

## Partitioning Strategy

The partitioning strategy partitions data and schema into different Regions, and allocates Regions to different DataNodes.

It is recommended to set 1 database, and the cluster will dynamically allocate resources according to the number of nodes and cores.

The database contains multiple SchemaRegions and DataRegions, which are managed by DataNodes.

* Schema partition strategy 
    * For a time series schema, the ConfigNode maps the device ID (full path from root to the penultimate tier node) into a SeriesSlot and allocate this SeriesSlot to a SchemaRegionGroup.
* Data partition strategy
    * For a time series data point, the ConfigNode will map to a SeriesSlot according to the DeviceId, and then map it to a SeriesTimeSlot according to the timestamp, and allocate this SeriesTimeSlot to a DataRegionGroup.
  
IoTDB uses a slot-based partitioning strategy, so the size of the partition information is controllable and does not grow infinitely with the number of time series or devices.

Regions will be allocated to different DataNodes to avoid single point of failure, and the load balance of different DataNodes will be ensured when Regions are allocated.

## Replication Strategy

The replication strategy replicates data in multiple replicas, which are copies of each other. Multiple copies can provide high-availability services together and tolerate the failure of some copies.

A region is the basic unit of replication. Multiple replicas of a region construct a high-availability RegionGroup.

* Replication and consensus
  * ConfigNode Group: Consisting of all ConfigNodes.
  * SchemaRegionGroup: The cluster has multiple SchemaRegionGroups, and each SchemaRegionGroup has multiple SchemaRegions with the same id.
  * DataRegionGroup: The cluster has multiple DataRegionGroups, and each DataRegionGroup has multiple DataRegions with the same id.

An illustration of the partition allocation in cluster:

<img style="width:100%; max-width:500px; max-height:500px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Cluster/Data-Partition.png?raw=true">

The figure contains 1 SchemaRegionGroup, and the schema_replication_factor is 3, so the 3 white SchemaRegion-0s form a replication group.

The figure contains 3 DataRegionGroups, and the data_replication_factor is 3, so there are 9 DataRegions in total.

## Consensus Protocol (Consistency Protocol)

Among multiple Regions of each RegionGroup, consistency is guaranteed through a consensus protocol, which routes read and write requests to multiple replicas.

* Current supported consensus protocol
  * SimpleConsensus：Provide strong consistency, could only be used when replica is 1, which is the empty implementation of the consensus protocol.
  * IoTConsensus：Provide eventual consistency, could be used in any number of replicas, 2 replicas could avoid single point failure, only for DataRegion, writings can be applied on each replica and replicated asynchronously to other replicas.
  * RatisConsensus：Provide Strong consistency, using raft consensus protocol, Could be used in any number of replicas, and could be used for any region groups. 
  Currently, DataRegion uses RatisConsensus does not support multiple data directories. This feature is planned to be supported in future releases.
