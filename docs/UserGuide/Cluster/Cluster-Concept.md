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

<img style="width:100%; max-width:500px; max-height:400px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Cluster/Architecture.png?raw=true">

ConfigNode is the control node of the cluster, which manages the node status of cluster, partition information, etc. All ConfigNodes in the cluster form a highly available group, which is fully replicated.

DataNode stores the data and schema of cluster, which manages multiple data regions and schema regions. Data is a time-value pair, and schema is the path and data type of each time series.

Client could only connect to the DataNode for operation.

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

It is recommended to set 1 database (there is no need to set the database according to the number of cores as in version 0.13), which is used as the database concept, and the cluster will dynamically allocate resources according to the number of nodes and cores.

The database contains multiple SchemaRegions (schema shards) and DataRegions (data shards), which are managed by DataNodes.

* Schema partition strategy 
    * For a time series schema, the ConfigNode maps the device ID (full path from root to the penultimate tier node) into a series\_partition\_slot and assigns this partition slot to a SchemaRegion group.
* Data partition strategy
    * For a time series data point, the ConfigNode will map to a series\_partition\_slot (vertical partition) according to the device ID, and then map it to a time\_partition\_slot (horizontal partition) according to the data timestamp, and allocate this data partition to a DataRegion group.
  
IoTDB uses a slot-based partitioning strategy, so the size of the partition information is controllable and does not grow infinitely with the number of time series or devices.

Multiple replicas of a Region will be allocated to different DataNodes to avoid single point of failure, and the load balance of different DataNodes will be ensured when Regions are allocated.

## Replication Strategy

The replication strategy replicates data in multiple replicas, which are copies of each other. Multiple copies can provide high-availability services together and tolerate the failure of some copies.

A region is the basic unit of replication. Multiple replicas of a region construct a high-availability replication group, to support high availability.

* Replication and consensus
  * Partition information: The cluster has 1 partition information group consisting of all ConfigNodes.
  * Data: The cluster has multiple DataRegion groups, and each DataRegion group has multiple DataRegions with the same id.
  * Schema: The cluster has multiple SchemaRegion groups, and each SchemaRegion group has multiple SchemaRegions with the same id.

An illustration of the partition allocation in cluster:

<img style="width:100%; max-width:500px; max-height:500px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Cluster/Data-Partition.png?raw=true">

The figure contains 1 SchemaRegion group, and the schema_replication_factor is 3, so the 3 white SchemaRegion-0s form a replication group, and the Raft protocol is used to ensure data consistency.

The figure contains 3 DataRegion groups, and the data_replication_factor is 3, so there are 9 DataRegions in total.

## Consensus Protocol (Consistency Protocol)

Among multiple replicas of each region group, data consistency is guaranteed through a consensus protocol, which routes read and write requests to multiple replicas.

* Current supported consensus protocol
  * Standalone：Could only be used when replica is 1, which is the empty implementation of the consensus protocol.
  * MultiLeader：Could be used in any number of replicas, only for DataRegion, writings can be applied on each replica and replicated asynchronously to other replicas.
  * Ratis：Raft consensus protocol, Could be used in any number of replicas, and could be used for any region groups。
  
## 0.14.0-preview1 Function Map

<img style="width:100%; max-width:800px; max-height:1000px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Cluster/Preview1-Function.png?raw=true">
