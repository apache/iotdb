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

__NOTICE: CURRENT IoTDB CLUSTER IS FOR TESTING NOW! 
PLEASE BE DELIBERATE IF YOU RUN IT IN PRODUCT ENVIRONMENT.__

# Cluster Setup
For installation prerequisites, please refer to [QuickStart](../QuickStart/QuickStart.md)
## Prerequisite
Note: Please install MinGW or WSL or git bash if you are using Windows.
## Start Service
Users can build clusters in pseudo-distributed mode or distributed mode. 
The main difference between pseudo-distributed mode and distributed mode is the difference in `seed_nodes` in the configuration file. 
For detail descriptions, please refer to [Cluster Configuration Items](#Cluster Configuration Items).

To start the service of one of the nodes, you need to execute the following commands:

```bash
# Unix/OS X
> nohup sbin/start-node.sh [printgc] [<conf_path>] >/dev/null 2>&1 &

# Windows
> sbin\start-node.bat [printgc] [<conf_path>]
```
`printgc` means whether enable the gc and print gc logs when start the node,  
`<conf_path>` use the configuration file in the `conf_path` folder to override the default configuration file.

## OverWrite the configurations of Stand-alone node

Some configurations in the iotdb-engines.properties will be ignored

* `enable_auto_create_schema` is always considered as `false`. Use `enable_auto_create_schema` in 
 iotdb-cluster.properties to enable it, instead.

* `is_sync_enable` is always considered as `false`.

## Cluster Configuration Items
Before starting to use IoTDB, you need to config the configuration files first. 
For your convenience, we have already set the default config in the files.

In total, we provide users four kinds of configurations module: 

* environment configuration file (`iotdb-env.bat`, `iotdb-env.sh`). 
The default configuration file for the environment configuration item. 
Users can configure the relevant system configuration items of JAVA-JVM in the file.

* system configuration file (`iotdb-engine.properties`). The default configuration file for the IoTDB engine layer configuration item. 
Users can configure the IoTDB engine related parameters in the file, such as the precision of timestamp(`timestamp_precision`), etc. 
What's more, Users can configure settings of TsFile (the data files), such as the data size written to the disk per time(`group_size_in_byte`). 

* log configuration file (`logback.xml`). The default log configuration file, such as the log levels.

* `iotdb-cluster.properties`. Some configurations required by IoTDB cluster. Such as replication number(`default_replica_num`), etc.

For detailed description of the two configuration files `iotdb-engine.properties`, `iotdb-env.sh`/`iotdb-env.bat`, please refer to [Configuration Manual](../Appendix/Config-Manual.md). 
The configuration items described below are in the `iotdb-cluster.properties` file, you can also review the comments in the [configuration file](https://github.com/apache/iotdb/blob/master/cluster/src/assembly/resources/conf/iotdb-cluster.properties) directly. 

* internal\_ip

|Name|internal\_ip|
|:---:|:---|
|Description|IP address of internal communication between nodes in IOTDB cluster, such as heartbeat, snapshot, raft log, etc|
|Type|String|
|Default|127.0.0.1|
|Effective| After restart system, shall NOT change after cluster is up|

* internal\_meta\_port

|Name|internal\_meta\_port|
|:---:|:---|
|Description|IoTDB meta service port, for meta group's communication, which involves all nodes and manages the cluster configuration and storage groups. **IoTDB will automatically create a heartbeat port for each meta service. The default meta service heartbeat port is `internal_meta_port+1`, please confirm that these two ports are not reserved by the system and are not occupied**|
|Type|Int32|
|Default|9003|
|Effective| After restart system, shall NOT change after cluster is up|

* internal\_data\_port

|Name|internal\_data\_port|
|:---:|:---|
|Description|IoTDB data service port, for data groups' communication, each consists of one node and its replicas, managing timeseries schemas and data. **IoTDB will automatically create a heartbeat port for each data service. The default data service heartbeat port is `internal_data_port+1`. Please confirm that these two ports are not reserved by the system and are not occupied**|
|Type|Int32|
|Default|40010|
|Effective| After restart system, shall NOT change after cluster is up|

* cluster\_info\_public\_port

|Name|cluster\_info\_public\_port|
|:---:|:---|
|Description|The port of RPC service that getting the cluster info (e.g., data partition)|
|Type|Int32|
|Default|6567|
|Effective| After restart system|

* open\_server\_rpc\_port

|Name|open\_server\_rpc\_port|
|:---:|:---|
|Description|whether open port for server module (for debug purpose), if true, the single's server rpc_port will be changed to `rpc_port (in iotdb-engines.properties) + 1`|
|Type|Boolean|
|Default|False|
|Effective| After restart system|

* seed\_nodes

|Name|seed\_nodes|
|:---:|:---|
|Description|The address of the nodes in the cluster, `{IP/DOMAIN}:internal_meta_port` format, separated by commas; for the pseudo-distributed mode, you can fill in `localhost`, or `127.0.0.1` or mixed, but the real ip address cannot appear; for the distributed mode, real ip or hostname is supported, but `localhost` or `127.0.0.1` cannot appear. When used by `start-node.sh(.bat)`, this configuration means the nodes that will form the initial cluster, so every node that use `start-node.sh(.bat)` should have the same `seed_nodes`, or the building of the initial cluster will fail. WARNING: if the initial cluster is built, this should not be changed before the environment is cleaned. When used by `add-node.sh(.bat)`, this means the nodes to which that the application of joining the cluster will be sent, as all nodes can respond to a request, this configuration can be any nodes that already in the cluster, unnecessary to be the nodes that were used to build the initial cluster by `start-node.sh(.bat)`. Several nodes will be picked randomly to send the request, the number of nodes picked depends on the number of retries.|
|Type|String|
|Default|127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007|
|Effective| After restart system|

* rpc\_thrift\_compression\_enable

|Name|rpc\_thrift\_compression\_enable|
|:---:|:---|
|Description|Whether to enable thrift compression, **Note that this parameter should be consistent with each node and with the client, and also consistent with the `rpc_thrift_compression_enable` parameter in `iotdb-engine.properties`**|
|Type| Boolean|
|Default|false|
|Effective| After restart system, must be changed with all other nodes|

* default\_replica\_num

|Name|default\_replica\_num|
|:---:|:---|
|Description|Number of cluster replicas of timeseries schema and data. Storage group info is always fully replicated in all nodes.|
|Type|Int32|
|Default|3|
|Effective| After restart system, shall NOT change after cluster is up|

* cluster\_name

|Name|cluster\_name|
|:---:|:---|
|Description|Cluster name is used to identify different clusters, **`cluster_name` of all nodes in a cluster must be the same**|
|Type|String|
|Default|default|
|Effective| After restart system|

* connection\_timeout\_ms

|Name|connection\_timeout\_ms|
|:---:|:---|
|Description|Heartbeat timeout time period between nodes in the same raft group, in milliseconds|
|Type|Int32|
|Default|20000|
|Effective| After restart system|

* read\_operation\_timeout\_ms

|Name|read\_operation\_timeout\_ms|
|:---:|:---|
|Description|Read operation timeout time period, for internal communication only, not for the entire operation, in milliseconds|
|Type|Int32|
|Default|30000|
|Effective| After restart system|

* write\_operation\_timeout\_ms

|Name|write\_operation\_timeout\_ms|
|:---:|:---|
|Description|The write operation timeout period, for internal communication only, not for the entire operation, in milliseconds|
|Type|Int32|
|Default|30000|
|Effective| After restart system|

* min\_num\_of\_logs\_in\_mem

|Name|min\_num\_of\_logs\_in\_mem|
|:---:|:---|
|Description|The minimum number of committed logs in memory, after each log deletion, at most such number of logs will remain in memory. Increasing the number will reduce the chance to use snapshot in catch-ups, but will also increase the memory footprint|
|Type|Int32|
|Default|100|
|Effective| After restart system|

* max\_num\_of\_logs\_in\_mem

|Name|max\_num\_of\_logs\_in\_mem|
|:---:|:---|
|Description|Maximum number of committed logs in memory, when reached, a log deletion will be triggered. Increasing the number will reduce the chance to use snapshot in catch-ups, but will also increase memory footprint|
|Type|Int32|
|Default|1000|
|Effective| After restart system|

* log\_deletion\_check\_interval\_second

|Name|log\_deletion\_check\_interval\_second|
|:---:|:---|
|Description|The interval for checking and deleting the committed log task, which removes oldest in-memory committed logs when their size exceeds `min\_num\_of\_logs\_in\_mem` , in seconds|
|Type|Int32|
|Default|60|
|Effective| After restart system|

* enable\_auto\_create\_schema

|Name|enable\_auto\_create\_schema|
|:---:|:---|
|Description|Whether creating schema automatically is enabled, this will replace the one in `iotdb-engine.properties`|
|Type|BOOLEAN|
|Default|true|
|Effective| After restart system|

* consistency\_level

|Name|consistency\_level|
|:---:|:---|
|Description|Consistency level, now three consistency levels are supported: strong, mid, and weak. Strong consistency means the server will first try to synchronize with the leader to get the newest data, if failed(timeout), directly report an error to the user; While mid consistency means the server will first try to synchronize with the leader, but if failed(timeout), it will give up and just use current data it has cached before; Weak consistency does not synchronize with the leader and simply use the local data|
|Type|strong, mid, weak|
|Default|mid|
|Effective| After restart system|

* is\_enable\_raft\_log\_persistence

|Name|is\_enable\_raft\_log\_persistence|
|:---:|:---|
|Description|Whether to enable raft log persistence|
|Type|BOOLEAN|
|Default|true|
|Effective| After restart system|

## Enable GC log
GC log is off by default.
For performance tuning, you may want to collect the GC info. 

To enable GC log, just add a parameter `printgc` when you start the server.

```bash
nohup sbin/start-node.sh printgc >/dev/null 2>&1 &
```
Or
```bash
sbin\start-node.bat printgc
```

GC log is stored at `IOTDB_HOME/logs/gc.log`.

