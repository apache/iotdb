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

# ConfigNode Configuration

IoTDB ConfigNode files are under `confignode/conf`.

* `confignode-env.sh/bat`：Environment configurations, in which we could set the memory allocation of ConfigNode.

* `iotdb-confignode.properties`：IoTDB ConfigNode system configurations.

## Environment Configuration File（confignode-env.sh/bat）

The environment configuration file is mainly used to configure the Java environment related parameters when ConfigNode is running, such as JVM related configuration. This part of the configuration is passed to the JVM when the ConfigNode starts.

The details of each parameter are as follows:

* MAX\_HEAP\_SIZE

|Name|MAX\_HEAP\_SIZE|
|:---:|:---|
|Description|The maximum heap memory size that IoTDB can use |
|Type|String|
|Default| On Linux or MacOS, the default is one quarter of the memory. On Windows, the default value for 32-bit systems is 512M, and the default for 64-bit systems is 2G.|
|Effective|After restarting system|

* HEAP\_NEWSIZE

|Name|HEAP\_NEWSIZE|
|:---:|:---|
|Description|The minimum heap memory size that IoTDB will use when startup |
|Type|String|
|Default| On Linux or MacOS, the default is min{cores * 100M, one quarter of MAX\_HEAP\_SIZE}. On Windows, the default value for 32-bit systems is 512M, and the default for 64-bit systems is 2G.|
|Effective|After restarting system|

* MAX\_DIRECT\_MEMORY\_SIZE

|Name|MAX\_DIRECT\_MEMORY\_SIZE|
|:---:|:---|
|Description|The max direct memory that IoTDB could use|
|Type|String|
|Default| Equal to the MAX\_HEAP\_SIZE|
|Effective|After restarting system|


## ConfigNode Configuration File（iotdb-confignode.properties）

The global configuration of cluster is in ConfigNode.

### Internal RPC Service Configurations

* internal\_address

|Name| internal\_address |
|:---:|:---|
|Description| ConfigNode internal service address |
|Type| String |
|Default| 0.0.0.0|
|Effective|After restarting system|

* internal\_port

|Name| internal\_port |
|:---:|:---|
|Description| ConfigNode internal service port|
|Type| Short Int : [0,65535] |
|Default| 22277 |
|Effective|After restarting system|

* target\_config\_nodes

|Name| target\_config\_nodes |
|:---:|:---|
|Description| Target ConfigNode address, for current ConfigNode to join the cluster |
|Type| String |
|Default| 127.0.0.1:22277 |
|Effective|After restarting system|

* rpc\_thrift\_compression\_enable

|Name| rpc\_thrift\_compression\_enable |
|:---:|:---|
|Description| Whether enable thrift's compression (using GZIP).|
|Type|Boolean|
|Default| false |
|Effective|After restarting system|

* rpc\_advanced\_compression\_enable

|Name| rpc\_advanced\_compression\_enable |
|:---:|:---|
|Description| Whether enable thrift's advanced compression.|
|Type|Boolean|
|Default| false |
|Effective|After restarting system|

* rpc\_max\_concurrent\_client\_num

|Name| rpc\_max\_concurrent\_client\_num |
|:---:|:---|
|Description| Max concurrent rpc connections|
|Type| Short Int : [0,65535] |
|Description| 65535 |
|Effective|After restarting system|

* thrift\_max\_frame\_size

|Name| thrift\_max\_frame\_size |
|:---:|:---|
|Description| Max size of bytes of each thrift RPC request/response|
|Type| Long |
|Unit|Byte|
|Default| 536870912 |
|Effective|After restarting system|

* thrift\_init\_buffer\_size

|Name| thrift\_init\_buffer\_size |
|:---:|:---|
|Description| Initial size of bytes of buffer that thrift used |
|Type| long |
|Default| 1024 |
|Effective|After restarting system|


### Replication and Consensus

* consensus\_port

|Name| consensus\_port |
|:---:|:---|
|Description| ConfigNode data Consensus Port  |
|Type| Short Int : [0,65535] |
|Default| 22278 |
|Effective|After restarting system|


* data\_replication\_factor

|Name| data\_replication\_factor |
|:---:|:---|
|Description| Data replication num|
|Type| Int |
|Default| 1 |
|Effective|After restarting system|

* data\_region\_consensus\_protocol\_class

|Name| data\_region\_consensus\_protocol\_class |
|:---:|:---|
|Description| Consensus protocol of data replicas, StandAloneConsensus could only be used in 1 replica，larger than 1 replicas could use MultiLeaderConsensus or RatisConsensus |
|Type| String |
|Default| org.apache.iotdb.consensus.standalone.StandAloneConsensus |
|Effective|Only allowed to be modified in first start up|

* schema\_replication\_factor

|Name| schema\_replication\_factor |
|:---:|:---|
|Description| Schema replication num|
|Type| Int |
|Default| 1 |
|Effective|After restarting system|


* schema\_region\_consensus\_protocol\_class

|Name| schema\_region\_consensus\_protocol\_class |
|:---:|:---|
|Description| Consensus protocol of schema replicas, StandAloneConsensus could only be used in 1 replica，larger than 1 replicas could only use RatisConsensus | |
|Type| String |
|Default| org.apache.iotdb.consensus.standalone.StandAloneConsensus |
|Effective|Only allowed to be modified in first start up|


* region\_allocate\_strategy

|Name| region\_allocate\_strategy |
|:---:|:---|
|Description| Region allocate strategy, COPY_SET is suitable for large clusters, GREEDY is suitable for small clusters  |
|Type| String |
|Default| GREEDY |
|Effective|After restarting system |

### HeartBeat 

* heartbeat\_interval

|Name| heartbeat\_interval |
|:---:|:---|
|Description| Heartbeat interval in the cluster nodes |
|Type| Long |
|Unit| ms |
|Default| 1000 |
|Effective|After restarting system|


### Partition Strategy

* series\_partition\_slot\_num

|Name| series\_partition\_slot\_num |
|:---:|:---|
|Description| Slot num of series partition |
|Type| Int |
|Default| 10000 |
|Effective|Only allowed to be modified in first start up|

* series\_partition\_executor\_class

|Name| series\_partition\_executor\_class |
|:---:|:---|
|Description| Series partition hash function |
|Type| String |
|Default| org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor |
|Effective|Only allowed to be modified in first start up|

### Storage Group

* default\_ttl

|Name| default\_ttl |
|:---:|:---|
|Description| Default ttl when each storage group created |
|Type| Long |
|Default| Infinity |
|Effective|After restarting system|

* time\_partition\_interval\_for\_routing

|Name| time\_partition\_interval\_for\_routing                       |
|:---:|:--------------------------------------------------------------|
|Description| Time partition interval of data when ConfigNode allocate data |
|Type| Long                                                          |
|Unit| ms |
|Default| 86400000                                                      |
|Effective| Only allowed to be modified in first start up                 |


### Data Directory

* system\_dir

|Name| system\_dir |
|:---:|:---|
|Description| ConfigNode system data dir |
|Type| String |
|Default| data/system（Windows：data\\system） |
|Effective|After restarting system|

* consensus\_dir

|Name| consensus\_dir |
|:---:|:---|
|Description| ConfigNode Consensus protocol data dir |
|Type| String |
|Default| data/consensus（Windows：data\\consensus） |
|Effective|After restarting system|

* udf\_lib\_dir

|Name| udf\_lib\_dir |
|:---:|:---|
|Description| UDF log and jar file dir |
|Type| String |
|Default| ext/udf（Windows：ext\\udf） |
|Effective|After restarting system|

* temporary\_lib\_dir

|Name| temporary\_lib\_dir |
|:---:|:---|
|Description| UDF jar file temporary dir |
|Type| String |
|Default| ext/temporary（Windows：ext\\temporary） |
|Effective|After restarting system|