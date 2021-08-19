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

## Prerequisite
Note: Please install MinGW or WSL or git bash if you are using Windows.

## Example of distributed configurations for 1 node and 1 replica

### Compile from source code:

```
mvn clean package -DskipTests
chmod -R 777 ./cluster/target/
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh >/dev/null 2>&1 &
```

### Use the official website release version:

```
curl -O https://mirrors.tuna.tsinghua.edu.cn/apache/iotdb/0.12.1/apache-iotdb-0.12.1-cluster-bin.zip
unzip apache-iotdb-0.12.1-cluster-bin.zip
cd apache-iotdb-0.12.1-cluster-bin
sed -i -e 's/^seed_nodes=127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007$/seed_nodes=127.0.0.1:9003/g' conf/iotdb-cluster.properties
sed -i -e 's/^default_replica_num=3$/default_replica_num=1/g' conf/iotdb-cluster.properties
nohup ./sbin/start-node.sh >/dev/null 2>&1 &
```

## Example of distributed configurations for 3 nodes and 1 replica on a single machine

### Configurations

You can start multiple instances on a single machine by modifying the configurations yourself to handling port and file directory conflicts.

**Node1**:**(default)**

***iotdb-cluster.properties***

seed_nodes = 127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007

default_replica_num = 1

internal_meta_port = 9003

internal_data_port = 40010

***iotdb-engine.properties***

rpc_port=6667

system_dir=data/system
data_dirs=data/data
wal_dir=data/wal
index_root_dir=data/index
udf_root_dir=ext/udf
tracing_dir=data/tracing

**Node2**:

***iotdb-cluster.properties***

seed_nodes = 127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007

default_replica_num = 1

internal_meta_port = 9005

internal_data_port = 40012

***iotdb-engine.properties***

rpc_port=6669

system_dir=node2/system
data_dirs=node2/data
wal_dir=node2/wal
index_root_dir=node2/index
udf_root_dir=node2/ext/udf
tracing_dir=node2/tracing

**Node3**:

***iotdb-cluster.properties***

seed_nodes = 127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007

default_replica_num = 1

internal_meta_port = 9007

internal_data_port = 40014

***iotdb-engine.properties***

rpc_port=6671

system_dir=node3/system
data_dirs=node3/data
wal_dir=node3/wal
index_root_dir=node3/index
udf_root_dir=node3/ext/udf
tracing_dir=node3/tracing

### Compile from source code:

```
mvn clean package -DskipTests
chmod -R 777 ./cluster/target/
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node1conf/ >/dev/null 2>&1 &
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node2conf/ >/dev/null 2>&1 &
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node3conf/ >/dev/null 2>&1 &
```
### Use the official website release version:

Download the release version:
```
curl -O https://mirrors.tuna.tsinghua.edu.cn/apache/iotdb/0.12.1/apache-iotdb-0.12.1-cluster-bin.zip
```

Unzip the package:

```
unzip apache-iotdb-0.12.1-cluster-bin.zip
```

Enter IoTDB cluster root directory:

```
cd apache-iotdb-0.12.1-cluster-bin
```

Set default_replica_num = 1:

```
sed -i -e 's/^default_replica_num=3$/default_replica_num=1/g' conf/iotdb-cluster.properties
```

Create conf_dir for node2 and node3:

```
cp -r conf node2_conf
cp -r conf node3_conf
```

Handle port and file directory conflicts:

```
sed -i -e 's/^internal_meta_port=9003$/internal_meta_port=9005/g' -e 's/^internal_data_port=40010$/internal_data_port=40012/g' node2_conf/iotdb-cluster.properties
sed -i -e 's/^internal_meta_port=9003$/internal_meta_port=9007/g' -e 's/^internal_data_port=40010$/internal_data_port=40014/g' node3_conf/iotdb-cluster.properties
sed -i -e 's/^rpc_port=6667$/rpc_port=6669/g' -e node2_conf/iotdb-engine.properties
sed -i -e 's/^rpc_port=6667$/rpc_port=6671/g' -e node3_conf/iotdb-engine.properties
sed -i -e 's/^# data_dirs=data\/data$/data_dirs=node2\/data/g' -e 's/^# wal_dir=data\/wal$/wal_dir=node2\/wal/g' -e 's/^# tracing_dir=data\/tracing$/tracing_dir=node2\/tracing/g' -e 's/^# system_dir=data\/system$/system_dir=node2\/system/g' -e 's/^# udf_root_dir=ext\/udf$/udf_root_dir=node2\/ext\/udf/g' -e 's/^# index_root_dir=data\/index$/index_root_dir=node2\/index/g' node2_conf/iotdb-engine.properties
sed -i -e 's/^# data_dirs=data\/data$/data_dirs=node3\/data/g' -e 's/^# wal_dir=data\/wal$/wal_dir=node3\/wal/g' -e 's/^# tracing_dir=data\/tracing$/tracing_dir=node3\/tracing/g' -e 's/^# system_dir=data\/system$/system_dir=node3\/system/g' -e 's/^# udf_root_dir=ext\/udf$/udf_root_dir=node3\/ext\/udf/g' -e 's/^# index_root_dir=data\/index$/index_root_dir=node3\/index/g' node3_conf/iotdb-engine.properties
```

**You can modify the configuration items by yourself instead of using "sed" command**

Start the three nodes with their configurations:


```
nohup ./sbin/start-node.sh >/dev/null 2>&1 &
nohup ./sbin/start-node.sh ./node2_conf/ >/dev/null 2>&1 &
nohup ./sbin/start-node.sh ./node3_conf/ >/dev/null 2>&1 &
```



## Example of distributed configurations for 3 nodes and 3 replicas

Suppose we need to deploy the distributed IoTDB on three physical nodes, A, B, and C, whose public network IP is *A_public_IP*, *B_public_IP*, and *C_public_IP*, and private network IP is *A_private_IP*, *B_private_IP*, and *C_private_IP*.
Note: If there is no public network IP or private network IP, both can be set to the same, just need to ensure that the client can access the server.

### Configurations

**Each Node:**

***iotdb-cluster.properties***

seed_nodes = *A_private_Ip*:9003,*B_private_Ip*:9003,*C_private_Ip*:9003

default_replica_num = 3

internal_ip = *A_private_Ip* (or *B_private_Ip*, *C_private_Ip*)

***iotdb-enginer.properties***

rpc_port = *A_public_Ip* (or *B_private_Ip*, *C_public_Ip*)

### Start IoTDB cluster

The operation steps are as follows:

* Use 'mvn clean package -pl cluster -am -DskipTests' to compile the distributed module or directly go into the [website](https://iotdb.apache.org/Download/) to download the latest version.
* Make sure ports 6567, 6667, 9003, 9004, 40010, 40011 and 31999 are open on all three nodes.

* Send the package to all servers.

* Modify the configuration items.
* Run sh sbin/start-node.sh on each of the three nodes (or run in the background).
