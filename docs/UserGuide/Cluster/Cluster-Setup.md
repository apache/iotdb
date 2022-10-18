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

## Cluster Setup

This article is the setup process of IoTDB Cluster (0.14.0-preview1).

## Environments

1. JDK>=1.8.

2. Max open file 65535.

3. Disable the swap memory.

## Get the binary distribution

### Download

Download the binary distribution from website [Download Page](https://iotdb.apache.org/Download/).

### Compiled from source code

Download the source code:

```
git clone https://github.com/apache/iotdb.git
```

The default branch is master, you should checkout to the release tag:

```
git checkout v0.14.0-preview1
```

Under the source root folder:

```
mvn clean package -pl distribution -am -DskipTests
```

Then you will get the binary distribution under **distribution/target**, in which the **all-bin** contains ConfigNode and DataNode, and DataNode contains the Cli.

## Binary Distribution Content

| **Folder**              | **Description**                                                                     |
|-------------------------|-------------------------------------------------------------------------------------|
| confignode              | Contains start/stop/remove shell, configurations, logs, data of ConfigNode          |
| datanode                | Contains start/stop/remove shell, configurations, logs, data of DataNode, cli shell |
| grafana-metrics-example | Grafana metric page module                                                          |
| lib                     | Jar files folder                                                                    |
| tools                   | System tools                                                                        |

## Start the Cluster

Users could start a cluster which contains multiple ConfigNode and DataNode.
A cluster need at least one ConfigNode and no less than the number of data/schema_replication_factor DataNodes.

The total process are three steps:

* Start the first ConfigNode
* Add ConfigNode (Optional)
* Add DataNode

### Start the first ConfigNode

Please set the important parameters in iotdb-confignode.properties:

| **Configuration**                          | **Description**                                                                              |
|--------------------------------------------|----------------------------------------------------------------------------------------------|
| internal\_address                          | Internal rpc service address of ConfigNode                                                   |
| internal\_port                             | Internal rpc service address of ConfigNode                                                   |
| consensus\_port                            | ConfigNode replication consensus protocol communication port                                 |
| target\_config\_nodes                      | Target ConfigNode address, if the current is the first ConfigNode, then set its address:port |
| data\_replication\_factor                  | Data replication factor, no more than DataNode number                                        |
| data\_region\_consensus\_protocol\_class   | Consensus protocol of data replicas                                                          |
| schema\_replication\_factor                | Schema replication factor, no more than DataNode number                                      |
| schema\_region\_consensus\_protocol\_class | Consensus protocol of schema replicas                                                        |

Start on Linux:
```
# Foreground
./confignode/sbin/start-confignode.sh

# Background
nohup ./confignode/sbin/start-confignode.sh >/dev/null 2>&1 &
```

Start on Windows:
```
confignode\sbin\start-confignode.bat
```

More details  [ConfigNode Configurations](https://iotdb.apache.org/UserGuide/Master/Reference/ConfigNode-Config-Manual.html).

### Add ConfigNode (Optional)

This will add the replication factor of ConfigNode, except for the port couldn't conflict, make sure other configurations are the same with existing ConfigNode in Cluster.

The adding ConfigNode also use the start-confignode.sh/bat.

### Start DataNode

You could add any number of DataNode.

Please set the important parameters in iotdb-datanode.properties.

| **Configuration**               | **Description**                                  |
|---------------------------------|--------------------------------------------------|
| rpc\_address                    | Client RPC Service address                       |
| rpc\_port                       | Client RPC Service port                          |
| internal\_address               | Control flow address of DataNode inside cluster  |
| internal\_port                  | Control flow port of DataNode inside cluster     |
| mpp\_data\_exchange\_port       | Data flow port of DataNode inside cluster        |
| data\_region\_consensus\_port   | Data replicas communication port for consensus   |
| schema\_region\_consensus\_port | Schema replicas communication port for consensus |
| target\_config\_nodes           | Running ConfigNode of the Cluster                |

Start on Linux:
```
# Foreground
./datanode/sbin/start-datanode.sh

# Background
nohup ./datanode/sbin/start-datanode.sh >/dev/null 2>&1 &
```

Start on Windows:
```
datanode\sbin\start-datanode.bat
```

More details [DataNode Configurations](https://iotdb.apache.org/UserGuide/Master/Reference/DataNode-Config-Manual.html).

### Start Cli

Cli is in datanode/sbin folder.

Start on Linux:
```
./datanode/sbin/start-cli.sh
```

Start on Windows:
```
datanode\sbin\start-cli.bat
```

## Shrink the Cluster

### Remove ConfigNode

Execute the remove-confignode shell on an active ConfigNode.

Remove on Linux:
```
./confignode/sbin/remove-confignode.sh <confignode_id>
./confignode/sbin/remove-confignode.sh <internal_address>:<internal_port>
```

Remove on Windows:
```
confignode\sbin\remove-confignode.bat <confignode_id>
confignode\sbin\remove-confignode.bat <internal_address>:<internal_port>
```

### Remove DataNode

Execute the remove-datanode shell on an active DataNode.

Remove on Linux:
```
./datanode/sbin/remove-datanode.sh <datanode_id>
./datanode/sbin/remove-datanode.sh <rpc_address>:<rpc_port>
```

Remove on Windows:
```
datanode\sbin\remove-datanode.bat <datanode_id>
datanode\sbin\remove-datanode.bat <rpc_address>:<rpc_port>
```

## Quick Start

This section uses a local environment as an example to 
illustrate how to start, expand, and shrink a IoTDB Cluster.

### 1. Prepare the Start Environment

Unzip the apache-iotdb-0.14.0-preview1-all-bin.zip file to cluster0 folder.

### 2. Starting a Minimum Cluster

Starting the Cluster version with one ConfigNode and one DataNode(1C1D),
the default number of replica is one.
```
./cluster0/confignode/sbin/start-confignode.sh
./cluster0/datanode/sbin/start-datanode.sh
```

### 3. Verify the Minimum Cluster

+ The minimum cluster is successfully started. Start the Cli for verification.
```
./cluster0/datanode/sbin/start-cli.sh
```

+ Execute the [show cluster](https://iotdb.apache.org/UserGuide/Master/Maintenance-Tools/Maintenance-Command.html#show-all-node-information)
  command on the Cli. The result is shown below:
```
IoTDB> show cluster
+------+----------+-------+---------+------------+
|NodeID|  NodeType| Status|     Host|InternalPort|
+------+----------+-------+---------+------------+
|     0|ConfigNode|Running|  0.0.0.0|       22277|
|     1|  DataNode|Running|127.0.0.1|        9003|
+------+----------+-------+---------+------------+
Total line number = 2
It costs 0.160s
```

### 4. Prepare the Expanding Environment

Unzip the apache-iotdb-0.14.0-preview1-all-bin.zip file to cluster1 and cluster2 folder.

### 5. Modify the Node Configuration file

For folder cluster1:

+ Modify ConfigNode address:

| **configuration item** | **value**     |
|------------------------|---------------|
| internal\_address      | 0.0.0.0       |
| internal\_port         | 22279         |
| consensus\_port        | 22280         |
| target\_config\_nodes  | 0.0.0.0:22277 |

+ Modify DataNode address:

| **configuration item**          | **value**       |
|---------------------------------|-----------------|
| rpc\_address                    | 0.0.0.0         |
| rpc\_port                       | 6668            |
| internal\_address               | 127.0.0.1       |
| internal\_port                  | 9004            |
| mpp\_data\_exchange\_port       | 8778            |
| data\_region\_consensus\_port   | 40011           |
| schema\_region\_consensus\_port | 50011           |
| target\_config\_nodes           | 127.0.0.1:22277 |

For folder cluster1:

+ Modify ConfigNode address:

| **configuration item** | **value**     |
|------------------------|---------------|
| internal\_address      | 0.0.0.0       |
| internal\_port         | 22281         |
| consensus\_port        | 22282         |
| target\_config\_nodes  | 0.0.0.0:22277 |

+ Modify DataNode address:

| **configuration item**          | **value**       |
|---------------------------------|-----------------|
| rpc\_address                    | 0.0.0.0         |
| rpc\_port                       | 6669            |
| internal\_address               | 127.0.0.1       |
| internal\_port                  | 9005            |
| mpp\_data\_exchange\_port       | 8779            |
| data\_region\_consensus\_port   | 40012           |
| schema\_region\_consensus\_port | 50012           |
| target\_config\_nodes           | 127.0.0.1:22277 |

### 6. Expanding the Cluster

Expanding the Cluster to three ConfigNode and three DataNode(3C3D).
The following commands can be executed in no particular order.

```
./cluster1/confignode/sbin/start-confignode.sh
./cluster1/datanode/sbin/start-datanode.sh
./cluster2/confignode/sbin/start-confignode.sh
./cluster2/datanode/sbin/start-datanode.sh
```

### 7. Verify Cluster expansion

Execute the show cluster command, the result is shown below:
```
IoTDB> show cluster
+------+----------+-------+---------+------------+
|NodeID|  NodeType| Status|     Host|InternalPort|
+------+----------+-------+---------+------------+
|     0|ConfigNode|Running|  0.0.0.0|       22277|
|     2|ConfigNode|Running|  0.0.0.0|       22279|
|     3|ConfigNode|Running|  0.0.0.0|       22281|
|     1|  DataNode|Running|127.0.0.1|        9003|
|     4|  DataNode|Running|127.0.0.1|        9004|
|     5|  DataNode|Running|127.0.0.1|        9005|
+------+----------+-------+---------+------------+
Total line number = 6
It costs 0.012s
```

### 8. Shrinking the Cluster

+ Remove a ConfigNode:
```
./cluster0/confignode/sbin/remove-confignode.sh -r 0.0.0.0:22279
```

+ Remove a DataNode:
```
./cluster0/datanode/sbin/remove-datanode.sh 127.0.0.1:6668
```

### 9. Verify Cluster shrinkage

Execute the show cluster command, the result is shown below:
```
IoTDB> show cluster
+------+----------+-------+---------+------------+
|NodeID|  NodeType| Status|     Host|InternalPort|
+------+----------+-------+---------+------------+
|     0|ConfigNode|Running|  0.0.0.0|       22277|
|     3|ConfigNode|Running|  0.0.0.0|       22281|
|     1|  DataNode|Running|127.0.0.1|        9003|
|     5|  DataNode|Running|127.0.0.1|        9005|
+------+----------+-------+---------+------------+
Total line number = 4
It costs 0.007s
```
