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

This article is the setup process of IoTDB Cluster (1.0.0).

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
git checkout v1.0.0
```

Under the source root folder:

```
mvn clean package -pl distribution -am -DskipTests
```

Then you will get the binary distribution under **distribution/target**, in which the **all-bin** contains ConfigNode and DataNode, and DataNode contains the Cli.

## Binary Distribution Content

| **Folder**              | **Description**                                                                            |
|-------------------------|--------------------------------------------------------------------------------------------|
| conf                    | Configuration files folder, contains configuration files of ConfigNode and DataNode        |
| data                    | Data files folder, contains data files of ConfigNode and DataNode                          |       |
| grafana-metrics-example | Grafana metric page module                                                                 |
| lib                     | Jar files folder                                                                           |
| licenses                | Licenses files folder                                                                      |
| logs                    | Logs files folder, contains logs files of ConfigNode and DataNode                          |
| sbin                    | Shell files folder, contains start/stop/remove shell of ConfigNode and DataNode, cli shell |
| tools                   | System tools                                                                               |

## Start the Cluster

Users could start a cluster which contains multiple ConfigNode and DataNode.
A cluster need at least one ConfigNode and no less than the number of data/schema_replication_factor DataNodes.

The total process are three steps:

* Start the first ConfigNode
* Add ConfigNode (Optional)
* Add DataNode

### Start the first ConfigNode

Please set the important parameters in conf/iotdb-confignode.properties and conf/iotdb-common.properties:

iotdb-confignode.properties:

| **Configuration**              | **Description**                                                                              |
|--------------------------------|----------------------------------------------------------------------------------------------|
| cn\_internal\_address          | Internal rpc service address of ConfigNode                                                   |
| cn\_internal\_port             | Internal rpc service port of ConfigNode                                                      |
| cn\_consensus\_port            | ConfigNode replication consensus protocol communication port                                 |
| cn\_target\_config\_node\_list | Target ConfigNode address, if the current ConfigNode is the first one, then set its own address:port |

iotdb-common.properties:

| **Configuration**                          | **Description**                                                                                      |
|--------------------------------------------|------------------------------------------------------------------------------------------------------|
| data\_replication\_factor                  | Data replication factor, no more than DataNode number                                                |
| data\_region\_consensus\_protocol\_class   | Consensus protocol of data replicas                                                                  |
| schema\_replication\_factor                | Schema replication factor, no more than DataNode number                                              |
| schema\_region\_consensus\_protocol\_class | Consensus protocol of schema replicas                                                                |

Start on Linux:
```
# Foreground
bash ./sbin/start-confignode.sh

# Background
nohup bash ./sbin/start-confignode.sh >/dev/null 2>&1 &
```

Start on Windows:
```
sbin\start-confignode.bat
```

More details  [ConfigNode Configurations](https://iotdb.apache.org/UserGuide/Master/Reference/ConfigNode-Config-Manual.html).

### Add ConfigNode (Optional)

This will add the replication factor of ConfigNode, except for the ports that couldn't conflict with, make sure other configurations are the same with existing ConfigNode in Cluster, and set parameter cn\_target\_config\_nodes\_list as an active ConfigNode in Cluster.

The adding ConfigNode also use the start-confignode.sh/bat.

### Start DataNode

You could add any number of DataNode.

Please set the important parameters in iotdb-datanode.properties:

| **Configuration**                   | **Description**                                  |
|-------------------------------------|--------------------------------------------------|
| dn\_rpc\_address                    | Client RPC Service address                       |
| dn\_rpc\_port                       | Client RPC Service port                          |
| dn\_internal\_address               | Control flow address of DataNode inside cluster  |
| dn\_internal\_port                  | Control flow port of DataNode inside cluster     |
| dn\_mpp\_data\_exchange\_port       | Data flow port of DataNode inside cluster        |
| dn\_data\_region\_consensus\_port   | Data replicas communication port for consensus   |
| dn\_schema\_region\_consensus\_port | Schema replicas communication port for consensus |
| dn\_target\_config\_node\_list      | Running ConfigNode of the Cluster                |

Start on Linux:
```
# Foreground
bash ./sbin/start-datanode.sh

# Background
nohup bash ./sbin/start-datanode.sh >/dev/null 2>&1 &
```

Start on Windows:
```
sbin\start-datanode.bat
```

More details are in [DataNode Configurations](https://iotdb.apache.org/UserGuide/Master/Reference/DataNode-Config-Manual.html).

### Stop IoTDB
When you meet problem, and want to stop IoTDB ConfigNode and DataNode directly, our shells can help you do this.

In Windows:

```
sbin\stop-datanode.bat
```
```
sbin\stop-confignode.bat
```
In Linux:
```
sudo bash sbin\stop-datanode.sh
```
```
sudo bash sbin\stop-confignode.sh
```
Be careful not to miss the "sudo" label, because some port info's acquisition may require root authority. If you can't sudo, just
use "jps" or "ps aux | grep iotdb" to get the process's id, then use "kill -9 <process-id>" to stop the process.  

## Start StandAlone
If you just want to setup your IoTDB locally, 
You can quickly init 1C1D (i.e. 1 Confignode and 1 Datanode) environment by our shells.

This will work well if you don't change our default settings.

Start on Windows:
```
sbin\start-datanode.bat
```
Start on Linux:
```
sudo bash sbin\start-standalone.sh
```
Besides, with our shell, you can also directly kill these processes.

Stop on Windows:
```
sbin\stop-datanode.bat
```
Stop on Linux:
```
sudo bash sbin\stop-standalone.sh
```

Note: On Linux, the 1C1D processes both launches in the background, and you can see confignode1.log and datanode1.log 
for details. 

The stop-standalone.sh may not work well without sudo, since IoTDB's port numbers may be invisible without permission. If stop-standalone.sh meets some error, you can use "jps" or "ps aux | grep iotdb" to obtain the process ids,
and use "sudo kill -9 <process-id>" to manually stop the processes.

## Start Cli

Cli shell is in sbin folder.

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

Execute the remove-confignode shell on an active ConfigNode, and make sure that there is at least one active ConfigNode in Cluster after removing.

Remove on Linux:
```
# Remove the ConfigNode with confignode_id
./confignode/sbin/remove-confignode.sh <confignode_id>

# Remove the ConfigNode with address:port
./confignode/sbin/remove-confignode.sh <internal_address>:<internal_port>
```

Remove on Windows:
```
# Remove the ConfigNode with confignode_id
confignode\sbin\remove-confignode.bat <confignode_id>

# Remove the ConfigNode with address:port
confignode\sbin\remove-confignode.bat <internal_address>:<internal_port>
```

### Remove DataNode

Execute the remove-datanode shell on an active, and make sure that there are no less than the number of data/schema_replication_factor DataNodes in Cluster after removing.

Remove on Linux:
```
# Remove the DataNode with datanode_id
./datanode/sbin/remove-datanode.sh <datanode_id>

# Remove the DataNode with rpc address:port
./datanode/sbin/remove-datanode.sh <rpc_address>:<rpc_port>
```

Remove on Windows:
```
# Remove the DataNode with datanode_id
datanode\sbin\remove-datanode.bat <datanode_id>

# Remove the DataNode with rpc address:port
datanode\sbin\remove-datanode.bat <rpc_address>:<rpc_port>
```

## Quick Start

This section uses a local environment as an example to 
illustrate how to start, expand, and shrink a IoTDB Cluster.

### 1. Prepare the Start Environment

Unzip the apache-iotdb-1.0.0-all-bin.zip file to cluster0 folder.

### 2. Starting a Minimum Cluster

Starting the Cluster version with one ConfigNode and one DataNode(1C1D),
the default number of replica is one.
```
./cluster0/sbin/start-confignode.sh
./cluster0/sbin/start-datanode.sh
```

### 3. Verify the Minimum Cluster

+ The minimum cluster is successfully started. Start the Cli for verification.
```
./cluster0/sbin/start-cli.sh
```

+ Execute the [show cluster](https://iotdb.apache.org/UserGuide/Master/Maintenance-Tools/Maintenance-Command.html#show-all-node-information)
  command on the Cli. The result is shown below:
```
IoTDB> show cluster
+------+----------+-------+---------------+------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|
+------+----------+-------+---------------+------------+
|     0|ConfigNode|Running|      127.0.0.1|       22277|
|     1|  DataNode|Running|      127.0.0.1|        9003|
+------+----------+-------+---------------+------------+
Total line number = 2
It costs 0.160s
```

### 4. Prepare the Expanding Environment

Unzip the apache-iotdb-1.0.0-all-bin.zip file to cluster1 and cluster2 folder.

### 5. Modify the Node Configuration file

For folder cluster1:

+ Modify ConfigNode configurations:

| **configuration item**         | **value**       |
|--------------------------------|-----------------|
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 22279           |
| cn\_consensus\_port            | 22280           |
| cn\_target\_config\_node\_list | 127.0.0.1:22277 |

+ Modify DataNode configurations:

| **configuration item**              | **value**       |
|-------------------------------------|-----------------|
| dn\_rpc\_address                    | 127.0.0.1       |
| dn\_rpc\_port                       | 6668            |
| dn\_internal\_address               | 127.0.0.1       |
| dn\_internal\_port                  | 9004            |
| dn\_mpp\_data\_exchange\_port       | 8778            |
| dn\_data\_region\_consensus\_port   | 40011           |
| dn\_schema\_region\_consensus\_port | 50011           |
| dn\_target\_config\_node\_list      | 127.0.0.1:22277 |

For folder cluster2:

+ Modify ConfigNode configurations:

| **configuration item**         | **value**       |
|--------------------------------|-----------------|
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 22281           |
| cn\_consensus\_port            | 22282           |
| cn\_target\_config\_node\_list | 127.0.0.1:22277 |

+ Modify DataNode configurations:

| **configuration item**              | **value**       |
|-------------------------------------|-----------------|
| dn\_rpc\_address                    | 127.0.0.1       |
| dn\_rpc\_port                       | 6669            |
| dn\_internal\_address               | 127.0.0.1       |
| dn\_internal\_port                  | 9005            |
| dn\_mpp\_data\_exchange\_port       | 8779            |
| dn\_data\_region\_consensus\_port   | 40012           |
| dn\_schema\_region\_consensus\_port | 50012           |
| dn\_target\_config\_node\_list      | 127.0.0.1:22277 |

### 6. Expanding the Cluster

Expanding the Cluster to three ConfigNode and three DataNode(3C3D).
The following commands can be executed in no particular order.

```
./cluster1/sbin/start-confignode.sh
./cluster1/sbin/start-datanode.sh
./cluster2/sbin/start-confignode.sh
./cluster2/sbin/start-datanode.sh
```

### 7. Verify Cluster expansion

Execute the show cluster command, the result is shown below:
```
IoTDB> show cluster
+------+----------+-------+---------------+------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|
+------+----------+-------+---------------+------------+
|     0|ConfigNode|Running|      127.0.0.1|       22277|
|     2|ConfigNode|Running|      127.0.0.1|       22279|
|     3|ConfigNode|Running|      127.0.0.1|       22281|
|     1|  DataNode|Running|      127.0.0.1|        9003|
|     4|  DataNode|Running|      127.0.0.1|        9004|
|     5|  DataNode|Running|      127.0.0.1|        9005|
+------+----------+-------+---------------+------------+
Total line number = 6
It costs 0.012s
```

### 8. Shrinking the Cluster

+ Remove a ConfigNode:
```
./cluster0/sbin/remove-confignode.sh 127.0.0.1:22279
```

+ Remove a DataNode:
```
./cluster0/sbin/remove-datanode.sh 127.0.0.1:6668
```

### 9. Verify Cluster shrinkage

Execute the show cluster command, the result is shown below:
```
IoTDB> show cluster
+------+----------+-------+---------------+------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|
+------+----------+-------+---------------+------------+
|     0|ConfigNode|Running|      127.0.0.1|       22277|
|     3|ConfigNode|Running|      127.0.0.1|       22281|
|     1|  DataNode|Running|      127.0.0.1|        9003|
|     5|  DataNode|Running|      127.0.0.1|        9005|
+------+----------+-------+---------------+------------+
Total line number = 4
It costs 0.007s
```
