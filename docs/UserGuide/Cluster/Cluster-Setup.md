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

# 1. Purpose
This article is the setup process of IoTDB Cluster (1.0.0).

# 2. Prerequisites

1. JDK>=1.8.
2. Max open file 65535.
3. Disable the swap memory.
4. Ensure there are no data dir or the data dir is empty before the Node is started for the first time.
5. Turn off the firewall of the server if the entire cluster is in a trusted environment.

# 3. Get the Installation Package

## 3.1 Download the binary distribution

1. Open our website [Download Page](https://iotdb.apache.org/Download/).
2. Download the binary distribution.
3. Decompress to get the apache-iotdb-1.0.0-all-bin directory.

## 3.2 Compile with source code

### 3.2.1 Download the source code

**Git**
```
git clone https://github.com/apache/iotdb.git
git checkout v1.0.0
```

**Website**
1. Open our website [Download Page](https://iotdb.apache.org/Download/).
2. Download the source code.
3. Decompress to get the apache-iotdb-1.0.0 directory.

### 3.2.2 Compile source code

Under the source root folder:
```
mvn clean package -pl distribution -am -DskipTests
```

Then you will get the binary distribution under 
**distribution/target/apache-iotdb-1.0.0-SNAPSHOT-all-bin/apache-iotdb-1.0.0-SNAPSHOT-all-bin**.

# 4. Binary Distribution Content

| **Folder**              | **Description**                                                                            |
|-------------------------|--------------------------------------------------------------------------------------------|
| conf                    | Configuration files folder, contains configuration files of ConfigNode and DataNode        |
| data                    | Data files folder, contains data files of ConfigNode and DataNode                          |       |
| lib                     | Jar files folder                                                                           |
| licenses                | Licenses files folder                                                                      |
| logs                    | Logs files folder, contains logs files of ConfigNode and DataNode                          |
| sbin                    | Shell files folder, contains start/stop/remove shell of ConfigNode and DataNode, cli shell |
| tools                   | System tools                                                                               |

# 5. Cluster Installation and Configuration

## 5.1 Cluster Installation

Deploy apache-iotdb-1.0.0-SNAPSHOT-all-bin to the specified directory on the server or VM 
to complete cluster installation on a server or VM.
Each apache-iotdb-1.0.0-SNAPSHOT-all-bin can start one ConfigNode and one DataNode.

## 5.2 Cluster Configuration

Switch the working path to apache-iotdb-1.0.0-SNAPSHOT-all-bin.
The cluster configuration files is stored in the ./conf directory.

### 5.2.1 Common configuration

Open the common configuration file ./conf/iotdb-common.properties,
and set the following parameters base on the 
[Deployment Recommendation](https://iotdb.apache.org/UserGuide/Master/Cluster/Deployment-Recommendation.html):

| **Configuration**                          | **Description**                                                                                                    |
|--------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| config_node_consensus_protocol_class       | Consensus protocol of ConfigNode                                                                                   |
| schema\_replication\_factor                | Schema replication factor, no more than DataNode number                                                            |
| schema\_region\_consensus\_protocol\_class | Consensus protocol of schema replicas                                                                              |
| data\_replication\_factor                  | Data replication factor, no more than DataNode number                                                              |
| data\_region\_consensus\_protocol\_class   | Consensus protocol of data replicas. Note that RatisConsensus currently does not support multiple data directories |

**Notice: The preceding configuration parameters cannot be changed after the cluster is started. Ensure that the common configurations of all Nodes are the same. Otherwise, the Nodes cannot be started.**

### 5.2.2 ConfigNode configuration

Open the ConfigNode configuration file ./conf/iotdb-confignode.properties,
and set the following parameters based on the IP address and available port of the server or VM:

| **Configuration**              | **Description**                                                                                                                          |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| cn\_internal\_address          | Internal rpc service address of ConfigNode                                                                                               |
| cn\_internal\_port             | Internal rpc service port of ConfigNode                                                                                                  |
| cn\_consensus\_port            | ConfigNode replication consensus protocol communication port                                                                             |
| cn\_target\_config\_node\_list | ConfigNode address to which the node is connected when it is registered to the cluster. Note that Only one ConfigNode can be configured. |

**Notice: The preceding configuration parameters cannot be changed after the node is started. Ensure that all ports are not occupied. Otherwise, the Node cannot be started.**

### 5.2.3 DataNode configuration

Open the DataNode configuration file ./conf/iotdb-datanode.properties,
and set the following parameters based on the IP address and available port of the server or VM:

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

**Notice: The preceding configuration parameters cannot be changed after the node is started. Ensure that all ports are not occupied. Otherwise, the Node cannot be started.**

# 6. Cluster Operation

## 6.1 Starting the cluster

This section describes how to start a cluster that includes several ConfigNodes and DataNodes.
The cluster can provide services only by starting at least one ConfigNode
and no less than the number of data/schema_replication_factor DataNodes.

The total process are three steps:

* Start the Seed-ConfigNode
* Add ConfigNode (Optional)
* Add DataNode

### 6.1.1 Start the Seed-ConfigNode

The first ConfigNode to start is the Seed-ConfigNode, which marks the creation of the new cluster.
Open the Seed-ConfigNode configuration file ./conf/iotdb-confignode.properties and set the following parameters:

| **Configuration**              | **Description**                                                                                    |
|--------------------------------|----------------------------------------------------------------------------------------------------|
| cn\_internal\_address          | Internal rpc service address of ConfigNode                                                         |
| cn\_internal\_port             | Internal rpc service port of ConfigNode                                                            |
| cn\_consensus\_port            | ConfigNode replication consensus protocol communication port                                       |
| cn\_target\_config\_node\_list | Set it to its own internal communication address, that is cn\_internal\_address:cn\_internal\_port |

After the configuration is complete, run the startup script:
```
# Linux foreground
bash ./sbin/start-confignode.sh

# Linux background
nohup bash ./sbin/start-confignode.sh >/dev/null 2>&1 &

# Windows
.\sbin\start-confignode.bat
```

For more details about other configuration parameters of ConfigNode, see the
[ConfigNode Configurations](https://iotdb.apache.org/UserGuide/Master/Reference/ConfigNode-Config-Manual.html).

### 6.1.2 Add ConfigNode (Optional)

You can add more ConfigNodes to the cluster to ensure high availability of ConfigNodes.
Ensure that all configuration parameters in the ./conf/iotdb-common.properites are the same as those in the Seed-ConfigNode; 
otherwise, it may fail to start or generate runtime errors.
Open the configuration file ./conf/iotdb-confignode.properties for the new ConfigNode and set the following parameters:

| **Configuration**              | **Description**                                                                                                                                            |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cn\_internal\_address          | Internal rpc service address of ConfigNode                                                                                                                 |
| cn\_internal\_port             | Internal rpc service port of ConfigNode                                                                                                                    |
| cn\_consensus\_port            | ConfigNode replication consensus protocol communication port                                                                                               |
| cn\_target\_config\_node\_list | Configure the internal communication address of an arbitrary running ConfigNode. The internal communication address of the seed ConfigNode is recommended. |

After the configuration is complete, run the startup script:
```
# Linux foreground
bash ./sbin/start-confignode.sh

# Linux background
nohup bash ./sbin/start-confignode.sh >/dev/null 2>&1 &

# Windows
.\sbin\start-confignode.bat
```

For more details about other configuration parameters of ConfigNode, see the
[ConfigNode Configurations](https://iotdb.apache.org/UserGuide/Master/Reference/ConfigNode-Config-Manual.html).

### 6.1.3 Start DataNode

You can add any number of DataNodes to the cluster.
Before adding a new DataNode, 
open the DataNode configuration file ./conf/iotdb-datanode.properties and configure the following parameters:

| **Configuration**                   | **Description**                                                                                                                                            |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dn\_rpc\_address                    | Client RPC Service address                                                                                                                                 |
| dn\_rpc\_port                       | Client RPC Service port                                                                                                                                    |
| dn\_internal\_address               | Control flow address of DataNode inside cluster                                                                                                            |
| dn\_internal\_port                  | Control flow port of DataNode inside cluster                                                                                                               |
| dn\_mpp\_data\_exchange\_port       | Data flow port of DataNode inside cluster                                                                                                                  |
| dn\_data\_region\_consensus\_port   | Data replicas communication port for consensus                                                                                                             |
| dn\_schema\_region\_consensus\_port | Schema replicas communication port for consensus                                                                                                           |
| dn\_target\_config\_node\_list      | Configure the internal communication address of an arbitrary running ConfigNode. The internal communication address of the seed ConfigNode is recommended. |

After the configuration is complete, run the startup script:
```
# Linux foreground
bash ./sbin/start-datanode.sh

# Linux background
nohup bash ./sbin/start-datanode.sh >/dev/null 2>&1 &

# Windows
.\sbin\start-datanode.bat
```

For more details about other configuration parameters of DataNode, see the
[DataNode Configurations](https://iotdb.apache.org/UserGuide/Master/Reference/DataNode-Config-Manual.html).

## 6.2 Stop IoTDB

This section describes how to manually shut down the ConfigNode or DataNode process of the IoTDB.

### 6.2.1 Stop ConfigNode by script

Run the stop ConfigNode script:
```
# Linux
./sbin/stop-confignode.sh

# Windows
.\sbin\stop-confignode.bat
```

### 6.2.2 Stop DataNode by script

Run the stop DataNode script:
```
# Linux
./sbin/stop-datanode.sh

# Windows
.\sbin\stop-datanode.bat
```

### 6.2.3 Kill Node process

Get the process number of the Node:
```
jps

# or

ps aux | grep iotdb
```

Kill the process：
```
kill -9 <pid>
```

**Notice Some ports require root access, in which case use sudo**

## 6.3 Start Cli

Run the Cli startup script in the ./sbin directory:
```
# Linux
./sbin/start-cli.sh

# Windows
.\sbin\start-cli.bat
```

## 6.4 Shrink the Cluster

This section describes how to remove ConfigNode or DataNode from the cluster.

### 6.4.1 Remove ConfigNode

Before removing a ConfigNode, ensure that there is at least one active ConfigNode in the cluster after the removal.
Run the remove-confignode script on an active ConfigNode:

```
# Linux
# Remove the ConfigNode with confignode_id
./sbin/remove-confignode.sh <confignode_id>

# Remove the ConfigNode with address:port
./sbin/remove-confignode.sh <cn_internal_address>:<cn_internal_port>


# Windows
# Remove the ConfigNode with confignode_id
.\sbin\remove-confignode.bat <confignode_id>

# Remove the ConfigNode with address:port
.\sbin\remove-confignode.bat <cn_internal_address>:<cn_internal_portcn_internal_port>
```

### 6.4.2 Remove DataNode

Before removing a DataNode, ensure that the cluster has at least the number of data/schema replicas DataNodes.
Run the remove-datanode script on an active DataNode:
```
# Linux
# Remove the DataNode with datanode_id
./sbin/remove-datanode.sh <datanode_id>

# Remove the DataNode with rpc address:port
./sbin/remove-datanode.sh <dn_rpc_address>:<dn_rpc_port>


# Windows
# Remove the DataNode with datanode_id
.\sbin\remove-datanode.bat <datanode_id>

# Remove the DataNode with rpc address:port
.\sbin\remove-datanode.bat <dn_rpc_address>:<dn_rpc_port>
```

# 7. FAQ

See [FAQ](https://iotdb.apache.org/UserGuide/Master/FAQ/Frequently-asked-questions.html)
