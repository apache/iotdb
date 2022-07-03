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

1. JDK>=1.8

2. Max open file 65535

3. Disable the swap memory

## Get the binary distribution

### Download

Download the binary distribution from website [Download Page](https://iotdb.apache.org/Download/)

### Compiled from source code

Download the source code:

```
git clone https://github.com/apache/iotdb.git
```

The default branch is master, you could checkout to the release tag:

```
git checkout v0.14.0-preview1
```

Under the source root folder:

```
mvn clean package -pl distribution -am -DskipTests
```

Then you will get the binary distribution under **distribution/target**, in which the **all-bin** contains ConfigNode and DataNode，and DataNode contains the Cli.

## Binary Distribution Content

| **Folder** | **Description**                                      |
| -------- | -------------------------------------------- |
| confignode |  Contains start/stop shell, configurations, logs, data of ConfigNode |
| datanode   | Contains start/stop shell, configurations, logs, data of DataNode, cli shell|
| grafana-metrics-example  | Grafana metric page module           |
| lib      | Jar files folder                                     |
| tools    | System tools                                   |

## Start the Cluster

Users could start a cluster which contains multiple ConfigNode and DataNode.
A cluster need at least one ConfigNode and no less than the number of data/schema_replication_factor DataNodes.

The total process are three steps:

* Start the first ConfigNode
* Add ConfigNode (Optional)
* Add DataNode

### Start the first ConfigNode

Please set the important parameters in iotdb-confignode.properties:

| **Configuration** | **Description**                                      |
| -------- | -------------------------------------------- |
| internal\_address    | Internal rpc service address of ConfigNode          |
| internal\_port    | Internal rpc service address of ConfigNode       |
| consensus\_port    | ConfigNode replication consensus protocol communication port    |
| target\_config\_nodes    | Target ConfigNode address, if the current is the first ConfigNode, then set its address:port    |
| data\_replication\_factor  | Data replication factor, no more than DataNode number        |
| data\_region\_consensus\_protocol\_class | Consensus protocol of data replicas |
| schema\_replication\_factor  | Schema replication factor, no more than DataNode number       |
| schema\_region\_consensus\_protocol\_class   | Consensus protocol of schema replicas |

Start on Linux
```
# Foreground
./confignode/sbin/start-confignode.sh

# Background
nohup ./confignode/sbin/start-confignode.sh >/dev/null 2>&1 &
```

Start on Windows
```
confignode\sbin\start-confignode.bat
```

More details  [ConfigNode Configurations](https://iotdb.apache.org/UserGuide/Master/Reference/ConfigNode-Config-Manual.html)

### Add ConfigNode (Optional)

This will add the replication factor of ConfigNode, except for the port couldn't conflict, make sure other configurations are the same with existing ConfigNode in Cluster.

The adding ConfigNode also use the start-confignode.sh/bat.

### Start DataNode

You could add any number of DataNode.

Please set the important parameters in iotdb-datanode.properties

| **Configuration** | **Description**                                      |
| -------- | -------------------------------------------- |
| rpc\_address    | Client RPC Service address         |
| rpc\_port    | Client RPC Service port           |
| internal\_address    | Control flow address of DataNode inside cluster         |
| internal\_port    | Control flow port of DataNode inside cluster           |
| mpp\_data\_exchange\_port    | Data flow port of DataNode inside cluster           |
| data\_region\_consensus\_port    | Data replicas communication port for consensus     |
| schema\_region\_consensus\_port    | Schema replicas communication port for consensus          |
| target\_config\_nodes    | Running ConfigNode of the Cluster      |

Start on Linux
```
# Foreground
./datanode/sbin/start-datanode.sh

# Background
nohup ./datanode/sbin/start-datanode.sh >/dev/null 2>&1 &
```

Start on Windows
```
datanode\sbin\start-datanode.bat
```

More details [DataNode Configurations](https://iotdb.apache.org/UserGuide/Master/Reference/DataNode-Config-Manual.html)

### Start Cli

Cli is in datanode/sbin folder

Start on Linux
```
./datanode/sbin/start-cli.sh
```

Start on Windows
```
datanode\sbin\start-cli.bat
```

## Quick Start

unzip apache-iotdb-0.14.0-preview1-all-bin.zip

Deploy a one-ConfigNode and one-DataNode（1C1D）cluster, default is one replica.
```
./confignode/sbin/start-confignode.sh
./datanode/sbin/start-datanode.sh
```

Start Cli
```
./datanode/sbin/start-cli.sh
```