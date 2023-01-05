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

## Quick Start

This article uses a local environment as an example to
illustrate how to start, expand, and shrink an IoTDB Cluster.

**Notice: This document is a tutorial for deploying in a pseudo-cluster environment using different local ports, and is for exercise only. In real deployment scenarios, you only need to configure the IPV4 address or domain name of the server, and do not need to change the Node ports.**

### 1. Prepare the Start Environment

Unzip the apache-iotdb-1.0.0-all-bin.zip file to cluster0 folder.

### 2. Start a Minimum Cluster

Start the Cluster version with one ConfigNode and one DataNode(1C1D), and
the default number of replicas is one.
```
./cluster0/sbin/start-confignode.sh
./cluster0/sbin/start-datanode.sh
```

### 3. Verify the Minimum Cluster

+ If everything goes well, the minimum cluster will start successfully. Then, we can start the Cli for verification.
```
./cluster0/sbin/start-cli.sh
```

+ Execute the [show cluster details](https://iotdb.apache.org/UserGuide/Master/Maintenance-Tools/Maintenance-Command.html#show-all-node-information)
  command on the Cli. The result is shown below:
```
IoTDB> show cluster details
+------+----------+-------+---------------+------------+-------------------+----------+-------+--------+-------------------+-----------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|ConfigConsensusPort|RpcAddress|RpcPort|MppPort |SchemaConsensusPort|DataConsensusPort|
+------+----------+-------+---------------+------------+-------------------+----------+-------+--------+-------------------+-----------------+
|     0|ConfigNode|Running|      127.0.0.1|       10710|              10720|          |       |        |                   |                 |
|     1|  DataNode|Running|      127.0.0.1|       10730|                   | 127.0.0.1|   6667|   10740|              10750|            10760|
+------+----------+-------+---------------+------------+-------------------+----------+-------+--------+-------------------+-----------------+
Total line number = 2
It costs 0.242s
```

### 4. Prepare the Expanding Environment

Unzip the apache-iotdb-1.0.0-all-bin.zip file to cluster1 and cluster2 folder.

### 5. Modify the Node Configuration file

For folder cluster1:

+ Modify ConfigNode configurations:

| **configuration item**         | **value**       |
|--------------------------------|-----------------|
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 10711           |
| cn\_consensus\_port            | 10721           |
| cn\_target\_config\_node\_list | 127.0.0.1:10710 |

+ Modify DataNode configurations:

| **configuration item**              | **value**       |
|-------------------------------------|-----------------|
| dn\_rpc\_address                    | 127.0.0.1       |
| dn\_rpc\_port                       | 6668            |
| dn\_internal\_address               | 127.0.0.1       |
| dn\_internal\_port                  | 10731           |
| dn\_mpp\_data\_exchange\_port       | 10741           |
| dn\_schema\_region\_consensus\_port | 10751           |
| dn\_data\_region\_consensus\_port   | 10761           |
| dn\_target\_config\_node\_list      | 127.0.0.1:10710 |

For folder cluster2:

+ Modify ConfigNode configurations:

| **configuration item**         | **value**       |
|--------------------------------|-----------------|
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 10712           |
| cn\_consensus\_port            | 10722           |
| cn\_target\_config\_node\_list | 127.0.0.1:10710 |

+ Modify DataNode configurations:

| **configuration item**                 | **value**       |
|----------------------------------------|-----------------|
| dn\_rpc\_address                       | 127.0.0.1       |
| dn\_rpc\_port                          | 6669            |
| dn\_internal\_address                  | 127.0.0.1       |
| dn\_internal\_port                     | 10732           |
| dn\_mpp\_data\_exchange\_port          | 10742           |
| dn\_schema\_region\_consensus\_port    | 10752           |
| dn\_data\_region\_consensus\_port      | 10762           |
| dn\_target\_config\_node\_list         | 127.0.0.1:10710 |

### 6. Expanding the Cluster

Expanding the Cluster to three ConfigNode and three DataNode(3C3D).
The following commands can be executed in arbitrary order.

```
./cluster1/sbin/start-confignode.sh
./cluster1/sbin/start-datanode.sh
./cluster2/sbin/start-confignode.sh
./cluster2/sbin/start-datanode.sh
```

### 7. Verify Cluster expansion

Execute the `show cluster details` command, then the result is shown below:
```
IoTDB> show cluster details
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|ConfigConsensusPort|RpcAddress|RpcPort|MppPort|SchemaConsensusPort|DataConsensusPort|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|     0|ConfigNode|Running|      127.0.0.1|       10710|              10720|          |       |       |                   |                 |
|     2|ConfigNode|Running|      127.0.0.1|       10711|              10721|          |       |       |                   |                 |
|     3|ConfigNode|Running|      127.0.0.1|       10712|              10722|          |       |       |                   |                 |
|     1|  DataNode|Running|      127.0.0.1|       10730|                   | 127.0.0.1|   6667|  10740|              10750|            10760|
|     4|  DataNode|Running|      127.0.0.1|       10731|                   | 127.0.0.1|   6668|  10741|              10751|            10761|
|     5|  DataNode|Running|      127.0.0.1|       10732|                   | 127.0.0.1|   6669|  10742|              10752|            10762|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
Total line number = 6
It costs 0.012s
```

### 8. Shrinking the Cluster

+ Remove a ConfigNode:
```
# Removing by ip:port
./cluster0/sbin/remove-confignode.sh 127.0.0.1:10711

# Removing by Node index
./cluster0/sbin/remove-confignode.sh 2
```

+ Remove a DataNode:
```
# Removing by ip:port
./cluster0/sbin/remove-datanode.sh 127.0.0.1:6668

# Removing by Node index
./cluster0/sbin/remove-confignode.sh 4
```

### 9. Verify Cluster shrinkage

Execute the `show cluster details` command, then the result is shown below:
```
IoTDB> show cluster details
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|ConfigConsensusPort|RpcAddress|RpcPort|MppPort|SchemaConsensusPort|DataConsensusPort|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|     0|ConfigNode|Running|      127.0.0.1|       10710|              10720|          |       |       |                   |                 |
|     3|ConfigNode|Running|      127.0.0.1|       10712|              10722|          |       |       |                   |                 |
|     1|  DataNode|Running|      127.0.0.1|       10730|                   | 127.0.0.1|   6667|  10740|              10750|            10760|
|     5|  DataNode|Running|      127.0.0.1|       10732|                   | 127.0.0.1|   6669|  10742|              10752|            10762|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
Total line number = 4
It costs 0.005s
```