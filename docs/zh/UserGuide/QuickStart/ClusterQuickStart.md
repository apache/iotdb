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

## 集群快速上手

以本地环境为例，演示 IoTDB 集群的启动、扩容与缩容。

**注意：本文档为使用本地不同端口，进行伪分布式环境部署的教程，仅用于练习。在真实环境部署时，一般不需要修改节点端口，仅需配置节点 IPV4 地址或域名即可。**

### 1. 准备启动环境

解压 apache-iotdb-1.0.0-all-bin.zip 至 cluster0 目录。

### 2. 启动最小集群

在 Linux 环境中，部署 1 个 ConfigNode 和 1 个 DataNode（1C1D）集群版，默认 1 副本：
```
./cluster0/sbin/start-confignode.sh
./cluster0/sbin/start-datanode.sh
```

### 3. 验证最小集群

+ 最小集群启动成功，启动 Cli 进行验证：
```
./cluster0/sbin/start-cli.sh
```

+ 在 Cli 执行 [show cluster details](https://iotdb.apache.org/zh/UserGuide/Master/Maintenance-Tools/Maintenance-Command.html#%E6%9F%A5%E7%9C%8B%E5%85%A8%E9%83%A8%E8%8A%82%E7%82%B9%E4%BF%A1%E6%81%AF)
  指令，结果如下所示：
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

### 4. 准备扩容环境

解压 apache-iotdb-1.0.0-all-bin.zip 至 cluster1 目录和 cluster2 目录

### 5. 修改节点配置文件

对于 cluster1 目录：

+ 修改 ConfigNode 配置：

| **配置项**                        | **值**           |
|--------------------------------|-----------------|
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 10711           |
| cn\_consensus\_port            | 10721           |
| cn\_target\_config\_node\_list | 127.0.0.1:10710 |

+ 修改 DataNode 配置：

| **配置项**                                | **值**           |
|----------------------------------------|-----------------|
| dn\_rpc\_address                       | 127.0.0.1       |
| dn\_rpc\_port                          | 6668            |
| dn\_internal\_address                  | 127.0.0.1       |
| dn\_internal\_port                     | 10731           |
| dn\_mpp\_data\_exchange\_port          | 10741           |
| dn\_schema\_region\_consensus\_port    | 10751           |
| dn\_data\_region\_consensus\_port      | 10761           |
| dn\_target\_config\_node\_list         | 127.0.0.1:10710 |

对于 cluster2 目录：

+ 修改 ConfigNode 配置：

| **配置项**                        | **值**           |
|--------------------------------|-----------------|
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 10712           |
| cn\_consensus\_port            | 10722           |
| cn\_target\_config\_node\_list | 127.0.0.1:10710 |

+ 修改 DataNode 配置：

| **配置项**                              | **值**           |
|--------------------------------------|-----------------|
| dn\_rpc\_address                     | 127.0.0.1       |
| dn\_rpc\_port                        | 6669            |
| dn\_internal\_address                | 127.0.0.1       |
| dn\_internal\_port                   | 10732           |
| dn\_mpp\_data\_exchange\_port        | 10742           |
| dn\_schema\_region\_consensus\_port  | 10752           |
| dn\_data\_region\_consensus\_port    | 10762           |
| dn\_target\_config\_node\_list       | 127.0.0.1:10710 |

### 6. 集群扩容

将集群扩容至 3 个 ConfigNode 和 3 个 DataNode（3C3D）集群版，
指令执行顺序为先启动 ConfigNode，再启动 DataNode：
```
./cluster1/sbin/start-confignode.sh
./cluster2/sbin/start-confignode.sh
./cluster1/sbin/start-datanode.sh
./cluster2/sbin/start-datanode.sh
```

### 7. 验证扩容结果

在 Cli 执行 `show cluster details`，结果如下：
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

### 8. 集群缩容

+ 缩容一个 ConfigNode：
```
# 使用 ip:port 移除
./cluster0/sbin/remove-confignode.sh 127.0.0.1:10711

# 使用节点编号移除
./cluster0/sbin/remove-confignode.sh 2
```

+ 缩容一个 DataNode：
```
# 使用 ip:port 移除
./cluster0/sbin/remove-datanode.sh 127.0.0.1:6668

# 使用节点编号移除
./cluster0/sbin/remove-confignode.sh 4
```

### 9. 验证缩容结果

在 Cli 执行 `show cluster details`，结果如下：
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