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

# 快速上手

以本地环境为例，演示 IoTDB 集群的启动、扩容与缩容：

## 1. 准备启动环境

解压 apache-iotdb-1.0.0-all-bin.zip 至 cluster0 目录。

## 2. 启动最小集群

在 Linux 环境中，部署 1 个 ConfigNode 和 1 个 DataNode（1C1D）集群版，默认 1 副本：
```
./cluster0/sbin/start-confignode.sh
./cluster0/sbin/start-datanode.sh
```

## 3. 验证最小集群

+ 最小集群启动成功，启动 Cli 进行验证：
```
./cluster0/sbin/start-cli.sh
```

+ 在 Cli 执行 [show cluster](https://iotdb.apache.org/zh/UserGuide/Master/Maintenance-Tools/Maintenance-Command.html#%E6%9F%A5%E7%9C%8B%E5%85%A8%E9%83%A8%E8%8A%82%E7%82%B9%E4%BF%A1%E6%81%AF)
  指令，结果如下所示：
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

## 4. 准备扩容环境

解压 apache-iotdb-1.0.0-all-bin.zip 至 cluster1 目录和 cluster2 目录

## 5. 修改节点配置文件

对于 cluster1 目录：

+ 修改 ConfigNode 配置：

| **配置项**                        | **值**           |
|--------------------------------|-----------------|
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 22279           |
| cn\_consensus\_port            | 22280           |
| cn\_target\_config\_node\_list | 127.0.0.1:22277 |

+ 修改 DataNode 配置：

| **配置项**                             | **值**           |
|-------------------------------------|-----------------|
| dn\_rpc\_address                    | 127.0.0.1       |
| dn\_rpc\_port                       | 6668            |
| dn\_internal\_address               | 127.0.0.1       |
| dn\_internal\_port                  | 9004            |
| dn\_mpp\_data\_exchange\_port       | 8778            |
| dn\_data\_region\_consensus\_port   | 40011           |
| dn\_schema\_region\_consensus\_port | 50011           |
| dn\_target\_config\_node\_list      | 127.0.0.1:22277 |

对于 cluster2 目录：

+ 修改 ConfigNode 配置：

| **配置项**                        | **值**           |
|--------------------------------|-----------------|
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 22281           |
| cn\_consensus\_port            | 22282           |
| cn\_target\_config\_node\_list | 127.0.0.1:22277 |

+ 修改 DataNode 配置：

| **配置项**                             | **值**           |
|-------------------------------------|-----------------|
| dn\_rpc\_address                    | 127.0.0.1       |
| dn\_rpc\_port                       | 6669            |
| dn\_internal\_address               | 127.0.0.1       |
| dn\_internal\_port                  | 9005            |
| dn\_mpp\_data\_exchange\_port       | 8779            |
| dn\_data\_region\_consensus\_port   | 40012           |
| dn\_schema\_region\_consensus\_port | 50012           |
| dn\_target\_config\_node\_list      | 127.0.0.1:22277 |

## 6. 集群扩容

将集群扩容至 3 个 ConfigNode 和 3 个 DataNode（3C3D）集群版，
指令执行顺序为先启动 ConfigNode，再启动 DataNode：
```
./cluster1/sbin/start-confignode.sh
./cluster2/sbin/start-confignode.sh
./cluster1/sbin/start-datanode.sh
./cluster2/sbin/start-datanode.sh
```

## 7. 验证扩容结果

在 Cli 执行 show cluster，结果如下：
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

## 8. 集群缩容

+ 缩容一个 ConfigNode：
```
./cluster0/sbin/remove-confignode.sh 127.0.0.1:22279
```

+ 缩容一个 DataNode：
```
./cluster0/sbin/remove-datanode.sh 127.0.0.1:6668
```

## 9. 验证缩容结果

在 Cli 执行 show cluster，结果如下：
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