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

## 集群设置

本文档为 IoTDB 集群版（1.0.0）启动教程。

## 安装环境

1. JDK>=1.8 的运行环境，并配置好 JAVA_HOME 环境变量。

2. 设置最大文件打开数为 65535。

3. 关闭交换内存。

## 安装包获取

### 下载

可以直接在官网下载二进制版本 [Download Page](https://iotdb.apache.org/Download/)。

### 源码编译

下载源码:

```
git clone https://github.com/apache/iotdb.git
```

默认分支为 master 分支，你可以切换到发布版本的 tag，例如：

```
git checkout v1.0.0
```

在 IoTDB 根目录下:

```
mvn clean package -pl distribution -am -DskipTests
```

集群的二进制版本在目录 **distribution/target** 下，其中，all-bin 包含 ConfigNode 和 DataNode，DataNode 内包含 Cli。

## 安装包说明

| **目录**                  | **说明**                                               |
|-------------------------|------------------------------------------------------|
| conf                    | 配置文件目录，包含 ConfigNode 和 DataNode 的配置文件                |
| data                    | 数据文件目录，包含 ConfigNode 和 DataNode 的数据文件                |
| grafana-metrics-example | Grafana 监控界面模板                                       |
| lib                     | 库文件目录                                                |
| licenses                | 证书文件目录                                               |
| logs                    | 日志文件目录，包含 ConfigNode 和 DataNode 的日志文件                |
| sbin                    | 脚本目录，包含 ConfigNode 和 DataNode 的启停移除脚本目录，以及 Cli 的启动脚本 |
| tools                   | 系统工具目录                                               |

## 启动集群

用户可以启动包括若干 ConfigNode 和 DataNode 的集群。
集群可以提供服务的标准是至少启动一个 ConfigNode 且启动 不小于（数据/元数据）副本个数 的 DataNode。

总体启动流程分为三步：

* 启动种子 ConfigNode
* 增加 ConfigNode（可选）
* 增加 DataNode

### 启动种子 ConfigNode

对 confignode/conf/iotdb-confignode.properties 和 conf/iotdb-common.properties 中的重要参数进行配置：
iotdb-confignode.properties：

| **配置项**                                   | **说明**                                             |
|-------------------------------------------|----------------------------------------------------|
| cn\_internal\_address                     | ConfigNode 在集群内部通讯使用的地址                            |
| cn\_internal\_port                        | ConfigNode 在集群内部通讯使用的端口                            |
| cn\_consensus\_port                       | ConfigNode 副本组共识协议通信使用的端口                          |
| cn\_target\_config\_node\_list            | 种子 ConfigNode 地址，第一个 ConfigNode 配置自己的 address:port |

iotdb-common.properties：

| **配置项**                                    | **说明**                    |
|--------------------------------------------|---------------------------|
| data\_replication\_factor                  | 数据副本数，DataNode 数量不应少于此数目  |
| data\_region\_consensus\_protocol\_class   | 数据副本组的共识协议                |
| schema\_replication\_factor                | 元数据副本数，DataNode 数量不应少于此数目 |
| schema\_region\_consensus\_protocol\_class | 元数据副本组的共识协议               |

Linux 启动方式：
```
# 前台启动
bash ./sbin/start-confignode.sh

# 后台启动
nohup bash ./sbin/start-confignode.sh >/dev/null 2>&1 &
```

Windows 启动方式：
```
sbin\start-confignode.bat
```

具体参考 [ConfigNode配置参数](https://iotdb.apache.org/zh/UserGuide/Master/Reference/ConfigNode-Config-Manual.html)

### 增加 ConfigNode（可选）

增加 ConfigNode 是一个扩容操作，除 IP 和端口不能冲突外，上述其它重要参数需要与集群已有的 ConfigNode 保持一致，并将 cn\_target\_config\_nodes\_list 配置为集群活跃的 ConfigNode。

启动方式同上。

### 增加 DataNode

可以向集群中添加任意个 DataNode。

iotdb-datanode.properties 中的重要配置如下：

| **配置项**                             | **说明**                    |
|-------------------------------------|---------------------------|
| dn\_rpc\_address                    | 客户端 RPC 服务的地址             |
| dn\_rpc\_port                       | 客户端 RPC 服务的端口             |
| dn\_internal\_address               | DataNode 在集群内部接收控制流使用的地址  |
| dn\_internal\_port                  | DataNode 在集群内部接收控制流使用的端口  |
| dn\_mpp\_data\_exchange\_port       | DataNode 在集群内部接收数据流使用的端口  |
| dn\_data\_region\_consensus\_port   | DataNode 的数据副本间共识协议通信的端口  |
| dn\_schema\_region\_consensus\_port | DataNode 的元数据副本间共识协议通信的端口 |
| dn\_target\_config\_node\_list      | 集群中正在运行的 ConfigNode 地址    |


Linux 启动方式：
```
# 前台启动
bash ./sbin/start-datanode.sh

# 后台启动
nohup bash ./sbin/start-datanode.sh >/dev/null 2>&1 &
```

Windows 启动方式：
```
sbin\start-datanode.bat
```

具体参考 [DataNode配置参数](https://iotdb.apache.org/zh/UserGuide/Master/Reference/DataNode-Config-Manual.html)。

### 停止 IoTDB 进程
如果你碰到了问题，希望手动关闭 IoTDB 的 ConfigNode 和 DataNode 进程，可以使用我们的脚本。

在 Windows 上:

```
sbin\stop-datanode.bat
```
```
sbin\stop-confignode.bat
```
在 Linux 上:
```
sudo bash sbin\stop-datanode.sh
```
```
sudo bash sbin\stop-confignode.sh
```

注意不要遗漏 ”sudo“ 的标签，因为一些端口信息的获取需要 root 权限。如果无法 sudo 或遇到其他问题，可以使用 jps 或 ps aux | grep iotdb 的命令来获取 IoTDB 的进程，然后使用 kill -9 进程号来结束此进程。
## 启动单机
除了集群之外， 我们的脚本也提供了单机 1C1D（也就是1个Confignode + 1个Datanode） 的便捷启动方式。

在不更改配置文件的情况下，该脚本可以成功执行。

Windows 启动方式:
```
sbin\start-datanode.bat
```
Linux 启动方式:
```
sudo bash sbin\start-standalone.sh
```
也可以使用脚本直接关闭这些进程。

Windows 停止方式:
```
sbin\stop-datanode.bat
```
Linux 停止方式:
```
sudo bash sbin\stop-standalone.sh
```
注意： 在 Linux 平台上，1C1D 的两个进程都在后台启动，可以查看 confignode1.log 和 datanode1.log 来找到它们的运行日志。

一般来说，stop-standalone.sh需要sudo权限，因为iotdb的端口信息在非sudo下可能是隐形的。如果 stop-standalone.sh
出现错误，可以使用“jps”命令或“ps aux | grep iotdb“命令来查看iotdb的进程，再使用" kill -9 <进程号>"的方式来停止它们。

### 启动 Cli

Cli 启动脚本在 sbin 目录。

Linux 启动方式：
```
./sbin/start-cli.sh
```

Windows 启动方式：
```
sbin\start-cli.bat
```

## 集群缩容

### 移除 ConfigNode

需要在活跃的 ConfigNode 上执行 remove-confignode 脚本，且保证移除后集群中至少有一个活跃的 ConfigNode。

Linux 移除方式：
```
# 根据 confignode_id 移除节点
./sbin/remove-confignode.sh <confignode_id>

# 根据 ConfigNode 内部通讯地址和端口移除节点
./sbin/remove-confignode.sh <internal_address>:<internal_port>
```

Windows 移除方式：
```
# 根据 confignode_id 移除节点
sbin\remove-confignode.bat <confignode_id>

# 根据 ConfigNode 内部通讯地址和端口移除节点
sbin\remove-confignode.bat <internal_address>:<internal_port>
```

### 移除 DataNode

需要在活跃的 DataNode 上执行 remove-datanode 脚本，且保证移除后集群中至少有不少于（数据/元数据）副本个数 的 DataNode。

Linux 移除方式：

```
# 根据 datanode_id 移除节点
./sbin/remove-datanode.sh <datanode_id>

# 根据 DataNode RPC 服务地址和端口移除节点
./sbin/remove-datanode.sh <rpc_address>:<rpc_port>
```

Windows 移除方式：

```
# 根据 datanode_id 移除节点
sbin\remove-datanode.bat <datanode_id>

# 根据 DataNode RPC 服务地址和端口移除节点
sbin\remove-datanode.bat <rpc_address>:<rpc_port>
```

## 快速上手

以本地环境为例，演示 IoTDB 集群的启动、扩容与缩容：

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

### 4. 准备扩容环境

解压 apache-iotdb-1.0.0-all-bin.zip 至 cluster1 目录和 cluster2 目录

### 5. 修改节点配置文件

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

### 8. 集群缩容

+ 缩容一个 ConfigNode：
```
./cluster0/sbin/remove-confignode.sh 127.0.0.1:22279
```

+ 缩容一个 DataNode：
```
./cluster0/sbin/remove-datanode.sh 127.0.0.1:6668
```

### 9. 验证缩容结果

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
