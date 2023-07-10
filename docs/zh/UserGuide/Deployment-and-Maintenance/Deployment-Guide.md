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

# 部署指导

## 单机版部署

本文将介绍关于 IoTDB 使用的基本流程，如果需要更多信息，请浏览我们官网的 [指引](../IoTDB-Introduction/What-is-IoTDB.md).

### 安装环境

安装前需要保证设备上配有 JDK>=1.8 的运行环境，并配置好 JAVA_HOME 环境变量。

设置最大文件打开数为 65535。

### 安装步骤

IoTDB 支持多种安装途径。用户可以使用三种方式对 IoTDB 进行安装——下载二进制可运行程序、使用源码、使用 docker 镜像。

* 使用源码：您可以从代码仓库下载源码并编译，具体编译方法见下方。

* 二进制可运行程序：请从 [下载](https://iotdb.apache.org/Download/) 页面下载最新的安装包，解压后即完成安装。

* 使用 Docker 镜像：dockerfile 文件位于[github](https://github.com/apache/iotdb/blob/master/docker/src/main)

### 软件目录结构

* sbin 启动和停止脚本目录
* conf 配置文件目录
* tools 系统工具目录
* lib 依赖包目录

### IoTDB 试用

用户可以根据以下操作对 IoTDB 进行简单的试用，若以下操作均无误，则说明 IoTDB 安装成功。

#### 启动 IoTDB

IoTDB 是一个基于分布式系统的数据库。要启动 IoTDB ，你可以先启动单机版（一个 ConfigNode 和一个 DataNode）来检查安装。

用户可以使用 sbin 文件夹下的 start-standalone 脚本启动 IoTDB。

Linux 系统与 MacOS 系统启动命令如下：

```
> bash sbin/start-standalone.sh
```

Windows 系统启动命令如下：

```
> sbin\start-standalone.bat
```

注意：目前，要使用单机模式，你需要保证所有的地址设置为 127.0.0.1，如果需要从非 IoTDB 所在的机器访问此IoTDB，请将配置项 `dn_rpc_address` 修改为 IoTDB 所在的机器 IP。副本数设置为1。并且，推荐使用 SimpleConsensus，因为这会带来额外的效率。这些现在都是默认配置。

## 集群版部署
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

| **配置项**                     | **值**          |
| ------------------------------ | --------------- |
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 10711           |
| cn\_consensus\_port            | 10721           |
| cn\_target\_config\_node\_list | 127.0.0.1:10710 |

+ 修改 DataNode 配置：

| **配置项**                          | **值**          |
| ----------------------------------- | --------------- |
| dn\_rpc\_address                    | 127.0.0.1       |
| dn\_rpc\_port                       | 6668            |
| dn\_internal\_address               | 127.0.0.1       |
| dn\_internal\_port                  | 10731           |
| dn\_mpp\_data\_exchange\_port       | 10741           |
| dn\_schema\_region\_consensus\_port | 10751           |
| dn\_data\_region\_consensus\_port   | 10761           |
| dn\_target\_config\_node\_list      | 127.0.0.1:10710 |

对于 cluster2 目录：

+ 修改 ConfigNode 配置：

| **配置项**                     | **值**          |
| ------------------------------ | --------------- |
| cn\_internal\_address          | 127.0.0.1       |
| cn\_internal\_port             | 10712           |
| cn\_consensus\_port            | 10722           |
| cn\_target\_config\_node\_list | 127.0.0.1:10710 |

+ 修改 DataNode 配置：

| **配置项**                          | **值**          |
| ----------------------------------- | --------------- |
| dn\_rpc\_address                    | 127.0.0.1       |
| dn\_rpc\_port                       | 6669            |
| dn\_internal\_address               | 127.0.0.1       |
| dn\_internal\_port                  | 10732           |
| dn\_mpp\_data\_exchange\_port       | 10742           |
| dn\_schema\_region\_consensus\_port | 10752           |
| dn\_data\_region\_consensus\_port   | 10762           |
| dn\_target\_config\_node\_list      | 127.0.0.1:10710 |

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

## 手动部署

### 前置检查

1. JDK>=1.8 的运行环境，并配置好 JAVA_HOME 环境变量。
2. 设置最大文件打开数为 65535。
3. 关闭交换内存。
4. 首次启动ConfigNode节点时，确保已清空ConfigNode节点的data/confignode目录；首次启动DataNode节点时，确保已清空DataNode节点的data/datanode目录。
5. 如果整个集群处在可信环境下，可以关闭机器上的防火墙选项。
6. 在集群默认配置中，ConfigNode 会占用端口 10710 和 10720，DataNode 会占用端口 6667、10730、10740、10750 和 10760，
    请确保这些端口未被占用，或者手动修改配置文件中的端口配置。

### 安装包获取

你可以选择下载二进制文件（见 3.1）或从源代码编译（见 3.2）。

#### 下载二进制文件

1. 打开官网[Download Page](https://iotdb.apache.org/Download/)。
2. 下载 IoTDB 1.0.0 版本的二进制文件。
3. 解压得到 apache-iotdb-1.0.0-all-bin 目录。

#### 使用源码编译

##### 下载源码

**Git**

```
git clone https://github.com/apache/iotdb.git
git checkout v1.0.0
```

**官网下载**

1. 打开官网[Download Page](https://iotdb.apache.org/Download/)。
2. 下载 IoTDB 1.0.0 版本的源码。
3. 解压得到 apache-iotdb-1.0.0 目录。

##### 编译源码

在 IoTDB 源码根目录下:

```
mvn clean package -pl distribution -am -DskipTests
```

编译成功后，可在目录 
**distribution/target/apache-iotdb-1.0.0-SNAPSHOT-all-bin/apache-iotdb-1.0.0-SNAPSHOT-all-bin** 
找到集群版本的二进制文件。

### 安装包说明

打开 apache-iotdb-1.0.0-SNAPSHOT-all-bin，可见以下目录：

| **目录** | **说明**                                                     |
| -------- | ------------------------------------------------------------ |
| conf     | 配置文件目录，包含 ConfigNode、DataNode、JMX 和 logback 等配置文件 |
| data     | 数据文件目录，包含 ConfigNode 和 DataNode 的数据文件         |
| lib      | 库文件目录                                                   |
| licenses | 证书文件目录                                                 |
| logs     | 日志文件目录，包含 ConfigNode 和 DataNode 的日志文件         |
| sbin     | 脚本目录，包含 ConfigNode 和 DataNode 的启停移除脚本，以及 Cli 的启动脚本等 |
| tools    | 系统工具目录                                                 |

### 集群安装配置

#### 集群安装

`apache-iotdb-1.0.0-SNAPSHOT-all-bin` 包含 ConfigNode 和 DataNode，
请将安装包部署于你目标集群的所有机器上，推荐将安装包部署于所有服务器的相同目录下。

如果你希望先在一台服务器上尝试部署 IoTDB 集群，请参考
[Cluster Quick Start](https://iotdb.apache.org/zh/UserGuide/Master/QuickStart/ClusterQuickStart.html)。

#### 集群配置

接下来需要修改每个服务器上的配置文件，登录服务器，
并将工作路径切换至 `apache-iotdb-1.0.0-SNAPSHOT-all-bin`，
配置文件在 `./conf` 目录内。

对于所有部署 ConfigNode 的服务器，需要修改通用配置（见 5.2.1）和 ConfigNode 配置（见 5.2.2）。

对于所有部署 DataNode 的服务器，需要修改通用配置（见 5.2.1）和 DataNode 配置（见 5.2.3）。

##### 通用配置

打开通用配置文件 ./conf/iotdb-common.properties，
可根据 [部署推荐](https://iotdb.apache.org/zh/UserGuide/Master/Cluster/Deployment-Recommendation.html)
设置以下参数：

| **配置项**                                 | **说明**                                                     | **默认**                                        |
| ------------------------------------------ | ------------------------------------------------------------ | ----------------------------------------------- |
| cluster\_name                              | 节点希望加入的集群的名称                                     | defaultCluster                                  |
| config\_node\_consensus\_protocol\_class   | ConfigNode 使用的共识协议                                    | org.apache.iotdb.consensus.ratis.RatisConsensus |
| schema\_replication\_factor                | 元数据副本数，DataNode 数量不应少于此数目                    | 1                                               |
| schema\_region\_consensus\_protocol\_class | 元数据副本组的共识协议                                       | org.apache.iotdb.consensus.ratis.RatisConsensus |
| data\_replication\_factor                  | 数据副本数，DataNode 数量不应少于此数目                      | 1                                               |
| data\_region\_consensus\_protocol\_class   | 数据副本组的共识协议。注：RatisConsensus 目前不支持多数据目录 | org.apache.iotdb.consensus.iot.IoTConsensus     |

**注意：上述配置项在集群启动后即不可更改，且务必保证所有节点的通用配置完全一致，否则节点无法启动。**

##### ConfigNode 配置

打开 ConfigNode 配置文件 ./conf/iotdb-confignode.properties，根据服务器/虚拟机的 IP 地址和可用端口，设置以下参数：

| **配置项**                     | **说明**                                                     | **默认**        | **用法**                                                     |
| ------------------------------ | ------------------------------------------------------------ | --------------- | ------------------------------------------------------------ |
| cn\_internal\_address          | ConfigNode 在集群内部通讯使用的地址                          | 127.0.0.1       | 设置为服务器的 IPV4 地址或域名                               |
| cn\_internal\_port             | ConfigNode 在集群内部通讯使用的端口                          | 10710           | 设置为任意未占用端口                                         |
| cn\_consensus\_port            | ConfigNode 副本组共识协议通信使用的端口                      | 10720           | 设置为任意未占用端口                                         |
| cn\_target\_config\_node\_list | 节点注册加入集群时连接的 ConfigNode 的地址。注：只能配置一个 | 127.0.0.1:10710 | 对于 Seed-ConfigNode，设置为自己的 cn\_internal\_address:cn\_internal\_port；对于其它 ConfigNode，设置为另一个正在运行的 ConfigNode 的 cn\_internal\_address:cn\_internal\_port |

**注意：上述配置项在节点启动后即不可更改，且务必保证所有端口均未被占用，否则节点无法启动。**

##### DataNode 配置

打开 DataNode 配置文件 ./conf/iotdb-datanode.properties，根据服务器/虚拟机的 IP 地址和可用端口，设置以下参数：

| **配置项**                          | **说明**                                  | **默认**        | **用法**                                                     |
| ----------------------------------- | ----------------------------------------- | --------------- | ------------------------------------------------------------ |
| dn\_rpc\_address                    | 客户端 RPC 服务的地址                     | 127.0.0.1       | 设置为服务器的 IPV4 地址或域名                               |
| dn\_rpc\_port                       | 客户端 RPC 服务的端口                     | 6667            | 设置为任意未占用端口                                         |
| dn\_internal\_address               | DataNode 在集群内部接收控制流使用的地址   | 127.0.0.1       | 设置为服务器的 IPV4 地址或域名                               |
| dn\_internal\_port                  | DataNode 在集群内部接收控制流使用的端口   | 10730           | 设置为任意未占用端口                                         |
| dn\_mpp\_data\_exchange\_port       | DataNode 在集群内部接收数据流使用的端口   | 10740           | 设置为任意未占用端口                                         |
| dn\_data\_region\_consensus\_port   | DataNode 的数据副本间共识协议通信的端口   | 10750           | 设置为任意未占用端口                                         |
| dn\_schema\_region\_consensus\_port | DataNode 的元数据副本间共识协议通信的端口 | 10760           | 设置为任意未占用端口                                         |
| dn\_target\_config\_node\_list      | 集群中正在运行的 ConfigNode 地址          | 127.0.0.1:10710 | 设置为任意正在运行的 ConfigNode 的 cn\_internal\_address:cn\_internal\_port，可设置多个，用逗号（","）隔开 |

**注意：上述配置项在节点启动后即不可更改，且务必保证所有端口均未被占用，否则节点无法启动。**

### 集群操作

#### 启动集群

本小节描述如何启动包括若干 ConfigNode 和 DataNode 的集群。
集群可以提供服务的标准是至少启动一个 ConfigNode 且启动 不小于（数据/元数据）副本个数 的 DataNode。

总体启动流程分为三步：

1. 启动种子 ConfigNode
2. 增加 ConfigNode（可选）
3. 增加 DataNode

##### 启动 Seed-ConfigNode

**集群第一个启动的节点必须是 ConfigNode，第一个启动的 ConfigNode 必须遵循本小节教程。**

第一个启动的 ConfigNode 是 Seed-ConfigNode，标志着新集群的创建。
在启动 Seed-ConfigNode 前，请打开通用配置文件 ./conf/iotdb-common.properties，并检查如下参数：

| **配置项**                                 | **检查**                   |
| ------------------------------------------ | -------------------------- |
| cluster\_name                              | 已设置为期望的集群名称     |
| config\_node\_consensus\_protocol\_class   | 已设置为期望的共识协议     |
| schema\_replication\_factor                | 已设置为期望的元数据副本数 |
| schema\_region\_consensus\_protocol\_class | 已设置为期望的共识协议     |
| data\_replication\_factor                  | 已设置为期望的数据副本数   |
| data\_region\_consensus\_protocol\_class   | 已设置为期望的共识协议     |

**注意：** 请根据[部署推荐](https://iotdb.apache.org/zh/UserGuide/Master/Cluster/Deployment-Recommendation.html)配置合适的通用参数，这些参数在首次配置后即不可修改。

接着请打开它的配置文件 ./conf/iotdb-confignode.properties，并检查如下参数：

| **配置项**                     | **检查**                                                     |
| ------------------------------ | ------------------------------------------------------------ |
| cn\_internal\_address          | 已设置为服务器的 IPV4 地址或域名                             |
| cn\_internal\_port             | 该端口未被占用                                               |
| cn\_consensus\_port            | 该端口未被占用                                               |
| cn\_target\_config\_node\_list | 已设置为自己的内部通讯地址，即 cn\_internal\_address:cn\_internal\_port |

检查完毕后，即可在服务器上运行启动脚本：

```
# Linux 前台启动
bash ./sbin/start-confignode.sh

# Linux 后台启动
nohup bash ./sbin/start-confignode.sh >/dev/null 2>&1 &

# Windows
.\sbin\start-confignode.bat
```

ConfigNode 的其它配置参数可参考
[ConfigNode 配置参数](https://iotdb.apache.org/zh/UserGuide/Master/Reference/ConfigNode-Config-Manual.html)。

##### 增加更多 ConfigNode（可选）

**只要不是第一个启动的 ConfigNode 就必须遵循本小节教程。**

可向集群添加更多 ConfigNode，以保证 ConfigNode 的高可用。常用的配置为额外增加两个 ConfigNode，使集群共有三个 ConfigNode。

新增的 ConfigNode 需要保证 ./conf/iotdb-common.properites 中的所有配置参数与 Seed-ConfigNode 完全一致，否则可能启动失败或产生运行时错误。
因此，请着重检查通用配置文件中的以下参数：

| **配置项**                                 | **检查**                    |
| ------------------------------------------ | --------------------------- |
| cluster\_name                              | 与 Seed-ConfigNode 保持一致 |
| config\_node\_consensus\_protocol\_class   | 与 Seed-ConfigNode 保持一致 |
| schema\_replication\_factor                | 与 Seed-ConfigNode 保持一致 |
| schema\_region\_consensus\_protocol\_class | 与 Seed-ConfigNode 保持一致 |
| data\_replication\_factor                  | 与 Seed-ConfigNode 保持一致 |
| data\_region\_consensus\_protocol\_class   | 与 Seed-ConfigNode 保持一致 |

接着请打开它的配置文件 ./conf/iotdb-confignode.properties，并检查以下参数：

| **配置项**                     | **检查**                                                     |
| ------------------------------ | ------------------------------------------------------------ |
| cn\_internal\_address          | 已设置为服务器的 IPV4 地址或域名                             |
| cn\_internal\_port             | 该端口未被占用                                               |
| cn\_consensus\_port            | 该端口未被占用                                               |
| cn\_target\_config\_node\_list | 已设置为另一个正在运行的 ConfigNode 的内部通讯地址，推荐使用 Seed-ConfigNode 的内部通讯地址 |

检查完毕后，即可在服务器上运行启动脚本：

```
# Linux 前台启动
bash ./sbin/start-confignode.sh

# Linux 后台启动
nohup bash ./sbin/start-confignode.sh >/dev/null 2>&1 &

# Windows
.\sbin\start-confignode.bat
```

ConfigNode 的其它配置参数可参考
[ConfigNode配置参数](https://iotdb.apache.org/zh/UserGuide/Master/Reference/ConfigNode-Config-Manual.html)。

##### 增加 DataNode

**确保集群已有正在运行的 ConfigNode 后，才能开始增加 DataNode。**

可以向集群中添加任意个 DataNode。
在添加新的 DataNode 前，请先打开通用配置文件 ./conf/iotdb-common.properties 并检查以下参数：

| **配置项**    | **检查**                    |
| ------------- | --------------------------- |
| cluster\_name | 与 Seed-ConfigNode 保持一致 |

接着打开它的配置文件 ./conf/iotdb-datanode.properties 并检查以下参数：

| **配置项**                          | **检查**                                                     |
| ----------------------------------- | ------------------------------------------------------------ |
| dn\_rpc\_address                    | 已设置为服务器的 IPV4 地址或域名                             |
| dn\_rpc\_port                       | 该端口未被占用                                               |
| dn\_internal\_address               | 已设置为服务器的 IPV4 地址或域名                             |
| dn\_internal\_port                  | 该端口未被占用                                               |
| dn\_mpp\_data\_exchange\_port       | 该端口未被占用                                               |
| dn\_data\_region\_consensus\_port   | 该端口未被占用                                               |
| dn\_schema\_region\_consensus\_port | 该端口未被占用                                               |
| dn\_target\_config\_node\_list      | 已设置为正在运行的 ConfigNode 的内部通讯地址，推荐使用 Seed-ConfigNode 的内部通讯地址 |

检查完毕后，即可在服务器上运行启动脚本：

```
# Linux 前台启动
bash ./sbin/start-datanode.sh

# Linux 后台启动
nohup bash ./sbin/start-datanode.sh >/dev/null 2>&1 &

# Windows
.\sbin\start-datanode.bat
```

DataNode 的其它配置参数可参考
[DataNode配置参数](https://iotdb.apache.org/zh/UserGuide/Master/Reference/DataNode-Config-Manual.html)。

**注意：当且仅当集群拥有不少于副本个数（max{schema\_replication\_factor, data\_replication\_factor}）的 DataNode 后，集群才可以提供服务**

#### 启动 Cli

若搭建的集群仅用于本地调试，可直接执行 ./sbin 目录下的 Cli 启动脚本：

```
# Linux
./sbin/start-cli.sh

# Windows
.\sbin\start-cli.bat
```

若希望通过 Cli 连接生产环境的集群，
请阅读 [Cli 使用手册](https://iotdb.apache.org/zh/UserGuide/Master/QuickStart/Command-Line-Interface.html)。

#### 验证集群

以在6台服务器上启动的3C3D（3个ConfigNode 和 3个DataNode）集群为例，
这里假设3个ConfigNode的IP地址依次为192.168.1.10、192.168.1.11、192.168.1.12，且3个ConfigNode启动时均使用了默认的端口10710与10720；
3个DataNode的IP地址依次为192.168.1.20、192.168.1.21、192.168.1.22，且3个DataNode启动时均使用了默认的端口6667、10730、10740、10750与10760。

当按照6.1步骤成功启动集群后，在 Cli 执行 `show cluster details`，看到的结果应当如下：

```
IoTDB> show cluster details
+------+----------+-------+---------------+------------+-------------------+------------+-------+-------+-------------------+-----------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|ConfigConsensusPort|  RpcAddress|RpcPort|MppPort|SchemaConsensusPort|DataConsensusPort|
+------+----------+-------+---------------+------------+-------------------+------------+-------+-------+-------------------+-----------------+
|     0|ConfigNode|Running|   192.168.1.10|       10710|              10720|            |       |       |                   |                 |
|     2|ConfigNode|Running|   192.168.1.11|       10710|              10720|            |       |       |                   |                 |
|     3|ConfigNode|Running|   192.168.1.12|       10710|              10720|            |       |       |                   |                 |
|     1|  DataNode|Running|   192.168.1.20|       10730|                   |192.168.1.20|   6667|  10740|              10750|            10760|
|     4|  DataNode|Running|   192.168.1.21|       10730|                   |192.168.1.21|   6667|  10740|              10750|            10760|
|     5|  DataNode|Running|   192.168.1.22|       10730|                   |192.168.1.22|   6667|  10740|              10750|            10760|
+------+----------+-------+---------------+------------+-------------------+------------+-------+-------+-------------------+-----------------+
Total line number = 6
It costs 0.012s
```

若所有节点的状态均为 **Running**，则说明集群部署成功；
否则，请阅读启动失败节点的运行日志，并检查对应的配置参数。

#### 停止 IoTDB 进程

本小节描述如何手动关闭 IoTDB 的 ConfigNode 或 DataNode 进程。

##### 使用脚本停止 ConfigNode

执行停止 ConfigNode 脚本：

```
# Linux
./sbin/stop-confignode.sh

# Windows
.\sbin\stop-confignode.bat
```

##### 使用脚本停止 DataNode

执行停止 DataNode 脚本：

```
# Linux
./sbin/stop-datanode.sh

# Windows
.\sbin\stop-datanode.bat
```

##### 停止节点进程

首先获取节点的进程号：

```
jps

# 或

ps aux | grep iotdb
```

结束进程：

```
kill -9 <pid>
```

**注意：有些端口的信息需要 root 权限才能获取，在此情况下请使用 sudo**

#### 集群缩容

本小节描述如何将 ConfigNode 或 DataNode 移出集群。

##### 移除 ConfigNode

在移除 ConfigNode 前，请确保移除后集群至少还有一个活跃的 ConfigNode。
在活跃的 ConfigNode 上执行 remove-confignode 脚本：

```
# Linux
## 根据 confignode_id 移除节点
./sbin/remove-confignode.sh <confignode_id>

## 根据 ConfigNode 内部通讯地址和端口移除节点
./sbin/remove-confignode.sh <cn_internal_address>:<cn_internal_port>


# Windows
## 根据 confignode_id 移除节点
.\sbin\remove-confignode.bat <confignode_id>

## 根据 ConfigNode 内部通讯地址和端口移除节点
.\sbin\remove-confignode.bat <cn_internal_address>:<cn_internal_port>
```

##### 移除 DataNode

在移除 DataNode 前，请确保移除后集群至少还有不少于（数据/元数据）副本个数的 DataNode。
在活跃的 DataNode 上执行 remove-datanode 脚本：

```
# Linux
## 根据 datanode_id 移除节点
./sbin/remove-datanode.sh <datanode_id>

## 根据 DataNode RPC 服务地址和端口移除节点
./sbin/remove-datanode.sh <dn_rpc_address>:<dn_rpc_port>


# Windows
## 根据 datanode_id 移除节点
.\sbin\remove-datanode.bat <datanode_id>

## 根据 DataNode RPC 服务地址和端口移除节点
.\sbin\remove-datanode.bat <dn_rpc_address>:<dn_rpc_port>
```

### 常见问题

请参考 [分布式部署FAQ](https://iotdb.apache.org/zh/UserGuide/Master/FAQ/FAQ-for-cluster-setup.html)