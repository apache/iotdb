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

## 1. 目标

本文档为 IoTDB 集群版（1.0.0）的安装及启动教程。

## 2. 前置检查

1. JDK>=1.8 的运行环境，并配置好 JAVA_HOME 环境变量。
2. 设置最大文件打开数为 65535。
3. 关闭交换内存。
4. 首次启动ConfigNode节点时，确保已清空ConfigNode节点的data/confignode目录；首次启动DataNode节点时，确保已清空DataNode节点的data/datanode目录。
5. 如果整个集群处在可信环境下，可以关闭机器上的防火墙选项。
6. 在集群默认配置中，ConfigNode 会占用端口 10710 和 10720，DataNode 会占用端口 6667、10730、10740、10750 和 10760，
请确保这些端口未被占用，或者手动修改配置文件中的端口配置。

## 3. 安装包获取

你可以选择下载二进制文件（见 3.1）或从源代码编译（见 3.2）。

### 3.1 下载二进制文件

1. 打开官网[Download Page](https://iotdb.apache.org/Download/)。
2. 下载 IoTDB 1.0.0 版本的二进制文件。
3. 解压得到 apache-iotdb-1.0.0-all-bin 目录。

### 3.2 使用源码编译

#### 3.2.1 下载源码

**Git**
```
git clone https://github.com/apache/iotdb.git
git checkout v1.0.0
```

**官网下载**
1. 打开官网[Download Page](https://iotdb.apache.org/Download/)。
2. 下载 IoTDB 1.0.0 版本的源码。
3. 解压得到 apache-iotdb-1.0.0 目录。

#### 3.2.2 编译源码

在 IoTDB 源码根目录下:
```
mvn clean package -pl distribution -am -DskipTests
```

编译成功后，可在目录 
**distribution/target/apache-iotdb-1.0.0-SNAPSHOT-all-bin/apache-iotdb-1.0.0-SNAPSHOT-all-bin** 
找到集群版本的二进制文件。

## 4. 安装包说明

打开 apache-iotdb-1.0.0-SNAPSHOT-all-bin，可见以下目录：

| **目录**   | **说明**                                              |
|----------|-----------------------------------------------------|
| conf     | 配置文件目录，包含 ConfigNode、DataNode、JMX 和 logback 等配置文件   |
| data     | 数据文件目录，包含 ConfigNode 和 DataNode 的数据文件               |
| lib      | 库文件目录                                               |
| licenses | 证书文件目录                                              |
| logs     | 日志文件目录，包含 ConfigNode 和 DataNode 的日志文件               |
| sbin     | 脚本目录，包含 ConfigNode 和 DataNode 的启停移除脚本，以及 Cli 的启动脚本等 |
| tools    | 系统工具目录                                              |

## 5. 集群安装配置

### 5.1 集群安装

`apache-iotdb-1.0.0-SNAPSHOT-all-bin` 包含 ConfigNode 和 DataNode，
请将安装包部署于你目标集群的所有机器上，推荐将安装包部署于所有服务器的相同目录下。

如果你希望先在一台服务器上尝试部署 IoTDB 集群，请参考
[Cluster Quick Start](https://iotdb.apache.org/zh/UserGuide/Master/QuickStart/ClusterQuickStart.html)。

### 5.2 集群配置

接下来需要修改每个服务器上的配置文件，登录服务器，
并将工作路径切换至 `apache-iotdb-1.0.0-SNAPSHOT-all-bin`，
配置文件在 `./conf` 目录内。

对于所有部署 ConfigNode 的服务器，需要修改通用配置（见 5.2.1）和 ConfigNode 配置（见 5.2.2）。

对于所有部署 DataNode 的服务器，需要修改通用配置（见 5.2.1）和 DataNode 配置（见 5.2.3）。

#### 5.2.1 通用配置

打开通用配置文件 ./conf/iotdb-common.properties，
可根据 [部署推荐](https://iotdb.apache.org/zh/UserGuide/Master/Cluster/Deployment-Recommendation.html)
设置以下参数：

| **配置项**                                    | **说明**                                 | **默认**                                          |
|--------------------------------------------|----------------------------------------|-------------------------------------------------|
| cluster\_name                              | 节点希望加入的集群的名称                           | defaultCluster                                  |
| config\_node\_consensus\_protocol\_class   | ConfigNode 使用的共识协议                     | org.apache.iotdb.consensus.ratis.RatisConsensus |
| schema\_replication\_factor                | 元数据副本数，DataNode 数量不应少于此数目              | 1                                               |
| schema\_region\_consensus\_protocol\_class | 元数据副本组的共识协议                            | org.apache.iotdb.consensus.ratis.RatisConsensus |
| data\_replication\_factor                  | 数据副本数，DataNode 数量不应少于此数目               | 1                                               |
| data\_region\_consensus\_protocol\_class   | 数据副本组的共识协议。注：RatisConsensus 目前不支持多数据目录 | org.apache.iotdb.consensus.iot.IoTConsensus     |

**注意：上述配置项在集群启动后即不可更改，且务必保证所有节点的通用配置完全一致，否则节点无法启动。**

#### 5.2.2 ConfigNode 配置

打开 ConfigNode 配置文件 ./conf/iotdb-confignode.properties，根据服务器/虚拟机的 IP 地址和可用端口，设置以下参数：

| **配置项**                        | **说明**                               | **默认**          | **用法**                                                                                                                                               |
|--------------------------------|--------------------------------------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| cn\_internal\_address          | ConfigNode 在集群内部通讯使用的地址              | 127.0.0.1       | 设置为服务器的 IPV4 地址或域名                                                                                                                                   |
| cn\_internal\_port             | ConfigNode 在集群内部通讯使用的端口              | 10710           | 设置为任意未占用端口                                                                                                                                           |
| cn\_consensus\_port            | ConfigNode 副本组共识协议通信使用的端口            | 10720           | 设置为任意未占用端口                                                                                                                                           |
| cn\_target\_config\_node\_list | 节点注册加入集群时连接的 ConfigNode 的地址。注：只能配置一个 | 127.0.0.1:10710 | 对于 Seed-ConfigNode，设置为自己的 cn\_internal\_address:cn\_internal\_port；对于其它 ConfigNode，设置为另一个正在运行的 ConfigNode 的 cn\_internal\_address:cn\_internal\_port |

**注意：上述配置项在节点启动后即不可更改，且务必保证所有端口均未被占用，否则节点无法启动。**

#### 5.2.3 DataNode 配置

打开 DataNode 配置文件 ./conf/iotdb-datanode.properties，根据服务器/虚拟机的 IP 地址和可用端口，设置以下参数：

| **配置项**                             | **说明**                    | **默认**          | **用法**                                                                            |
|-------------------------------------|---------------------------|-----------------|-----------------------------------------------------------------------------------|
| dn\_rpc\_address                    | 客户端 RPC 服务的地址             | 127.0.0.1       | 设置为服务器的 IPV4 地址或域名                                                                |
| dn\_rpc\_port                       | 客户端 RPC 服务的端口             | 6667            | 设置为任意未占用端口                                                                        |
| dn\_internal\_address               | DataNode 在集群内部接收控制流使用的地址  | 127.0.0.1       | 设置为服务器的 IPV4 地址或域名                                                                |
| dn\_internal\_port                  | DataNode 在集群内部接收控制流使用的端口  | 10730           | 设置为任意未占用端口                                                                        |
| dn\_mpp\_data\_exchange\_port       | DataNode 在集群内部接收数据流使用的端口  | 10740           | 设置为任意未占用端口                                                                        |
| dn\_data\_region\_consensus\_port   | DataNode 的数据副本间共识协议通信的端口  | 10750           | 设置为任意未占用端口                                                                        |
| dn\_schema\_region\_consensus\_port | DataNode 的元数据副本间共识协议通信的端口 | 10760           | 设置为任意未占用端口                                                                        |
| dn\_target\_config\_node\_list      | 集群中正在运行的 ConfigNode 地址    | 127.0.0.1:10710 | 设置为任意正在运行的 ConfigNode 的 cn\_internal\_address:cn\_internal\_port，可设置多个，用逗号（","）隔开 |

**注意：上述配置项在节点启动后即不可更改，且务必保证所有端口均未被占用，否则节点无法启动。**

## 6. 集群操作

### 6.1 启动集群

本小节描述如何启动包括若干 ConfigNode 和 DataNode 的集群。
集群可以提供服务的标准是至少启动一个 ConfigNode 且启动 不小于（数据/元数据）副本个数 的 DataNode。

总体启动流程分为三步：

1. 启动种子 ConfigNode
2. 增加 ConfigNode（可选）
3. 增加 DataNode

#### 6.1.1 启动 Seed-ConfigNode

**集群第一个启动的节点必须是 ConfigNode，第一个启动的 ConfigNode 必须遵循本小节教程。**

第一个启动的 ConfigNode 是 Seed-ConfigNode，标志着新集群的创建。
在启动 Seed-ConfigNode 前，请打开通用配置文件 ./conf/iotdb-common.properties，并检查如下参数：

| **配置项**                                    | **检查**        |
|--------------------------------------------|---------------|
| cluster\_name                              | 已设置为期望的集群名称   |
| config\_node\_consensus\_protocol\_class   | 已设置为期望的共识协议   |
| schema\_replication\_factor                | 已设置为期望的元数据副本数 |
| schema\_region\_consensus\_protocol\_class | 已设置为期望的共识协议   |
| data\_replication\_factor                  | 已设置为期望的数据副本数  |
| data\_region\_consensus\_protocol\_class   | 已设置为期望的共识协议   |

**注意：** 请根据[部署推荐](https://iotdb.apache.org/zh/UserGuide/Master/Cluster/Deployment-Recommendation.html)配置合适的通用参数，这些参数在首次配置后即不可修改。

接着请打开它的配置文件 ./conf/iotdb-confignode.properties，并检查如下参数：

| **配置项**                        | **检查**                                                   |
|--------------------------------|----------------------------------------------------------|
| cn\_internal\_address          | 已设置为服务器的 IPV4 地址或域名                                      |
| cn\_internal\_port             | 该端口未被占用                                                  |
| cn\_consensus\_port            | 该端口未被占用                                                  |
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

#### 6.1.2 增加更多 ConfigNode（可选）

**只要不是第一个启动的 ConfigNode 就必须遵循本小节教程。**

可向集群添加更多 ConfigNode，以保证 ConfigNode 的高可用。常用的配置为额外增加两个 ConfigNode，使集群共有三个 ConfigNode。

新增的 ConfigNode 需要保证 ./conf/iotdb-common.properites 中的所有配置参数与 Seed-ConfigNode 完全一致，否则可能启动失败或产生运行时错误。
因此，请着重检查通用配置文件中的以下参数：

| **配置项**                                    | **检查**                 |
|--------------------------------------------|------------------------|
| cluster\_name                              | 与 Seed-ConfigNode 保持一致 |
| config\_node\_consensus\_protocol\_class   | 与 Seed-ConfigNode 保持一致 |
| schema\_replication\_factor                | 与 Seed-ConfigNode 保持一致 |
| schema\_region\_consensus\_protocol\_class | 与 Seed-ConfigNode 保持一致 |
| data\_replication\_factor                  | 与 Seed-ConfigNode 保持一致 |
| data\_region\_consensus\_protocol\_class   | 与 Seed-ConfigNode 保持一致 |

接着请打开它的配置文件 ./conf/iotdb-confignode.properties，并检查以下参数：

| **配置项**                        | **检查**                                                       |
|--------------------------------|--------------------------------------------------------------|
| cn\_internal\_address          | 已设置为服务器的 IPV4 地址或域名                                          |
| cn\_internal\_port             | 该端口未被占用                                                      |
| cn\_consensus\_port            | 该端口未被占用                                                      |
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

#### 6.1.3 增加 DataNode

**确保集群已有正在运行的 ConfigNode 后，才能开始增加 DataNode。**

可以向集群中添加任意个 DataNode。
在添加新的 DataNode 前，请先打开通用配置文件 ./conf/iotdb-common.properties 并检查以下参数：

| **配置项**                                    | **检查**                 |
|--------------------------------------------|------------------------|
| cluster\_name                              | 与 Seed-ConfigNode 保持一致 |

接着打开它的配置文件 ./conf/iotdb-datanode.properties 并检查以下参数：

| **配置项**                             | **检查**                                                    |
|-------------------------------------|-----------------------------------------------------------|
| dn\_rpc\_address                    | 已设置为服务器的 IPV4 地址或域名                                       |
| dn\_rpc\_port                       | 该端口未被占用                                                   |
| dn\_internal\_address               | 已设置为服务器的 IPV4 地址或域名                                       |
| dn\_internal\_port                  | 该端口未被占用                                                   |
| dn\_mpp\_data\_exchange\_port       | 该端口未被占用                                                   |
| dn\_data\_region\_consensus\_port   | 该端口未被占用                                                   |
| dn\_schema\_region\_consensus\_port | 该端口未被占用                                                   |
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

### 6.2 启动 Cli

若搭建的集群仅用于本地调试，可直接执行 ./sbin 目录下的 Cli 启动脚本：

```
# Linux
./sbin/start-cli.sh

# Windows
.\sbin\start-cli.bat
```

若希望通过 Cli 连接生产环境的集群，
请阅读 [Cli 使用手册](https://iotdb.apache.org/zh/UserGuide/Master/QuickStart/Command-Line-Interface.html)。

### 6.3 验证集群

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

### 6.4 停止 IoTDB 进程

本小节描述如何手动关闭 IoTDB 的 ConfigNode 或 DataNode 进程。

#### 6.4.1 使用脚本停止 ConfigNode

执行停止 ConfigNode 脚本：

```
# Linux
./sbin/stop-confignode.sh

# Windows
.\sbin\stop-confignode.bat
```

#### 6.4.2 使用脚本停止 DataNode

执行停止 DataNode 脚本：

```
# Linux
./sbin/stop-datanode.sh

# Windows
.\sbin\stop-datanode.bat
```

#### 6.4.3 停止节点进程

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

### 6.5 集群缩容

本小节描述如何将 ConfigNode 或 DataNode 移出集群。

#### 6.5.1 移除 ConfigNode

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

#### 6.5.2 移除 DataNode

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

## 7. 常见问题

请参考 [分布式部署FAQ](https://iotdb.apache.org/zh/UserGuide/Master/FAQ/FAQ-for-cluster-setup.html)