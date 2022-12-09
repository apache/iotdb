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

# 1. 目标

本文档为 IoTDB 集群版（1.0.0）启动教程。

# 2. 前置检查

1. JDK>=1.8 的运行环境，并配置好 JAVA_HOME 环境变量。
2. 设置最大文件打开数为 65535。
3. 关闭交换内存。
4. 第一次启动节点时，确保该节点不存在 data 目录或 data 目录为空。
5. 如果整个集群处在可信环境下，可以关闭机器上的防火墙选项。

# 3. 安装包获取

## 3.1 下载二进制文件

1. 打开官网[Download Page](https://iotdb.apache.org/Download/)。
2. 下载 IoTDB 1.0.0 版本的二进制文件。
3. 解压得到 apache-iotdb-1.0.0-all-bin 目录。

## 3.2 使用源码编译

### 3.2.1 下载源码

**Git**
```
git clone https://github.com/apache/iotdb.git
git checkout v1.0.0
```

**官网下载**
1. 打开官网[Download Page](https://iotdb.apache.org/Download/)。
2. 下载 IoTDB 1.0.0 版本的源码。
3. 解压得到 apache-iotdb-1.0.0 目录。

### 3.2.2 编译源码

在 IoTDB 源码根目录下:
```
mvn clean package -pl distribution -am -DskipTests
```

编译成功后，可在目录 
**distribution/target/apache-iotdb-1.0.0-SNAPSHOT-all-bin/apache-iotdb-1.0.0-SNAPSHOT-all-bin** 
找到集群版本的二进制文件。

# 4. 安装包说明

打开 apache-iotdb-1.0.0-SNAPSHOT-all-bin，可见以下目录：

| **目录**                  | **说明**                                               |
|-------------------------|------------------------------------------------------|
| conf                    | 配置文件目录，包含 ConfigNode 和 DataNode 的配置文件                |
| data                    | 数据文件目录，包含 ConfigNode 和 DataNode 的数据文件                |
| lib                     | 库文件目录                                                |
| licenses                | 证书文件目录                                               |
| logs                    | 日志文件目录，包含 ConfigNode 和 DataNode 的日志文件                |
| sbin                    | 脚本目录，包含 ConfigNode 和 DataNode 的启停移除脚本目录，以及 Cli 的启动脚本 |
| tools                   | 系统工具目录                                               |

# 5. 集群安装配置

## 5.1 集群安装

将 apache-iotdb-1.0.0-SNAPSHOT-all-bin 部署至服务器/虚拟机的指定目录下，即完成一台服务器/虚拟机上的集群安装，
每个 apache-iotdb-1.0.0-SNAPSHOT-all-bin 可启动一个 ConfigNode 和一个 DataNode。

## 5.2 集群配置

切换工作路径至 apache-iotdb-1.0.0-SNAPSHOT-all-bin 中，
集群配置文件存放于 ./conf 目录内。

### 5.2.1 通用配置

打开通用配置文件 ./conf/iotdb-common.properties，
可根据 [部署推荐](https://iotdb.apache.org/zh/UserGuide/Master/Cluster/Deployment-Recommendation.html)
设置以下参数：

| **配置项**                                    | **说明**                                 |
|--------------------------------------------|----------------------------------------|
| config_node_consensus_protocol_class       | ConfigNode 使用的共识协议                     |
| schema\_replication\_factor                | 元数据副本数，DataNode 数量不应少于此数目              |
| schema\_region\_consensus\_protocol\_class | 元数据副本组的共识协议                            |
| data\_replication\_factor                  | 数据副本数，DataNode 数量不应少于此数目               |
| data\_region\_consensus\_protocol\_class   | 数据副本组的共识协议。注：RatisConsensus 目前不支持多数据目录 |

**注意：上述配置项在集群启动后即不可更改，且务必保证所有节点的通用配置完全一致，否则节点无法启动。**

### 5.2.2 ConfigNode 配置

打开 ConfigNode 配置文件 ./conf/iotdb-confignode.properties，根据服务器/虚拟机的 IP 地址和可用端口，设置以下参数：

| **配置项**                                   | **说明**                               |
|-------------------------------------------|--------------------------------------|
| cn\_internal\_address                     | ConfigNode 在集群内部通讯使用的地址              |
| cn\_internal\_port                        | ConfigNode 在集群内部通讯使用的端口              |
| cn\_consensus\_port                       | ConfigNode 副本组共识协议通信使用的端口            |
| cn\_target\_config\_node\_list            | 节点注册加入集群时连接的 ConfigNode 的地址。注：只能配置一个 |

**注意：上述配置项在节点启动后即不可更改，且务必保证所有端口均未被占用，否则节点无法启动。**

### 5.2.3 DataNode 配置

打开 DataNode 配置文件 ./conf/iotdb-datanode.properties，根据服务器/虚拟机的 IP 地址和可用端口，设置以下参数：

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

**注意：上述配置项在节点启动后即不可更改，且务必保证所有端口均未被占用，否则节点无法启动。**

# 6. 集群操作

## 6.1 启动集群

本小节描述如何启动包括若干 ConfigNode 和 DataNode 的集群。
集群可以提供服务的标准是至少启动一个 ConfigNode 且启动 不小于（数据/元数据）副本个数 的 DataNode。

总体启动流程分为三步：

1. 启动种子 ConfigNode
2. 增加 ConfigNode（可选）
3. 增加 DataNode

### 6.1.1 启动种子 ConfigNode

第一个启动的 ConfigNode 是种子 ConfigNode，标志着新集群的创建。
打开种子 ConfigNode 的配置文件 ./conf/iotdb-confignode.properties，并配置如下参数：

| **配置项**                        | **说明**                                                  |
|--------------------------------|---------------------------------------------------------|
| cn\_internal\_address          | ConfigNode 在集群内部通讯使用的地址                                 |
| cn\_internal\_port             | ConfigNode 在集群内部通讯使用的端口                                 |
| cn\_consensus\_port            | ConfigNode 副本组共识协议通信使用的端口                               |
| cn\_target\_config\_node\_list | 配置为自己的内部通讯地址，即 cn\_internal\_address:cn\_internal\_port |

配置完毕后运行启动脚本：
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

### 6.1.2 增加 ConfigNode（可选）

可向集群添加更多 ConfigNode，以保证 ConfigNode 的高可用。
新增的 ConfigNode 需要保证 ./conf/iotdb-common.properites 中的所有配置参数与种子 ConfigNode 完全一致，否则可能启动失败或产生运行时错误。
打开新 ConfigNode 的配置文件 ./conf/iotdb-confignode.properties，并配置以下参数：

| **配置项**                        | **说明**                                               |
|--------------------------------|------------------------------------------------------|
| cn\_internal\_address          | ConfigNode 在集群内部通讯使用的地址                              |
| cn\_internal\_port             | ConfigNode 在集群内部通讯使用的端口                              |
| cn\_consensus\_port            | ConfigNode 副本组共识协议通信使用的端口                            |
| cn\_target\_config\_node\_list | 配置为任意一个 ConfigNode 的内部通讯地址，推荐使用种子 ConfigNode 的内部通讯地址 |

配置完毕后运行启动脚本：
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

### 6.1.3 增加 DataNode

可以向集群中添加任意个 DataNode。
在添加新的 DataNode 前，打开配置文件 ./conf/iotdb-datanode.properties 并配置以下参数：

| **配置项**                             | **说明**                                               |
|-------------------------------------|------------------------------------------------------|
| dn\_rpc\_address                    | 客户端 RPC 服务的地址                                        |
| dn\_rpc\_port                       | 客户端 RPC 服务的端口                                        |
| dn\_internal\_address               | DataNode 在集群内部接收控制流使用的地址                             |
| dn\_internal\_port                  | DataNode 在集群内部接收控制流使用的端口                             |
| dn\_mpp\_data\_exchange\_port       | DataNode 在集群内部接收数据流使用的端口                             |
| dn\_data\_region\_consensus\_port   | DataNode 的数据副本间共识协议通信的端口                             |
| dn\_schema\_region\_consensus\_port | DataNode 的元数据副本间共识协议通信的端口                            |
| dn\_target\_config\_node\_list      | 配置为任意一个 ConfigNode 的内部通讯地址，推荐使用种子 ConfigNode 的内部通讯地址 |

配置完毕后运行启动脚本：
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

## 6.2 停止 IoTDB 进程

本小节描述如何手动关闭 IoTDB 的 ConfigNode 或 DataNode 进程。

### 6.2.1 使用脚本停止 ConfigNode

执行停止 ConfigNode 脚本：
```
# Linux
./sbin/stop-confignode.sh

# Windows
.\sbin\stop-confignode.bat
```

### 6.2.2 使用脚本停止 DataNode

执行停止 DataNode 脚本：
```
# Linux
./sbin/stop-datanode.sh

# Windows
.\sbin\stop-datanode.bat
```

### 6.2.3 停止节点进程

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

## 6.3 启动 Cli

执行 ./sbin 目录下的 Cli 启动脚本：
```
# Linux
./sbin/start-cli.sh

# Windows
.\sbin\start-cli.bat
```

## 6.4 集群缩容

本小节描述如何将 ConfigNode 或 DataNode 移出集群。

### 6.4.1 移除 ConfigNode

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

### 6.4.2 移除 DataNode

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

# 7. 常见问题

请参考 [常见问题](https://iotdb.apache.org/zh/UserGuide/Master/FAQ/Frequently-asked-questions.html)