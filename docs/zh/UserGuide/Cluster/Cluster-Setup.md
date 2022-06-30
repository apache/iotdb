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

本文档为 IoTDB 集群版（0.14.0-preview1）启动教程。

## 安装环境

1. JDK>=1.8 的运行环境，并配置好 JAVA_HOME 环境变量。

2. 设置最大文件打开数为 65535。

3. 关闭交换内存。

## 安装包获取

### 下载

可以直接在官网下载二进制版本 [Download Page](https://iotdb.apache.org/Download/)

### 源码编译

下载源码:

```
git clone https://github.com/apache/iotdb.git
```

默认分支为 master 分支，你可以切换到发布版本的 tag，例如：

```
git checkout v0.14.0-preview1
```

在 IoTDB 根目录下:

```
mvn clean package -pl distribution -am -DskipTests
```

集群的二进制版本在目录 **distribution/target** 下，其中，all-bin 包含 ConfigNode 和 DataNode，DataNode 内包含 Cli。

## 安装包说明

| **目录** | **说明**                                      |
| -------- | -------------------------------------------- |
| confignode |  包含 ConfigNode 的启停脚本、配置文件、日志、数据 |
| datanode   | 包含 DataNode 的启停脚本、配置文件、日志、数据；Cli的启动脚本 |
| grafana-metrics-example  | Grafana 监控界面模板           |
| lib      | 库文件目录                                     |
| tools    | 系统工具目录                                   |

## 启动集群

用户可以启动包括若干 ConfigNode 和 DataNode 的集群。
集群可以提供服务的标准是至少启动一个 ConfigNode 且启动 不小于（数据/元数据）副本个数 的 DataNode。

总体启动流程分为三步

* 启动种子 ConfigNode
* 增加 ConfigNode（可选）
* 增加 DataNode

### 启动种子 ConfigNode

对 confignode/conf/iotdb-confignode.properties 中的重要参数进行配置：

| **配置项** | **说明**                                      |
| -------- | -------------------------------------------- |
| internal\_address    | ConfigNode 在集群内部通讯使用的地址          |
| internal\_port    | ConfigNode 在集群内部通讯使用的端口           |
| consensus\_port    | ConfigNode 副本组共识协议通信使用的端口         |
| target\_config\_nodes    | 种子 ConfigNode 地址，第一个 ConfigNode 配置自己的 address:port        |
| data\_replication\_factor  | 数据副本数，DataNode 数量不应少于此数目        |
| data\_region\_consensus\_protocol\_class |  数据副本组的共识协议 |
| schema\_replication\_factor  | 元数据副本数，DataNode 数量不应少于此数目       |
| schema\_region\_consensus\_protocol\_class   | 元数据副本组的共识协议 |

Linux 启动方式
```
# 前台启动
./confignode/sbin/start-confignode.sh

# 后台启动
nohup ./confignode/sbin/start-confignode.sh >/dev/null 2>&1 &
```

Windows 启动方式
```
confignode\sbin\start-confignode.bat
```

具体参考 [ConfigNode配置参数](https://iotdb.apache.org/zh/UserGuide/Master/Reference/ConfigNode-Config-Manual.html)

### 增加 ConfigNode（可选）

增加 ConfigNode 是一个扩容操作，除端口不能冲突外，其他参数需要与集群已有的 ConfigNode 保持一致，并将 config\_nodes 配置为集群已有节点。

启动方式同上。

### 增加 DataNode

可以像集群中添加任意个 DataNode。

iotdb-datanode.properties 中的重要配置如下

| **配置项** | **说明**                                      |
| -------- | -------------------------------------------- |
| rpc\_address    | 客户端 RPC 服务的地址         |
| rpc\_port    | 客户端 RPC 服务的端口         |
| internal\_address    | DataNode 在集群内部接收控制流使用的端口         |
| internal\_port    | DataNode 在集群内部接收控制流使用的端口           |
| mpp\_data\_exchange\_port    | DataNode 在集群内部接收数据流使用的端口           |
| data\_region\_consensus\_port    | DataNode 的数据副本间共识协议通信的端口           |
| schema\_region\_consensus\_port    | DataNode 的元数据副本间共识协议通信的端口           |
| target\_config\_nodes    | 集群中正在运行的 ConfigNode 地址       |


Linux 启动方式
```
# 前台启动
./datanode/sbin/start-datanode.sh

# 后台启动
nohup ./datanode/sbin/start-datanode.sh >/dev/null 2>&1 &
```

Windows 启动方式
```
datanode\sbin\start-datanode.bat
```

具体参考 [DataNode配置参数](https://iotdb.apache.org/zh/UserGuide/Master/Reference/DataNode-Config-Manual.html)

### 启动 Cli

Cli 启动脚本在 datanode/sbin 目录

Linux 启动方式
```
./datanode/sbin/start-cli.sh
```

Windows 启动方式
```
datanode\sbin\start-cli.bat
```

## 快速上手

解压 apache-iotdb-0.14.0-preview1-all-bin.zip

部署 1 个 ConfigNode 和 1 个 DataNode（1C1D）集群版，默认 1 副本。
```
./confignode/sbin/start-confignode.sh
./datanode/sbin/start-datanode.sh
```

启动 Cli
```
./datanode/sbin/start-cli.sh
```