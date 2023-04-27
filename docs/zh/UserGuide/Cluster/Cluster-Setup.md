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
# 集群搭建

## 集群设置

你可以根据此文档启动IoTDB集群。

### 安装环境

为使用IoTDB，你首先需要:

1. 安装前需要保证设备上配有 JDK>=1.8 的运行环境，并配置好 JAVA_HOME 环境变量。

2. 设置最大文件打开数为 65535。

### 安装步骤

IoTDB 支持多种安装途径。用户可以使用三种方式对 IoTDB 进行安装——下载二进制可运行程序、使用源码、使用 docker 镜像。

- 使用源码：您可以从代码仓库下载源码并编译，具体编译方法见下方。
- 二进制可运行程序：请从 [下载](https://iotdb.apache.org/Download/) 页面下载最新的安装包，解压后即完成安装。
- 使用 Docker 镜像：dockerfile 文件位于 [github](https://github.com/apache/iotdb/blob/master/docker/src/main)

#### 源码编译

下载源码:

```
git clone https://github.com/apache/iotdb.git
```

默认分支为master分支，你可以切换到其他发布版本，例如：

```
git checkout rel/0.12
```

在iotdb根目录下:

```
> mvn clean package -pl cluster -am -DskipTests
```

集群的二进制版本在目录 **cluster/target/{iotdb-project.version}** 下

#### 下载

你可以直接下载二进制版本 [Download Page](https://iotdb.apache.org/Download/)



### 文件目录

完成IoTDB Cluster安装后，默认会在IoTDB Cluster的根目录下生成下列目录文件：

| **目录** | **说明**                                     |
| -------- | -------------------------------------------- |
| conf     | 配置文件目录                                 |
| data     | 默认数据文件目录，可通过修改配置文件修改位置 |
| ext      | 默认udf目录，可通过修改配置文件修改位置      |
| lib      | 库文件目录                                   |
| logs     | 运行日志目录                                 |
| sbin     | 可执行文件目录                               |
| tools    | 系统工具目录                                 |



### 配置

为方便 IoTDB Server 的配置与管理，IoTDB Server 为用户提供三种配置项，使得您可以在启动服务或服务运行时对其进行配置。

三种配置项的配置文件均位于 IoTDB 安装目录：`$IOTDB_HOME/conf`文件夹下，其中涉及 server 配置的共有 4 个文件，分别为：`iotdb-cluster.properties`、`iotdb-engine.properties`、`logback.xml` 和 `iotdb-env.sh`(Unix 系统）/`iotdb-env.bat`(Windows 系统）, 您可以通过更改其中的配置项对系统运行的相关配置项进行配置。

配置文件的说明如下：

- `iotdb-env.sh`/`iotdb-env.bat`：环境配置项的默认配置文件。您可以在文件中配置 JAVA-JVM 的相关系统配置项。
- `iotdb-engine.properties`：IoTDB 引擎层系统配置项的默认配置文件。您可以在文件中配置 IoTDB 引擎运行时的相关参数。此外，用户可以在文件中配置 IoTDB 存储时 TsFile 文件的相关信息，如每次将内存中的数据写入到磁盘前的缓存大小 (`group_size_in_byte`)，内存中每个列打一次包的大小 (`page_size_in_byte`) 等。
- `logback.xml`: 日志配置文件，比如日志级别等。
- `iotdb-cluster.properties`: IoTDB 集群所需要的一些配置。

`iotdb-engine.properties`、`iotdb-env.sh`/`iotdb-env.bat` 两个配置文件详细说明请参考 [附录/配置手册](https://github.com/apache/iotdb/blob/master/docs/zh/UserGuide/Appendix/Config-Manual.md)，与集群有关的配置项是在`iotdb-cluster.properties`文件中的，你可以直接查看 [配置文件](https://github.com/apache/iotdb/blob/master/cluster/src/assembly/resources/conf/iotdb-cluster.properties) 中的注释，也可以参考 [Cluster Configuration](#集群配置项)。

配置文件位于 **{cluster\_root\_dir}/conf**。

**你需要修改每个节点以下的配置项去启动你的IoTDB集群：**

* iotdb-engine.properties:

  * rpc\_address

  - rpc\_port

  - base\_dir

  - data\_dirs

  - wal\_dir

* iotdb-cluster.properties
  * internal\_ip
  * internal\_meta\_port
  * internal\_data\_port
  * cluster\_info\_public\_port
  * seed\_nodes

  * default\_replica\_num

## 被覆盖的单机版选项

iotdb-engines.properties 配置文件中的部分内容会不再生效：

- `enable_auto_create_schema` 不再生效，并被视为`false`. 应使用 iotdb-cluster.properties 中的
   `enable_auto_create_schema` 来控制是否自动创建序列。
- `is_sync_enable` 不再生效，并被视为 `false`.



### 启动服务

#### 启动集群

您可以多节点部署或单机部署分布式集群，两者的主要区别是后者需要处理端口和文件目录的冲突，配置项含义请参考 [配置项](https://github.com/apache/iotdb/blob/master/docs/zh/UserGuide/Cluster/Cluster-Setup.md#配置项)。 启动其中一个节点的服务，需要执行如下命令：

```
# Unix/OS X
> nohup sbin/start-node.sh [printgc] [<conf_path>] >/dev/null 2>&1 &

# Windows
> sbin\start-node.bat [printgc] [<conf_path>] 
```

`printgc`表示在启动的时候，会开启 GC 日志。 `<conf_path>`使用`conf_path`文件夹里面的配置文件覆盖默认配置文件。GC 日志默认是关闭的。为了性能调优，用户可能会需要收集 GC 信息。GC 日志会被存储在`IOTDB_HOME/logs/`下面。

**如果你启动了所有seed节点，并且所有seed节点可以相互通信并没有ip地址/端口和文件目录的冲突，集群就成功启动了。**

#### 集群扩展

在集群运行过程中，用户可以向集群中加入新的节点或者删除集群中已有节点。目前仅支持逐节点集群扩展操作，多节点的集群扩展可以转化为一系列单节点集群扩展操作来实现。集群只有在上一次集群扩展操作完成后才会接收新的集群扩展操作。

**在要加入集群的新节点上启动以下脚本进行增加节点操作：**

```
# Unix/OS X
> nohup sbin/add-node.sh [printgc] [<conf_path>] >/dev/null 2>&1 &

# Windows
> sbin\add-node.bat [printgc] [<conf_path>] 
```

`printgc`表示在启动的时候，会开启 GC 日志。 `<conf_path>`使用`conf_path`文件夹里面的配置文件覆盖默认配置文件。

**在任一集群中的节点上启动以下脚本进行删除节点操作：**

```
# Unix/OS X
> sbin/remove-node.sh <internal_ip> <internal_meta_port>

# Windows
> sbin\remove-node.bat <internal_ip> <internal_meta_port>
```

`internal_ip`表示待删除节点的 IP 地址 `internal_meta_port`表示待删除节点的 meta 服务端口

#### 使用 Cli 工具

安装环境请参考 [快速上手/安装环境章节](https://github.com/apache/iotdb/blob/master/docs/zh/UserGuide/QuickStart/QuickStart.md)。你可以根据节点的rpc\_address和rpc\_port向任何节点发起连接。

#### 停止集群

在任一机器上启动以下脚本进行停止所有运行在该机器上的节点服务操作：

```
# Unix/OS X
> sbin/stop-node.sh

# Windows
> sbin\stop-node.bat
```



### 附录

#### 集群配置项

- internal_ip

| 名字         | internal_ip                                                  |
| ------------ | ------------------------------------------------------------ |
| 描述         | IOTDB 集群各个节点之间内部通信的 IP 地址，比如心跳、snapshot 快照、raft log 等。**`internal_ip`是集群内部的私有ip** |
| 类型         | String                                                       |
| 默认值       | 127.0.0.1                                                    |
| 改后生效方式 | 重启服务生效，集群建立后不可再修改                           |

- internal_meta_port

| 名字         | internal_meta_port                                           |
| ------------ | ------------------------------------------------------------ |
| 描述         | IoTDB meta 服务端口，用于元数据组（又称集群管理组）通信，元数据组管理集群配置和存储组信息**IoTDB 将为每个 meta 服务自动创建心跳端口。默认 meta 服务心跳端口为`internal_meta_port+1`，请确认这两个端口不是系统保留端口并且未被占用** |
| 类型         | Int32                                                        |
| 默认值       | 9003                                                         |
| 改后生效方式 | 重启服务生效，集群建立后不可再修改                           |

- internal_data_port

| 名字         | internal_data_port                                           |
| ------------ | ------------------------------------------------------------ |
| 描述         | IoTDB data 服务端口，用于数据组通信，数据组管理数据模式和数据的存储**IoTDB 将为每个 data 服务自动创建心跳端口。默认的 data 服务心跳端口为`internal_data_port+1`。请确认这两个端口不是系统保留端口并且未被占用** |
| 类型         | Int32                                                        |
| 默认值       | 40010                                                        |
| 改后生效方式 | 重启服务生效，集群建立后不可再修改                           |

- cluster_info_public_port

| 名字         | cluster_info_public_port                        |
| ------------ | ----------------------------------------------- |
| 描述         | 用于查看集群信息（如数据分区）的 RPC 服务的接口 |
| 类型         | Int32                                           |
| 默认值       | 6567                                            |
| 改后生效方式 | 重启服务生效                                    |

- open_server_rpc_port

| 名字         | open_server_rpc_port                                         |
| ------------ | ------------------------------------------------------------ |
| 描述         | 是否打开单机模块的 rpc port，用于调试模式，如果设置为 true，则单机模块的 rpc port 设置为`rpc_port (in iotdb-engines.properties) + 1` |
| 类型         | Boolean                                                      |
| 默认值       | false                                                        |
| 改后生效方式 | 重启服务生效，集群建立后不可再修改                           |

- seed_nodes

| 名字         | seed_nodes                                                   |
| ------------ | ------------------------------------------------------------ |
| 描述         | 集群中节点的地址（私有ip），`{IP/DOMAIN}:internal_meta_port`格式，用逗号分割；对于伪分布式模式，可以都填写`localhost`，或是`127.0.0.1` 或是混合填写，但是不能够出现真实的 ip 地址；对于分布式模式，支持填写 real ip 或是 hostname，但是不能够出现`localhost`或是`127.0.0.1`。当使用`start-node.sh(.bat)`启动节点时，此配置意味着形成初始群集的节点，每个节点的`seed_nodes`应该一致，否则群集将初始化失败；当使用`add-node.sh(.bat)`添加节点到集群中时，此配置项可以是集群中已经存在的任何节点，不需要是用`start-node.sh(bat)`构建初始集群的节点。 |
| 类型         | String                                                       |
| 默认值       | 127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007                 |
| 改后生效方式 | 重启服务生效                                                 |

- rpc_thrift_compression_enable

| 名字         | rpc_thrift_compression_enable                                |
| ------------ | ------------------------------------------------------------ |
| 描述         | 是否开启 thrift 压缩通信，**注意这个参数要各个节点保持一致，也要与客户端保持一致，同时也要与`iotdb-engine.properties`中`rpc_thrift_compression_enable`参数保持一致** |
| 类型         | Boolean                                                      |
| 默认值       | false                                                        |
| 改后生效方式 | 重启服务生效，需要整个集群同时更改                           |

- default_replica_num

| 名字         | default_replica_num              |
| ------------ | -------------------------------- |
| 描述         | 集群副本数                       |
| 类型         | Int32                            |
| 默认值       | 3                                |
| 改后生效方式 | 重启服务生效，集群建立后不可更改 |

- multi_raft_factor

| 名字         | multi_raft_factor                                            |
| ------------ | ------------------------------------------------------------ |
| 描述         | 每个数据组启动的 raft 组实例数量，默认每个数据组启动一个 raft 组 |
| 类型         | Int32                                                        |
| 默认值       | 1                                                            |
| 改后生效方式 | 重启服务生效，集群建立后不可更改                             |

- cluster_name

| 名字         | cluster_name                                                 |
| ------------ | ------------------------------------------------------------ |
| 描述         | 集群名称，集群名称用以标识不同的集群，**一个集群中所有节点的 cluster_name 都应相同** |
| 类型         | String                                                       |
| 默认值       | default                                                      |
| 改后生效方式 | 重启服务生效                                                 |

- connection_timeout_ms

| 名字         | connection_timeout_ms                              |
| ------------ | -------------------------------------------------- |
| 描述         | raft 节点间的 thrift 连接超时和 socket 超时时间，单位毫秒. **对于发送心跳和投票请求的 thrift 连接的超时时间会被自动调整为 connection_timeout_ms 和 heartbeat_interval_ms 的最小值.** |
| 类型         | Int32                                              |
| 默认值       | 20000                                              |
| 改后生效方式 | 重启服务生效

- heartbeat\_interval\_ms

| 名字         | heartbeat\_interval\_ms                                 |
| ------------ | ------------------------------------------------------- |
| 描述         | 领导者发送心跳广播的间隔时间，单位毫秒                  |
| 类型         | Int64                                                   |
| 默认值       | 1000                                                    |
| 改后生成方式 | 重启服务生效                                            |

- election\_timeout\_ms

| 名字         | election\_timeout\_ms                                        |
| ------------ | ------------------------------------------------------------ |
| 描述         | 跟随者的选举超时时间, 以及选举者等待投票的超时时间, 单位毫秒 |
| 类型         | Int64                                                        |
| 默认值       | 20000                                                        |
| 改后生成方式 | 重启服务生效                                                 |

- read_operation_timeout_ms

| 名字         | read_operation_timeout_ms                                    |
| ------------ | ------------------------------------------------------------ |
| 描述         | 读取操作超时时间，仅用于内部通信，不适用于整个操作，单位毫秒 |
| 类型         | Int32                                                        |
| 默认值       | 30000                                                        |
| 改后生效方式 | 重启服务生效                                                 |

- write_operation_timeout_ms

| 名字         | write_operation_timeout_ms                                   |
| ------------ | ------------------------------------------------------------ |
| 描述         | 写入操作超时时间，仅用于内部通信，不适用于整个操作，单位毫秒 |
| 类型         | Int32                                                        |
| 默认值       | 30000                                                        |
| 改后生效方式 | 重启服务生效                                                 |

- min_num_of_logs_in_mem

| 名字         | min_num_of_logs_in_mem                                       |
| ------------ | ------------------------------------------------------------ |
| 描述         | 删除日志操作执行后，内存中保留的最多的提交的日志的数量。增大这个值将减少在 CatchUp 使用快照的机会，但也会增加内存占用量 |
| 类型         | Int32                                                        |
| 默认值       | 100                                                          |
| 改后生效方式 | 重启服务生效                                                 |

- max_num_of_logs_in_mem

| 名字         | max_num_of_logs_in_mem                                       |
| ------------ | ------------------------------------------------------------ |
| 描述         | 当内存中已提交的日志条数达到这个值之后，就会触发删除日志的操作，增大这个值将减少在 CatchUp 使用快照的机会，但也会增加内存占用量 |
| 类型         | Int32                                                        |
| 默认值       | 1000                                                         |
| 改后生效方式 | 重启服务生效                                                 |

- log_deletion_check_interval_second

| 名字         | log_deletion_check_interval_second                           |
| ------------ | ------------------------------------------------------------ |
| 描述         | 检查删除日志任务的时间间隔，每次删除日志任务将会把已提交日志超过 min_num_of_logs_in_mem 条的最老部分删除，单位秒 |
| 类型         | Int32                                                        |
| 默认值       | 60                                                           |
| 改后生效方式 | 重启服务生效                                                 |

- enable_auto_create_schema

| 名字         | enable_auto_create_schema                                    |
| ------------ | ------------------------------------------------------------ |
| 描述         | 是否支持自动创建 schema，**这个值会覆盖`iotdb-engine.properties`中`enable_auto_create_schema`的配置** |
| 类型         | BOOLEAN                                                      |
| 默认值       | true                                                         |
| 改后生效方式 | 重启服务生效                                                 |

- consistency_level

| 名字         | consistency_level                                            |
| ------------ | ------------------------------------------------------------ |
| 描述         | 读一致性，目前支持 3 种一致性：strong、mid、weak。strong consistency  每次操作都会尝试与 Leader 同步以获取最新的数据，如果失败（超时），则直接向用户返回错误； mid consistency  每次操作将首先尝试与 Leader 进行同步，但是如果失败（超时），它将使用本地当前数据向用户提供服务； weak consistency  不会与 Leader 进行同步，而只是使用本地数据向用户提供服务 |
| 类型         | strong、mid、weak                                            |
| 默认值       | mid                                                          |
| 改后生效方式 | 重启服务生效                                                 |

- is_enable_raft_log_persistence

| 名字         | is_enable_raft_log_persistence |
| ------------ | ------------------------------ |
| 描述         | 是否开启 raft log 持久化       |
| 类型         | BOOLEAN                        |
| 默认值       | true                           |
| 改后生效方式 | 重启服务生效                   |
