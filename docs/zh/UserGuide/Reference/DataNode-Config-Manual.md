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

## DataNode 配置参数

IoTDB DataNode 与 Standalone 模式共用一套配置文件，均位于 IoTDB 安装目录：`conf`文件夹下。

* `datanode-env.sh/bat`：环境配置项的配置文件，可以配置 DataNode 的内存大小。

* `iotdb-datanode.properties`：IoTDB DataNode 和单机版的配置文件。

### 热修改配置项

为方便用户使用，IoTDB 为用户提供了热修改功能，即在系统运行过程中修改 `iotdb-datanode.properties` 和 `iotdb-common.properties` 中部分配置参数并即时应用到系统中。下面介绍的参数中，改后 生效方式为`热加载`
的均为支持热修改的配置参数。

通过 Session 或 Cli 发送 ```load configuration``` 命令（SQL）至 IoTDB 可触发配置热加载。

### 环境配置项（datanode-env.sh/bat）

环境配置项主要用于对 DataNode 运行的 Java 环境相关参数进行配置，如 JVM 相关配置。DataNode/Standalone 启动时，此部分配置会被传给 JVM，详细配置项说明如下：

* MAX\_HEAP\_SIZE

|名字|MAX\_HEAP\_SIZE|
|:---:|:---|
|描述|IoTDB 能使用的最大堆内存大小 |
|类型|String|
|默认值|取决于操作系统和机器配置。在 Linux 或 MacOS 系统下默认为机器内存的四分之一。在 Windows 系统下，32 位系统的默认值是 512M，64 位系统默认值是 2G。|
|改后生效方式|重启服务生效|

* HEAP\_NEWSIZE

|名字|HEAP\_NEWSIZE|
|:---:|:---|
|描述|IoTDB 启动时分配的最小堆内存大小 |
|类型|String|
|默认值|取决于操作系统和机器配置。在 Linux 或 MacOS 系统下默认值为机器 CPU 核数乘以 100M 的值与 MAX\_HEAP\_SIZE 四分之一这二者的最小值。在 Windows 系统下，32 位系统的默认值是 512M，64 位系统默认值是 2G。|
|改后生效方式|重启服务生效|

* MAX\_DIRECT\_MEMORY\_SIZE

|名字|MAX\_DIRECT\_MEMORY\_SIZE|
|:---:|:---|
|描述|IoTDB 能使用的最大堆外内存大小 |
|类型|String|
|默认值|默认与最大堆内存相等|
|改后生效方式|重启服务生效|

* JMX\_LOCAL

|名字|JMX\_LOCAL|
|:---:|:---|
|描述|JMX 监控模式，配置为 true 表示仅允许本地监控，设置为 false 的时候表示允许远程监控。如想在本地通过网络连接JMX Service，比如nodeTool.sh会尝试连接127.0.0.1:31999，请将JMX_LOCAL设置为false。|
|类型|枚举 String : “true”, “false”|
|默认值|true|
|改后生效方式|重启服务生效|

* JMX\_PORT

|名字|JMX\_PORT|
|:---:|:---|
|描述|JMX 监听端口。请确认该端口是不是系统保留端口并且未被占用。|
|类型|Short Int: [0,65535]|
|默认值|31999|
|改后生效方式|重启服务生效|

### 系统配置项（iotdb-datanode.properties）

系统配置项是 IoTDB DataNode/Standalone 运行的核心配置，它主要用于设置 DataNode/Standalone 数据库引擎的参数。

#### Data Node RPC 服务配置

* dn\_rpc\_address

|名字| dn\_rpc\_address |
|:---:|:-----------------|
|描述| 客户端 RPC 服务监听地址   |
|类型| String           |
|默认值| 0.0.0.0          |
|改后生效方式| 重启服务生效           |

* dn\_rpc\_port

|名字| dn\_rpc\_port |
|:---:|:---|
|描述| Client RPC 服务监听端口|
|类型| Short Int : [0,65535] |
|默认值| 6667 |
|改后生效方式|重启服务生效|

* dn\_internal\_address

|名字| dn\_internal\_address |
|:---:|:---|
|描述| DataNode 内网通信地址 |
|类型| string |
|默认值| 127.0.0.1 |
|改后生效方式|仅允许在第一次启动服务前修改|

* dn\_internal\_port

|名字| dn\_internal\_port |
|:---:|:-------------------|
|描述| DataNode 内网通信端口    |
|类型| int                |
|默认值| 10730               |
|改后生效方式| 仅允许在第一次启动服务前修改             |

* dn\_mpp\_data\_exchange\_port

|名字| dn\_mpp\_data\_exchange\_port |
|:---:|:---|
|描述| MPP 数据交换端口 |
|类型| int |
|默认值| 10740 |
|改后生效方式|仅允许在第一次启动服务前修改|

* dn\_schema\_region\_consensus\_port

|名字| dn\_schema\_region\_consensus\_port |
|:---:|:---|
|描述| DataNode 元数据副本的共识协议通信端口 |
|类型| int |
|默认值| 10750 |
|改后生效方式|仅允许在第一次启动服务前修改|

* dn\_data\_region\_consensus\_port

|名字| dn\_data\_region\_consensus\_port |
|:---:|:---|
|描述| DataNode 数据副本的共识协议通信端口 |
|类型| int |
|默认值| 10760 |
|改后生效方式|仅允许在第一次启动服务前修改|

* dn\_join\_cluster\_retry\_interval\_ms

|名字| dn\_join\_cluster\_retry\_interval\_ms |
|:---:|:---------------------------------------|
|描述| DataNode 再次重试加入集群等待时间                  |
|类型| long                                   |
|默认值| 5000                                   |
|改后生效方式| 重启服务生效                                 |

#### 目标 Config Nodes 配置

* dn\_target\_config\_node\_list

|名字| dn\_target\_config\_node\_list      |
|:---:|:------------------------------------|
|描述| ConfigNode 地址，DataNode 启动时通过此地址加入集群 |
|类型| String                              |
|默认值| 127.0.0.1:10710                     |
|改后生效方式| 仅允许在第一次启动服务前修改                              |

#### 连接配置

* dn\_session\_timeout\_threshold

|名字| dn\_session_timeout_threshold |
|:---:|:------------------------------|
|描述| 最大的会话空闲时间                     |
|类型| int                           |
|默认值| 0                             |
|改后生效方式| 重启服务生效                        |


* dn\_rpc\_thrift\_compression\_enable

|名字| dn\_rpc\_thrift\_compression\_enable |
|:---:|:---------------------------------|
|描述| 是否启用 thrift 的压缩机制                |
|类型| Boolean                          |
|默认值| false                            |
|改后生效方式| 重启服务生效                           |

* dn\_rpc\_advanced\_compression\_enable

|名字| dn\_rpc\_advanced\_compression\_enable |
|:---:|:-----------------------------------|
|描述| 是否启用 thrift 的自定制压缩机制               |
|类型| Boolean                            |
|默认值| false                              |
|改后生效方式| 重启服务生效                             |

* dn\_rpc\_selector\_thread\_count

|   名字   | rpc\_selector\_thread\_count |
|:------:|:-----------------------------|
|   描述   | rpc 选择器线程数量                  |
|   类型   | int                          |
|  默认值   | 1                            |
| 改后生效方式 | 重启服务生效                       |

* dn\_rpc\_min\_concurrent\_client\_num

|   名字   | rpc\_min\_concurrent\_client\_num |
|:------:|:----------------------------------|
|   描述   | 最小连接数                             |
|   类型   | Short Int : [0,65535]             |
|  默认值   | 1                                 |
| 改后生效方式 | 重启服务生效                            |

* dn\_rpc\_max\_concurrent\_client\_num

|   名字   | dn\_rpc\_max\_concurrent\_client\_num |
|:------:|:----------------------------------|
|   描述   | 最大连接数                             |
|   类型   | Short Int : [0,65535]             |
|  默认值   | 65535                             |
| 改后生效方式 | 重启服务生效                            |

* dn\_thrift\_max\_frame\_size

|名字| dn\_thrift\_max\_frame\_size |
|:---:|:---|
|描述| RPC 请求/响应的最大字节数|
|类型| long |
|默认值| 536870912 （默认值512MB，应大于等于 512 * 1024 * 1024) |
|改后生效方式|重启服务生效|

* dn\_thrift\_init\_buffer\_size

|名字| dn\_thrift\_init\_buffer\_size |
|:---:|:---|
|描述| 字节数 |
|类型| long |
|默认值| 1024 |
|改后生效方式|重启服务生效|

* dn\_core\_client\_count\_for\_each\_node\_in\_client\_manager

|   名字   | dn\_core\_client\_count\_for\_each\_node\_in\_client\_manager |
|:------:|:--------------------------------------------------------------|
|   描述   | 单 ClientManager 中路由到每个节点的核心 Client 个数                         |
|   类型   | int                                                           |
|  默认值   | 200                                                           |
| 改后生效方式 | 重启服务生效                                                        |

* dn\_max\_client\_count\_for\_each\_node\_in\_client\_manager

|   名字   | dn\_max\_client\_count\_for\_each\_node\_in\_client\_manager |
|:------:|:-------------------------------------------------------------|
|   描述   | 单 ClientManager 中路由到每个节点的最大 Client 个数                        |
|   类型   | int                                                          |
|  默认值   | 300                                                          |
| 改后生效方式 | 重启服务生效                                                       |

#### 目录配置

* dn\_system\_dir

|   名字   | dn\_system\_dir                                                     |
|:------:|:--------------------------------------------------------------------|
|   描述   | IoTDB 元数据存储路径，默认存放在和 sbin 目录同级的 data 目录下。相对路径的起始目录与操作系统相关，建议使用绝对路径。 |
|   类型   | String                                                              |
|  默认值   | data/datanode/system（Windows：data\\datanode\\system）                |
| 改后生效方式 | 重启服务生效                                                                    |

* dn\_data\_dirs

|   名字   | dn\_data\_dirs                                                     |
|:------:|:-------------------------------------------------------------------|
|   描述   | IoTDB 数据存储路径，默认存放在和 sbin 目录同级的 data 目录下。相对路径的起始目录与操作系统相关，建议使用绝对路径。 |
|   类型   | String                                                             |
|  默认值   | data/datanode/data（Windows：data\\datanode\\data）                   |
| 改后生效方式 | 重启服务生效                                                                   |

* dn\_multi\_dir\_strategy

|   名字   | dn\_multi\_dir\_strategy                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|:------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|   描述   | IoTDB 在 data\_dirs 中为 TsFile 选择目录时采用的策略。可使用简单类名或类名全称。系统提供以下三种策略：<br>1. SequenceStrategy：IoTDB 按顺序选择目录，依次遍历 data\_dirs 中的所有目录，并不断轮循；<br>2. MaxDiskUsableSpaceFirstStrategy：IoTDB 优先选择 data\_dirs 中对应磁盘空余空间最大的目录；<br>您可以通过以下方法完成用户自定义策略：<br>1. 继承 org.apache.iotdb.db.conf.directories.strategy 类并实现自身的 Strategy 方法；<br>2. 将实现的类的完整类名（包名加类名，UserDefineStrategyPackage）填写到该配置项；<br>3. 将该类 jar 包添加到工程中。                                                                                                                  |
|   类型   | String                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|  默认值   | SequenceStrategy                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 改后生效方式 | 热加载                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

* dn\_consensus\_dir

|   名字   | dn\_consensus\_dir                                                       |
|:------:|:-------------------------------------------------------------------------|
|   描述   | IoTDB 共识层日志存储路径，默认存放在和 sbin 目录同级的 data 目录下。相对路径的起始目录与操作系统相关，建议使用绝对路径。    |
|   类型   | String                                                                   |
|  默认值   | data/datanode/consensus（Windows：data\\datanode\\consensus）               |
| 改后生效方式 | 重启服务生效                                                                         |

* dn\_wal\_dirs

|   名字   | dn\_wal\_dirs                                                        |
|:------:|:---------------------------------------------------------------------|
|   描述   | IoTDB 写前日志存储路径，默认存放在和 sbin 目录同级的 data 目录下。相对路径的起始目录与操作系统相关，建议使用绝对路径。 |
|   类型   | String                                                               |
|  默认值   | data/datanode/wal（Windows：data\\datanode\\wal）                       |
| 改后生效方式 | 重启服务生效                                                                     |

* dn\_tracing\_dir

|   名字   | dn\_tracing\_dir                                                    |
|:------:|:--------------------------------------------------------------------|
|   描述   | IoTDB 追踪根目录路径，默认存放在和 sbin 目录同级的 data 目录下。相对路径的起始目录与操作系统相关，建议使用绝对路径。 |
|   类型   | String                                                              |
|  默认值   | datanode/tracing                                                    |
| 改后生效方式 | 重启服务生效                                                                    |

* dn\_sync\_dir

|   名字   | dn\_sync\_dir                                                         |
|:------:|:----------------------------------------------------------------------|
|   描述   | IoTDB sync 存储路径，默认存放在和 sbin 目录同级的 data 目录下。相对路径的起始目录与操作系统相关，建议使用绝对路径。 |
|   类型   | String                                                                |
|  默认值   | data/datanode/sync                                                    |
| 改后生效方式 | 重启服务生效                                                                      |

#### Metric 配置

### 开启 GC 日志

GC 日志默认是关闭的。为了性能调优，用户可能会需要收集 GC 信息。
若要打开 GC 日志，则需要在启动 IoTDB Server 的时候加上"printgc"参数：

```bash
nohup sbin/start-datanode.sh printgc >/dev/null 2>&1 &
```

或者

```bash
sbin\start-datanode.bat printgc
```

GC 日志会被存储在`IOTDB_HOME/logs/gc.log`. 至多会存储 10 个 gc.log 文件，每个文件最多 10MB。
