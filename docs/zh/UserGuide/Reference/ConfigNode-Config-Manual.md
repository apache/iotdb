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

# ConfigNode 配置参数

IoTDB ConfigNode 配置文件均位于 IoTDB 安装目录：`confignode/conf`文件夹下。

* `confignode-env.sh/bat`：环境配置项的配置文件，可以配置 ConfigNode 的内存大小。

* `iotdb-confignode.properties`：IoTDB ConfigNode 的配置文件。

## 环境配置项（confignode-env.sh/bat）

环境配置项主要用于对 ConfigNode 运行的 Java 环境相关参数进行配置，如 JVM 相关配置。ConfigNode 启动时，此部分配置会被传给 JVM，详细配置项说明如下：

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


## 系统配置项（iotdb-confignode.properties）

IoTDB 集群的全局配置通过 ConfigNode 配置。

### Internal RPC Service 配置

* internal\_address

|名字| internal\_address |
|:---:|:---|
|描述| ConfigNode 集群内部地址 |
|类型| String |
|默认值| 0.0.0.0|
|改后生效方式|重启服务生效|

* internal\_port

|名字| internal\_port |
|:---:|:---|
|描述| ConfigNode 集群服务监听端口|
|类型| Short Int : [0,65535] |
|默认值| 6667 |
|改后生效方式|重启服务生效|

* target\_config\_nodes

|名字| target\_config\_nodes |
|:---:|:---|
|描述| 目标 ConfigNode 地址，ConfigNode 通过此地址加入集群 |
|类型| String |
|默认值| 127.0.0.1:22277 |
|改后生效方式|重启服务生效|

* rpc\_thrift\_compression\_enable

|名字| rpc\_thrift\_compression\_enable |
|:---:|:---|
|描述| 是否启用 thrift 的压缩机制。|
|类型| Boolean |
|默认值| false |
|改后生效方式|重启服务生效|

* rpc\_advanced\_compression\_enable

|名字| rpc\_advanced\_compression\_enable |
|:---:|:---|
|描述| 是否启用 thrift 的自定制压缩机制。|
|类型| Boolean |
|默认值| false |
|改后生效方式|重启服务生效|

* rpc\_max\_concurrent\_client\_num

|名字| rpc\_max\_concurrent\_client\_num |
|:---:|:---|
|描述| 最大连接数。|
|类型| Short Int : [0,65535] |
|默认值| 65535 |
|改后生效方式|重启服务生效|

* thrift\_max\_frame\_size

|名字| thrift\_max\_frame\_size |
|:---:|:---|
|描述| RPC 请求/响应的最大字节数|
|类型| long |
|默认值| 536870912 （默认值512MB，应大于等于 512 * 1024 * 1024) |
|改后生效方式|重启服务生效|

* thrift\_init\_buffer\_size

|名字| thrift\_init\_buffer\_size |
|:---:|:---|
|描述| 字节数 |
|类型| Long |
|默认值| 1024 |
|改后生效方式|重启服务生效|


### 副本及共识协议

* consensus\_port

|名字| consensus\_port |
|:---:|:---|
|描述| ConfigNode 的共识协议通信端口 |
|类型| Short Int : [0,65535] |
|默认值| 22278 |
|改后生效方式|重启服务生效|


* data\_replication\_factor

|名字| data\_replication\_factor |
|:---:|:---|
|描述| 存储组的默认数据副本数|
|类型| Int |
|默认值| 1 |
|改后生效方式|重启服务生效|

* data\_region\_consensus\_protocol\_class

|名字| data\_region\_consensus\_protocol\_class |
|:---:|:---|
|描述| 数据副本的共识协议，1 副本时可以使用 StandAloneConsensus 协议，多副本时可以使用 MultiLeaderConsensus 或 RatisConsensus |
|类型| String |
|默认值| org.apache.iotdb.consensus.standalone.StandAloneConsensus |
|改后生效方式|仅允许在第一次启动服务前修改|

* schema\_replication\_factor

|名字| schema\_replication\_factor |
|:---:|:---|
|描述| 存储组的默认元数据副本数 |
|类型| Int |
|默认值| 1 |
|改后生效方式|重启服务生效|

* schema\_region\_consensus\_protocol\_class

|名字| schema\_region\_consensus\_protocol\_class |
|:---:|:---|
|描述| 元数据副本的共识协议，1 副本时可以使用 StandAloneConsensus 协议，多副本时只能使用 RatisConsensus |
|类型| String |
|默认值| org.apache.iotdb.consensus.standalone.StandAloneConsensus |
|改后生效方式|仅允许在第一次启动服务前修改|

* region\_allocate\_strategy

|名字| region\_allocate\_strategy |
|:---:|:---|
|描述| 元数据和数据的节点分配策略，COPY_SET适用于大集群；当数据节点数量较少时，GREEDY表现更佳|
|类型| String |
|默认值| GREEDY |
|改后生效方式|重启服务生效|

### 心跳配置

* heartbeat\_interval

|名字| heartbeat\_interval |
|:---:|:---|
|描述| 集群节点间的心跳间隔 |
|类型| Long |
|单位| ms |
|默认值| 1000 |
|改后生效方式|重启服务生效|


### 分区配置

* series\_partition\_slot\_num

|名字| series\_partition\_slot\_num |
|:---:|:---|
|描述| 序列分区槽数 |
|类型| Int |
|默认值| 10000 |
|改后生效方式|仅允许在第一次启动服务前修改|

* series\_partition\_executor\_class

|名字| series\_partition\_executor\_class |
|:---:|:---|
|描述| 序列分区槽数 |
|类型| String |
|默认值| org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor |
|改后生效方式|仅允许在第一次启动服务前修改|

### 存储组配置

* default\_ttl

|名字| default\_ttl |
|:---:|:---|
|描述| 默认数据保留时间 |
|类型| Long |
|默认值| 无限 |
|改后生效方式|重启服务生效|

* time\_partition\_interval

|名字| time\_partition\_interval |
|:---:|:---|
|描述| 存储组默认的数据时间分区间隔 |
|类型| Long |
|默认值| 604800 |
|改后生效方式|仅允许在第一次启动服务前修改|

### 数据目录

* system\_dir

|名字| system\_dir |
|:---:|:---|
|描述| ConfigNode 系统数据存储路径 |
|类型| String |
|默认值| data/system（Windows：data\\system） |
|改后生效方式|重启服务生效|

* consensus\_dir

|名字| consensus\_dir |
|:---:|:---|
|描述| ConfigNode 共识协议数据存储路径 |
|类型| String |
|默认值| data/consensus（Windows：data\\consensus） |
|改后生效方式|重启服务生效|

* udf\_lib\_dir

|名字| udf\_lib\_dir |
|:---:|:---|
|描述| UDF 日志及jar文件存储路径 |
|类型| String |
|默认值| ext/udf（Windows：ext\\udf） |
|改后生效方式|重启服务生效|

* temporary\_lib\_dir

|名字| temporary\_lib\_dir |
|:---:|:---|
|描述| UDF jar文件临时存储路径 |
|类型| String |
|默认值| ext/temporary（Windows：ext\\temporary） |
|改后生效方式|重启服务生效|