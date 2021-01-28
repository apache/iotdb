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

# 工具介绍

IoTDB集群版为您提供了NodeTool Shell工具用于监控指定集群的工作状态，您可以通过运行多种指令获取集群各项状态。

下面具体介绍每个指令的使用方式及示例，其中$IOTDB_CLUSTER_HOME表示IoTDB分布式的安装目录所在路径。

# 前提条件
使用NodeTool需要开启JMX服务，具体请参考[JMX Tool](JMX Tool.md)。

# 使用说明

## 运行方式

NodeTool Shell工具启动脚本位于$IOTDB_CLUSTER_HOME/sbin文件夹下，启动时可以指定集群运行的IP和PORT。
其中IP为您期望连接的节点的IP，PORT为IoTDB集群启动时指定的JMX服务端口号，分别默认为`127.0.0.1`和`31999`。

如果您需要监控远程集群或修改了JMX服务端口号，请通过启动参数`-h`和`-p`项来使用实际的IP和PORT。

对于开启了JMX鉴权服务的，启动时候需要指定JMX服务的用户名和密码，默认分别为`iotdb`和`passw!d`，请通过启动参数`-u` 和`-pw`
项来指定JMX服务的用户名和密码。

## 指令说明
在分布式系统中，一个节点由节点IP，元数据端口，数据端口和服务端口来标识，即Node\<IP:METAPORT:DATAPORT:CLUSTERPORT>。

### 展示节点环

IoTDB集群版采用一致性哈希的方式实现数据分布，用户可以通过打印哈希环信息了解每个节点在环中的位置。

1.输入

> 打印哈希环的指令为ring

2.输出

> 输出为多行字符串，每行字符串为一个键值对，其中键表示节点标识，值表示节点(IP:METAPORT:DATAPORT:CLUSTERPORT)，格式为\<key -> value>。

3.示例

> 假设当前集群运行在127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561和127.0.0.1:9007:40014:55562三个节点上。
不同系统的输入指令示例如下：

Linux系统与MacOS系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 ring
```

Windows系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 ring
```
 
回车后即可执行指令。示例指令的输出如下：
```
Node Identifier                                 Node 
330411070           ->          127.0.0.1:9003:40010:55560 
330454032           ->          127.0.0.1:9005:40012:55561
330496472           ->          127.0.0.1:9007:40014:55562
```
 
上述输出表示当前集群共有3个节点，按照节点标识从小到大输出结果。

### 查询数据分区和元数据分区

IoTDB集群版的时间序列元数据按照存储组分给多个数据组，其中存储组和数据组为多对一的关系，
即同一个存储组的时间序列元数据只存在于同一个数据组，一个数据组可能包含多个存储组的时间序列元数据；

数据按照存储组和其时间戳分给不同数据组，时间分区粒度默认为一天。

数据组由多个节点组成，节点数量为副本数，保证数据高可用，其中某一个节点担任Leader的角色。

通过该指令，用户可以获知某个路径下的元数据或数据具体存储在哪些节点下。

1.输入

> 查询数据分区信息的指令为partition，参数具体说明如下：

|参数名|参数说明|示例|
| --- | --- | --- |
|-m | --metadata	查询元数据分区，默认为查询数据分区|	-m |
|-path | --path 	必要参数，需要查询的路径，若该路径无对应的存储组，则查询失败|	-path root.guangzhou.d1|
|-st | --StartTime	查询数据分区时使用，起始时间，默认为系统当前时间|	-st 1576724778159 |
|-et | --EndTime	查询数据分区时使用，终止时间，默认为系统当前时间。若终止时间小于起始时间，则终止时间默认为起始时间|	-et 1576724778159 |

2.输出

> 输出为多行字符串，每行字符串为一个键值对，其中键表示分区，值表示数据组，格式为\<key -> value>。

3.示例

> 假设当前集群运行在127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561和127.0.0.1:9007:40014:55562三个节点上。
> 副本数为2，共有3个存储组：{root.beijing、root.shanghai、root.guangzhou}。

+ 查询数据的分区(默认时间范围, 时间按天分区)

不同系统的输入指令示例如下：

Linux系统与MacOS系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1
```

Windows系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1
```
回车后即可执行指令。示例指令的输出如下：

```
DATA<root.guangzhou.d1, 1576723735188, 1576723735188>	->	[127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561]
```

+ 查询数据的分区(指定时间范围, 时间按天分区)

不同系统的输入指令示例如下：

Linux系统与MacOS系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -st 1576624778159 -et 1576724778159
```

Windows系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -st 1576624778159 -et 1576724778159
```

回车后即可执行指令。示例指令的输出如下：

```
DATA<root.guangzhou.d1, 1576627200000, 1576713599999>	->	[127.0.0.1:9007:40014:55562, 127.0.0.1:9003:40010:55560] 
DATA<root.guangzhou.d1, 1576713600000, 1576724778159>	->	[127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561] 
DATA<root.guangzhou.d1, 1576624778159, 1576627199999>	->	[127.0.0.1:9005:40012:55561, 127.0.0.1:9007:40014:55562]
```

+ 查询元数据分区

不同系统的输入指令示例如下：

Linux系统与MacOS系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -m
```

Windows系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -m
```

回车后即可执行指令。示例指令的输出如下：

```
DATA<root.guangzhou.d1, 1576723735188, 1576723735188>	->	[127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561]
```
上述输出表示root.t1.d1所属的数据分区包含2个节点，其中127.0.0.1:9003:40010:55560为header节点。


### 查询节点管理的槽数

IoTDB集群版将哈希环划分为固定数量(默认10000)个槽，并由集群管理组（元数据组）的leader将槽划分各个数据组。通过该指令，用户可以获知数据组管理的槽数。

1. 输入

> 查询节点对应数据分区信息的指令为host，参数具体说明如下：

|参数名|参数说明|示例|
|---|---|---|
|-a --all |查询所有数据组管理的槽数，默认为查询的节点所在的数据组|-a|

2.输出

> 输出为多行字符串，其中每行字符串为一个键值对，其中键表示数据组，值表示管理的槽数，格式为\<key -> value>。

3.示例

> 假设当前集群运行在127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561和127.0.0.1:9007:40014:55562三个节点上, 副本数为2。

+ 默认节点所在分区

不同系统的输入指令示例如下：

Linux系统与MacOS系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 host
```

Windows系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 host
```

回车后即可执行指令。示例指令的输出如下：

```
Raft group                                                 Slot Number
(127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561)      ->                3333
(127.0.0.1:9007:40014:55562, 127.0.0.1:9003:40010:55560)      ->                3334
```

+ 所有分区

不同系统的输入指令示例如下：

Linux系统与MacOS系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 host -a
```

Windows系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 host -a
```

回车后即可执行指令。示例指令的输出如下：

```
Raft group                                                 Slot Number
(127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561)      ->                3333
(127.0.0.1:9005:40012:55561, 127.0.0.1:9007:40014:55562)      ->                3333
(127.0.0.1:9007:40014:55562, 127.0.0.1:9003:40010:55560)      ->                3334 
```

### 查询节点状态

IoTDB集群版包含多个节点，对于任意节点都存在因为网络、硬件等问题导致无法正常提供服务的可能。

通过该指令，用户即可获知集群中所有节点当前的状态。

1.输入

> 查询节点状态的指令为status，无其他参数。

2.输出

> 输出为多行字符串，其中每行字符串为一个键值对，其中键表示节点(IP:METAPORT:DATAPORT)，
> 值表示该节点的状态，“on”为正常，“off”为不正常，格式为\<key -> value>。

3.示例
> 假设当前集群运行在127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561和127.0.0.1:9007:40014:55562三个节点上, 副本数为2。

不同系统的输入指令示例如下：

Linux系统与MacOS系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 status
```

Windows系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 status
```

回车后即可执行指令。示例指令的输出如下：

```
Node                                Status 
127.0.0.1:9003:40010:55560          ->        on 
127.0.0.1:9005:40012:55561          ->        off 
127.0.0.1:9007:40014:55562          ->        on
```
上述输出表示127.0.0.1:9003:40010:55560节点和127.0.0.1:9007:40014:55562节点状态正常，127.0.0.1:9005:40012:55561节点无法提供服务。