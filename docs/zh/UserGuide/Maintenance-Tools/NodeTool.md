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
## Node 工具
### 工具介绍

IoTDB 集群版为您提供了 NodeTool Shell 工具用于监控指定集群的工作状态，您可以通过运行多种指令获取集群各项状态。

下面具体介绍每个指令的使用方式及示例，其中 $IOTDB_CLUSTER_HOME 表示 IoTDB 分布式的安装目录所在路径。

### 前提条件
使用 NodeTool 需要开启 JMX 服务，具体请参考 [JMX Tool](JMX-Tool.md)。

### 使用说明

#### 运行方式

NodeTool Shell 工具启动脚本位于 $IOTDB_CLUSTER_HOME/sbin 文件夹下，启动时可以指定集群运行的 IP 和 PORT。
其中 IP 为您期望连接的节点的 IP，PORT 为 IoTDB 集群启动时指定的 JMX 服务端口号，分别默认为`127.0.0.1`和`31999`。

如果您需要监控远程集群或修改了 JMX 服务端口号，请通过启动参数`-h`和`-p`项来使用实际的 IP 和 PORT。

对于开启了 JMX 鉴权服务的，启动时候需要指定 JMX 服务的用户名和密码，默认分别为`iotdb`和`passw!d`，请通过启动参数`-u` 和`-pw`
项来指定 JMX 服务的用户名和密码。

#### 指令说明
在分布式系统中，一个节点由节点 IP，元数据端口，数据端口和服务端口来标识，即 Node\<IP:METAPORT:DATAPORT:CLUSTERPORT>。

##### 展示节点环

IoTDB 集群版采用一致性哈希的方式实现数据分布，用户可以通过打印哈希环信息了解每个节点在环中的位置。

1. 输入

> 打印哈希环的指令为 ring

2. 输出

> 输出为多行字符串，每行字符串为一个键值对，其中键表示节点标识，值表示节点 (IP:METAPORT:DATAPORT:CLUSTERPORT)，格式为、<key -> value>。

3. 示例

> 假设当前集群运行在 127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668 和 127.0.0.1:9007:40014:6669 三个节点上。
不同系统的输入指令示例如下：

Linux 系统与 MacOS 系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 ring
```

Windows 系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 ring
```
 
回车后即可执行指令。示例指令的输出如下：
```
Node Identifier                                 Node 
330411070           ->          127.0.0.1:9003:40010:6667 
330454032           ->          127.0.0.1:9005:40012:6668
330496472           ->          127.0.0.1:9007:40014:6669
```
 
上述输出表示当前集群共有 3 个节点，按照节点标识从小到大输出结果。

##### 查询数据分区和元数据分区

IoTDB 集群版的时间序列元数据按照存储组分给多个数据组，其中存储组和数据组为多对一的关系，
即同一个存储组的时间序列元数据只存在于同一个数据组，一个数据组可能包含多个存储组的时间序列元数据；

数据按照存储组和其时间戳分给不同数据组，时间分区粒度默认为一天。

数据组由多个节点组成，节点数量为副本数，保证数据高可用，其中某一个节点担任 Leader 的角色。

通过该指令，用户可以获知某个路径下的元数据或数据具体存储在哪些节点下。

1. 输入

> 查询数据分区信息的指令为 partition，参数具体说明如下：

|参数名|参数说明|示例|
| --- | --- | --- |
|-m | --metadata	查询元数据分区，默认为查询数据分区|	-m |
|-path | --path 	必要参数，需要查询的路径，若该路径无对应的存储组，则查询失败|	-path root.guangzhou.d1|
|-st | --StartTime	查询数据分区时使用，起始时间，默认为系统当前时间|	-st 1576724778159 |
|-et | --EndTime	查询数据分区时使用，终止时间，默认为系统当前时间。若终止时间小于起始时间，则终止时间默认为起始时间|	-et 1576724778159 |

2. 输出

> 输出为多行字符串，每行字符串为一个键值对，其中键表示分区，值表示数据组，格式为、<key -> value>。

3. 示例

> 假设当前集群运行在 127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668 和 127.0.0.1:9007:40014:6669 三个节点上。
> 副本数为 2，共有 3 个存储组：{root.beijing、root.shanghai、root.guangzhou}。

+ 查询数据的分区（默认时间范围，时间按天分区）

不同系统的输入指令示例如下：

Linux 系统与 MacOS 系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1
```

Windows 系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1
```
回车后即可执行指令。示例指令的输出如下：

```
DATA<root.guangzhou.d1, 1576723735188, 1576723735188>	->	[127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668]
```

+ 查询数据的分区（指定时间范围，时间按天分区）

不同系统的输入指令示例如下：

Linux 系统与 MacOS 系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -st 1576624778159 -et 1576724778159
```

Windows 系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -st 1576624778159 -et 1576724778159
```

回车后即可执行指令。示例指令的输出如下：

```
DATA<root.guangzhou.d1, 1576627200000, 1576713599999>	->	[127.0.0.1:9007:40014:6669, 127.0.0.1:9003:40010:6667] 
DATA<root.guangzhou.d1, 1576713600000, 1576724778159>	->	[127.0.0.1:9003:40010:6668, 127.0.0.1:9005:40012:6668] 
DATA<root.guangzhou.d1, 1576624778159, 1576627199999>	->	[127.0.0.1:9005:40012:6667, 127.0.0.1:9007:40014:6669]
```

+ 查询元数据分区

不同系统的输入指令示例如下：

Linux 系统与 MacOS 系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -m
```

Windows 系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -m
```

回车后即可执行指令。示例指令的输出如下：

```
DATA<root.guangzhou.d1, 1576723735188, 1576723735188>	->	[127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668]
```
上述输出表示 root.t1.d1 所属的数据分区包含 2 个节点，其中 127.0.0.1:9003:40010:6667 为 header 节点。

##### 查询节点管理的槽数

IoTDB 集群版将哈希环划分为固定数量（默认 10000) 个槽，并由集群管理组（元数据组）的 leader 将槽划分各个数据组。通过该指令，用户可以获知数据组管理的槽数。

1. 输入

> 查询节点对应数据分区信息的指令为 host，参数具体说明如下：

|参数名|参数说明|示例|
|---|---|---|
|-a --all |查询所有数据组管理的槽数，默认为查询的节点所在的数据组|-a|

2. 输出

> 输出为多行字符串，其中每行字符串为一个键值对，其中键表示数据组，值表示管理的槽数，格式为、<key -> value>。

3. 示例

> 假设当前集群运行在 127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668 和 127.0.0.1:9007:40014:6669 三个节点上，副本数为 2。

+ 默认节点所在分区

不同系统的输入指令示例如下：

Linux 系统与 MacOS 系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 host
```

Windows 系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 host
```

回车后即可执行指令。示例指令的输出如下：

```
Raft group                                                 Slot Number
(127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668)      ->                3333
(127.0.0.1:9007:40014:6669, 127.0.0.1:9003:40010:6667)      ->                3334
```

+ 所有分区

不同系统的输入指令示例如下：

Linux 系统与 MacOS 系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 host -a
```

Windows 系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 host -a
```

回车后即可执行指令。示例指令的输出如下：

```
Raft group                                                 Slot Number
(127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668)      ->                3333
(127.0.0.1:9005:40012:6668, 127.0.0.1:9007:40014:6669)      ->                3333
(127.0.0.1:9007:40014:6669, 127.0.0.1:9003:40010:6667)      ->                3334 
```

##### 查询节点状态

IoTDB 集群版包含多个节点，对于任意节点都存在因为网络、硬件等问题导致无法正常提供服务的可能。

通过该指令，用户即可获知集群中所有节点当前的状态。

1. 输入

> 查询节点状态的指令为 status，无其他参数。

2. 输出

> 输出为多行字符串，其中每行字符串为一个键值对，其中键表示节点 (IP:METAPORT:DATAPORT)，
> 值表示该节点的状态，“on”为正常，“off”为不正常，格式为、<key -> value>。

3. 示例
> 假设当前集群运行在 127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668 和 127.0.0.1:9007:40014:6669 三个节点上，副本数为 2。

不同系统的输入指令示例如下：

Linux 系统与 MacOS 系统：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 status
```

Windows 系统：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 status
```

回车后即可执行指令。示例指令的输出如下：

```
Node                                Status 
127.0.0.1:9003:40010:6667          ->        on 
127.0.0.1:9005:40012:6668          ->        off 
127.0.0.1:9007:40014:6669          ->        on
```
上述输出表示 127.0.0.1:9003:40010:6667 节点和 127.0.0.1:9007:40014:6669 节点状态正常，127.0.0.1:9005:40012:6668 节点无法提供服务。
