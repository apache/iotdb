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

# TsFile 同步

## 1.介绍

同步工具是持续将边缘端（发送端） IoTDB 中的时间序列数据上传并加载至云端（接收端） IoTDB 的套件工具。

IoTDB 同步工具内嵌于 IoTDB 引擎，与下游接收端相连，下游支持 IoTDB（单机/集群）。

可以在发送端使用 SQL 命令来启动或者关闭一个同步任务，并且可以随时查看同步任务的状态。在接收端，您可以通过设置 IP 白名单来规定准入 IP 地址范围。

## 2.模型定义

![pipe2.png](https://alioss.timecho.com/docs/img/UserGuide/System-Tools/Sync-Tool/pipe2.png?raw=true)

TsFile 同步工具实现了数据从 "流入-> IoTDB ->流出" 的闭环。假设目前有两台机器A和B都安装了IoTDB，希望将 A 上的数据不断同步至 B 中。为了更好地描述这个过程，我们引入以下概念。

- Pipe
  - 指一次同步任务，在上述案例中，我们可以看作在 A 和 B 之间有一根数据流管道连接了 A 和 B。
  - 一个正常运行的 Pipe 有两种状态：RUNNING 表示正在向接收端同步数据，STOP 表示暂停向接收端同步数据。
- PipeSink
  - 指接收端，在上述案例中，PipeSink 即是 B 这台机器。PipeSink 的类型目前仅支持 IoTDB，即接收端为 B 上安装的 IoTDB 实例。

## 3.注意事项

- 同步工具的发送端目前仅支持 IoTDB 1.0 版本**单数据副本配置**，接收端支持 IoTDB 1.0 版本任意配置。
- 当有一个或多个发送端指向一个接收端时，这些发送端和接收端各自的设备路径集合之间应当没有交集，否则可能产生不可预料错误 
  - 例如：当发送端A包括路径`root.sg.d.s`，发送端B也包括路径`root.sg.d.s`，当发送端A删除`root.sg` database 时将也会在接收端删除所有B在接收端的`root.sg.d.s`中存放的数据。
- 两个“端”之间目前不支持相互同步。
- 同步工具仅同步数据写入，若接收端未创建 database，自动创建与发送端同级 database。当前版本删除操作不保证被同步，不支持 TTL 的设置、Trigger、CQ 等其他操作的同步。
  - 若在发送端设置了 TTL，则启动 Pipe 时候 IoTDB 中所有未过期的数据以及未来所有的数据写入和删除都会被同步至接收端
- 对同步任务进行操作时，需保证 `SHOW DATANODES` 中所有处于 Running 状态的 DataNode 节点均可连通，否则将执行失败。

## 4.快速上手

在发送端和接收端执行如下语句即可快速开始两个 IoTDB 之间的数据同步，完整的 SQL 语句和配置事项请查看`配置参数`和`SQL`两节，更多使用范例请参考`使用范例`节。

- 启动发送端 IoTDB 与接收端 IoTDB
- 创建接收端为 IoTDB 类型的 Pipe Sink

```
IoTDB> CREATE PIPESINK my_iotdb AS IoTDB (ip='接收端IP', port='接收端端口')
```

- 创建同步任务Pipe（请确保接收端 IoTDB 已经启动）

```
IoTDB> CREATE PIPE my_pipe TO my_iotdb
```

- 开始同步任务

```
IoTDB> START PIPE my_pipe
```

- 显示所有同步任务状态

```
IoTDB> SHOW PIPES
```

- 暂停任务

```
IoTDB> STOP PIPE my_pipe
```

- 继续被暂停的任务

```
IoTDB> START PIPE my_pipe
```

- 关闭任务（状态信息将被删除）

```
IoTDB> DROP PIPE my_pipe
```

## 5.配置参数

所有参数修改均在`$IOTDB_HOME$/conf/iotdb-common.properties`中，所有修改完成之后执行`load configuration`之后即可立刻生效。

### 5.1发送端相关

| **参数名** | **max_number_of_sync_file_retry**          |
| ---------- | ------------------------------------------ |
| 描述       | 发送端同步文件到接收端失败时的最大重试次数 |
| 类型       | Int : [0,2147483647]                       |
| 默认值     | 5                                          |

### 5.2接收端相关

| **参数名** | **ip_white_list**                                            |
| ---------- | ------------------------------------------------------------ |
| 描述       | 设置同步功能发送端 IP 地址的白名单，以网段的形式表示，多个网段之间用逗号分隔。发送端向接收端同步数据时，只有当该发送端 IP 地址处于该白名单设置的网段范围内，接收端才允许同步操作。如果白名单为空，则接收端不允许任何发送端同步数据。默认接收端拒绝除了本地以外的全部 IP 的同步请求。 对该参数进行配置时，需要保证发送端所有 DataNode 地址均被覆盖。 |
| 类型       | String                                                       |
| 默认值     | 127.0.0.1/32                                                    |

## 6.SQL

### SHOW PIPESINKTYPE

- 显示当前所能支持的 PipeSink 类型。

```Plain%20Text
IoTDB> SHOW PIPESINKTYPE
IoTDB>
+-----+
| type|
+-----+
|IoTDB|
+-----+
```

### CREATE PIPESINK

- 创建接收端为 IoTDB 类型的 PipeSink，其中IP和port是可选参数。当接收端为集群时，填写任意一个 DataNode 的 `rpc_address` 与 `rpc_port`。

```
IoTDB> CREATE PIPESINK <PipeSinkName> AS IoTDB [(ip='127.0.0.1',port=6667);]
```

### DROP PIPESINK

- 删除 PipeSink。当 PipeSink 正在被同步任务使用时，无法删除 PipeSink。

```
IoTDB> DROP PIPESINK <PipeSinkName>
```

### SHOW PIPESINK

- 显示当前所有 PipeSink 定义，结果集有三列，分别表示 PipeSink 的名字，PipeSink 的类型，PipeSink 的属性。

```
IoTDB> SHOW PIPESINKS
IoTDB> SHOW PIPESINK [PipeSinkName]
IoTDB> 
+-----------+-----+------------------------+
|       name| type|              attributes|
+-----------+-----+------------------------+
|my_pipesink|IoTDB|ip='127.0.0.1',port=6667|
+-----------+-----+------------------------+
```

### CREATE PIPE

- 创建同步任务
  - 其中 select 语句目前仅支持`**`（即所有序列中的数据），from 语句目前仅支持`root`，where语句仅支持指定 time 的起始时间。起始时间的指定形式可以是 yyyy-mm-dd HH:MM:SS或时间戳。

```Plain%20Text
IoTDB> CREATE PIPE my_pipe TO my_iotdb [FROM (select ** from root WHERE time>=yyyy-mm-dd HH:MM:SS)]
```

### STOP PIPE

- 暂停任务

```
IoTDB> STOP PIPE <PipeName>
```

### START PIPE

- 开始任务

```
IoTDB> START PIPE <PipeName>
```

### DROP PIPE

- 关闭任务（状态信息可被删除）

```
IoTDB> DROP PIPE <PipeName>
```

### SHOW PIPE

> 该指令在发送端和接收端均可执行

- 显示所有同步任务状态

  - `create time`：Pipe 的创建时间

  - `name`：Pipe 的名字

  - `role`：当前 IoTDB 在 Pipe 中的角色，可能有两种角色：
    - sender，当前 IoTDB 为同步发送端
    - receiver，当前 IoTDB 为同步接收端

  - `remote`：Pipe的对端信息
    - 当 role 为 sender 时，这一字段值为 PipeSink 名称
    - 当 role 为 receiver 时，这一字段值为发送端 IP

  - `status`：Pipe状态

  - `attributes`：Pipe属性
    - 当 role 为 sender 时，这一字段值为 Pipe 的同步起始时间和是否同步删除操作
    - 当 role 为 receiver 时，这一字段值为当前 DataNode 上创建的同步连接对应的存储组名称

  - `message`：Pipe运行信息，当 Pipe 正常运行时，这一字段通常为NORMAL，当出现异常时，可能出现两种状态：
    - WARN 状态，这表明发生了数据丢失或者其他错误，但是 Pipe 会保持运行
    - ERROR 状态，这表明出现了网络连接正常但数据无法传输的问题，例如发送端 IP 不在接收端白名单中，或是发送端与接收端版本不兼容
    - 当出现 ERROR 状态时，建议 STOP PIPE 后查看 DataNode 日志，检查接收端配置或网络情况后重新 START PIPE

```Plain%20Text
IoTDB> SHOW PIPES
IoTDB>
+-----------------------+--------+--------+-------------+---------+------------------------------------+-------+
|            create time|   name |    role|       remote|   status|                          attributes|message|
+-----------------------+--------+--------+-------------+---------+------------------------------------+-------+
|2022-03-30T20:58:30.689|my_pipe1|  sender|  my_pipesink|     STOP|SyncDelOp=false,DataStartTimestamp=0| NORMAL|
+-----------------------+--------+--------+-------------+---------+------------------------------------+-------+ 
|2022-03-31T12:55:28.129|my_pipe2|receiver|192.168.11.11|  RUNNING|             Database='root.vehicle'| NORMAL|
+-----------------------+--------+--------+-------------+---------+------------------------------------+-------+
```

- 显示指定同步任务状态，当未指定PipeName时，与`SHOW PIPES`等效

```
IoTDB> SHOW PIPE [PipeName]
```

## 7.使用示例

### 目标

- 创建一个从边端 IoTDB 到 云端 IoTDB 的 同步工作
- 边端希望同步从2022年3月30日0时之后的数据
- 边端不希望同步所有的删除操作
- 云端 IoTDB 仅接受来自边端的 IoTDB 的数据

### 接收端操作

`vi conf/iotdb-common.properties` 配置云端参数，将白名单设置为仅接收来自 IP 为 192.168.0.1 的边端的数据

```
####################
### PIPE Server Configuration
####################
# White IP list of Sync client.
# Please use the form of network segment to present the range of IP, for example: 192.168.0.0/16
# If there are more than one IP segment, please separate them by commas
# The default is to allow all IP to sync
# Datatype: String
ip_white_list=192.168.0.1/32
```

### 发送端操作

创建云端 PipeSink，指定类型为 IoTDB，指定云端 IP 地址为 192.168.0.1，指定云端的 PipeServer 服务端口为6667

```
IoTDB> CREATE PIPESINK my_iotdb AS IoTDB (ip='192.168.0.1',port=6667)
```

创建Pipe，指定连接到my_iotdb的PipeSink，在WHREE子句中输入开始时间点2022年3月30日0时。以下两条执行语句等价。

```
IoTDB> CREATE PIPE p TO my_iotdb FROM (select ** from root where time>=2022-03-30 00:00:00)
IoTDB> CREATE PIPE p TO my_iotdb FROM (select ** from root where time>= 1648569600000)
```

启动Pipe

```Plain%20Text
IoTDB> START PIPE p
```

显示同步任务状态

```
IoTDB> SHOW PIPE p
```

### 结果验证

在发送端执行以下 SQL

```SQL
CREATE DATABASE root.vehicle;
CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE;
CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN;
CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE;
CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN;
insert into root.vehicle.d0(timestamp,s0) values(now(),10);
insert into root.vehicle.d0(timestamp,s0,s1) values(now(),12,'12');
insert into root.vehicle.d0(timestamp,s1) values(now(),'14');
insert into root.vehicle.d1(timestamp,s2) values(now(),16.0);
insert into root.vehicle.d1(timestamp,s2,s3) values(now(),18.0,true);
insert into root.vehicle.d1(timestamp,s3) values(now(),false);
flush;
```

在发送端和接受端执行查询，可查询到相同的结果

```Plain%20Text
IoTDB> select ** from root.vehicle
+-----------------------------+------------------+------------------+------------------+------------------+
|             Time|root.vehicle.d0.s0|root.vehicle.d0.s1|root.vehicle.d1.s3|root.vehicle.d1.s2|
+-----------------------------+------------------+------------------+------------------+------------------+
|2022-04-03T20:08:17.127+08:00|        10|       null|       null|       null|
|2022-04-03T20:08:17.358+08:00|        12|        12|       null|       null|
|2022-04-03T20:08:17.393+08:00|       null|        14|       null|       null|
|2022-04-03T20:08:17.538+08:00|       null|       null|       null|       16.0|
|2022-04-03T20:08:17.753+08:00|       null|       null|       true|       18.0|
|2022-04-03T20:08:18.263+08:00|       null|       null|       false|       null|
+-----------------------------+------------------+------------------+------------------+------------------+
Total line number = 6
It costs 0.134s
```

## 8.常见问题

- 执行 `CREATE PIPESINK demo as IoTDB` 提示 `PIPESINK [demo] already exists in IoTDB.`
  - 原因：当前 PipeSink 已存在
  - 解决方案：删除 PipeSink 后重新创建
- 执行 `DROP PIPESINK pipesinkName` 提示 `Can not drop PIPESINK [demo], because PIPE [mypipe] is using it.`
  - 原因：不允许删除有正在运行的PIPE所使用的 PipeSink
  - 解决方案：在发送端执行 `SHOW PIPE`，停止使用该 PipeSink 的 PIPE
- 执行 `CREATE PIPE p to demo` 提示 `PIPE [p] is STOP, please retry after drop it.`
  - 原因：当前 Pipe 已存在
  - 解决方案：删除 Pipe 后重新创建
- 执行 `CREATE PIPE p to demo`提示 `Fail to create PIPE [p] because Connection refused on DataNode: {id=2, internalEndPoint=TEndPoint(ip:127.0.0.1, port:10732)}.`
  - 原因：存在状态为 Running 的 DataNode 无法连通
  - 解决方案：执行 `SHOW DATANODES` 语句，检查无法连通的 DataNode 网络，或等待其状态变为 Unknown 后重新执行语句。
- 执行 `START PIPE p` 提示 `Fail to start PIPE [p] because Connection refused on DataNode: {id=2, internalEndPoint=TEndPoint(ip:127.0.0.1, port:10732)}.`
  - 原因：存在状态为 Running 的 DataNode 无法连通
  - 解决方案：执行 `SHOW DATANODES` 语句，检查无法连通的 DataNode 网络，或等待其状态变为 Unknown 后重新执行语句。
- 执行 `STOP PIPE p` 提示 `Fail to stop PIPE [p] because Connection refused on DataNode: {id=2, internalEndPoint=TEndPoint(ip:127.0.0.1, port:10732)}.`
  - 原因：存在状态为 Running 的 DataNode 无法连通
  - 解决方案：执行 `SHOW DATANODES` 语句，检查无法连通的 DataNode 网络，或等待其状态变为 Unknown 后重新执行语句。
- 执行 `DROP PIPE p` 提示 `Fail to DROP_PIPE because Fail to drop PIPE [p] because Connection refused on DataNode: {id=2, internalEndPoint=TEndPoint(ip:127.0.0.1, port:10732)}. Please execute [DROP PIPE p] later to retry.`
  - 原因：存在状态为 Running 的 DataNode 无法连通，Pipe 已在部分节点上被删除，状态被置为 ***DROP***。
  - 解决方案：执行 `SHOW DATANODES` 语句，检查无法连通的 DataNode 网络，或等待其状态变为 Unknown 后重新执行语句。
- 运行时日志提示 `org.apache.iotdb.commons.exception.IoTDBException: root.** already been created as database`
  - 原因：同步工具试图在接收端自动创建发送端的Database，属于正常现象
  - 解决方案：无需干预
