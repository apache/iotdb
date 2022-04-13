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

# 端云协同

## 1.介绍

同步工具是持续将边缘端（发送端） IoTDB 中的时间序列数据上传并加载至云端（接收端） IoTDB 的套件工具。

同步工具的发送端内嵌于 IoTDB 的引擎，使用同步工具需要首先启动IoTDB。

可以在发送端使用 SQL 命令来启动或者关闭一个同步任务，并且可以随时查看同步任务的状态。在接收端，您可以通过设置IP白名单来规定准入IP地址范围。

## 2.模型定义

![img](https://y8dp9fjm8f.feishu.cn/space/api/box/stream/download/asynccode/?code=ODYwOTUxMGI4YjI0N2FlOWFlZmI4MDcxNDlmZDE1MGZfQ1hzcVM1bXJxUkthd1hta3hSaDdnZW1aZHh2cE5iTUxfVG9rZW46Ym94Y25CRjF5dXd0QkVEYzVrSnJHcUdkd3VlXzE2NDkwNTkxNDc6MTY0OTA2Mjc0N19WNA)

假设目前有两台机器A和B都安装了IoTDB，希望将A上的数据不断同步至B中。为了更好地描述这个过程，我们引入以下概念。

- Pipe
  - 指一次同步任务，在上述案例中，我们可以看作在A和B之间有一根数据流管道连接了A和B。
  - 一个Pipe有三种状态，RUNNING，STOP，DROP，分别表示正在运行，暂停和永久取消。
- PipeSink
  - 指接收端，在上述案例中，PipeSink即是B这台机器。PipeSink的类型目前仅支持IoTDB，即接收端为B上安装的IoTDB实例。
  -  PipeServer：当PipeSink的类型为IoTDB的时候，需要打开IoTDB的PipeServer服务来让Pipe数据得到处理。

## 3.注意事项

- 目前仅支持多对一模式，不支持一对多，即一个发送端只能发送数据到一个接收端，而一个接收端可以接受来自多个发送端的数据。

- 发送端只能有一个非DROP状态的Pipe，如果想创建一个新的Pipe，请取消当前Pipe。

- 当有一个或多个发送端指向一个接收端时，这些发送端和接收端各自的设备路径集合之间应当没有交集，否则可能产生不可预料错误 

  。

  - 例如：当发送端A包括路径`root.sg.d.s`，发送端B也包括路径`root.sg.d.s`，当发送端A删除`root.sg`存储组时将也会在接收端删除所有B在接收端的`root.sg.d.s`中存放的数据。

- 两台机器之间目前不支持相互同步。

- 同步工具仅同步所有对数据插入和删除，元数据的创建和删除，如TTL的设置，Trigger，CQ等其他操作均不同步。

## 4.快速上手

在发送端和接收端执行如下语句即可快速开始两个 IoTDB 之间的数据同步，完整的 SQL 语句和配置事项请查看`配置参数`和`SQL`两节，更多使用范例请参考`使用范例`节。

#### 4.1接收端

- 开启PipeServer

```
IoTDB> START PIPESERVER
```

- 关闭PipeServer（在所有发送端取消了Pipe之后执行）

```
IOTDB> STOP PIPESERVER
```

#### 4.2发送端

- 创建接收端为 IoTDB 类型的 Pipe Sink

```
IoTDB> CREATE PIPESINK my_iotdb AS IoTDB (ip='输入你的IP')
```

- 创建同步任务Pipe（开启之前请确保接收端 IoTDB 的 PipeServer 已经启动）

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

- 关闭任务（状态信息可被删除）

```
IoTDB> DROP PIPE my_pipe
```

## 5.配置参数

所有参数修改均在`$IOTDB_HOME$/conf/iotdb-engine.properties`中，所有修改完成之后执行`load configuration`之后即可立刻生效。

#### 5.1发送端相关

| **参数名** | **max_number_of_sync_file_retry**          |
| ---------- | ------------------------------------------ |
| 描述       | 发送端同步文件到接收端失败时的最大重试次数 |
| 类型       | Int : [0,2147483647]                       |
| 默认值     | 5                                          |



#### 5.2接收端相关

| **参数名** | **ip_white_list**                                            |
| ---------- | ------------------------------------------------------------ |
| 描述       | 设置同步功能发送端 IP 地址的白名单，以网段的形式表示，多个网段之间用逗号分隔。发送端向接收端同步数据时，只有当该发送端 IP 地址处于该白名单设置的网段范围内，接收端才允许同步操作。如果白名单为空，则接收端不允许任何发送端同步数据。默认接收端接受全部 IP 的同步请求。 |
| 类型       | String                                                       |
| 默认值     | 0.0.0.0/0                                                    |



| **参数名** | ***pipe_server_port***                                       |
| ---------- | ------------------------------------------------------------ |
| 描述       | 同步接收端服务器监听接口，请确认该端口不是系统保留端口并且未被占用。 |
| 类型       | Short Int : [0,65535]                                        |
| 默认值     | 6670                                                         |



## 6.SQL

#### 6.1发送端

- 创建接收端为 IoTDB 类型的 Pipe Sink，其中IP和port是可选参数

```
IoTDB> CREATE PIPESINK <PipeSinkName> AS IoTDB [(ip='127.0.0.1',port=6670);]
```

- 显示当前所能支持的 Pipe Sink 类型

```Plain%20Text
IoTDB> SHOW PIPESINKTYPE
IoTDB>
+-----+
| type|
+-----+
|IoTDB|
+-----+
```

- 显示当前所有 Pipe Sink 定义，结果集有三列，分别表示pipesink的名字，pipesink的类型，pipesink的属性

```
IoTDB> SHOW PIPESINKS
IoTDB> SHOW PIPESINK [PipeSinkName]
IoTDB> 
+-----------+-----+------------------------+
|       name| type|              attributes|
+-----------+-----+------------------------+
|my_pipesink|IoTDB|ip='127.0.0.1',port=6670|
+-----------+-----+------------------------+
```

- 删除 Pipe Sink 信息

```
IoTDB> DROP PIPESINK <PipeSinkName>
```

- 创建同步任务
  - 其中Select语句目前仅支持`**`（即所有序列中的数据），FROM语句目前仅支持`root`，Where语句仅支持指定time的起始时间
  - `SyncDelOp`参数为true时会同步删除数据操作，否则不同步删除数据操作

```
IoTDB> CREATE PIPE my_pipe TO my_iotdb [FROM (select ** from root WHERE time>=yyyy-mm-dd HH:MM:SS)] [WITH SyncDelOp=true]
```

- 显示所有同步任务状态

> 该指令在发送端和接收端均可执行

- create time，pipe的创建时间

- name，pipe的名字

- role，当前IoTDB在pipe中的角色，可能有两种角色：
  - sender，当前IoTDB为同步发送端

- receiver，当前IoTDB为同步接收端

- remote，pipe的对端信息
  - 当role为receiver时，这一字段值为发送端ip

- 当role为sender时，这一字段值为pipeSink名称

- status，pipe状态
- message，pipe运行信息，当pipe正常运行时，这一字段通常为空，当出现异常时，可能出现两种状态：
  - WARN状态，这表明发生了数据丢失或者其他错误，但是Pipe会保持运行
  - ERROR状态，这表明发生了网络长时间中断或者接收端出现问题，Pipe被停止，置为STOP状态

```
IoTDB> SHOW PIPES
IoTDB>
+-----------------------+--------+--------+-------------+---------+-------+
|            create time|   name |    role|       remote|   status|message|
+-----------------------+--------+--------+-------------+---------+-------+
|2022-03-30T20:58:30.689|my_pipe1|  sender|  my_pipesink|     STOP|       |
+-----------------------+--------+--------+-------------+---------+-------+ 
|2022-03-31T12:55:28.129|my_pipe2|receiver|192.168.11.11|  RUNNING|       |
+-----------------------+--------+--------+-------------+---------+-------+
```

- 显示指定同步任务状态，当未指定PipeName时，与`SHOW PIPES`等效

```
IoTDB> SHOW PIPE [PipeName]
```

- 暂停任务

```
IoTDB> STOP PIPE <PipeName>
```

- 继续被暂停的任务

```
IoTDB> START PIPE <PipeName>
```

- 关闭任务（状态信息可被删除）

```
IoTDB> DROP PIPE <PipeName>
```

#### 6.2接收端

- 启动本地的 IoTDB Pipe Server

```
IoTDB> START PIPESERVER
```

- 关闭本地的 IoTDB Pipe Server

```
IoTDB> STOP PIPESERVER
```

- 显示本地 Pipe Server 的信息
  - true表示PipeServer正在运行，false表示PipeServer停止服务

```
IoTDB> SHOW PIPESERVER
+----------+
|    enalbe|
+----------+
|true/false|
+----------+
```

## 7.使用示例

#### **目标**

- 创建一个从边端 IoTDB 到 云端 IoTDB 的 同步工作
- 边端希望同步从2022年3月30日0时之后的数据
- 边端不希望同步所有的删除操作
- 边端处于弱网环境，需要配置更多的重试次数
- 云端IoTDB仅接受来自边端的IoTDB的数据

#### **接收端操作**

- `vi conf/iotdb-engine.properties` 配置云端参数，将白名单设置为仅接收来自IP为 192.168.0.1的边端的数据

```
####################
### PIPE Server Configuration
####################
# PIPE server port to listen
# Datatype: int
# pipe_server_port=6670

# White IP list of Sync client.
# Please use the form of network segment to present the range of IP, for example: 192.168.0.0/16
# If there are more than one IP segment, please separate them by commas
# The default is to allow all IP to sync
# Datatype: String
ip_white_list=192.168.0.1/32
```

- 云端启动 IoTDB 同步接收端

```
IoTDB> START PIPESERVER
```

- 云端显示 IoTDB 同步接收端信息，如果结果为true则表示正确启动

```
IoTDB> SHOW PIPESERVER
```

#### **发送端操作**

- 配置边端参数，将`max_number_of_sync_file_retry`参数设置为10

```
####################
### PIPE Sender Configuration
####################
# The maximum number of retry when syncing a file to receiver fails.
max_number_of_sync_file_retry=10
```

- 创建云端PipeSink，指定类型为IoTDB，指定云端IP地址为192.168.0.1，指定云端的PipeServer服务端口为6670

```
IoTDB> CREATE PIPESINK my_iotdb AS IoTDB (ip='192.168.0.1',port=6670)
```

- 创建Pipe，指定连接到my_iotdb的PipeSink，在WHREE子句中输入开始时间点2022年3月30日0时，将SyncDelOp置为false

```
IoTDB> CREATE PIPE p TO my_iotdb FROM (select ** from root where time>=2022-03-30 00:00:00) WITH SyncDelOp=false
```

- 启动Pipe

```Plain%20Text
IoTDB> START PIPE p
```

- 显示同步任务状态

```
IoTDB> SHOW PIPE p
```

#### 结果验证

在发送端执行以下SQL

```SQL
SET STORAGE GROUP TO root.vehicle;
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

- 执行 

  ```
  STOP PIPESERVER
  ```

   关闭本地的 IoTDB Pipe Server 时提示 

  ```
  Msg: 328: Failed to stop pipe server because there is pipe still running.
  ```

  - 原因：接收端有正在运行的同步任务
  - 解决方案：在发送端先执行 `STOP PIPE` PipeName 停止任务，后关闭 IoTDB Pipe Server

- 执行 

  ```
  CREATE PIPE mypipe
  ```

    提示  

  ```
  Msg: 411: Create transport for pipe mypipe error, because CREATE request connects to receiver 127.0.0.1:6670 error..
  ```

  - 原因：接收端未启动或接收端无法连接
  - 解决方案：在接收端执行 `SHOW PIPESERVER` 检查是否启动接收端，若未启动使用 `START PIPESERVER` 启动；检查接收端`iotdb-engine.properties`中的白名单是否包含发送端ip。

- 执行 

  ```
  DROP PIPESINK pipesinkName
  ```

   提示 

  ```
  Msg: 411: Can not drop pipeSink demo, because pipe mypipe is using it.
  ```

  - 原因：不允许删除有正在运行的PIPE所使用的 PipeSink
  - 解决方案：在发送端执行 `SHOW PIPE`，停止使用该 PipeSink 的 PIPE

- 发送端创建 PIPE 提示 

  ```
  Msg: 411: Pipe p is RUNNING, please retry after drop it.
  ```

  - 原因：已有运行中的 PIPE
  - 解决方案：执行 `DROP PIPE p `后重试
