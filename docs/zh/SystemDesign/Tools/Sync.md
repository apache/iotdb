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

<!-- TOC -->

- [同步工具](#同步工具)
    - [概述](#概述)
        - [场景](#场景)
        - [目标](#目标)
    - [目录结构](#目录结构)
        - [目录结构设计](#目录结构设计)
        - [目录结构说明](#目录结构说明)
            - [发送端](#发送端)
            - [接收端](#接收端)
            - [其他](#其他)
    - [同步工具发送端](#同步工具发送端)
        - [需求说明](#需求说明)
        - [模块设计](#模块设计)
            - [文件管理模块](#文件管理模块)
                - [包](#包)
                - [文件选择](#文件选择)
                - [文件清理](#文件清理)
            - [文件传输模块](#文件传输模块)
                - [包](#包-1)
                - [同步 schema](#同步-schema)
                - [同步数据文件](#同步数据文件)
            - [恢复模块](#恢复模块)
                - [包](#包-2)
                - [流程](#流程)
    - [同步工具接收端](#同步工具接收端)
        - [需求说明](#需求说明-1)
        - [模块设计](#模块设计-1)
            - [文件传输模块](#文件传输模块-1)
                - [包](#包-3)
                - [流程](#流程-1)
            - [文件加载模块](#文件加载模块)
                - [包](#包-4)
                - [文件删除](#文件删除)
                - [加载新文件](#加载新文件)
            - [恢复模块](#恢复模块-1)
                - [包](#包-5)
                - [流程](#流程-2)

<!-- /TOC -->

# 同步工具

同步工具是定期将本地磁盘中和新增的已持久化的 tsfile 文件上传至云端并加载到 Apache IoTDB 的套件工具。

## 概述

本文档主要介绍了同步工具的需求定义、模块设计等方面。

### 场景

同步工具的需求主要有以下几个方面：

* 在生产环境中，Apache IoTDB 会收集数据源（工业设备、移动端等）产生的数据存储到本地。由于数据源可能分布在不同的地方，可能会有多个 Apache IoTDB 同时负责收集数据。针对每一个 IoTDB，它需要将自己本地的数据同步到数据中心中。数据中心负责收集并管理来自多个 Apache IoTDB 的数据。

* 随着 Apache IoTDB 系统的广泛应用，用户根据目标业务需求需要将一些 Apache IoTDB 实例生成的 tsfile 文件放在另一个 Apache IoTDB 实例的数据目录下加载并应用，实现数据同步。

* 同步模块在发送端以独立进程的形式存在，在接收端和 Apache IoTDB 位于同一进程内。

* 支持一个发送端向多个接收端同步数据且一个接收端可同时接收多个发送端的数据，但需要保证多个发送端同步的数据不冲突（即一个设备的数据来源只能有一个），否则需要提示冲突。

### 目标

利用同步工具可以将数据文件在两个 Apache IoTDB 实例间传输并加载。在网络不稳定或宕机等情况发生时，保证文件能够被完整、正确地传送到数据中心。

## 目录结构

为方便说明，设应用场景为节点`192.168.130.15`向节点`192.168.130.16:5555`同步数据，同时节点`192.168.130.15`接收来自`192.168.130.14`节点同步的数据。由于节点`192.168.130.15`同时作为发送端和接收端，因此下面以节点`192.168.130.15`来说明目录结构。

### 目录结构设计

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/26211279/74145347-849dc380-4c39-11ea-9ef2-e10a3fe2074d.png">

### 目录结构说明

sync-sender 文件夹中包含该节点作为发送端时同步数据期间的临时文件、状态日志等。

sync-receiver 文件夹中包含该节点作为接收端时接收数据并加载期间的临时文件、状态日志等。

schema/sync 文件夹下保存的是需要持久化的同步信息。

#### 发送端

`data/sync-sender`为发送端文件夹，该目录下的文件夹名表示接收端的 IP 和端口，在该实例中有一个接收端`192.168.130.16:5555`, 每个文件夹下包含以下几个文件：

* last_local_files.txt 
记录同步任务结束后所有已被同步的本地 tsfile 文件列表，每次同步任务结束后更新。

* snapshot 
在同步数据期间，该文件夹下含有所有的待同步的 tsfile 文件的硬链接。

* sync.log
记录同步模块的任务进度日志，用以系统宕机恢复时使用，后文会对该文件的结构进行详细阐述。

#### 接收端

`sync-receiver`为接收端文件夹，该目录下的文件夹名表示发送端的 IP 和 UUID，表示从该发送端接收到的数据文件和文件加载日志情况等，在该实例中有一个发送端`192.168.130.14`，它的 UUID 为`a45b6e63eb434aad891264b5c08d448e`。 每个文件夹下包含以下几个文件：

* load.log 
该文件是记录 tsfile 文件加载的任务进度日志，用以系统宕机恢复时使用。

* data
该文件夹中含有已从发送端接收到的 tsfile 文件。

#### 其他

`schema/sync`文件夹下包含以下信息：

* 作为发送端时，发送端实例的文件锁`sync.lock`
    该文件锁的目的是保证同一个发送端对同一个接收端仅能启动一个发送端实例，即只有一个向该接收端同步数据的进程。图示中目录`192.168.130.16_5555/sync_lock`表示对接收端`192.168.130.16_5555`同步的
    实例锁。每次启动时首先会检查该文件是否上锁，如果上锁说明已经有向该接收端同步数据的发送端，则停止本实例。

* 作为发送端时，发送端的唯一标识 UUID`uuid.txt`
    每个发送端有一个唯一的标识，以供接收端区分不同的发送端

* 作为发送端时，每个接收端的 schema 同步进度`sync_schema_pos`

    由于 schema 日志`mlog.txt`数据是追加的，其中记录了所有元信息的变化过程，因此每次同步完 schema 后记录下当前位置在下次同步时直接增量同步即可减少重复 schema 传输。

* 作为接收端，接收端中每个设备的所有信息`device_owner.log`
    同步工具的应用中，一个接收端可以同时接收多个发送端的数据，但是不能产生冲突，否则接收端将不能保证数据的正确性。因此需要记录下每个设备是由哪个发送端进行同步的，遵循先到先得原则。

单独将这些信息放在 schmea 文件夹下的原因是一个 Apache IoTDB 实例可以拥有多个数据文件目录，也就是 data 目录可以有多个，但是 schema 文件夹只有一个，而这些信息是一个发送端实例共享的信息，而 data 文件夹下的信息表示的是该文件目录下的同步情况，属于子任务信息（每个数据文件目录即为一个子任务）。

## 同步工具发送端

### 需求说明

* 每隔一段时间将发送端收集到的最新数据回传到接收端上。同时，针对历史数据的更新和删除，将这部分信息同步到接收端上。

* 同步数据必须完整，如果在传输的过程中因为网络不稳定、机器故障等因素造成数据文件不完整或者损坏，需要在下一次传输的过程中修复。

### 模块设计

#### 文件管理模块

##### 包

org.apache.iotdb.db.sync.sender.manage

##### 文件选择

文件选择的功能是选出当前 Apache IoTDB 实例中已封口的 tsfile 文件（有对应的`.resource`文件，且不含`.modification`文件和`.merge`文件）列表和上次同步任务结束后记录的 tsfile 文件列表的差异，共有两部分：删除的 tsfile 文件列表和新增的 tsfile 文件列表。并对所有的新增的文件进行硬链接，防止同步期间由于系统运行导致的文件删除等操作。

##### 文件清理

当接收到文件传输模块的任务结束的通知时，执行以下命令：

* 将`last_local_files.txt`文件中的文件名列表加载到内存形成 set，并逐行解析`log.sync`对 set 进行删除和添加

* 将内存中的文件名列表 set 写入`current_local_files.txt`文件中

* 删除`last_local_files.txt`文件

* 将`current_local_files.txt`重命名为`last_local_files.txt`

* 删除 sequence 文件夹和`sync.log`文件

#### 文件传输模块

##### 包

org.apache.iotdb.db.sync.sender.transfer

##### 同步 schema

在同步数据文件前，首先同步新增的 schmea 信息，并更新`sync_schema_pos`。

##### 同步数据文件

对于每个文件路径，调用文件管理模块，获得删除的文件列表和新增的文件列表，然后执行以下流程：

1. 开始同步任务，在`sync.log`记录`sync start`
2. 开始同步删除的文件列表，在`sync.log`记录`sync deleted file names start`
3. 通知接收端开始同步删除的文件名列表，
4. 对删除列表中的每一个文件名
    4.1. 向接收端传输文件名（示例`1581324718762-101-1.tsfile`)
    4.2. 传输成功，在`sync.log`中记录`1581324718762-101-1.tsfile`
5. 开始同步新增的 tsfile 文件列表，在`sync.log`记录`sync deleted file names end 和 sync tsfile start`
6. 通知接收端开始同步文件
7. 对新增列表中的每一个 tsfile：
    7.1. 将文件按块传输给接收端（示例`1581324718762-101-1.tsfile`)
    7.2. 若文件传输失败，则多次尝试，若尝试超过一定次数（可由用户配置，默认为 5)，放弃该文件的传输；若传输成功，在`sync.log`中记录`1581324718762-101-1.tsfile`
8. 通知接收端同步任务结束，在`sync.log`记录`sync tsfile end`和`sync end`
9. 调用文件管理模块清理文件
10. 结束同步任务

#### 恢复模块

##### 包

org.apache.iotdb.db.sync.sender.recover

##### 流程

同步工具发送端每次启动同步任务时，首先检查发送端文件夹下有没有对应的接收端文件夹，若没有，表示没有和该接收端进行过同步任务，跳过恢复模块；否则，根据该文件夹下的文件执行恢复算法：

1. 若存在`current_local_files.txt`，跳转到步骤 2；若不存在，跳转到步骤 3
2. 若存在`last_local_files.txt`，则删除`current_local_files.txt`文件并跳转到步骤
3；若不存在，跳转到步骤 7
3. 若存在`sync.log`，跳转到步骤 4；若不存在，跳转到步骤 8
4. 将`last_local_files.txt`文件中的文件名列表加载到内存形成 set，并逐行解析
`sync.log`对 set 进行删除和添加
5. 将内存中的文件名列表 set 写入`current_local_files.txt`文件中
6. 删除`last_local_files.txt`文件
7. 将`current_local_files.txt`重命名为`last_local_files.txt`
8. 删除 sequence 文件夹和`sync.log`文件
9. 算法结束

## 同步工具接收端

### 需求说明

* 由于接收端需要同时接收来自多个发送端的文件，需要将不同发送端的文件区分开来，统一管理这些文件。

* 接收端从发送端接收文件并检验文件名、文件数据和该文件的 MD5 值。文件接收完成后，存储文件到接收端本地，并对接收到的 tsfile 文件进行 MD5 值校验和文件尾部检查，若检查通过若未正确接收，则对文件进行重传。

* 针对发送端传来的数据文件（可能包含了对旧数据的更新，新数据的插入等操作），需要将这部分数据合并到接收端本地的文件中。

### 模块设计

#### 文件传输模块

##### 包

org.apache.iotdb.db.sync.receiver.transfer

##### 流程

文件传输模块负责接收从发送端传输的文件名和文件，其流程如下：

1. 接收到发送端的同步开始指令，检查是否存在 sync.log 文件，若存在则表示上次同步的数据还未加载完毕，拒绝本次同步任务；否则在 sync.log 中记录 sync.start
2. 接收到发送端的开始同步删除的文件名列表的指令，在 sync.log 中记录 sync deleted file names start
3. 依次接收发送端传输的删除文件名
    3.1. 接收到发送端传输的文件名（示例`1581324718762-101-1.tsfile`)
    3.2. 接收成功，在`sync.log`中记录`1581324718762-101-1.tsfile`，并提交给数据加载模块处理
4. 接收到发送单的开始同步传输的文件的指令，在`sync.log`中记录`sync deleted file names end`和`sync tsfile start`
5. 依次接收发送端传输的 tsfile 文件
    5.1. 按块接收发送端传输的文件（示例`1581324718762-101-2.tsfile`)
    5.2. 对文件进行校验，若检验失败，删除该文件并通知发送端失败；否则，在`sync.log`中记录`1581324718762-101-2.tsfile`，并提交给数据加载模块处理
6. 接收到发送端的同步任务结束命令，在`sync.log`中记录`sync tsfile end`和`sync end`
7. 创建 sync.end 空文件

#### 文件加载模块

##### 包

org.apache.iotdb.db.sync.receiver.load

##### 文件删除

对于需要删除的文件（示例`1581324718762-101-1.tsfile`)，在内存中的`sequence tsfile list`中搜索是否有该文件，如有则将该文件从内存中维护的列表中删除并将磁盘中的文件删除。执行成功后在`load.log`中记录`delete 1581324718762-101-1.tsfile`。

##### 加载新文件

对于需要加载的文件（示例`1581324718762-101-1.tsfile`)，首先用`device_owner.log`检查该文件是否符合应用场景，即是否和其他发送端传输了相同设备的数据导致了冲突），如果发生了冲突，则拒绝此次加载并向发送端发送错误信息；否则，更新`device_owner.log`信息。

符合应用场景要求后，将该文件插入`sequence tsfile list`中合适的位置并将文件移动到`data/sequence`目录下。执行成功后在`load.log`中记录`load 1581324718762-101-1.tsfile`。每次文件加载完毕后，检查同步的目录下是否含有 sync.end 文件，如含有该文件且 sequence 文件夹下为空，则先删除 sync.log 文件，再删除 load.log 和 sync.end 文件。

#### 恢复模块

##### 包
org.apache.iotdb.db.sync.receiver.recover

##### 流程

ApacheIoTDB 系统启动时，依次检查 sync 文件夹下的各个子文件夹，每个子文件表示由文件夹名所代表的发送端的同步任务。根据每个子文件夹下的文件执行恢复算法：

1. 若不存在`sync.log`文件，跳转到步骤 4；若存在，跳转到步骤 2
2. 逐行扫描 sync.log 的日志，执行对应的删除文件的操作和加载文件的操作，若该操作已在`load.log`文件中记录，则表明已经执行完毕，跳过该操作。跳转到步骤 3
3. 删除`sync.log`文件
4. 删除`load.log`文件
5. 删除`sync.end`文件
6. 算法结束

每一次同步任务开始时，接收端对相应的子文件夹进行检查并恢复。