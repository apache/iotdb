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

> 下面是IoTDB生成或使用的文件
>
> 持续更新中...

# 单机模式

## 配置文件
> conf目录下
1. iotdb-engine.properties
2. logback.xml
3. iotdb-env.sh
4. jmx.access
5. jmx.password
6. iotdb-sync-client.properties
    + 只有Sync工具会使用

> 在basedir/system/schema目录下
1. system.properties
    + 记录的是所有不能变动的配置，启动时会检查，防止系统错误

## 状态相关的文件

### 元数据相关文件
> 在basedir/system/schema目录下

#### 元数据
1. mlog.txt
    + 记录的是元数据操作
2. mtree-1.snapshot
    + 元数据快照
3. mtree-1.snapshot.tmp
    + 临时文件，防止快照更新时，损坏旧快照文件

#### 标签和属性
1. tlog.txt
    + 存储每个时序的标签和属性
    + 默认情况下每个时序700字节

### 数据相关文件
> 在basedir/data/目录下

#### WAL
> 在basedir/wal目录下
1. {StroageName}-{TsFileName}/wal1
    + 每个memtable会对应一个wal文件

#### TsFile
> 在basedir/data/sequence or unsequence/{StorageGroupName}/{TimePartitionId}/目录下
1. {time}-{version}-{mergeCnt}.tsfile
    + 数据文件
2. {TsFileName}-{level}-{time}.vm
    + 虚拟合并数据文件，用于优化查询，详细请看https://issues.apache.org/jira/browse/IOTDB-706
3. {TsFileName}.tsfile.mod
    + 更新文件，主要记录删除操作

#### TsFileResource
1. {TsFileName}.tsfile.resource
    + TsFile的概要与索引文件
2. {TsFileName}.tsfile.resource.temp
    + 临时文件，用于避免更新tsfile.resource时损坏tsfile.resource
3. {TsFileName}.tsfile.resource.closing
    + 关闭标记文件，用于标记TsFile处于关闭状态，重启后可以据此选择是关闭或继续写入该文件

#### Version
> 在basedir/system/storage_groups/{StorageGroupName}/{TimePartitionId} or upgrade目录下
1. Version-{version}
    + 版本号文件，使用文件名来记录当前最大的版本号

#### Upgrade
> 在basedir/system/upgrade目录下
1. upgrade.txt
    + 记录升级进度

#### Merge
> 在basedir/system/storage_groups/{StrorageGroup}/目录下
1. merge.mods
    + 记录合并过程中发生的删除等操作
2. merge.log
    + 记录合并进展
3. tsfile.merge
    + 临时文件，每个顺序文件在合并时会产生一个对应的merge文件，用于存放临时数据

#### Authority
> 在basedir/system/users/目录下是用户信息
> 在basedir/system/roles/目录下是角色信息

#### CompressRatio
> 在basedir/system/compression_ration目录下
1. Ration-{compressionRatioSum}-{calTimes}
    + 记录每个文件的压缩率

---

# 集群模式
> 注意：下面文件是相对单机新增的文件列表

## 配置文件
1. iotdb-cluster.properties
2. cluster-env.sh

## 状态相关文件
> 在basedir目录下
1. node_identifier
    + 本地节点在集群中的唯一标识
2. partitions
    + 分区表文件，记录数据分布信息
3. {time}_{random}.task
    + pullSnapshotTask文件，记录的是slot和owner，当一个节点加入集群时，会创建这个文件来跟踪数据拉取情况
    + 在basedir/raft/{nodeIdentifier}/snapshot_task/目录下

## Raft相关文件
> 在basedir/system/raftLog/{nodeIdentifier}/目录下

### Raft Log
1. .data-{version}
    + raft committed log, 默认只保留最新的1000条日志

### Raft Meta
1. logMeta
    + raft一些相关元数据，例如选举相关信息和日志提交信息
        + hardState: voteFor, term
        + Meta: commitLogTerm, commitLogIndex, lastLogTerm, lastLogIndex
        + ...
2. logMeta.tmp
    + 临时文件，用于更新logMeta时防止损坏旧的logMeta文件

### Raft Catch Up
> 在basedir/remote/{nodeIdentifier}/{storageGroupName}/{partitionNum}/目录下
1. {fileName}.tsfile
    + 远程TsFile，拉取完毕会load进对应的存储组数据目录下
2. {fileName}.tsfile.mod
    + 远程更新文件TsFile.mod，拉取完毕会load进对应的存储组数据目录下