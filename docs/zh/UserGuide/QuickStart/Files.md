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

# 数据文件存储

本节将介绍 IoTDB 的数据存储方式，便于您对 IoTDB 的数据管理有一个直观的了解。

IoTDB 需要存储的数据分为三类，分别为数据文件、系统文件以及写前日志文件。

## 数据文件
> 在 basedir/data/目录下

数据文件存储了用户写入 IoTDB 系统的所有数据。包含 TsFile 文件和其他文件，可通过 [data_dirs 配置项](../Reference/DataNode-Config-Manual.md) 进行配置。

为了更好的支持用户对于磁盘空间扩展等存储需求，IoTDB 为 TsFile 的存储配置增加了多文件目录的存储方式，用户可自主配置多个存储路径作为数据的持久化位置（详情见 [data_dirs 配置项](../Reference/DataNode-Config-Manual.md)），并可以指定或自定义目录选择策略（详情见 [multi_dir_strategy 配置项](../Reference/DataNode-Config-Manual.md)）。

### TsFile
> 在 basedir/data/sequence or unsequence/{DatabaseName}/{DataRegionId}/{TimePartitionId}/目录下
1. {time}-{version}-{inner_compaction_count}-{cross_compaction_count}.tsfile
    + 数据文件
2. {TsFileName}.tsfile.mod
    + 更新文件，主要记录删除操作

### TsFileResource
1. {TsFileName}.tsfile.resource
    + TsFile 的概要与索引文件

### 与合并相关的数据文件
> 在 basedir/data/sequence or unsequence/{DatabaseName}/目录下

1. 后缀为`.cross ` 或者 `.inner`
    + 合并过程中产生的临时文件
2. 后缀为`.inner-compaction.log` 或者 `.cross-compaction.log`
    + 记录合并进展的日志文件
3. 后缀为`.compaction.mods`
    + 记录合并过程中发生的删除等操作
4. 后缀为`.meta`的文件
    + 合并过程生成的元数据临时文件

## 系统文件

系统 Schema 文件，存储了数据文件的元数据信息。可通过 system_dir 配置项进行配置（详情见 [system_dir 配置项](../Reference/DataNode-Config-Manual.md)）。

### 元数据相关文件
> 在 basedir/system/schema 目录下

#### 元数据
1. mlog.bin
    + 记录的是元数据操作
2. mtree-1.snapshot
    + 元数据快照
3. mtree-1.snapshot.tmp
    + 临时文件，防止快照更新时，损坏旧快照文件

#### 标签和属性
1. tlog.txt
    + 存储每个时序的标签和属性
    + 默认情况下每个时序 700 字节

### 其他系统文件
#### Version
> 在 basedir/system/database/{DatabaseName}/{TimePartitionId} or upgrade 目录下
1. Version-{version}
    + 版本号文件，使用文件名来记录当前最大的版本号

#### Upgrade
> 在 basedir/system/upgrade 目录下
1. upgrade.txt
    + 记录升级进度

#### Authority
> 在 basedir/system/users/目录下是用户信息
> 在 basedir/system/roles/目录下是角色信息

#### CompressRatio
> 在 basedir/system/compression_ration 目录下
1. Ration-{compressionRatioSum}-{calTimes}
    + 记录每个文件的压缩率
## 写前日志文件
写前日志文件存储了系统的写前日志。可通过`wal_dir`配置项进行配置（详情见 [wal_dir 配置项](../Reference/DataNode-Config-Manual.md)）。
> 在 basedir/wal 目录下
1. {DatabaseName}-{TsFileName}/wal1
    + 每个 memtable 会对应一个 wal 文件


## 数据存储目录设置举例

接下来我们将举一个数据目录配置的例子，来具体说明如何配置数据的存储目录。

IoTDB 涉及到的所有数据目录路径有：data_dirs, multi_dir_strategy, system_dir 和 wal_dir，它们分别涉及的是 IoTDB 的数据文件、数据文件多目录存储策略、系统文件以及写前日志文件。您可以选择输入路径自行配置，也可以不进行任何操作使用系统默认的配置项。

以下我们给出一个用户对五个目录都进行自行配置的例子。

```
dn_system_dir = $IOTDB_HOME/data/datanode/system
dn_data_dirs = /data1/datanode/data, /data2/datanode/data, /data3/datanode/data 
dn_multi_dir_strategy=MaxDiskUsableSpaceFirstStrategy
dn_wal_dir= $IOTDB_HOME/data/datanode/wal
```
按照上述配置，系统会：

* 将 TsFile 存储在路径 /data1/datanode/data、路径 /data2/datanode/data 和路径 /data3/datanode/data 中。且对这三个路径的选择策略是：`优先选择磁盘剩余空间最大的目录`，即在每次数据持久化到磁盘时系统会自动选择磁盘剩余空间最大的一个目录将数据进行写入
* 将系统文件存储在$IOTDB_HOME/data/datanode/data
* 将写前日志文件存储在$IOTDB_HOME/data/datanode/wal
