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

# 磁盘文件汇总

## 系统文件目录 data/system

* data/system/schema/mlog.txt (元数据日志)

	http://iotdb.apache.org/SystemDesign/SchemaManager/SchemaManager.html#log-management-of-metadata

* data/system/schema/system.properties (系统不可变参数)

	```
	partition_interval=9223372036854775807
	timestamp_precision=ms
	tsfile_storage_fs=LOCAL
	enable_partition=false
	max_degree_of_index_node=1024
	tag_attribute_total_size=700
	iotdb_version=UNKNOWN
	```

* data/system/schema/tlog.txt (标签、属性文件）

	http://iotdb.apache.org/SystemDesign/SchemaManager/SchemaManager.html#tlog


* data/system/storage_groups/{sg_name}/{partition}/Version-xxx （每个分区的版本控制文件，无内容）

* data/system/storage_groups/upgrade/Version-xxx (升级过程的版本控制文件)

* data/system/upgrade/upgrade.txt (升级日志文件)

格式：

```
oldTsFilePath,UpgradeCheckStatus
```

* UpgradeCheckStatus: 表示文件的升级状态
	* BEGIN_UPGRADE_FILE(1): 待升级的文件
	* AFTER_UPGRADE_FILE(2): 升级后的文件已经封口
	* UPGRADE_SUCCESS(3): 完成了旧文件删除，移动新文件

示例：

```
xxx/root.group_11/upgrade/1587634378188-101-0.tsfile,1
xxx/root.group_11/upgrade/1587634378188-101-0.tsfile,2
xxx/root.group_11/upgrade/1587634378188-101-0.tsfile,3
...
```

* data/system/users/{user_name}.profile (用户信息)

```
1. username (UTF-8)
2. password (UTF-8)
3. privilegeNumber n (int)
4. n privileges (Privelege)
5. rolNumber k (int)
6. k role names (UTF-8)
7. UseWaterMarkFlag (Boolean)
```

* data/system/roles/ (权限信息)

```
1. roleName (UTF-8)
2. privilegeNumber n (int)
3. n privileges (Privilege)
```

Privilege 格式

```
1. bindingPath (UTF-8)
2. privilegeNumber n (int)
3. n privilegeOrdinals (int)
```

## 数据文件目录 data/data

* data/data/sequence/{storage_group}/{partition}/{timestamp}-{version}-{merge_times}.tsfile (数据文件)

http://iotdb.apache.org/SystemDesign/TsFile/Format.html#_1-2-tsfile-overview

* data/data/sequence/{storage_group}/{partition}/{tsfile}.resource (对应TsFile的设备时间索引)

```
deviceId(String),start_time(long)
deviceId(String),start_time(long)
deviceId(String),end_time(long)
deviceId(String),end_time(long)
```

* data/data/sequence/{storage_group}/{partition}/{tsfile}.mods (数据修改操作标记)

http://iotdb.apache.org/SystemDesign/DataQuery/QueryFundamentals.html#query-with-modifications

* data/data/sequence/{storage_group}/{partition}/{tsfile}-{level}-{timestamp}.vm (未封口的 TsFile 文件)

* data/data/sequence/{storage_group}/{partition}/{tsfile}.flush (TsFile持久化memtable时的日志文件，无内容)

* data/data/sequence/{storage_group}/{partition}/{tsfile}.vm.log (合并 vm 的日志文件)

```
源文件
文件path
目标文件
文件path
device，offset （已经合并的设备和目标文件完整的ChunkGroup的offset）
```

## 写前日志目录 data/wal

* data/wal/{storage_group}-{tsfile_name}/wal{x} (对应某个 TsFile 的写前日志文件)

文件格式为写入执行计划的序列化内容: InsertRowPlan、InsertTabletPlan、DeletePlan

## 性能追踪目录 data/tracing

* data/tracing/tracing.txt

性能追踪选项默认关闭，用户可以使用 `TRACING ON/OFF` 命令开启/关闭该功能。开启后，追踪日志记录在上述目录下，其格式为：

```
QueryId - Start time
QueryId - Query statement
QueryId - Number of series paths
QueryId - Number of tsfiles
QueryId - Number of sequence files
QueryId - Number of unsequence files
QueryId - Number of chunks
QueryId - Average size of chunks
QueryId - End time
```
