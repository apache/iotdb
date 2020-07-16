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

# 数据分区

时间序列数据在存储组和时间范围两个层级上进行分区

## 存储组

存储组由用户显示指定，使用语句"SET STORAGE GROUP TO"来指定存储组，每一个存储组有一个对应的 StorageGroupProcessor

其拥有的主要字段为：

* 一个读写锁: insertLock

* 每个时间分区所对应的未关闭的顺序文件处理器: workSequenceTsFileProcessors

* 每个时间分区所对应的未关闭的乱序文件处理器: workUnsequenceTsFileProcessors

* 该存储组的全部顺序文件列表（按照时间排序）: sequenceFileTreeSet

* 该存储组的全部乱序文件列表（无顺序）: unSequenceFileList

* 记录每一个设备最后写入时间的map，顺序数据刷盘时会使用该map记录的时间: latestTimeForEachDevice

* 记录每一个设备最后刷盘时间的map，用来区分顺序和乱序数据: latestFlushedTimeForEachDevice

* 每个时间分区所对应的版本生成器map，便于查询时确定不同chunk的优先级: timePartitionIdVersionControllerMap


### 相关代码

* src/main/java/org/apache/iotdb/db/engine/StorageEngine.java


## 时间范围

同一个存储组中的数据按照用户指定的时间范围进行分区，相关参数为partition_interval，默认为周，也就是不同周的数据会放在不同的分区中

### 实现逻辑

StorageGroupProcessor 对插入的数据进行分区计算，找到指定的 TsFileProcessor，而每一个 TsFileProcessor 对应的 TsFile 会被放置在不同的分区文件夹内

### 文件结构

分区后的文件结构如下：

data

-- sequence

---- [存储组名1]

------ [时间分区ID1]

-------- xxxx.tsfile

-------- xxxx.resource

------ [时间分区ID2]

---- [存储组名2]

-- unsequence

### 相关代码

* src/main/java/org/apache/iotdb/db/engine/storagegroup.StoragetGroupProcessor.java 中的 getOrCreateTsFileProcessorIntern 方法
