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

当前用户可以使用多种手段对正在运行的 IoTDB 进程进行系统监控，包括使用 Java 的 Jconsole 工具对正在运行的 IoTDB 进程进行系统状态监控，使用 IoTDB 为用户开发的接口查看数据统计量，使用监控框架进行 IoTDB 的运行状态监控

# 1. 监控框架

在 IoTDB 的运行过程中，我们希望对 IoTDB 的状态进行观测，以便于排查系统问题或者及时发现系统潜在的风险，能够**反映系统运行状态的一系列指标**就是系统监控指标。

## 1.1. 什么场景下会使用到监控框架?

那么什么时候会用到监控框架呢？下面列举一些常见的场景。

1. 系统变慢了

   系统变慢几乎是最常见也最头疼的问题，这时候我们需要尽可能多的信息来帮助我们找到系统变慢的原因，比如：

    - JVM信息：是不是有FGC？GC耗时多少？GC后内存有没有恢复？是不是有大量的线程？
    - 系统信息：CPU使用率是不是太高了？磁盘IO是不是很频繁？
    - 连接数：当前连接是不是太多？
    - 接口：当前TPS是多少？各个接口耗时有没有变化？
    - 线程池：系统中各种任务是否有积压？
    - 缓存命中率

2. 磁盘快满了

   这时候我们迫切想知道最近一段时间数据文件的增长情况，看看是不是某种文件有突增。

3. 系统运行是否正常

   此时我们可能需要通过错误日志的数量、集群节点的状态等指标来判断系统是否在正常运行。

## 1.2. 什么人需要使用监控框架?

所有关注系统状态的人员都可以使用，包括但不限于研发、测试、运维、DBA等等

## 1.3. 什么是监控指标？

### 1.3.1. 监控指标名词解释

在 IoTDB 的监控模块，每个监控质指标被 Metric Name 和 Tags 唯一标识。

- Metric Name：**指标名称**，比如logback_events_total表示日志事件发生的总次数。
- Tags：**指标分类**，形式为Key-Value对，每个指标下面可以有0到多个分类，常见的Key-Value对：
  - `name = xxx`：被监控项的名称，比如对`entry_seconds_count`这个监控项，name 的含义是被监控的接口名称。
  - `status = xxx`：被监控项的状态细分，比如监控 Task 的监控项可以通过该参数，将运行的 Task 和停止的 Task 分开。
  - `user = xxx`：被监控项和某个特定用户相关，比如统计root用户的写入总次数。
  - 根据具体情况自定义：比如logback_events_total下有一个```level```的分类，用来表示特定级别下的日志数量
- Metric Level：**指标管理级别**：默认启动级别为`Core`级别，建议启动级别为`Important级别`，审核严格程度`Core > Important > Normal > All`
    - `Core`：系统的核心指标，供**系统内核和运维人员**使用，关乎系统的**性能、稳定性、安全性**，比如实例的状况，系统的负载等。
    - `Important`：模块的重要指标，供**运维和测试人员**使用，直接关乎**每个模块的运行状态**，比如合并文件个数、执行情况等。
    - `Normal`：模块的一般指标，供**开发人员**使用，方便在出现问题时**定位模块**，比如合并中的特定关键操作情况。
    - `All`：模块的全部指标，供**模块开发人员**使用，往往在复现问题的时候使用，从而快速解决问题。

### 1.3.2. 数据格式

- IoTDB 对外提供 JMX 、 Prometheus 和 IoTDB 格式的监控指标：
  - 对于 JMX ，可以通过```org.apache.iotdb.metrics```获取系统监控指标指标。
  - 对于 Prometheus ，可以通过对外暴露的端口的 /metrics 获取。
  - 对于 IoTDB ，可以通过执行具体查询获取监控指标

## 1.4. 监控指标有哪些？

目前，IoTDB 对外提供一些主要模块的监控指标，并且随着新功能的开发以及系统优化或者重构，监控指标也会同步添加和更新。

IoTDB 的监控指标被划分为 Core, Important, Normal, All 四个级别，如果想自己在 IoTDB 中添加更多系统监控指标埋点，可以参考[IoTDB Metrics Framework](https://github.com/apache/iotdb/tree/master/metrics)使用说明。

### 1.4.1. Core 级别监控指标

Core 级别的监控指标在系统运行中默认开启，每一个 Core 级别的监控指标的添加都需要经过谨慎的评估，目前 Core 级别的监控指标如下所述：

#### 1.4.1.1. 集群运行状态

| Metric      | Tags                                            | Description                            | Example                                          |
| ----------- | ----------------------------------------------- | -------------------------------------- | ------------------------------------------------ |
| config_node | name="total",status="Registered/Online/Unknown" | 已注册/在线/离线 confignode 的节点数量 | config_node{name="total",status="Online",} 2.0   |
| data_node   | name="total",status="Registered/Online/Unknown" | 已注册/在线/离线 datanode 的节点数量   | data_node{name="total",status="Registered",} 3.0 |

#### 1.4.1.2. IoTDB 进程运行状态
| Metric            | Tags          | Description                         | Example                                         |
| ----------------- | ------------- | ----------------------------------- | ----------------------------------------------- |
| process_cpu_load  | name="cpu"    | IoTDB 进程的 CPU 占用率，单位为%    | process_cpu_load{name="process",} 5.0           |
| process_cpu_time  | name="cpu"    | IoTDB 进程占用的 CPU 时间，单位为ns | process_cpu_time{name="process",} 3.265625E9    |
| process_max_mem   | name="memory" | IoTDB 进程最大可用内存              | process_max_mem{name="process",} 3.545759744E9  |
| process_total_mem | name="memory" | IoTDB 进程当前已申请内存            | process_total_mem{name="process",} 2.39599616E8 |
| process_free_mem  | name="memory" | IoTDB 进程当前剩余可用内存          | process_free_mem{name="process",} 1.94035584E8  |

#### 1.4.1.3. 系统运行状态
| Metric                         | Tags          | Description                              | Example                                                        |
| ------------------------------ | ------------- | ---------------------------------------- | -------------------------------------------------------------- |
| sys_cpu_load                   | name="cpu"    | 系统的 CPU 占用率，单位为%               | sys_cpu_load{name="system",} 15.0                              |
| sys_cpu_cores                  | name="cpu"    | 系统的可用处理器数                       | sys_cpu_cores{name="system",} 16.0                             |
| sys_total_physical_memory_size | name="memory" | 系统的最大物理内存                       | sys_total_physical_memory_size{name="system",} 1.5950999552E10 |
| sys_free_physical_memory_size  | name="memory" | 系统的剩余可用内存                       | sys_free_physical_memory_size{name="system",} 4.532396032E9    |
| sys_total_swap_space_size      | name="memory" | 系统的交换区最大空间                     | sys_total_swap_space_size{name="system",} 2.1051273216E10      |
| sys_free_swap_space_size       | name="memory" | 系统的交换区剩余可用空间                 | sys_free_swap_space_size{name="system",} 2.931576832E9         |
| sys_committed_vm_size          | name="memory" | 系统保证可用于正在运行的进程的虚拟内存量 | sys_committed_vm_size{name="system",} 5.04344576E8             |
| sys_disk_total_space           | name="disk"   | 系统磁盘总大小                           | sys_disk_total_space{name="system",} 5.10770798592E11          |
| sys_disk_free_space            | name="disk"   | 系统磁盘可用大小                         | sys_disk_free_space{name="system",} 3.63467845632E11           |

### 1.4.2. Important 级别监控指标

目前 Important 级别的监控指标如下所述：

#### 1.4.2.1. 集群运行状态
| Metric                    | Tags                                              | Description                    | Example                                                          |
| ------------------------- | ------------------------------------------------- | ------------------------------ | ---------------------------------------------------------------- |
| cluster_node_leader_count | name="{{ip}}:{{port}}"                            | 节点上共识组Leader的数量       | cluster_node_leader_count{name="127.0.0.1",} 2.0                 |
| cluster_node_status       | name="{{ip}}:{{port}}",type="ConfigNode/DataNode" | 节点的状态，0=Unkonwn 1=online | cluster_node_status{name="0.0.0.0:22277",type="ConfigNode",} 1.0 |

#### 1.4.2.2. 节点统计信息
| Metric   | Tags                                       | Description                        | Example                                               |
| -------- | ------------------------------------------ | ---------------------------------- | ----------------------------------------------------- |
| quantity | name="database"                            | 系统数据库数量                     | quantity{name="timeSeries",} 1.0                      |
| quantity | name="timeSeries"                          | 系统时间序列数量                   | quantity{name="timeSeries",} 1.0                      |
| quantity | name="pointsIn"                            | 系统累计写入点数                   | quantity_total{name="pointsIn",} 1.0                  |
| region   | name="total",type="SchemaRegion"           | 分区表中SchemaRegion总数量         | region{name="127.0.0.1:6671",type="DataRegion",} 10.0 |
| region   | name="total",type="DataRegion"             | 分区表中DataRegion总数量           | region{name="127.0.0.1:6671",type="DataRegion",} 10.0 |
| region   | name="{{ip}}:{{port}}",type="SchemaRegion" | 分区表中对应节点上DataRegion总数量 | region{name="127.0.0.1:6671",type="DataRegion",} 10.0 |
| region   | name="{{ip}}:{{port}}",type="DataRegion"   | 分区表中对应节点上DataRegion总数量 | region{name="127.0.0.1:6671",type="DataRegion",} 10.0 |

#### 1.4.2.3. 缓存统计信息
| Metric    | Tags                               | Description                                             | Example                                             |
| --------- | ---------------------------------- | ------------------------------------------------------- | --------------------------------------------------- |
| cache_hit | name="chunk"                       | ChunkCache的命中率，单位为%                             | cache_hit{name="chunk",} 80                         |
| cache_hit | name="schema"                      | SchemaCache的命中率，单位为%                            | cache_hit{name="chunk",} 80                         |
| cache_hit | name="timeSeriesMeta"              | TimeseriesMetadataCache的命中率，单位为%                | cache_hit{name="chunk",} 80                         |
| cache_hit | name="bloomFilter"                 | TimeseriesMetadataCache中的bloomFilter的拦截率，单位为% | cache_hit{name="chunk",} 80                         |
| cache     | name="StorageGroup", type="hit"    | StorageGroup Cache 的命中次数                           | cache_total{name="DataPartition",type="all",} 801.0 |
| cache     | name="StorageGroup", type="all"    | StorageGroup Cache 的访问次数                           | cache_total{name="DataPartition",type="all",} 801.0 |
| cache     | name="SchemaPartition", type="hit" | SchemaPartition Cache 的命中次数                        | cache_total{name="DataPartition",type="all",} 801.0 |
| cache     | name="SchemaPartition", type="all" | SchemaPartition Cache 的访问次数                        | cache_total{name="DataPartition",type="all",} 801.0 |
| cache     | name="DataPartition", type="hit"   | DataPartition Cache 的命中次数                          | cache_total{name="DataPartition",type="all",} 801.0 |
| cache     | name="DataPartition", type="all"   | DataPartition Cache 的访问次数                          | cache_total{name="DataPartition",type="all",} 801.0 |

### 1.4.3. Normal 级别监控指标

#### 1.4.3.1. 集群运行状态
| Metric | Tags                                                               | Description                                    | Example                                                   |
| ------ | ------------------------------------------------------------------ | ---------------------------------------------- | --------------------------------------------------------- |
| region | name="{{storageGroupName}}",type="SchemaRegion/DataRegion"         | 节点上对应 database 的 DataRegion/Schema个数   | region{name="root.schema.sg1",type="DataRegion",} 14.0    |
| slot   | name="{{storageGroupName}}",type="schemaSlotNumber/dataSlotNumber" | 节点上对应 database 的 schemaSlot/dataSlot个数 | slot{name="root.schema.sg1",type="schemaSlotNumber",} 2.0 |

### 1.4.4. All 级别监控指标
目前还没有All级别的监控指标，后续会持续添加。

#### 1.4.4.1. Interface

| Metric                | Tag                      | level     | 说明                | 示例                                         |
| --------------------- | ------------------------ | --------- | ------------------- | -------------------------------------------- |
| entry_seconds_count   | name="{{interface}}"     | important | 接口累计访问次数    | entry_seconds_count{name="openSession",} 1.0 |
| entry_seconds_sum     | name="{{interface}}"     | important | 接口累计耗时(s)     | entry_seconds_sum{name="openSession",} 0.024 |
| entry_seconds_max     | name="{{interface}}"     | important | 接口最大耗时(s)     | entry_seconds_max{name="openSession",} 0.024 |
| thrift_connections    | name="{{thriftService}}" | important | thrift当前连接数    | thrift_connections{name="RPC",} 1.0          |
| thrift_active_threads | name="{{thriftThread}}"  | important | thrift worker线程数 | thrift_active_threads{name="RPC",} 1.0       |

#### 1.4.4.2. Task

| Metric                      | Tag                                                                          | level     | 说明                            | 示例                                                                                               |
| --------------------------- | ---------------------------------------------------------------------------- | --------- | ------------------------------- | -------------------------------------------------------------------------------------------------- |
| queue                       | name="compaction_inner/compaction_cross/flush",<br/>status="running/waiting" | important | 当前时间任务数                  | queue{name="flush",status="waiting",} 0.0<br/>queue{name="compaction/flush",status="running",} 0.0 |
| cost_task_seconds_count     | name="inner_compaction/cross_compaction/flush"                               | important | 任务累计发生次数                | cost_task_seconds_count{name="flush",} 1.0                                                         |
| cost_task_seconds_max       | name="inner_compaction/cross_compaction/flush"                               | important | 到目前为止任务耗时(s)最大的一次 | cost_task_seconds_max{name="flush",} 0.363                                                         |
| cost_task_seconds_sum       | name="inner_compaction/cross_compaction/flush"                               | important | 任务累计耗时(s)                 | cost_task_seconds_sum{name="flush",} 0.363                                                         |
| data_written_total          | name="compaction", <br/>type="aligned/not-aligned/total"                     | important | 合并文件时写入量                | data_written_total{name="compaction",type="total",} 10240                                          |
| data_read_total             | name="compaction"                                                            | important | 合并文件时的读取量              | data_read_total{name="compaction",} 10240                                                          |
| compaction_task_count_total | name = "inner_compaction/cross_compaction", type="sequence/unsequence/cross" | important | 合并任务个数                    | compaction_task_count_total{name="inner_compaction",type="sequence",} 1                            |

#### 1.4.4.3. 内存占用

| Metric | Tag                                                          | level     | 说明                       | 示例                              |
| ------ | ------------------------------------------------------------ | --------- | -------------------------- | --------------------------------- |
| mem    | name="chunkMetaData/storageGroup/mtree/MultiLeaderConsensus" | important | 对应部分占用的内存（byte） | mem{name="chunkMetaData",} 2050.0 |

#### 1.4.4.5. 集群

##### 1.4.4.5.1. 集群状态

| Metric          | Tag           | level | 说明                    | 示例                                |
| --------------- | ------------- | ----- | ----------------------- | ----------------------------------- |
| partition_table | name="number" | core  | partition table表的个数 | partition_table{name="number",} 2.0 |



##### 1.4.4.5.2. 弱一致性
| Metric       | Tag                                                                                          | level     | 说明                                                 | 示例                                                                                                             |
| ------------ | -------------------------------------------------------------------------------------------- | --------- | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| mutli_leader | name="multiLeaderServerImpl", region="{{region}}", type="searchIndex/safeIndex"              | core      | 弱一致性对应region的写入index和同步index             | multi_leader{name="multiLeaderServerImpl",region="DataRegion[7]",type="searchIndex",} 1945.0                     |
| mutli_leader | name="logDispatcher-{{IP}}:{{Port}}", region="{{region}}", type="currentSyncIndex"           | important | 弱一致性对应region的同步线程当前的同步index          | multi_leader{name="logDispatcher-127.0.0.1:40014",region="DataRegion[7]",type="currentSyncIndex",} 1945.0        |
| mutli_leader | name="logDispatcher-{{IP}}:{{Port}}", region="{{region}}", type="cachedRequestInMemoryQueue" | important | 弱一致性对应region的同步线程缓存的队列总大小         | multi_leader{name="logDispatcher-127.0.0.1:40014",region="DataRegion[9]",type="cachedRequestInMemoryQueue",} 0.0 |
| stage        | name="multi_leader", region="{{region}}", type="getStateMachineLock"                         | important | 弱一致性对应region获取状态机锁的耗时                 | stage{name="multi_leader",region="DataRegion[6]",type="getStateMachineLock",quantile="0.5",} 0.0                 |
| stage        | name="multi_leader", region="{{region}}", type="checkingBeforeWrite"                         | important | 弱一致性对应region状态机完成写前检查的耗时           | stage{name="multi_leader",region="DataRegion[5]",type="checkingBeforeWrite",quantile="0.5",} 0.0                 |
| stage        | name="multi_leader", region="{{region}}", type="writeStateMachine"                           | important | 弱一致性对应region状态机写入请求的耗时               | stage{name="multi_leader",region="DataRegion[6]",type="writeStateMachine",quantile="0.5",} 1.0                   |
| stage        | name="multi_leader", region="{{region}}", type="offerRequestToQueue"                         | important | 弱一致性对应region状态机尝试将请求放入同步队列的耗时 | stage{name="multi_leader",region="DataRegion[6]",type="offerRequestToQueue",quantile="0.5",} 1.0                 |
| stage        | name="multi_leader", region="{{region}}", type="consensusWrite"                              | important | 弱一致性对应region状态机处理共识层请求的耗时         | stage{name="multi_leader",region="DataRegion[6]",type="consensusWrite",quantile="0.5",} 2.0625                   |
| stage        | name="multi_leader", region="{{region}}", type="constructBatch"                              | important | 弱一致性对应同步线程完成一个请求构造的耗时           | stage{name="multi_leader",region="DataRegion[7]",type="constructBatch",quantile="0.5",} 0.0                      |
| stage        | name="multi_leader", region="{{region}}", type="syncLogTimePerRequest"                       | important | 弱一致性对应同步线程完成一个请求同步的耗时           | stage{name="multi_leader",region="DataRegion[7]",type="syncLogTimePerRequest",quantile="0.5",} 0.0               |


### 1.4.5. IoTDB 预定义指标集

#### 1.4.5.1. JVM

##### 1.4.5.1.1. 线程

| Metric                     | Tag                                                           | level     | 说明                     | 示例                                               |
| -------------------------- | ------------------------------------------------------------- | --------- | ------------------------ | -------------------------------------------------- |
| jvm_threads_live_threads   | 无                                                            | important | 当前线程数               | jvm_threads_live_threads 25.0                      |
| jvm_threads_daemon_threads | 无                                                            | important | 当前daemon线程数         | jvm_threads_daemon_threads 12.0                    |
| jvm_threads_peak_threads   | 无                                                            | important | 峰值线程数               | jvm_threads_peak_threads 28.0                      |
| jvm_threads_states_threads | state="runnable/blocked/waiting/timed-waiting/new/terminated" | important | 当前处于各种状态的线程数 | jvm_threads_states_threads{state="runnable",} 10.0 |

##### 1.4.5.1.2. 垃圾回收

| Metric                              | Tag                                                    | level     | 说明                                         | 示例                                                                                    |
| ----------------------------------- | ------------------------------------------------------ | --------- | -------------------------------------------- | --------------------------------------------------------------------------------------- |
| jvm_gc_pause_seconds_count          | action="end of major GC/end of minor GC",cause="xxxx"  | important | YGC/FGC发生次数及其原因                      | jvm_gc_pause_seconds_count{action="end of major GC",cause="Metadata GC Threshold",} 1.0 |
| jvm_gc_pause_seconds_sum            | action="end of major GC/end of minor GC",cause="xxxx"  | important | YGC/FGC累计耗时及其原因                      | jvm_gc_pause_seconds_sum{action="end of major GC",cause="Metadata GC Threshold",} 0.03  |
| jvm_gc_pause_seconds_max            | action="end of major GC",cause="Metadata GC Threshold" | important | YGC/FGC最大耗时及其原因                      | jvm_gc_pause_seconds_max{action="end of major GC",cause="Metadata GC Threshold",} 0.0   |
| jvm_gc_memory_promoted_bytes_total  | 无                                                     | important | 从GC之前到GC之后老年代内存池大小正增长的累计 | jvm_gc_memory_promoted_bytes_total 8425512.0                                            |
| jvm_gc_max_data_size_bytes          | 无                                                     | important | 老年代内存的历史最大值                       | jvm_gc_max_data_size_bytes 2.863661056E9                                                |
| jvm_gc_live_data_size_bytes         | 无                                                     | important | GC后老年代内存的大小                         | jvm_gc_live_data_size_bytes 8450088.0                                                   |
| jvm_gc_memory_allocated_bytes_total | 无                                                     | important | 在一个GC之后到下一个GC之前年轻代增加的内存   | jvm_gc_memory_allocated_bytes_total 4.2979144E7                                         |

##### 1.4.5.1.3. 内存

| Metric                          | Tag                             | level     | 说明                    | 示例                                                                                                                                                          |
| ------------------------------- | ------------------------------- | --------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| jvm_buffer_memory_used_bytes    | id="direct/mapped"              | important | 已经使用的缓冲区大小    | jvm_buffer_memory_used_bytes{id="direct",} 3.46728099E8                                                                                                       |
| jvm_buffer_total_capacity_bytes | id="direct/mapped"              | important | 最大缓冲区大小          | jvm_buffer_total_capacity_bytes{id="mapped",} 0.0                                                                                                             |
| jvm_buffer_count_buffers        | id="direct/mapped"              | important | 当前缓冲区数量          | jvm_buffer_count_buffers{id="direct",} 183.0                                                                                                                  |
| jvm_memory_committed_bytes      | {area="heap/nonheap",id="xxx",} | important | 当前向JVM申请的内存大小 | jvm_memory_committed_bytes{area="heap",id="Par Survivor Space",} 2.44252672E8<br/>jvm_memory_committed_bytes{area="nonheap",id="Metaspace",} 3.9051264E7<br/> |
| jvm_memory_max_bytes            | {area="heap/nonheap",id="xxx",} | important | JVM最大内存             | jvm_memory_max_bytes{area="heap",id="Par Survivor Space",} 2.44252672E8<br/>jvm_memory_max_bytes{area="nonheap",id="Compressed Class Space",} 1.073741824E9   |
| jvm_memory_used_bytes           | {area="heap/nonheap",id="xxx",} | important | JVM已使用内存大小       | jvm_memory_used_bytes{area="heap",id="Par Eden Space",} 1.000128376E9<br/>jvm_memory_used_bytes{area="nonheap",id="Code Cache",} 2.9783808E7<br/>             |

##### 1.4.5.1.4. Classes

| Metric                       | Tag                                           | level     | 说明                   | 示例                                                                          |
| ---------------------------- | --------------------------------------------- | --------- | ---------------------- | ----------------------------------------------------------------------------- |
| jvm_classes_unloaded_classes | 无                                            | important | jvm累计卸载的class数量 | jvm_classes_unloaded_classes 680.0                                            |
| jvm_classes_loaded_classes   | 无                                            | important | jvm累计加载的class数量 | jvm_classes_loaded_classes 5975.0                                             |
| jvm_compilation_time_ms      | {compiler="HotSpot 64-Bit Tiered Compilers",} | important | jvm耗费在编译上的时间  | jvm_compilation_time_ms{compiler="HotSpot 64-Bit Tiered Compilers",} 107092.0 |

#### 1.4.5.2. 文件（File）

| Metric     | Tag                  | level     | 说明                                | 示例                        |
| ---------- | -------------------- | --------- | ----------------------------------- | --------------------------- |
| file_size  | name="wal/seq/unseq" | important | 当前时间wal/seq/unseq文件大小(byte) | file_size{name="wal",} 67.0 |
| file_count | name="wal/seq/unseq" | important | 当前时间wal/seq/unseq文件个数       | file_count{name="seq",} 1.0 |

#### 1.4.5.3. 日志(logback)

| Metric               | Tag                                    | level     | 说明                                    | 示例                                    |
| -------------------- | -------------------------------------- | --------- | --------------------------------------- | --------------------------------------- |
| logback_events_total | {level="trace/debug/info/warn/error",} | important | trace/debug/info/warn/error日志累计数量 | logback_events_total{level="warn",} 0.0 |

#### 1.4.5.4. 进程（Process）
| Metric | Tag | level | 说明 | 示例 |
| ------ | --- | ----- | ---- | ---- |

| process_used_mem      | name="memory"  | important | JVM当前使用内存                    | process_used_mem{name="process",} 4.6065456E7   |
| process_mem_ratio     | name="memory"  | important | 进程的内存占用比例                 | process_mem_ratio{name="process",} 0.0          |
| process_threads_count | name="process" | important | 当前线程数                         | process_threads_count{name="process",} 11.0     |
| process_status        | name="process" | important | 进程存活状态，1.0为存活，0.0为终止 | process_status{name="process",} 1.0             |

## 1.5. 怎样获取这些系统监控指标？

监控模块的相关配置均在`conf/iotdb-{datanode/confignode}.properties`中，所有配置项支持通过`load configuration`命令热加载。

### 1.5.1. 配置文件
以DataNode为例

```properties
# Whether enable metric module
# Datatype: boolean
dn_enable_metric=true

# The reporters of metric module to report metrics
# If there are more than one reporter, please separate them by commas ",".
# Options: [JMX, PROMETHEUS, IOTDB]
# Datatype: String
dn_metric_reporter_list=JMX,PROMETHEUS

# The level of metric module
# Options: [Core, Important, Normal, All]
# Datatype: String
dn_metric_level=CORE

# The port of prometheus reporter of metric module
# Datatype: int
dn_metric_prometheus_reporter_port=9091
```

1. 在配置文件中修改如上配置
2. 启动IoTDB
3. 打开浏览器或者用```curl``` 访问 ```http://servier_ip:9091/metrics```, 就能看到metric数据了:

```
...
# HELP file_count
# TYPE file_count gauge
file_count{name="wal",} 0.0
file_count{name="unseq",} 0.0
file_count{name="seq",} 2.0
# HELP file_size
# TYPE file_size gauge
file_size{name="wal",} 0.0
file_size{name="unseq",} 0.0
file_size{name="seq",} 560.0
# HELP queue
# TYPE queue gauge
queue{name="flush",status="waiting",} 0.0
queue{name="flush",status="running",} 0.0
# HELP quantity
# TYPE quantity gauge
quantity{name="timeSeries",} 1.0
quantity{name="storageGroup",} 1.0
quantity{name="device",} 1.0
# HELP logback_events_total Number of error level events that made it to the logs
# TYPE logback_events_total counter
logback_events_total{level="warn",} 0.0
logback_events_total{level="debug",} 2760.0
logback_events_total{level="error",} 0.0
logback_events_total{level="trace",} 0.0
logback_events_total{level="info",} 71.0
# HELP mem
# TYPE mem gauge
mem{name="storageGroup",} 0.0
mem{name="mtree",} 1328.0
...
```

### 1.5.2. 对接Prometheus和Grafana

如上面所述，IoTDB对外暴露出标准Prometheus格式的监控指标数据，可以直接和Prometheus以及Grafana集成。

IoTDB、Prometheus、Grafana三者的关系如下图所示:

![iotdb_prometheus_grafana](https://raw.githubusercontent.com/apache/iotdb-bin-resources/main/docs/UserGuide/System%20Tools/Metrics/iotdb_prometheus_grafana.png)

1. IoTDB在运行过程中持续收集监控指标数据。
2. Prometheus以固定的间隔（可配置）从IoTDB的HTTP接口拉取监控指标数据。
3. Prometheus将拉取到的监控指标数据存储到自己的TSDB中。
4. Grafana以固定的间隔（可配置）从Prometheus查询监控指标数据并绘图展示。

从交互流程可以看出，我们需要做一些额外的工作来部署和配置Prometheus和Grafana。

比如，你可以对Prometheus进行如下的配置（部分参数可以自行调整）来从IoTDB获取监控数据

```yaml
job_name: pull-metrics
honor_labels: true
honor_timestamps: true
scrape_interval: 15s
scrape_timeout: 10s
metrics_path: /metrics
scheme: http
follow_redirects: true
static_configs:
  - targets:
      - localhost:9091
```

更多细节可以参考下面的文档：

[Prometheus安装使用文档](https://prometheus.io/docs/prometheus/latest/getting_started/)

[Prometheus从HTTP接口拉取metrics数据的配置说明](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config)

[Grafana安装使用文档](https://grafana.com/docs/grafana/latest/getting-started/getting-started/)

[Grafana从Prometheus查询数据并绘图的文档](https://prometheus.io/docs/visualization/grafana/#grafana-support-for-prometheus)

### 1.5.3. Apache IoTDB Dashboard
我们提供了Apache IoTDB Dashboard，在Grafana中显示的效果图如下所示：

![Apache IoTDB Dashboard](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/System%20Tools/Metrics/dashboard.png)

#### 1.5.3.1. 获取方式
1. 您可以在grafana-metrics-example文件夹下获取到对应不同iotdb版本的Dashboard的json文件。
2. 您可以访问[Grafana Dashboard官网](https://grafana.com/grafana/dashboards/)搜索`Apache IoTDB Dashboard`并使用

在创建Grafana时，您可以选择Import刚刚下载的json文件，并为Apache IoTDB Dashboard选择对应目标数据源。

#### 1.5.3.2. Apache IoTDB StandAlone Dashboard 说明
> 除特殊说明的监控项以外，以下监控项均保证在Important级别的监控框架中可用。

1. `Overview`：系统概述
   1. `The number of entity`：实体数量，目前包含时间序列的数量
   2. `write point per minute`：每分钟系统累计写入点数
   3. `database used memory`：每个 database 使用的内存大小
2. `Interface`：接口
   1. `The QPS of Interface`：系统接口每秒钟访问次数
   2. `The time consumed of Interface`：系统接口的平均耗时
   3. `Cache hit rate`：缓存命中率
3. `Engine`：引擎
   1. `Task number(pending and active)`：系统中不同状态的任务个数
   2. `The time consumed of tasking(pending and active)`：系统中不同状态的任务的耗时
4. `System`：系统
   1. `The size of file`：IoTDB系统相关的文件大小，包括wal下的文件总大小、seq下的tsfile文件总大小、unseq下的tsfile文件总大小
   2. `The number of file`：IoTDB系统相关的文件个数，包括wal下的文件个数、seq下的tsfile文件个数、unseq下的tsfile文件个数
   3. `The number of GC(per minute)`：IoTDB每分钟的GC数量，包括Young GC和Full GC
   4. `The time consumed of GC(per minute)`：IoTDB的每分钟平均GC耗时，包括Young GC和Full GC
   5. `Heap Memory`：IoTDB的堆内存
   6. `Off-heap Memory`：IoTDB的堆外内存
   7. `The number of Java Thread`：IoTDB的不同状态线程数

#### 1.5.3.3. Apache IoTDB ConfigNode Dashboard 说明
> 除特殊说明的监控项以外，以下监控项均保证在Important级别的监控框架中可用。

1. `Overview`：系统概述
   1. `Online ConfigNode`：正常运行ConfigNode个数
   2. `Registered ConfigNode`：注册ConfigNode个数
   3. `Unknown ConfigNode`：状态未知ConfigNode个数
   4. `Online DataNode`：正常运行DataNode个数
   5. `Registered DataNode`：注册DataNode个数
   3. `Unknown DataNode`：状态未知DataNode个数
   4. `TotalRegion`：Region总数量
   5. `DataRegion`：DataRegion总数量
   6. `SchemaRegion`：SchemaRegion总数量
2. `Node Info`：节点信息
   1. `The status of cluster node`：集群节点状态
   2. `Leadership distribution`：Leader分布情况
3. `Region`：Region分布情况
   1. `Total Region in Node`：不同Node的Region总数量
   2. `Region in Node`：不同Node的Region数量，包括SchemaRegion、DataRegion
   3. `Region in Database`(Normal级别)：不同数据库的Region数量，包括SchemaRegion、DataRegion
   4. `Slot in Database`(Normal级别)：不同数据库的Slot数量，包括DataSlot数量和SchemaSlot数量
4. `System`：系统
   1. `The number of GC(per minute)`：IoTDB每分钟的GC数量，包括Young GC和Full GC
   2. `The time consumed of GC(per minute)`：IoTDB的每分钟平均GC耗时，包括Young GC和Full GC
   3. `Heap Memory`：IoTDB的堆内存
   4. `Off-heap Memory`：IoTDB的堆外内存
   5. `The number of Java Thread`：IoTDB的不同状态线程数
   6. `The time consumed of Interface`：系统接口的平均耗时
   7. `CPU Load`：当前处理器的总负载
   8. `Memory`：系统内存大小和已经使用的大小

#### 1.5.3.4. Apache IoTDB DataNode Dashboard 说明
> 除特殊说明的监控项以外，以下监控项均保证在Important级别的监控框架中可用。

1. `Overview`：系统概述
   1. `The number of entity`：实体数量，目前包含时间序列的数量
   2. `write point per minute`：每分钟系统累计写入点数
   3. `database used memory`：每个 database 使用的内存大小
   4. `Memory`：系统内存大小和已经使用的大小
2. `Interface`：接口
   1. `The QPS of Interface`：系统接口每秒钟访问次数
   2. `The time consumed of Interface`：系统接口的平均耗时
   3. `Cache hit Rate`：缓存命中率
3. `Engine`：引擎
   1. `Task number(pending and active)`：系统中不同状态的任务个数
   2. `The time consumed of tasking(pending and active)`：系统中不同状态的任务的耗时
4. `MultiLeader`：弱一致性共识协议
   1. `MultiLeader Used Memory`：弱一致性共识层使用的内存大小
   2. `MultiLeader Sync Index`：不同的Region的写入Index和同步Index
   3. `MultiLeader Overview`：不同节点的同步总差距、总缓存的请求个数
   4. `The time consumed of different stages(50%)`：不同阶段耗时的中位数
   5. `The time consumed of different stages(75%)`：不同阶段耗时的上四分位数
   6. `The time consumed of different stages(100%)`：不同阶段耗时的最大值
   7. `MultiLeader Search Index Rate`：不同region的写入Index的增长速度
   8. `MultiLeader Safe Index Rate`：不同region的同步Index的增长速度
   9. `MultiLeader LogDispatcher Request Size`：不同的LogDispatcherThread缓存的请求个数
   10. `Sync Lag`：每个region的同步index差距
   11. `Min Peer Sync Lag`：每个region的写入index和同步最快的LogDispatcherThread的同步index之间的差距
   12. `Sync speed diff of Peers`：每个region中同步最快的LogDispatcherThread与同步最慢的LogDispatcherThread之间的同步index差距
5. `CPU`：处理器
   1. `CPU Load`：当前处理器的总负载
   2. `Process CPU Load`：IoTDB进程占用处理器的负载
6. `File System`：文件系统
   1. `The size of file`：IoTDB系统相关的文件大小，包括wal下的文件总大小、seq下的tsfile文件总大小、unseq下的tsfile文件总大小
   2. `The number of file`：IoTDB系统相关的文件个数，包括wal下的文件个数、seq下的tsfile文件个数、unseq下的tsfile文件个数
   3. `Disk Space`：当前data目录所挂载的磁盘总大小和剩余大小
7. `JVM`：系统
   1. `The number of GC(per minute)`：IoTDB每分钟的GC数量，包括Young GC和Full GC
   2. `The time consumed of GC(per minute)`：IoTDB的每分钟平均GC耗时，包括Young GC和Full GC
   3. `Heap Memory`：IoTDB的堆内存
   4. `Off-heap Memory`：IoTDB的堆外内存
   5. `The number of Java Thread`：IoTDB的不同状态线程数

# 2. 系统状态监控
进入 Jconsole 监控页面后，首先看到的是 IoTDB 各类运行情况的概览。在这里，您可以看到堆内存信息、线程信息、类信息以及服务器的 CPU 使用情况。

# 3. JMX MBean 监控
通过使用 JConsole 工具并与 JMX 连接，您可以查看一些系统统计信息和参数。
本节描述如何使用 JConsole 的 "Mbean" 选项卡来监视 IoTDB 的一些系统配置、写入数据统计等等。 连接到 JMX 后，您可以通过 "MBeans" 标签找到名为 "org.apache.iotdb.service" 的 "MBean"，如下图所示。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/46039728/149951720-707f1ee8-32ee-4fde-9252-048caebd232e.png"> <br>

# 4. 性能监控

## 4.1. 介绍

性能监控模块用来监控 IOTDB 每一个操作的耗时，以便用户更好的了解数据库的整体性能。该模块会统计每一种操作的下四分位数、中位数、上四分位数和最大值。目前操作包括`EXECUTE_BATCH`、`EXECUTE_ONE_SQL_IN_BATCH`和`EXECUTE_QUERY`。

## 4.2. 配置参数

- 配置文件位置
  - datanode：conf/iotdb-datanode.properties
  - confignode：conf/iotdb-confignode.properties

<center>

**表-配置参数以及描述项**

| 参数                      | 默认值 | 描述                 |
| :------------------------ | :----- | :------------------- |
| enable\_performance\_stat | false  | 是否开启性能监控模块 |
</center>

# 5. Cache 命中率统计

为了提高查询性能，IOTDB 对 ChunkMetaData 和 TsFileMetaData 进行了缓存。用户可以通过 debug 级别的日志以及 MXBean 两种方式来查看缓存的命中率，并根据缓存命中率以及系统内存来调节缓存所使用的内存大小。使用 MXBean 查看缓存命中率的方法为：
1. 通过端口 31999 连接 jconsole，并在上方菜单项中选择‘MBean’. 
2. 展开侧边框并选择 'org.apache.iotdb.db.service'. 将会得到如下图所示结果：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/112426760-73e3da80-8d73-11eb-9a8f-9232d1f2033b.png">