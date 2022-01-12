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

## 什么是Metrics?

在IoTDB运行过程中，我们希望对IoTDB的状态进行观测，以便于排查系统问题或者及时发现系统潜在的风险。能**反映系统运行状态的一系列指标**就是metrics。

## 什么场景下会使用到metrics?

那么什么时候会用到metrics呢？下面列举一些常见的场景。

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

## 什么人需要使用metrics?

所有关注系统状态的人员都可以使用，包括但不限于研发、测试、运维、DBA等等

## IoTDB都有哪些metrics?

目前，IoTDB对外提供一些主要模块的metrics，并且随着新功能的开发以及系统优化或者重构，metrics也会同步添加和更新。

### 名词解释

在进一步了解这些指标之前，我们先来看几个名词解释：

- Metric Name

  指标名称唯，比如logback_events_total表示日志事件发生的总次数。

- Tag

  每个指标下面可以有0到多个分类，比如logback_events_total下有一个```level```的分类，用来表示特定级别下的日志数量。
### 数据格式

IoTDB对外提供JMX和Prometheus格式的监控指标，对于JMX，可以通过```org.apache.iotdb.metrics```获取metrics指标。

接下来我们以Prometheus格式为例对各个监控项进行说明。

### IoTDB Metrics

### 接入层

| Metric              | Tag             | 说明             | 示例                                         |
| ------------------- | --------------- | ---------------- | -------------------------------------------- |
| entry_seconds_count | name="接口名"   | 接口累计访问次数 | entry_seconds_count{name="openSession",} 1.0 |
| entry_seconds_sum   | name="接口名"   | 接口累计耗时(s)  | entry_seconds_sum{name="openSession",} 0.024 |
| entry_seconds_max   | name="接口名"   | 接口最大耗时(s)  | entry_seconds_max{name="openSession",} 0.024 |
| quantity_total      | name="pointsIn" | 系统累计写入点数 | quantity_total{name="pointsIn",} 1.0         |

### 文件

| Metric     | Tag                  | 说明                                | 示例                        |
| ---------- | -------------------- | ----------------------------------- | --------------------------- |
| file_size  | name="wal/seq/unseq" | 当前时间wal/seq/unseq文件大小(byte) | file_size{name="wal",} 67.0 |
| file_count | name="wal/seq/unseq" | 当前时间wal/seq/unseq文件个数       | file_count{name="seq",} 1.0 |

### Flush

| Metric                  | Tag                                         | 说明                             | 示例                                                         |
| ----------------------- | ------------------------------------------- | -------------------------------- | ------------------------------------------------------------ |
| queue                   | name="flush",<br />status="running/waiting" | 当前时间flush任务数              | queue{name="flush",status="waiting",} 0.0<br/>queue{name="flush",status="running",} 0.0 |
| cost_task_seconds_count | name="flush"                                | flush累计发生次数                | cost_task_seconds_count{name="flush",} 1.0                   |
| cost_task_seconds_max   | name="flush"                                | 到目前为止flush耗时(s)最大的一次 | cost_task_seconds_max{name="flush",} 0.363                   |
| cost_task_seconds_sum   | name="flush"                                | flush累计耗时(s)                 | cost_task_seconds_sum{name="flush",} 0.363                   |

### Compaction

| Metric                  | Tag                                                          | 说明                                  | 示例                                                 |
| ----------------------- | ------------------------------------------------------------ | ------------------------------------- | ---------------------------------------------------- |
| queue                   | name="compaction_inner/compaction_cross",<br />status="running/waiting" | 当前时间compaction任务数              | queue{name="compaction_inner",status="waiting",} 0.0 |
| cost_task_seconds_count | name="compaction"                                            | compaction累计发生次数                | cost_task_seconds_count{name="compaction",} 1.0      |
| cost_task_seconds_max   | name="compaction"                                            | 到目前为止compaction耗时(s)最大的一次 | cost_task_seconds_max{name="compaction",} 0.363      |
| cost_task_seconds_sum   | name="compaction"                                            | compaction累计耗时(s)                 | cost_task_seconds_sum{name="compaction",} 0.363      |

### 内存占用

| Metric | Tag                                     | 说明                                               | 示例                              |
| ------ | --------------------------------------- | -------------------------------------------------- | --------------------------------- |
| mem    | name="chunkMetaData/storageGroup/mtree" | chunkMetaData/storageGroup/mtree占用的内存（byte） | mem{name="chunkMetaData",} 2050.0 |

### 缓存命中率

| Metric    | Tag                                     | 说明                                            | 示例                        |
| --------- | --------------------------------------- | ----------------------------------------------- | --------------------------- |
| cache_hit | name="chunk/timeSeriesMeta/bloomFilter" | chunk/timeSeriesMeta缓存命中率,bloomFilter拦截率 | cache_hit{name="chunk",} 80 |

### 业务数据

| Metric   | Tag                                   | 说明                                         | 示例                             |
| -------- | ------------------------------------- | -------------------------------------------- | -------------------------------- |
| quantity | name="timeSeries/storageGroup/device" | 当前时间timeSeries/storageGroup/device的数量 | quantity{name="timeSeries",} 1.0 |

### 集群

| Metric                    | Tag                             | 说明                                                         | 示例                                                         |
| ------------------------- | ------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| cluster_node_leader_count | name="{{ip}}"                   | 节点上```dataGroupLeader```的数量，用来观察leader是否分布均匀 | cluster_node_leader_count{name="127.0.0.1",} 2.0             |
| cluster_uncommitted_log   | name="{{ip_datagroupHeader}}"   | 节点```uncommitted_log```的数量                                    | cluster_uncommitted_log{name="127.0.0.1_Data-127.0.0.1-40010-raftId-0",} 0.0 |
| cluster_node_status       | name="{{ip}}"                   | 节点状态，1=online  2=offline                                | cluster_node_status{name="127.0.0.1",} 1.0                   |
| cluster_elect_total       | name="{{ip}}",status="fail/win" | 节点参与选举的次数及结果                                     | cluster_elect_total{name="127.0.0.1",status="win",} 1.0      |

### 日志

| Metric               | Tag                                    | 说明                                    | 示例                                    |
| -------------------- | -------------------------------------- | --------------------------------------- | --------------------------------------- |
| logback_events_total | {level="trace/debug/info/warn/error",} | trace/debug/info/warn/error日志累计数量 | logback_events_total{level="warn",} 0.0 |

### JVM

#### 线程

| Metric                     | Tag                                                          | 说明                     | 示例                                               |
| -------------------------- | ------------------------------------------------------------ | ------------------------ | -------------------------------------------------- |
| jvm_threads_live_threads   | 无                                                           | 当前线程数               | jvm_threads_live_threads 25.0                      |
| jvm_threads_daemon_threads | 无                                                           | 当前daemon线程数         | jvm_threads_daemon_threads 12.0                    |
| jvm_threads_peak_threads   | 无                                                           | 峰值线程数               | jvm_threads_peak_threads 28.0                      |
| jvm_threads_states_threads | state="runnable/blocked/waiting/timed-waiting/new/terminated" | 当前处于各种状态的线程数 | jvm_threads_states_threads{state="runnable",} 10.0 |

#### 垃圾回收

| Metric                              | Tag                                                    | 说明                                         | 示例                                                         |
| ----------------------------------- | ------------------------------------------------------ | -------------------------------------------- | ------------------------------------------------------------ |
| jvm_gc_pause_seconds_count          | action="end of major GC/end of minor GC",cause="xxxx"  | YGC/FGC发生次数及其原因                      | jvm_gc_pause_seconds_count{action="end of major GC",cause="Metadata GC Threshold",} 1.0 |
| jvm_gc_pause_seconds_sum            | action="end of major GC/end of minor GC",cause="xxxx"  | YGC/FGC累计耗时及其原因                      | jvm_gc_pause_seconds_sum{action="end of major GC",cause="Metadata GC Threshold",} 0.03 |
| jvm_gc_pause_seconds_max            | action="end of major GC",cause="Metadata GC Threshold" | YGC/FGC最大耗时及其原因                      | jvm_gc_pause_seconds_max{action="end of major GC",cause="Metadata GC Threshold",} 0.0 |
| jvm_gc_overhead_percent             | 无                                                     | GC消耗cpu的比例                              | jvm_gc_overhead_percent 0.0                                  |
| jvm_gc_memory_promoted_bytes_total  | 无                                                     | 从GC之前到GC之后老年代内存池大小正增长的累计 | jvm_gc_memory_promoted_bytes_total 8425512.0                 |
| jvm_gc_max_data_size_bytes          | 无                                                     | 老年代内存的历史最大值                       | jvm_gc_max_data_size_bytes 2.863661056E9                     |
| jvm_gc_live_data_size_bytes         | 无                                                     | GC后老年代内存的大小                         | jvm_gc_live_data_size_bytes 8450088.0                        |
| jvm_gc_memory_allocated_bytes_total | 无                                                     | 在一个GC之后到下一个GC之前年轻代增加的内存   | jvm_gc_memory_allocated_bytes_total 4.2979144E7              |

#### 内存

| Metric                          | Tag                             | 说明                    | 示例                                                         |
| ------------------------------- | ------------------------------- | ----------------------- | ------------------------------------------------------------ |
| jvm_buffer_memory_used_bytes    | id="direct/mapped"              | 已经使用的缓冲区大小    | jvm_buffer_memory_used_bytes{id="direct",} 3.46728099E8      |
| jvm_buffer_total_capacity_bytes | id="direct/mapped"              | 最大缓冲区大小          | jvm_buffer_total_capacity_bytes{id="mapped",} 0.0            |
| jvm_buffer_count_buffers        | id="direct/mapped"              | 当前缓冲区数量          | jvm_buffer_count_buffers{id="direct",} 183.0                 |
| jvm_memory_committed_bytes      | {area="heap/nonheap",id="xxx",} | 当前向JVM申请的内存大小 | jvm_memory_committed_bytes{area="heap",id="Par Survivor Space",} 2.44252672E8<br/>jvm_memory_committed_bytes{area="nonheap",id="Metaspace",} 3.9051264E7<br/> |
| jvm_memory_max_bytes            | {area="heap/nonheap",id="xxx",} | JVM最大内存             | jvm_memory_max_bytes{area="heap",id="Par Survivor Space",} 2.44252672E8<br/>jvm_memory_max_bytes{area="nonheap",id="Compressed Class Space",} 1.073741824E9 |
| jvm_memory_used_bytes           | {area="heap/nonheap",id="xxx",} | JVM已使用内存大小       | jvm_memory_used_bytes{area="heap",id="Par Eden Space",} 1.000128376E9<br/>jvm_memory_used_bytes{area="nonheap",id="Code Cache",} 2.9783808E7<br/> |

#### Classes

| Metric                             | Tag                                           | 说明                   | 示例                                                         |
| ---------------------------------- | --------------------------------------------- | ---------------------- | ------------------------------------------------------------ |
| jvm_classes_unloaded_classes_total | 无                                            | jvm累计卸载的class数量 | jvm_classes_unloaded_classes_total 680.0                     |
| jvm_classes_loaded_classes         | 无                                            | jvm累计加载的class数量 | jvm_classes_loaded_classes 5975.0                            |
| jvm_compilation_time_ms_total      | {compiler="HotSpot 64-Bit Tiered Compilers",} | jvm耗费在编译上的时间  | jvm_compilation_time_ms_total{compiler="HotSpot 64-Bit Tiered Compilers",} 107092.0 |

如果想自己在IoTDB中添加更多Metrics埋点，可以参考[IoTDB Metrics Framework](https://github.com/apache/iotdb/tree/master/metrics)使用说明

## 怎样获取这些metrics？

metric采集默认是关闭的，需要先到conf/iotdb-metric.yml中打开

### 配置文件

```yaml
# 默认是false，改成true后启动iotdb，就可以获取到metrics数据了
enableMetric: false            

# 对外以jmx和prometheus协议提供metrics数据
metricReporterList:						
  - jmx                    
  - prometheus

# 底层采用的metric库，推荐使用micrometer
monitorType: micrometer          

# 该参数只对 monitorType=dropwizard生效
pushPeriodInSecond: 5								

########################################################
#                                                      #
# if the reporter is prometheus,                       #
# then the following must be set                       #
#                                                      #
########################################################
prometheusReporterConfig:
  prometheusExporterUrl: http://localhost

# 通过这个端口可以以http协议获取到metric数据
  prometheusExporterPort: 9091						
```

然后按照下面的操作获取metrics数据
1. 打开配置文件中的metric开关
2. 其他参数默认不动即可
3. 启动IoTDB
4. 打开浏览器或者用```curl``` 访问 ```http://servier_ip:9001/metrics```, 就能看到metric数据了:

```
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
```

### 对接Prometheus和Grafana

如上面所述，IoTDB对外透出标准Prometheus格式的metrics数据，可以直接和Prometheus以及Grafana集成。

IoTDB、Prometheus、Grafana三者的关系如下图所示:

![iotdb_prometheus_grafana](https://raw.githubusercontent.com/apache/iotdb-bin-resources/main/docs/UserGuide/System%20Tools/Metrics/iotdb_prometheus_grafana.png)

1. IoTDB在运行过程中持续收集metrics数据。
2. Prometheus以固定的间隔（可配置）从IoTDB的HTTP接口拉取metrics数据。
3. Prometheus将拉取到的metrics数据存储到自己的TSDB中。
4. Grafana以固定的间隔（可配置）从Prometheus查询metrics数据并绘图展示。

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

最后是IoTDB的metrics数据在Grafana中显示的效果图：

![metrics_demo_1](https://raw.githubusercontent.com/apache/iotdb-bin-resources/main/docs/UserGuide/System%20Tools/Metrics/metrics_demo_1.png)

![metrics_demo_2](http://raw.githubusercontent.com/apache/iotdb-bin-resources/main/docs/UserGuide/System%20Tools/Metrics/metrics_demo_2.png)