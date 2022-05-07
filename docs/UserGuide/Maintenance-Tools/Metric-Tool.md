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

## 1. What is metrics?

Along with IoTDB running, some metrics reflecting current system's status will be collected continuously, which will provide some useful information helping us resolving system problems and detecting potential system risks.

## 2. When to use metrics?

Belows are some typical application scenarios

1. System is running slowly

   When system is running slowly, we always hope to have information about system's running status as detail as possible, such as

   - JVM：Is there FGC？How long does it cost？How much does  the memory usage decreased after GC？Are there lots of threads？
   - System：Is the CPU usage too hi？Are there many disk IOs？
   - Connections：How many connections are there in the current time？
   - Interface：What is the TPS and latency of every interface？
   - ThreadPool：Are there many pending tasks？
   - Cache Hit Ratio

2. No space left on device

   When meet a "no space left on device" error, we really want to know which kind of data file had a rapid rise in the past hours.

3. Is the system running in abnormal status

   We could use the count of error logs、the alive status of nodes in cluster, etc, to determine whether the system is running abnormally.

## 3. Who will use metrics?

Any person cares about the system's status, including but not limited to RD, QA, SRE, DBA, can use the metrics to work more efficiently.

## 4. What metrics does IoTDB have?

For now, we have provided some metrics for several core modules of IoTDB, and more metrics will be added or updated along with the development of new features and optimization or refactoring of architecture.

### 4.1. Key Concept

Before step into next, we'd better stop to have a look into some key concepts about metrics.

Every metric data has two properties

- Metric Name

  The name of this metric，for example, ```logback_events_total``` indicates the total count of log events。

- Tag

  Each metric could have 0 or several sub classes (Tag), for the same example, the ```logback_events_total``` metric has a sub class named ```level```, which means ```the total count of log events at the specific level```

### 4.2. Data Format

IoTDB provides metrics data both in JMX and Prometheus format. For JMX, you can get these metrics via ```org.apache.iotdb.metrics```.  

Next, we will choose Prometheus format data as samples to describe each kind of metric.

### 4.3. IoTDB Metrics

#### 4.3.1. API

| Metric              | Tag                   | level  | Description                              | Sample                                       |
| ------------------- | --------------------- | ------ | ---------------------------------------- | -------------------------------------------- |
| entry_seconds_count | name="interface name" | important | The total request count of the interface | entry_seconds_count{name="openSession",} 1.0 |
| entry_seconds_sum   | name="interface name" | important | The total cost seconds of the interface  | entry_seconds_sum{name="openSession",} 0.024 |
| entry_seconds_max   | name="interface name" | important | The max latency of the interface         | entry_seconds_max{name="openSession",} 0.024 |
| quantity_total      | name="pointsIn"       | important | The total points inserted into IoTDB     | quantity_total{name="pointsIn",} 1.0         |

#### 4.3.2. File

| Metric     | Tag                  | level  | Description                                     | Sample                      |
| ---------- | -------------------- | ------ | ----------------------------------------------- | --------------------------- |
| file_size  | name="wal/seq/unseq" | important | The current file size of wal/seq/unseq in bytes | file_size{name="wal",} 67.0 |
| file_count | name="wal/seq/unseq" | important | The current count of wal/seq/unseq files        | file_count{name="seq",} 1.0 |

#### 4.3.3. Flush

| Metric                  | Tag                                         | level  | Description                                                       | Sample                                                                                  |
| ----------------------- | ------------------------------------------- | ------ | ----------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| queue                   | name="flush",<br />status="running/waiting" | important | The count of current flushing tasks in running and waiting status | queue{name="flush",status="waiting",} 0.0<br/>queue{name="flush",status="running",} 0.0 |
| cost_task_seconds_count | name="flush"                                | important | The total count of flushing occurs till now                       | cost_task_seconds_count{name="flush",} 1.0                                              |
| cost_task_seconds_max   | name="flush"                                | important | The seconds of the longest flushing task takes till now           | cost_task_seconds_max{name="flush",} 0.363                                              |
| cost_task_seconds_sum   | name="flush"                                | important | The total cost seconds of all flushing tasks till now             | cost_task_seconds_sum{name="flush",} 0.363                                              |

#### 4.3.4. Compaction

| Metric                  | Tag                                                                     | level       | Description                                                         | Sample                                                        |
|-------------------------|-------------------------------------------------------------------------|-------------|---------------------------------------------------------------------|---------------------------------------------------------------|
| queue                   | name="compaction_inner/compaction_cross",<br />status="running/waiting" | important   | The count of current compaction tasks in running and waiting status | queue{name="compaction_inner",status="waiting",} 0.0          |
| cost_task_seconds_count | name="compaction"                                                       | important   | The total count of compaction occurs till now                       | cost_task_seconds_count{name="compaction",} 1.0               |
| cost_task_seconds_max   | name="compaction"                                                       | important   | The seconds of the longest compaction task takes till now           | cost_task_seconds_max{name="compaction",} 0.363               |
| cost_task_seconds_sum   | name="compaction"                                                       | important   | The total cost seconds of all compaction tasks till now             | cost_task_seconds_sum{name="compaction",} 0.363               |
| data_written            | name="compaction", <br />type="aligned/not-aligned/total"               | important   | The size of data written in compaction                              | data_written{name="compaction",type="total",} 10240           |
| data_read               | name="compaction"                                                       | important   | The size of data read in compaction                                 | data_read={name="compaction",} 10240                          |
#### 4.3.5. Memory Usage

| Metric | Tag                                     | level  | Description                                                           | Sample                            |
| ------ | --------------------------------------- | ------ | --------------------------------------------------------------------- | --------------------------------- |
| mem    | name="chunkMetaData/storageGroup/mtree" | important | Current memory size of chunkMetaData/storageGroup/mtree data in bytes | mem{name="chunkMetaData",} 2050.0 |

#### 4.3.6. Cache Hit Ratio

| Metric    | Tag                                     | level  | Description                                                                   | Sample                      |
| --------- | --------------------------------------- | ------ | ----------------------------------------------------------------------------- | --------------------------- |
| cache_hit | name="chunk/timeSeriesMeta/bloomFilter" | important | Cache hit ratio of chunk/timeSeriesMeta  and prevention ratio of bloom filter | cache_hit{name="chunk",} 80 |

#### 4.3.7. Business Data

| Metric   | Tag                                   | level  | Description                                                   | Sample                           |
| -------- | ------------------------------------- | ------ | ------------------------------------------------------------- | -------------------------------- |
| quantity | name="timeSeries/storageGroup/device" | important | The current count of timeSeries/storageGroup/devices in IoTDB | quantity{name="timeSeries",} 1.0 |

#### 4.3.8. Cluster

| Metric                    | Tag                             | level  | Description                                                                                  | Sample                                                                       |
| ------------------------- | ------------------------------- | ------ | -------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| cluster_node_leader_count | name="{{ip}}"                   | important | The count of  ```dataGroupLeader``` on each node, which reflects the distribution of leaders | cluster_node_leader_count{name="127.0.0.1",} 2.0                             |
| cluster_uncommitted_log   | name="{{ip_datagroupHeader}}"   | important | The count of ```uncommitted_log``` on each node in data groups it belongs to                 | cluster_uncommitted_log{name="127.0.0.1_Data-127.0.0.1-40010-raftId-0",} 0.0 |
| cluster_node_status       | name="{{ip}}"                   | important | The current node status, 1=online  2=offline                                                 | cluster_node_status{name="127.0.0.1",} 1.0                                   |
| cluster_elect_total       | name="{{ip}}",status="fail/win" | important | The count and result (won or failed) of elections the node participated in.                  | cluster_elect_total{name="127.0.0.1",status="win",} 1.0                      |

### 4.4. IoTDB PreDefined Metrics Set
Users can modify the value of `predefinedMetrics` in the `iotdb-metric.yml` file to enable the predefined set of metrics, which `LOGBACK` does not support in `dropwizard`.

#### 4.4.1. JVM

##### 4.4.1.1. Threads

| Metric                     | Tag                                                           | Description                          | Sample                                             |
| -------------------------- | ------------------------------------------------------------- | ------------------------------------ | -------------------------------------------------- |
| jvm_threads_live_threads   | None                                                          | The current count of threads         | jvm_threads_live_threads 25.0                      |
| jvm_threads_daemon_threads | None                                                          | The current count of  daemon threads | jvm_threads_daemon_threads 12.0                    |
| jvm_threads_peak_threads   | None                                                          | The max count of threads till now    | jvm_threads_peak_threads 28.0                      |
| jvm_threads_states_threads | state="runnable/blocked/waiting/timed-waiting/new/terminated" | The count of threads in each status  | jvm_threads_states_threads{state="runnable",} 10.0 |

##### 4.4.1.2. GC

| Metric                              | Tag                                                    | Description                                                                                                                                                          | Sample                                                                                  |
| ----------------------------------- | ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| jvm_gc_pause_seconds_count          | action="end of major GC/end of minor GC",cause="xxxx"  | The total count of YGC/FGC events and its cause                                                                                                                      | jvm_gc_pause_seconds_count{action="end of major GC",cause="Metadata GC Threshold",} 1.0 |
| jvm_gc_pause_seconds_sum            | action="end of major GC/end of minor GC",cause="xxxx"  | The total cost seconds of YGC/FGC and its cause                                                                                                                      | jvm_gc_pause_seconds_sum{action="end of major GC",cause="Metadata GC Threshold",} 0.03  |
| jvm_gc_pause_seconds_max            | action="end of major GC",cause="Metadata GC Threshold" | The max  cost seconds of YGC/FGC till now and its cause                                                                                                              | jvm_gc_pause_seconds_max{action="end of major GC",cause="Metadata GC Threshold",} 0.0   |
| jvm_gc_overhead_percent             | None                                                   | An approximation of the percent of CPU time used by GC activities over the last lookback period or since monitoring began, whichever is shorter, in the range [0..1] | jvm_gc_overhead_percent 0.0                                                             |
| jvm_gc_memory_promoted_bytes_total  | None                                                   | Count of positive increases in the size of the old generation memory pool before GC to after GC                                                                      | jvm_gc_memory_promoted_bytes_total 8425512.0                                            |
| jvm_gc_max_data_size_bytes          | None                                                   | Max size of long-lived heap memory pool                                                                                                                              | jvm_gc_max_data_size_bytes 2.863661056E9                                                |
| jvm_gc_live_data_size_bytes         | 无                                                     | Size of long-lived heap memory pool after reclamation                                                                                                                | jvm_gc_live_data_size_bytes 8450088.0                                                   |
| jvm_gc_memory_allocated_bytes_total | None                                                   | Incremented for an increase in the size of the (young) heap memory pool after one GC to before the next                                                              | jvm_gc_memory_allocated_bytes_total 4.2979144E7                                         |

##### 4.4.1.3. Memory

| Metric                          | Tag                             | Description                                                                           | Sample                                                                                                                                                        |
| ------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| jvm_buffer_memory_used_bytes    | id="direct/mapped"              | An estimate of the memory that the Java virtual machine is using for this buffer pool | jvm_buffer_memory_used_bytes{id="direct",} 3.46728099E8                                                                                                       |
| jvm_buffer_total_capacity_bytes | id="direct/mapped"              | An estimate of the total capacity of the buffers in this pool                         | jvm_buffer_total_capacity_bytes{id="mapped",} 0.0                                                                                                             |
| jvm_buffer_count_buffers        | id="direct/mapped"              | An estimate of the number of buffers in the pool                                      | jvm_buffer_count_buffers{id="direct",} 183.0                                                                                                                  |
| jvm_memory_committed_bytes      | {area="heap/nonheap",id="xxx",} | The amount of memory in bytes that is committed for the Java virtual machine to use   | jvm_memory_committed_bytes{area="heap",id="Par Survivor Space",} 2.44252672E8<br/>jvm_memory_committed_bytes{area="nonheap",id="Metaspace",} 3.9051264E7<br/> |
| jvm_memory_max_bytes            | {area="heap/nonheap",id="xxx",} | The maximum amount of memory in bytes that can be used for memory management          | jvm_memory_max_bytes{area="heap",id="Par Survivor Space",} 2.44252672E8<br/>jvm_memory_max_bytes{area="nonheap",id="Compressed Class Space",} 1.073741824E9   |
| jvm_memory_used_bytes           | {area="heap/nonheap",id="xxx",} | The amount of used memory                                                             | jvm_memory_used_bytes{area="heap",id="Par Eden Space",} 1.000128376E9<br/>jvm_memory_used_bytes{area="nonheap",id="Code Cache",} 2.9783808E7<br/>             |

##### 4.4.1.4. Classes

| Metric                             | Tag                                           | Description                                                                               | Sample                                                                              |
| ---------------------------------- | --------------------------------------------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| jvm_classes_unloaded_classes_total | 无                                            | The total number of classes unloaded since the Java virtual machine has started execution | jvm_classes_unloaded_classes_total 680.0                                            |
| jvm_classes_loaded_classes         | 无                                            | The number of classes that are currently loaded in the Java virtual machine               | jvm_classes_loaded_classes 5975.0                                                   |
| jvm_compilation_time_ms_total      | {compiler="HotSpot 64-Bit Tiered Compilers",} | The approximate accumulated elapsed time spent in compilation                             | jvm_compilation_time_ms_total{compiler="HotSpot 64-Bit Tiered Compilers",} 107092.0 |

#### 4.4.2. Log Events

| Metric               | Tag                                    | Description                                                   | Sample                                  |
| -------------------- | -------------------------------------- | ------------------------------------------------------------- | --------------------------------------- |
| logback_events_total | {level="trace/debug/info/warn/error",} | The count of  trace/debug/info/warn/error log events till now | logback_events_total{level="warn",} 0.0 |

### 4.5. Add custom metrics
- If you want to add your own metrics data in IoTDB, please see the [IoTDB Metric Framework] (https://github.com/apache/iotdb/tree/master/metrics) document.
- Metric embedded point definition rules
  - `Metric`: The name of the monitoring item. For example, `entry_seconds_count` is the cumulative number of accesses to the interface, and `file_size` is the total number of files.
  - `Tags`: Key-Value pair, used to identify monitored items, optional
    - `name = xxx`: The name of the monitored item. For example, for the monitoring item`entry_seconds_count`, the meaning of name is the name of the monitored interface.
    - `status = xxx`: The status of the monitored item is subdivided. For example, the monitoring item of the monitoring task can use this parameter to separate the running task and the stopped task.
    - `user = xxx`: The monitored item is related to a specific user, such as the total number of writes by the root user.
    - Customize for the situation...
- Monitoring indicator level meaning:
  - The default startup level for online operation is `Important` level, the default startup level for offline debugging is `Normal` level, and the audit strictness is `Core > Important > Normal > All`
  - `Core`: The core indicator of the system, used by the **operation and maintenance personnel**, which is related to the performance, stability, and security** of the system, such as the status of the instance, the load of the system, etc.
  - `Important`: An important indicator of the module, which is used by **operation and maintenance and testers**, and is directly related to **the running status of each module**, such as the number of merged files, execution status, etc.
  - `Normal`: General indicators of the module, used by **developers** to facilitate **locating the module** when problems occur, such as specific key operation situations in the merger.
  - `All`: All indicators of the module, used by **module developers**, often used when the problem is reproduced, so as to solve the problem quickly.

## 5. How to get these metrics？

The metrics collection switch is disabled by default，you need to enable it from ```conf/iotdb-metric.yml```, Currently, it also supports hot loading via `load configuration` after startup.

### 5.1. Iotdb-metric.yml

```yaml
# whether enable the module
enableMetric: false

# Multiple reporter, options: [JMX, PROMETHEUS, IOTDB], IOTDB is off by default
metricReporterList:
  - JMX
  - PROMETHEUS

# Type of monitor frame, options: [MICROMETER, DROPWIZARD]
monitorType: MICROMETER

# Level of metric level, options: [CORE, IMPORTANT, NORMAL, ALL]
metricLevel: IMPORTANT

# Predefined metric, options: [JVM, LOGBACK], LOGBACK are not supported in dropwizard
predefinedMetrics:
  - JVM

# The http server's port for prometheus exporter to get metric data.
prometheusExporterPort: 9091

# The config of iotdb reporter
ioTDBReporterConfig:
  host: 127.0.0.1
  port: 6667
  username: root
  password: root
  database: _metric
  pushPeriodInSecond: 15
```

Then you can get metrics data as follows

1. Enable metrics switch in ```iotdb-metric.yml```
2. You can just stay other config params as default.
3. Start/Restart your IoTDB server/cluster
4. Open your browser or use the ```curl``` command to request ```http://servier_ip:9091/metrics```，then you will get metrics data like follows:

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

### 5.2. Integrating with Prometheus and Grafana

As above descriptions，IoTDB provides metrics data in standard Prometheus format，so we can integrate with Prometheus and Grafana directly. 

The following picture describes the relationships among IoTDB, Prometheus and Grafana

![iotdb_prometheus_grafana](https://raw.githubusercontent.com/apache/iotdb-bin-resources/main/docs/UserGuide/System%20Tools/Metrics/iotdb_prometheus_grafana.png)

1. Along with running, IoTDB will collect its metrics continuously.
2. Prometheus scrapes metrics from IoTDB at a constant interval (can be configured).
3. Prometheus saves these metrics to its inner TSDB.
4. Grafana queries metrics from Prometheus at a constant interval (can be configured) and then presents them on the graph.

So, we need to do some additional works to configure and deploy Prometheus and Grafana.

For instance, you can config your Prometheus as follows to get metrics data from IoTDB:

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

The following documents may help you have a good journey with Prometheus and Grafana.

[Prometheus getting_started](https://prometheus.io/docs/prometheus/latest/getting_started/)

[Prometheus scrape metrics](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config)

[Grafana getting_started](https://grafana.com/docs/grafana/latest/getting-started/getting-started/)

[Grafana query metrics from Prometheus](https://prometheus.io/docs/visualization/grafana/#grafana-support-for-prometheus)

### 5.3. Apache IoTDB Dashboard
We provide the Apache IoTDB Dashboard, and the rendering shown in Grafana is as follows:

![Apache IoTDB Dashboard](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/System%20Tools/Metrics/dashboard.png)

How to get Apache IoTDB Dashboard:

1. You can obtain the json files of Dashboards corresponding to different iotdb versions in the grafana-metrics-example folder.
2. You can visit [Grafana Dashboard official website](https://grafana.com/grafana/dashboards/), search for `Apache IoTDB Dashboard` and use

When creating Grafana, you can select the json file you just downloaded to `Import` and select the corresponding target data source for Apache IoTDB Dashboard.