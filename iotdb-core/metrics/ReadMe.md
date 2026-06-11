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
Metric Module

- In this project, we provide the metrics interface and its default implementation
  - metrics-interface
  - metrics-core
- The built-in implementation supports the following reporters
  - JMX Reporter
  - Prometheus Reporter
  - IoTDB Reporter

- [1. Design](#1-design)
- [2. How to use?](#2-how-to-use)
  - [2.1. Configuration](#21-configuration)
  - [2.2. Use Guide in IoTDB Server Module](#22-use-guide-in-iotdb-server-module)
- [3. How to implement your own metric framework?](#3-how-to-implement-your-own-metric-framework)
- [4. Some docs](#4-some-docs)

# 1. Design
> The acquisition system consists of following four parts.

1. Metrics: Provide tools for collecting metrics in different scenarios, including Counter, AutoGauge, Gauge, Histogram, Timer and Rate, each with tags.
2. MetricManager
   1. Provide functions such as create, query, update and remove metrics.
   2. Provide its own start and stop methods.
3. CompositeReporter
   1. Provide management of reporter.
   2. Provide metric value to other systems, such as Jmx, Prometheus, IoTDB, etc.
   3. Provide its own start and stop methods.
4. MetricService
   1. Provide the start and stop method of metric service.
   2. Provide the ability to reload properties when running.
   3. Provide the ability to load metric sets.
   4. Provide the access of metricManager and CompositeReporter.

# 2. How to use?

## 2.1. Configuration
Configure the metrics module through `iotdb-system.properties`. The main options supported by the current code are listed below.

| properties | meaning | example |
| --- | --- | --- |
| `dn(cn)_metric_reporter_list` | Reporter list. The current implementation supports `JMX`, `PROMETHEUS` and `IOTDB`. | `JMX,PROMETHEUS` |
| `dn(cn)_metric_level` | Initial metric level. | `OFF`, `CORE`, `IMPORTANT`, `NORMAL`, `ALL` |
| `cn_metric_prometheus_reporter_port` | Prometheus HTTP port for ConfigNode. | `9091` |
| `dn_metric_prometheus_reporter_port` | Prometheus HTTP port for DataNode. | `9092` |

More details, see the User Guide and the `iotdb-system.properties.template` file.

## 2.2. Use Guide in IoTDB Server Module
1. `MetricService` is registered as an `IService` in both DataNode and ConfigNode modules. Enable it with `dn(cn)_enable_metric=true`.
2. In server-side code you can use metrics through `MetricService.getInstance()`, for example:

```java
MetricService.getInstance().count(1, "operation_count", MetricLevel.IMPORTANT, "name", operation.getName());
```

3. If you want to bind or remove a metric set, use `addMetricSet(IMetricSet)` and `removeMetricSet(IMetricSet)`.

# 3. How to implement your own metric framework?
1. Implement your metric types and an `AbstractMetricManager`.
   1. The built-in implementation provides Counter, AutoGauge, Gauge, Histogram, Rate and Timer.
2. Implement your reporters.
   1. Implement `Reporter` for a general reporter.
   2. Implement `JmxReporter` if you need JMX registration. The built-in JMX domain is `org.apache.iotdb.metrics`.
3. Wire your implementation into the server-side metric service.
   1. The current `MetricService` directly loads `IoTDBMetricManager`, `IoTDBJmxReporter`, `PrometheusReporter` and `IoTDBSessionReporter` in `loadManager()` and `loadReporter()`.
   2. If you replace the default framework, update the corresponding loading logic there.
4. Keep service descriptor files consistent if your packaging depends on them.
   1. `src/main/resources/META-INF/services/org.apache.iotdb.metrics.AbstractMetricManager`
   2. `src/main/resources/META-INF/services/org.apache.iotdb.metrics.reporter.JmxReporter`

# 4. Some docs
1. <a href = "https://iotdb.apache.org/UserGuide/latest/Tools-System/Monitor-Tool.html">Metric Tool</a>
2. <a href = "https://iotdb.apache.org/zh/UserGuide/latest/Tools-System/Monitor-Tool.html">Metric Tool(zh)</a>
