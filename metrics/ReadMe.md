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

- In this project, we provide interface and two implementations
  - metrics-interface
  - dropwizard metric
  - micrometer metric
- In each implementation, you can use many types of reporter to report the details of metric
  - Jmx Reporter
  - Prometheus Reporter
  - IoTDB Reporter

- [1. Design](#1-design)
- [2. Test Report](#2-test-report)
  - [2.1. Test Environment](#21-test-environment)
  - [2.2. Test Metrics](#22-test-metrics)
  - [2.3. Test parameters](#23-test-parameters)
  - [2.4. Test Result](#24-test-result)
- [3. How to use?](#3-how-to-use)
  - [3.1. Configuration](#31-configuration)
  - [3.2. Module Use Guide](#32-module-use-guide)
  - [3.3. Use Guide in IoTDB Server Module](#33-use-guide-in-iotdb-server-module)
- [4. How to implement your own metric framework?](#4-how-to-implement-your-own-metric-framework)
- [5. Some docs](#5-some-docs)

# 1. Design
1. The acquisition system consists of following four parts.
   1. Metrics：Provide tools for collecting metric in different scenarios, including Counter, Gauge, Meter, Histogram, Timer, each with tags.
   2. MetricManager
      1. Provide functions such as creating, finding, updating, and deleting metrics.
      2. Provide the ability to introduce default metrics(Known Metric).
      3. Provide its own start and stop methods.
   3. CompositeReporter
      1. Provide management of reporter, including starting and stopping reporter.
      2. Push the collector's data to other systems, such as Prometheus, JMX, etc.
      3. Provide its own start and stop methods.
   4. MetricService
      1. Provide the start and stop method of metric module.
      2. Provide the ability to hot load some properties.
      3. Provide the access of metricManager and the control of reporters.
2. The structure of acquisition system

![](https://cwiki.apache.org/confluence/download/attachments/184616789/image2021-11-3_10-49-3.png?version=1&modificationDate=1635907745000&api=v2)

# 2. Test Report
We implemented the monitoring framework using Dropwizard and Micrometer respectively, and tested the results as follows:

## 2.1. Test Environment
1. Processor：Inter(R) Core(TM) i7-1065G7 CPU
2. RAM: 32G

## 2.2. Test Metrics
1. We use a single thread to create counter and run the test cases separately in two frameworks of Micrometer and Dropwizard. The test metrics as follows:
   1. memory : Memory usage in MB.
   2. create : The time required to create, in ms.
   3. searchInorder : The time required for the sequential query, in ms.
   4. searchDisorder : The time required for random queries in ms.

## 2.3. Test parameters
1. metric : test metric 
2. name : The name of the test metric, unify to one length.
3. tag : The tag of the test metric, unify to one length.
4. metricNumberTotal：The number of metrics tested.
5. tagSingleNumber：Number of tags of the test metric.
6. tagTotalNumber：The number of tag pools, the default is 1000, all
7. tags are taken out of the tag pool.
8. searchNumber：The number of queries, the default is 1000000.
9. loop：The number of query loops, the default is 10.

## 2.4. Test Result
![](https://cwiki.apache.org/confluence/download/attachments/184617400/image2021-7-14_16-32-55.png?version=1&modificationDate=1626403814000&api=v2)

# 3. How to use?

## 3.1. Configuration
1. firstly, you need to set up some system property, for example:

```
System.setProperty("line.separator", "\n");
System.setProperty("IOTDB_CONF", "metrics/dropwizard-metrics/src/test/resources");
```

2. Then, you can modify `iotdb-metric.yml` as you like, some details:

| properties         | meaning                                               | example                |
| ------------------ | ----------------------------------------------------- | ---------------------- |
| enableMetric       | whether enable the module                             | true                   |
| metricReporterList | the list of reporter                                  | JMX, PROMETHEUS        |
| predefinedMetrics  | predefined set of metrics                             | JMX, LOGBACK           |
| metricLevel | the init level of metrics| ALL, NORMAL, IMPORTANT, CORE |
| monitorType        | The type of monitor manager                           | DROPWIZARD, MICROMETER |
| pushPeriodInSecond | the period time of push(used for prometheus, unit: s) | 5                      |

## 3.2. Module Use Guide
1. After all above, you can use it in the following way
   1. use `startService` method to load manager and reporters.
   2. use `MetricService.getMetricManager()` to get metric manager.
   3. use the method in metric manager, method details in `metrics/interface/src/main/java/org/apache/iotdb/metrics/MetricManager`
2. example code

```java
public class PrometheusRunTest {
  static MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
  static MetricService metricService = new DoNothingMetricService();
  static MetricManager metricManager;

  public static void main(String[] args) throws InterruptedException {
    metricConfig.setMonitorType(MonitorType.dropwizard);
    metricConfig.setPredefinedMetrics(new ArrayList<>());
    metricService.startService();
    metricManager = metricService.getMetricManager();
    Counter counter = metricManager.getOrCreateCounter("counter", MetricLevel.NORMAL);
    while (true) {
      counter.inc();
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
```

## 3.3. Use Guide in IoTDB Server Module
1. Now, MetricsService is registered as IService in server module, you can simple set properties: `enableMetric: true` to get a instance of MetricsService.
2. In server module you can easily use these metric by `MetricsService.getInstance().getMetricManager()`, for example:

```java
MetricsService.getInstance()
   .count(1, "operation_count", MetricLevel.NORMAL, "name", operation.getName());
```

# 4. How to implement your own metric framework?
1. implement your MetricService
   1. You need to implement `collectFileSystemInfo` to collect file system info as you like.
   2. You need to implement `reloadProperties` to support hot load.
2. implement your MetricManager
   1. The name of MetricManager should start with `monitorType`, MetricService will init manager according to the prefix of class name.
   2. You need to create `src/main/resources/META-INF/services/org.apache.iotdb.metrics.MetricManager`，and record your MetricManager class name in this file, such as `org.apache.iotdb.metrics.dropwizard.DropwizardMetricManager`
3. implement your reporter
   1. You need to implement jmx reporter and prometheus reporter, notice that your jmx bean name should be unified as `org.apache.iotdb.metrics`
   2. The name of your reporter should also start with `monitorType`
   3. You need to create `src/main/resources/META-INF/services/org.apache.iotdb.metrics.Reporter`，and record your MetricManager class name in this file, such as `org.apache.iotdb.metrics.dropwizard.reporter.DropwizardPrometheusReporter`
4. implement your specific metric
   1. They are counter, gauge, histogram, histogramSnapshot, rate and timer.
   2. These metrics will be managed by your MetricManager, and reported by your reporter.
5. extends preDefinedMetric module:
   1. you can add value into `metrics/interface/src/main/java/org/apache/iotdb/metrics/utils/PredefinedMetric`, such as `System` and `Thread`.
   2. then you need to fix the implementation of `enablePredefinedMetric(PredefinedMetric metric)` in your manager.

# 5. Some docs
1. <a href = "https://cwiki.apache.org/confluence/display/IOTDB/Monitor+Module">Monitor Module</a>
2. <a href = "https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=184616789">Monitor Module(zh)</a>