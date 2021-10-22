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
Dropwizard-metrics

- In this project, we provide interface and two implementations
  - Dropwizard Metric
  - Micrometer Metric

- [1. Design](#1-design)
  - [1.1. Over all Design for acquisition System](#11-over-all-design-for-acquisition-system)
  - [1.2. Class diagram](#12-class-diagram)
- [2. Test Report](#2-test-report)
  - [2.1. Test Environment](#21-test-environment)
  - [2.2. Test Metrics](#22-test-metrics)
  - [2.3. Test parameters](#23-test-parameters)
  - [2.4. Test Result](#24-test-result)
- [3. How to use?](#3-how-to-use)
- [4. Some docs](#4-some-docs)

# 1. Design

## 1.1. Over all Design for acquisition System
1. The acquisition system consists of following four parts.
   1.  Metrics：Provide tools for collecting metric in different scenarios, including Counter, Gauge, Meter, Histogram, Timer, each with tags.
   2. MetricManager
      1. Provides functions such as creating, finding, updating, and deleting metrics.
      2. Provides management of reporter, including starting and stopping reporter.
      3. Provides the ability to introduce default metrics(Known Metric).
      4. Provides its own start and stop methods.
   3. CompositeReporter：Push the collector's data to other systems, such as Prometheus, JMX, IoTDB, etc.
   4. MetricService：Provides metricManager start-up, acquisition, and shutdown functions that can be used in the future after being registered as Iservice.
2. The structure of acquisition system:

![](https://cwiki.apache.org/confluence/download/attachments/184617400/image2021-7-19_20-48-39.png?version=1&modificationDate=1626698920000&api=v2)

## 1.2. Class diagram
![](https://cwiki.apache.org/confluence/download/attachments/184617400/image2021-7-19_20-26-30.png?version=1&modificationDate=1626697881000&api=v2)

# 2. Test Report
We implemented the monitoring framework using Dropwizard and Micromometer respectively, and tested the results as follows:

## 2.1. Test Environment
1. Processor：Inter(R) Core(TM) i7-1065G7 CPU
2. RAM: 32G

## 2.2. Test Metrics
1. We use a single thread to create counter and run the test cases separately in two frameworks of Microsoometer and Dropwizard. The test metrics as follows:
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
1. firstly, you need to set up some system property, for example:

```
System.setProperty("line.separator", "\n");
System.setProperty("IOTDB_CONF", "The directory of iotdb-metric.yml");
```

2. Then, you can modify `iotdb-metric.yml` as you like, some details:

| properties         | meaning                                               | example                                            |
| ------------------ | ----------------------------------------------------- | -------------------------------------------------- |
| enableMetric       | whether enable the module                             | true                                               |
| metricReporterList | the list of reporter                                  | jmx, prometheus, iotdb                             |
| metricManagerType  | The type of metric manager                            | DropwizardMetricManager, MicrometerMetricManager   |
| metricReporterType | the type of metric reporter                           | DropwizardMetricReporter, MicrometerMetricReporter |
| pushPeriodInSecond | the period time of push(used for prometheus, unit: s) | 5                                                  |

3. After all above, you can use it in the following way
   1. use `MetricService.getMetricManager()` to get metric manager.
   2. use the method in metric manager, method details in `metrics/interface/src/main/java/org/apache/iotdb/metrics/MetricManager`
4. example code

```java
public class PrometheusRunTest {
  public MetricManager metricManager = MetricService.getMetricManager();

  public static void main(String[] args) throws InterruptedException {
    System.setProperty("line.separator", "\n");
    // e.g. iotdb/metrics/dropwizard-metrics/src/test/resources
    System.setProperty("IOTDB_CONF", "The directory of iotdb-metric.yml");
    PrometheusRunTest prometheusRunTest = new PrometheusRunTest();
    Counter counter = prometheusRunTest.metricManager.getOrCreateCounter("counter");
    while (true) {
      counter.inc();
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
```

# 4. Some docs
1. <a href = "https://cwiki.apache.org/confluence/display/IOTDB/Monitor+Module">Monitor Module</a>
2. <a href = "https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=184616789">Monitor Module(zh)</a>