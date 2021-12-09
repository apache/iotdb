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

# Metric 工具
IoTDB Server 提供了监控模块，可以在需要地方的引入变量监控，目前提供Jmx和Prometheus两种监控方式，可以引入counter等5种指标。关于接口的更多说明请参考<a href = "https://github.com/apache/iotdb/tree/master/metrics">metric部分说明文档</a>


## 使用

第一步：获得 IoTDB-server。

第二步：编辑配置文件`iotdb-metric.yml`

- 可以选择使用获取指标的方式，修改`metricReporterList`：可选参数`jmx`、`prometheus`
- 可以选择监控框架的类型，修改`monitortype`：可选参数`dropwizard`、`micrometer`，同时在IoTDB Server的配置文件中引入对应监控框架的依赖。

第三步：在 IoTDB Server 中的对应位置埋点。

- 首先通过`MetricsService.getInstance().getMetricManager()`获取到对应的监控管理器，之后调用对应方法即可获取到对应指标。
- 之后对获取到的指标进行操作即可，监控框架会自动将指标通过选定方式发布。
- 需要注意的是，所有getOrCreate方法提供的tag均应为偶数，组成对应的key-value。

第三步：启动 IoTDB-server。

第四步：查看具体的指标参数，根据选择的获取的指标的方式分别查看即可，即`jmx`或`prometheus`

- 对于jmx，请连接后查看`org.apache.iotdb.metrics`
- 对于prometheus进行如下的配置（部分参数可以自行调整，从而获取监控数据）

```
job_name: push-metrics
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