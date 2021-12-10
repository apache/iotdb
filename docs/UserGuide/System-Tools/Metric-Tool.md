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

# Metric Tools
IoTDB Server provides a monitoring module, which can introduce variable monitoring where needed. Currently, it provides two monitoring methods, Jmx and Prometheus, and can introduce 5 indicators such as counter. For more instructions on the interface, please refer to <a href = "https://github.com/apache/iotdb/tree/master/metrics">the metric part of the description document</a>


## use

Step 1: Obtain IoTDB-server.

Step 2: Edit the configuration file `iotdb-metric.yml`

- You can choose to use the method of obtaining metrics and modify the `metricReporterList`: optional parameters `jmx`, `prometheus`
- You can select the type of monitoring framework and modify the `monitortype`: optional parameters `dropwizard`, `micrometer`, and at the same time introduce the dependency of the corresponding monitoring framework in the IoTDB Server configuration file.

The third step: bury the points in the corresponding position in the IoTDB Server.

- First obtain the corresponding monitoring manager through `MetricsService.getInstance().getMetricManager()`, and then call the corresponding method to obtain the corresponding indicator.
- Afterwards, you can operate on the obtained indicators, and the monitoring framework will automatically publish the indicators through the selected method.
- It should be noted that all tags provided by the getOrCreate method should be even numbers to form the corresponding key-value.

Step 3: Start IoTDB-server.

Step 4: View the specific indicator parameters, and you can view them separately according to the selected method of obtaining the indicators, that is, `jmx` or `prometheus`

- For jmx, please check `org.apache.iotdb.metrics` after connecting
- Perform the following configuration for prometheus (some parameters can be adjusted by yourself to obtain monitoring data), It should be noted that if you use docker to run iotdb-server, you need to use the `-p 9091:9091` parameter to expose the corresponding port when using `docker run`.

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
-targets:
  -localhost:9091
```