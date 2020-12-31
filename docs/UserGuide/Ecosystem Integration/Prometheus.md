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

## Requirement
* Support IoTDB version : V0.12+
* Support Prometheus version : V2.23+
* Support Go version : V1.13+

## Start IoTDB

See https://iotdb.apache.org/zh/UserGuide/Master/Get%20Started/QuickStart.html

## Join Prometheus

In addition to supporting jdbc, session, and cli to write data, IoTDB also supports access to Prometheus data through the API service program. You only need to add relevant configurations in Prometheus.
You can write data directly into IoTDB, and automatically create time series according to rules, without any code, and do any configuration in IoTDB in advance.

## Use prometheus-connector (Compile from source)

```
git clone https://github.com/apache/iotdb.git
cd iotdb/prometheus-connector
go mod init iotdb_prometheus
go build
run write_adaptor.go
```

## Prometheus

As a project graduated from Cloud Native Computing Fundation, Prometheus has a very wide range of applications in performance monitoring and K8S performance monitoring. IoTDB can realize fast access without code through the prometheus-connector service program, and efficiently write data into IoTDB. And use Grafana to query the data in IoTDB

### Install prometheus

Install through the official website https://prometheus.io/download/

### Configure prometheus

Refer to Prometheus [Configuration Document](https://prometheus.io/docs/prometheus/latest/configuration/configuration/), add the following configuration in prometheus.yml
(ip,port and sg name can be configured in conf.yaml)

```
remote_write: 
      - url: "http://localhost:12345/receive"
```

### Start prometheus

## View data

```
select * from root.system_p_sg1
```




