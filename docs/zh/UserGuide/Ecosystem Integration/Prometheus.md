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

## 环境
* 支持的IoTDB版本 : V0.12+
* 支持的Prometheus版本 : V2.23+
* 支持的Go版本 : V1.13+

## 启动IoTDB

参见https://iotdb.apache.org/zh/UserGuide/Master/Get%20Started/QuickStart.html

## Join Prometheus

除了支持session,jdbc,cli写入数据外, IoTDB还支持通过API服务程序来接入Prometheus的数据. 仅需在Prometheus添加相关配置.
即可将数据直接写入IoTDB, 并按规则自动创建时序, 无需任何代码，以及提前在IoTDB做任何配置.

## 使用prometheus-connector (从源码编译)

```
git clone https://github.com/apache/iotdb.git
cd iotdb/prometheus-connector
go mod init iotdb_prometheus
go build
run write_adaptor.go
```

## Prometheus

Prometheus作为Cloud Native Computing Fundation毕业的项目，在性能监控以及K8S性能监控领域有着非常广泛的应用。IoTDB可以通过prometheus-connector服务程序实现无代码的快速接入，高效的将数据写入IoTDB中。并通过Grafana来查询IoTDB中的数据

### 安装 prometheus

通过Prometheus的官网下载安装 https://prometheus.io/download/

### 配置 prometheus

参考prometheus[配置文件](https://prometheus.io/docs/prometheus/latest/configuration/configuration/), add the following configuration in prometheus.yml
(ip,端口号和存储组数量可以在conf.yaml配置)

```
remote_write: 
      - url: "http://localhost:12345/receive"
```

### 启动 prometheus

## 查看数据

```
select * from root.system_p_sg1
```




