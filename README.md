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

# IoTDB
[![Build Status](https://www.travis-ci.org/apache/incubator-iotdb.svg?branch=master)](https://www.travis-ci.org/apache/incubator-iotdb)
[![codecov](https://codecov.io/gh/thulab/incubator-iotdb/branch/master/graph/badge.svg)](https://codecov.io/gh/thulab/incubator-iotdb)
[![GitHub release](https://img.shields.io/github/release/apache/incubator-iotdb.svg)](https://github.com/apache/incubator-iotdb/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/incubator-iotdb.svg)
![](https://img.shields.io/github/downloads/apache/incubator-iotdb/total.svg)
![](https://img.shields.io/badge/platform-win10%20%7C%20macox%20%7C%20linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)

# Overview

IoTDB (Internet of Things Database) is an integrated data management engine designed for time series data, which can provide users specific services for data collection, storage and analysis. Due to its light weight structure, high performance and usable features together with its intense integration with Hadoop and Spark ecology, IoTDB meets the requirements of massive dataset storage, high-speed data input and complex data analysis in the IoT industrial field.

# Main Features

IoTDB's features are as following:

1. Flexible deployment. IoTDB provides users one-click installation tool on the cloud, once-decompressed-used terminal tool and the bridge tool between cloud platform and terminal tool (Data Synchronization Tool).
2. Low cost on hardware. IoTDB can reach a high compression ratio of disk storage
3. Efficient directory structure. IoTDB supports efficient organization for complex time series data structure from intelligent networking devices, organization for time series data from devices of the same type, fuzzy searching strategy for massive and complex directory of time series data.
4. High-throughput read and write. IoTDB supports millions of low-power devices' strong connection data access, high-speed data read and write for intelligent networking devices and mixed devices mentioned above.
5. Rich query semantics. IoTDB supports time alignment for time series data across devices and sensors, computation in time series field (frequency domain transformation) and rich aggregation function support in time dimension.
6. Easy to get start. IoTDB supports SQL-Like language, JDBC standard API and import/export tools which is easy to use.
7. Intense integration with Open Source Ecosystem. IoTDB supports Hadoop, Spark, etc. analysis ecosystems and Grafana visualization tool.

For the latest information about IoTDB, please visit our [IoTDB official website](https://iotdb.apache.org/).

# Prerequisites

IoTDB requires Java (>= 1.8).
To use IoTDB, JRE should be installed. To compile IoTDB, JDK should be installed.

If you want to compile and install IoTDB from source code, JDK and Maven (>= 3.1) are required.
While Maven is not mandatory to be installed standalone, you can use the provided Maven wrapper, `./mvnw.sh` on Linux/OS X or `.\mvnw.cmd` on Windows, to facilitate development.

Set the max open files num as 65535 to avoid "too many open files" problem.

If you want to use Hadoop or Spark to analyze IoTDB data file (called as TsFile), you need to compile the hadoop and spark modules.

# Quick Start

This short guide will walk you through the basic process of using IoTDB. For a more-complete guide, please visit our website's [User Guide](https://iotdb.apache.org/#/Documents/0.8.0/chap1/sec1).

## Get source code

* https://iotdb.apache.org/#/Download
* https://github.com/apache/incubator-iotdb/tree/master

## Build from source

Under the root path of incubator-iotdb:

```
> mvn clean package -DskipTests
```

Then the binary version (including both server and client) can be found at **distribution/target/apache-iotdb-{project.version}-incubating-bin.zip**

> NOTE: Directories "service-rpc/target/generated-sources/thrift" and "server/target/generated-sources/antlr3" need to be added to sources roots to avoid compilation errors in IDE.

### Configurations

configuration files are under "conf" folder

  * environment config module (`iotdb-env.bat`, `iotdb-env.sh`), 
  * system config module (`tsfile-format.properties`, `iotdb-engine.properties`)
  * log config module (`logback.xml`). 

For more, see [Chapter4: Deployment and Management](https://iotdb.apache.org/#/Documents/0.8.0/chap4/sec1) in detail.

### Start server

```
# Unix/OS X
> sbin/start-server.sh

# Windows
> sbin\start-server.bat
```

### Stop Server

The server can be stopped with ctrl-C or the following script:

```
# Unix/OS X
> sbin/stop-server.sh

# Windows
> sbin\stop-server.bat
```

### Using client 

```
# Unix/OS X
> sbin/start-cli.sh -h <IP> -p <PORT> -u <USER_NAME>

# Windows
> sbin\start-cli.bat -h <IP> -p <PORT> -u <USER_NAME>
```

> The default user is 'root'. The default password for 'root' is 'root'.

> The default parameters are "-h 127.0.0.1 -p 6667 -u root -pw root".

``` 
IoTDB> set storage group to root.vehicle

IoTDB> create timeseries root.vehicle.d0.s0 with datatype=INT32, encoding=RLE

IoTDB> show timeseries root
+-----------------------------+---------------------+--------+--------+
|                   Timeseries|        Storage Group|DataType|Encoding|
+-----------------------------+---------------------+--------+--------+
|           root.vehicle.d0.s0|         root.vehicle|   INT32|     RLE|
+-----------------------------+---------------------+--------+--------+

IoTDB> insert into root.vehicle.d0(timestamp,s0) values(1,101);

IoTDB> SELECT d0.s0 FROM root.vehicle
+-----------------------------+------------------+
|                         Time|root.vehicle.d0.s0|
+-----------------------------+------------------+
|1970-01-01T08:00:00.001+08:00|               101|
+-----------------------------+------------------+
Total line number = 1

IoTDB> quit
or
IoTDB> exit
```

For more on what commands are supported by IoTDB SQL, see [Chapter 5: IoTDB SQL Documentation](https://iotdb.apache.org/#/Documents/0.8.0/chap5/sec1).

## Only build server

Under the root path of incubator-iotdb:

```
> mvn clean package -pl server -am -DskipTests=true
```

After build, the IoTDB server will be at the folder "server/target/iotdb-server-{project.version}". 
 

## Only build client 

Under the root path of incubator-iotdb:

```
> mvn clean package -pl client -am -DskipTests=true
```

After build, the IoTDB client will be at the folder "client/target/iotdb-client-{project.version}".