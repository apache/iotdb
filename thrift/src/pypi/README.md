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

# Apache IoTDB

[![Build Status](https://www.travis-ci.org/apache/incubator-iotdb.svg?branch=master)](https://www.travis-ci.org/apache/incubator-iotdb)
[![codecov](https://codecov.io/gh/thulab/incubator-iotdb/branch/master/graph/badge.svg)](https://codecov.io/gh/thulab/incubator-iotdb)
[![GitHub release](https://img.shields.io/github/release/apache/incubator-iotdb.svg)](https://github.com/apache/incubator-iotdb/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/incubator-iotdb.svg)
![](https://img.shields.io/github/downloads/apache/incubator-iotdb/total.svg)
![](https://img.shields.io/badge/platform-win10%20%7C%20macox%20%7C%20linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)


Apache IoTDB (incubating) (Database for Internet of Things) is an integrated data management engine designed for
timeseries data. It provides users with services for data collection, storage and analysis. Due to its light-weight
architecture, high performance and rich feature set together with its deep integration with Apache Hadoop and Spark,
Apache IoTDB (incubating) can meet the requirements of massive data storage, high-speed data ingestion and complex data
analysis in the IoT industrial fields.


# Apache IoTDB Python Client API

Using the package, you can write data to IoTDB, read data from IoTDB and maintain the schema of IoTDB.

## Requirements

You have to install thrift (>=0.13) before using the package.

## How to use (Example)

First, download the package: `pip3 install apache-iotdb`

You can get an example of using the package to read and write data at here: [Example](https://github.com/apache/incubator-iotdb/blob/rel/0.10/client-py/src/SessionExample.py)

(you need to add `import iotdb` in the head of the file)

# DISCLAIMER

Apache IoTDB is an effort undergoing incubation at The Apache Software Foundation (ASF).
Incubation is required of all newly accepted projects until a further review indicates that the
infrastructure, communications, and decision making process have stabilized in a manner consistent
with other successful ASF projects. While incubation status is not necessarily a reflection of the
completeness or stability of the code, it does indicate that the project has yet to be fully
endorsed by the ASF.