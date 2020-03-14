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

# Application Overview

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625222-ddd88680-467e-11ea-9098-e808ed4979c5.png">

The architecture diagram of the IoT time series database Apache IoTDB is shown above. It covers the life-cycle data management functions such as collection, storage, query, analysis, and visualization of time series data. The gray part is the IoTDB component.

## Introduction to IoTDB architecture

As shown in the following figure ,  IoTDB uses a client-server architecture.

<img style="width:100%; max-width:400px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625221-ddd88680-467e-11ea-9cf3-70367e5886f4.png">

The server mainly includes a query engine that processes all user requests and distributes them to the corresponding management components, including data writing layer, data query, schema management, and rights management modules.

* [TsFile](/#/SystemDesign/progress/chap1/sec1)
* [QueryEngine](/#/SystemDesign/progress/chap2/sec1)
* [SchemaManager](/#/SystemDesign/progress/chap3/sec1)
* [StorageEngine](/#/SystemDesign/progress/chap4/sec1)
* [DataQuery](/#/SystemDesign/progress/chap5/sec1)

## System Tools

* [Data synchronization tool](/#/SystemDesign/progress/chap6/sec1)

## Connector

IoTDB is connected with big data systems.

* [Hadoop-TsFile](/#/SystemDesign/progress/chap7/sec1)
* [Hive-TsFile](/#/SystemDesign/progress/chap7/sec2)
* [Spark-TsFile](/#/SystemDesign/progress/chap7/sec3)
* [Spark-IoTDB](/#/SystemDesign/progress/chap7/sec4)
* [Grafana](/#/SystemDesign/progress/chap7/sec5)

