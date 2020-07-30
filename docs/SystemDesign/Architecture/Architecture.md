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

The server mainly includes a query engine that processes all user requests and distributes them to the corresponding management components, including the data writing layer, data query, schema management, and administration modules.

* [TsFile](../TsFile/TsFile.md)
* [QueryEngine](../QueryEngine/QueryEngine.md)
* [SchemaManager](../SchemaManager/SchemaManager.md)
* [StorageEngine](../StorageEngine/StorageEngine.md)
* [DataQuery](../DataQuery/DataQuery.md)

## System Tools

* [Data synchronization tool](../Tools/Sync.md)

## Connector

IoTDB is connected with big data systems.

* [Hadoop-TsFile](../../UserGuide/Ecosystem%20Integration/MapReduce%20TsFile.md)
* [Hive-TsFile](../Connector/Hive-TsFile.md)
* [Spark-TsFile](../Connector/Spark-TsFile.md)
* [Spark-IoTDB](../Connector/Spark-IOTDB.md)
* [Grafana](../../UserGuide/Ecosystem%20Integration/Grafana.md)
