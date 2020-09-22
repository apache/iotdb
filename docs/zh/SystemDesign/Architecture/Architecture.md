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

# 应用概览

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625222-ddd88680-467e-11ea-9098-e808ed4979c5.png">

物联网时序数据库 Apache IoTDB 的架构图如上所示，覆盖了对时序数据的采集、存储、查询、分析以及可视化等全生命周期的数据管理功能，其中灰色部分为 IoTDB 组件。

## IoTDB 架构介绍

IoTDB 采用客户端-服务器架构，如下图所示。

<img style="width:100%; max-width:400px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625221-ddd88680-467e-11ea-9cf3-70367e5886f4.png">

其中服务器端主要包括查询引擎，用来处理用户的所有请求，并分发到对应的管理组件，包括数据写入层、数据查询、元数据管理、权限管理等模块。

* [数据文件](../TsFile/TsFile.md)
* [查询引擎](../QueryEngine/QueryEngine.md)
* [元数据管理](../SchemaManager/SchemaManager.md)
* [存储引擎](../StorageEngine/StorageEngine.md)
* [数据查询](../DataQuery/DataQuery.md)

## 系统工具

* [数据同步工具](../Tools/Sync.md)

## 连接器

IoTDB 与大数据系统进行了对接。

* [Hadoop-TsFile](../../UserGuide/Ecosystem%20Integration/MapReduce%20TsFile.md)
* [Hive-TsFile](../Connector/Hive-TsFile.md)
* [Spark-TsFile](../Connector/Spark-TsFile.md)
* [Spark-IoTDB](../Connector/Spark-IOTDB.md)
* [Grafana](../../UserGuide/Ecosystem%20Integration/Grafana.md)

