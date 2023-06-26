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

## 背景

在IoTDB服务启动时，通过加载日志文件`mlog.bin`组织元数据信息，并将结果长期持有在内存中；随着元数据的不断增长，内存会持续上涨；为支持海量元数据场景下，内存在可控范围内波动，我们提供了基于rocksDB的元数据存储方式。

## 使用

首先使用下面的命令将 `schema-engine-rocksdb` 打包

```shell
mvn clean package -pl schema-engine-rocksdb -am -DskipTests
```

命令运行结束后，在其 target/schema-engine-rocksdb 中会有一个 lib 文件夹和 conf 文件夹。将 conf 文件夹下的文件拷贝到 server 的 conf 文件夹中，将 lib 文件夹下的文件也拷贝到
server 的 lib 的文件夹中。

在系统配置文件`iotdb-datanode.properties`中，将配置项`schema_engine_mode`修改为`Rocksdb_based`， 如：

```
####################
### Schema Engine Configuration
####################
# Choose the mode of schema engine. The value could be Memory,PBTree and Rocksdb_based. If the provided value doesn't match any pre-defined value, Memory mode will be used as default.
# Datatype: string
schema_engine_mode=Rocksdb_based
```

当指定rocksdb作为元数据的存储方式时，我们开放了rocksdb相关的配置参数，您可以通过修改配置文件`schema-rocksdb.properties`，根据自己的需求，进行合理的参数调整，例如查询的缓存等。如没有特殊需求，使用默认值即可。

## 功能支持说明

该模块仍在不断完善中，部分功能暂不支持，功能模块支持情况如下：

| 功能 | 支持情况 | 
| :-----| ----: |
| 时间序列增删 | 支持 |
| 路径通配符（* 及 **）查询 | 支持 |
| tag增删 | 支持 |
| 对齐时间序列 | 支持 |
| 节点名称(*)通配 | 不支持 |
| 元数据模板template | 不支持 |
| tag索引 | 不支持 |
| continuous query | 不支持 |


## 附: 所有接口支持情况

外部接口，即客户端可以感知到，相关sql不支持；

内部接口，即服务内部的其他模块调用逻辑，与外部sql无直接依赖关系；

| 接口名称 | 接口类型 | 支持情况 | 说明 |
| :-----| ----: | :----: | :----: |
| createTimeseries | 外部接口 | 支持 | |
| createAlignedTimeSeries | 外部接口 | 支持 | |
| showTimeseries | 外部接口 | 部分支持 | 不支持LATEST |
| changeAlias | 外部接口 | 支持 | |
| upsertTagsAndAttributes | 外部接口 | 支持 | |
| addAttributes | 外部接口 | 支持 | |
| addTags | 外部接口 | 支持 | |
| dropTagsOrAttributes | 外部接口 | 支持 | |
| setTagsOrAttributesValue | 外部接口 | 支持 | |
| renameTagOrAttributeKey | 外部接口 | 支持 | |
| *template | 外部接口 | 不支持 | |
| *trigger | 外部接口 | 不支持 | |
| deleteSchemaRegion | 内部接口 | 支持 | |
| autoCreateDeviceMNode | 内部接口 | 不支持 | |
| isPathExist | 内部接口 | 支持 | |
| getAllTimeseriesCount | 内部接口 | 支持 | |
| getDevicesNum | 内部接口 | 支持 | |
| getNodesCountInGivenLevel | 内部接口 | 有条件支持 | 路径不支持通配 |
| getMeasurementCountGroupByLevel | 内部接口 | 支持 | |
| getNodesListInGivenLevel | 内部接口 | 有条件支持 | 路径不支持通配 |
| getChildNodePathInNextLevel | 内部接口 | 有条件支持 | 路径不支持通配 |
| getChildNodeNameInNextLevel | 内部接口 | 有条件支持 | 路径不支持通配 |
| getBelongedDevices | 内部接口 | 支持 | |
| getMatchedDevices | 内部接口 | 支持 | |
| getMeasurementPaths | 内部接口 | 支持 | |
| getMeasurementPathsWithAlias | 内部接口 | 支持 | |
| getAllMeasurementByDevicePath | 内部接口 | 支持 | |
| getDeviceNode | 内部接口 | 支持 | |
| getMeasurementMNodes | 内部接口 | 支持 | |
| getSeriesSchemasAndReadLockDevice | 内部接口 | 支持 | |