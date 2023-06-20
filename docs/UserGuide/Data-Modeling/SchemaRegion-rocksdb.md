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

# Background

When IoTDB service is started, metadata information is organized by loading log file `mlog.bin` and the results are held
in memory for a long time. As metadata continues to grow, memory continues to grow. In order to support the controllable
fluctuation in the massive metadata scenario, we provide a metadata storage type based on rocksDB.

# Usage

Firstly, you should package **schema-engine-rocksdb** by the following command:

```shell
mvn clean package -pl schema-engine-rocksdb -am -DskipTests
```

After that, you can get a **conf** directory and a **lib** directory in
schema-engine-rocksdb/target/schema-engine-rocksdb. Copy the file in the conf directory to the conf directory of server,
and copy the files in the lib directory to the lib directory of server.

Then, open the **iotdb-datanode.properties** in the conf directory of server, and set the `schema_engine_mode` to
Rocksdb_based. Restart the IoTDB, the system will use `RSchemaRegion` to manage the metadata.

```
####################
### Schema Engine Configuration
####################
# Choose the mode of schema engine. The value could be Memory,PBTree and Rocksdb_based. If the provided value doesn't match any pre-defined value, Memory mode will be used as default.
# Datatype: string
schema_engine_mode=Rocksdb_based

```

When rocksdb is specified as the metadata storage type, configuration parameters of rocksDB are open to the public as file. You can modify the configuration file `schema-rocksdb.properties` to adjust parameters according to your own requirements, such as block cache.  If there is no special requirement, use the default value.

# Function Support

The module is still being improved, and some functions are not supported at the moment. The function modules are supported as follows:

| function | support | 
| :-----| ----: |
| timeseries addition and deletion | yes |
| query the wildcard path(* and **) | yes |
| tag addition and deletion | yes |
| aligned timeseries | yes |
| wildcard node name(*) | no |
| meta template | no |
| tag index | no |
| continuous query | no |


# Appendix: Interface support

The external interface, that is, the client can sense, related SQL is not supported;

The internal interface, that is, the invocation logic of other modules within the service, has no direct dependence on the external SQL;

| interface | type | support | comment |
| :-----| ----: | :----: | :----: |
| createTimeseries | external | yes | |
| createAlignedTimeSeries | external | yes | |
| showTimeseries | external | part of the support | not support LATEST |
| changeAlias | external | yes | |
| upsertTagsAndAttributes | external | yes | |
| addAttributes | external | yes | |
| addTags | external | yes | |
| dropTagsOrAttributes | external | yes | |
| setTagsOrAttributesValue | external | yes | |
| renameTagOrAttributeKey | external | yes | |
| *template | external | no | |
| *trigger | external | no | |
| deleteSchemaRegion | internal | yes | |
| autoCreateDeviceMNode | internal | no | |
| isPathExist | internal | yes | |
| getAllTimeseriesCount | internal | yes | |
| getDevicesNum | internal | yes | |
| getNodesCountInGivenLevel | internal | conditional support | path does not support wildcard |
| getMeasurementCountGroupByLevel | internal | yes | |
| getNodesListInGivenLevel | internal | conditional support | path does not support wildcard |
| getChildNodePathInNextLevel | internal | conditional support | path does not support wildcard |
| getChildNodeNameInNextLevel | internal | conditional support | path does not support wildcard |
| getBelongedDevices | internal | yes | |
| getMatchedDevices | internal | yes | |
| getMeasurementPaths | internal | yes | |
| getMeasurementPathsWithAlias | internal | yes | |
| getAllMeasurementByDevicePath | internal | yes | |
| getDeviceNode | internal | yes | |
| getMeasurementMNodes | internal | yes | |
| getSeriesSchemasAndReadLockDevice | internal | yes | |