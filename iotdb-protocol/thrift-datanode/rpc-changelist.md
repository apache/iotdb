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

# 0.12.x -> 0.13.x

Last Updated on 2022.1.17 by Xin Zhao.

## 1. Delete Old

| Latest Changes                     | Related Committers |
| ---------------------------------- | ------------------ |

## 2. Add New

| Latest Changes                                                                                                                                                                                                                                                                                                                                        | Related Committers |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|
| Add TSTracingInfo                                                                                                                                                                                                                                                                                                                                     | Minghui Liu        |
| Add structs and interfaces to append, prune, query and unset Schema Template (detail: TSAppendSchemaTemplateReq, TSPruneSchemaTemplateReq, TSQueryTemplateReq, TSQueryTemplateResp, TSUnsetSchemaTemplateReq, appendSchemaTemplate, pruneSchemaTemplate, querySchemaTemplate, unsetSchemaTemplate), and serializedTemplate in TSCreateSchemaTemplateReq | Xin Zhao           |
| Add struct TSInsertStringRecordsOfOneDeviceReq                                                                                                                                                                                                                                                                                                        | Hang Zhang         |
| Add method TSStatus insertStringRecordsOfOneDevice(1:TSInsertStringRecordsOfOneDeviceReq req)                                                                                                                                                                                                                                                         | Hang Zhang         |
| Add TSDropSchemaTemplateReq, TSStatus dropSchemaTemplate                                                                                                                                                                                                                                                                                              | Xin Zhao           |
| Add TSCreateAlignedTimeseriesReq                                                                                                                                                                                                                                                                                                                        | Haonan Hou         |

## 3. Update

| Latest Changes                                                                       | Related Committers |
|--------------------------------------------------------------------------------------|--------------------|
| Add Optional field `isAligned` for all TSInsertReqs                                    | Haonan Hou         |
| Change schemaNames from required to optional in TSCreateSchemaTemplateReq            | Xin Zhao           |
| Change TSCreateAlignedTimeseriesReq, from `i32 compressor` to `List<i32> compressors` | Minghui Liu        |

# 0.11.x(version-2) -> 0.12.x(version-1)

Last Updated on 2021.01.19 by Xiangwei Wei.


## 1. Delete Old

| Latest Changes                     | Related Committers |
| ---------------------------------- | ------------------ |


## 2. Add New

| Latest Changes                                               | Related Committers     |
| ------------------------------------------------------------ | ---------------------- |
| Add timeout in TSFetchResultsReq and TSExecuteStatementReq | Xiangwei Wei | 


## 3. Update

| Latest Changes                                               | Related Committers     |
| ------------------------------------------------------------ | ---------------------- |


# 0.10.x (version-2) -> 0.11.x (version-3)

Last Updated on 2020-10-27 by Xiangwei Wei.


## 1. Delete Old

| Latest Changes                     | Related Committers |
| ---------------------------------- | ------------------ |
| Remove TSBatchExecuteStatementResp            | Tian Jiang         |


## 2. Add New

| Latest Changes                                               | Related Committers     |
| ------------------------------------------------------------ | ---------------------- |
| set the input/output as TFramedTransport      |  Tian Jiang        |
| Add timeout(optional) in TSFetchResultsReq and TSExecuteStatementReq | Xiangwei Wei | 


## 3. Update

| Latest Changes                                               | Related Committers     |
| ------------------------------------------------------------ | ---------------------- |
| Add sub-status in TSStatus  | Tian Jiang  |
| Change the result of executeBatchStatement  as   TSStatus    | Tian Jiang  |
| Change TSDeleteDataReq, delete timestamp and add startTime and endTime   | Wei Shao   |
| Add zoneId in TSOpenSessionReq | Xiangwei Wei |


# 0.9.x (version-1) -> 0.10.x (version-2)

Last Updated on 2020-5-25 by Kaifeng Xue.


## 1. Delete Old

| Latest Changes                     | Related Committers |
| ---------------------------------- | ------------------ |
| Remove TS_SessionHandle,TSHandleIdentifier            | Tian Jiang         |
| Remove TSStatus,TSExecuteInsertRowInBatchResp            | Jialin Qiao|


## 2. Add New

| Latest Changes                                               | Related Committers                 |
| ------------------------------------------------------------ | ---------------------------------- |
| Add parameter sessionId in getTimeZone, getProperties, setStorageGroup, createTimeseries... | Tian Jiang|
| Add struct TSQueryNonAlignDataSet                            | Haonan Hou|
| Add struct TSInsertTabletsReq                            | Jialin Qiao|
| Add method insertTablets                            | Jialin Qiao|
| Add method testInsertTablets                            | Xiangdong Huang |
| add new field `inferType` in TSInsertRecordReq  | Jialin Qiao      |

## 3. Update

| Latest Changes                                               | Related Committers     |
| ------------------------------------------------------------ | ---------------------- |
| Replace TS_SessionHandles with SessionIds, TSOperationHandle with queryIds  | Tian Jiang  |
| Add optional TSQueryNonAlignDataSet in TSExecuteStatementResp, TSFetchResultsResp and required bool isAlign in TSFetchResultsReq | Haonan Hou |
| Rename TSStatusType to TSStatus   | Jialin Qiao   |
| Remove sessionId in TSExecuteBatchStatementResp   | Jialin Qiao   |
| Rename insertRows to insertReords, insert to insertRecord, insertBatch to insertTablet   | Jialin Qiao   |
| Use TsDataType and binary rather than string in TSInsertInBatchReq and TSInsertReq  | Kaifeng Xue  |



# 0.8.x -> 0.9.x (version-1)

Last Updated on 2019-10-27 by Lei Rui.


## 1. Delete Old

| Latest Changes                     | Related Committers |
| ---------------------------------- | ------------------ |
| Delete struct TSSetStorageGroupReq | Jialin Qiao        |
| Remove struct TSDataValue          | Lei Rui            |
| Remove struct TSRowRecord          | Lei Rui            |
| Remove optional string version in TSFetchMetadataResp | Genius_pig |
| Remove optional set\<string> childPaths, nodesList, storageGroups, devices in TSFetchMetadataResp | Genius_pig |
| Remove optional map\<string, string> nodeTimeseriesNum in TSFetchMetadataResp | Genius_pig |
| Remove optional list\<list\<string>> timeseriesList in TSFetchMetadataResp | Genius_pig |
| Remove optinoal optional i32 timeseriesNum in TSFetchMetadataResp | Genius_pig |
| Remove optional i32 nodeLevel in TSFetchMetadataReq | Genius_pig |


## 2. Add New

| Latest Changes                                               | Related Committers                 |
| ------------------------------------------------------------ | ---------------------------------- |
| Add struct TSBatchInsertionReq                               | qiaojialin                         |
| Add method TSExecuteBatchStatementResp insertBatch(1:TSBatchInsertionReq req) | qiaojialin                         |
| Add Struct TSStatusType                                      | Zesong Sun                         |
| Add TSCreateTimeseriesReq                                    | Zesong Sun                         |
| Add method TSStatus setStorageGroup(1:string storageGroup)   | Zesong Sun, Jialin Qiao            |
| Add method TSStatus createTimeseries(1:TSCreateTimeseriesReq req) | Zesong Sun                         |
| Add struct TSInsertReq                                       | qiaojialin                         |
| Add method TSRPCResp insertRow(1:TSInsertReq req)            | qiaojialin                         |
| Add struct TSDeleteDataReq                                   | Jack Tsai, qiaojialin              |
| Add method TSStatus deleteData(1:TSDeleteDataReq req)        | Jack Tsai, Jialin Qiao, qiaojialin |
| Add method TSStatus deleteTimeseries(1:list\<string> path)   | qiaojialin                         |
| Add method TSStatus deleteStorageGroups(1:list\<string> storageGroup) | Yi Tao                             |
| Add Struct TSExecuteInsertRowInBatchResp                     | Kaifeng Xue |
| Add method insertRowInBatch(1:TSInsertInBatchReq req);       | Kaifeng Xue |
| Add method testInsertRowInBatch(1:TSInsertInBatchReq req);   | Kaifeng Xue |
| Add method testInsertRow(1:TSInsertReq req);                 | Kaifeng Xue |
| Add method testInsertBatch(1:TSBatchInsertionReq req);       | Kaifeng Xue |
| Add struct TSCreateMultiTimeseriesReq                        | qiaojialin |
| Add method createMultiTimeseries(1:TSCreateMultiTimeseriesReq req);       | qiaojialin |


## 3. Update

| Latest Changes                                               | Related Committers     |
| ------------------------------------------------------------ | ---------------------- |
| Add required string timestampPrecision in ServerProperties   | 1160300922             |
| Add optional list\<string\> dataTypeList in TSExecuteStatementResp | suyue                  |
| Update TSStatus to use TSStatusType, instead of using ~~TS_StatusCode, errorCode and errorMessage~~ | Zesong Sun             |
| Rename item in enum TSProtocolVersion from ~~TSFILE_SERVICE_PROTOCOL_V1~~ to IOTDB_SERVICE_PROTOCOL_V1 | qiaojialin             |
| Rename method name from ~~TSExecuteStatementResp executeInsertion(1:TSInsertionReq req)~~ to TSExecuteStatementResp insert(1:TSInsertionReq req) | qiaojialin             |
| Add required i32 compressor in TSCreateTimeseriesReq         | Jialin Qiao            |
| Add optional list\<string> nodesList, optional map\<string, string> nodeTimeseriesNum in TSFetchMetadataResp | jack870131             |
| Add optional i32 nodeLevel in TSFetchMetadataReq             | jack870131, Zesong Sun |
| Change the following methods' returned type to be TSStatus: <br />TSStatus closeSession(1:TSCloseSessionReq req), <br />TSStatus cancelOperation(1:TSCancelOperationReq req), <br />TSStatus closeOperation(1:TSCloseOperationReq req), <br />TSStatus setTimeZone(1:TSSetTimeZoneReq req), <br />TSStatus setStorageGroup(1:string storageGroup), <br />TSStatus createTimeseries(1:TSCreateTimeseriesReq req), <br />TSStatus insertRow(1:TSInsertReq req), <br />TSStatus deleteData(1:TSDeleteDataReq req) | Zesong Sun, qiaojialin |
| Change from ~~required string path~~ to required list\<string> paths in TSDeleteDataReq | qiaojialin             |
| Add optional set\<string> devices in TSFetchMetadataResp     | Zesong Sun             |
| Rename some fields in TSFetchMetadataResp: ~~ColumnsList~~ to columnsList, ~~showTimeseriesList~~ to timeseriesList, ~~showStorageGroups~~ to storageGroups | Zesong Sun             |
| Change struct TSQueryDataSet to eliminate row-wise rpc writing | Lei Rui                |
| Add optional i32 timeseriesNum in TSFetchMetadataResp        | Jack Tsai              |
| Add required i64 queryId in TSHandleIdentifier               | Yuan Tian    |
| Add optional set\<string> childPaths in TSFetchMetadataResp     | Haonan Hou             |
| Add optional string version in TSFetchMetadataResp           | Genius_pig             |
| Add required i64 statementId in TSExecuteStatementReq        | Yuan Tian |
| Add required binary time, required list\<binary> valueList, required list\<binary> bitmapList and remove required binary values, required i32 rowCount in TSQueryDataSet| Yuan Tian |
| Add optional i32 fetchSize in TSExecuteStatementReq,<br />Add optional TSQueryDataSet in TSExecuteStatementResp| liutaohua |
| Add optional map\<string, string> props, optional map\<string, string> tags, optional map\<string, string> attributes and optional string aliasPath in TSCreateTimeseriesReq | Yuan Tian | 
