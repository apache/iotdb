/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

include "common.thrift"
namespace java org.apache.iotdb.service.rpc.thrift
namespace py iotdb.thrift.rpc
namespace go rpc

struct TSQueryDataSet{
  // ByteBuffer for time column
  1: required binary time
  // ByteBuffer for each column values
  2: required list<binary> valueList
  // Bitmap for each column to indicate whether it is a null value
  3: required list<binary> bitmapList
}

struct TSQueryNonAlignDataSet{
  // ByteBuffer for each time column
  1: required list<binary> timeList
  // ByteBuffer for each column values
  2: required list<binary> valueList
}

struct TSTracingInfo{
  1: required list<string> activityList
  2: required list<i64> elapsedTimeList
  3: optional i32 seriesPathNum
  4: optional i32 seqFileNum
  5: optional i32 unSeqFileNum
  6: optional i32 sequenceChunkNum
  7: optional i64 sequenceChunkPointNum
  8: optional i32 unsequenceChunkNum
  9: optional i64 unsequenceChunkPointNum
  10: optional i32 totalPageNum
  11: optional i32 overlappedPageNum
}

struct TSExecuteStatementResp {
  1: required common.TSStatus status
  2: optional i64 queryId
  // Column names in select statement of SQL
  3: optional list<string> columns
  4: optional string operationType
  5: optional bool ignoreTimeStamp
  // Data type list of columns in select statement of SQL
  6: optional list<string> dataTypeList
  7: optional TSQueryDataSet queryDataSet
  // for disable align statements, queryDataSet is null and nonAlignQueryDataSet is not null
  8: optional TSQueryNonAlignDataSet nonAlignQueryDataSet
  9: optional map<string, i32> columnNameIndexMap
  10: optional list<string> sgColumns
  11: optional list<byte> aliasColumns
  12: optional TSTracingInfo tracingInfo
  13: optional list<binary> queryResult
  14: optional bool moreData
}

enum TSProtocolVersion {
  IOTDB_SERVICE_PROTOCOL_V1,
  IOTDB_SERVICE_PROTOCOL_V2,//V2 is the first version that we can check version compatibility
  IOTDB_SERVICE_PROTOCOL_V3,//V3 is incompatible with V2
}

struct TSOpenSessionResp {
  1: required common.TSStatus status

  // The protocol version that the server is using.
  2: required TSProtocolVersion serverProtocolVersion = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1

  // Session id
  3: optional i64 sessionId

  // The configuration settings for this session.
  4: optional map<string, string> configuration
}

// OpenSession()
// Open a session (connection) on the server against which operations may be executed.
struct TSOpenSessionReq {
  1: required TSProtocolVersion client_protocol = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3
  2: required string zoneId
  3: required string username
  4: optional string password
  5: optional map<string, string> configuration
}

// CloseSession()
// Closes the specified session and frees any resources currently allocated to that session.
// Any open operations in that session will be canceled.
struct TSCloseSessionReq {
  1: required i64 sessionId
}

// ExecuteStatement()
//
// Execute a statement.
// The returned OperationHandle can be used to check on the status of the statement, and to fetch results once the
// statement has finished executing.
struct TSExecuteStatementReq {
  // The session to execute the statement against
  1: required i64 sessionId

  // The statement to be executed (DML, DDL, SET, etc)
  2: required string statement

  // statementId
  3: required i64 statementId

  4: optional i32 fetchSize

  5: optional i64 timeout

  6: optional bool enableRedirectQuery;

  7: optional bool jdbcQuery;
}

struct TSExecuteBatchStatementReq{
  // The session to execute the statement against
  1: required i64 sessionId

  // The statements to be executed (DML, DDL, SET, etc)
  2: required list<string> statements
}

struct TSGetOperationStatusReq {
  1: required i64 sessionId
  // Session to run this request against
  2: required i64 queryId
}

// CancelOperation()
//
// Cancels processing on the specified operation handle and frees any resources which were allocated.
struct TSCancelOperationReq {
  1: required i64 sessionId
  // Operation to cancel
  2: required i64 queryId
}

// CloseOperation()
struct TSCloseOperationReq {
  1: required i64 sessionId
  2: optional i64 queryId
  3: optional i64 statementId
}

struct TSFetchResultsReq{
  1: required i64 sessionId
  2: required string statement
  3: required i32 fetchSize
  4: required i64 queryId
  5: required bool isAlign
  6: optional i64 timeout
}

struct TSFetchResultsResp{
  1: required common.TSStatus status
  2: required bool hasResultSet
  3: required bool isAlign
  4: optional TSQueryDataSet queryDataSet
  5: optional TSQueryNonAlignDataSet nonAlignQueryDataSet
  6: optional list<binary> queryResult
  7: optional bool moreData
}

struct TSFetchMetadataResp{
  1: required common.TSStatus status
  2: optional string metadataInJson
  3: optional list<string> columnsList
  4: optional string dataType
}

struct TSFetchMetadataReq{
  1: required i64 sessionId
  2: required string type
  3: optional string columnPath
}

struct TSGetTimeZoneResp {
  1: required common.TSStatus status
  2: required string timeZone
}

struct TSSetTimeZoneReq {
  1: required i64 sessionId
  2: required string timeZone
}

// for session
struct TSInsertRecordReq {
  1: required i64 sessionId
  2: required string prefixPath
  3: required list<string> measurements
  4: required binary values
  5: required i64 timestamp
  6: optional bool isAligned
}

struct TSInsertStringRecordReq {
  1: required i64 sessionId
  2: required string prefixPath
  3: required list<string> measurements
  4: required list<string> values
  5: required i64 timestamp
  6: optional bool isAligned
  7: optional i64 timeout
}

struct TSInsertTabletReq {
  1: required i64 sessionId
  2: required string prefixPath
  3: required list<string> measurements
  4: required binary values
  5: required binary timestamps
  6: required list<i32> types
  7: required i32 size
  8: optional bool isAligned
}

struct TSInsertTabletsReq {
  1: required i64 sessionId
  2: required list<string> prefixPaths
  3: required list<list<string>> measurementsList
  4: required list<binary> valuesList
  5: required list<binary> timestampsList
  6: required list<list<i32>> typesList
  7: required list<i32> sizeList
  8: optional bool isAligned
}

struct TSInsertRecordsReq {
  1: required i64 sessionId
  2: required list<string> prefixPaths
  3: required list<list<string>> measurementsList
  4: required list<binary> valuesList
  5: required list<i64> timestamps
  6: optional bool isAligned
}

struct TSInsertRecordsOfOneDeviceReq {
    1: required i64 sessionId
    2: required string prefixPath
    3: required list<list<string>> measurementsList
    4: required list<binary> valuesList
    5: required list<i64> timestamps
    6: optional bool isAligned
}

struct TSInsertStringRecordsOfOneDeviceReq {
    1: required i64 sessionId
    2: required string prefixPath
    3: required list<list<string>> measurementsList
    4: required list<list<string>> valuesList
    5: required list<i64> timestamps
    6: optional bool isAligned
}

struct TSInsertStringRecordsReq {
  1: required i64 sessionId
  2: required list<string> prefixPaths
  3: required list<list<string>> measurementsList
  4: required list<list<string>> valuesList
  5: required list<i64> timestamps
  6: optional bool isAligned
}

struct TSDeleteDataReq {
  1: required i64 sessionId
  2: required list<string> paths
  3: required i64 startTime
  4: required i64 endTime
}

struct TSCreateTimeseriesReq {
  1: required i64 sessionId
  2: required string path
  3: required i32 dataType
  4: required i32 encoding
  5: required i32 compressor
  6: optional map<string, string> props
  7: optional map<string, string> tags
  8: optional map<string, string> attributes
  9: optional string measurementAlias
}

struct TSCreateAlignedTimeseriesReq {
  1: required i64 sessionId
  2: required string prefixPath
  3: required list<string> measurements
  4: required list<i32> dataTypes
  5: required list<i32> encodings
  6: required list<i32> compressors
  7: optional list<string> measurementAlias
  8: optional list<map<string, string>> tagsList
  9: optional list<map<string, string>> attributesList
}

struct TSRawDataQueryReq {
  1: required i64 sessionId
  2: required list<string> paths
  3: optional i32 fetchSize
  4: required i64 startTime
  5: required i64 endTime
  6: required i64 statementId
  7: optional bool enableRedirectQuery
  8: optional bool jdbcQuery
  9: optional i64 timeout
}

struct TSLastDataQueryReq {
  1: required i64 sessionId
  2: required list<string> paths
  3: optional i32 fetchSize
  4: required i64 time
  5: required i64 statementId
  6: optional bool enableRedirectQuery
  7: optional bool jdbcQuery
  8: optional i64 timeout
}

struct TSAggregationQueryReq {
  1: required i64 sessionId
  2: required i64 statementId
  3: required list<string> paths
  4: required list<common.TAggregationType> aggregations
  5: optional i64 startTime
  6: optional i64 endTime
  7: optional i64 interval
  8: optional i64 slidingStep
  9: optional i32 fetchSize
  10: optional i64 timeout
}

struct TSCreateMultiTimeseriesReq {
  1: required i64 sessionId
  2: required list<string> paths
  3: required list<i32> dataTypes
  4: required list<i32> encodings
  5: required list<i32> compressors
  6: optional list<map<string, string>> propsList
  7: optional list<map<string, string>> tagsList
  8: optional list<map<string, string>> attributesList
  9: optional list<string> measurementAliasList
}

struct ServerProperties {
  1: required string version;
  2: required list<string> supportedTimeAggregationOperations;
  3: required string timestampPrecision;
  4: i32 maxConcurrentClientNum;
  5: optional string watermarkSecretKey;
  6: optional string watermarkBitString
  7: optional i32 watermarkParamMarkRate;
  8: optional i32 watermarkParamMaxRightBit;
  9: optional i32 thriftMaxFrameSize;
  10:optional bool isReadOnly;
  11:optional string buildInfo;
}

struct TSSetSchemaTemplateReq {
  1: required i64 sessionId
  2: required string templateName
  3: required string prefixPath
}

struct TSCreateSchemaTemplateReq {
  1: required i64 sessionId
  2: required string name
  3: required binary serializedTemplate
}

struct TSAppendSchemaTemplateReq {
  1: required i64 sessionId
  2: required string name
  3: required bool isAligned
  4: required list<string> measurements
  5: required list<i32> dataTypes
  6: required list<i32> encodings
  7: required list<i32> compressors
}

struct TSPruneSchemaTemplateReq {
  1: required i64 sessionId
  2: required string name
  3: required string path
}

struct TSQueryTemplateReq {
  1: required i64 sessionId
  2: required string name
  3: required i32 queryType
  4: optional string measurement
}

struct TSQueryTemplateResp {
  1: required common.TSStatus status
  2: required i32 queryType
  3: optional bool result
  4: optional i32 count
  5: optional list<string> measurements
}

struct TSUnsetSchemaTemplateReq {
  1: required i64 sessionId
  2: required string prefixPath
  3: required string templateName
}

struct TSDropSchemaTemplateReq {
  1: required i64 sessionId
  2: required string templateName
}

// The sender and receiver need to check some info to confirm validity
struct TSyncIdentityInfo{
  // Sender needs to tell receiver its identity.
  1:required string pipeName
  2:required i64 createTime
  // The version of sender and receiver need to be the same.
  3:required string version
  4:required string database
}

struct TSyncTransportMetaInfo{
  // The name of the file in sending.
  1:required string fileName
  // The start index of the file slice in sending.
  2:required i64 startIndex
}

struct TSBackupConfigurationResp {
  1: required common.TSStatus status
  2: optional bool enableOperationSync
  3: optional string secondaryAddress
  4: optional i32 secondaryPort
}

enum TSConnectionType {
  THRIFT_BASED
  MQTT_BASED
  INTERNAL
}

struct TSConnectionInfo {
  1: required string userName
  2: required i64 logInTime
  3: required string connectionId // ip:port for thrift-based service and clientId for mqtt-based service
  4: required TSConnectionType type
}

struct TSConnectionInfoResp {
  1: required list<TSConnectionInfo> connectionInfoList
}

service IClientRPCService {

  TSExecuteStatementResp executeQueryStatementV2(1:TSExecuteStatementReq req);

  TSExecuteStatementResp executeUpdateStatementV2(1:TSExecuteStatementReq req);

  TSExecuteStatementResp executeStatementV2(1:TSExecuteStatementReq req);

  TSExecuteStatementResp executeRawDataQueryV2(1:TSRawDataQueryReq req);

  TSExecuteStatementResp executeLastDataQueryV2(1:TSLastDataQueryReq req);

  TSExecuteStatementResp executeAggregationQueryV2(1:TSAggregationQueryReq req);

  TSFetchResultsResp fetchResultsV2(1:TSFetchResultsReq req);

  TSOpenSessionResp openSession(1:TSOpenSessionReq req);

  common.TSStatus closeSession(1:TSCloseSessionReq req);

  TSExecuteStatementResp executeStatement(1:TSExecuteStatementReq req);

  common.TSStatus executeBatchStatement(1:TSExecuteBatchStatementReq req);

  TSExecuteStatementResp executeQueryStatement(1:TSExecuteStatementReq req);

  TSExecuteStatementResp executeUpdateStatement(1:TSExecuteStatementReq req);

  TSFetchResultsResp fetchResults(1:TSFetchResultsReq req);

  TSFetchMetadataResp fetchMetadata(1:TSFetchMetadataReq req);

  common.TSStatus cancelOperation(1:TSCancelOperationReq req);

  common.TSStatus closeOperation(1:TSCloseOperationReq req);

  TSGetTimeZoneResp getTimeZone(1:i64 sessionId);

  common.TSStatus setTimeZone(1:TSSetTimeZoneReq req);

  ServerProperties getProperties();

  common.TSStatus setStorageGroup(1:i64 sessionId, 2:string storageGroup);

  common.TSStatus createTimeseries(1:TSCreateTimeseriesReq req);

  common.TSStatus createAlignedTimeseries(1:TSCreateAlignedTimeseriesReq req);

  common.TSStatus createMultiTimeseries(1:TSCreateMultiTimeseriesReq req);

  common.TSStatus deleteTimeseries(1:i64 sessionId, 2:list<string> path)

  common.TSStatus deleteStorageGroups(1:i64 sessionId, 2:list<string> storageGroup);

  common.TSStatus insertRecord(1:TSInsertRecordReq req);

  common.TSStatus insertStringRecord(1:TSInsertStringRecordReq req);

  common.TSStatus insertTablet(1:TSInsertTabletReq req);

  common.TSStatus insertTablets(1:TSInsertTabletsReq req);

  common.TSStatus insertRecords(1:TSInsertRecordsReq req);

  common.TSStatus insertRecordsOfOneDevice(1:TSInsertRecordsOfOneDeviceReq req);

  common.TSStatus insertStringRecordsOfOneDevice(1:TSInsertStringRecordsOfOneDeviceReq req);

  common.TSStatus insertStringRecords(1:TSInsertStringRecordsReq req);

  common.TSStatus testInsertTablet(1:TSInsertTabletReq req);

  common.TSStatus testInsertTablets(1:TSInsertTabletsReq req);

  common.TSStatus testInsertRecord(1:TSInsertRecordReq req);

  common.TSStatus testInsertStringRecord(1:TSInsertStringRecordReq req);

  common.TSStatus testInsertRecords(1:TSInsertRecordsReq req);

  common.TSStatus testInsertRecordsOfOneDevice(1:TSInsertRecordsOfOneDeviceReq req);

  common.TSStatus testInsertStringRecords(1:TSInsertStringRecordsReq req);

  common.TSStatus deleteData(1:TSDeleteDataReq req);

  TSExecuteStatementResp executeRawDataQuery(1:TSRawDataQueryReq req);

  TSExecuteStatementResp executeLastDataQuery(1:TSLastDataQueryReq req);

  TSExecuteStatementResp executeAggregationQuery(1:TSAggregationQueryReq req);

  i64 requestStatementId(1:i64 sessionId);

  common.TSStatus createSchemaTemplate(1:TSCreateSchemaTemplateReq req);

  common.TSStatus appendSchemaTemplate(1:TSAppendSchemaTemplateReq req);

  common.TSStatus pruneSchemaTemplate(1:TSPruneSchemaTemplateReq req);

  TSQueryTemplateResp querySchemaTemplate(1:TSQueryTemplateReq req);

  common.TSStatus setSchemaTemplate(1:TSSetSchemaTemplateReq req);

  common.TSStatus unsetSchemaTemplate(1:TSUnsetSchemaTemplateReq req);

  common.TSStatus dropSchemaTemplate(1:TSDropSchemaTemplateReq req);

  common.TSStatus handshake(TSyncIdentityInfo info);

  common.TSStatus sendPipeData(1:binary buff);

  common.TSStatus sendFile(1:TSyncTransportMetaInfo metaInfo, 2:binary buff);

  TSBackupConfigurationResp getBackupConfiguration();

  TSConnectionInfoResp fetchAllConnectionsInfo();
}