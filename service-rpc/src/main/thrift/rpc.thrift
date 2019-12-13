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
namespace java org.apache.iotdb.service.rpc.thrift

// The return status code and message in each response.
struct TSStatusType {
  1: required i32 code
  2: required string message
}

// The return status of a remote request
struct TSStatus {
  1: required TSStatusType statusType
  2: optional list<string> infoMessages
  3: optional string sqlState  // as defined in the ISO/IEF CLIENT specification
}

struct TSExecuteStatementResp {
	1: required TSStatus status
	2: optional i64 queryId
  // Column names in select statement of SQL
	3: optional list<string> columns
	4: optional string operationType
	5: optional bool ignoreTimeStamp
  // Data type list of columns in select statement of SQL
  6: optional list<string> dataTypeList
}

enum TSProtocolVersion {
  IOTDB_SERVICE_PROTOCOL_V1,
}

struct TSOpenSessionResp {
  1: required TSStatus status

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
  1: required TSProtocolVersion client_protocol = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1
  2: optional string username
  3: optional string password
  4: optional map<string, string> configuration
}

struct TSAuthenticatedReq {
  1: required i64 sessionId
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
}

struct TSExecuteInsertRowInBatchResp{
  1: required i64 sessionId
	2: required list<TSStatus> statusList
}

struct TSExecuteBatchStatementResp{
	1: required TSStatus status
  // For each value in result, Statement.SUCCESS_NO_INFO represents success, Statement.EXECUTE_FAILED represents fail otherwise.
	2: optional list<i32> result
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
}

struct TSFetchResultsResp{
	1: required TSStatus status
	2: required bool hasResultSet
	3: optional TSQueryDataSet queryDataSet
}

struct TSFetchMetadataResp{
		1: required TSStatus status
		2: optional string metadataInJson
		3: optional list<string> columnsList
		4: optional i32 timeseriesNum
		5: optional string dataType
		6: optional list<list<string>> timeseriesList
		7: optional set<string> storageGroups
		8: optional set<string> devices
		9: optional list<string> nodesList
		10: optional map<string, string> nodeTimeseriesNum
		11: optional set<string> childPaths
}

struct TSFetchMetadataReq{
    1: required i64 sessionId
		2: required string type
		3: optional string columnPath
		4: optional i32 nodeLevel
}

struct TSGetTimeZoneResp {
    1: required TSStatus status
    2: required string timeZone
}

struct TSSetTimeZoneReq {
    1: required i64 sessionId
    2: required string timeZone
}

// for prepared statement
struct TSInsertionReq {
    1: required i64 sessionId
    2: optional string deviceId
    3: optional list<string> measurements
    4: optional list<string> values
    5: optional i64 timestamp
    6: optional i64 queryId
}

// for session
struct TSInsertReq {
    1: required i64 sessionId
    2: required string deviceId
    3: required list<string> measurements
    4: required list<string> values
    5: required i64 timestamp
}

struct TSBatchInsertionReq {
    1: required i64 sessionId
    2: required string deviceId
    3: required list<string> measurements
    4: required binary values
    5: required binary timestamps
    6: required list<i32> types
    7: required i32 size
}

struct TSInsertInBatchReq {
    1: required i64 sessionId
    2: required list<string> deviceIds
    3: required list<list<string>> measurementsList
    4: required list<list<string>> valuesList
    5: required list<i64> timestamps
}

struct TSDeleteDataReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 timestamp
}

struct TSCreateTimeseriesReq {
  1: required i64 sessionId
  2: required string path
  3: required i32 dataType
  4: required i32 encoding
  5: required i32 compressor
}

struct ServerProperties {
	1: required string version;
	2: required list<string> supportedTimeAggregationOperations;
	3: required string timestampPrecision;
}

struct TSQueryDataSet{
   // ByteBuffer for time column
   1: required binary time
   // ByteBuffer for each column values
   2: required list<binary> valueList
   // Bitmap for each column to indicate whether it is a null value
   3: required list<binary> bitmapList
}

struct TSColumnSchema{
	1: optional string name;
	2: optional string dataType;
	3: optional string encoding;
	4: optional map<string, string> otherArgs;
}

service TSIService {
	TSOpenSessionResp openSession(1:TSOpenSessionReq req);

	TSStatus closeSession(1:TSCloseSessionReq req);

	TSExecuteStatementResp executeStatement(1:TSExecuteStatementReq req);

	TSExecuteBatchStatementResp executeBatchStatement(1:TSExecuteBatchStatementReq req);

	TSExecuteStatementResp executeQueryStatement(1:TSExecuteStatementReq req);

	TSExecuteStatementResp executeUpdateStatement(1:TSExecuteStatementReq req);

	TSFetchResultsResp fetchResults(1:TSFetchResultsReq req)

	TSFetchMetadataResp fetchMetadata(1:TSFetchMetadataReq req)

	TSStatus cancelOperation(1:TSCancelOperationReq req);

	TSStatus closeOperation(1:TSCloseOperationReq req);

	TSGetTimeZoneResp getTimeZone(1:i64 sessionId);

	TSStatus setTimeZone(1:TSSetTimeZoneReq req);

	ServerProperties getProperties();

	TSExecuteStatementResp insert(1:TSInsertionReq req);

	TSStatus setStorageGroup(1:i64 sessionId, 2:string storageGroup);

	TSStatus createTimeseries(1:TSCreateTimeseriesReq req);

  TSStatus deleteTimeseries(1:i64 sessionId, 2:list<string> path)

  TSStatus deleteStorageGroups(1:i64 sessionId, 2:list<string> storageGroup);

  TSExecuteBatchStatementResp insertBatch(1:TSBatchInsertionReq req);

	TSStatus insertRow(1:TSInsertReq req);

	TSExecuteInsertRowInBatchResp insertRowInBatch(1:TSInsertInBatchReq req);

	TSExecuteBatchStatementResp testInsertBatch(1:TSBatchInsertionReq req);

  TSStatus testInsertRow(1:TSInsertReq req);

  TSExecuteInsertRowInBatchResp testInsertRowInBatch(1:TSInsertInBatchReq req);

	TSStatus deleteData(1:TSDeleteDataReq req);

	i64 requestStatementId(1:i64 sessionId);
}
