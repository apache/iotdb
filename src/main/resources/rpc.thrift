namespace java cn.edu.thu.tsfiledb.service.rpc.thrift

service TSIService {
	TSOpenSessionResp openSession(1:TSOpenSessionReq req);

	TSCloseSessionResp closeSession(1:TSCloseSessionReq req);

	TSExecuteStatementResp executeStatement(1:TSExecuteStatementReq req);
	
	TSExecuteBatchStatementResp executeBatchStatement(1:TSExecuteBatchStatementReq req);

	TSExecuteStatementResp executeQueryStatement(1:TSExecuteStatementReq req);

	TSExecuteStatementResp executeUpdateStatement(1:TSExecuteStatementReq req);

	TSFetchResultsResp fetchResults(1:TSFetchResultsReq req)

	TSFetchMetadataResp fetchMetadata(1:TSFetchMetadataReq req)

	TSCancelOperationResp cancelOperation(1:TSCancelOperationReq req);

	TSCloseOperationResp closeOperation(1:TSCloseOperationReq req);
}

enum TSProtocolVersion {
  TSFILE_SERVICE_PROTOCOL_V1,
}

// OpenSession()
//
// Open a session (connection) on the server against
// which operations may be executed.
struct TSOpenSessionReq {
  1: required TSProtocolVersion client_protocol = TSProtocolVersion.TSFILE_SERVICE_PROTOCOL_V1
  2: optional string username
  3: optional string password
  4: optional map<string, string> configuration
}

struct TSOpenSessionResp {
  1: required TS_Status status

  // The protocol version that the server is using.
  2: required TSProtocolVersion serverProtocolVersion = TSProtocolVersion.TSFILE_SERVICE_PROTOCOL_V1

  // Session Handle
  3: optional TS_SessionHandle sessionHandle

  // The configuration settings for this session.
  4: optional map<string, string> configuration
}

// The return status code contained in each response.
enum TS_StatusCode {
  SUCCESS_STATUS,
  SUCCESS_WITH_INFO_STATUS,
  STILL_EXECUTING_STATUS,
  ERROR_STATUS,
  INVALID_HANDLE_STATUS
}

// The return status of a remote request
struct TS_Status {
  1: required TS_StatusCode statusCode

  // If status is SUCCESS_WITH_INFO, info_msgs may be populated with
  // additional diagnostic information.
  2: optional list<string> infoMessages

  // If status is ERROR, then the following fields may be set
  3: optional string sqlState  // as defined in the ISO/IEF CLI specification
  4: optional i32 errorCode    // internal error code
  5: optional string errorMessage
}


// CloseSession()
//
// Closes the specified session and frees any resources
// currently allocated to that session. Any open
// operations in that session will be canceled.
struct TSCloseSessionReq {
  1: required TS_SessionHandle sessionHandle
}

struct TSCloseSessionResp {
  1: required TS_Status status
}

struct TSHandleIdentifier {
  // 16 byte globally unique identifier
  // This is the public ID of the handle and
  // can be used for reporting.
  1: required binary guid,

  // 16 byte secret generated by the server
  // and used to verify that the handle is not
  // being hijacked by another user.
  2: required binary secret,
}

// Client-side handle to persistent
// session information on the server-side.
struct TS_SessionHandle {
  1: required TSHandleIdentifier sessionId
}


struct TSGetOperationStatusReq {
  // Session to run this request against
  1: required TSOperationHandle operationHandle
}

struct TSGetOperationStatusResp {
  1: required TS_Status status
  //2: optional TSOperationState operationState

  // If operationState is ERROR_STATE, then the following fields may be set
  // sqlState as defined in the ISO/IEF CLI specification
  //3: optional string sqlState

  // Internal error code
  //4: optional i32 errorCode

  // Error message
  //5: optional string errorMessage

  // List of statuses of sub tasks
  //6: optional string taskStatus

  // When was the operation started
  //7: optional i64 operationStarted

  // When was the operation completed
  //8: optional i64 operationCompleted

  // If the operation has the result
  //9: optional bool hasResultSet
}

// CancelOperation()
//
// Cancels processing on the specified operation handle and
// frees any resources which were allocated.
struct TSCancelOperationReq {
  // Operation to cancel
  1: required TSOperationHandle operationHandle
}

struct TSCancelOperationResp {
  1: required TS_Status status
}


// CloseOperation()
//
// Given an operation in the FINISHED, CANCELED,
// or ERROR states, CloseOperation() will free
// all of the resources which were allocated on
// the server to service the operation.
struct TSCloseOperationReq {
  1: required TSOperationHandle operationHandle
}

struct TSCloseOperationResp {
  1: required TS_Status status
}

// Client-side reference to a task running
// asynchronously on the server.
struct TSOperationHandle {
  1: required TSHandleIdentifier operationId

  // If hasResultSet = TRUE, then this operation
  // generates a result set that can be fetched.
  // Note that the result set may be empty.
  //
  // If hasResultSet = FALSE, then this operation
  // does not generate a result set, and calling
  // GetResultSetMetadata or FetchResults against
  // this OperationHandle will generate an error.
  2: required bool hasResultSet

  //3: required TSOperationType operationType

  // For operations that don't generate result sets,
  // modifiedRowCount is either:
  //
  // 1) The number of rows that were modified by
  //    the DML operation (e.g. number of rows inserted,
  //    number of rows deleted, etc).
  //
  // 2) 0 for operations that don't modify or add rows.
  //
  // 3) < 0 if the operation is capable of modifiying rows,
  //    but Hive is unable to determine how many rows were
  //    modified. For example, Hive's LOAD DATA command
  //    doesn't generate row count information because
  //    Hive doesn't inspect the data as it is loaded.
  //
  // modifiedRowCount is unset if the operation generates
  // a result set.
  //4: optional double modifiedRowCount
}

// ExecuteStatement()
//
// Execute a statement.
// The returned OperationHandle can be used to check on the
// status of the statement, and to fetch results once the
// statement has finished executing.
struct TSExecuteStatementReq {
  // The session to execute the statement against
  1: required TS_SessionHandle sessionHandle

  // The statement to be executed (DML, DDL, SET, etc)
  2: required string statement

  // Configuration properties that are overlayed on top of the
  // the existing session configuration before this statement
  // is executed. These properties apply to this statement
  // only and will not affect the subsequent state of the Session.
  //3: optional map<string, string> confOverlay

  // Execute asynchronously when runAsync is true
  //4: optional bool runAsync = false

  // The number of seconds after which the query will timeout on the server
  //5: optional i64 queryTimeout = 0
}

struct TSExecuteStatementResp {
	1: required TS_Status status
	2: optional TSOperationHandle operationHandle
	3: optional list<string> columns
	4: optional string operationType
}

struct TSExecuteBatchStatementResp{
	1: required TS_Status status
	
	2: optional list<i32> result
}

struct TSExecuteBatchStatementReq{
  // The session to execute the statement against
  1: required TS_SessionHandle sessionHandle

  // The statements to be executed (DML, DDL, SET, etc)
  2: required list<string> statements
}

struct TSQueryDataSet{
	1: required list<string> keys
  2: required list<TSDynamicOneColumnData> values
}

struct TSDynamicOneColumnData{
  1: required string deviceType
  2: required string dataType
	3: required i32 length

  4: required list<i64> timeRet

  5: optional list<bool> boolList
	6: optional list<i32> i32List
	7: optional list<i64> i64List
	8: optional list<double> floatList
	9: optional list<double> doubleList
	10: optional list<binary> binaryList
}

struct TSFetchResultsReq{
	1: required string statement
	2: required i32 fetch_size
}

struct TSFetchResultsResp{
	1: required TS_Status status
	2: required bool hasResultSet
	3: optional TSQueryDataSet queryDataSet
}
//
// struct TSJDBCRecord {
// 	1: required string deviceType
// 	2: required string deviceId
// 	3: required list<TSDataPoint> dataList
// 	4: required TSTimeValue timeValue
// }
//
// struct TSTimeValue {
// 	1: required i64 time
// }
//
// struct TSDataPoint{
//   1: required string type
//   2: required string sensorId
//   3: required string deviceId
// 	4: required string valueStr
// 	5: optional i32 groupId
// }

struct TSFetchMetadataResp{
		1: required TS_Status status
		2: optional string metadataInJson
		3: optional map<string, list<string>> deltaObjectMap
		4: optional string dataType
}

enum MEATADATA_OPERATION_TYPE{
METADATA_IN_JSON,
DELTAOBJECT,
COLUMN
}

struct TSFetchMetadataReq{
		1: required MEATADATA_OPERATION_TYPE type
}


struct TSColumnSchema{
	1: optional string name;
	2: optional string dataType;
	3: optional string encoding;
	4: optional map<string, string> otherArgs;
}
