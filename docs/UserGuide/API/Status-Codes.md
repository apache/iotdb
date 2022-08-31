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

# Status Codes

**Status Code** is introduced in the latest version. A sample solution as IoTDB requires registering the time series first before writing data is:

```
try {
    writeData();
} catch (SQLException e) {
  // the most case is that the time series does not exist
  if (e.getMessage().contains("exist")) {
      //However, using the content of the error message is not so efficient
      registerTimeSeries();
      //write data once again
      writeData();
  }
}

```

With Status Code, instead of writing codes like `if (e.getErrorMessage().contains("exist"))`, we can simply use `e.getErrorCode() == TSStatusCode.TIME_SERIES_NOT_EXIST_ERROR.getStatusCode()`.

Here is a list of Status Code and related message:

|Status Code|Status Type|Meanings|
|:---|:---|:---|
|200|SUCCESS_STATUS||
|201|STILL_EXECUTING_STATUS||
|202|INVALID_HANDLE_STATUS||
|203|INCOMPATIBLE_VERSION|Incompatible version|
|298|NODE_DELETE_FAILED_ERROR|Failed while deleting node|
|299|ALIAS_ALREADY_EXIST_ERROR|Alias already exists|
|300|PATH_ALREADY_EXIST_ERROR|Path already exists|
|301|PATH_NOT_EXIST_ERROR|Path does not exist|
|302|UNSUPPORTED_FETCH_METADATA_OPERATION_ERROR|Unsupported fetch metadata operation|
|303|METADATA_ERROR|Meet error when dealing with metadata|
|305|OUT_OF_TTL_ERROR|Insertion time is less than TTL time bound|
|306|CONFIG_ADJUSTER|IoTDB system load is too large|
|307|MERGE_ERROR|Meet error while merging|
|308|SYSTEM_CHECK_ERROR|Meet error while system checking|
|309|SYNC_DEVICE_OWNER_CONFLICT_ERROR|Sync device owners conflict|
|310|SYNC_CONNECTION_EXCEPTION|Meet error while sync connecting|
|311|STORAGE_GROUP_PROCESSOR_ERROR|Storage group processor related error|
|312|STORAGE_GROUP_ERROR|Storage group related error|
|313|STORAGE_ENGINE_ERROR|Storage engine related error|
|314|TSFILE_PROCESSOR_ERROR|TsFile processor related error|
|315|PATH_ILLEGAL|Illegal path|
|316|LOAD_FILE_ERROR|Meet error while loading file|
|317|STORAGE_GROUP_NOT_READY| The storage group is in recovery mode, not ready fore accepting read/write operation|
|400|EXECUTE_STATEMENT_ERROR|Execute statement error|
|401|SQL_PARSE_ERROR|Meet error while parsing SQL|
|402|GENERATE_TIME_ZONE_ERROR|Meet error while generating time zone|
|403|SET_TIME_ZONE_ERROR|Meet error while setting time zone|
|404|NOT_STORAGE_GROUP_ERROR|Operating object is not a storage group|
|405|QUERY_NOT_ALLOWED|Query statements are not allowed error|
|406|AST_FORMAT_ERROR|AST format related error|
|407|LOGICAL_OPERATOR_ERROR|Logical operator related error|
|408|LOGICAL_OPTIMIZE_ERROR|Logical optimize related error|
|409|UNSUPPORTED_FILL_TYPE_ERROR|Unsupported fill type related error|
|410|PATH_ERROR|Path related error|
|411|QUERY_PROCESS_ERROR|Query process related error|
|412|WRITE_PROCESS_ERROR|Writing data related error|
|413|WRITE_PROCESS_REJECT|Writing data rejected error|
|414|QUERY_ID_NOT_EXIST|Kill query with non existent queryId|
|500|INTERNAL_SERVER_ERROR|Internal server error|
|501|CLOSE_OPERATION_ERROR|Meet error in close operation|
|502|READ_ONLY_SYSTEM_ERROR|Operating system is read only|
|503|DISK_SPACE_INSUFFICIENT_ERROR|Disk space is insufficient|
|504|START_UP_ERROR|Meet error while starting up|
|505|SHUT_DOWN_ERROR|Meet error while shutdown|
|506|MULTIPLE_ERROR|Meet error when executing multiple statements|
|600|WRONG_LOGIN_PASSWORD_ERROR|Username or password is wrong|
|601|NOT_LOGIN_ERROR|Has not logged in|
|602|NO_PERMISSION_ERROR|No permissions for this operation, please add privilege|
|603|UNINITIALIZED_AUTH_ERROR|Uninitialized authorizer|
|700|PARTITION_NOT_READY|Partition table not ready|
|701|TIME_OUT|Operation timeout|
|702|NO_LEADER|No leader|
|703|UNSUPPORTED_OPERATION|Unsupported operation|
|704|NODE_READ_ONLY|Node read only|
|705|CONSISTENCY_FAILURE|Consistency check failure|
|706|NO_CONNECTION|Can not get connection error|
|707|NEED_REDIRECTION|Need direction|
|800|CONFIG_ERROR|Configuration error|

> All exceptions are refactored in latest version by extracting uniform message into exception classes. Different error codes are added to all exceptions. When an exception is caught and a higher-level exception is thrown, the error code will keep and pass so that users will know the detailed error reason.
A base exception class "ProcessException" is also added to be extended by all exceptions.

