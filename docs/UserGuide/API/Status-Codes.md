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

A sample solution as IoTDB requires registering the time series first before writing data is:

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
|:--|:---|:---|
|200|SUCCESS_STATUS||

|203|INCOMPATIBLE_VERSION|Incompatible version|
|800|CONFIGURATION_ERROR|Configuration error|
|504|START_UP_ERROR|Meet error while starting|
|505|SHUT_DOWN_ERROR|Meet error while shutdown|


|703|UNSUPPORTED_OPERATION|Unsupported operation|
|400|EXECUTE_STATEMENT_ERROR|Execute statement error|
|506|MULTIPLE_ERROR|Meet error when executing multiple statements|
|318|ILLEGAL_PARAMETER|Parameter is illegal|
|920|OVERLAP_WITH_EXISTING_TASK|Current task has some conflict with existing tasks|
|500|INTERNAL_SERVER_ERROR|Internal server error|


|707|REDIRECTION_RECOMMEND|Recommend Client redirection|


|322|DATABASE_NOT_EXIST|Database does not exist|
|903|DATABASE_ALREADY_EXISTS|Database already exist|
|337|SERIES_OVERFLOW|Series number exceeds the threshold|
|338|TIMESERIES_ALREADY_EXIST|Timeseries number exceeds the threshold|
|344|TIMESERIES_IN_BLACK_LIST|Timeseries is being deleted|
|299|ALIAS_ALREADY_EXIST|Alias already exists|
|300|PATH_ALREADY_EXIST|Path already exists|
|303|METADATA_ERROR|Meet error when dealing with metadata|
|304|PATH_NOT_EXIST|Path does not exist|
|315|ILLEGAL_PATH|Illegal path|
|340|CREATE_TEMPLATE_ERROR|Create schema template error|
|320|DUPLICATED_TEMPLATE|Schema template is duplicated|
|321|UNDEFINED_TEMPLATE|Schema template is not defined|
|324|NO_TEMPLATE_ON_MNODE|No schema template on current MNode|
|325|DIFFERENT_TEMPLATE|Template is not consistent|
|326|TEMPLATE_IS_IN_USE|Template is in use|
|327|TEMPLATE_INCOMPATIBLE|Template is not compatible|
|328|SEGMENT_NOT_FOUND|Segment not found|
|329|PAGE_OUT_OF_SPACE|No enough space on schema page|
|330|RECORD_DUPLICATED|Record is duplicated|
|331|SEGMENT_OUT_OF_SPACE|No enough space on schema segment|
|332|SCHEMA_FILE_NOT_EXISTS|SchemaFile does not exist|
|349|OVERSIZE_RECORD|Size of record exceeds the threshold of page of SchemaFile|
|350|SCHEMA_FILE_REDO_LOG_BROKEN|SchemaFile redo log has broken|

|502|SYSTEM_READ_ONLY|IoTDB system is read only|
|313|STORAGE_ENGINE_ERROR|Storage engine related error|
|317|STORAGE_ENGINE_NOT_READY|The storage engine is in recovery, not ready fore accepting read/write operation|
|311|DATAREGION_PROCESS_ERROR|DataRegion related error|
|314|TSFILE_PROCESSOR_ERROR|TsFile processor related error|
|412|WRITE_PROCESS_ERROR|Writing data related error|
|413|WRITE_PROCESS_REJECT|Writing data rejected error|
|305|OUT_OF_TTL|Insertion time is less than TTL time bound|
|307|COMPACTION_ERROR|Meet error while merging|
|319|ALIGNED_TIMESERIES_ERROR|Meet error in aligned timeseries|
|333|WAL_ERROR|WAL error|
|503|DISK_SPACE_INSUFFICIENT|Disk space is insufficient|

|401|SQL_PARSE_ERROR|Meet error while parsing SQL|
|416|SEMANTIC_ERROR|SQL semantic error|
|402|GENERATE_TIME_ZONE_ERROR|Meet error while generating time zone|
|403|SET_TIME_ZONE_ERROR|Meet error while setting time zone|
|405|QUERY_NOT_ALLOWED|Query statements are not allowed error|
|407|LOGICAL_OPERATOR_ERROR|Logical operator related error|
|408|LOGICAL_OPTIMIZE_ERROR|Logical optimize related error|
|409|UNSUPPORTED_FILL_TYPE|Unsupported fill type related error|
|411|QUERY_PROCESS_ERROR|Query process related error|
|423|MPP_MEMORY_NOT_ENOUGH|Not enough memory for task execution in MPP|
|501|CLOSE_OPERATION_ERROR|Meet error in close operation|
|508|TSBLOCK_SERIALIZE_ERROR|TsBlock serialization error|
|701|INTERNAL_REQUEST_TIME_OUT|MPP Operation timeout|
|709|INTERNAL_REQUEST_RETRY_ERROR|Internal operation retry failed|

|607|AUTHENTICATION_ERROR|Error in authentication|
|600|WRONG_LOGIN_PASSWORD|Username or password is wrong|
|601|NOT_LOGIN|Has not logged in|
|602|NO_PERMISSION|No permissions for this operation, please add privilege|
|603|UNINITIALIZED_AUTH_ERROR|Uninitialized authorizer|
|605|USER_NOT_EXIST|User does not exist|
|606|ROLE_NOT_EXIST|Role does not exist|
|608|CLEAR_PERMISSION_CACHE_ERROR|Error when clear the permission cache|

|710|MIGRATE_REGION_ERROR|Error when migrate region|
|711|CREATE_REGION_ERROR|Create region error|
|712|DELETE_REGION_ERROR|Delete region error|
|713|PARTITION_CACHE_UPDATE_ERROR|Update partition cache failed|
|715|CONSENSUS_NOT_INITIALIZED|Consensus is not initialized and cannot provide service|
|918|REGION_LEADER_CHANGE_ERROR|Region leader migration failed|
|921|NO_AVAILABLE_REGION_GROUP|Cannot find an available region group|

|901|DATANODE_ALREADY_REGISTERED|DataNode already registered in cluster|
|904|NO_ENOUGH_DATANODE|The number of DataNode is not enough, cannot remove DataNode or create enough replication|
|906|ADD_CONFIGNODE_ERROR|Add ConfigNode error|
|907|REMOVE_CONFIGNODE_ERROR|Remove ConfigNode error|
|912|DATANODE_NOT_EXIST|DataNode not exist error|
|917|DATANODE_STOP_ERROR|DataNode stop error|
|919|REMOVE_DATANODE_ERROR|Remove datanode failed|
|925|REGISTER_REMOVED_DATANODE|The DataNode to be registered is removed before|
|706|CAN_NOT_CONNECT_DATANODE|Can not connect to DataNode|

|316|LOAD_FILE_ERROR|Meet error while loading file|
|417|LOAD_PIECE_OF_TSFILE_ERROR|Error when load a piece of TsFile when loading|
|714|DESERIALIZE_PIECE_OF_TSFILE_ERROR|Error when deserialize a piece of TsFile|

|334|CREATE_PIPE_SINK_ERROR|Failed to create a PIPE sink|
|335|PIPE_ERROR|PIPE error|
|336|PIPESERVER_ERROR|PIPE server error|
|310|SYNC_CONNECTION_ERROR|Meet error while sync connecting|
|341|SYNC_FILE_REDIRECTION_ERROR|Sync TsFile redirection error|
|342|SYNC_FILE_ERROR|Sync TsFile error|
|343|VERIFY_METADATA_ERROR|Meet error in validate timeseries schema|

|370|UDF_LOAD_CLASS_ERROR|Error when loading UDF class|
|371|UDF_DOWNLOAD_ERROR|DataNode cannot download UDF from ConfigNode|
|372|CREATE_UDF_ON_DATANODE_ERROR|Error when create UDF on DataNode|
|373|DROP_UDF_ON_DATANODE_ERROR|Error when drop a UDF on DataNode|

|950|CREATE_TRIGGER_ERROR|ConfigNode create trigger error|
|951|DROP_TRIGGER_ERROR|ConfigNode delete Trigger error|
|952|TRIGGER_FIRE_ERROR|Error when firing trigger|
|953|TRIGGER_LOAD_CLASS_ERROR|Error when load class of trigger|
|954|TRIGGER_DOWNLOAD_ERROR|Error when download trigger from ConfigNode|
|955|CREATE_TRIGGER_INSTANCE_ERROR|Error when create trigger instance|
|956|ACTIVE_TRIGGER_INSTANCE_ERROR|Error when activate trigger instance|
|957|DROP_TRIGGER_INSTANCE_ERROR|Error when drop trigger instance|
|958|UPDATE_TRIGGER_LOCATION_ERROR|Error when move stateful trigger to new datanode|

|1000|NO_SUCH_CQ|CQ task does not exist|
|1001|CQ_ALREADY_ACTIVE|CQ is already active|
|1002|CQ_AlREADY_EXIST|CQ is already exist|
|1003|CQ_UPDATE_LAST_EXEC_TIME_ERROR|CQ update last execution time failed|

> All exceptions are refactored in the latest version by extracting uniform message into exception classes. Different error codes are added to all exceptions. When an exception is caught and a higher-level exception is thrown, the error code will keep and pass so that users will know the detailed error reason.
A base exception class "ProcessException" is also added to be extended by all exceptions.

