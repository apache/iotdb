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
|201|STILL_EXECUTING_STATUS||
|203|INCOMPATIBLE_VERSION|Incompatible version|
|298|NODE_DELETE_FAILED|Failed while deleting node|
|299|ALIAS_ALREADY_EXIST_ERROR|Alias already exists|
|300|PATH_ALREADY_EXIST_ERROR|Path already exists|
|303|METADATA_ERROR|Meet error when dealing with metadata|
|304|PATH_NOT_EXIST_ERROR|Path does not exist|
|305|OUT_OF_TTL|Insertion time is less than TTL time bound|
|307|MERGE_ERROR|Meet error while merging|
|308|SYSTEM_CHECK_ERROR|Meet error while system checking|
|310|SYNC_CONNECTION_EXCEPTION|Meet error while sync connecting|
|311|DATABASE_PROCESS_ERROR|Database processor related error|
|313|STORAGE_ENGINE_ERROR|Storage engine related error|
|314|TSFILE_PROCESSOR_ERROR|TsFile processor related error|
|315|PATH_ILLEGAL|Illegal path|
|316|LOAD_FILE_ERROR|Meet error while loading file|
|317|DATABASE_NOT_READY|The database is in recovery mode, not ready fore accepting read/write operation|
|318|ILLEGAL_PARAMETER|Parameter is illegal|
|319|ALIGNED_TIMESERIES_ERROR|Meet error in aligned timeseries|
|320|DUPLICATED_TEMPLATE|Schema template is duplicated|
|321|UNDEFINED_TEMPLATE|Schema template is not defined|
|322|DATABASE_NOT_EXIST|Database does not exist|
|323|CONTINUOUS_QUERY_ERROR|Continuous query error|
|324|NO_TEMPLATE_ON_MNODE|No schema template on current MNode|
|325|DIFFERENT_TEMPLATE|Template is not consistent|
|326|TEMPLATE_IS_IN_USE|Template is in use|
|327|TEMPLATE_INCOMPATIBLE|Template is not compatible|
|328|SEGMENT_NOT_FOUND|Segment not found|
|329|PAGE_OUT_OF_SPACE|No enough space on schema page|
|330|RECORD_DUPLICATED|Record is duplicated|
|331|SEGMENT_OUT_OF_SPACE|No enough space on schema segment|
|332|SCHEMA_FILE_NOT_EXISTS|SchemaFile does not exist|
|333|WRITE_AHEAD_LOG_ERROR|WAL error|
|334|CREATE_PIPE_SINK_ERROR|Error in creating PIPE sink|
|335|PIPE_ERROR|PIPE error|
|336|PIPESERVER_ERROR|PIPE server error|
|337|SERIES_OVERFLOW|Series number exceeds the threshold|
|340|CREATE_TEMPLATE_ERROR|Create schema template error|
|341|SYNC_FILE_REBASE|Sync TsFile error|
|342|SYNC_FILE_ERROR|Sync TsFile error|
|343|VERIFY_METADATA_ERROR|Meet error in validate timeseries schema|
|344|TIMESERIES_IN_BLACK_LIST|Timeseries is being deleted|
|349|OVERSIZE_RECORD|Size of record exceeds the threshold of page of SchemaFile|
|350|SCHEMA_FILE_REDO_LOG_BROKEN|SchemaFile redo log has broken|
|355|TRIGGER_FIRE_ERROR|Error when firing trigger|
|360|TRIGGER_LOAD_CLASS_ERROR|Error when load class of trigger|
|361|TRIGGER_DOWNLOAD_ERROR|Error when download trigger from ConfigNode|
|362|CREATE_TRIGGER_INSTANCE_ERROR|Error when create trigger instance|
|363|ACTIVE_TRIGGER_INSTANCE_ERROR|Error when activate trigger instance|
|364|DROP_TRIGGER_INSTANCE_ERROR|Error when drop trigger instance|
|365|UPDATE_TRIGGER_LOCATION_ERROR|Error when move stateful trigger to new datanode|
|370|UDF_LOAD_CLASS_ERROR|Error when loading UDF class|
|372|CREATE_FUNCTION_ON_DATANODE_ERROR|Error when create UDF on DataNode|
|373|DROP_FUNCTION_ON_DATANODE_ERROR|Error when drop a UDF on DataNode|
|400|EXECUTE_STATEMENT_ERROR|Execute statement error|
|401|SQL_PARSE_ERROR|Meet error while parsing SQL|
|402|GENERATE_TIME_ZONE_ERROR|Meet error while generating time zone|
|403|SET_TIME_ZONE_ERROR|Meet error while setting time zone|
|405|QUERY_NOT_ALLOWED|Query statements are not allowed error|
|407|LOGICAL_OPERATOR_ERROR|Logical operator related error|
|408|LOGICAL_OPTIMIZE_ERROR|Logical optimize related error|
|409|UNSUPPORTED_FILL_TYPE|Unsupported fill type related error|
|411|QUERY_PROCESS_ERROR|Query process related error|
|412|WRITE_PROCESS_ERROR|Writing data related error|
|413|WRITE_PROCESS_REJECT|Writing data rejected error|
|416|SEMANTIC_ERROR|SQL semantic error|
|417|LOAD_PIECE_OF_TSFILE_ERROR|Error when load a piece of TsFile when loading|
|423|MEMORY_NOT_ENOUGH|Not enough memory for task execution in MPP|
|500|INTERNAL_SERVER_ERROR|Internal server error|
|501|CLOSE_OPERATION_ERROR|Meet error in close operation|
|502|READ_ONLY_SYSTEM|Operating system is read only|
|503|DISK_SPACE_INSUFFICIENT|Disk space is insufficient|
|504|START_UP_ERROR|Meet error while starting up|
|505|SHUT_DOWN_ERROR|Meet error while shutdown|
|506|MULTIPLE_ERROR|Meet error when executing multiple statements|
|508|TSBLOCK_SERIALIZE_ERROR|TsBlock serialization error|
|600|WRONG_LOGIN_PASSWORD|Username or password is wrong|
|601|NOT_LOGIN|Has not logged in|
|602|NO_PERMISSION|No permissions for this operation, please add privilege|
|603|UNINITIALIZED_AUTH_ERROR|Uninitialized authorizer|
|605|USER_NOT_EXIST|User does not exist|
|606|ROLE_NOT_EXIST|Role does not exist|
|607|AUTHENTICATION_FAILED|Error in authentication|
|608|CLEAR_PERMISSION_CACHE_ERROR|Error when clear the permission cache|
|701|TIME_OUT|Operation timeout|
|703|UNSUPPORTED_OPERATION|Unsupported operation|
|706|NO_CONNECTION|Can not get connection error|
|707|NEED_REDIRECTION|Need direction|
|709|ALL_RETRY_FAILED|All retry failed|
|710|MIGRATE_REGION_FAILED|Error when migrate region|
|711|CREATE_REGION_FAILED|Create region error|
|712|DELETE_REGION_FAILED|Delete region error|
|713|PARTITION_CACHE_UPDATE_FAIL|Update partition cache failed|
|714|DESERIALIZE_PIECE_OF_TSFILE_FAILED|Error when deserialize a piece of TsFile|
|715|CONSENSUS_NOT_INITIALIZED|Consensus is not initialized and cannot provide service|
|800|CONFIGURATION_ERROR|Configuration error|
|901|DATANODE_ALREADY_REGISTERED|DataNode already registered in cluster|
|903|DATABASE_ALREADY_EXISTS|Database already exist|
|904|NOT_ENOUGH_DATANODE|The number of DataNode is not enough|
|905|ERROR_GLOBAL_CONFIG|Global config in cluster does not consistent|
|906|ADD_CONFIGNODE_FAILED|Add ConfigNode failed|
|907|REMOVE_CONFIGNODE_FAILED|Remove ConfigNode failed|
|912|DATANODE_NOT_EXIST|DataNode not exist error|
|916|DATANODE_NO_LARGER_THAN_REPLICATION|DataNode no larger than replication factor, cannot remove|
|917|DATANODE_STOP_FAILED|DataNode stop error|
|918|REGION_LEADER_CHANGE_FAILED|Region leader migration failed|
|919|REMOVE_DATANODE_FAILED|Remove datanode failed|
|920|OVERLAP_WITH_EXISTING_TASK|Current task has some conflict with existing tasks|
|921|NOT_AVAILABLE_REGION_GROUP|Cannot find an available region group|
|922|CREATE_TRIGGER_FAILED|ConfigNode create trigger error|
|923|DROP_TRIGGER_FAILED|ConfigNode delete Trigger error|
|925|REGISTER_REMOVED_DATANODE|The DataNode to be registered is removed before|
|930|NO_SUCH_CQ|CQ task does not exist|
|931|CQ_ALREADY_ACTIVE|CQ is already active|
|932|CQ_AlREADY_EXIST|CQ is already exist|
|933|CQ_UPDATE_LAST_EXEC_TIME_FAILED|CQ update last execution time failed|

> All exceptions are refactored in latest version by extracting uniform message into exception classes. Different error codes are added to all exceptions. When an exception is caught and a higher-level exception is thrown, the error code will keep and pass so that users will know the detailed error reason.
A base exception class "ProcessException" is also added to be extended by all exceptions.

