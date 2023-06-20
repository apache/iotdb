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

| Status Code | Status Type                            | Meanings                                                                                  |
|:------------|:---------------------------------------|:------------------------------------------------------------------------------------------|
| 200         | SUCCESS_STATUS                         |                                                                                           |
| 201         | INCOMPATIBLE_VERSION                   | Incompatible version                                                                      |
| 202         | CONFIGURATION_ERROR                    | Configuration error                                                                       |
| 203         | START_UP_ERROR                         | Meet error while starting                                                                 |
| 204         | SHUT_DOWN_ERROR                        | Meet error while shutdown                                                                 |
| 300         | UNSUPPORTED_OPERATION                  | Unsupported operation                                                                     |
| 301         | EXECUTE_STATEMENT_ERROR                | Execute statement error                                                                   |
| 302         | MULTIPLE_ERROR                         | Meet error when executing multiple statements                                             |
| 303         | ILLEGAL_PARAMETER                      | Parameter is illegal                                                                      |
| 304         | OVERLAP_WITH_EXISTING_TASK             | Current task has some conflict with existing tasks                                        |
| 305         | INTERNAL_SERVER_ERROR                  | Internal server error                                                                     |
| 306         | DISPATCH_ERROR                         | Meet error while dispatching                                                              |
| 400         | REDIRECTION_RECOMMEND                  | Recommend Client redirection                                                              |
| 500         | DATABASE_NOT_EXIST                     | Database does not exist                                                                   |
| 501         | DATABASE_ALREADY_EXISTS                | Database already exist                                                                    |
| 502         | SERIES_OVERFLOW                        | Series number exceeds the threshold                                                       |
| 503         | TIMESERIES_ALREADY_EXIST               | Timeseries already exists                                                                 |
| 504         | TIMESERIES_IN_BLACK_LIST               | Timeseries is being deleted                                                               |
| 505         | ALIAS_ALREADY_EXIST                    | Alias already exists                                                                      |
| 506         | PATH_ALREADY_EXIST                     | Path already exists                                                                       |
| 507         | METADATA_ERROR                         | Meet error when dealing with metadata                                                     |
| 508         | PATH_NOT_EXIST                         | Path does not exist                                                                       |
| 509         | ILLEGAL_PATH                           | Illegal path                                                                              |
| 510         | CREATE_TEMPLATE_ERROR                  | Create schema template error                                                              |
| 511         | DUPLICATED_TEMPLATE                    | Schema template is duplicated                                                             |
| 512         | UNDEFINED_TEMPLATE                     | Schema template is not defined                                                            |
| 513         | TEMPLATE_NOT_SET                       | Schema template is not set                                                                |
| 514         | DIFFERENT_TEMPLATE                     | Template is not consistent                                                                |
| 515         | TEMPLATE_IS_IN_USE                     | Template is in use                                                                        |
| 516         | TEMPLATE_INCOMPATIBLE                  | Template is not compatible                                                                |
| 517         | SEGMENT_NOT_FOUND                      | Segment not found                                                                         |
| 518         | PAGE_OUT_OF_SPACE                      | No enough space on schema page                                                            |
| 519         | RECORD_DUPLICATED                      | Record is duplicated                                                                      |
| 520         | SEGMENT_OUT_OF_SPACE                   | No enough space on schema segment                                                         |
| 521         | PBTREE_FILE_NOT_EXISTS                | PBTreeFile does not exist                                                                 |
| 522         | OVERSIZE_RECORD                        | Size of record exceeds the threshold of page of PBTreeFile                                |
| 523         | PBTREE_FILE_REDO_LOG_BROKEN           | PBTreeFile redo log has broken                                                            |
| 524         | TEMPLATE_NOT_ACTIVATED                 | Schema template is not activated                                                          |
| 526         | SCHEMA_QUOTA_EXCEEDED                  | Schema usage exceeds quota limit                                                          |
| 527  | MEASUREMENT_ALREADY_EXISTS_IN_TEMPLATE | Measurement already exists in schema template                                    | 
| 600         | SYSTEM_READ_ONLY                       | IoTDB system is read only                                                                 |
| 601         | STORAGE_ENGINE_ERROR                   | Storage engine related error                                                              |
| 602         | STORAGE_ENGINE_NOT_READY               | The storage engine is in recovery, not ready fore accepting read/write operation          |
| 603         | DATAREGION_PROCESS_ERROR               | DataRegion related error                                                                  |
| 604         | TSFILE_PROCESSOR_ERROR                 | TsFile processor related error                                                            |
| 605         | WRITE_PROCESS_ERROR                    | Writing data related error                                                                |
| 606         | WRITE_PROCESS_REJECT                   | Writing data rejected error                                                               |
| 607         | OUT_OF_TTL                             | Insertion time is less than TTL time bound                                                |
| 608         | COMPACTION_ERROR                       | Meet error while merging                                                                  |
| 609         | ALIGNED_TIMESERIES_ERROR               | Meet error in aligned timeseries                                                          |
| 610         | WAL_ERROR                              | WAL error                                                                                 |
| 611         | DISK_SPACE_INSUFFICIENT                | Disk space is insufficient                                                                |
| 700         | SQL_PARSE_ERROR                        | Meet error while parsing SQL                                                              |
| 701         | SEMANTIC_ERROR                         | SQL semantic error                                                                        |
| 702         | GENERATE_TIME_ZONE_ERROR               | Meet error while generating time zone                                                     |
| 703         | SET_TIME_ZONE_ERROR                    | Meet error while setting time zone                                                        |
| 704         | QUERY_NOT_ALLOWED                      | Query statements are not allowed error                                                    |
| 705         | LOGICAL_OPERATOR_ERROR                 | Logical operator related error                                                            |
| 706         | LOGICAL_OPTIMIZE_ERROR                 | Logical optimize related error                                                            |
| 707         | UNSUPPORTED_FILL_TYPE                  | Unsupported fill type related error                                                       |
| 708         | QUERY_PROCESS_ERROR                    | Query process related error                                                               |
| 709         | MPP_MEMORY_NOT_ENOUGH                  | Not enough memory for task execution in MPP                                               |
| 710         | CLOSE_OPERATION_ERROR                  | Meet error in close operation                                                             |
| 711         | TSBLOCK_SERIALIZE_ERROR                | TsBlock serialization error                                                               |
| 712         | INTERNAL_REQUEST_TIME_OUT              | MPP Operation timeout                                                                     |
| 713         | INTERNAL_REQUEST_RETRY_ERROR           | Internal operation retry failed                                                           |
| 714         | NO_SUCH_QUERY                          | Cannot find target query                                                                  |
| 715         | QUERY_WAS_KILLED                       | Query was killed when execute                                                             |
| 800         | UNINITIALIZED_AUTH_ERROR               | Failed to initialize auth module                                                          |
| 801         | WRONG_LOGIN_PASSWORD                   | Username or password is wrong                                                             |
| 802         | NOT_LOGIN                              | Not login                                                                                 |
| 803         | NO_PERMISSION                          | No permisstion to operate                                                                 |
| 804         | USER_NOT_EXIST                         | User not exists                                                                           |
| 805         | USER_ALREADY_EXIST                     | User already exists                                                                       |
| 806         | USER_ALREADY_HAS_ROLE                  | User already has target role                                                              |
| 807         | USER_NOT_HAS_ROLE                      | User not has target role                                                                  |
| 808         | ROLE_NOT_EXIST                         | Role not exists                                                                           |
| 809         | ROLE_ALREADY_EXIST                     | Role already exists                                                                       |
| 810         | ALREADY_HAS_PRIVILEGE                  | Already has privilege                                                                     |
| 811         | NOT_HAS_PRIVILEGE                      | Not has privilege                                                                         |
| 812         | CLEAR_PERMISSION_CACHE_ERROR           | Failed to clear permission cache                                                          |
| 813         | UNKNOWN_AUTH_PRIVILEGE                 | Unknown auth privilege                                                                    |
| 814         | UNSUPPORTED_AUTH_OPERATION             | Unsupported auth operation                                                                |
| 815         | AUTH_IO_EXCEPTION                      | IO Exception in auth module                                                               |
| 900         | MIGRATE_REGION_ERROR                   | Error when migrate region                                                                 |
| 901         | CREATE_REGION_ERROR                    | Create region error                                                                       |
| 902         | DELETE_REGION_ERROR                    | Delete region error                                                                       |
| 903         | PARTITION_CACHE_UPDATE_ERROR           | Update partition cache failed                                                             |
| 904         | CONSENSUS_NOT_INITIALIZED              | Consensus is not initialized and cannot provide service                                   |
| 905         | REGION_LEADER_CHANGE_ERROR             | Region leader migration failed                                                            |
| 906         | NO_AVAILABLE_REGION_GROUP              | Cannot find an available region group                                                     |
| 907         | LACK_DATA_PARTITION_ALLOCATION         | Lacked some data partition allocation result in the response                              |
| 1000        | DATANODE_ALREADY_REGISTERED            | DataNode already registered in cluster                                                    |
| 1001        | NO_ENOUGH_DATANODE                     | The number of DataNode is not enough, cannot remove DataNode or create enough replication |
| 1002        | ADD_CONFIGNODE_ERROR                   | Add ConfigNode error                                                                      |
| 1003        | REMOVE_CONFIGNODE_ERROR                | Remove ConfigNode error                                                                   |
| 1004        | DATANODE_NOT_EXIST                     | DataNode not exist error                                                                  |
| 1005        | DATANODE_STOP_ERROR                    | DataNode stop error                                                                       |
| 1006        | REMOVE_DATANODE_ERROR                  | Remove datanode failed                                                                    |
| 1007        | REGISTER_DATANODE_WITH_WRONG_ID        | The DataNode to be registered has incorrect register id                                   |
| 1008        | CAN_NOT_CONNECT_DATANODE               | Can not connect to DataNode                                                               |
| 1100        | LOAD_FILE_ERROR                        | Meet error while loading file                                                             |
| 1101        | LOAD_PIECE_OF_TSFILE_ERROR             | Error when load a piece of TsFile when loading                                            |
| 1102        | DESERIALIZE_PIECE_OF_TSFILE_ERROR      | Error when deserialize a piece of TsFile                                                  |
| 1103        | SYNC_CONNECTION_ERROR                  | Sync connection error                                                                     |
| 1104        | SYNC_FILE_REDIRECTION_ERROR            | Sync TsFile redirection error                                                             |
| 1105        | SYNC_FILE_ERROR                        | Sync TsFile error                                                                         |
| 1106        | CREATE_PIPE_SINK_ERROR                 | Failed to create a PIPE sink                                                              |
| 1107        | PIPE_ERROR                             | PIPE error                                                                                |
| 1108        | PIPESERVER_ERROR                       | PIPE server error                                                                         |
| 1109        | VERIFY_METADATA_ERROR                  | Meet error in validate timeseries schema                                                  |
| 1200        | UDF_LOAD_CLASS_ERROR                   | Error when loading UDF class                                                              |
| 1201        | UDF_DOWNLOAD_ERROR                     | DataNode cannot download UDF from ConfigNode                                              |
| 1202        | CREATE_UDF_ON_DATANODE_ERROR           | Error when create UDF on DataNode                                                         |
| 1203        | DROP_UDF_ON_DATANODE_ERROR             | Error when drop a UDF on DataNode                                                         |
| 1300        | CREATE_TRIGGER_ERROR                   | ConfigNode create trigger error                                                           |
| 1301        | DROP_TRIGGER_ERROR                     | ConfigNode delete Trigger error                                                           |
| 1302        | TRIGGER_FIRE_ERROR                     | Error when firing trigger                                                                 |
| 1303        | TRIGGER_LOAD_CLASS_ERROR               | Error when load class of trigger                                                          |
| 1304        | TRIGGER_DOWNLOAD_ERROR                 | Error when download trigger from ConfigNode                                               |
| 1305        | CREATE_TRIGGER_INSTANCE_ERROR          | Error when create trigger instance                                                        |
| 1306        | ACTIVE_TRIGGER_INSTANCE_ERROR          | Error when activate trigger instance                                                      |
| 1307        | DROP_TRIGGER_INSTANCE_ERROR            | Error when drop trigger instance                                                          |
| 1308        | UPDATE_TRIGGER_LOCATION_ERROR          | Error when move stateful trigger to new datanode                                          |
| 1400        | NO_SUCH_CQ                             | CQ task does not exist                                                                    |
| 1401        | CQ_ALREADY_ACTIVE                      | CQ is already active                                                                      |
| 1402        | CQ_AlREADY_EXIST                       | CQ is already exist                                                                       |
| 1403        | CQ_UPDATE_LAST_EXEC_TIME_ERROR         | CQ update last execution time failed                                                      |

> All exceptions are refactored in the latest version by extracting uniform message into exception classes. Different error codes are added to all exceptions. When an exception is caught and a higher-level exception is thrown, the error code will keep and pass so that users will know the detailed error reason.
A base exception class "ProcessException" is also added to be extended by all exceptions.

