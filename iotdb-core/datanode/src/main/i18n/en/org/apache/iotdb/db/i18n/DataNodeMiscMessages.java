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

package org.apache.iotdb.db.i18n;

/** Compile-time i18n constants for DataNode misc subsystems (English). */
public final class DataNodeMiscMessages {

    public static final String INVALID_PIPE_NAME =
      "Invalid pipeName";
  public static final String READ_OBJECT_CONTENT_FROM_REMOTE_FILE =
      "readObjectContentFromRemoteFile";
  public static final String ERROR_EXCEPTION =
      "error,  exception:";
  public static final String ACCOUNT_BLOCKED_DUE_TO_CONSECUTIVE_FAILED_LOGINS =
      "Account is blocked due to consecutive failed logins.";
  public static final String VERSION_INCOMPATIBLE_PLEASE_UPGRADE_TO =
      "The version is incompatible, please upgrade to ";
  public static final String ADD_PEER_FOR_REGION_SUCCEED =
      "addPeer %s for region %s succeed";
  public static final String REMOVE_PEER_FOR_REGION_SUCCEED =
      "removePeer %s for region %s succeed";
  public static final String DELETE_PEER_FROM_CONSENSUS_GROUP_SUCCEED =
      "deletePeer from consensus group %ssucceed";
  public static final String DELETE_REGION_ERROR =
      "deleteRegion %s error, %s";
  public static final String DELETE_REGION_SUCCEED =
      "deleteRegion %s succeed";
  public static final String ERROR_PROCESSING_DATA_REGION =
      "Error processing data region: {}";
  public static final String FAILED_TO_PROCESS_TSFILE =
      "Failed to process tsfile {}, {}";

  public static final String CREATE_NEW_REGION_ERROR_FMT = "create new region %s error,  exception:%s";
  public static final String CREATE_NEW_REGION_SUCCEED_FMT = "create new region %s succeed";
  private DataNodeMiscMessages() {}

  // ---------------------------------------------------------------------------
  // protocol – BaseServerContextHandler
  // ---------------------------------------------------------------------------
  public static final String MULTIPLE_SERVER_CONTEXT_FACTORY =
      "There are more than one ServerContextFactory implementation. pls check.";
  public static final String SET_SERVER_CONTEXT_FACTORY =
      "Will set ServerContextFactory from {} ";

  // ---------------------------------------------------------------------------
  // protocol – ConfigNodeInfo
  // ---------------------------------------------------------------------------
  public static final String UPDATE_CONFIG_NODE_SUCCESSFULLY =
      "Update ConfigNode Successfully: {}, which takes {} ms.";
  public static final String UPDATE_CONFIG_NODE_FAILED = "Update ConfigNode failed.";
  public static final String SYSTEM_PROPERTIES_NOT_EXIST =
      "System properties file not exist, not necessary to store ConfigNode list";
  public static final String LOAD_CONFIG_NODE_SUCCESSFULLY =
      "Load ConfigNode successfully: {}, which takes {} ms.";
  public static final String CANNOT_PARSE_CONFIG_NODE_LIST =
      "Cannot parse config node list in system.properties";

  // ---------------------------------------------------------------------------
  // protocol – ConfigNodeClient
  // ---------------------------------------------------------------------------
  public static final String NODE_LEADER_MAY_DOWN_TRY_NEXT =
      "The current node leader may have been down {}, try next node";
  public static final String UNEXPECTED_INTERRUPTION_CONNECT_CONFIG_NODE =
      "Unexpected interruption when waiting to try to connect to ConfigNode";
  public static final String NODE_MAY_DOWN_TRY_NEXT =
      "The current node may have been down {},try next node";
  public static final String FAILED_CONNECT_CONFIG_NODE_NOT_LEADER =
      "Failed to connect to ConfigNode {} from DataNode {}, because the current node is not "
          + "leader or not ready yet, will try again later";
  public static final String UNEXPECTED_INTERRUPTION_CONNECT_CONFIG_NODE_BREAK =
      "Unexpected interruption when waiting to try to connect to ConfigNode, may because current node has been down. Will break current execution process to avoid meaningless wait.";

  // ---------------------------------------------------------------------------
  // protocol – DataNodeInternalClient
  // ---------------------------------------------------------------------------
  public static final String USER_OPENS_INTERNAL_SESSION =
      "User: {}, opens internal Session-{}.";
  public static final String USER_OPENS_INTERNAL_SESSION_FAILED =
      "User {} opens internal Session failed.";
  public static final String USER_OPENS_INTERNAL_SESSION_FAILED_FMT =
      "User %s opens internal Session failed.";

  // ---------------------------------------------------------------------------
  // protocol – AsyncTSStatusRPCHandler / AsyncConfigNodeTSStatusRPCHandler
  // ---------------------------------------------------------------------------
  public static final String SUCCESSFULLY_ON_DATANODE =
      "Successfully {} on DataNode: {}";
  public static final String FAILED_ON_DATANODE =
      "Failed to {} on DataNode: {}, response: {}";
  public static final String SUCCESSFULLY_ON_CONFIG_NODE =
      "Successfully {} on ConfigNode: {}";
  public static final String FAILED_ON_CONFIG_NODE =
      "Failed to {} on ConfigNode: {}, response: {}";

  // ---------------------------------------------------------------------------
  // protocol – AINodeClient
  // ---------------------------------------------------------------------------
  public static final String AINODE_MAY_DOWN =
      "The current AINode may have been down {}, because";
  public static final String CANNOT_CONNECT_ANY_AINODE =
      "Cannot connect to any AINode due to there are no available ones.";
  public static final String UNEXPECTED_INTERRUPTION_CONNECT_AINODE =
      "Unexpected interruption when waiting to try to connect to AINode, may because current node has been down. Will break current execution process to avoid meaningless wait.";

  // ---------------------------------------------------------------------------
  // protocol – SessionManager
  // ---------------------------------------------------------------------------
  public static final String LOGIN_STATUS =
      "{}: Login status: {}. User : {}, opens Session-{}";
  public static final String CLIENT_TRYING_CLOSE_ANOTHER_SESSION =
      "The client-%s is trying to close another session %s, pls check if it's a bug";
  public static final String SESSION_CLOSING = "Session-%s is closing";
  public static final String FAILED_RELEASE_PREPARED_STATEMENT =
      "Failed to release PreparedStatement resources for session {}: {}";
  public static final String FAILED_RELEASE_PREPARED_STATEMENT_CLOSE =
      "Failed to release PreparedStatement '{}' resources when closing statement {} for session {}: {}";
  public static final String NOT_LOGIN = "{}: Not login. ";
  public static final String CLIENT_SESSION_REGISTERED_REPEATEDLY =
      "the client session is registered repeatedly, pls check whether this is a bug.";

  // ---------------------------------------------------------------------------
  // protocol – DataNodeRegionManager
  // ---------------------------------------------------------------------------
  public static final String CREATE_SCHEMA_REGION_FAILED_ILLEGAL_PATH =
      "Create Schema Region {} failed because path is illegal.";
  public static final String CREATE_SCHEMA_REGION_FAILED =
      "Create Schema Region {} failed because {}";
  public static final String CREATE_SCHEMA_REGION_FAILED_FMT =
      "Create Schema Region failed because of %s";
  public static final String SCHEMA_REGION_ALREADY_EXISTS_FMT =
      "SchemaRegion %d already exists.";
  public static final String CREATE_DATA_REGION_FAILED =
      "Create Data Region {} failed because {}";
  public static final String CREATE_DATA_REGION_FAILED_FMT =
      "Create Data Region failed because of %s";
  public static final String DATA_REGION_ALREADY_EXISTS_FMT = "DataRegion %d already exists.";
  public static final String START_CREATE_NEW_REGION = "start to create new region {}";
  public static final String CREATE_NEW_REGION_ERROR = "create new region {} error";
  public static final String SUCCEED_CREATE_NEW_REGION = "succeed to create new region {}";
  public static final String METADATA_ERROR = "{}: MetaData error: ";
  public static final String CREATE_SCHEMA_REGION_FAILED_ILLEGAL_PATH_MSG =
      "Create Schema Region failed because storageGroup path is illegal.";

  // ---------------------------------------------------------------------------
  // protocol – DataNodeInternalRPCServiceImpl
  // ---------------------------------------------------------------------------
  public static final String CONSENSUS_NOT_STARTED =
      "Consensus has not been started after {} seconds, rejecting region request";
  public static final String RECEIVE_FRAGMENT_INSTANCE =
      "receive FragmentInstance to group[{}]";
  public static final String DESERIALIZE_CONSENSUS_GROUP_ID_FAILED =
      "Deserialize ConsensusGroupId failed. ";
  public static final String DESERIALIZE_FRAGMENT_INSTANCE_FAILED =
      "Deserialize FragmentInstance failed.";
  public static final String RECEIVE_LOAD_NODE = "Receive load node from uuid {}.";
  public static final String SCHEMA_CACHE_INVALIDATED =
      "Schema cache of {} has been invalidated";
  public static final String ERROR_PUSHING_PIPE_META =
      "Error occurred when pushing pipe meta";
  public static final String ERROR_PUSHING_SINGLE_PIPE_META =
      "Error occurred when pushing single pipe meta";
  public static final String ERROR_PUSHING_MULTI_PIPE_META =
      "Error occurred when pushing multi pipe meta";
  public static final String ERROR_PUSHING_TOPIC_META =
      "Error occurred when pushing topic meta";
  public static final String ERROR_PUSHING_SINGLE_TOPIC_META =
      "Error occurred when pushing single topic meta";
  public static final String ERROR_PUSHING_MULTI_TOPIC_META =
      "Error occurred when pushing multi topic meta";
  public static final String ERROR_PUSHING_TOPIC_OWNER_LEASE =
      "Error occurred when pushing topic owner lease";
  public static final String ERROR_PUSHING_CONSUMER_GROUP_META =
      "Error occurred when pushing consumer group meta";
  public static final String ERROR_PUSHING_SINGLE_CONSUMER_GROUP_META =
      "Error occurred when pushing single consumer group meta";
  public static final String EXCEPTION_EXECUTING_INTERNAL_SCHEMA_TASK =
      "Exception occurs when executing internal schema task: ";
  public static final String UNSUPPORTED_TYPE_UPDATING_TABLE =
      "Unsupported type {} when updating table";
  public static final String UNSUPPORTED_TYPE_UPDATING_TEMPLATE =
      "Unsupported type {} when updating template";
  public static final String FAILED_GET_MEMORY_FROM_METRIC =
      "Failed to get memory from metric because: ";
  public static final String CHANGE_REGION_LEADER = "[ChangeRegionLeader] {}";
  public static final String REGION_TYPE_ILLEGAL = "region {} type is illegal";
  public static final String START_DISABLE_DATA_NODE =
      "start disable data node in the request: {}";
  public static final String EXECUTE_STOP_AND_CLEAR = "Execute stopAndClearDataNode RPC method";
  public static final String INTERRUPTED_STOP_AND_CLEAR =
      "Meets InterruptedException in stopAndClearDataNode RPC method";
  public static final String STOP_AND_CLEAR_ERROR = "Stop And Clear Data Node error";
  public static final String RETRIEVED_EARLIEST_TIMESLOTS =
      "Retrieved earliest timeslots for {} databases";
  public static final String FAILED_GET_EARLIEST_TIMESLOTS = "Failed to get earliest timeslots";
  public static final String FAILED_GENERATE_DATA_PARTITION_TABLE =
      "Failed to generate DataPartitionTable";
  public static final String FAILED_CHECK_DATA_PARTITION_TABLE_STATUS =
      "Failed to check DataPartitionTable generation status";
  public static final String DATA_PARTITION_TABLE_COMPLETED =
      "DataPartitionTable generation completed with task ID: {}";
  public static final String DATA_PARTITION_TABLE_FAILED =
      "DataPartitionTable generation failed with task ID: {}";
  public static final String PROCESS_DATA_DIR_COMPLETED =
      "Process data directory for earliestTimeslots completed successfully";
  public static final String ERROR_EXECUTING_BATCH_STATEMENT =
      "Error occurred when executing executeBatchStatement: ";

  // ---------------------------------------------------------------------------
  // protocol – ClientRPCServiceImpl
  // ---------------------------------------------------------------------------
  public static final String IOTDB_SERVER_VERSION = "IoTDB server version: {}";
  public static final String TEST_INSERT_BATCH_RECEIVE = "Test insert batch request receive.";
  public static final String TEST_INSERT_ROW_RECEIVE = "Test insert row request receive.";
  public static final String TEST_INSERT_STRING_RECORD_RECEIVE =
      "Test insert string record request receive.";
  public static final String TEST_INSERT_ROW_IN_BATCH_RECEIVE =
      "Test insert row in batch request receive.";
  public static final String TEST_INSERT_ROWS_IN_BATCH_RECEIVE =
      "Test insert rows in batch request receive.";
  public static final String TEST_INSERT_STRING_RECORDS_RECEIVE =
      "Test insert string records request receive.";
  public static final String START_BATCH_EXECUTING_TREE =
      "Start batch executing {} sub-statement(s) in tree model, queryId: {}";
  public static final String EXECUTING_SUB_STATEMENT_TREE =
      "Executing sub-statement {}/{} in tree model, queryId: {}";
  public static final String FAILED_EXECUTE_SUB_STATEMENT_TREE =
      "Failed to execute sub-statement {}/{} in tree model, queryId: {}, completed: {}, remaining: {}, progress: {}%, error: {}";
  public static final String SUCCESSFULLY_EXECUTED_SUB_STATEMENT_TREE =
      "Successfully executed sub-statement {}/{} in tree model, queryId: {}";
  public static final String COMPLETED_BATCH_EXECUTING_TREE =
      "Completed batch executing all {} sub-statement(s) in tree model, queryId: {}";
  public static final String START_BATCH_EXECUTING_TABLE =
      "Start batch executing {} sub-statement(s) in table model, queryId: {}";
  public static final String EXECUTING_SUB_STATEMENT_TABLE =
      "Executing sub-statement {}/{} in table model, queryId: {}";
  public static final String FAILED_EXECUTE_SUB_STATEMENT_TABLE =
      "Failed to execute sub-statement {}/{} in table model, queryId: {}, completed: {}, remaining: {}, progress: {}%, error: {}";
  public static final String SUCCESSFULLY_EXECUTED_SUB_STATEMENT_TABLE =
      "Successfully executed sub-statement {}/{} in table model, queryId: {}";
  public static final String COMPLETED_BATCH_EXECUTING_TABLE =
      "Completed batch executing all {} sub-statement(s) in table model, queryId: {}";

  // ---------------------------------------------------------------------------
  // service – DataNode
  // ---------------------------------------------------------------------------
  public static final String DATANODE_ENV_VARS =
      "IoTDB-DataNode environment variables: {}";
  public static final String DATANODE_DEFAULT_CHARSET =
      "IoTDB-DataNode default charset is: {}";
  public static final String STARTING_DATANODE = "Starting DataNode...";
  public static final String DATANODE_FIRST_START =
      "DataNode is starting for the first time...";
  public static final String DATANODE_RESTARTING = "DataNode is restarting...";
  public static final String IOTDB_CONFIGURATION = "IoTDB configuration: {}";
  public static final String DATANODE_SETUP_SUCCESSFULLY =
      "Congratulations, IoTDB DataNode is set up successfully. Now, enjoy yourself!";
  public static final String FAIL_TO_START_SERVER = "Fail to start server";
  public static final String DATANODE_STARTED = "DataNode started";
  public static final String DATANODE_PREPARED_SUCCESSFULLY =
      "The DataNode is prepared successfully, which takes {} ms";
  public static final String PULLING_SYSTEM_CONFIGURATIONS =
      "Pulling system configurations from the ConfigNode-leader...";
  public static final String CANNOT_PULL_SYSTEM_CONFIGURATIONS =
      "Cannot pull system configurations from ConfigNode-leader";
  public static final String SENDING_REGISTER_REQUEST =
      "Sending register request to ConfigNode-leader...";
  public static final String CANNOT_REGISTER_TO_CLUSTER =
      "Cannot register to the cluster, because: {}";
  public static final String CANNOT_REGISTER_AFTER_RETRIES =
      "Cannot register into cluster after {} retries.";
  public static final String PRECHECK_PASSED =
      "Successfully pass the precheck, will do the formal registration soon.";
  public static final String DELETE_SUCCEED = "delete {} succeed.";
  public static final String DELETE_FAILED_NOT_EXIST =
      "delete {} failed, because it does not exist.";
  public static final String SENDING_RESTART_REQUEST =
      "Sending restart request to ConfigNode-leader...";
  public static final String CLEANED_SORT_TEMP_DIR =
      "Cleaned up stale sort temp directory: {}";
  public static final String MEET_ERROR_STARTING_UP = "Meet error while starting up.";
  public static final String IOTDB_DATANODE_HAS_STARTED = "IoTDB DataNode has started.";
  public static final String SETTING_UP_DATANODE = "Setting up IoTDB DataNode...";
  public static final String RECOVER_SCHEMA = "Recover the schema...";
  public static final String DATANODE_FAILED_SETUP = "IoTDB DataNode failed to set up.";
  public static final String WAIT_DATABASES_READY =
      "Wait for all databases ready, which takes {} ms.";
  public static final String PREPARE_PIPE_RESOURCES =
      "Prepare pipe resources successfully, which takes {} ms.";
  public static final String RECOVER_SCHEMA_SUCCESSFULLY =
      "Recover schema successfully, which takes {} ms.";
  public static final String LOAD_CLASS_ERROR = "load class error: ";
  public static final String EXCEPTION_SCHEMA_REGION_CONSENSUS_STOPPING =
      "Exception during SchemaRegionConsensusImpl stopping";
  public static final String EXCEPTION_DATA_REGION_CONSENSUS_STOPPING =
      "Exception during DataRegionConsensusImpl stopping";

  // ---------------------------------------------------------------------------
  // service – DataNodeShutdownHook
  // ---------------------------------------------------------------------------
  public static final String DATANODE_EXITING = "DataNode exiting...";
  public static final String INTERRUPTED_WAITING_PIPE_FINISH =
      "Interrupted when waiting for pipe to finish";
  public static final String TIMED_OUT_WAITING_PIPES =
      "Timed out when waiting for pipes to finish, will break";
  public static final String FAILED_BORROW_CONFIG_NODE_CLIENT =
      "Failed to borrow ConfigNodeClient";
  public static final String FAILED_REPORT_SHUTDOWN = "Failed to report shutdown";

  // ---------------------------------------------------------------------------
  // service – RegionMigrateService
  // ---------------------------------------------------------------------------
  public static final String REGION_BEGIN_MIGRATING =
      "Region {} is notified to begin migrating";
  public static final String REGION_FINISH_MIGRATING =
      "Region {} is notified to finish migrating";
  public static final String RESET_PEER_LIST_FAIL = "reset peer list fail";
  public static final String REGION_MIGRATE_SERVICE_START = "Region migrate service start";
  public static final String REGION_MIGRATE_SERVICE_STOP = "Region migrate service stop";

  // ---------------------------------------------------------------------------
  // service – SettleService
  // ---------------------------------------------------------------------------
  public static final String START_ERROR = "Start error";
  public static final String WAITING_SETTLE_POOL_SHUTDOWN =
      "Waiting for settle task pool to shut down";
  public static final String SETTLE_SERVICE_STOPPED = "Settle service stopped";

  // ---------------------------------------------------------------------------
  // service – IoTDBInternalLocalReporter
  // ---------------------------------------------------------------------------
  public static final String CHECK_OR_CREATE_DATABASE_FAILED =
      "IoTDBSessionReporter checkOrCreateDatabase failed.";
  public static final String CHECK_OR_CREATE_DATABASE_FAILED_BECAUSE =
      "IoTDBSessionReporter checkOrCreateDatabase failed because ";
  public static final String INTERNAL_REPORTER_ALREADY_STARTED =
      "IoTDB Internal Reporter already start";
  public static final String INTERNAL_REPORTER_START = "IoTDBInternalReporter start!";
  public static final String INTERNAL_REPORTER_STOP = "IoTDBInternalReporter stop!";
  public static final String FAILED_UPDATE_METRIC_VALUE =
      "Failed to update the value of metric with status {}";
  public static final String FAILED_AUTO_CREATE_TIMESERIES =
      "Failed to auto create timeseries for {} with status {}";

  // ---------------------------------------------------------------------------
  // service – ExternalService
  // ---------------------------------------------------------------------------
  public static final String FAILED_MAKE_EXTERNAL_SERVICE_DIR =
      "Failed to make external service dir";
  public static final String EXTERNAL_SERVICE_LIB_ROOT = "External Service lib root: {}";
  public static final String FAILED_GET_OPEN_FILE_NUMBER =
      "Failed to get open file number, because ";
  public static final String UNEXPECTED_ERROR_GETTING_TSFILE_NAME =
      "Unexpected error occurred when getting tsfile name";

  // ---------------------------------------------------------------------------
  // service – metrics
  // ---------------------------------------------------------------------------
  public static final String FAILED_GET_PROCESS_RESIDENT_MEMORY =
      "Failed to get process resident memory for pid {}";
  public static final String DATANODE_PORT_CHECK_SUCCESSFUL = "DataNode port check successful.";

  // ---------------------------------------------------------------------------
  // tools – WalChecker
  // ---------------------------------------------------------------------------
  public static final String CHECKING_FOLDER = "Checking folder: {}";
  public static final String NO_SUB_DIRECTORIES =
      "No sub-directories under the given directory, check ends";
  public static final String CHECKING_DIRECTORY = "Checking the No.{} directory {}";
  public static final String WAL_FILE_NOT_EXIST = "Wal file doesn't exist, skipping";
  public static final String WAL_CHECK_FAILED = "{} fails the check because";
  public static final String CHECK_FINISHED_NO_DAMAGED =
      "Check finished. There is no damaged file";
  public static final String FAILED_FILES_FOUND =
      "There are {} failed files. They are {}";
  public static final String NO_ENOUGH_ARGS =
      "No enough args: require the walRootDirectory";

  // ---------------------------------------------------------------------------
  // tools – TsFileSketchTool
  // ---------------------------------------------------------------------------
  public static final String FAIL_INIT_SKETCH_TOOL = "Fail to init TsFileSketchTool, {}";
  public static final String FAIL_PARSE_TSFILE_METADATA = "Fail to parse TsFileMetadata, {}";
  public static final String FAIL_PRINT_FILE_INFO = "Fail to printFileInfo, {}";
  public static final String FAIL_PARSE_CHUNK = "Fail to parse chunk, {}";
  public static final String FAIL_PRINT_TIMESERIES_INDEX = "Fail to printTimeseriesIndex, {}";

  // ---------------------------------------------------------------------------
  // tools – TsFileSplitTool
  // ---------------------------------------------------------------------------
  public static final String SPLITTING_TSFILE = "Splitting TsFile {} ...";
  public static final String UNSUPPORTED_SPLIT_WITH_MODIFICATION =
      "Unsupported to split TsFile with modification currently.";
  public static final String UNSUPPORTED_SPLIT_WITH_ALIGNED =
      "Unsupported to split TsFile with aligned timeseries currently.";

  // ---------------------------------------------------------------------------
  // tools – TsFileSplitByPartitionTool
  // ---------------------------------------------------------------------------
  public static final String DELETE_UNCOMPLETED_FILE = "delete uncomplated file {}";
  public static final String CREATE_TSFILE_FAILED_EXISTS =
      "Create new TsFile {} failed because it exists";
  public static final String CREATE_TSFILE_FAILED = "Create new TsFile {} failed ";
  public static final String INCORRECT_MAGIC_STRING =
      "the file's MAGIC STRING is incorrect, file path: {}";
  public static final String INCORRECT_VERSION_NUMBER =
      "the file's Version Number is incorrect, file path: {}";
  public static final String FILE_NOT_CLOSED_CORRECTLY =
      "the file is not closed correctly, file path: {}";

  // ---------------------------------------------------------------------------
  // tools – TsFileSelfCheckTool
  // ---------------------------------------------------------------------------
  public static final String ERROR_GETTING_TIMESERIES_METADATA =
      "Error occurred while getting all TimeseriesMetadata with offset in TsFile.";
  public static final String FILE_PATH = "file path: {}";

  // ---------------------------------------------------------------------------
  // tools – TsFileValidationTool
  // ---------------------------------------------------------------------------
  public static final String NOT_DIRECTORY_OR_NOT_EXIST =
      "{} is not a directory or does not exist, skip it.";

  // ---------------------------------------------------------------------------
  // tools – TsFileValidationScan / TsFileStatisticScan
  // ---------------------------------------------------------------------------
  public static final String MEET_ERRORS_READING_FILE =
      "Meet errors in reading file {} , skip it.";
  public static final String MEET_ERROR = "meet error.";

  // ---------------------------------------------------------------------------
  // tools – MLogParser / PBTreeFileSketchTool
  // ---------------------------------------------------------------------------
  public static final String TOO_FEW_PARAMS =
      "Too few params input, please check the following hint.";
  public static final String PARSE_ERROR = "Parse error: {}";
  public static final String ENCOUNTER_ERROR = "Encounter an error, because: {} ";
  public static final String USE_HELP = "Use -help for more information";

  // ---------------------------------------------------------------------------
  // tools – SchemaRegionSnapshotParser
  // ---------------------------------------------------------------------------
  public static final String IOEXCEPTION_GET_FOLDER =
      "ioexception when get {}'s folder";

  // ---------------------------------------------------------------------------
  // tools – SRStatementGenerator
  // ---------------------------------------------------------------------------
  public static final String ERROR_PARSER_TAG_ATTRIBUTES =
      "Error when parser tag and attributes files";
  public static final String MEASUREMENT_ATTRIBUTES_NO_SNAPSHOT =
      "Measurement has set attributes or tags, but not find snapshot files";

  // ---------------------------------------------------------------------------
  // tools – TsFileAndModSettleTool
  // ---------------------------------------------------------------------------
  public static final String CANNOT_FIND_TSFILE = "Cannot find TsFile : {}";
  public static final String NOT_DIRECTORY_PATH = "It's not a directory path : {}";
  public static final String CANNOT_FIND_DIRECTORY = "Cannot find Directory : {}";
  public static final String START_SETTLING_TSFILE =
      "Start settling for tsFile : {}";
  public static final String FINISH_SETTLING_ALL =
      "Finish settling all tsfiles Successfully!";
  public static final String FAIL_SERIALIZE_TSFILE_RESOURCE =
      "fail to serialize new tsfile resource.";
  public static final String FAILED_DELETE_SETTLE_LOG =
      "failed to delete settle log, log path:{}";

  // ---------------------------------------------------------------------------
  // tools – TsFileSettleByCompactionTool
  // ---------------------------------------------------------------------------
  public static final String PARSE_COMMAND_LINE_FAILED =
      "Parse command line args failed: {}";
  public static final String ADD_SETTLE_COMPACTION_TASK_SUCCESS =
      "Add Settle Compaction Task Successfully";
  public static final String ADD_SETTLE_COMPACTION_TASK_FAILED =
      "Add settle compaction task failed with status code: {}";

  // ---------------------------------------------------------------------------
  // tools – TsFileResourcePipeStatisticsSetTool
  // ---------------------------------------------------------------------------
  public static final String UNKNOWN_ARGUMENT = "Unknown argument: {}";
  public static final String NO_DATA_DIRS_PROVIDED =
      "No data directories provided. Please specify with --dirs <dir1> <dir2> ...";
  public static final String VALIDATION_REPAIR_COMPLETED =
      "Validation and repair completed. Statistics:";
  public static final String SEPARATOR_LINE = "------------------------------------------------------";
  public static final String IS_GENERATED_BY_PIPE_MARK = "isGeneratedByPipe mark: {}";
  public static final String RESET_PROGRESS_INDEX = "resetProgressIndex: {}";
  public static final String DATA_DIRECTORIES = "Data directories: ";
  public static final String INDENT_PATH = "  {}";
  public static final String ERROR_VALIDATING_REPAIRING_RESOURCE = "Error validating or repairing resource {}: {}";
  public static final String ERROR_LOADING_RESOURCES_FROM_PARTITION = "Error loading resources from partition {}: {}";
  public static final String TIME_PARTITION_PROCESS_COMPLETED = "TimePartition {} has {} total resources, {} to set isGeneratedByPipe resources, {} to reset progressIndex resources, {} changed resources. Process completed.";
  public static final String SKIPPED_RESOURCE_FILE_NOT_EXIST = "{} is skipped because resource file is not exist.";
  public static final String REPAIRING_TSFILE_RESOURCE = "Repairing TsFileResource: {}, isGeneratedByPipe mark: {}, actual mark: {}";
  public static final String RESETTING_PROGRESS_INDEX_TO_MINIMUM = "Resetting TsFileResource:{} 's progressIndex to minimum, original progressIndex: {}";
  public static final String MARKED_TSFILE_RESOURCE_AS = "Marked TsFileResource as {} in resource: {}";
  public static final String RESET_PROGRESS_INDEX_TO_MINIMUM = "Reset TsFileResource:{} 's progressIndex to minimum.";
  public static final String FAILED_TO_REPAIR_TSFILE_RESOURCE = "ERROR: Failed to repair TsFileResource: {}";
  public static final String TOTAL_TIME_TAKEN = "Total time taken: {} ms, total TsFile resources: {}, set isGeneratedByPipe resources: {}, reset progressIndex resources: {}, changed resources: {}";

  // ---------------------------------------------------------------------------
  // tools – DelayAnalyzer
  // ---------------------------------------------------------------------------
  public static final String DELAY_ANALYZER_RESET = "[DelayAnalyzer] DelayAnalyzer has been reset";

  // ---------------------------------------------------------------------------
  // utils – DataNodeObjectFileService
  // ---------------------------------------------------------------------------
  public static final String FAILED_REMOVE_OBJECT_FILE =
      "Failed to remove object file {}";
  public static final String FAILED_REMOVE_EMPTY_OBJECT_DIR =
      "Failed to remove empty object dir {}";
  public static final String REMOVE_OBJECT_FILE =
      "Remove object file {}, size is {}(byte)";

  // ---------------------------------------------------------------------------
  // utils – OpenFileNumUtil
  // ---------------------------------------------------------------------------
  public static final String CANNOT_GET_PID =
      "Cannot get PID of IoTDB process because ";
  public static final String UNSUPPORTED_OS_GET_PID =
      "Unsupported OS {} for OpenFileNumUtil to get the PID of IoTDB.";
  public static final String CANNOT_GET_OPEN_FILE_NUMBER =
      "Cannot get open file number of IoTDB process because ";

  // ---------------------------------------------------------------------------
  // utils – MemUtils
  // ---------------------------------------------------------------------------
  public static final String UNSUPPORTED_DATA_POINT_TYPE = "Unsupported data point type";

  // ---------------------------------------------------------------------------
  // utils – ErrorHandlingUtils
  // ---------------------------------------------------------------------------
  public static final String ERROR_OPERATION_LOG =
      "Status code: {}, operation: {} failed";

  // ---------------------------------------------------------------------------
  // utils – CommonUtils
  // ---------------------------------------------------------------------------
  public static final String INPUT_FLOAT_INFINITY = "The input float value is Infinity";
  public static final String INPUT_DOUBLE_INFINITY = "The input double value is Infinity";
  public static final String BOOLEAN_PARSE_ERROR =
      "The BOOLEAN should be true/TRUE, false/FALSE or 0/1";
  public static final String UNSUPPORTED_DATA_TYPE_FMT = "Unsupported data type:%s";
  public static final String UNSUPPORTED_DATA_TYPE = "Unsupported data type: ";
  public static final String AGGREGATE_FUNCTION_NAME_NULL =
      "AggregateFunction Name must not be null";
  public static final String INVALID_AGGREGATION_FUNCTION =
      "Invalid Aggregation function: ";
  public static final String INVALID_AGGREGATION_FUNCTION_FMT =
      "Invalid Aggregation function: %s";
  public static final String SCALAR_FUNCTION_NAME_NULL =
      "ScalarFunction Name must not be null.";
  public static final String DELETE_CURSOR_SIZE_ERROR =
      "deleteCursor should be an array whose size is 1";

  // ---------------------------------------------------------------------------
  // utils – ThreadUtils
  // ---------------------------------------------------------------------------
  public static final String WAITING_TERMINATED_TIMEOUT =
      "Waiting {} to be terminated is timeout";
  public static final String POOL_NOT_EXIT_AFTER_TIMEOUT =
      "{} still doesn't exit after 60s";

  // ---------------------------------------------------------------------------
  // utils – WindowEvaluationTaskPoolManager
  // ---------------------------------------------------------------------------
  public static final String WINDOW_EVAL_POOL_INIT =
      "WindowEvaluationTaskPoolManager is initializing, thread number: {}";

  // ---------------------------------------------------------------------------
  // utils – LogWriter
  // ---------------------------------------------------------------------------
  public static final String INTERRUPTED_NO_WRITE =
      "someone interrupt current thread, so no need to do write for io safety";

  // ---------------------------------------------------------------------------
  // conf – IoTDBStartCheck
  // ---------------------------------------------------------------------------
  public static final String STARTING_IOTDB = "Starting IoTDB {}";
  public static final String CANNOT_CREATE_SCHEMA_DIR = "Can not create schema dir: {}";
  public static final String SCHEMA_DIR_CREATED = " {} dir has been created.";
  public static final String IOTDB_VERSION_TOO_OLD = "IoTDB version is too old";
  public static final String REPAIR_SYSTEM_PROPERTIES = "repair system.properties, lack {}";
  public static final String UNEXPECTED_CONSENSUS_GROUP_TYPE =
      "Unexpected consensus group type";
  public static final String ENCRYPT_MAGIC_STRING_NOT_MATCHED =
      "encrypt_magic_string is not matched";

  // ---------------------------------------------------------------------------
  // conf – IoTDBDescriptor
  // ---------------------------------------------------------------------------
  public static final String FAILED_UPDATE_CONFIG_FILE = "Failed to update config file";
  public static final String WILL_RELOAD_PROPERTIES = "Will reload properties from {} ";
  public static final String GET_URL_FAILED = "get url failed";
  public static final String START_READ_CONFIG_FILE = "Start to read config file {}";
  public static final String FAIL_FIND_CONFIG_FILE =
      "Fail to find config file {}, reject DataNode startup.";
  public static final String CANNOT_LOAD_CONFIG_FILE =
      "Cannot load config file, reject DataNode startup.";
  public static final String INCORRECT_FORMAT_CONFIG_FILE =
      "Incorrect format in config file, reject DataNode startup.";
  public static final String COULD_NOT_LOAD_CONFIG =
      "Couldn't load the configuration from any of the known sources.";
  public static final String START_RELOAD_CONFIG_FILE = "Start to reload config file {}";
  public static final String FAIL_RELOAD_CONFIG_FILE = "Fail to reload config file {}";
  public static final String RELOAD_METRIC_SERVICE = "Reload metric service in level {}";
  public static final String PAGE_SIZE_GREATER_THAN_GROUP_SIZE =
      "page_size is greater than group size, will set it as the same with group size";
  public static final String MQTT_HOST_NOT_CONFIGURED =
      "MQTT host is not configured, will use dn_rpc_address.";
  public static final String FAILED_PARSE_TRUSTED_URI =
      "Failed to parse trusted_uri_pattern {}";
  public static final String FAILED_GET_FILE_SIZE = "Failed to get file size of {}, because";
  public static final String SET_DELAY_ANALYZER_WINDOW_SIZE =
      "[DelayAnalyzer] Set delay_analyzer_window_size to {}";
  public static final String FAIL_RELOAD_CONFIGURATION_FMT =
      "Fail to reload configuration because %s";

  // ---------------------------------------------------------------------------
  // conf – IoTDBConfig
  // ---------------------------------------------------------------------------
  public static final String FAIL_GET_CANONICAL_PATH = "Fail to get canonical path of {}";
  public static final String NO_DATA_DIR_SET =
      "No data directory is set. loadTsFileDirs is kept as the default value.";
  public static final String FAILED_GET_FIELD = "Failed to get field {}";
  public static final String SKIP_FAILED_TABLE_SCHEMA_CHECK =
      "skipFailedTableSchemaCheck is set to {}.";
  public static final String DIR_REMOVED_FROM_DATA_DIRS =
      "%s is removed from data_dirs parameter, please add it back.";

  // ---------------------------------------------------------------------------
  // conf – DataNodeMemoryConfig
  // ---------------------------------------------------------------------------
  public static final String FAIL_RELOAD_MEMORY_CONFIG_FMT =
      "Fail to reload configuration because %s";

  // ---------------------------------------------------------------------------
  // conf – DataNodeStartupCheck
  // ---------------------------------------------------------------------------
  public static final String PORTS_HAVE_REPEAT =
      "ports used in datanode have repeat.";

  // ---------------------------------------------------------------------------
  // conf – REST service
  // ---------------------------------------------------------------------------
  public static final String REST_COULD_NOT_LOAD_CONFIG =
      "Couldn't load the REST Service configuration from any of the known sources.";
  public static final String REST_START_READ_CONFIG = "Start to read config file {}";
  public static final String REST_FAIL_FIND_CONFIG =
      "REST service fail to find config file {}";
  public static final String REST_CANNOT_LOAD_CONFIG =
      "REST service cannot load config file, use default configuration";
  public static final String REST_INCORRECT_FORMAT =
      "REST service Incorrect format in config file, use default configuration";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionBroker
  // ---------------------------------------------------------------------------
  public static final String SUBSCRIPTION_PREFETCHING_QUEUE_STATE =
      "Subscription: SubscriptionPrefetchingQueue state {}";
  public static final String SUBSCRIPTION_UNEXPECTED_EXCEPTION =
      "Subscription: unexpected exception (broken invariant) {}";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionReceiverV1
  // ---------------------------------------------------------------------------
  public static final String SUBSCRIPTION_UNKNOWN_REQUEST_TYPE =
      "Subscription: Unknown PipeSubscribeRequestType, response status = {}.";
  public static final String SUBSCRIPTION_CONSUMER_HEARTBEAT_SUCCESS =
      "Subscription: consumer {} heartbeat successfully";
  public static final String SUBSCRIPTION_CONSUMER_SUBSCRIBE_SUCCESS =
      "Subscription: consumer {} subscribe {} successfully";
  public static final String SUBSCRIPTION_CONSUMER_CLOSE_SUCCESS =
      "Subscription: consumer {} close successfully";
  public static final String SUBSCRIPTION_EXCEPTION_HANDSHAKING =
      "Exception occurred when handshaking with request {}";
  public static final String SUBSCRIPTION_EXCEPTION_HEARTBEAT =
      "Exception occurred when heartbeat with request {}";
  public static final String SUBSCRIPTION_EXCEPTION_SUBSCRIBING =
      "Exception occurred when subscribing with request {}";
  public static final String SUBSCRIPTION_EXCEPTION_UNSUBSCRIBING =
      "Exception occurred when unsubscribing with request {}";
  public static final String SUBSCRIPTION_EXCEPTION_POLLING =
      "Exception occurred when polling with request {}";
  public static final String SUBSCRIPTION_EXCEPTION_COMMITTING =
      "Exception occurred when committing with request {}";
  public static final String SUBSCRIPTION_EXCEPTION_CLOSING =
      "Exception occurred when closing with request {}";
  public static final String SUBSCRIPTION_EXCEPTION_CREATING_CONSUMER =
      "Exception occurred when creating consumer {} in config node";
  public static final String SUBSCRIPTION_EXCEPTION_CLOSING_CONSUMER =
      "Exception occurred when closing consumer {} in config node";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionBrokerAgent
  // ---------------------------------------------------------------------------
  public static final String SUBSCRIPTION_CREATE_BROKER =
      "Subscription: create broker bound to consumer group [{}]";
  public static final String SUBSCRIPTION_DROP_BROKER =
      "Subscription: drop broker bound to consumer group [{}]";
  public static final String SUBSCRIPTION_BROKER_NOT_EXIST_FMT =
      "Subscription: broker bound to consumer group [%s] does not exist";
  public static final String SUBSCRIPTION_PIPE_BROKER_NOT_EMPTY =
      "Subscription: pipe broker bound to consumer group [{}] is not empty when dropping";
  public static final String SUBSCRIPTION_CONSENSUS_BROKER_NOT_EMPTY =
      "Subscription: consensus broker bound to consumer group [{}] is not empty when dropping";
  public static final String SUBSCRIPTION_DROP_CONSENSUS_BROKER =
      "Subscription: drop consensus broker bound to consumer group [{}]";
  public static final String SUBSCRIPTION_CREATE_PIPE_BROKER_FOR_BINDING =
      "Subscription: pipe broker bound to consumer group [{}] does not exist, create new for binding prefetching queue";
  public static final String SUBSCRIPTION_CREATE_CONSENSUS_BROKER_FOR_BINDING =
      "Subscription: consensus broker bound to consumer group [{}] does not exist, create new for binding consensus prefetching queue";
  public static final String SUBSCRIPTION_CONSENSUS_UNEXPECTED_IN_FLIGHT_RESPONSE_FMT =
      "ConsensusPrefetchingQueue %s: unexpected in-flight response for consumer %s, commit context %s, offset %s";
  public static final String SUBSCRIPTION_UNSUPPORTED_CONSENSUS_PROGRESS_FILE_VERSION_FMT =
      "Unsupported consensus subscription progress file version %s";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionConsumerAgent
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_DROPPING_CONSUMER_GROUP =
      "Exception occurred when dropping consumer group {}";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionTopicAgent
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_DROPPING_TOPIC =
      "Exception occurred when dropping topic {}";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionEvent
  // ---------------------------------------------------------------------------
  public static final String EVENT_NACKED_TIMES = "{} has been nacked {} times";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionPollResponseCache
  // ---------------------------------------------------------------------------
  public static final String NULL_RESPONSE_INVALIDATING =
      "null response when invalidating, skip it";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionEventTsFileResponse
  // ---------------------------------------------------------------------------
  public static final String UNEXPECTED_RESPONSE_TYPE = "unexpected response type: {}";
  public static final String UNEXPECTED_MESSAGE_TYPE = "unexpected message type: {}";
  public static final String UNEXPECTED_RESPONSE_TYPE_FMT = "unexpected response type: %s";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionPipeEventBatches
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_SEALING_EVENTS =
      "Exception occurred when sealing events from batch {}";
  public static final String EXCEPTION_CONSTRUCT_NEW_BATCH =
      "Exception occurred when construct new batch";

  // ---------------------------------------------------------------------------
  // subscription – SubscriptionPrefetchingQueue
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_EXECUTE_RECEIVER_SUBTASK =
      "Exception {} occurred when {} execute receiver subtask";
  public static final String EXCEPTION_CONSTRUCT_TABLET_ITERATOR =
      "Exception {} occurred when {} construct ToTabletIterator";
  public static final String EXCEPTION_EMIT_EVENTS_BEFORE_COMMIT_TERMINATE_EVENT =
      "Subscription: SubscriptionPrefetchingQueue {} failed to emit remaining events before "
          + "committing PipeTerminateEvent {}";
  public static final String COMMIT_TERMINATE_EVENT =
      "Subscription: SubscriptionPrefetchingQueue {} commit PipeTerminateEvent {}";

  // ---------------------------------------------------------------------------
  // consensus – BaseStateMachine
  // ---------------------------------------------------------------------------
  public static final String UNEXPECTED_CONSENSUS_REQUEST =
      "Unexpected IConsensusRequest : {}";
  public static final String UNEXPECTED_CONSENSUS_REQUEST_EXCEPTION =
      "Unexpected IConsensusRequest!";

  // ---------------------------------------------------------------------------
  // consensus – SchemaExecutionVisitor
  // ---------------------------------------------------------------------------
  public static final String IO_ERROR = "{}: IO error: ";
  public static final String OPENED_PIPE_LISTENING_QUEUE =
      "Opened pipe listening queue on schema region {}";
  public static final String CLOSED_PIPE_LISTENING_QUEUE =
      "Closed pipe listening queue on schema region {}";

  // ---------------------------------------------------------------------------
  // consensus – SchemaRegionStateMachine
  // ---------------------------------------------------------------------------
  public static final String FAIL_LOAD_SNAPSHOT = "Fail to load snapshot from {}";

  // ---------------------------------------------------------------------------
  // consensus – DataExecutionVisitor
  // ---------------------------------------------------------------------------
  public static final String ERROR_EXECUTING_PLAN_NODE =
      "Error in executing plan node: {}";
  public static final String ERROR_EXECUTING_PLAN_NODE_CAUSED =
      "Error in executing plan node: {}, caused by {}";
  public static final String REJECT_EXECUTING_PLAN_NODE =
      "Reject in executing plan node: {}, caused by {}";
  public static final String BATCH_FAILURE_INSERT_ROWS =
      "Batch failure in executing a InsertRowsNode.";
  public static final String BATCH_FAILURE_INSERT_MULTI_TABLETS =
      "Batch failure in executing a InsertMultiTabletsNode.";
  public static final String BATCH_FAILURE_INSERT_ROWS_ONE_DEVICE =
      "Batch failure in executing a InsertRowsOfOneDeviceNode.";

  // ---------------------------------------------------------------------------
  // consensus – DataRegionStateMachine
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_REPLACING_DATA_REGION =
      "Exception occurs when replacing data region in storage engine.";
  public static final String UNEXPECTED_PLAN_NODE_TYPE =
      "Unexpected PlanNode type {}, which is not SearchNode";
  public static final String TABLE_NOT_EXISTS_OR_LOST =
      "table is not exists or lost, result code is {}";
  public static final String GET_FRAGMENT_INSTANCE_FAILED = "Get fragment instance failed";
  public static final String CANNOT_GET_CANONICAL_FILE =
      "{}: cannot get the canonical file of {} due to {}";

  // ---------------------------------------------------------------------------
  // auth – LoginLockManager
  // ---------------------------------------------------------------------------
  public static final String IP_LOGIN_ATTEMPTS_DISABLED =
      "IP-level login attempts disabled (set to {})";
  public static final String USER_LOGIN_ATTEMPTS_DISABLED =
      "User-level login attempts disabled (set to {})";
  public static final String IP_LOCKED = "IP '{}' locked for user ID '{}'";
  public static final String USER_UNLOCKED_MANUAL = "User ID '{}' unlocked (manual)";
  public static final String IP_UNLOCKED_MANUAL =
      "IP '{}' for user ID '{}' unlocked (manual)";
  public static final String USER_UNLOCKED_EXPIRED = "User ID '{}' unlocked (expired)";
  public static final String IP_UNLOCKED_EXPIRED =
      "IP '{}' for user ID '{}' unlocked (expired)";
  public static final String IP_LOCKED_MULTIPLE_USERS =
      "IP '{}' locked by {} different users → potential attack";
  public static final String USER_MULTIPLE_IP_LOCKS =
      "User ID '{}' has {} IP locks → potential attack";
  public static final String FAILED_CHECK_IP_UP =
      "Failed to check if IP address={} is up";

  // ---------------------------------------------------------------------------
  // auth – ClusterAuthorityFetcher
  // ---------------------------------------------------------------------------
  public static final String CACHE_USER_PATH_PRIVILEGES_ERROR =
      "cache user's path privileges error";
  public static final String CACHE_ROLE_PATH_PRIVILEGES_ERROR =
      "cache role's path privileges error";

  // ---------------------------------------------------------------------------
  // auth – BasicAuthorityCache
  // ---------------------------------------------------------------------------
  public static final String DATANODE_CACHE_INIT_FAILED =
      "datanode cache initialization failed";

  // ---------------------------------------------------------------------------
  // trigger – TriggerExecutor
  // ---------------------------------------------------------------------------
  public static final String TRIGGER_FIRE_ERROR =
      "Error occurred when firing trigger, trigger: {}, cause: {}";

  // ---------------------------------------------------------------------------
  // trigger – TriggerInformationUpdater
  // ---------------------------------------------------------------------------
  public static final String TRIGGER_INFO_UPDATER_STARTED =
      "Stateful-Trigger-Information-Updater is successfully started.";
  public static final String TRIGGER_INFO_UPDATER_STOPPED =
      "Stateful-Trigger-Information-Updater is successfully stopped.";
  public static final String ERROR_UPDATING_TRIGGER_INFO =
      "Meet error when updating trigger information:";

  // ---------------------------------------------------------------------------
  // trigger – TriggerFireVisitor
  // ---------------------------------------------------------------------------
  public static final String TRIGGER_INTERRUPTED_SLEEP =
      "{} interrupted when sleep";

  // ---------------------------------------------------------------------------
  // trigger – TriggerClassLoaderManager / TriggerClassLoader
  // ---------------------------------------------------------------------------
  public static final String TRIGGER_LIB_ROOT = "Trigger lib root: {}";

  // ---------------------------------------------------------------------------
  // trigger – TriggerManagementService
  // ---------------------------------------------------------------------------
  public static final String ERROR_READING_MD5 =
      "Error occurred when trying to read md5 of {}";

  // ---------------------------------------------------------------------------
  // partition – DataPartitionTableGenerator
  // ---------------------------------------------------------------------------
  public static final String TASK_ALREADY_STARTED =
      "Task is already started or completed";

  public static final String FROM_CONFIG_NODE = "' from config node.'";
  public static final String IS_NOT_SUPPORTED = "' is not supported'";
  public static final String CANNOT_SSL_HANDSHAKE_WITH_CN_LEADER = "Cannot SSL Handshake with ConfigNode-leader.";
  public static final String CANNOT_CONNECT_TO_CN_LEADER = "Cannot connect to ConfigNode-leader.";
  public static final String CAPACITY_LARGER_THAN_INITIAL_PERMITS = "Capacity should be larger than initial permits.";
  public static final String CURRENT_TV_LIST_NOT_SORTED = "Current TVList is not sorted";
  public static final String DN_CLIENT_NOT_SUPPORT_ADD_CONSENSUS_GROUP = "DataNode to ConfigNode client doesn't support addConsensusGroup.";
  public static final String DN_CLIENT_NOT_SUPPORT_GET_HEARTBEAT = "DataNode to ConfigNode client doesn't support getConfigNodeHeartBeat.";
  public static final String DN_CLIENT_NOT_SUPPORT_NOTIFY_REGISTER = "DataNode to ConfigNode client doesn't support notifyRegisterSuccess.";
  public static final String DN_CLIENT_NOT_SUPPORT_REGISTER_CN = "DataNode to ConfigNode client doesn't support registerConfigNode.";
  public static final String DN_CLIENT_NOT_SUPPORT_REMOVE_CONSENSUS_GROUP = "DataNode to ConfigNode client doesn't support removeConsensusGroup.";
  public static final String DN_CLIENT_NOT_SUPPORT_REPORT_SHUTDOWN = "DataNode to ConfigNode client doesn't support reportConfigNodeShutdown.";
  public static final String DN_CLIENT_NOT_SUPPORT_SET_STATUS = "DataNode to ConfigNode client doesn't support setDataNodeStatus.";
  public static final String DN_CLIENT_NOT_SUPPORT_STOP_AND_CLEAR = "DataNode to ConfigNode client doesn't support stopAndClearConfigNode.";
  public static final String ERROR_OCCURRED_DURING_CREATING_DIR = "Error occurred during creating directory ";
  public static final String EXPECTING_NON_EMPTY_STRING_FOR = "Expecting a non-empty string for ";
  public static final String FAILED_TO_CONSTRUCT_PIPE_SINK = "Failed to construct PipeSink, because of ";
  public static final String FAILED_TO_GET_UDF_JAR = "Failed to get UDF jar from config node.";
  public static final String FAILED_TO_GET_CONSUMER_GROUP_META = "Failed to get consumer group meta from config node.";
  public static final String FAILED_TO_GET_TOPIC_META = "Failed to get topic meta from config node.";
  public static final String FAILED_TO_GET_TRIGGER_JAR = "Failed to get trigger jar from config node.";
  public static final String FETCH_SCHEMA_FAILED = "Fetch Schema failed. ";
  public static final String INDEX_BELOW_START_POSITION = "Index below startPosition: ";
  public static final String INDEX_EXCEEDS_END_POSITION = "Index exceeds endPosition: ";
  public static final String INDEX_OUT_OF_BOUND_ERROR = "Index out of bound error!";
  public static final String INVALID_PUSH_MULTI_PIPE_META_REQ = "Invalid TPushMultiPipeMetaReq";
  public static final String INVALID_PUSH_MULTI_TOPIC_META_REQ = "Invalid TPushMultiTopicMetaReq";
  public static final String INVALID_PUSH_SINGLE_PIPE_META_REQ = "Invalid TPushSinglePipeMetaReq";
  public static final String INVALID_PARAM = "Invalid param";
  public static final String INVALID_PARAMETERS_CHECK_USER_GUIDE = "Invalid parameters. Please check the user guide.";
  public static final String INVALID_REQUEST = "Invalid request ";
  public static final String PREPARED_STMT_NOT_SUPPORTED_FOR_TREE = "PreparedStatement is not supported for Tree model";
  public static final String FILE_LENGTH_LARGER_THAN_MAX = "The file length is larger than max_object_file_size_in_bytes";
  public static final String UNKNOWN_CONSENSUS_GROUP_TYPE = "Unknown consensus group type: ";
  public static final String UNKNOWN_DATA_TYPE = "Unknown data type: ";
  public static final String UNKNOWN_PARAMETER_TYPE = "Unknown parameter type: ";
  public static final String UNKNOWN_SQL_DIALECT = "Unknown sql_dialect: ";
  public static final String UNRECOGNIZED_MNODE_TYPE = "Unrecognized MNode type";
  public static final String UNRECOGNIZED_DATATYPE = "Unrecognized datatype: ";
  public static final String UNSUPPORTED_COLUMN_GENERATOR_TYPE = "Unsupported ColumnGeneratorType: ";
  public static final String UNSUPPORTED_TRIGGER_FIRE_RESULT_TYPE = "Unsupported TriggerFireResult Type";
  public static final String UTILITY_CLASS = "Utility class";
  public static final String APPEND_SIZE_MUST_BE_POSITIVE = "appendSize must be positive";
  public static final String BLOCKS_SHOULD_NEVER_BE_ZERO = "blocks should never be zero.";
  public static final String END_INDEX_MUST_BE_GE_START_INDEX = "endIndex must be >= startIndex";
  public static final String ERROR_CODE = "error code: ";
  public static final String NULL_RESPONSE_WHEN_SERIALIZING = "null response when serializing";
  public static final String OBJECT_STORAGE_NOT_SUPPORTED_YET = "object storage is not supported yet";
  public static final String REGISTERED_TASK_COUNT_LT_ZERO = "registeredTaskCount < 0";
  public static final String REGISTERED_TASK_COUNT_LE_ZERO = "registeredTaskCount <= 0";
  public static final String REQUEST_TYPE_NOT_SUPPORTED = "request type is not supported: ";
  public static final String UNEXPECTED_REQUEST_TYPE = "unexpected request type: %s";

  // --- DataNodeInternalRPCServiceImpl ---
  public static final String LOAD_COMMAND_REQUIRES_TIME_PARTITION_TO_PROGRESS_INDEX_MAP =
      "Load command requires time partition to progress index map";
  public static final String TOPOLOGY_PROBING_TIMED_OUT_AFTER_S_MS =
      "Topology probing timed out after %sms";
  public static final String NO_SUCH_QUERY = "No such query";
  public static final String CHANGE_REGION_LEADER_ERROR_REGION_TYPE =
      "[ChangeRegionLeader] Error Region type: ";
  public static final String SUBMIT_ADD_REGION_PEER_TASK_FAILED_REGION =
      "Submit addRegionPeer task failed, region: ";
  public static final String SUBMIT_REMOVE_REGION_PEER_TASK_FAILED_REGION =
      "Submit removeRegionPeer task failed, region: ";
  public static final String SUBMIT_DELETE_OLD_REGION_PEER_TASK_FAILED_REGION =
      "Submit deleteOldRegionPeer task failed, region: ";
  public static final String CREATE_NEW_REGION_PEER_SUCCEED_REGION_ID =
      "createNewRegionPeer succeed, regionId: ";
  public static final String DISABLE_DATANODE_SUCCEED = "disable datanode succeed";
  public static final String STOP_AND_CLEAR_DATA_NODE_SUCCEED = "Stop And Clear Data Node succeed";
  public static final String NO_DATA_PARTITION_TABLE_GENERATION_TASK_FOUND =
      "No DataPartitionTable generation task found";

  // --- DataNode ---
  public static final String SUCCESSFULLY_REGISTERED_ALL_UDFS_TAKES_MS =
      "successfully registered all the UDFs, which takes {} ms.";
  public static final String GET_TREE_UDF = "get tree udf: {}";
  public static final String GET_TABLE_UDF = "get table udf: {}";
  public static final String GET_TRIGGER = "get trigger: {}";
}
