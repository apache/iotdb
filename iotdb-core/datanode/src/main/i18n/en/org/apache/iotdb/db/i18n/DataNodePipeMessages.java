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

public final class DataNodePipeMessages {

  // ===================== CONSENSUS =====================

  public static final String CLOSING_DELETION_RESOURCE_MANAGER_FOR =
      "Closing deletion resource manager for {}...";
  public static final String DAL_THREAD_STILL_DOESN_T_EXIT_AFTER =
      "DAL Thread {} still doesn't exit after 30s";
  public static final String DELETIONMANAGER_CURRENT_DAL_DIR_IS_DELETED_SUCCESSFULLY =
      "DeletionManager-{}: current DAL dir {} is deleted successfully";
  public static final String DELETIONMANAGER_CURRENT_DAL_DIR_IS_NOT_INITIALIZED =
      "DeletionManager-{}: current DAL dir {} is not initialized, no need to delete.";
  public static final String DELETIONMANAGER_CURRENT_WAITING_IS_INTERRUPTED_MAY_BECAUSE =
      "DeletionManager-{}: current waiting is interrupted. May because current application is "
          + "down. ";
  public static final String DELETIONMANAGER_DELETE_DELETION_FILE_IN_DIR =
      "DeletionManager-{} delete deletion file in {} dir...";
  public static final String DELETIONMANAGER_FAILED_TO_DELETE_FILE_IN_DIR =
      "DeletionManager-{} failed to delete file in {} dir, please manually check!";
  public static final String DELETIONRESOURCE_HAS_BEEN_RELEASED_TRIGGER_A_REMOVE =
      "DeletionResource {} has been released, trigger a remove of DAL...";
  public static final String DELETION_PERSIST_CANNOT_CREATE_FILE_PLEASE_CHECK =
      "Deletion persist: Cannot create file {}, please check your file system manually.";
  public static final String DELETION_PERSIST_CANNOT_WRITE_TO_MAY_CAUSE =
      "Deletion persist: Cannot write to {}, may cause data inconsistency.";
  public static final String DELETION_PERSIST_CURRENT_BATCH_FSYNC_DUE_TO =
      "Deletion persist-{}: current batch fsync due to timeout";
  public static final String DELETION_PERSIST_CURRENT_FILE_HAS_BEEN_CLOSED =
      "Deletion persist-{}: current file has been closed";
  public static final String DELETION_PERSIST_SERIALIZE_DELETION_RESOURCE =
      "Deletion persist-{}: serialize deletion resource {}";
  public static final String DELETION_PERSIST_STARTING_TO_PERSIST_CURRENT_WRITING =
      "Deletion persist-{}: starting to persist, current writing: {}";
  public static final String DELETION_PERSIST_SWITCHING_TO_A_NEW_FILE =
      "Deletion persist-{}: switching to a new file, current writing: {}";
  public static final String DELETION_RESOURCE_MANAGER_FOR_HAS_BEEN_SUCCESSFULLY =
      "Deletion resource manager for {} has been successfully closed!";
  public static final String DETECT_FILE_CORRUPTED_WHEN_RECOVER_DAL_DISCARD =
      "Detect file corrupted when recover DAL-{}, discard all subsequent DALs...";
  public static final String FAILED_TO_INITIALIZE_DELETIONRESOURCEMANAGER =
      "Failed to initialize DeletionResourceManager";
  public static final String FAILED_TO_READ_DELETION_FILE_MAY_BECAUSE =
      "Failed to read deletion file {}, may because this file corrupted when writing it.";
  public static final String FAILED_TO_RECOVER_DELETIONRESOURCEMANAGER =
      "Failed to recover DeletionResourceManager";
  public static final String FAIL_TO_ALLOCATE_DELETIONBUFFER_GROUP_S_BUFFER =
      "Fail to allocate deletionBuffer-group-{}'s buffer because out of memory.";
  public static final String FAIL_TO_CLOSE_CURRENT_LOGGING_FILE_WHEN =
      "Fail to close current logging file when closing";
  public static final String FAIL_TO_REGISTER_DELETIONRESOURCE_INTO_DELETIONBUFFER_BECAUSE =
      "Fail to register DeletionResource into deletionBuffer-{} because this buffer is closed.";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_ALL_DELETIONS_FLUSHED =
      "Interrupted when waiting for all deletions flushed.";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_RESULT =
      "Interrupted when waiting for result.";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_TAKING_DELETIONRESOURCE_FROM =
      "Interrupted when waiting for taking DeletionResource from blocking queue to serialize.";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_TAKING_WALENTRY_FROM =
      "Interrupted when waiting for taking WALEntry from blocking queue to serialize.";
  public static final String INVALID_DELETION_PROGRESS_INDEX = "Invalid deletion progress index: ";
  public static final String PERSISTTHREAD_DID_NOT_TERMINATE_WITHIN_S =
      "persistThread did not terminate within {}s";
  public static final String READ_DELETION_FILE_MAGIC_VERSION =
      "Read deletion file-{} magic version: {}";
  public static final String READ_DELETION_FROM_FILE = "Read deletion: {} from file {}";
  public static final String UNABLE_TO_CREATE_IOTCONSENSUSV2_DELETION_DIR_AT =
      "Unable to create iotConsensusV2 deletion dir at {}";

  // ===================== AGENT =====================

  public static final String ATTEMPT_TO_REPORT_PIPE_EXCEPTION_TO_A =
      "Attempt to report pipe exception to a null PipeTaskMeta.";
  public static final String CANNOT_PARSE_REBOOT_TIMES_FROM_FILE_SET =
      "Cannot parse reboot times from file {}, set the current time in seconds ({}) as the "
          + "reboot times";
  public static final String CANNOT_RECORD_REBOOT_TIMES_TO_FILE_THE =
      "Cannot record reboot times {} to file {}, the reboot times will not be updated";
  public static final String CANNOT_START_SIMPLEPROGRESSINDEXASSIGNER_BECAUSE_OF =
      "Cannot start SimpleProgressIndexAssigner because of {}";
  public static final String CREATE_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "Create pipe DN task {} successfully within {} ms";
  public static final String DEREGISTER_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "Deregister subtask {}. runningTaskCount: {}, registeredTaskCount: {}";
  public static final String DROP_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "Drop pipe DN task {} successfully within {} ms";
  public static final String ERROR_OCCURRED_WHEN_COLLECTING_EVENTS_FROM_PROCESSOR =
      "Error occurred when collecting events from processor.";
  public static final String EXCEPTION_IN_PIPE_EVENT_PROCESSING_IGNORED_BECAUSE =
      "Exception in pipe event processing, ignored because pipe is dropped.{}";
  public static final String TEMPORARILY_OUT_OF_MEMORY_IN_PIPE_EVENT_PROCESSING =
      "Temporarily out of memory in pipe event processing, will wait for the memory to release. "
          + "Message: {}";
  public static final String EXCEPTION_OCCURRED_WHEN_CLOSING_PIPE_CONNECTOR_SUBTASK =
      "Exception occurred when closing pipe connector subtask {}, root cause: {}";
  public static final String EXCEPTION_OCCURRED_WHEN_CLOSING_PIPE_PROCESSOR_SUBTASK =
      "Exception occurred when closing pipe processor subtask {}, root cause: {}";
  public static final String EXCEPTION_OCCURS_WHEN_EXECUTING_PIPE_TASK =
      "Exception occurs when executing pipe task: ";
  public static final String FAILED_TO_CHECK_IF_PIPE_HAS_RELEASE =
      "Failed to check if pipe has release region related resource with consensus group id: {}.";
  public static final String FAILED_TO_CLEAR_CLOSE_THE_SCHEMA_REGION =
      "Failed to clear/close the schema region listening queue, because {}. Will wait until "
          + "success or the region's state machine is stopped.";
  public static final String FAILED_TO_CLOSE_CONNECTOR_AFTER_FAILED_TO =
      "Failed to close connector after failed to initialize connector. Ignore this exception.";
  public static final String FAILED_TO_CLOSE_LISTENING_QUEUE_FOR_SCHEMAREGION =
      "Failed to close listening queue for SchemaRegion ";
  public static final String FAILED_TO_CLOSE_SOURCE_AFTER_FAILED_TO =
      "Failed to close source after failed to initialize source. Ignore this exception.";
  public static final String FAILED_TO_CONSTRUCT_PIPECONNECTOR_BECAUSE_OF =
      "Failed to construct PipeConnector, because of ";
  public static final String FAILED_TO_DECREASE_REFERENCE_COUNT_FOR_EVENT =
      "Failed to decrease reference count for event {} in PipeRealtimePriorityBlockingQueue";
  public static final String FAILED_TO_GET_PENDINGQUEUE_NO_SUCH_SUBTASK =
      "Failed to get PendingQueue. No such subtask: ";
  public static final String FAILED_TO_GET_PIPE_INFO_FROM_CONFIG_NODE_STATUS =
      "Failed to get pipe info from config node, status is %s.";
  public static final String FAILED_TO_GET_PIPE_METAS_WILL_BE =
      "Failed to get pipe metas, will be synced by configNode later...";
  public static final String FAILED_TO_GET_PIPE_PLUGIN_JAR_FROM =
      "Failed to get pipe plugin jar from config node.";
  public static final String FAILED_TO_GET_PIPE_TASK_META_FROM =
      "Failed to get pipe task meta from config node. Ignore the exception, because config "
          + "node may not be ready yet, and meta will be pushed by config node later.";
  public static final String FAILED_TO_PERSIST_PROGRESS_INDEX_TO_CONFIGNODE =
      "Failed to persist progress index to configNode, status: {}";
  public static final String SHUTDOWN_PROGRESS_NOT_CONFIRMED =
      "This shutdown progress was not confirmed to be persisted on ConfigNode.";
  public static final String START_TO_PERSIST_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
      "Start to persist all pipe progress indexes during shutdown, pipe count {}, deadline {} ms";
  public static final String
      INTERRUPTED_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
          "Interrupted while persisting all pipe progress indexes during shutdown. "
              + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String
      TIMED_OUT_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
          "Timed out after {} ms while persisting all pipe progress indexes during shutdown. "
              + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String FAILED_TO_PERSIST_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
      "Failed to persist all pipe progress indexes during shutdown within {} ms. "
          + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String COLLECTED_PIPE_METAS_FOR_SHUTDOWN_PROGRESS_PERSIST =
      "Collected pipe metas for shutdown progress persist, pipe count {}, pipe meta count {}, "
          + "pipe meta size {} bytes, took {} ms";
  public static final String COLLECTED_EMPTY_PIPE_METAS_DURING_SHUTDOWN =
      "Collected empty pipe metas for {} pipes during shutdown.";
  public static final String START_TO_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE =
      "Start to pushHeartbeat shutdown pipe meta to ConfigNode, dataNode id {}, pipe count {}, "
          + "pipe meta count {}, pipe meta size {} bytes";
  public static final String FAILED_TO_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE =
      "Failed to pushHeartbeat shutdown pipe meta to ConfigNode, status {}, took {} ms. "
          + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String
      SUCCESSFULLY_FINISHED_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE =
          "Successfully finished pushHeartbeat shutdown pipe meta to ConfigNode, pipe count {}, "
              + "pipe meta count {}, pipe meta size {} bytes, took {} ms";
  public static final String
      EXCEPTION_OCCURRED_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
          "Exception occurred while persisting all pipe progress indexes during shutdown. "
              + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String PERSISTING_PIPE_PROGRESS_INDEXES_BEFORE_SHUTDOWN =
      "Persisting pipe progress indexes before shutdown with timeout {} ms.";
  public static final String PIPE_PROGRESS_INDEXES_WERE_NOT_CONFIRMED_DURING_SHUTDOWN =
      "Pipe progress indexes were not confirmed by ConfigNode during shutdown. "
          + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String FAILURE_WHEN_REGISTER_PIPE_PLUGIN_SKIP_THIS =
      "Failure when register pipe plugin {}. Skip this plugin and continue startup.";
  public static final String
      FAILED_TO_REGISTER_PIPE_PLUGIN_BECAUSE_NAME_CONFLICTS_WITH_BUILTIN =
          "Failed to register PipePlugin %s, because the given PipePlugin name is the same as a built-in PipePlugin name.";
  public static final String
      FAILED_TO_REGISTER_PIPE_PLUGIN_BECAUSE_INSTANCE_CONSTRUCTION_FAILED =
          "Failed to register PipePlugin %s(%s), because its instance can not be constructed successfully. Exception: %s";
  public static final String FAILED_TO_REGISTER_PIPE_PLUGIN_BECAUSE_JAR_MD5_MISMATCH =
      "Failed to register PipePlugin %s, because existed md5 of jar file for pipe plugin %s is different from the new jar file.";
  public static final String FAILED_TO_DEREGISTER_BUILTIN_PIPE_PLUGIN =
      "Failed to deregister builtin PipePlugin %s.";
  public static final String PIPECONNECTOR = "PipeConnector: ";
  public static final String EXCEPTION_PIPECONNECTOR_ARG_ID_ARG_HEARTBEAT_FAILED_OR_ENCOUNTERED_FAILURE_WHEN_TRANSFERRING_GENERIC_EVENT_FAILURE_ARG_679A4A49 =
      "PipeConnector: %s(id: %s) heartbeat failed, or encountered failure when transferring generic "
          + "event. Failure: %s";
  public static final String EXCEPTION_THE_DATABASE_NAME_IN_TREE_MODEL_MUST_START_WITH_ROOT_7BFA4609 =
      "the database name in tree model must start with 'root.'.";
  public static final String EXCEPTION_THE_LENGTH_OF_DATABASE_NAME_SHALL_NOT_EXCEED_82C7199C =
      "the length of database name shall not exceed ";
  public static final String PIPEDATANODETASKBUILDER_FAILED_TO_PARSE_INCLUSION_AND_EXCLUSION =
      "PipeDataNodeTaskBuilder failed to parse 'inclusion' and 'exclusion' parameters: {}";
  public static final String PIPEDATANODETASKBUILDER_WHEN_INCLUSION_CONTAINS_DATA_DELETE_REALTIME =
      "PipeDataNodeTaskBuilder: When 'inclusion' contains 'data.delete', 'realtime-first' is "
          + "defaulted to 'false' to prevent sync issues after deletion.";
  public static final String PIPEDATANODETASKBUILDER_WHEN_INCLUSION_INCLUDES_DATA_DELETE_REALTIME =
      "PipeDataNodeTaskBuilder: When 'inclusion' includes 'data.delete', 'realtime-first' set "
          + "to 'true' may result in data synchronization issues after deletion.";
  public static final String PIPEDATANODETASKBUILDER_WHEN_SOURCE_USES_SNAPSHOT_MODEL_REALTIME =
      "PipeDataNodeTaskBuilder: When source uses snapshot model, 'realtime-first' is defaulted "
          + "to 'false' to prevent premature halt before transfer completion.";
  public static final String PIPEDATANODETASKBUILDER_WHEN_SOURCE_USES_SNAPSHOT_MODEL_REALTIME_1 =
      "PipeDataNodeTaskBuilder: When source uses snapshot model, 'realtime-first' set to "
          + "'true' may cause prevent premature halt before transfer completion.";
  public static final String PIPEDATANODETASKBUILDER_WHEN_THE_REALTIME_SYNC_IS_ENABLED =
      "PipeDataNodeTaskBuilder: When the realtime sync is enabled, not enabling the rate "
          + "limiter in sending tsfile may introduce delay for realtime sending.";
  public static final String PIPEDATANODETASKBUILDER_WHEN_THE_REALTIME_SYNC_IS_ENABLED_1 =
      "PipeDataNodeTaskBuilder: When the realtime sync is enabled, we enable rate limiter in "
          + "sending tsfile by default to reserve disk and network IO for realtime sending.";
  public static final String PIPEEVENTCOLLECTOR_THE_EVENT_IS_ALREADY_RELEASED_SKIPPING =
      "PipeEventCollector: The event {} is already released, skipping it.";
  public static final String PIPE_CONNECTOR_SUBTASK_WAS_CLOSED_WITHIN_MS =
      "Pipe: connector subtask {} ({}) was closed within {} ms";
  public static final String FAILED_TO_DISCARD_EVENTS_OF_PIPE_IN_CONNECTOR_SUBTASK =
      "Failed to discard events of pipe {} in connector subtask {}.";
  public static final String PIPE_META_NOT_FOUND = "Pipe meta not found: ";
  public static final String PIPE_SINK_SUBTASKS_WITH_ATTRIBUTES_IS_BOUNDED =
      "Pipe sink subtasks with attributes {} is bounded with sinkExecutor {} and "
          + "callbackExecutor {}.";
  public static final String PIPE_SINK_SUBTASK_CLOSE_OPERATION_STILL_RUNNING =
      "is still running";
  public static final String
      PIPE_SINK_SUBTASK_CLOSE_OPERATION_WILL_RUN_AFTER_CURRENT_CONNECTOR_OPERATION =
          "will run after the current connector operation finishes";
  public static final String PIPE_SINK_SUBTASK_CLOSE_TIMED_OUT =
      "Timed out after {} ms when closing pipe connector subtask {}. Continue dropping it. "
          + "The close operation {}.";
  public static final String PIPE_SINK_SUBTASK_DELAYED_TO_AVOID_FREQUENT_HANDSHAKES =
      "Pipe sink subtask {} is delayed for {} ms before polling events to avoid frequent "
          + "handshakes after client borrow failures.";
  public static final String PIPE_SKIPPING_TEMPORARY_TSFILE_WHICH_SHOULDN_T =
      "Pipe skipping temporary TsFile which shouldn't be transferred: {}";
  public static final String PULLED_PIPE_META_FROM_CONFIG_NODE_RECOVERING =
      "Pulled pipe meta from config node: {}, recovering ...";
  public static final String FAILED_TO_SHOW_CREATE_PIPE_NOT_EXIST =
      "Failed to show create pipe %s, the pipe does not exist.";
  public static final String FAILED_TO_SHOW_CREATE_TOPIC_NOT_EXIST =
      "Failed to show create topic %s, the topic does not exist.";
  public static final String RECEIVED_PIPE_HEARTBEAT_REQUEST_FROM_CONFIG_NODE =
      "Received pipe heartbeat request {} from config node.";
  public static final String REGION_NO_TSFILEINSERTIONEVENTS_TO_REPLACE_FOR_SOURCE =
      "Region {}: No TsFileInsertionEvents to replace for source files {}";
  public static final String REGION_REPLACED_TSFILEINSERTIONEVENTS_WITH =
      "Region {}: Replaced TsFileInsertionEvents {} with {}";
  public static final String REGISTEREDTASKCOUNT_0 = "registeredTaskCount < 0";
  public static final String REGISTEREDTASKCOUNT_0_1 = "registeredTaskCount <= 0";
  public static final String REGISTER_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "Register subtask {}. runningTaskCount: {}, registeredTaskCount: {}";
  public static final String REPORT_PIPERUNTIMEEXCEPTION_TO_LOCAL_PIPETASKMETA_EXCEPTION_MESSAGE =
      "Report PipeRuntimeException to local PipeTaskMeta({}), exception message: {}";
  public static final String RUNNINGTASKCOUNT_0 = "runningTaskCount < 0";
  public static final String RUNNINGTASKCOUNT_0_1 = "runningTaskCount <= 0";
  public static final String SIMPLEPROGRESSINDEXASSIGNER_STARTED_SUCCESSFULLY_ISSIMPLECONSENSUSENABLE_R =
      "SimpleProgressIndexAssigner started successfully. isSimpleConsensusEnable: {}, "
          + "rebootTimes: {}";
  public static final String STARTING_SIMPLEPROGRESSINDEXASSIGNER =
      "Starting SimpleProgressIndexAssigner ...";
  public static final String START_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "Start pipe DN task {} successfully within {} ms";
  public static final String START_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "Start subtask {}. runningTaskCount: {}, registeredTaskCount: {}";
  public static final String STOP_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "Stop pipe DN task {} successfully within {} ms";
  public static final String STOP_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "Stop subtask {}. runningTaskCount: {}, registeredTaskCount: {}";
  public static final String SUBTASK_IS_CLOSED_IGNORE_EXCEPTION =
      "subtask {} is closed, ignore exception";
  public static final String SUBTASK_WORKER_IS_INTERRUPTED = "subtask worker is interrupted";
  public static final String SUCCESSFULLY_PERSISTED_ALL_PIPE_S_INFO_TO =
      "Successfully persisted all pipe's info to configNode.";
  public static final String THE_EXECUTOR_AND_HAS_BEEN_SUCCESSFULLY_SHUTDOWN =
      "The executor {} and {} has been successfully shutdown.";

  // ===================== EVENT =====================

  public static final String DATABASENAMEFROMDATAREGION_IS_NULL =
      "databaseNameFromDataRegion is null";
  public static final String DECREASE_REFERENCE_COUNT_ERROR = "Decrease reference count error.";
  public static final String DECREASE_REFERENCE_COUNT_FOR_MTREE_SNAPSHOT_OR =
      "Decrease reference count for mTree snapshot {} or tLog {} or attribute snapshot {} error.";
  public static final String DECREASE_REFERENCE_COUNT_FOR_TSFILE_ERROR =
      "Decrease reference count for TsFile {} error.";
  public static final String DO_NOT_HAS_A_COMPLETE_PAGE_BODY =
      "do not has a complete page body. Expected:";
  public static final String ERROR_WHILE_PARSING_TSFILE_INSERTION_EVENT =
      "Error while parsing tsfile insertion event";
  public static final String EXCEPTION_OCCURRED_WHEN_DETERMINING_THE_EVENT_TIME =
      "Exception occurred when determining the event time of "
          + "PipeInsertNodeTabletInsertionEvent({}) overlaps with the time range: [{}, {}]. "
          + "Returning true to ensure data integrity.";
  public static final String FAILED_TO_ALLOCATE_MEMORY_FOR_PARSING_TSFILE =
      "{}: failed to allocate memory for parsing TsFile {}, tablet event no. {}, "
          + "will release parser memory and retry the TsFile event later.";
  public static final String FAILED_TO_CONSUME_PARSED_TABLET_FROM_TSFILE_KEEP_PARSER =
      "{}: failed to consume parsed tablet from TsFile {}, tablet event no. {}, retry count is {}, "
          + "will keep parser and retry locally for a short time.";
  public static final String FAILED_TO_BUILD_TABLET = "Failed to build tablet";
  public static final String FAILED_TO_CHECK_NEXT = "Failed to check next";
  public static final String FAILED_TO_CLOSE_TSFILEREADER = "Failed to close TsFileReader";
  public static final String FAILED_TO_CLOSE_TSFILESEQUENCEREADER =
      "Failed to close TsFileSequenceReader";
  public static final String FAILED_TO_CREATE_TSFILEINSERTIONDATATABLETITERATOR =
      "failed to create TsFileInsertionDataTabletIterator";
  public static final String FAILED_TO_GET_NEXT_TABLET_INSERTION_EVENT =
      "Failed to get next tablet insertion event.";
  public static final String FAILED_TO_LOAD_MODIFICATIONS_FROM_TSFILE =
      "Failed to load modifications from TsFile: ";
  public static final String FAILED_TO_READ_METADATA_FOR_DEVICEID_MEASUREMENT =
      "Failed to read metadata for deviceId: {}, measurement: {}, removing";
  public static final String FAILED_TO_RECORD_PARSE_END_TIME_FOR =
      "Failed to record parse end time for pipe {}";
  public static final String FAILED_TO_RECORD_TABLET_METRICS_FOR_PIPE =
      "Failed to record tablet metrics for pipe {}";
  public static final String FOUND_NULL_DEVICEID_REMOVING_ENTRY =
      "Found null deviceId, removing entry";
  public static final String INITIALIZE_DATA_CONTAINER_ERROR = "Initialize data container error.";
  public static final String INSERTNODE_HAS_BEEN_RELEASED = "InsertNode has been released";
  public static final String INSERTROWNODE_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "InsertRowNode({}) is parsed to zero rows according to the pattern({}) and time range "
          + "[{}, {}], the corresponding source event({}) will be ignored.";
  public static final String INSERTTABLETNODE_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "InsertTabletNode({}) is parsed to zero rows according to the pattern({}) and time range "
          + "[{}, {}], the corresponding source event({}) will be ignored.";
  public static final String INVALID_EVENT_TYPE = "Invalid event type: ";
  public static final String INVALID_INPUT = "Invalid input: ";
  public static final String ISGENERATEDBYPIPE_IS_NOT_SUPPORTED =
      "isGeneratedByPipe() is not supported!";
  public static final String MAYEVENTPATHSOVERLAPPEDWITHPATTERN_IS_NOT_SUPPORTED =
      "mayEventPathsOverlappedWithPattern() is not supported!";
  public static final String MAYEVENTTIMEOVERLAPPEDWITHTIMERANGE_IS_NOT_SUPPORTED =
      "mayEventTimeOverlappedWithTimeRange() is not supported!";
  public static final String NO_COMMIT_IDS_FOUND_IN_PIPECOMPACTEDTSFILEINSERTIONEVENT =
      "No commit IDs found in PipeCompactedTsFileInsertionEvent.";
  public static final String PIPECOMPACTEDTSFILEINSERTIONEVENT_DOES_NOT_SUPPORT_EQUALSINIOTCONSENSUSV2 =
      "PipeCompactedTsFileInsertionEvent does not support equalsInIoTConsensusV2.";
  public static final String PIPECOMPACTEDTSFILEINSERTIONEVENT_DOES_NOT_SUPPORT_GETREBOOTTIMES =
      "PipeCompactedTsFileInsertionEvent does not support getRebootTimes.";
  public static final String PIPE_FAILED_TO_GET_DEVICES_FROM_TSFILE =
      "Pipe {}: failed to get devices from TsFile {}, extract it anyway";
  public static final String PIPE_SKIPPING_TEMPORARY_TSFILE_S_PARSING_WHICH =
      "Pipe skipping temporary TsFile's parsing which shouldn't be transferred: {}";
  public static final String ROW_CAN_NOT_BE_CUSTOMIZED = "Row can not be customized";
  public static final String SHALLOWCOPYSELFANDBINDPIPETASKMETAFORPROGRESSREPORT_IS_NOT_SUPPORTED =
      "shallowCopySelfAndBindPipeTaskMetaForProgressReport() is not supported!";
  public static final String SKIPPING_TEMPORARY_TSFILE_S_PROGRESSINDEX_WILL_REPORT =
      "Skipping temporary TsFile {}'s progressIndex, will report MinimumProgressIndex";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_ROW_BY_ROW =
      "TablePatternParser does not support row by row processing";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_TABLET_PROCESSING =
      "TablePatternParser does not support tablet processing";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_TABLET_PROCESSING_WITH =
      "TablePatternParser does not support tablet processing with collect";
  public static final String TABLET_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "Tablet({}) is parsed to zero rows according to the pattern({}) and time range [{}, {}], "
          + "the corresponding source event({}) will be ignored.";
  public static final String TABLE_MODEL_TSFILE_PARSING_DOES_NOT_SUPPORT =
      "Table model tsfile parsing does not support this type of ChunkMeta";
  public static final String TEMPORARY_TSFILE_DETECTED_WILL_SKIP_ITS_TRANSFER =
      "Temporary tsFile {} detected, will skip its transfer.";
  public static final String TSFILE_HAS_INITIALIZED_PIPENAME_CREATION_TIME_PATTERN =
      "TsFile {} has initialized {}, pipeName: {}, creation time: {}, pattern: {}, startTime: "
          + "{}, endTime: {}, withMod: {}";
  public static final String UNCOMPRESS_ERROR_UNCOMPRESS_SIZE =
      "Uncompress error! uncompress size: ";
  public static final String UNSUPPORTED = "UnSupported";
  public static final String UNSUPPORTED_NODE_TYPE = "Unsupported node type ";
  public static final String WAIT_FOR_MEMORY_ENOUGH_FOR_PARSING_FOR =
      "Wait for memory enough for parsing {} for {} seconds.";

  // ===================== PROCESSOR =====================

  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_BINARY_INPUT =
      "AbstractSameTypeNumericOperator does not support binary input";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_BOOLEAN_INPUT =
      "AbstractSameTypeNumericOperator does not support boolean input";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_DATE_INPUT =
      "AbstractSameTypeNumericOperator does not support date input";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_STRING_INPUT =
      "AbstractSameTypeNumericOperator does not support string input";
  public static final String CHANGINGVALUESAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH =
      "ChangingValueSamplingProcessor in {} is initialized with {}: {}, {}: {}, {}: {}.";
  public static final String CLEAN_OUTDATED_INCOMPLETE_COMBINER_PIPENAME_CREATIONTIME_COMBINEID =
      "Clean outdated incomplete combiner: pipeName={}, creationTime={}, combineId={}";
  public static final String COMBINEHANDLER_NOT_FOUND_FOR_PIPEID =
      "CombineHandler not found for pipeId = ";
  public static final String COMBINER_COMBINE_COMPLETED_REGIONID_STATE_RECEIVEDREGIONIDSET_EX =
      "Combiner combine completed: regionId: {}, state: {}, receivedRegionIdSet: {}, "
          + "expectedRegionIdSet: {}";
  public static final String COMBINER_COMBINE_REGIONID_STATE_RECEIVEDREGIONIDSET_EXPECTEDREGI =
      "Combiner combine: regionId: {}, state: {}, receivedRegionIdSet: {}, expectedRegionIdSet: {}";
  public static final String DATA_NODES_ENDPOINTS_FOR_TWO_STAGE_AGGREGATION =
      "Data nodes' endpoints for two-stage aggregation: {}";
  public static final String DIFFERENT_DATA_TYPE_ENCOUNTERED_IN_ONE_WINDOW =
      "Different data type encountered in one window, will purge. Previous type: {}, now type: {}";
  public static final String ENCOUNTERED_EXCEPTION_WHEN_DESERIALIZING_FROM_PIPETASKMETA =
      "Encountered exception when deserializing from PipeTaskMeta";
  public static final String END_POINTS_FOR_TWO_STAGE_AGGREGATION_PIPE =
      "End points for two-stage aggregation pipe (pipeName={}, creationTime={}) were updated to {}";
  public static final String ERROR_OCCURRED_WHEN_CLOSING_COMBINEHANDLER_ID =
      "Error occurred when closing CombineHandler(id = {})";
  public static final String ERROR_OCCURS_WHEN_RECEIVING_REQUEST =
      "Error occurs when receiving request: {}.";
  public static final String LOGIN_FAILED_OR_SESSION_TIMED_OUT =
      "Log in failed. Either you are not authorized or the session has timed out.";
  public static final String FAILED_TO_CLOSE_IOTDBSYNCCLIENT = "Failed to close IoTDBSyncClient";
  public static final String FAILED_TO_CLOSE_OLD_IOTDBSYNCCLIENT =
      "Failed to close old IoTDBSyncClient";
  public static final String FAILED_TO_COMBINE_COUNT = "Failed to combine count: ";
  public static final String FAILED_TO_CONSTRUCT_IOTDBSYNCCLIENT =
      "Failed to construct IoTDBSyncClient";
  public static final String FAILED_TO_FETCH_COMBINE_RESULT = "Failed to fetch combine result: ";
  public static final String FAILED_TO_FETCH_DATA_NODES = "Failed to fetch data nodes";
  public static final String FAILED_TO_FETCH_DATA_REGION_IDS = "Failed to fetch data region ids";
  public static final String FAILED_TO_RECONSTRUCT_IOTDBSYNCCLIENT_AFTER_FAILURE_TO =
      "Failed to reconstruct IoTDBSyncClient {} after failure to send request {} (watermark = {})";
  public static final String FAILED_TO_SEND_REQUEST_WATERMARK_TO =
      "Failed to send request {} (watermark = {}) to {}";
  public static final String FAILED_TO_TRIGGER_COMBINE_WATERMARK_COUNT_PROGRESSINDEX =
      "Failed to trigger combine. watermark={}, count={}, progressIndex={}";
  public static final String FAILURE_OCCURRED_WHEN_TRYING_TO_COMMIT_PROGRESS =
      "Failure occurred when trying to commit progress index. timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String FETCHED_DATA_REGION_IDS_AT = "Fetched data region ids {} at {}";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_BINARY_INPUT =
      "FractionPoweredSumOperator does not support binary input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_BOOLEAN_INPUT =
      "FractionPoweredSumOperator does not support boolean input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_DATE_INPUT =
      "FractionPoweredSumOperator does not support date input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_STRING_INPUT =
      "FractionPoweredSumOperator does not support string input";
  public static final String GLOBAL_COUNT_IS_LESS_THAN_THE_LAST =
      "Global count is less than the last collected count: timestamp={}, count={}";
  public static final String IGNORED_TABLETINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF =
      "Ignored TabletInsertionEvent is not an instance of PipeInsertNodeTabletInsertionEvent "
          + "or PipeRawTabletInsertionEvent: {}";
  public static final String IGNORED_TSFILEINSERTIONEVENT_IS_EMPTY =
      "Ignored TsFileInsertionEvent is empty: {}";
  public static final String IGNORED_TSFILEINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF =
      "Ignored TsFileInsertionEvent is not an instance of PipeTsFileInsertionEvent: {}";
  public static final String ILLEGAL_OUTPUT_SERIES_PATH = "Illegal output series path: ";
  public static final String NO_DATA_NODES_ENDPOINTS_FETCHED = "No data nodes' endpoints fetched";
  public static final String NO_EXPECTED_REGION_ID_SET_FETCHED =
      "No expected region id set fetched";
  public static final String PARTIALPATHLASTOBJECTCACHE_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_FROM_TO =
      "PartialPathLastObjectCache.allocatedMemoryBlock has expanded from {} to {}.";
  public static final String PARTIALPATHLASTOBJECTCACHE_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_FROM_TO =
      "PartialPathLastObjectCache.allocatedMemoryBlock has shrunk from {} to {}.";
  public static final String SENDING_REQUEST_WATERMARK_TO =
      "Sending request {} (watermark = {}) to {}";
  public static final String SWINGINGDOORTRENDINGSAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH =
      "SwingingDoorTrendingSamplingProcessor in {} is initialized with {}: {}, {}: {}, {}: {}.";
  public static final String THE_ABSTRACT_FORMAL_PROCESSOR_DOES_NOT_SUPPORT =
      "The abstract formal processor does not support process events";
  public static final String TUMBLINGTIMESAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH_S =
      "TumblingTimeSamplingProcessor in {} is initialized with {}: {}s, {}: {}, {}: {}.";
  public static final String TWOSTAGECOUNTPROCESSOR_CUSTOMIZED_BY_THREAD_PIPENAME_CREATIONTIME_RE =
      "TwoStageCountProcessor customized by thread {}: pipeName={}, creationTime={}, "
          + "regionId={}, outputSeries={}, localCommitProgressIndex={}, localCount={}";
  public static final String TWO_STAGE_AGGREGATE_PIPE_PIPENAME_CREATIONTIME_RELATED =
      "Two stage aggregate pipe (pipeName={}, creationTime={}) related region ids {}";
  public static final String TWO_STAGE_AGGREGATE_RECEIVER_IS_EXITING =
      "Two stage aggregate receiver is exiting.";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID =
      "Two stage combine (region id = {}, combine id = {}) incomplete: timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_1 =
      "Two stage combine (region id = {}, combine id = {}) outdated: timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_2 =
      "Two stage combine (region id = {}, combine id = {}) success: timestamp={}, count={}, "
          + "progressIndex={}, committed progressIndex={}";
  public static final String UNEXPECTED_STATE_CLASS = "Unexpected state class: ";
  public static final String UNKNOWN_COMBINE_RESULT_TYPE = "Unknown combine result type: ";
  public static final String UNKNOWN_REQUEST_TYPE = "Unknown request type {}: {}.";

  // ===================== SOURCE =====================

  public static final String ALL_DATA_IN_TSFILEEPOCH_WAS_EXTRACTED =
      "All data in TsFileEpoch {} was extracted";
  public static final String BUFFERSIZE_MUST_BE_A_POWER_OF_2 = "bufferSize must be a power of 2";
  public static final String BUFFERSIZE_MUST_NOT_BE_LESS_THAN_1 =
      "bufferSize must not be less than 1";
  public static final String CAPTURE_TREE_AND_CAPTURE_TABLE_CAN_NOT =
      "capture.tree and capture.table can not both be specified as false";
  public static final String DATABASE_NAME_IS_NULL_WHEN_MATCHING_SOURCES =
      "Database name is null when matching sources for table model event.";
  public static final String DATA_REGION_INJECTED_WATERMARK_EVENT_WITH_TIMESTAMP =
      "Data region {}: Injected watermark event with timestamp: {}";
  public static final String DISCARD_TABLET_EVENT_BECAUSE_IT_IS_NOT =
      "Discard tablet event {} because it is not reliable anymore. Change the state of "
          + "TsFileEpoch to USING_BOTH.";
  public static final String DISRUPTOR_ALREADY_STARTED = "Disruptor already started";
  public static final String DISRUPTOR_SHUTDOWN_COMPLETED = "Disruptor shutdown completed";
  public static final String DISRUPTOR_STARTED_WITH_BUFFER_SIZE =
      "Disruptor started with buffer size: {}";
  public static final String EXCEPTION_DURING_ONSHUTDOWN = "Exception during onShutdown()";
  public static final String EXCEPTION_DURING_ONSTART = "Exception during onStart()";
  public static final String EXCEPTION_ENCOUNTERED_WHEN_TRIGGERING_SCHEMA_REGION_SNAPSHOT =
      "Exception encountered when triggering schema region snapshot.";
  public static final String EXCEPTION_PROCESSING = "Exception processing: {} {}";
  public static final String FAILED_TO_LOAD_SNAPSHOT = "Failed to load snapshot {}";
  public static final String FAILED_TO_LOAD_SNAPSHOT_FROM_BYTEBUFFER =
      "Failed to load snapshot from byteBuffer {}.";
  public static final String FAILED_TO_START_SOURCES = "failed to start sources.";
  public static final String HEARTBEAT_EVENT_CAN_NOT_BE_SUPPLIED_BECAUSE =
      "Heartbeat Event {} can not be supplied because the reference count can not be increased";
  public static final String EVENT_CAN_NOT_BE_SUPPLIED_BECAUSE_DATA_IS_LOST =
      "Event %s can not be supplied because the reference count can not be increased, the data represented by this event is lost";
  public static final String INTERRUPTED_WAITING_FOR_PROCESSOR_TO_STOP =
      "Interrupted waiting for processor to stop";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_PARSING_PRIVILEGE_FOR_TSFILE =
      "Interrupted when waiting for parsing privilege for TsFile %s.";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_CLOSING_TSFILE =
      "Interrupted when waiting for closing TsFile %s.";
  public static final String PARSE_TSFILE_ERROR_BECAUSE = "Parse TsFile %s error. Because: %s";
  public static final String PARSE_TSFILE_WHEN_CHECKING_PRIVILEGE_ERROR =
      "Parse TsFile %s when checking privilege error. Because: %s";
  public static final String READ_TSFILE_ERROR = "Read TsFile %s error.";
  public static final String IOTDBSCHEMAREGIONSOURCE_DOES_NOT_SUPPORT_TRANSFERRING_EVENTS_UNDER =
      "IoTDBSchemaRegionSource does not support transferring events under simple consensus";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_EVENT =
      "Not has privilege to transfer event: ";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN =
      "Not has privilege to transfer plan: ";
  public static final String NO_EVENT_HANDLER_CONFIGURED = "No event handler configured";
  public static final String N_MUST_BE_0 = "n must be > 0";
  public static final String PIPEREALTIMEDATAREGIONEXTRACTOR_OBSERVED_DATA_REGION_TIME_PARTITION_GROWT =
      "PipeRealtimeDataRegionExtractor({}) observed data region {} time partition growth, "
          + "recording time partition id bound: {}.";
  public static final String PIPE_AND_IS_NOT_SET_USE_HYBRID =
      "Pipe: '{}' ('{}') and '{}' ('{}') is not set, use hybrid mode by default.";
  public static final String PIPE_ASSIGNER_ON_DATA_REGION_SHUTDOWN_INTERNAL =
      "Pipe: Assigner on data region {} shutdown internal disruptor within {} ms";
  public static final String PIPE_FAILED_TO_GET_DEVICES_FROM_TSFILE_1 =
      "Pipe {}@{}: failed to get devices from TsFile {}, extract it anyway";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR =
      "Pipe {}@{}: failed to increase reference count for historical deletion event {}, will "
          + "discard it";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_1 =
      "Pipe {}@{}: failed to increase reference count for historical tsfile event {}, will "
          + "discard it";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_2 =
      "Pipe {}@{}: failed to increase reference count for terminate event, will resend it";
  public static final String PIPE_FAILED_TO_PIN_TSFILERESOURCE =
      "Pipe: failed to pin TsFileResource {}";
  public static final String PIPE_FAILED_TO_START_TO_EXTRACT_HISTORICAL =
      "Pipe {}@{}: failed to start to extract historical TsFile, storage engine is not ready. "
          + "Will retry later.";
  public static final String PIPE_FAILED_TO_UNPIN_SKIPPED_HISTORICAL_TSFILERESOURCE =
      "Pipe {}@{}: failed to unpin skipped historical TsFileResource, original path: {}";
  public static final String PIPE_FAILED_TO_UNPIN_TSFILERESOURCE_AFTER_CREATING =
      "Pipe {}@{}: failed to unpin TsFileResource after creating event, original path: {}";
  public static final String PIPE_FAILED_TO_UNPIN_TSFILERESOURCE_AFTER_DROPPING =
      "Pipe {}@{}: failed to unpin TsFileResource after dropping pipe, original path: {}";
  public static final String PIPE_FINISH_TO_EXTRACT_DELETIONS_EXTRACT_DELETIONS =
      "Pipe {}@{}: finish to extract deletions, extract deletions count {}/{}, took {} ms";
  public static final String PIPE_FINISH_TO_EXTRACT_HISTORICAL_TSFILE_EXTRACTED =
      "Pipe {}@{}: finish to extract historical TsFile, extracted sequence file count {}/{}, "
          + "extracted unsequence file count {}/{}, extracted file count {}/{}, took {} ms";
  public static final String PIPE_FINISH_TO_SORT_ALL_EXTRACTED_RESOURCES =
      "Pipe {}@{}: finish to sort all extracted resources, took {} ms";
  public static final String PIPE_HISTORICAL_DATA_EXTRACTION_TIME_RANGE_START =
      "Pipe {}@{}: historical data extraction time range, start time {}({}), end time {}({}), "
          + "sloppy pattern {}, sloppy time range {}, should transfer mod file {}, username: {}, "
          + "skip if no privileges: {}, is forwarding pipe requests: {}";
  public static final String PIPE_IS_SET_TO_FALSE_USE_HEARTBEAT =
      "Pipe: '{}' ('{}') is set to false, use heartbeat realtime source.";
  public static final String PIPE_ON_DATA_REGION_SKIP_COMMIT_OF =
      "Pipe {} on data region {} skip commit of event {} because it was flushed prematurely.";
  public static final String PIPE_REALTIME_DATA_REGION_SOURCE_IS_INITIALIZED =
      "Pipe {}@{}: realtime data region source is initialized with parameters: {}.";
  public static final String PIPE_RESOURCE_MEETS_MAYTSFILECONTAINUNPROCESSEDDATA_CONDITION_EXTRACT =
      "Pipe {}@{}: resource {} meets mayTsFileContainUnprocessedData condition, extractor "
          + "progressIndex: {}, resource ProgressIndex: {}";
  public static final String PIPE_SET_WATERMARK_INJECTOR_WITH_INTERVAL_MS =
      "Pipe {}@{}: Set watermark injector with interval {} ms.";
  public static final String PIPE_SKIP_HISTORICAL_TSFILE_BECAUSE_REALTIME_SOURCE =
      "Pipe {}@{}: skip historical tsfile {} because realtime source in current task {} has "
          + "already captured it.";
  public static final String PIPE_SNAPSHOT_MODE_IS_ENABLED_USE_HEARTBEAT =
      "Pipe: snapshot mode is enabled, use heartbeat realtime source.";
  public static final String PIPE_STARTED_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}: Started historical source {} and realtime source {} successfully within {} ms.";
  public static final String PIPE_STARTING_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}: Starting historical source {} and realtime source {}.";
  public static final String PIPE_START_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}: Start historical source {} and realtime source {} error.";
  public static final String PIPE_START_TO_EXTRACT_DELETIONS =
      "Pipe {}@{}: start to extract deletions";
  public static final String PIPE_START_TO_EXTRACT_HISTORICAL_TSFILE_ORIGINAL =
      "Pipe {}@{}: start to extract historical TsFile, original sequence file count {}, "
          + "original unSequence file count {}, start progress index {}";
  public static final String PIPE_START_TO_FLUSH_DATA_REGION =
      "Pipe {}@{}: start to flush data region";
  public static final String PIPE_START_TO_SORT_ALL_EXTRACTED_RESOURCES =
      "Pipe {}@{}: start to sort all extracted resources";
  public static final String PIPE_TASK_CANNOTUSETABLETANYMORE_FOR_TSFILE_THE_MEMORY =
      "Pipe task {}@{} canNotUseTabletAnyMore for tsFile {}: The memory usage of the insert "
          + "node {} has reached the dangerous threshold of single pipe {}, event count: {}";
  public static final String PIPE_UNEXPECTED_PROGRESSINDEX_TYPE_FALLBACK_TO_ORIGIN =
      "Pipe {}@{}: unexpected ProgressIndex type {}, fallback to origin {}.";
  public static final String PIPE_UNSUPPORTED_SOURCE_REALTIME_MODE_CREATE_A =
      "Pipe: Unsupported source realtime mode: {}, create a hybrid source.";
  public static final String PROCESSOR_INTERRUPTED = "Processor interrupted";
  public static final String PROCESSOR_INTERRUPTED_UNEXPECTEDLY =
      "Processor interrupted unexpectedly, continue running";
  public static final String PROCESSOR_STOPPED = "Processor stopped";
  public static final String SET_FOR_HISTORICAL_DELETION_EVENT =
      "[{}]Set {} for historical deletion event {}";
  public static final String SET_FOR_HISTORICAL_EVENT = "[{}]Set {} for historical event {}";
  public static final String SET_FOR_REALTIME_EVENT = "[{}]Set {} for realtime event {}";
  public static final String SOURCES_FILTERED_BY_DATABASE_AND_TABLE_IS =
      "Sources filtered by database and table is null when matching sources for table model event.";
  public static final String SOURCES_FILTERED_BY_DEVICE_IS_NULL_WHEN =
      "Sources filtered by device is null when matching sources for tree model event.";
  public static final String TAKE_SNAPSHOT_ERROR = "Take snapshot error: {}";
  public static final String THE_ASSIGNER_QUEUE_CONTENT_HAS_EXCEEDED_HALF =
      "The assigner queue content has exceeded half, it may be stuck and may block insertion. "
          + "regionId: {}, capacity: {}, bufferSize: {}";
  public static final String THE_PIPE_CANNOT_EXTRACT_TABLE_MODEL_DATA =
      "The pipe cannot extract table model data when sql dialect is set to tree.";
  public static final String THE_PIPE_CANNOT_EXTRACT_TREE_MODEL_DATA =
      "The pipe cannot extract tree model data when sql dialect is set to table.";
  public static final String THE_REFERENCE_COUNT_OF_THE_EVENT_CANNOT =
      "The reference count of the event {} cannot be increased, skipping it.";
  public static final String THE_REFERENCE_COUNT_OF_THE_REALTIME_EVENT =
      "The reference count of the realtime event {} cannot be increased, skipping it.";
  public static final String TIMED_OUT_WAITING_FOR_PROCESSOR_TO_STOP =
      "Timed out waiting for processor to stop";
  public static final String TSFILEEPOCH_NOT_FOUND_FOR_TSFILE_CREATING_A =
      "TsFileEpoch not found for TsFile {}, creating a new one";
  public static final String WHEN_IS_SET_TO_FALSE_SPECIFYING_AND =
      "When '{}' ('{}') is set to false, specifying {} and {} is invalid.";
  public static final String WHEN_IS_SET_TO_TRUE_SPECIFYING_AND =
      "When '{}' ('{}', '{}', '{}') is set to true, specifying {} and {} is invalid.";
  public static final String WHEN_OR_IS_SPECIFIED_SPECIFYING_AND_IS =
      "When {}, {}, {} or {} is specified, specifying {}, {}, {}, {}, {} and {} is invalid.";

  // ===================== SINK =====================

  public static final String ACQUIRE_IOPCITEMMGT_SUCCESSFULLY_INTERFACE_ADDRESS =
      "Acquire IOPCItemMgt successfully! Interface address: {}";
  public static final String ACQUIRE_IOPCSYNCIO_SUCCESSFULLY_INTERFACE_ADDRESS =
      "Acquire IOPCSyncIO successfully! Interface address: {}";
  public static final String ADDED_EVENT_TO_RETRY_QUEUE = "Added event {} to retry queue.";
  public static final String BATCH_ID_CREATE_BATCH_DIR_SUCCESSFULLY_BATCH =
      "Batch id = {}: Create batch dir successfully, batch file dir = {}.";
  public static final String BATCH_ID_DELETE_THE_TSFILE_AFTER_FAILED =
      "Batch id = {}: {} delete the tsfile {} after failed to write tablets into {}. {}";
  public static final String MESSAGE_MAYBE_THE_TSFILE_NEEDS_TO_BE_DELETED_MANUALLY_342E28E2 =
      "Maybe the tsfile needs to be deleted manually.";
  public static final String BATCH_ID_FAILED_TO_BUILD_THE_TABLE =
      "Batch id = {}: Failed to build the table model TSFile. Please check whether the written "
          + "Tablet has time overlap and whether the Table Schema is correct.";
  public static final String BATCH_ID_FAILED_TO_CLOSE_THE_TSFILE =
      "Batch id = {}: Failed to close the tsfile {} after failed to write tablets into, because {}";
  public static final String BATCH_ID_FAILED_TO_CLOSE_THE_TSFILE_1 =
      "Batch id = {}: Failed to close the tsfile {} when trying to close batch, because {}";
  public static final String BATCH_ID_FAILED_TO_CREATE_BATCH_FILE =
      "Batch id = {}: Failed to create batch file dir {}.";
  public static final String BATCH_ID_FAILED_TO_DELETE_THE_TSFILE =
      "Batch id = {}: Failed to delete the tsfile {} when trying to close batch, because {}";
  public static final String BATCH_ID_FAILED_TO_WRITE_TABLETS_INTO =
      "Batch id = {}: Failed to write tablets into tsfile, because {}";
  public static final String BATCH_ID_SEAL_TSFILE_SUCCESSFULLY =
      "Batch id = {}: Seal tsfile {} successfully.";
  public static final String BATCH_ID_UNSUPPORTED_EVENT_TYPE_WHEN_CONSTRUCTING =
      "Batch id = {}: Unsupported event {} type {} when constructing tsfile batch";
  public static final String CANNOT_INCREASE_REFERENCE_COUNT_FOR_EVENT_IGNORE =
      "Cannot increase reference count for event: {}, ignore it in batch.";
  public static final String CANNOT_SERIALIZE_BOTH_TABLET_AND_STATEMENT_ARE =
      "Cannot serialize: both tablet and statement are null";
  public static final String CERTIFICATE_DIRECTORY_IS_PLEASE_MOVE_CERTIFICATES_FROM =
      "Certificate directory is: {}, Please move certificates from the reject dir to the "
          + "trusted directory to allow encrypted access";
  public static final String CLIENT_HAS_BEEN_RETURNED_TO_THE_POOL =
      "Client has been returned to the pool. Current handler status is {}. Will not transfer {}.";
  public static final String CLOSED_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTES =
      "Closed AsyncPipeDataTransferServiceClientManager for receiver attributes: {}";
  public static final String CREATE_GROUP_SUCCESSFULLY_SERVER_HANDLE_UPDATE_RATE =
      "Create group successfully! Server handle: {}, update rate: {} ms";
  public static final String DELETENODETRANSFER_NO_EVENT_SUCCESSFULLY_PROCESSED =
      "DeleteNodeTransfer: no.{} event successfully processed!";
  public static final String DESERIALIZE_PIPEDATA_ERROR_BECAUSE_UNKNOWN_TYPE =
      "Deserialize PipeData error because Unknown type ";
  public static final String DESERIALIZE_PIPEDATA_ERROR_BECAUSE_UNKNOWN_TYPE_1 =
      "Deserialize PipeData error because Unknown type {}.";
  public static final String ERROR_GETTING_OPC_CLIENT = "Error getting opc client: ";
  public static final String ERROR_PROGID_IS_INVALID_OR_UNREGISTERED_HRESULT =
      "Error: ProgID is invalid or unregistered, (HRESULT=0x";
  public static final String ERROR_RUNNING_OPC_CLIENT = "Error running opc client: ";
  public static final String EXCEPTION_OCCURRED_WHEN_PIPETABLEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO =
      "Exception occurred when PipeTableModelTsFileBuilderV2 writing tablets to tsfile, use "
          + "fallback tsfile builder: {}";
  public static final String EXCEPTION_OCCURRED_WHEN_PIPETREEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO =
      "Exception occurred when PipeTreeModelTsFileBuilderV2 writing tablets to tsfile, use "
          + "fallback tsfile builder: {}";
  public static final String EXECUTE_STATEMENT_TO_DATABASE_SKIP_BECAUSE_NO =
      "Execute statement {} to database {}, skip because no permission.";
  public static final String FAILED_TO_ACQUIRE_IOPCITEMMGT_ERROR_CODE_0X =
      "Failed to acquire IOPCItemMgt, error code: 0x";
  public static final String FAILED_TO_ACQUIRE_IOPCSYNCIO_ERROR_CODE_0X =
      "Failed to acquire IOPCSyncIO, error code: 0x";
  public static final String FAILED_TO_ADD_ITEM = "Failed to add item ";
  public static final String FAILED_TO_ADD_ITEM_WIN_ERROR_CODE =
      "Failed to add item, win error code: 0x";
  public static final String FAILED_TO_ADJUST_TIMEOUT_WHEN_FAILED_TO =
      "Failed to adjust timeout when failed to transfer file.";
  public static final String FAILED_TO_BORROW_CLIENT_FOR_CACHED_LEADER =
      "failed to borrow client {}:{} for cached leader.";
  public static final String HANDSHAKE_ERROR_WITH_RECEIVER =
      "Handshake error with receiver {}:{}, code: {}, message: {}.";
  public static final String HANDSHAKE_ERROR_WITH_RECEIVER_1 =
      "Handshake error with receiver {}:{}.";
  public static final String HANDSHAKE_ERROR_BY_HANDSHAKE_V2_RETRY_WITH_V1 =
      "Handshake error by PipeTransferHandshakeV2Req with receiver {}:{} retry to handshake by "
          + "PipeTransferHandshakeV1Req.";
  public static final String FAILED_TO_BUILD_AND_STARTUP_OPCUASERVER =
      "Failed to build and startup OpcUaServer";
  public static final String FAILED_TO_CLOSE_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTE =
      "Failed to close AsyncPipeDataTransferServiceClientManager for receiver attributes: {}";
  public static final String FAILED_TO_CLOSE_CLIENT_AFTER_HANDSHAKE_FAILURE =
      "Failed to close client {}:{} after handshake failure when the manager is closed.";
  public static final String FAILED_TO_CLOSE_CLIENT_MANAGER = "Failed to close client manager.";
  public static final String FAILED_TO_CLOSE_FILE_READER_OR_DELETE =
      "Failed to close file reader or delete tsFile when failed to transfer file.";
  public static final String FAILED_TO_CLOSE_FILE_READER_OR_DELETE_1 =
      "Failed to close file reader or delete tsFile when successfully transferred file.";
  public static final String FAILED_TO_CLOSE_FILE_READER_WHEN_SUCCESSFULLY =
      "Failed to close file reader when successfully transferred mod file.";
  public static final String FAILED_TO_CLOSE_OR_INVALIDATE_CLIENT_WHEN =
      "Failed to close or invalidate client when connector is closed. Client: {}, Exception: {}";
  public static final String FAILED_TO_CLOSE_TRUSTLISTMANAGER_BECAUSE =
      "Failed to close trustListManager, because {}.";
  public static final String FAILED_TO_CONNECT_TO_SERVER_ERROR_CODE =
      "Failed to connect to server, error code: 0x";
  public static final String FAILED_TO_CONVERT_STATEMENT_TO_TABLET =
      "Failed to convert statement to tablet.";
  public static final String FAILED_TO_CONVERT_STATEMENT_TO_TABLET_FOR =
      "Failed to convert statement to tablet for serialization";
  public static final String FAILED_TO_CREATE_GROUP_ERROR_CODE_0X =
      "Failed to create group，error code: 0x";
  public static final String FAILED_TO_CREATE_NODES_AFTER_TRANSFER_DATA =
      "Failed to create nodes after transfer data value, creation status: ";
  public static final String FAILED_TO_DELETE_BATCH_FILE_THIS_FILE =
      "Failed to delete batch file {}, this file should be deleted manually later";
  public static final String FAILED_TO_GET_THE_SIZE_OF_PIPETRANSFERBATCHREQBUILDER =
      "Failed to get the size of PipeTransferBatchReqBuilder, return 0. Exception: {}";
  public static final String FAILED_TO_HANDSHAKE = "Failed to handshake.";
  public static final String FAILED_TO_LOG_ERROR_WHEN_FAILED_TO =
      "Failed to log error when failed to transfer file.";
  public static final String FAILED_TO_PUSH_VALUE_CHANGE_TO_CLIENT =
      "Failed to push value change to client, nodeId={}";
  public static final String FAILED_TO_SEND_INITIAL_VALUE_TO_NEW =
      "Failed to send initial value to new subscription, nodeId={}";
  public static final String FAILED_TO_SERIALIZE_PROGRESS_INDEX =
      "Failed to serialize progress index {}";
  public static final String FAILED_TO_SHUTDOWN_EXECUTOR = "Failed to shutdown executor {}.";
  public static final String FAILED_TO_TRANSFER_DATAVALUE = "Failed to transfer dataValue";
  public static final String FAILED_TO_TRANSFER_DATAVALUE_AFTER_SUCCESSFULLY_CREATED =
      "Failed to transfer dataValue after successfully created nodes";
  public static final String FAILED_TO_TRANSFER_PIPEDELETENODEEVENT_COMMITTER_KEY_REPLICATE =
      "Failed to transfer PipeDeleteNodeEvent {} (committer key={}, replicate index={}).";
  public static final String FAILED_TO_TRANSFER_SLICE_RETRY_WHOLE_TRANSFER =
      "Failed to transfer slice. Origin req: {}-{}. Retry the whole transfer.";
  public static final String FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_COMMITTER_KEY_REPLICATE =
      "Failed to transfer TabletInsertionEvent {} (committer key={}, replicate index={}).";
  public static final String FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_COMMITTER_KEY_COMMIT_ID =
      "Failed to transfer TabletInsertionEvent {} (committer key={}, commit id={}).";
  public static final String FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_BATCH =
      "Failed to transfer TabletInsertionEvent batch. Total failed events: {}, related pipe "
          + "names: {}";
  public static final String FAILED_TO_TRANSFER_TSFILE_BATCH =
      "Failed to transfer tsfile batch ({}).";
  public static final String FAILED_TO_TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY =
      "Failed to transfer tsfile event {} asynchronously.";
  public static final String FAILED_TO_TRANSFER_TSFILEINSERTIONEVENT_COMMITTER_KEY_COMMIT_ID =
      "Failed to transfer TsFileInsertionEvent {} (committer key {}, commit id {}).";
  public static final String FAILED_TO_TRANSFER_TSFILEINSERTIONEVENT_BATCHED_TABLE_EVENTS =
      "Failed to transfer TsFileInsertionEvent {} (batched TableInsertionEvents).";
  public static final String FAILED_TO_UPDATE_LEADER_CACHE_FOR_DEVICE =
      "Failed to update leader cache for device {} with endpoint {}:{}.";
  public static final String FAILED_TO_WRITE = "Failed to write ";
  public static final String FAILED_TO_WRITE_WIN_ERROR_CODE_0X =
      "Failed to write, win error code: 0x";
  public static final String GENERATE_STATEMENT_FROM_TABLET_ERROR =
      "Generate Statement from tablet {} error.";
  public static final String GOT_AN_ERROR_FROM = "Got an error \\\"{}\\\" from {}:{}.";
  public static final String GOT_AN_ERROR_FROM_AN_UNKNOWN_CLIENT =
      "Got an error \\\"{}\\\" from an unknown client.";
  public static final String HANDSHAKE_SUCCESSFULLY_WITH_RECEIVER =
      "Handshake successfully with receiver {}:{}.";
  public static final String ILLEGAL_STATE_WHEN_RETURN_THE_CLIENT_TO =
      "Illegal state when return the client to object pool, maybe the pool is already cleared. "
          + "Will ignore.";
  public static final String INSERTNODETRANSFER_NO_EVENT_SUCCESSFULLY_PROCESSED =
      "InsertNodeTransfer: no.{} event successfully processed!";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_HANDSHAKE_RESPONSE =
      "Interrupted while waiting for handshake response.";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTConsensusV2AsyncConnector does not support transferring generic event: {}.";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFER_GENERIC_EVENT =
      "IoTConsensusV2AsyncConnector does not support transfer generic event: {}.";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVEN =
      "IoTConsensusV2AsyncConnector only support PipeTsFileInsertionEvent. Current event: {}.";
  public static final String IOTCONSENSUSV2CONNECTOR_TRANSFERBUFFER_QUEUE_OFFER_IS_INTERRUPTED =
      "IoTConsensusV2Connector transferBuffer queue offer is interrupted.";
  public static final String IOTCONSENSUSV2TRANSFERBATCHREQBUILDER_THE_MAX_BATCH_SIZE_IS_ADJUSTED =
      "IoTConsensusV2TransferBatchReqBuilder: the max batch size is adjusted from {} to {} due "
          + "to the memory restriction";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_NOT_FOUND_IN_TRANSFERBUFFER =
      "IoTConsensusV2-ConsensusGroup-{}: event-{} not found in transferBuffer, skip removing. "
          + "queue size = {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_REPLICATE_INDEX_TRANSFER_FAILED =
      "IoTConsensusV2-ConsensusGroup-{}: Event {} replicate index {} transfer failed, added to "
          + "retry queue failed, this event will be ignored.";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_REPLICATE_INDEX_TRANSFER_FAILED_1 =
      "IoTConsensusV2-ConsensusGroup-{}: Event {} replicate index {} transfer failed, will be "
          + "added to retry queue.";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_NO_EVENT_ADDED_TO_CONNECTOR =
      "IoTConsensusV2-ConsensusGroup-{}: no.{} event-{} added to connector buffer";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_ONE_EVENT_SUCCESSFULLY_RECEIVED_BY =
      "IoTConsensusV2-ConsensusGroup-{}: one event-{} successfully received by the follower, "
          + "will be removed from queue, queue size = {}, limit size = {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_RETRYEVENTQUEUE_IS_NOT_EMPTY_AFTER =
      "IoTConsensusV2-ConsensusGroup-{}: retryEventQueue is not empty after 20 seconds. "
          + "retryQueue size: {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_RETRY_WITH_INTERVAL_FOR_INDEX =
      "IoTConsensusV2-ConsensusGroup-{}: retry with interval {} for index {} {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_TRY_TO_REMOVE_EVENT_AFTER =
      "IoTConsensusV2-ConsensusGroup-{}: try to remove event-{} after "
          + "iotConsensusV2AsyncConnector being closed. Ignore it.";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN =
      "IoTConsensusV2-{}: Failed to close file reader when failed to transfer file.";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN_1 =
      "IoTConsensusV2-{}: Failed to close file reader when successfully transferred file.";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN_2 =
      "IoTConsensusV2-{}: Failed to close file reader when successfully transferred mod file.";
  public static final String IOTCONSENSUSV2_FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_BATCH_TOTAL =
      "IoTConsensusV2: Failed to transfer TabletInsertionEvent batch. Total failed events: {}, "
          + "related pipe names: {}";
  public static final String IOTCONSENSUSV2_FAILED_TO_TRANSFER_TSFILEINSERTIONEVENT_COMMITTER_KEY =
      "IoTConsensusV2-{}: Failed to transfer TsFileInsertionEvent {} (committer key {}, "
          + "replicate index {}).";
  public static final String IOTCONSENSUSV2_REDIRECT_FILE_POSITION_TO =
      "IoTConsensusV2-{}: Redirect file position to {}.";
  public static final String IOTCONSENSUSV2_SUCCESSFULLY_TRANSFERRED_FILE_COMMITTER_KEY_REPLICATE =
      "IoTConsensusV2-{}: Successfully transferred file {} (committer key={}, replicate index={}).";
  public static final String IOTDBCDCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTAB =
      "IoTDBCDCConnector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent.";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBDataRegionAirGapConnector does not support transferring generic event: {}.";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_A =
      "IoTDBDataRegionAirGapConnector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Ignore {}.";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE =
      "IoTDBDataRegionAirGapConnector only support PipeTsFileInsertionEvent. Ignore {}.";
  public static final String FAILED_TO_LOGIN_TO_RECEIVER_FOR_LEGACY_PIPE_TRANSFER =
      "Failed to login to receiver %s:%s for legacy pipe transfer because code: %d, message: %s";
  public static final String IOTDBLEGACYPIPECONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBLegacyPipeConnector does not support transferring generic event: {}.";
  public static final String IOTDBLEGACYPIPECONNECTOR_ONLY_SUPPORT_PIPEINSERTNODEINSERTIONEVENT_AND_PIPETABLE =
      "IoTDBLegacyPipeConnector only support PipeInsertNodeInsertionEvent and "
          + "PipeTabletInsertionEvent.";
  public static final String IOTDBLEGACYPIPECONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT =
      "IoTDBLegacyPipeConnector only support PipeTsFileInsertionEvent.";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBSchemaRegionAirGapSink can't transfer TabletInsertionEvent.";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBSchemaRegionAirGapSink can't transfer TsFileInsertionEvent.";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBSchemaRegionAirGapSink does not support transferring generic event: {}.";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBSchemaRegionConnector can't transfer TabletInsertionEvent.";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBSchemaRegionConnector can't transfer TsFileInsertionEvent.";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBSchemaRegionConnector does not support transferring generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBThriftAsyncConnector does not support transferring generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFER_GENERIC_EVENT =
      "IoTDBThriftAsyncConnector does not support transfer generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PI =
      "IoTDBThriftAsyncConnector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Current event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVENT =
      "IoTDBThriftAsyncConnector only support PipeTsFileInsertionEvent. Current event: {}.";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBThriftSyncConnector does not support transferring generic event: {}.";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIP =
      "IoTDBThriftSyncConnector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Ignore {}.";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE =
      "IoTDBThriftSyncConnector only support PipeTsFileInsertionEvent. Ignore {}.";
  public static final String LEADERCACHEMANAGER_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_FROM_TO =
      "LeaderCacheManager.allocatedMemoryBlock has expanded from {} to {}.";
  public static final String LEADERCACHEMANAGER_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_FROM_TO =
      "LeaderCacheManager.allocatedMemoryBlock has shrunk from {} to {}.";
  public static final String LOADING_KEYSTORE_AT = "Loading KeyStore at {}";
  public static final String LOADING_KEYSTORE_AT_1 = "Loading KeyStore at {}.";
  public static final String LOAD_KEYSTORE_FAILED_THE_EXISTING_KEYSTORE_MAY =
      "Load keyStore failed, the existing keyStore may be stale, re-constructing...";
  public static final String NO_OPC_CLIENT_OR_SERVER_IS_SPECIFIED =
      "No OPC client or server is specified when transferring tablet";
  public static final String OPC_DA_SINK_MUST_RUN_ON_WINDOWS =
      "opc-da-sink must run on windows system.";
  public static final String PIPETABLEMODETSFILEBUILDERV2_DOES_NOT_SUPPORT_TREE_MODEL_TABLET =
      "PipeTableModeTsFileBuilderV2 does not support tree model tablet to build TSFile";
  public static final String PIPETABLEMODETSFILEBUILDER_DOES_NOT_SUPPORT_TREE_MODEL_TABLET =
      "PipeTableModeTsFileBuilder does not support tree model tablet to build TSFile";
  public static final String PIPETREEMODELTSFILEBUILDERV2_DOES_NOT_SUPPORT_TABLE_MODEL_TABLET =
      "PipeTreeModelTsFileBuilderV2 does not support table model tablet to build TSFile";
  public static final String PIPETREEMODELTSFILEBUILDER_DOES_NOT_SUPPORT_TABLE_MODEL_TABLET =
      "PipeTreeModelTsFileBuilder does not support table model tablet to build TSFile";
  public static final String POLLED_EVENT_FROM_RETRY_QUEUE = "Polled event {} from retry queue.";
  public static final String RECEIVED_AN_ERROR_MESSAGE_FROM =
      "Received an error message {} from {}:{}";
  public static final String RECEIVED_AN_UNKNOWN_MESSAGE_FROM =
      "Received an unknown message {} from {}:{}";
  public static final String RECEIVED_A_ACK_MESSAGE_FROM = "Received a ack message from {}:{}";
  public static final String RECEIVED_A_BIND_MESSAGE_FROM = "Received a bind message from {}:{}";
  public static final String REDIRECT_FILE_POSITION_TO = "Redirect file position to {}.";
  public static final String REDIRECT_TO_POSITION_IN_TRANSFERRING_TSFILE =
      "Redirect to position {} in transferring tsFile {}.";
  public static final String NETWORK_FAILED_TO_RECEIVE_TSFILE_STATUS =
      "Network failed to receive tsFile %s, status: %s";
  public static final String SECURITY_DIR = "security dir: {}";
  public static final String SECURITY_PKI_DIR = "security pki dir: {}";
  public static final String SUCCESSFULLY_ADDED_ITEM = "Successfully added item {}.";
  public static final String SUCCESSFULLY_CONVERTED_PROGID_TO_CLSID =
      "Successfully converted progID {} to CLSID: {{}}";
  public static final String SUCCESSFULLY_SHUTDOWN_EXECUTOR = "Successfully shutdown executor {}.";
  public static final String SUCCESSFULLY_TRANSFERRED_DELETION_EVENT =
      "Successfully transferred deletion event {}.";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE = "Successfully transferred file {}.";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_AND =
      "Successfully transferred file {}, {} and {}.";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_BATCHED_TABLEINSERTIONEVENTS_REFERENCE_COUNT =
      "Successfully transferred file {} (batched TableInsertionEvents, reference count={}).";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_COMMITTER_KEY_COMMIT_ID =
      "Successfully transferred file {} (committer key={}, commit id={}, reference count={}).";
  public static final String SUCCESSFULLY_TRANSFERRED_SCHEMA_EVENT =
      "Successfully transferred schema event {}.";
  public static final String SUCCESSFULLY_TRANSFERRED_SCHEMA_REGION_SNAPSHOT_AND =
      "Successfully transferred schema region snapshot {}, {} and {}.";
  public static final String THE_BATCH_SIZE_LIMIT_HAS_EXPANDED_FROM =
      "The batch size limit has expanded from {} to {}.";
  public static final String THE_BATCH_SIZE_LIMIT_HAS_SHRUNK_FROM =
      "The batch size limit has shrunk from {} to {}.";
  public static final String THE_DEFAULT_QUALITY_CAN_ONLY_BE_GOOD =
      "The default quality can only be 'GOOD', 'BAD' or 'UNCERTAIN'.";
  public static final String THE_EVENT_ACK_IS_NOT_FOUND = "The event ack {} is not found.";
  public static final String THE_EVENT_CAN_T_BE_TRANSFERRED_TO =
      "The event {} can't be transferred to client, it will be retried later.";
  public static final String THE_EVENT_IN_ERROR_IS_NOT_FOUND =
      "The event in error {} is not found.";
  public static final String THE_EVENT_POLLED_FROM_THE_QUEUE_IS =
      "The event polled from the queue is not the same as the event peeked from the queue. "
          + "Peeked event: {}, polled event: {}.";
  public static final String THE_FILE_IS_NOT_FOUND_MAY_ALREADY =
      "The file {} is not found, may already be deleted.";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT =
      "The pipe {} was dropped so the event ack {} will be ignored.";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT_1 =
      "The pipe {} was dropped so the event in error {} will be ignored.";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT_2 =
      "The pipe {} was dropped so the event {} will be dropped.";
  public static final String THE_QUALITY_VALUE_ONLY_SUPPORTS_BOOLEAN_TYPE =
      "The quality value only supports boolean type, while true == GOOD and false == BAD.";
  public static final String THE_SCHEMA_REGION_AIR_GAP_CONNECTOR_DOES =
      "The schema region air gap connector does not support transferring single file piece bytes.";
  public static final String THE_SCHEMA_REGION_CONNECTOR_DOES_NOT_SUPPORT =
      "The schema region connector does not support transferring single file piece req.";
  public static final String THE_SECURITY_POLICY_CANNOT_BE_EMPTY =
      "The security policy cannot be empty.";
  public static final String THE_SECURITY_POLICY_CAN_ONLY_BE_NONE =
      "The security policy can only be 'None', 'Basic128Rsa15', 'Basic256', 'Basic256Sha256', "
          + "'Aes128_Sha256_RsaOaep' or 'Aes256_Sha256_RsaPss'.";
  public static final String THE_SEGMENTS_OF_TABLETS_MUST_EXIST =
      "The segments of tablets must exist";
  public static final String THE_TABLET_OF_COMMITID_CAN_T_BE =
      "The tablet of commitId: {} can't be parsed by client, it will be retried later.";
  public static final String THE_TRANSFER_THREAD_IS_INTERRUPTED =
      "The transfer thread is interrupted.";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN =
      "The websocket connection from client has been closed!The code is {}. The reason is {}. "
          + "Is it closed by remote? {}";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN_1 =
      "The websocket connection from client {}:{} has been closed! The code is {}. The reason "
          + "is {}. Is it closed by remote? {}";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN_2 =
      "The websocket connection from client {}:{} has been opened!";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_HAS_BEEN_CLOSED =
      "The websocket connection from {}:{} has been closed, but the ack message of commitId: "
          + "{} is received.";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_HAS_BEEN_CLOSED_1 =
      "The websocket connection from {}:{} has been closed, but the error message of commitId: "
          + "{} is received.";
  public static final String THE_WEBSOCKET_SERVER_HAS_BEEN_STARTED =
      "The websocket server {}:{} has been started!";
  public static final String THE_WRITTEN_TABLET_TIME_MAY_OVERLAP_OR =
      "The written Tablet time may overlap or the Schema may be incorrect";
  public static final String THIS_CONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLET =
      "This Connector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Ignore {}.";
  public static final String TIMED_OUT_WHEN_WAITING_FOR_CLIENT_HANDSHAKE =
      "Timed out when waiting for client handshake finish.";
  public static final String TIOTCONSENSUSV2BATCHTRANSFERRESP_IS_NULL =
      "TIoTConsensusV2BatchTransferResp is null";
  public static final String TIOTCONSENSUSV2TRANSFERRESP_IS_NULL =
      "TIoTConsensusV2TransferResp is null";
  public static final String TPIPETRANSFERRESP_IS_NULL = "TPipeTransferResp is null";
  public static final String OPC_UA_SINK_MODEL_MUST_BE_CLIENT_SERVER_WHEN_OUTER_OR_WITH_QUALITY =
      "When the OPC UA sink points to an outer server or sets 'with-quality' to true, the %s or "
          + "%s must be %s.";
  public static final String WITH_QUALITY_MEASUREMENT_MUST_BE_VALUE_OR_QUALITY_NAME =
      "When the 'with-quality' mode is enabled, the measurement must be either \"value-name\" or "
          + "\"quality-name\"";
  public static final String SESSION_FAILED_TO_CHECK_AUTHORITY_FOR_STATEMENT =
      "Session {}: Failed to check authority for statement {}, username = {}, response = {}.";
  public static final String TRANSFER_REQUEST_BODY_TOO_LARGE_WILL_BE_SLICED =
      "The body size of the request is too large. The request will be sliced. Origin req: {}-{}. "
          + "Request body size: {}, threshold: {}";
  public static final String TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY_WAS_INTERRUPTED =
      "Transfer tsfile event {} asynchronously was interrupted.";
  public static final String UNABLE_TO_CREATE_SECURITY_DIR = "unable to create security dir: ";
  public static final String UNKNOWN_LOAD_BALANCE_STRATEGY_USE_ROUND_ROBIN =
      "Unknown load balance strategy: {}, use round-robin strategy instead.";
  public static final String UNSUPPORTED_BATCH_TYPE = "Unsupported batch type {}.";
  public static final String UNSUPPORTED_BATCH_TYPE_WHEN_TRANSFERRING_TABLET_INSERTION =
      "Unsupported batch type {} when transferring tablet insertion event.";
  public static final String UNSUPPORTED_DATATYPE = "UnSupported dataType ";
  public static final String UNSUPPORTED_EVENT_TYPE_WHEN_BUILDING_TRANSFER_REQUEST =
      "Unsupported event {} type {} when building transfer request";
  public static final String WAIT_FOR_RESOURCE_ENOUGH_FOR_SLICING_TSFILE =
      "Wait for resource enough for slicing tsfile {} for {} seconds.";
  public static final String WEBSOCKETCONNECTOR_FAILED_TO_INCREASE_THE_REFERENCE_COUNT =
      "WebsocketConnector failed to increase the reference count of the event. Ignore it. "
          + "Current event: {}.";
  public static final String WEBSOCKETCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTA =
      "WebsocketConnector only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Current event: {}.";
  public static final String WEBSOCKETCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVENT =
      "WebsocketConnector only support PipeTsFileInsertionEvent. Current event: {}.";
  public static final String WHEN_THE_OPC_UA_SINK_POINTS_TO =
      "When the OPC UA sink points to an outer server, the table model data is not supported.";
  public static final String WHEN_THE_OPC_UA_SINK_SETS_WITH =
      "When the OPC UA sink sets 'with-quality' to true, the table model data is not supported.";
  public static final String WRITEBACKSINK_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLETI =
      "WriteBackSink only support PipeInsertNodeTabletInsertionEvent and "
          + "PipeRawTabletInsertionEvent. Ignore {}.";

  // ===================== RECEIVER =====================

  public static final String ALL_RECEIVERS_RELATED_TO_ARE_RELEASED =
      "All Receivers related to {} are released.";
  public static final String AUTO_CREATE_DATABASE_FAILED_BECAUSE =
      "Auto create database failed because: ";
  public static final String CREATE_DATABASE_ERROR_STATEMENT_RESULT_STATUS =
      "Create Database error, statement: {}, result status : {}.";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_FOR_LOADTSFILESTATEMENT =
      "Database name is unexpectedly null for LoadTsFileStatement: {}. Skip data type conversion.";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_FOR_STATEMENT =
      "Database name is unexpectedly null for statement: {}. Skip data type conversion.";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_SKIP_DATA_TYPE_CONVERSION =
      "Pipe: Database name is unexpectedly null. Skip data type conversion.";
  public static final String DATA_TYPE_CONVERSION_FOR_LOADTSFILESTATEMENT_IS_SUCCESSFUL =
      "Data type conversion for LoadTsFileStatement {} is successful.";
  public static final String DATA_TYPE_MISMATCH_DETECTED_TSSTATUS_FOR_LOADTSFILESTATEMENT =
      "Data type mismatch detected (TSStatus: {}) for LoadTsFileStatement: {}. Start data type "
          + "conversion.";
  public static final String DELETE_ERROR_STATEMENT = "Delete {} error, statement: {}.";
  public static final String DELETE_RESULT_STATUS = "Delete result status : {}.";
  public static final String FAILED_TO_CLOSE_IOTDBAIRGAPRECEIVERAGENT_S_SERVER_SOCKET =
      "Failed to close IoTDBAirGapReceiverAgent's server socket";
  public static final String FAILED_TO_CONVERT_DATA_TYPE_FOR_LOADTSFILESTATEMENT =
      "Failed to convert data type for LoadTsFileStatement: {}.";
  public static final String FAILED_TO_EXECUTE_STATEMENT_AFTER_DATA_TYPE =
      "Failed to execute statement after data type conversion.";
  public static final String
      FAILED_TO_EXECUTE_STATEMENT_AFTER_DATA_TYPE_CONVERSION_WITH_EXCEPTION_TYPE =
          "Pipe: Failed to execute statement after data type conversion. Exception type: {}.";
  public static final String FAILED_TO_PARSE_ROW_VALUE_DURING_DATA_TYPE_CONVERSION =
      "Pipe: Failed to parse row value during data type conversion. Registered type {}.";
  public static final String FAILED_TO_HANDLE_CONFIG_CLIENT_ID_EXIT =
      "Failed to handle config client (id = {}) exit";
  public static final String FAIL_TO_CREATE_IOTCONSENSUSV2_RECEIVER_FILE_FOLDERS =
      "Fail to create iotConsensusV2 receiver file folders allocation strategy because all "
          + "disks of folders are full.";
  public static final String FAIL_TO_CREATE_PIPE_RECEIVER_FILE_FOLDERS =
      "Fail to create pipe receiver file folders allocation strategy because all disks of "
          + "folders are full.";
  public static final String FAIL_TO_INITIATE_FILE_BUFFER_FOLDER_ERROR =
      "Fail to initiate file buffer folder, Error msg: {}";
  public static final String FAIL_TO_LOAD_PIPEDATA_BECAUSE = "Fail to load pipeData because {}.";
  public static final String FAIL_TO_RENAME_FILE_TO = "Fail to rename file {} to {}";
  public static final String INVOKE_HANDSHAKE_METHOD_FROM_CLIENT_IP =
      "Invoke handshake method from client ip = {}";
  public static final String INVOKE_TRANSPORTDATA_METHOD_FROM_CLIENT_IP =
      "Invoke transportData method from client ip = {}";
  public static final String INVOKE_TRANSPORTPIPEDATA_METHOD_FROM_CLIENT_IP =
      "Invoke transportPipeData method from client ip = {}";
  public static final String IOTCONSENSUSV2RECEIVER_THREAD_IS_INTERRUPTED_WHEN_WAITING_FOR =
      "IoTConsensusV2Receiver thread is interrupted when waiting for receiver get initiated, "
          + "may because system exit.";
  public static final String IOTCONSENSUSV2_PIPENAME = "IoTConsensusV2-PipeName-{}: {}";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WAITING_IS_INTERRUPTED_ONSYNCEDCOMMITINDEX =
      "IoTConsensusV2-PipeName-{}: current waiting is interrupted. onSyncedCommitIndex: {}. "
          + "Exception: ";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WRITING_FILE_WRITER_IS =
      "IoTConsensusV2-PipeName-{}: Current writing file writer is null. No need to close.";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WRITING_FILE_WRITER_WAS =
      "IoTConsensusV2-PipeName-{}: Current writing file writer {} was closed.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CLOSE_CURRENT_WRITING =
      "IoTConsensusV2-PipeName-{}: Failed to close current writing file writer {}, because {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE =
      "IoTConsensusV2-PipeName-{}: Failed to create receiver file dir {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE_1 =
      "IoTConsensusV2-PipeName-{}: Failed to create receiver file dir {}. Because parent "
          + "system dir have been deleted due to system concurrently exit.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE_2 =
      "IoTConsensusV2-PipeName-{}: Failed to create receiver file dir {}. May because "
          + "authority or dir already exists etc.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_TSFILEWRITER =
      "IoTConsensusV2-PipeName-{}: Failed to create receiver tsFileWriter-{} file dir {}";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_DELETE_BECAUSE =
      "IoTConsensusV2-PipeName-{}: {} Failed to delete {}, because {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_GET_BASE_DIRECTORY =
      "IoTConsensusV2-PipeName-{}: Failed to get base directory";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_LOAD_FILE_FROM =
      "IoTConsensusV2-PipeName-{}: Failed to load file {} from req {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_READ_TSFILE_WHEN =
      "IoTConsensusV2-PipeName-{}: Failed to read TsFile when counting points: {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_RETURN_TSFILEWRITER =
      "IoTConsensusV2-PipeName-{}: Failed to return tsFileWriter {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE =
      "IoTConsensusV2-PipeName-{}: Failed to seal file {}, because the file does not exist.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE_1 =
      "IoTConsensusV2-PipeName-{}: Failed to seal file {}, because writing file is {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE_2 =
      "IoTConsensusV2-PipeName-{}: Failed to seal file {}, because {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_FROM =
      "IoTConsensusV2-PipeName-{}: Failed to seal file {} from req {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_STATUS =
      "IoTConsensusV2-PipeName-{}: Failed to seal file {}, status is {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_WHEN =
      "IoTConsensusV2-PipeName-{}: Failed to seal file {} when check final seal file, because "
          + "the length of file is not correct. The original file has length {}, but receiver file "
          + "has length {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_WHEN_1 =
      "IoTConsensusV2-PipeName-{}: Failed to seal file {} when check non final seal, because "
          + "the length of file is not correct. The original file has length {}, but receiver file "
          + "has length {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_WRITE_FILE_PIECE =
      "IoTConsensusV2-PipeName-{}: Failed to write file piece from req {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FILE_OFFSET_RESET_REQUESTED_BY =
      "IoTConsensusV2-PipeName-{}: File offset reset requested by receiver, response status = {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_ILLEGAL_FILE_NAME_WHEN_CHECKING =
      "IoTConsensusV2-PipeName-{}: Illegal file name {} when checking writing file.";
  public static final String IOTCONSENSUSV2_PIPENAME_IS_NOT_EXISTED_NO_NEED =
      "IoTConsensusV2-PipeName-{}: {} {} is not existed. No need to delete.";
  public static final String IOTCONSENSUSV2_PIPENAME_NO_EVENT_GET_EXECUTED_AFTER =
      "IoTConsensusV2-PipeName-{}: no.{} event get executed after awaiting timeout, current "
          + "receiver syncIndex: {}";
  public static final String IOTCONSENSUSV2_PIPENAME_NO_EVENT_GET_EXECUTED_BECAUSE =
      "IoTConsensusV2-PipeName-{}: no.{} event get executed because receiver buffer's len >= "
          + "pipeline, current receiver syncIndex {}, current buffer len {}";
  public static final String IOTCONSENSUSV2_PIPENAME_PATH_TRAVERSAL_ATTEMPT_DETECTED_FILENAME =
      "IoTConsensusV2-PipeName-{}: Path traversal attempt detected! Filename: {}";
  public static final String IOTCONSENSUSV2_PIPENAME_PROCESS_NO_EVENT_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}: process no.{} event successfully!";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVED_A_DEPRECATED_REQUEST_WHICH =
      "IoTConsensusV2-PipeName-{}: received a deprecated request-{}, which may because {}. ";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_DETECTED_AN_NEWER_PIPETASKRESTARTTIMES =
      "IoTConsensusV2-PipeName-{}: receiver detected an newer pipeTaskRestartTimes, which "
          + "indicates the pipe task has restarted. receiver will reset all its data.";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_DETECTED_AN_NEWER_REBOOTTIMES =
      "IoTConsensusV2-PipeName-{}: receiver detected an newer rebootTimes, which indicates the "
          + "leader has rebooted. receiver will reset all its data.";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_FILE_DIR_WAS_CREATED =
      "IoTConsensusV2-PipeName-{}: Receiver file dir {} was created.";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_THREAD_GET_INTERRUPTED_WHEN =
      "IoTConsensusV2-PipeName-{}: receiver thread get interrupted when exiting.";
  public static final String IOTCONSENSUSV2_PIPENAME_SEAL_FILE_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}: Seal file {} successfully.";
  public static final String IOTCONSENSUSV2_PIPENAME_SEAL_FILE_WITH_MODS_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}: Seal file with mods {} successfully.";
  public static final String IOTCONSENSUSV2_PIPENAME_SKIP_LOAD_TSFILE_WHEN_SEALING =
      "IoTConsensusV2-PipeName-{}: skip load tsfile-{} when sealing, because this region has "
          + "been removed or migrated.";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_PIECES =
      "IoTConsensusV2-PipeName-{}: starting to receive tsFile pieces";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_SEAL =
      "IoTConsensusV2-PipeName-{}: starting to receive tsFile seal";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_SEAL_1 =
      "IoTConsensusV2-PipeName-{}: starting to receive tsFile seal with mods";
  public static final String IOTCONSENSUSV2_PIPENAME_START_TO_RECEIVE_NO_EVENT =
      "IoTConsensusV2-PipeName-{}: start to receive no.{} event";
  public static final String IOTCONSENSUSV2_PIPENAME_THE_POINT_COUNT_OF_TSFILE =
      "IoTConsensusV2-PipeName-{}: The point count of TsFile {} is not given by sender, will "
          + "read actual point count from TsFile.";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILEWRITER_RETURNED_SELF =
      "IoTConsensusV2-PipeName-{}: tsFileWriter-{} returned self";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILEWRITER_ROLL_TO_WRITING_PATH =
      "IoTConsensusV2-PipeName-{}: tsfileWriter-{} roll to writing path {}";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILE_WRITER_IS_CLEANED_UP =
      "IoTConsensusV2-PipeName-{}: tsfile writer-{} is cleaned up because no new requests were "
          + "received for too long.";
  public static final String IOTCONSENSUSV2_PIPENAME_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "IoTConsensusV2-PipeName-{}: Unknown PipeRequestType, response status = {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_WAS_DELETED =
      "IoTConsensusV2-PipeName-{}: {} {} was deleted.";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_IS_NOT_AVAILABLE =
      "IoTConsensusV2-PipeName-{}: Writing file {} is not available. Writing file is null: {}, "
          + "writing file exists: {}, writing file writer is null: {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_IS_NOT_EXISTED =
      "IoTConsensusV2-PipeName-{}: Writing file {} is not existed or name is not correct, try "
          + "to create it. Current writing file is {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_S_OFFSET_IS =
      "IoTConsensusV2-PipeName-{}: Writing file {}'s offset is {}, but request sender's offset "
          + "is {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_WAS_CREATED_READY =
      "IoTConsensusV2-PipeName-{}: Writing file {} was created. Ready to write file pieces.";
  public static final String IOTCONSENSUSV2_RECEIVE_ON_THE_FLY_NO_EVENT =
      "IoTConsensusV2-{}: receive on-the-fly no.{} event after data region was deleted, discard it";
  public static final String IOTCONSENSUSV2_TRANSFER_BATCH_HASN_T_BEEN_IMPLEMENTED =
      "IoTConsensusV2 transfer batch hasn't been implemented yet.";
  public static final String IOTCONSENSUSV2_TSFILEWRITER_SET_NULL_WRITING_FILE =
      "IoTConsensusV2-{}: TsFileWriter-{} set null writing file";
  public static final String IOTCONSENSUSV2_TSFILEWRITER_SET_NULL_WRITING_FILE_WRITER =
      "IoTConsensusV2-{}: TsFileWriter-{} set null writing file writer";
  public static final String IOTCONSENSUSV2_UNKNOWN_IOTCONSENSUSV2REQUESTVERSION_RESPONSE_STATUS =
      "IoTConsensusV2: Unknown IoTConsensusV2RequestVersion, response status = {}.";
  public static final String IOTCONSENSUSV2_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "IoTConsensusV2 Unknown PipeRequestType, response status = {}.";
  public static final String IOTCONSENSUSV2_WAITING_FOR_THE_PREVIOUS_EVENT_TIMES =
      "IoTConsensusV2-{}: Waiting for the previous event times out, current peek {}, current id {}";
  public static final String IOTDBAIRGAPRECEIVERAGENT_STARTED =
      "IoTDBAirGapReceiverAgent {} started.";
  public static final String IOTDBAIRGAPRECEIVERAGENT_STOPPED =
      "IoTDBAirGapReceiverAgent {} stopped.";
  public static final String LOAD_ACTIVE_LISTENING_PIPE_DIR_IS_NOT =
      "Load active listening pipe dir is not set.";
  public static final String LOAD_PIPEDATA_WITH_SERIALIZE_NUMBER_SUCCESSFULLY =
      "Load pipeData with serialize number {} successfully.";
  public static final String LOAD_TSFILE_ERROR_STATEMENT = "Load TsFile {} error, statement: {}.";
  public static final String LOAD_TSFILE_RESULT_STATUS = "Load TsFile result status : {}.";
  public static final String PARSE_DATABASE_PARTIALPATH_ERROR =
      "Parse database PartialPath {} error";
  public static final String PIPE_AIR_GAP_RECEIVER_CHECKSUM_FAILED_EXPECTED =
      "Pipe air gap receiver {}: checksum failed, expected: {}, actual: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_OF =
      "Pipe air gap receiver {} closed because of checksum failed. Socket: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_OF_1 =
      "Pipe air gap receiver {} closed because of exception. Socket: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_SOCKET =
      "Pipe air gap receiver {} closed because socket is closed. Socket: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_EXCEPTION_DURING_HANDLING =
      "Pipe air gap receiver {}: Exception during handling receiving. Socket: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_HANDLE_DATA_FAILED =
      "Pipe air gap receiver {}: Handle data failed, status: {}, req: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_SOCKET_CLOSED_WHEN =
      "Pipe air gap receiver {}: Socket {} closed when listening to data. Because: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_STARTED_SOCKET =
      "Pipe air gap receiver {} started. Socket: {}";
  public static final String PIPE_AIR_GAP_RECEIVER_TEMPORARY_UNAVAILABLE_RETRY =
      "Pipe air gap receiver {}: Temporary unavailable retry timed out, returning FAIL to sender.";
  public static final String PIPE_AIR_GAP_RECEIVER_TSSTATUS_IS_ENCOUNTERED =
      "Pipe air gap receiver {}: TSStatus {} is encountered at the air gap receiver, will ignore.";
  public static final String PIPE_DATA_TRANSPORT_ERROR = "Pipe data transport error, {}";
  public static final String PIPE_INSERTING_ROW_CASTING_TYPE_FROM =
      "Pipe: Inserting row. Casting type from {} to {}.";
  public static final String PIPE_INSERTING_TABLET_CASTING_TYPE_FROM =
      "Pipe: Inserting tablet. Casting type from {} to {}.";
  public static final String PIPE_INSERTING_TABLET_TO_CASTING_TYPE_FROM =
      "Pipe: Inserting tablet to {}.{}. Casting type from {} to {}.";
  public static final String RECEIVERS_EXECUTOR_IS_CLOSED = "Receivers-{}' executor is closed.";
  public static final String RECEIVER_EXIT_SUCCESSFULLY = "Receiver-{} exit successfully.";
  public static final String RECEIVER_ID = "Receiver id = {}: {}";
  public static final String RECEIVER_ID_THE_NUMBER_OF_DEVICE_PATHS =
      "Receiver id = {}: The number of device paths is not equal to sub-status in statement "
          + "{}: {}.";
  public static final String RECEIVER_ID_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "Receiver id = {}: Unknown PipeRequestType, response status = {}.";
  public static final String RECEIVER_ID_UNSUPPORTED_STATEMENT_TYPE_FOR_REDIRECTION =
      "Receiver id = {}: Unsupported statement type {} for redirection.";
  public static final String RECEIVER_ID_FAILED_TO_CHECK_AUTHORITY_FOR_STATEMENT =
      "Receiver id = {}: Failed to check authority for statement {}, username = {}, response = {}.";
  public static final String RECEIVER_ID_FAILURE_STATUS_WHILE_EXECUTING_STATEMENT =
      "Receiver id = {}: Failure status encountered while executing statement {}: {}";
  public static final String RECEIVER_ID_EXCEPTION_WHILE_EXECUTING_STATEMENT =
      "Receiver id = {}: Exception encountered while executing statement {}: ";
  public static final String RECEIVER_ID_STATEMENT_EXCEPTION_MESSAGE =
      "Receiver id = {}, statement = {}, exception = {}, message = {}";
  public static final String UNKNOWN_PIPEREQUESTTYPE = "Unknown PipeRequestType %s.";
  public static final String EXCEPTION_ENCOUNTERED_WHILE_HANDLING_REQUEST =
      "Exception %s encountered while handling request %s.";
  public static final String RECEIVER_IS_READY = "Receiver-{} is ready";
  public static final String RECEIVER_TEMPORARILY_OUT_OF_MEMORY_FORMAT =
      "Temporarily out of memory when %s. Requested memory: %d bytes, used memory: %d bytes, "
          + "free memory: %d bytes, total non-floating memory: %d bytes";
  public static final String REGISTER_WITH_INTERVAL_IN_SECONDS_SUCCESSFULLY =
      "Register {} with interval in seconds {} successfully.";
  public static final String SOCKET_CLOSED_WHEN_EXECUTING_READTILLFULL =
      "Socket closed when executing readTillFull.";
  public static final String SOCKET_CLOSED_WHEN_EXECUTING_SKIPTILLENOUGH =
      "Socket closed when executing skipTillEnough.";
  public static final String START_LOAD_PIPEDATA_WITH_SERIALIZE_NUMBER_AND =
      "Start load pipeData with serialize number {} and type {},value={}";
  public static final String STORAGE_ENGINE_READONLY = "storage engine readonly";
  public static final String SYNC_START_AT_TO_IS_DONE = "Sync {} start at {} to {} is done.";
  public static final String TEMPORARY_UNAVAILABLE_EXCEPTION_ENCOUNTERED_AT_AIR_GAP =
      "Temporary unavailable exception encountered at air gap receiver, will retry locally.";
  public static final String THE_IOTCONSENSUSV2_REQUEST_VERSION_IS_DIFFERENT_FROM =
      "The iotConsensusV2 request version {} is different from the sender request version {}, "
          + "the receiver will be reset to the sender request version.";
  public static final String THE_START_INDEX_OF_DATA_SYNC_IS =
      "The start index {} of data sync is not valid. The file is not exist and start index "
          + "should equal to 0).";
  public static final String THE_START_INDEX_OF_DATA_SYNC_IS_1 =
      "The start index {} of data sync is not valid. The start index of the file should equal "
          + "to {}.";
  public static final String THRIFT_CONNECTION_IS_NOT_ALIVE = "Thrift connection is not alive.";
  public static final String TSFILECHECKER_DID_NOT_TERMINATE_WITHIN_S =
      "TsFileChecker did not terminate within {}s";
  public static final String TSFILECHECKER_THREAD_STILL_DOESN_T_EXIT_AFTER =
      "TsFileChecker Thread {} still doesn't exit after 30s";
  public static final String UNHANDLED_EXCEPTION_DURING_PIPE_AIR_GAP_RECEIVER =
      "Unhandled exception during pipe air gap receiver listening";
  public static final String UNSUPPORTED_DATA_TYPE = "Unsupported data type: ";

  // ===================== RESOURCE =====================

  public static final String CANNOT_GET_DATA_REGION_IDS_USE_DEFAULT =
      "Cannot get data region ids, use default lock segment size: {}";
  public static final String EXPAND_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "Expand callback is not supported in PipeFixedMemoryBlock";
  public static final String EXPAND_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "Expand method is not supported in PipeFixedMemoryBlock";
  public static final String FAILED_TO_CACHEDEVICEISALIGNEDMAPIFABSENT_FOR_TSFILE_BECAUSE_MEMORY =
      "Failed to cacheDeviceIsAlignedMapIfAbsent for tsfile {}, because memory usage is high";
  public static final String FAILED_TO_CACHEOBJECTSIFABSENT_FOR_TSFILE_BECAUSE_MEMORY =
      "Failed to cacheObjectsIfAbsent for tsfile {}, because memory usage is high";
  public static final String FAILED_TO_ESTIMATE_SIZE_FOR_INSERTNODE =
      "Failed to estimate size for InsertNode: {}";
  public static final String FAILED_TO_EXECUTE_THE_EXPAND_CALLBACK =
      "Failed to execute the expand callback.";
  public static final String FAILED_TO_EXECUTE_THE_SHRINK_CALLBACK =
      "Failed to execute the shrink callback.";
  public static final String FAILED_TO_GET_FILE_SIZE_OF_LINKED =
      "failed to get file size of linked TsFile {}: ";
  public static final String FORCEALLOCATEWITHRETRY_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceAllocateWithRetry: interrupted while waiting for available memory";
  public static final String FORCEALLOCATE_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceAllocate: interrupted while waiting for available memory";
  public static final String FORCERESIZE_CANNOT_RESIZE_A_NULL_OR_RELEASED =
      "forceResize: cannot resize a null or released memory block";
  public static final String FORCERESIZE_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceResize: interrupted while waiting for available memory";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_THE_LOCK =
      "Interrupted while waiting for the lock.";
  public static final String IS_RELEASED_AFTER_THREAD_INTERRUPTION =
      "{} is released after thread interruption.";
  public static final String PIPEPERIODICALLOGREDUCER_IS_ALLOCATED_TO_BYTES =
      "PipePeriodicalLogReducer is allocated to {} bytes.";
  public static final String PIPETSFILERESOURCE_CACHED_DEVICEISALIGNEDMAP_FOR_TSFILE =
      "PipeTsFileResource: Cached deviceIsAlignedMap for tsfile {}.";
  public static final String PIPETSFILERESOURCE_CACHED_OBJECTS_FOR_TSFILE =
      "PipeTsFileResource: Cached objects for tsfile {}.";
  public static final String PIPETSFILERESOURCE_CLOSED_TSFILE_AND_CLEANED_UP =
      "PipeTsFileResource: Closed tsfile {} and cleaned up.";
  public static final String PIPETSFILERESOURCE_FAILED_TO_CACHE_OBJECTS_FOR_TSFILE =
      "PipeTsFileResource: Failed to cache objects for tsfile {} in cache, because memory "
          + "usage is high";
  public static final String PIPETSFILERESOURCE_FAILED_TO_DELETE_TSFILE_WHEN_CLOSING =
      "PipeTsFileResource: Failed to delete tsfile {} when closing, because {}. Please "
          + "MANUALLY delete it.";
  public static final String PIPETSFILERESOURCE_S_REFERENCE_COUNT_IS_DECREASED_TO =
      "PipeTsFileResource's reference count is decreased to below 0.";
  public static final String PIPE_HARDLINK_DIR_FOUND_DELETING_IT_RESULT =
      "Pipe hardlink dir found, deleting it: {}, result: {}";
  public static final String PIPE_HARDLINK_DIR_FOUND_MOVED_TO_PERIODICAL_DELETE =
      "Pipe hardlink dir found, moved it from {} to {} for throttled periodical deletion.";
  public static final String PIPE_STALE_HARDLINK_DIR_FOUND_REGISTERING_PERIODICAL_DELETE =
      "Stale pipe hardlink dir found, registering it for throttled periodical deletion: {}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_FINISHED =
      "Finished deleting stale pipe hardlink dir {} by periodical job, result: {}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_PROGRESS =
      "Periodically deleted {} paths from stale pipe hardlink dirs, current dir: {}, current round result: {}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_ALL_FINISHED =
      "Finished deleting all stale pipe hardlink dirs by periodical job.";
  public static final String PIPE_HARDLINK_DIR_MOVE_FAILED_DELETING_SYNC =
      "Failed to move pipe hardlink dir {} for periodical deletion, deleting it synchronously.";
  public static final String PIPE_HARDLINK_DIR_MOVE_FAILED_SKIPPING_PERIODICAL_DELETE =
      "Failed to move pipe hardlink dir {} for periodical deletion, skip registering the original dir to avoid deleting files of a recreated pipe.";
  public static final String PIPE_SNAPSHOT_DIR_FOUND_DELETING_IT =
      "Pipe snapshot dir found, deleting it: {},";
  public static final String SHRINK_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "Shrink callback is not supported in PipeFixedMemoryBlock";
  public static final String SHRINK_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "Shrink method is not supported in PipeFixedMemoryBlock";
  public static final String THE_MEMORY_BLOCK_HAS_BEEN_RELEASED =
      "The memory block has been released";
  public static final String THE_MULTIPLE_N_MUST_BE_GREATER_THAN =
      "The multiple n must be greater than 0";
  public static final String TRYALLOCATE_ALLOCATED_MEMORY_TOTAL_MEMORY_SIZE_BYTES =
      "tryAllocate: allocated memory, total memory size {} bytes, used memory size {} bytes, "
          + "original requested memory size {} bytes, actual requested memory size {} bytes";
  public static final String TRYALLOCATE_FAILED_TO_ALLOCATE_MEMORY_TOTAL_MEMORY =
      "tryAllocate: failed to allocate memory, total memory size {} bytes, used memory size {} "
          + "bytes, requested memory size {} bytes";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_IS_NOT_CONSISTENT_WITH =
      "tryExpandAllAndCheckConsistency: memory usage is not consistent with allocated blocks, "
          + "usedMemorySizeInBytes is {} but sum of all blocks is {}";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TABLETS_IS_NOT =
      "tryExpandAllAndCheckConsistency: memory usage of tablets is not consistent with "
          + "allocated blocks, usedMemorySizeInBytesOfTablets is {} but sum of all tablet blocks is "
          + "{}";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TSFILES_IS_NOT =
      "tryExpandAllAndCheckConsistency: memory usage of tsfiles is not consistent with "
          + "allocated blocks, usedMemorySizeInBytesOfTsFiles is {} but sum of all tsfile blocks is "
          + "{}";

  // ===================== METRIC =====================

  public static final String FAILED_TO_DEREGISTER_PIPE_ASSIGNER_METRICS_PIPEDATAREGIONASSIGNER =
      "Failed to deregister pipe assigner metrics, PipeDataRegionAssigner({}) does not exist";
  public static final String FAILED_TO_DEREGISTER_PIPE_DATA_REGION_EXTRACTOR =
      "Failed to deregister pipe data region extractor metrics, IoTDBDataRegionExtractor({}) "
          + "does not exist";
  public static final String FAILED_TO_DEREGISTER_PIPE_DATA_REGION_SINK =
      "Failed to deregister pipe data region sink metrics, PipeSinkSubtask({}) does not exist";
  public static final String FAILED_TO_DEREGISTER_PIPE_REMAINING_EVENT_AND =
      "Failed to deregister pipe remaining event and time metrics, "
          + "RemainingEventAndTimeOperator({}) does not exist";
  public static final String FAILED_TO_DEREGISTER_PIPE_SCHEMA_REGION_CONNECTOR =
      "Failed to deregister pipe schema region connector metrics, PipeConnectorSubtask({}) "
          + "does not exist";
  public static final String FAILED_TO_DEREGISTER_PIPE_SCHEMA_REGION_SOURCE =
      "Failed to deregister pipe schema region source metrics, IoTDBSchemaRegionSource({}) "
          + "does not exist";
  public static final String SKIP_DEREGISTER_PIPE_TSFILE_TO_TABLETS =
      "Skip deregistering pipe tsfile to tablets metrics because pipeID({}) is not registered";
  public static final String FAILED_TO_DEREGISTER_SCHEMA_REGION_LISTENER_METRICS =
      "Failed to deregister schema region listener metrics, SchemaRegionListeningQueue({}) "
          + "does not exist";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR =
      "Failed to mark pipe data region extractor heartbeat event, IoTDBDataRegionExtractor({}) "
          + "does not exist";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR_1 =
      "Failed to mark pipe data region extractor tablet event, IoTDBDataRegionExtractor({}) "
          + "does not exist";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR_2 =
      "Failed to mark pipe data region extractor tsfile event, IoTDBDataRegionExtractor({}) "
          + "does not exist";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_SINK =
      "Failed to mark pipe data region sink tablet event, PipeSinkSubtask({}) does not exist";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_SINK_1 =
      "Failed to mark pipe data region sink tsfile event, PipeSinkSubtask({}) does not exist";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_HEARTBEAT_EVENT =
      "Failed to mark pipe processor heartbeat event, PipeProcessorSubtask({}) does not exist";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_TABLET_EVENT =
      "Failed to mark pipe processor tablet event, PipeProcessorSubtask({}) does not exist";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_TSFILE_EVENT =
      "Failed to mark pipe processor tsfile event, PipeProcessorSubtask({}) does not exist";
  public static final String FAILED_TO_MARK_PIPE_REGION_COMMIT_REMAININGEVENTANDTIMEOPERATOR =
      "Failed to mark pipe region commit, RemainingEventAndTimeOperator({}) does not exist";
  public static final String FAILED_TO_MARK_PIPE_SCHEMA_REGION_WRITE =
      "Failed to mark pipe schema region write plan event, PipeConnectorSubtask({}) does not exist";
  public static final String FAILED_TO_MARK_PIPE_TSFILE_TO_TABLETS =
      "Failed to mark pipe tsfile to tablets invocation, pipeID({}) does not exist";
  public static final String FAILED_TO_RECORD_PIPE_TSFILE_TO_TABLETS =
      "Failed to record pipe tsfile to tablets time, pipeID({}) does not exist";
  public static final String FAILED_TO_RECORD_TABLET_GENERATED_PIPEID_DOES =
      "Failed to record tablet generated, pipeID({}) does not exist";
  public static final String FAILED_TO_SET_RECENT_PROCESSED_TSFILE_EPOCH =
      "Failed to set recent processed tsfile epoch state, PipeRealtimeDataRegionExtractor({}) "
          + "does not exist";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_ASSIGNER_METRICS =
      "Failed to unbind from pipe assigner metrics, assigner map not empty";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_DATA_REGION =
      "Failed to unbind from pipe data region sink metrics, sink map not empty";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_EXTRACTOR_METRICS =
      "Failed to unbind from pipe extractor metrics, extractor map not empty";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_PROCESSOR_METRICS =
      "Failed to unbind from pipe processor metrics, processor map not empty";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_REMAINING_EVENT =
      "Failed to unbind from pipe remaining event and time metrics, "
          + "RemainingEventAndTimeOperator map not empty";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION =
      "Failed to unbind from pipe schema region connector metrics, connector map not empty";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION_1 =
      "Failed to unbind from pipe schema region extractor metrics, extractor map not empty";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION_2 =
      "Failed to unbind from pipe schema region listener metrics, listening queue map not empty";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_TSFILE_TO =
      "Failed to unbind from pipe tsfile to tablets metrics, pipe map is not empty, pipe: {}";

  // ---------------------------------------------------------------------------
  // pipe – AbstractSameTypeNumericOperator
  // ---------------------------------------------------------------------------
  public static final String UNSUPPORTED_OUTPUT_DATATYPE_FMT = "Unsupported output datatype %s";

  // ---------------------------------------------------------------------------
  // pipe – IoTDBDataRegionSource
  // ---------------------------------------------------------------------------
  public static final String ILLEGAL_TREE_PATTERN_FMT = "Pattern \"%s\" is illegal.";

  // ---------------------------------------------------------------------------
  // pipe – OpcUaServerBuilder
  // ---------------------------------------------------------------------------
  public static final String UNABLE_CREATE_SECURITY_DIR = "Unable to create security dir: ";
  public static final String OPC_UA_SECURITY_DIR = "Security dir: {}";
  public static final String OPC_UA_SECURITY_PKI_DIR = "Security pki dir: {}";

  // ---------------------------------------------------------------------------
  // pipe – PipeDataNodePluginAgent
  // ---------------------------------------------------------------------------
  public static final String PLUGIN_NOT_REGISTERED_FMT = "plugin %s is not registered.";

  // ---------------------------------------------------------------------------
  // pipe - WriteBackSink
  // ---------------------------------------------------------------------------
  public static final String TABLE_MODEL_DATABASE_INVALID_FMT =
      "The table-model database %s is invalid. It should not contain '%s', should match the "
          + "pattern %s, and the length should not exceed %d";
  public static final String TREE_MODEL_DATABASE_INVALID_FMT =
      "The tree-model database %s is invalid. It should be a legal tree-model database path, "
          + "should match the pattern %s, and the length should not exceed %d";
  public static final String TARGET_TREE_MODEL_DATABASE_CANNOT_BE_USED_FOR_TABLE_MODEL_EVENTS_FMT =
      "The target tree-model database %s cannot be used for table-model events because the "
          + "corresponding table-model database %s is invalid.";
  public static final String FAILED_TO_REWRITE_TREE_MODEL_DATABASE_FMT =
      "Failed to rewrite tree-model database from %s to %s for device %s.";

  // ---------------------------------------------------------------------------
  // pipe – PipeTransferTrackableHandler
  // ---------------------------------------------------------------------------
  public static final String TPIPE_TRANSFER_RESP_IS_NULL_WHEN_TRANSFERRING_SLICE =
      "TPipeTransferResp is null when transferring slice.";

  private DataNodePipeMessages() {}
  // ---------------------------------------------------------------------------
  // Additional log messages
  // ---------------------------------------------------------------------------
  public static final String PIPE_LOG_SUBSCRIPTION_DETECT_DUPLICATED_PIPETSFILEINSERTIONEVENT_23A4740C =
      "Subscription: Detect duplicated PipeTsFileInsertionEvent {}, commit it directly";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_ECB64624 =
      "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is completed, "
          + "return termination response to client";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_8F561EB2 =
      "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is completed, "
          + "reply to client heartbeat request";
  public static final String PIPE_LOG_SUBSCRIPTION_CREATE_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_E7F21F1E =
      "Subscription: create prefetching queue bound to topic [{}] for consumer group [{}]";
  public static final String PIPE_LOG_SUBSCRIPTION_DROP_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_21F313CB =
      "Subscription: drop prefetching queue bound to topic [{}] for consumer group [{}]";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_03B89C51 =
      "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] still exists, "
          + "unbind it before closing";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_EA7D450B =
      "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] is closed";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_12E69B65 =
      "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] does not exist";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_C2735402 =
      "Subscription: prefetching queue bound to topic [{}] for consumer group [{}] has already "
          + "existed";
  public static final String PIPE_LOG_SUBSCRIPTIONPREFETCHINGTABLETQUEUE_DETECTED_OUTDATED_POLL_C0001CCF =
      "SubscriptionPrefetchingTabletQueue {} detected outdated poll request, consumer {}, commit "
          + "context {}, offset {}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_POLL_CALLED_CONSUMERID_TOPICNAMES_5F1F5175 =
      "ConsensusSubscriptionBroker [{}]: poll called, consumerId={}, topicNames={}, queueCount={}, "
          + "maxBytes={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_POLL_RESULT_CONSUMERID_EVENTSPOLLED_06412726 =
      "ConsensusSubscriptionBroker [{}]: poll result, consumerId={}, eventsPolled={}, "
          + "eventsNacked={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_REFRESHED_OWNERSHIP_FOR_TOPIC_EB11CF64 =
      "ConsensusSubscriptionBroker [{}]: refreshed ownership for topic [{}], consumers={}, "
          + "regions={}, generation={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_STABLE_OWNERSHIP_POLL_ORDER_D40BB7D4 =
      "ConsensusSubscriptionBroker [{}]: stable ownership poll order for topic [{}], "
          + "assignedQueueCount={}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_REGION_B40792D9 =
      "Subscription: consensus prefetching queue for topic [{}], region [{}] in consumer group "
          + "[{}] already exists, skipping";
  public static final String PIPE_LOG_SUBSCRIPTION_CREATE_CONSENSUS_PREFETCHING_QUEUE_BOUND_TO_0DBFC05E =
      "Subscription: create consensus prefetching queue bound to topic [{}] for consumer group "
          + "[{}], consensusGroupId={}, fallbackCommittedRegionProgress={}, tailStartSearchIndex={}, "
          + "initialRuntimeVersion={}, initialActive={}, totalRegionQueues={}";
  public static final String PIPE_LOG_SUBSCRIPTION_CLOSED_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_3A9DDEC5 =
      "Subscription: closed consensus prefetching queue for topic [{}] region [{}] in consumer "
          + "group [{}] due to region removal";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUE_S_BOUND_TO_TOPIC_AB10ED07 =
      "Subscription: consensus prefetching queue(s) bound to topic [{}] for consumer group [{}] "
          + "still exist, unbind before closing";
  public static final String PIPE_LOG_SUBSCRIPTION_DROP_ALL_CONSENSUS_PREFETCHING_QUEUE_S_BOUND_FCC1B2C4 =
      "Subscription: drop all {} consensus prefetching queue(s) bound to topic [{}] for consumer "
          + "group [{}]";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_COMMIT_7D8CC39D =
      "ConsensusSubscriptionBroker [{}]: no queues for topic [{}] to commit";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_COMMIT_CONTEXT_NOT_FOUND_IN_46DF62A6 =
      "ConsensusSubscriptionBroker [{}]: commit context {} not found in any of {} region queue(s) "
          + "for topic [{}]";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEK_6307A90D =
      "ConsensusSubscriptionBroker [{}]: no queues for topic [{}] to seek";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_UNSUPPORTED_SEEKTYPE_FOR_TOPIC_EDCA2CF2 =
      "ConsensusSubscriptionBroker [{}]: unsupported seekType {} for topic [{}]";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEK_9AC3890C =
      "ConsensusSubscriptionBroker [{}]: no queues for topic [{}] to seek(topicProgress)";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEKAFTER_C6D87BFD =
      "ConsensusSubscriptionBroker [{}]: no queues for topic [{}] to seekAfter(topicProgress)";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUES_BOUND_TO_TOPIC_63B37089 =
      "Subscription: consensus prefetching queues bound to topic [{}] for consumer group [{}] do "
          + "not exist";
  public static final String PIPE_LOG_SUBSCRIPTIONPREFETCHINGTSFILEQUEUE_DETECTED_OUTDATED_POLL_7E0CE108 =
      "SubscriptionPrefetchingTsFileQueue {} detected outdated poll request, consumer {}, commit "
          + "context {}, writing offset {}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_COMMIT_PIPETERMINATEEVENT_36529DC9 =
      "Subscription: SubscriptionPrefetchingQueue {} commit PipeTerminateEvent {}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_IGNORE_ENRICHEDEVENT_95C6241C =
      "Subscription: SubscriptionPrefetchingQueue {} ignore EnrichedEvent {} when prefetching.";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_POLL_COMMITTED_8684FF17 =
      "Subscription: SubscriptionPrefetchingQueue {} poll committed event {} from prefetching "
          + "queue (broken invariant), remove it";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_POLL_NON_POLLABLE_644D5D6B =
      "Subscription: SubscriptionPrefetchingQueue {} poll non-pollable event {} from prefetching "
          + "queue (broken invariant), nack and remove it";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_INTERRUPTED_WHILE_F8923826 =
      "Subscription: SubscriptionPrefetchingQueue {} interrupted while polling events.";
  public static final String PIPE_LOG_SUBSCRIPTION_INCONSISTENT_HEARTBEAT_EVENT_WHEN_PEEKING_BROKEN_BFE1DF6E =
      "Subscription: inconsistent heartbeat event when {} peeking (broken invariant), expected {}, "
          + "actual {}, offer back";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_ONLY_SUPPORT_PREFETCH_F3B33B30 =
      "Subscription: SubscriptionPrefetchingQueue {} only support prefetch EnrichedEvent. Ignore "
          + "{}.";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_PREFETCH_TSFILEINSERTIONEVENT_19444D2C =
      "Subscription: SubscriptionPrefetchingQueue {} prefetch TsFileInsertionEvent when "
          + "ToTabletIterator is not null (broken invariant). Ignore {}.";
  public static final String PIPE_LOG_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_WHEN_ON_RETRYABLE_4E10BE3B =
      "Failed to increase reference count for {} when {} on retryable TabletInsertionEvent";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_ON_RETRYABLE_TABLETINSERTIONEVENT_2350D9F7 =
      "Exception occurred when {} on retryable TabletInsertionEvent {}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_COMMIT_CONTEXT_DOES_NOT_EXIST_0E4EF990 =
      "Subscription: subscription commit context {} does not exist, it may have been committed or "
          + "something unexpected happened, prefetching queue: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_EVENT_IS_COMMITTED_SUBSCRIPTION_BEE17D7F =
      "Subscription: subscription event {} is committed, subscription commit context {}, "
          + "prefetching queue: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_EVENT_IS_NOT_COMMITTABLE_SUBSCRIPTION_8D03A10C =
      "Subscription: subscription event {} is not committable, subscription commit context {}, "
          + "prefetching queue: {}";
  public static final String PIPE_LOG_INCONSISTENT_CONSUMER_GROUP_WHEN_ACKING_EVENT_CURRENT_INCOMING_AEE3E90F =
      "inconsistent consumer group when acking event, current: {}, incoming: {}, consumer id: {}, "
          + "event commit context: {}, prefetching queue: {}, commit it anyway...";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_COMMIT_CONTEXT_DOES_NOT_EXIST_DE907E05 =
      "Subscription: subscription commit context [{}] does not exist, it may have been committed "
          + "or something unexpected happened, prefetching queue: {}";
  public static final String PIPE_LOG_INCONSISTENT_CONSUMER_GROUP_WHEN_NACKING_EVENT_CURRENT_INCOMING_B0104C41 =
      "inconsistent consumer group when nacking event, current: {}, incoming: {}, consumer id: {}, "
          + "event commit context: {}, prefetching queue: {}, commit it anyway...";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_RECYCLE_EVENT_7B120BC3 =
      "Subscription: SubscriptionPrefetchingQueue {} recycle event {} from in flight events, nack "
          + "and enqueue it to prefetching queue";
  public static final String PIPE_LOG_SUBSCRIPTION_POISON_MESSAGE_DETECTED_NACKCOUNT_FORCE_ACKING_7528DD6B =
      "Subscription: poison message detected (nackCount={}), force-acking event {} in prefetching "
          + "queue: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_POISON_MESSAGE_DETECTED_NACKCOUNT_FORCE_ACKING_D984349C =
      "Subscription: poison message detected (nackCount={}), force-acking eagerly pollable event "
          + "{} in prefetching queue: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_POISON_MESSAGE_DETECTED_NACKCOUNT_FORCE_ACKING_FEF0F0BF =
      "Subscription: poison message detected (nackCount={}), force-acking pollable event {} in "
          + "prefetching queue: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_UNKNOWN_PIPESUBSCRIBEREQUESTVERSION_RESPONSE_56E5D93F =
      "Subscription: Unknown PipeSubscribeRequestVersion, response status = {}.";
  public static final String PIPE_LOG_THE_SUBSCRIPTION_REQUEST_VERSION_IS_DIFFERENT_FROM_THE_CLIENT_324A125F =
      "The subscription request version {} is different from the client request version {}, the "
          + "receiver will be reset to the client request version.";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_IS_A_NO_OP_ON_THIS_DATANODE_BECAUSE_28F7E92B =
      "Subscription: consensus {} is a no-op on this DataNode because no local queue exists, "
          + "consumerGroup={}, topic={}";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_REFRESHING_CONSENSUS_QUEUE_ORDER_1886704D =
      "SubscriptionBrokerAgent: refreshing consensus queue order-mode for topic [{}] to [{}]";
  public static final String PIPE_LOG_SUBSCRIPTION_UNBOUND_CONSENSUS_PREFETCHING_QUEUE_S_FOR_REMOVED_AC018742 =
      "Subscription: unbound {} consensus prefetching queue(s) for removed region [{}]";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_SETACTIVEFORREGION_REGIONID_ACTIVE_4AC3A2CB =
      "SubscriptionBrokerAgent: setActiveForRegion regionId={}, active={}";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_SETACTIVEWRITERSFORREGION_REGIONID_48B39B3E =
      "SubscriptionBrokerAgent: setActiveWritersForRegion regionId={}, activeWriterNodeIds={}";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_APPLYRUNTIMESTATEFORREGION_REGIONID_6D8C37A1 =
      "SubscriptionBrokerAgent: applyRuntimeStateForRegion regionId={}, runtimeState={}";
  public static final String PIPE_LOG_SUBSCRIPTION_FAILED_TO_PARSE_CONSENSUS_REGION_ID_FOR_COMMITTED_9F1A50EB =
      "Subscription: failed to parse consensus region id {} for committed progress, topic={}, "
          + "consumerGroup={}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_E46FCDD9 =
      "Subscription: consensus broker bound to consumer group [{}] does not exist";
  public static final String PIPE_LOG_SUBSCRIPTION_PIPE_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_E9B60B22 =
      "Subscription: pipe broker bound to consumer group [{}] does not exist";
  public static final String PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_EXIST_74CAD5BE =
      "Subscription: broker bound to consumer group [{}] does not exist";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_GROUP_META_CHANGE_DETECTED_TOPICSUNSUBBYGROUP_F6DAF20A =
      "Subscription: consumer group [{}] meta change detected, topicsUnsubByGroup={}, "
          + "newlySubscribedTopics={}";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_HANDLING_SINGLE_CONSUMER_GROUP_META_10E7688C =
      "Exception occurred when handling single consumer group meta changes for consumer group {}";
  public static final String PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_HAS_ALREADY_0F37997F =
      "Subscription: broker bound to consumer group [{}] has already existed when the creation "
          + "time of consumer group meta on local agent {} is inconsistent with meta from coordinator "
          + "{}, drop it";
  public static final String PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_EXISTED_9F09E4DE =
      "Subscription: broker bound to consumer group [{}] does not existed when the corresponding "
          + "consumer group meta has already existed on local agent, ignore it";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_HANDLING_SINGLE_TOPIC_META_CHANGES_43434FC4 =
      "Exception occurred when handling single topic meta changes for topic {}";
  public static final String PIPE_LOG_PULLED_TOPIC_META_FROM_CONFIG_NODE_RECOVERING_5C4B1AEE =
      "Pulled topic meta from config node: {}, recovering ...";
  public static final String PIPE_LOG_INTERRUPTED_WHILE_SLEEPING_WILL_RETRY_TO_GET_TOPIC_META_976E4BE2 =
      "Interrupted while sleeping, will retry to get topic meta from config node.";
  public static final String PIPE_LOG_PULLED_CONSUMER_GROUP_META_FROM_CONFIG_NODE_RECOVERING_A85B948F =
      "Pulled consumer group meta from config node: {}, recovering ...";
  public static final String PIPE_LOG_INTERRUPTED_WHILE_SLEEPING_WILL_RETRY_TO_GET_CONSUMER_GROUP_7E161F39 =
      "Interrupted while sleeping, will retry to get consumer group meta from config node.";
  public static final String PIPE_LOG_FAILED_TO_GET_TOPIC_META_FROM_CONFIG_NODE_FOR_TIMES_WILL_E8D0B7F8 =
      "Failed to get topic meta from config node for {} times, will retry at most {} times.";
  public static final String PIPE_LOG_FAILED_TO_GET_CONSUMER_GROUP_META_FROM_CONFIG_NODE_FOR_TIMES_3E4C727C =
      "Failed to get consumer group meta from config node for {} times, will retry at most {} "
          + "times.";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_REFRESHED_OF_PROCESSOR_BUFFERED_COMMIT_8C7A352A =
      "Subscription: consumer {} refreshed {} of {} processor-buffered commit context lease(s)";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_SUCCESSFULLY_WITH_REQUEST_6BC8BFED =
      "Subscription: consumer {} poll {} successfully with request: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_FULL_COMMIT_CONTEXTS_CFC18359 =
      "Subscription: consumer {} commit (nack: {}) full commit contexts: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_FULL_REQUESTED_COMMIT_1E67E8A3 =
      "Subscription: consumer {} commit (nack: {}) full requested commit contexts: {}, full "
          + "accepted commit contexts: {}, full stale unsubscribed commit contexts: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_REMOVE_CONSUMER_CONFIG_WHEN_HANDLING_EXIT_3827D0E8 =
      "Subscription: remove consumer config {} when handling exit";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_IS_INACTIVE_FOR_MS_EXCEEDING_TIMEOUT_36E06B11 =
      "Subscription: consumer {} is inactive for {} ms, exceeding timeout {} ms, close it on "
          + "server side.";
  public static final String PIPE_LOG_SUBSCRIPTION_THE_CONSUMER_HAS_ALREADY_EXISTED_WHEN_HANDSHAKING_3761AD81 =
      "Subscription: The consumer {} has already existed when handshaking, skip creating consumer.";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_HANDSHAKE_SUCCESSFULLY_DATA_NODE_ID_58DA6A5F =
      "Subscription: consumer {} handshake successfully, data node id: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_UNSUBSCRIBE_SUCCESSFULLY_AA5E0AA9 =
      "Subscription: consumer {} unsubscribe {} successfully";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_ACCEPTED_SUCCESSFULLY_58D1C111 =
      "Subscription: consumer {} commit (nack: {}) accepted successfully, summary: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_SEEK_TOPIC_TO_TOPICPROGRESS_REGIONCOUNT_41702313 =
      "Subscription: consumer {} seek topic {} to topicProgress(regionCount={})";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_SEEKAFTER_TOPIC_TO_TOPICPROGRESS_REGIONCOUNT_838584F8 =
      "Subscription: consumer {} seekAfter topic {} to topicProgress(regionCount={})";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_SEEK_TOPIC_WITH_SEEKTYPE_799FF449 =
      "Subscription: consumer {} seek topic {} with seekType={}";
  public static final String PIPE_LOG_SUBSCRIPTION_UNSUBSCRIBE_ALL_SUBSCRIBED_TOPICS_BEFORE_CLOSE_BFB787AE =
      "Subscription: unsubscribe all subscribed topics {} before close consumer {}";
  public static final String PIPE_LOG_SUBSCRIPTION_THE_CONSUMER_DOES_NOT_EXISTED_WHEN_CLOSING_CCB63DCB =
      "Subscription: The consumer {} does not existed when closing, skip dropping consumer.";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_UNSUBSCRIBE_COMPLETED_TOPICS_SUCCESSFULLY_44BAFF55 =
      "Subscription: consumer {} unsubscribe {} (completed topics) successfully";
  public static final String PIPE_LOG_SUBSCRIPTION_FAILED_TO_CLOSE_TIMED_OUT_CONSUMER_AFTER_MS_89CC11F1 =
      "Subscription: failed to close timed out consumer {} after {} ms inactivity";
  public static final String PIPE_LOG_SUBSCRIPTION_DETECT_STALE_CONSUMER_CONFIG_WHEN_HANDSHAKING_B0196DB8 =
      "Subscription: Detect stale consumer config when handshaking, stale consumer config {} will "
          + "be cleared, consumer config will set to the incoming consumer config {}.";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_HEARTBEAT_B9EFB1CC =
      "Subscription: missing consumer config when handling heartbeat request: {}";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_FETCH_ENDPOINTS_FOR_CONSUMER_IN_325B571A =
      "Exception occurred when fetch endpoints for consumer {} in config node";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBESUBSCRIBEREQ_DF466A30 =
      "Subscription: missing consumer config when handling PipeSubscribeSubscribeReq: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBEUNSUBSCRIBEREQ_673CE701 =
      "Subscription: missing consumer config when handling PipeSubscribeUnsubscribeReq: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBEPOLLREQ_6BB9292B =
      "Subscription: missing consumer config when handling PipeSubscribePollReq: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_NULL_RESPONSE_FOR_EVENT_OUTDATED_4CF7FAAA =
      "Subscription: consumer {} poll null response for event {} (outdated: {}) with request: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_FOR_EVENT_OUTDATED_FAILED_WITH_0BEFF244 =
      "Subscription: consumer {} poll {} for event {} (outdated: {}) failed with request: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBECOMMITREQ_76B28EBB =
      "Subscription: missing consumer config when handling PipeSubscribeCommitReq: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_PARTIALLY_ACCEPTED_REQUESTED_87D0C038 =
      "Subscription: consumer {} commit (nack: {}) partially accepted, requested summary: {}, "
          + "accepted summary: {}, stale unsubscribed summary: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBECLOSEREQ_717660F8 =
      "Subscription: missing consumer config when handling PipeSubscribeCloseReq: {}";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_SEEKING_WITH_REQUEST_6B581543 =
      "Exception occurred when seeking with request {}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_SUBSCRIPTION_B85D47A4 =
      "Subscription: missing consumer config when handling subscription seek request: {}";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_CREATING_CONSUMER_IN_CONFIG_5D2E1B97 =
      "Unexpected status code {} when creating consumer {} in config node";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_CLOSING_CONSUMER_IN_CONFIG_NODE_0C2E0CE6 =
      "Unexpected status code {} when closing consumer {} in config node";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_SUBSCRIBING_TOPICS_FOR_CONSUMER_8676DA8A =
      "Unexpected status code {} when subscribing topics {} for consumer {} in config node";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_SUBSCRIBING_TOPICS_FOR_CONSUMER_E5D72F10 =
      "Exception occurred when subscribing topics {} for consumer {} in config node";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_UNSUBSCRIBING_TOPICS_FOR_CONSUMER_EFC771F0 =
      "Unexpected status code {} when unsubscribing topics {} for consumer {} in config node";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_UNSUBSCRIBING_TOPICS_FOR_CONSUMER_FE4B3CEE =
      "Exception occurred when unsubscribing topics {} for consumer {} in config node";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_EXCESSIVE_PAYLOAD_FOR_EVENT_OUTDATED_2BFF690B =
      "Subscription: consumer {} poll excessive payload {} for event {} (outdated: {}) with "
          + "request: {}, something unexpected happened with parameter configuration or payload "
          + "control...";
  public static final String PIPE_LOG_FAILED_TO_UNBIND_FROM_SUBSCRIPTION_PREFETCHING_QUEUE_METRICS_6614388C =
      "Failed to unbind from subscription prefetching queue metrics, prefetching queue map not "
          + "empty";
  public static final String PIPE_LOG_FAILED_TO_DEREGISTER_SUBSCRIPTION_PREFETCHING_QUEUE_METRICS_F08479A7 =
      "Failed to deregister subscription prefetching queue metrics, "
          + "SubscriptionPrefetchingQueue({}) does not exist";
  public static final String PIPE_LOG_FAILED_TO_MARK_TRANSFER_EVENT_RATE_SUBSCRIPTIONPREFETCHINGQUEUE_7DEF95B5 =
      "Failed to mark transfer event rate, SubscriptionPrefetchingQueue({}) does not exist";
  public static final String PIPE_LOG_FAILED_TO_UNBIND_FROM_CONSENSUS_SUBSCRIPTION_PREFETCHING_A8F920D9 =
      "Failed to unbind from consensus subscription prefetching queue metrics, queue map not empty";
  public static final String PIPE_LOG_FAILED_TO_DEREGISTER_CONSENSUS_SUBSCRIPTION_PREFETCHING_8B180091 =
      "Failed to deregister consensus subscription prefetching queue metrics, "
          + "ConsensusPrefetchingQueue({}) does not exist";
  public static final String PIPE_LOG_FAILED_TO_MARK_TRANSFER_EVENT_RATE_CONSENSUSPREFETCHINGQUEUE_FE9B91C3 =
      "Failed to mark transfer event rate, ConsensusPrefetchingQueue({}) does not exist";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTSFILERESPONSE_IS_EMPTY_WHEN_FETCHING_NEXT_DFD60DF1 =
      "SubscriptionEventTsFileResponse {} is empty when fetching next response (broken invariant)";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTSFILERESPONSE_IS_NOT_EMPTY_WHEN_INITIALIZING_C9DE83C9 =
      "SubscriptionEventTsFileResponse {} is not empty when initializing (broken invariant)";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTSFILERESPONSE_IS_EMPTY_WHEN_GENERATING_B8D03E93 =
      "SubscriptionEventTsFileResponse {} is empty when generating next response (broken invariant)";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTABLETRESPONSE_WAIT_FOR_RESOURCE_ENOUGH_9926289F =
      "SubscriptionEventTabletResponse {} wait for resource enough for parsing tablets {} seconds.";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTABLETRESPONSE_IS_EMPTY_WHEN_FETCHING_NEXT_4464E3F2 =
      "SubscriptionEventTabletResponse {} is empty when fetching next response (broken invariant)";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTABLETRESPONSE_IS_NOT_EMPTY_WHEN_INITIALIZING_88F075C9 =
      "SubscriptionEventTabletResponse {} is not empty when initializing (broken invariant)";
  public static final String PIPE_LOG_DETECT_LARGE_TABLETS_WITH_BYTE_S_CURRENT_TABLETS_SIZE_BYTE_4D472E38 =
      "Detect large tablets with {} byte(s), current tablets size {} byte(s)";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTBINARYCACHE_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_08F23ADE =
      "SubscriptionEventBinaryCache.allocatedMemoryBlock has shrunk from {} to {}.";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTBINARYCACHE_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_52A971D9 =
      "SubscriptionEventBinaryCache.allocatedMemoryBlock has expanded from {} to {}.";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTBINARYCACHE_RAISED_AN_EXCEPTION_WHILE_SERIALIZING_F3B698CB =
      "SubscriptionEventBinaryCache raised an exception while serializing "
          + "CachedSubscriptionPollResponse: {}";
  public static final String PIPE_LOG_SUBSCRIPTION_SOMETHING_UNEXPECTED_HAPPENED_WHEN_SERIALIZING_5467B7B6 =
      "Subscription: something unexpected happened when serializing "
          + "CachedSubscriptionPollResponse: {}";
  public static final String PIPE_LOG_HAS_BEEN_ITERATED_TIMES_CURRENT_TSFILEINSERTIONEVENT_0939C298 =
      "{} has been iterated {} times, current TsFileInsertionEvent {}";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_ONLY_SUPPORT_CONVERT_PIPEINSERTNODETABLETINSERTIONEVENT_B888B8AA =
      "SubscriptionPipeTabletEventBatch {} only support convert PipeInsertNodeTabletInsertionEvent "
          + "or PipeRawTabletInsertionEvent to tablet. Ignore {}.";
  public static final String
      PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_POSTPONE_EMITTING_SUBSCRIPTION_TABLET_BATCH_FOR_TOPIC_ARG_BECAUSE_TABLE_SCHEMA_ARG_ARG_IS_NOT_AVAILABLE_LOCALLY_996C618D =
          "Postpone emitting subscription tablet batch for topic {} because table schema {}.{} is not available locally";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_UNEXPECTED_TABLET_INSERTION_8FB1B507 =
      "SubscriptionPipeTabletEventBatch: Unexpected tablet insertion event {}, skipping it.";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_FAILED_TO_INCREASE_THE_595722D8 =
      "SubscriptionPipeTabletEventBatch: Failed to increase the reference count of event {}, "
          + "skipping it.";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_OVERRIDE_NON_NULL_CURRENTTABLETINSERTIONEVENTSITERATOR_2633B158 =
      "SubscriptionPipeTabletEventBatch {} override non-null currentTabletInsertionEventsIterator "
          + "when iterating (broken invariant).";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_IGNORE_ENRICHEDEVENT_WHEN_E6BAEACE =
      "SubscriptionPipeTabletEventBatch {} ignore EnrichedEvent {} when iterating (broken "
          + "invariant).";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETSFILEEVENTBATCH_IGNORE_TSFILEINSERTIONEVENT_88189024 =
      "SubscriptionPipeTsFileEventBatch {} ignore TsFileInsertionEvent {} when batching.";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPEEVENTBATCH_IGNORE_ENRICHEDEVENT_WHEN_BATCHING_E69BE90D =
      "SubscriptionPipeEventBatch {} ignore EnrichedEvent {} when batching.";
  public static final String PIPE_LOG_CONSENSUS_PREFETCH_EXECUTOR_IS_SHUTDOWN_SKIP_REGISTERING_83E36171 =
      "Consensus prefetch executor is shutdown, skip registering {}";
  public static final String PIPE_LOG_CONSENSUS_PREFETCH_SUBTASK_IS_ALREADY_REGISTERED_419FE7AD =
      "Consensus prefetch subtask {} is already registered";
  public static final String PIPE_LOG_CONSENSUS_PREFETCH_WORKER_LOOP_EXITS_ABNORMALLY_531EE564 =
      "Consensus prefetch worker loop exits abnormally";
  public static final String PIPE_LOG_FAILED_TO_CLOSE_SINK_AFTER_FAILED_TO_INITIALIZE_SINK_IGNORE_CF2E3D90 =
      "Failed to close sink after failed to initialize sink. Ignore this exception.";
  public static final String PIPE_LOG_CONSENSUSPREFETCHSUBTASK_UNEXPECTED_ERROR_WHILE_DRIVING_D361F4C2 =
      "ConsensusPrefetchSubtask {}: unexpected error while driving queue {}";
  public static final String PIPE_LOG_SUBSCRIPTIONSINKSUBTASK_FOR_CONSENSUS_TOPIC_FAILED_UNEXPECTEDLY_FC41B565 =
      "SubscriptionSinkSubtask for consensus topic [{}] failed unexpectedly, skip auto-resubmit";
  public static final String PIPE_LOG_FAILED_TO_BROADCAST_SUBSCRIPTION_PROGRESS_TO_DATANODE_AT_7024F5B2 =
      "Failed to broadcast subscription progress to DataNode {} at {}: {}";
  public static final String PIPE_LOG_FAILED_TO_BROADCAST_SUBSCRIPTION_PROGRESS_FOR_REGION_DE9074BD =
      "Failed to broadcast subscription progress for region {}: {}";
  public static final String PIPE_LOG_RECEIVED_SUBSCRIPTION_PROGRESS_BROADCAST_CONSUMERGROUPID_CDAEF839 =
      "Received subscription progress broadcast: consumerGroupId={}, topicName={}, regionId={}, "
          + "physicalTime={}, localSeq={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IDEMPOTENT_RE_COMMIT_FOR_30464FC4 =
      "ConsensusSubscriptionCommitState: idempotent re-commit for ({},{},{})";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IDEMPOTENT_DIRECT_COMMIT_B093AC01 =
      "ConsensusSubscriptionCommitState: idempotent direct commit for ({},{},{})";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_RECOVERED_COMMITTEDREGIONPROGRESS_F6B92C6B =
      "ConsensusSubscriptionCommitManager: recovered committedRegionProgress={} from ConfigNode "
          + "for consumerGroupId={}, topicName={}, regionId={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_COMMIT_FOR_UNKNOWN_751BD2A9 =
      "ConsensusSubscriptionCommitManager: Cannot commit for unknown state, consumerGroupId={}, "
          + "topicName={}, regionId={}, writerId={}, writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_DIRECT_COMMIT_D6AD7D96 =
      "ConsensusSubscriptionCommitManager: Cannot direct-commit for unknown state, "
          + "consumerGroupId={}, topicName={}, regionId={}, writerId={}, writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_RESET_UNKNOWN_C469052F =
      "ConsensusSubscriptionCommitManager: Cannot reset unknown state, consumerGroupId={}, "
          + "topicName={}, regionId={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_IGNORE_BROADCAST_WITHOUT_211DE477 =
      "ConsensusSubscriptionCommitManager: ignore broadcast without writer identity, "
          + "consumerGroupId={}, topicName={}, regionId={}, writerId={}, writerProgress={}";
  public static final String PIPE_LOG_SKIP_MALFORMED_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_NAME_BB4D75F0 =
      "Skip malformed consensus subscription progress file name {}";
  public static final String PIPE_LOG_FAILED_TO_RECOVER_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_DF30716B =
      "Failed to recover consensus subscription progress for consumerGroupId={}, topicName={}";
  public static final String PIPE_LOG_FAILED_TO_DELETE_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_51C57096 =
      "Failed to delete consensus subscription progress file {}";
  public static final String PIPE_LOG_FAILED_TO_PERSIST_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_4EA71236 =
      "Failed to persist consensus subscription progress for consumerGroupId={}, topicName={}, "
          + "regionId={}";
  public static final String PIPE_LOG_FAILED_TO_REWRITE_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_8B230D50 =
      "Failed to rewrite consensus subscription progress for consumerGroupId={}, topicName={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_FAILED_TO_QUERY_COMMIT_31E47F21 =
      "ConsensusSubscriptionCommitManager: failed to query commit progress from ConfigNode for "
          + "consumerGroupId={}, topicName={}, regionId={}, status={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_FAILED_TO_QUERY_COMMIT_16CFDCD9 =
      "ConsensusSubscriptionCommitManager: failed to query commit progress from ConfigNode for "
          + "consumerGroupId={}, topicName={}, regionId={}, starting from 0";
  public static final String PIPE_LOG_FAILED_TO_SERIALIZE_COMMITTED_REGION_PROGRESS_0D8D2129 =
      "Failed to serialize committed region progress {}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IGNORE_MAPPING_WITHOUT_3E66A74D =
      "ConsensusSubscriptionCommitState: ignore mapping without writer identity, writerId={}, "
          + "writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_DUPLICATE_OUTSTANDING_MAPPING_B5B34891 =
      "ConsensusSubscriptionCommitState: duplicate outstanding mapping for slot={}, previous={}, "
          + "current={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_OUTSTANDING_SIZE_EXCEEDS_1463BF02 =
      "ConsensusSubscriptionCommitState: outstanding size ({}) exceeds threshold ({}), consumers "
          + "may not be committing. committed=({},{}), writerNodeId={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_MISSING_WRITER_IDENTITY_01040357 =
      "ConsensusSubscriptionCommitState: missing writer identity for commit, writerId={}, "
          + "writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_UNKNOWN_KEY_FOR_COMMIT_5F699CFD =
      "ConsensusSubscriptionCommitState: unknown key ({},{},{}) for commit";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_MISSING_WRITER_IDENTITY_BB10A3B1 =
      "ConsensusSubscriptionCommitState: missing writer identity for direct commit, writerId={}, "
          + "writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_REJECT_DIRECT_COMMIT_WITHOUT_5B975E49 =
      "ConsensusSubscriptionCommitState: reject direct commit without outstanding mapping for "
          + "({},{},{})";
  public static final String PIPE_LOG_ISCONSENSUSBASEDTOPIC_CHECK_FOR_TOPIC_MODE_RESULT_19EFA0F9 =
      "isConsensusBasedTopic check for topic [{}]: mode={}, result={}";
  public static final String PIPE_LOG_SET_IOTCONSENSUS_ONNEWPEERCREATED_CALLBACK_FOR_CONSENSUS_0766CE68 =
      "Set IoTConsensus.onNewPeerCreated callback for consensus subscription auto-binding";
  public static final String PIPE_LOG_SET_IOTCONSENSUS_ONPEERREMOVED_CALLBACK_FOR_CONSENSUS_SUBSCRIPTION_21D4D6AC =
      "Set IoTConsensus.onPeerRemoved callback for consensus subscription cleanup";
  public static final String PIPE_LOG_NEW_DATAREGION_CREATED_CHECKING_CONSUMER_GROUP_S_FOR_AUTO_787C16E9 =
      "New DataRegion {} created, checking {} consumer group(s) for auto-binding, "
          + "currentSearchIndex={}";
  public static final String PIPE_LOG_AUTO_BINDING_CONSENSUS_QUEUE_FOR_TOPIC_IN_GROUP_TO_NEW_REGION_86F21649 =
      "Auto-binding consensus queue for topic [{}] in group [{}] to new region {} (database={}, "
          + "tailStartSearchIndex={}, hasLocalPersistedState={}, committedRegionProgress={}, "
          + "initialRuntimeVersion={}, initialActive={})";
  public static final String PIPE_LOG_DATAREGION_BEING_REMOVED_UNBINDING_ALL_CONSENSUS_SUBSCRIPTION_848A29F0 =
      "DataRegion {} being removed, unbinding all consensus subscription queues";
  public static final String PIPE_LOG_SETTING_UP_CONSENSUS_SUBSCRIPTIONS_FOR_CONSUMER_GROUP_TOPICS_204374A2 =
      "Setting up consensus subscriptions for consumer group [{}], topics={}, total consensus "
          + "groups={}";
  public static final String PIPE_LOG_SETTING_UP_CONSENSUS_QUEUE_FOR_TOPIC_ISTABLETOPIC_ORDERMODE_4F1CDC66 =
      "Setting up consensus queue for topic [{}]: isTableTopic={}, orderMode={}, config={}";
  public static final String PIPE_LOG_DISCOVERED_CONSENSUS_GROUP_S_FOR_TOPIC_IN_CONSUMER_GROUP_012EE420 =
      "Discovered {} consensus group(s) for topic [{}] in consumer group [{}]: {}";
  public static final String PIPE_LOG_SKIPPING_REGION_DATABASE_FOR_TABLE_TOPIC_DATABASE_KEY_2DA27A84 =
      "Skipping region {} (database={}) for table topic [{}] (DATABASE_KEY={})";
  public static final String PIPE_LOG_BINDING_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_IN_CONSUMER_45239EEA =
      "Binding consensus prefetching queue for topic [{}] in consumer group [{}] to data region "
          + "consensus group [{}] (database={}, tailStartSearchIndex={}, hasLocalPersistedState={}, "
          + "committedRegionProgress={}, initialRuntimeVersion={}, initialActive={})";
  public static final String PIPE_LOG_TORE_DOWN_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_CONSUMER_GROUP_80B84227 =
      "Tore down consensus subscription for topic [{}] in consumer group [{}]";
  public static final String PIPE_LOG_CHECKING_NEW_SUBSCRIPTIONS_IN_CONSUMER_GROUP_FOR_CONSENSUS_4A56D78A =
      "Checking new subscriptions in consumer group [{}] for consensus-based topics: {}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_IGNORE_STALE_RUNTIME_STATE_6C36B250 =
      "ConsensusSubscriptionSetupHandler: ignore stale runtime state for region {}, "
          + "incomingRuntimeVersion={}, currentRuntimeVersion={}, runtimeState={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_APPLYING_RUNTIME_STATE_1FB8937E =
      "ConsensusSubscriptionSetupHandler: applying runtime state for region {}, preferred writer "
          + "{} -> {}, runtimeVersion {} -> {}, runtimeState={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_REGION_PREFERRED_WRITER_46C1A894 =
      "ConsensusSubscriptionSetupHandler: region {} preferred writer changed {} -> {}, "
          + "runtimeVersion {} -> {}, runtimeState={} (route hint)";
  public static final String PIPE_LOG_FAILED_TO_CHECK_IF_TOPIC_IS_CONSENSUS_BASED_DEFAULTING_TO_ECCE1509 =
      "Failed to check if topic [{}] is consensus-based, defaulting to false";
  public static final String PIPE_LOG_SKIPPING_SETUP_OF_CONSENSUS_BASED_SUBSCRIPTIONS_FOR_CONSUMER_A7B2C812 =
      "Skipping setup of consensus-based subscriptions for consumer group [{}] because "
          + "mode=consensus only supports data_region_consensus_protocol_class={}, but current "
          + "configured value is {} (runtime consensus implementation: {})";
  public static final String PIPE_LOG_TOPIC_CONFIG_NOT_FOUND_FOR_TOPIC_CANNOT_SET_UP_CONSENSUS_A93339CE =
      "Topic config not found for topic [{}], cannot set up consensus queue";
  public static final String PIPE_LOG_NO_LOCAL_IOTCONSENSUS_DATA_REGION_FOUND_FOR_TOPIC_IN_CONSUMER_6FD0600E =
      "No local IoTConsensus data region found for topic [{}] in consumer group [{}]. Consensus "
          + "subscription will be set up when a matching data region becomes available.";
  public static final String PIPE_LOG_FAILED_TO_TEAR_DOWN_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_F59E8B7C =
      "Failed to tear down consensus subscription for topic [{}] in consumer group [{}]";
  public static final String PIPE_LOG_FAILED_TO_AUTO_BIND_TOPIC_IN_GROUP_TO_NEW_REGION_5BFD0E7D =
      "Failed to auto-bind topic [{}] in group [{}] to new region {}";
  public static final String PIPE_LOG_FAILED_TO_UNBIND_CONSENSUS_SUBSCRIPTION_QUEUES_FOR_REMOVED_7086F70A =
      "Failed to unbind consensus subscription queues for removed region {}";
  public static final String PIPE_LOG_FAILED_TO_SET_UP_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_CONSUMER_1A30001B =
      "Failed to set up consensus subscription for topic [{}] in consumer group [{}]";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_DESERIALIZED_MERGED_INSERTNODE_51FB8295 =
      "ConsensusLogToTabletConverter: deserialized merged InsertNode for searchIndex={}, type={}, "
          + "deviceId={}, searchNodeCount={}";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_SEARCHINDEX_CONTAINS_NON_INSERTNODE_CFA9FA49 =
      "ConsensusLogToTabletConverter: searchIndex={} contains non-InsertNode PlanNode: {}";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_CONVERTING_INSERTNODE_TYPE_B80428A0 =
      "ConsensusLogToTabletConverter: converting InsertNode type={}, deviceId={}";
  public static final String PIPE_LOG_UNSUPPORTED_INSERTNODE_TYPE_FOR_SUBSCRIPTION_E488EF74 =
      "Unsupported InsertNode type for subscription: {}";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_FAILED_TO_DESERIALIZE_ICONSENSUSREQUEST_EC1F6BAD =
      "ConsensusLogToTabletConverter: failed to deserialize IConsensusRequest (type={}) in "
          + "searchIndex={}: {}";
  public static final String PIPE_LOG_INSERTNODE_TYPE_IS_NULL_SKIPPING_CONVERSION_A2F1ADF7 =
      "InsertNode type is null, skipping conversion";
  public static final String PIPE_LOG_UNSUPPORTED_DATA_TYPE_C8929F11 = "Unsupported data type: {}";
  public static final String PIPE_LOG_UNSUPPORTED_DATA_TYPE_FOR_COPY_8AD25FE7 =
      "Unsupported data type for copy: {}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCHING_QUEUE_IS_EMPTY_FOR_22836B5E =
      "ConsensusPrefetchingQueue {}: prefetching queue is empty for consumerId={}, "
          + "pendingEntriesSize={}, nextExpected={}, isClosed={}, prefetchInitialized={}, "
          + "subtaskScheduled={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLLING_QUEUE_SIZE_CONSUMERID_FCA0AAD3 =
      "ConsensusPrefetchingQueue {}: polling, queue size={}, consumerId={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_DRAINED_ENTRIES_FROM_PENDINGENTRIES_2D4E0BE7 =
      "ConsensusPrefetchingQueue {}: drained {} entries from pendingEntries, first searchIndex={}, "
          + "last searchIndex={}, nextExpected={}, prefetchingQueueSize={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_TIME_BASED_FLUSH_TABLETS_LINGERED_10A4EBA8 =
      "ConsensusPrefetchingQueue {}: time-based flush, {} tablets lingered for {}ms "
          + "(threshold={}ms)";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_GAP_DETECTED_EXPECTED_GOT_FILLING_70DD08B3 =
      "ConsensusPrefetchingQueue {}: gap detected, expected={}, got={}. Filling {} entries from "
          + "WAL.";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ACCUMULATE_COMPLETE_BATCHSIZE_FA3F3B41 =
      "ConsensusPrefetchingQueue {}: accumulate complete, batchSize={}, processed={}, skipped={}, "
          + "lingerTablets={}, nextExpected={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SUBSCRIPTION_WAL_READ_ENTRIES_14AA5096 =
      "ConsensusPrefetchingQueue {}: subscription WAL read {} entries, nextExpectedSearchIndex={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SUBSCRIPTION_WAL_EXHAUSTED_AT_E61AF763 =
      "ConsensusPrefetchingQueue {}: subscription WAL exhausted at {} while current WAL is {}. "
          + "Rolling WAL file to expose current-file entries.";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SKIP_STALE_EVENT_WITH_SEARCHINDEX_07A09B36 =
      "ConsensusPrefetchingQueue {}: skip stale event with searchIndex range [{}, {}], "
          + "expectedSeekGeneration={}, currentSeekGeneration={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ENQUEUED_EVENT_WITH_TABLETS_SEARCHINDEX_140FDDCB =
      "ConsensusPrefetchingQueue {}: ENQUEUED event with {} tablets, searchIndex range [{}, {}], "
          + "prefetchQueueSize={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_REJECT_WITHOUT_WRITER_PROGRESS_D84AA802 =
      "ConsensusPrefetchingQueue {}: reject {} without writer progress, commitContext={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_REJECT_FOR_INACTIVE_QUEUE_COMMITCONTEXT_AE6D382C =
      "ConsensusPrefetchingQueue {}: reject {} for inactive queue, commitContext={}, "
          + "runtimeVersion={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_RECYCLED_TIMED_OUT_EVENT_BACK_5E58639C =
      "ConsensusPrefetchingQueue {}: recycled timed-out event {} back to prefetching queue";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_INJECTED_WATERMARK_WATERMARKTIMESTAMP_BF373164 =
      "ConsensusPrefetchingQueue {}: injected WATERMARK, watermarkTimestamp={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_CREATED_DORMANT_CONSUMERGROUPID_863BC6D6 =
      "ConsensusPrefetchingQueue created (dormant): consumerGroupId={}, topicName={}, "
          + "orderMode={}, consensusGroupId={}, fallbackCommittedRegionProgress={}, "
          + "fallbackTailSearchIndex={}, initialRuntimeVersion={}, initialActive={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCH_INITIALIZED_STARTSEARCHINDEX_69B53EE6 =
      "ConsensusPrefetchingQueue {}: prefetch initialized, startSearchIndex={}, progressSource={}, "
          + "recoveryWriterCount={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PERIODIC_STATS_LAG_PENDINGDELTA_D75375D0 =
      "ConsensusPrefetchingQueue {}: periodic stats, lag={}, pendingDelta={}, walDelta={}, "
          + "pendingTotal={}, walTotal={}, pendingQueueSize={}, prefetchingQueueSize={}, "
          + "inFlightEventsSize={}, realtimeWriterCount={}, walHasNext={}, isActive={}, "
          + "subtaskScheduled={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_WAITING_MS_FOR_WAL_GAP_TO_BECOME_7D91C6C5 =
      "ConsensusPrefetchingQueue {}: waiting {}ms for WAL gap [{}, {}) to become visible, "
          + "currentNextExpected={}, currentWalIndex={}, seekGeneration={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKTOREGIONPROGRESS_WRITERCOUNT_3134A29B =
      "ConsensusPrefetchingQueue {}: seekToRegionProgress writerCount={} -> {} searchIndex={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKAFTERREGIONPROGRESS_WRITERCOUNT_C6B26D20 =
      "ConsensusPrefetchingQueue {}: seekAfterRegionProgress writerCount={} -> {} searchIndex={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ABORTED_PENDING_SEEK_DURING_RUNTIME_F9928604 =
      "ConsensusPrefetchingQueue {}: aborted pending seek({}) during runtime stop, restored "
          + "prefetchInitialized {} -> {}, seekGeneration {} -> {}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_SCHEDULE_SEEK_BECAUSE_9E407068 =
      "ConsensusPrefetchingQueue {}: failed to schedule seek({}) because {}, restored "
          + "prefetchInitialized {} -> {}, seekGeneration {} -> {}";
  public static final String MESSAGE_THE_QUEUE_IS_CLOSING_AC6C2AB4 = "the queue is closing";
  public static final String MESSAGE_PREFETCH_RUNTIME_IS_UNAVAILABLE_F1721E89 =
      "prefetch runtime is unavailable";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEK_APPLIED_TO_SEARCHINDEX_WRITERCOUNT_FA2C4327 =
      "ConsensusPrefetchingQueue {}: seek({}) applied to searchIndex={}, writerCount={}, "
          + "seekGeneration={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FLUSHING_LINGERING_TABLETS_DURING_4C4AF235 =
      "ConsensusPrefetchingQueue {}: flushing {} lingering tablets during close";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ISACTIVE_SET_TO_REGION_EC0AD7BA =
      "ConsensusPrefetchingQueue {}: isActive set to {} (region={})";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_RUNTIMEACTIVEWRITERNODEIDS_EFFECTIVEACTIVEWRITERNODEIDS_246519D2 =
      "ConsensusPrefetchingQueue {}: runtimeActiveWriterNodeIds={}, "
          + "effectiveActiveWriterNodeIds={} (region={}, orderMode={}, preferredWriterNodeId={})";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFERREDWRITERNODEID_SET_TO_EFFECTIVEACTIVEWRITERNODEIDS_B08E8180 =
      "ConsensusPrefetchingQueue {}: preferredWriterNodeId set to {}, "
          + "effectiveActiveWriterNodeIds={} (region={}, orderMode={})";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ORDERMODE_SET_TO_EFFECTIVEACTIVEWRITERNODEIDS_CDD3C86E =
      "ConsensusPrefetchingQueue {}: orderMode set to {}, effectiveActiveWriterNodeIds={} "
          + "(region={}, preferredWriterNodeId={}, runtimeActiveWriterNodeIds={})";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_APPLIED_RUNTIMEVERSION_36E05B80 =
      "ConsensusPrefetchingQueue {}: applied runtimeVersion {}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_APPLIED_RUNTIMESTATE_PREFERREDWRITERNODEID_D845E9D6 =
      "ConsensusPrefetchingQueue {}: applied runtimeState={}, preferredWriterNodeId={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLL_COMMITTED_EVENT_BROKEN_INVARIANT_E478FA3C =
      "ConsensusPrefetchingQueue {} poll committed event {} (broken invariant), remove it";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLL_NON_POLLABLE_EVENT_BROKEN_E9551325 =
      "ConsensusPrefetchingQueue {} poll non-pollable event {} (broken invariant), nack it";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_INTERRUPTED_WHILE_POLLING_B7CFF5FD =
      "ConsensusPrefetchingQueue {} interrupted while polling";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_READING_SUBSCRIPTION_WAL_A3888AC5 =
      "ConsensusPrefetchingQueue {}: error reading subscription WAL";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_CLOSING_SUBSCRIPTION_WAL_19711C01 =
      "ConsensusPrefetchingQueue {}: error closing subscription WAL iterator";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_COMMIT_CONTEXT_DOES_NOT_EXIST_99B8A8F3 =
      "ConsensusPrefetchingQueue {}: commit context {} does not exist for ack";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_EVENT_ALREADY_COMMITTED_AC34E829 =
      "ConsensusPrefetchingQueue {}: event {} already committed";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_ADVANCE_COMMIT_FRONTIER_56E606C0 =
      "ConsensusPrefetchingQueue {}: failed to advance commit frontier for {}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_COMMIT_CONTEXT_DOES_NOT_EXIST_05F6C6E0 =
      "ConsensusPrefetchingQueue {}: commit context {} does not exist for nack";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKTOREGIONPROGRESS_NOT_SUPPORTED_85477BAB =
      "ConsensusPrefetchingQueue {}: seekToRegionProgress not supported (no WAL directory)";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKAFTERREGIONPROGRESS_NOT_SUPPORTED_55F36BE8 =
      "ConsensusPrefetchingQueue {}: seekAfterRegionProgress not supported (no WAL directory)";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_READ_WAL_METADATA_FROM_A2ED50D1 =
      "ConsensusPrefetchingQueue {}: failed to read WAL metadata from {} while computing seekToEnd "
          + "frontier";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_DURING_DEREGISTER_34C332E7 =
      "ConsensusPrefetchingQueue {}: error during deregister";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_FLUSH_LINGERING_BATCH_F97D8AA7 =
      "ConsensusPrefetchingQueue {}: failed to flush lingering batch during close, discarding it";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCH_ROUND_FAILED_TYPE_MESSAGE_63BC909B =
      "ConsensusPrefetchingQueue {}: prefetch round failed (type={}, message={})";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POISON_MESSAGE_DETECTED_NACKCOUNT_3A9255FB =
      "ConsensusPrefetchingQueue {}: poison message detected (nackCount={}), force-acking event {} "
          + "to prevent infinite re-delivery";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POISON_MESSAGE_DETECTED_DURING_23159F02 =
      "ConsensusPrefetchingQueue {}: poison message detected during recycle (nackCount={}), "
          + "force-acking event {}";
  public static final String PIPE_LOG_PROGRESSWALITERATOR_FAILED_TO_OPEN_NEAR_LIVE_WAL_FILE_RETRYING_5AEB94AC =
      "ProgressWALIterator: failed to open near-live WAL file {}, retrying without blacklisting";
  public static final String PIPE_LOG_PROGRESSWALITERATOR_ERROR_READING_WAL_2DB46D41 =
      "ProgressWALIterator: error reading WAL";
  public static final String PIPE_LOG_PROGRESSWALITERATOR_FAILED_TO_OPEN_WAL_FILE_SKIPPING_29CA1092 =
      "ProgressWALIterator: failed to open WAL file {}, skipping";
  public static final String PIPE_LOG_PIPE_TERMINATE_EVENT_COMMITTED_FOR_HISTORICAL_TRANSFER_CREATIONTIME_9B807B28 =
      "Pipe {}@{}: terminate event committed for historical transfer. creationTime: {}, "
          + "shouldMark: {}. {}";
  public static final String PIPE_LOG_PIPE_HISTORICAL_SOURCE_HAS_SUPPLIED_ALL_EVENTS_EMITTING_8B58DE19 =
      "Pipe {}@{}: historical source has supplied all events, emitting terminate event. {}";
  public static final String PIPE_LOG_PIPE_REALTIME_SOURCE_ON_DATA_REGION_LISTENTOTSFILE_LISTENTOINSERTNODE_A02E1552 =
      "Pipe {}@{} {} realtime source on data region {} (listenToTsFile={}, listenToInsertNode={}, "
          + "registeredSourceCount={}, tsFileSourceCount={}, insertNodeSourceCount={}).";
  public static final String PIPE_LOG_INTERRUPTED_WHILE_WAITING_FOR_IN_FLIGHT_PUBLISHES_TO_FINISH_C8E3757B =
      "Interrupted while waiting for in-flight publishes to finish when closing assigner on data "
          + "region {}.";
  public static final String PIPE_LOG_SCHEMAREGIONSTATEMACHINE_EXECUTE_READ_PLAN_FRAGMENTINSTANCE_F85A001F =
      "SchemaRegionStateMachine[{}]: Execute read plan: FragmentInstance-{}";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_IS_NO_LONGER_THE_SCHEMA_REGION_LEADER_FD783B3C =
      "Current node [nodeId: {}] is no longer the schema region leader [regionId: {}], the new "
          + "leader is [nodeId:{}]";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_IS_NO_LONGER_THE_SCHEMA_REGION_LEADER_12E06F99 =
      "Current node [nodeId: {}] is no longer the schema region leader [regionId: {}], start "
          + "cleaning up related services.";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_IS_NO_LONGER_THE_SCHEMA_REGION_LEADER_3092822E =
      "Current node [nodeId: {}] is no longer the schema region leader [regionId: {}], all "
          + "services on old leader are unavailable now.";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_BECOMES_SCHEMA_REGION_LEADER_REGIONID_46C70A32 =
      "Current node [nodeId: {}] becomes schema region leader [regionId: {}]";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_AS_SCHEMA_REGION_LEADER_REGIONID_IS_F00BFAC5 =
      "Current node [nodeId: {}] as schema region leader [regionId: {}] is ready to work";
  public static final String PIPE_LOG_SCHEMA_REGION_LISTENING_QUEUE_LISTEN_TO_SNAPSHOT_FAILED_64845A44 =
      "Schema Region Listening Queue Listen to snapshot failed, the historical data may not be "
          + "transferred. snapshotPaths:{}";
  public static final String PIPE_LOG_WRITE_OPERATION_FAILED_BECAUSE_RETRYTIME_34EFBE99 =
      "write operation failed because {}, retryTime: {}.";
  public static final String PIPE_LOG_EXCEPTION_OCCURS_WHEN_TAKING_SNAPSHOT_FOR_IN_48CBDFCC =
      "Exception occurs when taking snapshot for {}-{} in {}";
  public static final String PIPE_LOG_MEETS_ERROR_WHEN_GETTING_SNAPSHOT_FILES_FOR_9BFA76B9 =
      "Meets error when getting snapshot files for {}-{}";
  public static final String PIPE_LOG_WRITE_OPERATION_STILL_FAILED_AFTER_RETRY_TIMES_BECAUSE_15EEA702 =
      "write operation still failed after {} retry times, because {}.";
  public static final String PIPE_LOG_NOW_TRY_TO_DELETE_DIRECTLY_DATABASEPATH_DELETEPATH_A427CD01 =
      "now try to delete directly, databasePath: {}, deletePath:{}";
  public static final String PIPE_LOG_BATCH_FAILURE_IN_EXECUTING_A_INSERTTABLETNODE_DEVICE_STARTTIME_9A5A70F6 =
      "Batch failure in executing a InsertTabletNode. device: {}, startTime: {}, measurements: {}, "
          + "failing status: {}";
  public static final String PIPE_LOG_INSERT_ROW_FAILED_DEVICE_TIME_MEASUREMENTS_FAILING_STATUS_63054E8B =
      "Insert row failed. device: {}, time: {}, measurements: {}, failing status: {}";
  public static final String PIPE_LOG_INSERT_TABLET_FAILED_DEVICE_STARTTIME_MEASUREMENTS_FAILING_B409B2C4 =
      "Insert tablet failed. device: {}, startTime: {}, measurements: {}, failing status: {}";

  // ---------------------------------------------------------------------------
  // Additional exception messages
  // ---------------------------------------------------------------------------
  public static final String PIPE_EXCEPTION_UNSUPPORTED_SUBSCRIPTION_REQUEST_VERSION_D_1E7C211A =
      "Unsupported subscription request version %d";
  public static final String PIPE_EXCEPTION_PAYLOAD_SIZE_S_BYTE_S_WILL_EXCEED_THE_THRESHOLD_S_BYTE_S_6043B3D8 =
      "payload size %s byte(s) will exceed the threshold %s byte(s)";
  public static final String PIPE_EXCEPTION_INCONSISTENT_READ_LENGTH_BROKEN_INVARIANT_EXPECTED_S_ACTUAL_9203668A =
      "inconsistent read length (broken invariant), expected: %s, actual: %s";
  public static final String PIPE_EXCEPTION_TIMEOUTEXCEPTION_WAITED_S_SECONDS_8B31A3A5 =
      "TimeoutException: Waited %s seconds";
  public static final String PIPE_EXCEPTION_THE_SUBSCRIPTIONCONNECTORSUBTASKMANAGER_ONLY_SUPPORTS_SUBSCRIPTION_CEFFAAA9 =
      "The SubscriptionConnectorSubtaskManager only supports subscription-sink.";
  public static final String PIPE_EXCEPTION_FAILED_TO_CONSTRUCT_SUBSCRIPTION_SINK_BECAUSE_OF_S_OR_S_DBA27DC2 =
      "Failed to construct subscription sink, because of %s or %s does not exist in pipe connector "
          + "parameters";
  public static final String PIPE_EXCEPTION_FAILED_TO_GET_PENDINGQUEUE_NO_SUCH_SUBTASK_S_B445404A =
      "Failed to get PendingQueue. No such subtask: %s";
  public static final String PIPE_EXCEPTION_INVALID_BASE64_URL_COMPONENT_LENGTH_F1F1B6BA =
      "Invalid base64 url component length";
  public static final String PIPE_EXCEPTION_INVALID_CONSENSUS_SUBSCRIPTION_PROGRESS_REGION_COUNT_S_7CE4FD8E =
      "Invalid consensus subscription progress region count %s";
  public static final String PIPE_EXCEPTION_INVALID_CONSENSUS_SUBSCRIPTION_PROGRESS_PAYLOAD_LENGTH_S_8C145986 =
      "Invalid consensus subscription progress payload length %s";
  public static final String PIPE_EXCEPTION_MALFORMED_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_S_83042847 =
      "Malformed consensus subscription progress file %s";
  public static final String PIPE_EXCEPTION_ILLEGAL_S_S_72D743AA = "Illegal %s=%s";
  public static final String PIPE_EXCEPTION_INTERRUPTED_WHILE_WAITING_FOR_SEEK_APPLICATION_7C7ECAF2 =
      "Interrupted while waiting for seek application";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_RECOVER_FROM_NON_EMPTY_C1B367EF =
      "ConsensusPrefetchingQueue %s: cannot recover from non-empty region progress without WAL "
          + "access: %s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_INITIALIZE_REPLAY_START_E02DE40E =
      "ConsensusPrefetchingQueue %s: cannot initialize replay start from region progress %s: %s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_SEEKTOREGIONPROGRESS_2746E514 =
      "ConsensusPrefetchingQueue %s: cannot seekToRegionProgress %s: %s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_SEEKAFTERREGIONPROGRESS_48A500C3 =
      "ConsensusPrefetchingQueue %s: cannot seekAfterRegionProgress %s: %s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_IS_CLOSING_WHILE_APPLYING_SEEK_2BB2B431 =
      "ConsensusPrefetchingQueue %s is closing while applying seek";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_RUNTIME_STOPPED_BEFORE_SEEK_7BCB4F4B =
      "ConsensusPrefetchingQueue %s runtime stopped before seek(%s) was applied";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_IS_CLOSING_BEFORE_SEEK_APPLIES_F893BB02 =
      "ConsensusPrefetchingQueue %s is closing before seek applies";
  public static final String PIPE_EXCEPTION_NO_PRIVILEGE_FOR_SELECT_FOR_USER_S_AT_TABLE_S_S_84B0C299 =
      "No privilege for SELECT for user %s at table %s.%s";
  public static final String PIPE_EXCEPTION_EXPECTED_BINARY_BYTE_OR_STRING_BUT_WAS_S_7976B10F =
      "Expected Binary, byte[] or String, but was %s.";
  public static final String PIPE_EXCEPTION_TIMEOUTEXCEPTION_WAITED_S_SECONDS_FOR_MEMORY_TO_PARSE_TSFILE_0E4EF8FD =
      "TimeoutException: Waited %s seconds for memory to parse TsFile";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_DATA_TYPE_S_FOR_COLUMN_S_9F870C01 =
      "unsupported data type %s for column %s";
  public static final String PIPE_EXCEPTION_COLUMN_S_NOT_FOUND_0FA13581 = "column %s not found";
  public static final String PIPE_EXCEPTION_INSERTNODE_TYPE_S_IS_NOT_SUPPORTED_7DF82B58 =
      "InsertNode type %s is not supported.";
  public static final String PIPE_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_5D5C02E4 =
      "Data type %s is not supported.";
  public static final String PIPE_EXCEPTION_FORCEALLOCATEFORTABLET_FAILED_TO_ALLOCATE_BECAUSE_THERE_F878474D =
      "forceAllocateForTablet: failed to allocate because there's too much memory for tablets, "
          + "total memory size %d bytes, used memory for tablet size %d bytes, requested memory size %d "
          + "bytes";
  public static final String PIPE_EXCEPTION_FORCEALLOCATEFORTSFILE_FAILED_TO_ALLOCATE_BECAUSE_THERE_6D614467 =
      "forceAllocateForTsFile: failed to allocate because there's too much memory for tsfiles, "
          + "total memory size %d bytes, used memory for tsfile size %d bytes, requested memory size %d "
          + "bytes";
  public static final String PIPE_EXCEPTION_FORCEALLOCATE_FAILED_TO_ALLOCATE_MEMORY_AFTER_D_RETRIES_44EF7AE7 =
      "forceAllocate: failed to allocate memory after %d retries, total memory size %d bytes, used "
          + "memory size %d bytes, requested memory size %d bytes";
  public static final String PIPE_EXCEPTION_FORCERESIZE_FAILED_TO_ALLOCATE_MEMORY_AFTER_D_RETRIES_TOTAL_8C6948BC =
      "forceResize: failed to allocate memory after %d retries, total memory size %d bytes, used "
          + "memory size %d bytes, requested memory size %d bytes";
  public static final String PIPE_EXCEPTION_FAILED_TO_GET_HARDLINK_OR_COPIED_FILE_IN_PIPE_DIR_FOR_FILE_F009D86E =
      "failed to get hardlink or copied file in pipe dir for file %s, it is not a tsfile, mod file "
          + "or resource file";
  public static final String PIPE_EXCEPTION_PIPEPLANTOSTATEMENTVISITOR_DOES_NOT_SUPPORT_VISITING_GENERAL_452AAA60 =
      "PipePlanToStatementVisitor does not support visiting general plan, PlanNode: %s";
  public static final String PIPE_EXCEPTION_AIRGAP_PAYLOAD_LENGTH_D_EXCEEDS_MAXIMUM_ALLOWED_D_CLOSING_D1712B3D =
      "AirGap payload length (%d) exceeds maximum allowed (%d). Closing connection from %s";
  public static final String PIPE_EXCEPTION_DETECTED_SUSPICIOUS_NESTED_E_LANGUAGE_PREFIX_CLOSING_CONNECTION_69C76172 =
      "Detected suspicious nested E-Language prefix. Closing connection from %s";
  public static final String PIPE_EXCEPTION_AUTO_CREATE_DATABASE_FAILED_S_STATUS_CODE_S_D8EB60FA =
      "Auto create database failed: %s, status code: %s";
  public static final String PIPE_EXCEPTION_IOTCONSENSUSV2_PIPENAME_S_FAILED_TO_CREATE_RECEIVER_FILE_DD67E854 =
      "IoTConsensusV2-PipeName-%s: Failed to create receiver file dir %s. Because parent system "
          + "dir have been deleted due to system concurrently exit.";
  public static final String PIPE_EXCEPTION_IOTCONSENSUSV2_PIPENAME_S_FAILED_TO_CREATE_RECEIVER_FILE_5ADC430A =
      "IoTConsensusV2-PipeName-%s: Failed to create receiver file dir %s. May because authority or "
          + "dir already exists etc.";
  public static final String PIPE_EXCEPTION_IOTCONSENSUSV2_PIPENAME_S_FAILED_TO_CREATE_TSFILEWRITER_85EC8DD2 =
      "IoTConsensusV2-PipeName-%s: Failed to create tsFileWriter-%d receiver file dir";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_IOTCONSENSUSV2_REQUEST_VERSION_D_E1D94606 =
      "Unsupported iotConsensusV2 request version %d";
  public static final String PIPE_EXCEPTION_CAN_NOT_EXECUTE_DELETE_STATEMENT_S_3563E8A3 =
      "Can not execute delete statement: %s";
  public static final String PIPE_EXCEPTION_CAN_NOT_EXECUTE_LOAD_TSFILE_STATEMENT_S_8CC1A096 =
      "Can not execute load TsFile statement: %s";
  public static final String PIPE_EXCEPTION_FAILED_TO_GET_PIPE_TASK_PROGRESS_INDEX_WITH_PIPE_NAME_S_CFE9DE7C =
      "Failed to get pipe task progress index with pipe name: %s, consensus group id %s.";
  public static final String PIPE_EXCEPTION_EXCEPTION_IN_PIPE_PROCESS_SUBTASK_S_LAST_EVENT_S_ROOT_CAUSE_95B49C24 =
      "Exception in pipe process, subtask: %s, last event: %s, root cause: %s";
  public static final String PIPE_EXCEPTION_THE_VISIBILITY_OF_THE_PIPE_S_S_IS_NOT_COMPATIBLE_WITH_THE_30B8BF0A =
      "The visibility of the pipe (%s, %s) is not compatible with the visibility of the source "
          + "(%s, %s, %s), processor (%s, %s, %s), and connector (%s, %s, %s).";
  public static final String PIPE_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_WHEN_CONVERT_DATA_AT_CLIENT_405429CC =
      "data type %s is not supported when convert data at client";
  public static final String PIPE_EXCEPTION_HANDSHAKE_ERROR_WITH_RECEIVER_S_S_CODE_D_MESSAGE_S_4ED82649 =
      "Handshake error with receiver %s:%s, code: %d, message: %s.";
  public static final String PIPE_EXCEPTION_THE_WEBSOCKET_SERVER_HAS_ALREADY_BEEN_CREATED_WITH_PORT_FFC420AE =
      "The websocket server has already been created with port = %d. Please set the option "
          + "cdc.port = %d.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_INSERTION_EVENT_S_703A2E9E =
      "Network error when transfer tsFile insertion event: %s.";
  public static final String PIPE_EXCEPTION_CANNOT_SEND_PIPE_DATA_TO_RECEIVER_S_S_BECAUSE_S_25143D54 =
      "Cannot send pipe data to receiver %s:%s, because: %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_EVENT_S_BECAUSE_S_60A63AD7 =
      "Network error when transfer event %s, because %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TABLET_INSERTION_EVENT_S_BECAUSE_A6F87EF5 =
      "Network error when transfer tablet insertion event %s, because %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_INSERTION_EVENT_S_BECAUSE_BDE61690 =
      "Network error when transfer tsfile insertion event %s, because %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_EVENT_S_BECAUSE_S_F36D2A6B =
      "Network error when transfer tsfile event %s, because %s.";
  public static final String PIPE_EXCEPTION_FAILED_TO_TRANSFER_TABLET_INSERTION_EVENT_S_BECAUSE_S_9710318F =
      "Failed to transfer tablet insertion event %s, because %s.";
  public static final String PIPE_EXCEPTION_FAILED_TO_TRANSFER_TSFILE_INSERTION_EVENT_S_BECAUSE_S_21AD3263 =
      "Failed to transfer tsfile insertion event %s, because %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_FILE_S_BECAUSE_S_3C673B7A =
      "Network error when transfer file %s, because %s.";
  public static final String PIPE_EXCEPTION_PARAMETERS_IN_SET_S_ARE_NOT_ALLOWED_IN_SKIPIF_AAF177AD =
      "Parameters in set %s are not allowed in 'skipif'";
  public static final String PIPE_EXCEPTION_FAILED_TO_CHECK_PASSWORD_FOR_PIPE_S_0B1A5C73 =
      "Failed to check password for pipe %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_DELETION_S_BECAUSE_S_3B250B4B =
      "Network error when transfer deletion %s, because %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TABLET_BATCH_BECAUSE_S_6BEC52E7 =
      "Network error when transfer tablet batch, because %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_INSERT_NODE_TABLET_INSERTION_D993C7AB =
      "Network error when transfer insert node tablet insertion event, because %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_RAW_TABLET_INSERTION_EVENT_BECAUSE_D8ACEC3C =
      "Network error when transfer raw tablet insertion event, because %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_SEAL_FILE_S_BECAUSE_S_DC87F263 =
      "Network error when seal file %s, because %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_SCHEMA_REGION_WRITE_PLAN_S_BECAUSE_AEB210C7 =
      "Network error when transfer schema region write plan %s, because %s.";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_SEAL_SNAPSHOT_FILE_S_S_AND_S_BECAUSE_5EF373E6 =
      "Network error when seal snapshot file %s, %s and %s, because %s.";
  public static final String PIPE_EXCEPTION_FAILED_TO_TRANSFER_SLICE_ORIGIN_REQ_S_S_SLICE_INDEX_D_SLICE_44E1CF32 =
      "Failed to transfer slice. Origin req: %s-%s, slice index: %d, slice count: %d. Reason: %s";
  public static final String PIPE_EXCEPTION_THE_EXISTING_SERVER_WITH_TCP_PORT_S_AND_HTTPS_PORT_S_S_S_08C076F7 =
      "The existing server with tcp port %s and https port %s's %s %s conflicts to the new %s %s, "
          + "reject reusing.";
  public static final String PIPE_EXCEPTION_INVALID_KEYSTORE_THE_SERVERPRIVATEKEY_IS_S_F5F3C02F =
      "Invalid keyStore, the serverPrivateKey is %s";
  public static final String PIPE_EXCEPTION_THE_FOLDER_NODE_FOR_S_DOES_NOT_EXIST_CC0776AE =
      "The folder node for %s does not exist.";
  public static final String PIPE_EXCEPTION_THE_NODE_S_DOES_NOT_EXIST_52F98935 =
      "The Node %s does not exist.";
  public static final String PIPE_EXCEPTION_THE_EXISTING_SERVER_WITH_NODEURL_S_S_S_S_CONFLICTS_TO_THE_1C06A4F6 =
      "The existing server with nodeUrl %s's %s %s conflicts to the new %s %s, reject reusing.";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_S_CONSTRUCTED_FROM_PIPETRANSFERTABLETINSERTNODEREQ_FF5ED1D7 =
      "Unknown InsertBaseStatement %s constructed from PipeTransferTabletInsertNodeReq.";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTNODE_TYPE_S_WHEN_CONSTRUCTING_STATEMENT_FROM_4A055174 =
      "Unknown InsertNode type %s when constructing statement from insert node.";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_S_CONSTRUCTED_FROM_PIPETRANSFERTABLETBINARYREQV2_06D274D2 =
      "unknown InsertBaseStatement %s constructed from PipeTransferTabletBinaryReqV2.";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_S_CONSTRUCTED_FROM_PIPETRANSFERTABLETINSERTNODEREQV2_16F399B6 =
      "Unknown InsertBaseStatement %s constructed from PipeTransferTabletInsertNodeReqV2.";
  public static final String PIPE_EXCEPTION_FAILED_TO_CREATE_FILE_DIR_FOR_BATCH_S_8FCD9125 =
      "Failed to create file dir for batch: %s";
  public static final String PIPE_EXCEPTION_FAILED_TO_CREATE_BATCH_FILE_DIR_BATCH_ID_S_EA8BE86C =
      "Failed to create batch file dir. (Batch id = %s)";
  public static final String PIPE_EXCEPTION_PIPETREESTATEMENTTOPLANVISITOR_DOES_NOT_SUPPORT_VISITING_3A4A6524 =
      "PipeTreeStatementToPlanVisitor does not support visiting general statement, Statement: %s";
  public static final String PIPE_EXCEPTION_PIPESTATEMENTTOPLANVISITOR_DOES_NOT_SUPPORT_VISITING_GENERAL_590C6BD7 =
      "PipeStatementToPlanVisitor does not support visiting general statement, Statement: %s";
  public static final String PIPE_EXCEPTION_THE_PATH_PATTERN_S_IS_NOT_VALID_FOR_THE_SOURCE_ONLY_PREFIX_139F93D6 =
      "The path pattern %s is not valid for the source. Only prefix or full path is allowed.";
  public static final String PIPE_EXCEPTION_S_S_S_SHOULD_BE_LESS_THAN_OR_EQUAL_TO_S_S_S_0B9726E1 =
      "%s (%s) [%s] should be less than or equal to %s (%s) [%s].";
  public static final String PIPE_EXCEPTION_PARAMETERS_IN_SET_S_ARE_NOT_ALLOWED_IN_REALTIME_LOOSE_RANGE_BACD2475 =
      "Parameters in set %s are not allowed in 'realtime.loose-range'";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_EVENT_TYPE_S_FOR_LOG_REALTIME_EXTRACTOR_S_961C5D2D =
      "Unsupported event type %s for log realtime extractor %s";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_EVENT_TYPE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_9C4F4C82 =
      "Unsupported event type %s for hybrid realtime extractor %s";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_STATE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_43BD62C2 =
      "Unsupported state %s for hybrid realtime extractor %s";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_EVENT_TYPE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_474BAAC2 =
      "Unsupported event type %s for hybrid realtime extractor %s to supply.";
  public static final String PIPE_EXCEPTION_PARAMETERS_IN_SET_S_ARE_NOT_ALLOWED_IN_HISTORY_LOOSE_RANGE_0F685D5C =
      "Parameters in set %s are not allowed in 'history.loose-range'";
  public static final String PIPE_EXCEPTION_THE_AGGREGATOR_AND_OUTPUT_NAME_S_IS_INVALID_BC22CF92 =
      "The aggregator and output name %s is invalid.";
  public static final String PIPE_EXCEPTION_THE_NEEDED_INTERMEDIATE_VALUES_S_ARE_NOT_DEFINED_3FF0C52D =
      "The needed intermediate values %s are not defined.";
  public static final String PIPE_EXCEPTION_THE_PROCESSOR_S_IS_NOT_A_WINDOWING_PROCESSOR_EA5B59BA =
      "The processor %s is not a windowing processor.";
  public static final String PIPE_EXCEPTION_THE_AGGREGATE_PROCESSOR_DOES_NOT_SUPPORT_PROGRESSINDEXTYPE_35351D27 =
      "The aggregate processor does not support progressIndexType %s";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_IS_NOT_SUPPORTED_E1A6F05D =
      "The type %s is not supported";
  public static final String PIPE_EXCEPTION_THE_OUTPUT_TABLET_DOES_NOT_SUPPORT_COLUMN_TYPE_S_62F3845C =
      "The output tablet does not support column type %s";
  public static final String PIPE_EXCEPTION_THE_NEW_DATABASE_NAME_S_IS_INVALID_IT_SHOULD_NOT_CONTAIN_C3AB555E =
      "The new database name %s is invalid, it should not contain '%s', should match the pattern "
          + "%s, and the length should not exceed %d";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_BOOLEAN_F19CCF75 =
      "The type %s cannot be casted to boolean.";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_INT_659069CC =
      "The type %s cannot be casted to int.";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_LONG_2D206561 =
      "The type %s cannot be casted to long.";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_FLOAT_C15A8A95 =
      "The type %s cannot be casted to float.";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_DOUBLE_E577C0D7 =
      "The type %s cannot be casted to double.";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_STRING_34983FBD =
      "The type %s cannot be casted to string.";
  public static final String PIPE_EXCEPTION_UNABLE_TO_CREATE_IOTCONSENSUSV2_DELETION_DIR_AT_S_800EE360 =
      "Unable to create iotConsensusV2 deletion dir at %s";
  public static final String PIPE_EXCEPTION_THE_TIMESERIES_S_USED_NEW_TYPE_S_IS_NOT_COMPATIBLE_WITH_455D4D4A =
      "The timeseries %s used new type %s is not compatible with the existing one %s.";
  public static final String PIPE_EXCEPTION_THERE_ARE_TWO_TYPES_OF_PLANNODE_IN_ONE_REQUEST_S_AND_S_30FB3EE5 =
      "There are two types of PlanNode in one request: %s and %s";
  public static final String PIPE_EXCEPTION_THERE_ARE_TWO_TYPES_OF_PLANNODE_IN_ONE_REQUEST_S_AND_SEARCHNODE_F8B4D860 =
      "There are two types of PlanNode in one request: %s and SearchNode";
  public static final String COMPLETE_PAGE_BODY_EXPECTED_ACTUAL_FMT =
      "do not has a complete page body. Expected:%s. Actual:%s";
  public static final String UNCOMPRESS_PAGE_DATA_FAILED_FMT =
      "Uncompress error! uncompress size: %scompressed size: %spage header: %s%s";
  public static final String FAILED_TO_CLOSE_LISTENING_QUEUE_FOR_SCHEMAREGION_BECAUSE_FMT =
      "Failed to close listening queue for SchemaRegion %s, because %s";
  public static final String PIPE_SINK_HEARTBEAT_OR_TRANSFER_FAILED_FMT =
      "PipeConnector: %s(id: %s) heartbeat failed, or encountered failure when transferring "
          + "generic event. Failure: %s";
  public static final String FAILED_TO_ADD_ITEM_WITH_OPC_ERROR_CODE_FMT =
      "Failed to add item %s, opc error code: 0x%s";
  public static final String FAILED_TO_WRITE_WITH_VALUE_AND_OPC_ERROR_CODE_FMT =
      "Failed to write %s, value: %s, opc error code: 0x%s";
  public static final String NO_CERTIFICATE_FOUND = "No certificate found";
  public static final String CERTIFICATE_MISSING_APPLICATION_URI =
      "Certificate is missing the application URI";
  public static final String NULL_VALUE = "null";
  public static final String INCREASE_REFERENCE_COUNT_ERROR_HOLDER_FMT =
      "Increase reference count error. Holder Message: %s";
  public static final String DECREASE_REFERENCE_COUNT_ERROR_HOLDER_FMT =
      "Decrease reference count error. Holder Message: %s";
  public static final String INCREASE_REFERENCE_COUNT_TSFILE_OR_MODFILE_ERROR_HOLDER_FMT =
      "Increase reference count for TsFile %s or modFile %s error. Holder Message: %s";
  public static final String DECREASE_REFERENCE_COUNT_TSFILE_ERROR_HOLDER_FMT =
      "Decrease reference count for TsFile %s error. Holder Message: %s";
  public static final String INCREASE_REFERENCE_COUNT_MTREE_OR_TLOG_ERROR_HOLDER_FMT =
      "Increase reference count for mTree snapshot %s or tLog %s error. Holder Message: %s";
  public static final String DECREASE_REFERENCE_COUNT_MTREE_OR_TLOG_ERROR_HOLDER_FMT =
      "Decrease reference count for mTree snapshot %s or tLog %s error. Holder Message: %s";
  public static final String CONSENSUS_PREFETCHING_QUEUE_CLOSING_BEFORE_SEEK_SCHEDULED_FMT =
      "ConsensusPrefetchingQueue %s is closing before seek(%s) can be scheduled";
  public static final String CONSENSUS_PREFETCHING_QUEUE_RUNTIME_UNAVAILABLE_FOR_SEEK_FMT =
      "ConsensusPrefetchingQueue %s cannot schedule seek(%s) because prefetch runtime is unavailable";
  public static final String ERROR_PROGID_INVALID_OR_UNREGISTERED_HRESULT_FMT =
      "Error: ProgID is invalid or unregistered, (HRESULT=0x%s)";
  public static final String ERROR_RUNNING_OPC_CLIENT_FMT = "Error running opc client: %s: %s";
  public static final String ERROR_GETTING_OPC_CLIENT_FMT = "Error getting opc client: %s: %s";

  // ---------------------------------------------------------------------------
  // slice A1 – datanode pipe (leftover literals)
  // ---------------------------------------------------------------------------
  public static final String MESSAGE_FAILED_TO_LOAD_SNAPSHOT_FROM_ARG_9391AA27 =
      "Failed to load snapshot from {}";
  public static final String MESSAGE_PIPE_ARG_ARG_HISTORICAL_TSFILE_SELECTION_SUMMARY_SELECTED_BY_PROGRESS_UNCOVERED_ARG_7B74E18D =
      "Pipe {}@{}: historical TsFile selection summary, selected by progress uncovered {}, "
          + "selected by unclosed/closing {}, filtered by time/path {} (time {}, path {}), "
          + "skipped covered {}, skipped deleted {}, skipped generated by pipe {}, "
          + "pin failed {}";
  public static final String EXCEPTION_INVALID_ROW_SIZE_ARG_IN_TABLET_FORMAT_DESERIALIZATION_76405615 =
      "Invalid row size %s in tablet format deserialization.";
  public static final String EXCEPTION_INVALID_SCHEMA_SIZE_ARG_IN_TABLET_FORMAT_DESERIALIZATION_838C5359 =
      "Invalid schema size %s in tablet format deserialization.";
  public static final String EXCEPTION_MISSING_COLUMN_CATEGORY_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_660BD963 =
      "Missing column category in current tablet format deserialization.";
  public static final String EXCEPTION_INVALID_COLUMN_CATEGORY_ARG_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_569FF178 =
      "Invalid column category %s in current tablet format deserialization.";
  public static final String EXCEPTION_MISSING_TIMESTAMPS_IN_TABLET_FORMAT_DESERIALIZATION_WITH_NON_EMPTY_ROWS_7550129E =
      "Missing timestamps in tablet format deserialization with non-empty rows.";
  public static final String EXCEPTION_MISSING_VALUES_IN_TABLET_FORMAT_DESERIALIZATION_WITH_NON_EMPTY_ROWS_1B9C08D9 =
      "Missing values in tablet format deserialization with non-empty rows.";
  public static final String EXCEPTION_MISSING_ARG_FLAG_IN_TABLET_FORMAT_DESERIALIZATION_2F802C0D =
      "Missing %s flag in tablet format deserialization.";
  public static final String EXCEPTION_INVALID_ARG_FLAG_ARG_IN_TABLET_FORMAT_DESERIALIZATION_40FF35AA =
      "Invalid %s flag %s in tablet format deserialization.";
  public static final String EXCEPTION_INSUFFICIENT_BYTES_FOR_ARG_IN_TABLET_FORMAT_DESERIALIZATION_EXPECTED_ARG_REMAINING_ARG_3FE76C83 =
      "Insufficient bytes for %s in tablet format deserialization, expected %s, remaining %s.";
  public static final String EXCEPTION_INVALID_BITMAP_SIZE_ARG_IN_TABLET_FORMAT_DESERIALIZATION_832E7C9C =
      "Invalid bitmap size %s in tablet format deserialization.";
  public static final String EXCEPTION_UNSUPPORTED_SCHEMA_PLAN_NODE_9A833E0B =
      "Unsupported schema plan node ";
  public static final String EXCEPTION_CANNOT_BUILD_SCHEMA_BATCH_PLAN_NODE_FROM_EMPTY_BATCH_842D9E9B =
      "Cannot build schema batch plan node from empty batch.";
  public static final String EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_ARG_CONSTRUCTED_FROM_PIPETRANSFERTABLETBINARYREQ_20BF2833 =
      "Unknown InsertBaseStatement %s constructed from PipeTransferTabletBinaryReq.";
  public static final String EXCEPTION_INVALID_BINARY_REQUEST_BODY_LENGTH_ARG_REMAINING_BODY_LENGTH_ARG_5E21BBFC =
      "Invalid binary request body length %s, remaining body length %s.";
  public static final String EXCEPTION_FAILED_TO_DESERIALIZE_INSERT_NODE_ARG_ARG_IN_TABLET_BATCH_AT_BODY_POSITION_ARG_WITH_REMAINING_BODY_LENGTH_ARG_EC41A1DD =
      "Failed to deserialize insert node %s/%s in tablet batch at body position %s with remaining body length %s.";
  public static final String EXCEPTION_FAILED_TO_DESERIALIZE_RAW_TABLET_ARG_ARG_IN_TABLET_BATCH_AT_BODY_POSITION_ARG_WITH_REMAINING_BODY_LENGTH_ARG_D36919BA =
      "Failed to deserialize raw tablet %s/%s in tablet batch at body position %s with remaining body length %s.";
  public static final String EXCEPTION_INSUFFICIENT_BYTES_TO_READ_ARG_IN_TABLET_BATCH_REMAINING_BODY_LENGTH_ARG_343C1B9A =
      "Insufficient bytes to read %s in tablet batch, remaining body length %s.";
  public static final String EXCEPTION_INVALID_NEGATIVE_ARG_ARG_IN_TABLET_BATCH_89A5F868 =
      "Invalid negative %s %s in tablet batch.";
  public static final String EXCEPTION_FAILED_TO_DESERIALIZE_RAW_TABLET_REQUEST_AT_BODY_POSITION_ARG_WITH_REMAINING_BODY_LENGTH_ARG_45AC3692 =
      "Failed to deserialize raw tablet request at body position %s with remaining body length %s.";
  public static final String EXCEPTION_INCOMPLETE_SCHEMA_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_A23A1C30 =
      "Incomplete schema in current tablet format deserialization.";
  public static final String EXCEPTION_COLUMN_COUNT_IS_INCONSISTENT_WITH_SCHEMA_COUNT_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_53BA037A =
      "Column count is inconsistent with schema count in current tablet format deserialization.";
  public static final String EXCEPTION_INCOMPLETE_MEASUREMENT_SCHEMA_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_B8DB28A8 =
      "Incomplete measurement schema in current tablet format deserialization.";
  public static final String EXCEPTION_INCOMPLETE_COLUMN_VALUES_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_269782B9 =
      "Incomplete column values in current tablet format deserialization.";
  public static final String EXCEPTION_INCOMPLETE_TIMESTAMPS_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_FE212461 =
      "Incomplete timestamps in current tablet format deserialization.";
  public static final String MESSAGE_RECEIVER_ARG_IS_TEMPORARILY_UNAVAILABLE_THROTTLE_REQUESTS_FOR_ARG_MS_STATUS_ARG_F37192D9 =
      "Receiver {} is temporarily unavailable, throttle requests for {} ms. Status: {}";
  public static final String MESSAGE_SUCCESSFULLY_TRANSFERRED_BATCHED_SCHEMA_EVENTS_BATCH_SIZE_ARG_CF2E881C =
      "Successfully transferred batched schema events, batch size {}.";
  public static final String EXCEPTION_AUTO_CREATE_TREE_DATABASE_FAILED_ARG_STATUS_CODE_ARG_C6175C27 =
      "Auto create tree database failed: %s, status code: %s";
  public static final String EXCEPTION_ILLEGAL_TREE_DATABASE_ARG_C805A990 =
      "Illegal tree database %s.";
  public static final String EXCEPTION_FAILED_TO_GET_PARENT_DIR_OF_8CE21C1D =
      "Failed to get parent dir of ";
  public static final String EXCEPTION_FAILED_TO_PREPARE_NEXT_TABLET_INSERTION_EVENT_70A57827 =
      "Failed to prepare next tablet insertion event.";
  public static final String EXCEPTION_INVALID_ALIGNED_VALUE_CHUNK_INDEX_ARG_WHILE_THERE_ARE_ARG_TIME_CHUNKS_A7AE6C57 =
      "Invalid aligned value chunk index %d, while there are %d time chunks.";
  public static final String MESSAGE_FAILED_TO_ROLLBACK_CREATED_REALTIME_PIPE_ARG_STATUS_ARG_CE14334A =
      "Failed to rollback created realtime pipe {}. Status: {}";
  public static final String LOG_REPORTING_PIPE_META_ARG_ISCOMPLETED_ARG_REMAININGEVENTCOUNT_ARG_8F996DF3 =
      "Reporting pipe meta: %s, isCompleted: %s, remainingEventCount: %s";
  public static final String LOG_REPORTED_ARG_PIPE_METAS_12068FC6 =
      "Reported %s pipe metas.";
  public static final String MESSAGE_TRANSFER_FILE_ARG_ERROR_RESULT_STATUS_ARG_E565D9FD =
      "Transfer file %s error, result status %s.";

  public static final String EXCEPTION_LEGACY_PIPE_RECEIVER_REQUIRES_A_LOGGED_IN_SESSION_D96219BF =
      "Legacy pipe receiver requires a logged-in session.";
}
