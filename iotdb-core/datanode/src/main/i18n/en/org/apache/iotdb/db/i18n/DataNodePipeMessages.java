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
  public static final String PIPE_META_NOT_FOUND = "Pipe meta not found: ";
  public static final String PIPE_SINK_SUBTASKS_WITH_ATTRIBUTES_IS_BOUNDED =
      "Pipe sink subtasks with attributes {} is bounded with sinkExecutor {} and "
          + "callbackExecutor {}.";
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
      "{}: failed to allocate memory for parsing TsFile {}, tablet event no. {}, retry count "
          + "is {}, will keep retrying.";
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
  public static final String FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_COMMITTER_KEY_REPLICATE =
      "Failed to transfer TabletInsertionEvent {} (committer key={}, replicate index={}).";
  public static final String FAILED_TO_TRANSFER_TSFILE_BATCH =
      "Failed to transfer tsfile batch ({}).";
  public static final String FAILED_TO_TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY =
      "Failed to transfer tsfile event {} asynchronously.";
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
  public static final String RECEIVER_IS_READY = "Receiver-{} is ready";
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
}
