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

  public static final String CLOSING_DELETION_RESOURCE_MANAGER_FOR = "???? {} ????????...";
  public static final String DAL_THREAD_STILL_DOESN_T_EXIT_AFTER = "DAL ?? {} ? 30 ??????";
  public static final String DELETIONMANAGER_CURRENT_DAL_DIR_IS_DELETED_SUCCESSFULLY =
      "DeletionManager-{}?current DAL dir {} ?????";
  public static final String DELETIONMANAGER_CURRENT_DAL_DIR_IS_NOT_INITIALIZED =
      "DeletionManager-{}?current DAL dir {} ??????????";
  public static final String DELETIONMANAGER_CURRENT_WAITING_IS_INTERRUPTED_MAY_BECAUSE =
      "DeletionManager-{}?????????????????????";
  public static final String DELETIONMANAGER_DELETE_DELETION_FILE_IN_DIR =
      "DeletionManager-{}??? {} ???? deletion file...";
  public static final String DELETIONMANAGER_FAILED_TO_DELETE_FILE_IN_DIR =
      "DeletionManager-{} ?? file in {} dir, please manually check! ??";
  public static final String DELETIONRESOURCE_HAS_BEEN_RELEASED_TRIGGER_A_REMOVE =
      "DeletionResource {} ???????? DAL...";
  public static final String DELETION_PERSIST_CANNOT_CREATE_FILE_PLEASE_CHECK =
      "Deletion persist??????? {}???????????";
  public static final String DELETION_PERSIST_CANNOT_WRITE_TO_MAY_CAUSE =
      "Deletion persist????? {}???????????";
  public static final String DELETION_PERSIST_CURRENT_BATCH_FSYNC_DUE_TO =
      "Deletion persist-{}?????????? fsync";
  public static final String DELETION_PERSIST_CURRENT_FILE_HAS_BEEN_CLOSED =
      "Deletion persist-{}?current file ???";
  public static final String DELETION_PERSIST_SERIALIZE_DELETION_RESOURCE =
      "Deletion persist-{}???? deletion resource {}";
  public static final String DELETION_PERSIST_STARTING_TO_PERSIST_CURRENT_WRITING =
      "Deletion persist-{}????????????{}";
  public static final String DELETION_PERSIST_SWITCHING_TO_A_NEW_FILE =
      "Deletion persist-{}?????????????{}";
  public static final String DELETION_RESOURCE_MANAGER_FOR_HAS_BEEN_SUCCESSFULLY =
      "{} ??????????????";
  public static final String DETECT_FILE_CORRUPTED_WHEN_RECOVER_DAL_DISCARD =
      "?? DAL-{} ??????????????? DAL...";
  public static final String FAILED_TO_INITIALIZE_DELETIONRESOURCEMANAGER =
      "??? DeletionResourceManager ??";
  public static final String FAILED_TO_READ_DELETION_FILE_MAY_BECAUSE =
      "?? deletion file {} ????????????????";
  public static final String FAILED_TO_RECOVER_DELETIONRESOURCEMANAGER =
      "?? DeletionResourceManager ??";
  public static final String FAIL_TO_ALLOCATE_DELETIONBUFFER_GROUP_S_BUFFER =
      "?? deletionBuffer-group-{} ? buffer ???????????";
  public static final String FAIL_TO_CLOSE_CURRENT_LOGGING_FILE_WHEN = "?????????????";
  public static final String FAIL_TO_REGISTER_DELETIONRESOURCE_INTO_DELETIONBUFFER_BECAUSE =
      "? DeletionResource ??? deletionBuffer-{} ??????? buffer ????";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_ALL_DELETIONS_FLUSHED = "???????????????";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_RESULT = "?????????";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_TAKING_DELETIONRESOURCE_FROM =
      "?????????? DeletionResource ??????????";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_TAKING_WALENTRY_FROM =
      "?????????? WALEntry ??????????";
  public static final String INVALID_DELETION_PROGRESS_INDEX = "??????????";
  public static final String PERSISTTHREAD_DID_NOT_TERMINATE_WITHIN_S = "persistThread ? {} ?????";
  public static final String READ_DELETION_FILE_MAGIC_VERSION =
      "?? deletion file-{} magic version: {}";
  public static final String READ_DELETION_FROM_FILE = "? file {} ?? deletion: {}";
  public static final String UNABLE_TO_CREATE_IOTCONSENSUSV2_DELETION_DIR_AT =
      "??? {} ?? iotConsensusV2 ????";

  // ===================== AGENT =====================

  public static final String ATTEMPT_TO_REPORT_PIPE_EXCEPTION_TO_A =
      "????? PipeTaskMeta ?? pipe ???";
  public static final String CANNOT_PARSE_REBOOT_TIMES_FROM_FILE_SET =
      "????? {} ?? reboot times?????????{}??? reboot times";
  public static final String CANNOT_RECORD_REBOOT_TIMES_TO_FILE_THE =
      "??? reboot times {} ????? {}?reboot times ??????";
  public static final String CANNOT_START_SIMPLEPROGRESSINDEXASSIGNER_BECAUSE_OF =
      "???? SimpleProgressIndexAssigner????{}";
  public static final String CREATE_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "?? pipe DN task {} ????? {} ms";
  public static final String DEREGISTER_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "????? {}?runningTaskCount: {}, registeredTaskCount: {}";
  public static final String DROP_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "?? pipe DN task {} ????? {} ms";
  public static final String ERROR_OCCURRED_WHEN_COLLECTING_EVENTS_FROM_PROCESSOR =
      "? processor ?????????";
  public static final String EXCEPTION_IN_PIPE_EVENT_PROCESSING_IGNORED_BECAUSE =
      "pipe event processing ???????? pipe ???????????{}";
  public static final String TEMPORARILY_OUT_OF_MEMORY_IN_PIPE_EVENT_PROCESSING =
      "Pipe ???????????????????????{}";
  public static final String EXCEPTION_OCCURRED_WHEN_CLOSING_PIPE_CONNECTOR_SUBTASK =
      "?? pipe connector ??? {} ?????????{}";
  public static final String EXCEPTION_OCCURRED_WHEN_CLOSING_PIPE_PROCESSOR_SUBTASK =
      "?? pipe processor ??? {} ?????????{}";
  public static final String EXCEPTION_OCCURS_WHEN_EXECUTING_PIPE_TASK =
      "?? pipe task ??????";
  public static final String FAILED_TO_CHECK_IF_PIPE_HAS_RELEASE =
      "check if pipe has release region related resource with consensus group id: {} ???";
  public static final String FAILED_TO_CLEAR_CLOSE_THE_SCHEMA_REGION =
      "??/?? schema region ??????????{}???????????? region ???????";
  public static final String FAILED_TO_CLOSE_CONNECTOR_AFTER_FAILED_TO =
      "??? connector ????? connector ?????????";
  public static final String FAILED_TO_CLOSE_LISTENING_QUEUE_FOR_SCHEMAREGION =
      "?? SchemaRegion ???????";
  public static final String FAILED_TO_CLOSE_SOURCE_AFTER_FAILED_TO =
      "??? source ????? source ?????????";
  public static final String FAILED_TO_CONSTRUCT_PIPECONNECTOR_BECAUSE_OF =
      "?? PipeConnector ??????";
  public static final String FAILED_TO_DECREASE_REFERENCE_COUNT_FOR_EVENT =
      "?? reference count for event {} in PipeRealtimePriorityBlockingQueue ??";
  public static final String FAILED_TO_GET_PENDINGQUEUE_NO_SUCH_SUBTASK =
      "?? PendingQueue ?????????:  ";
  public static final String FAILED_TO_GET_PIPE_INFO_FROM_CONFIG_NODE_STATUS =
      "? CN ??? pipe ???????? %s?";
  public static final String FAILED_TO_GET_PIPE_METAS_WILL_BE =
      "?? pipe metas ??????? CN ??????";
  public static final String FAILED_TO_GET_PIPE_PLUGIN_JAR_FROM =
      "? CN ??? pipe ?? jar ????";
  public static final String
      LOG_FAILED_TO_FETCH_PIPE_PLUGIN_JARS_FROM_CONFIGNODE_PLUGINS_ARG_JARS_ARG_STATUS_ARG_RETRYING_EACH_PLUGIN_INDIVIDUALLY_574C0077 =
          "? ConfigNode ?? pipe plugin jars ??????{}?jars?{}????{}????????";
  public static final String
      LOG_CONFIGNODE_RETURNED_ARG_PIPE_PLUGIN_JARS_FOR_ARG_REQUESTED_PLUGINS_PLUGINS_ARG_JARS_ARG_RETRYING_EACH_PLUGIN_INDIVIDUALLY_27E32FDE =
          "ConfigNode ? {} ????????? {} ? pipe plugin jar????{}?jars?{}????????";
  public static final String
      EXCEPTION_FAILED_TO_FETCH_PIPE_PLUGIN_JAR_FROM_CONFIGNODE_FOR_PLUGIN_ARG_JAR_ARG_STATUS_ARG_B7C7FDE5 =
          "? ConfigNode ?? pipe plugin jar ????? %s?jar %s?????%s?";
  public static final String
      EXCEPTION_CONFIGNODE_RETURNED_ARG_JARS_FOR_PIPE_PLUGIN_ARG_WHILE_ONE_WAS_REQUESTED_A724E582 =
          "???? pipe plugin jar ??ConfigNode ??? %d ? jar????? %s?";
  public static final String
      LOG_FAILED_TO_FETCH_PIPE_PLUGIN_JAR_ARG_FOR_PIPE_PLUGIN_ARG_FROM_CONFIGNODE_4929C5D9 =
          "? ConfigNode ?? pipe plugin jar {}??? {}????";
  public static final String LOG_FAILED_TO_SAVE_JAR_ARG_FOR_PIPE_PLUGIN_ARG_A64D1530 =
      "?? jar {}?pipe plugin {}????";
  public static final String FAILED_TO_GET_PIPE_TASK_META_FROM =
      "?? pipe task meta from config node. Ignore the exception ??????config node may not be "
          + "ready yet, and meta will be pushed by config node later.";
  public static final String FAILED_TO_PERSIST_PROGRESS_INDEX_TO_CONFIGNODE =
      "???????? ConfigNode ??????{}";
  public static final String SHUTDOWN_PROGRESS_NOT_CONFIRMED =
      "?????????????????? ConfigNode?";
  public static final String START_TO_PERSIST_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
      "???????????? Pipe ?????Pipe ?? {}????? {} ms";
  public static final String
      INTERRUPTED_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
          "?????????? Pipe ?????????"
              + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String
      TIMED_OUT_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
          "?????????? Pipe ????????? {} ms?"
              + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String FAILED_TO_PERSIST_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
      "?????????? Pipe ????????? {} ms?"
          + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String COLLECTED_PIPE_METAS_FOR_SHUTDOWN_PROGRESS_PERSIST =
      "??????????????? Pipe ????Pipe ?? {}?Pipe ????? {}?"
          + "Pipe ????? {} ????? {} ms";
  public static final String COLLECTED_EMPTY_PIPE_METAS_DURING_SHUTDOWN =
      "????? {} ? Pipe ???? Pipe ????";
  public static final String START_TO_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE =
      "??? ConfigNode ??????? Pipe ??????DataNode ID {}?Pipe ?? {}?"
          + "Pipe ????? {}?Pipe ????? {} ??";
  public static final String FAILED_TO_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE =
      "? ConfigNode ??????? Pipe ?????????? {}??? {} ms?"
          + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String
      SUCCESSFULLY_FINISHED_PUSH_HEARTBEAT_SHUTDOWN_PIPE_META_TO_CONFIGNODE =
          "??? ConfigNode ??????? Pipe ??????Pipe ?? {}?Pipe ????? {}?"
              + "Pipe ????? {} ????? {} ms";
  public static final String
      EXCEPTION_OCCURRED_WHILE_PERSISTING_ALL_PIPE_PROGRESS_INDEXES_DURING_SHUTDOWN =
          "?????????? Pipe ??????????"
              + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String PERSISTING_PIPE_PROGRESS_INDEXES_BEFORE_SHUTDOWN =
      "???????? Pipe ????????? {} ms?";
  public static final String PIPE_PROGRESS_INDEXES_WERE_NOT_CONFIRMED_DURING_SHUTDOWN =
      "???? Pipe ?????? ConfigNode ???"
          + SHUTDOWN_PROGRESS_NOT_CONFIRMED;
  public static final String FAILURE_WHEN_REGISTER_PIPE_PLUGIN_SKIP_THIS =
      "?? pipe plugin {} ???????????????";
  public static final String
      FAILED_TO_REGISTER_PIPE_PLUGIN_BECAUSE_NAME_CONFLICTS_WITH_BUILTIN =
          "?? PipePlugin %s ???????? PipePlugin ????? PipePlugin ?????";
  public static final String
      FAILED_TO_REGISTER_PIPE_PLUGIN_BECAUSE_INSTANCE_CONSTRUCTION_FAILED =
          "?? PipePlugin %s(%s) ??????????????????%s";
  public static final String FAILED_TO_REGISTER_PIPE_PLUGIN_BECAUSE_JAR_MD5_MISMATCH =
      "?? PipePlugin %s ????? pipe plugin %s ???? jar ?? MD5 ??? jar ?????";
  public static final String FAILED_TO_DEREGISTER_BUILTIN_PIPE_PLUGIN =
      "???? PipePlugin %s ???";
  public static final String PIPECONNECTOR = "PipeConnector: ";
  public static final String EXCEPTION_PIPECONNECTOR_ARG_ID_ARG_HEARTBEAT_FAILED_OR_ENCOUNTERED_FAILURE_WHEN_TRANSFERRING_GENERIC_EVENT_FAILURE_ARG_679A4A49 =
      "PipeConnector?%s(id: %s) ????????? generic event ???????????%s";
  public static final String EXCEPTION_THE_DATABASE_NAME_IN_TREE_MODEL_MUST_START_WITH_ROOT_7BFA4609 =
      "tree ???????????? 'root.' ???";
  public static final String EXCEPTION_THE_LENGTH_OF_DATABASE_NAME_SHALL_NOT_EXCEED_82C7199C =
      "???????????? ";
  public static final String PIPEDATANODETASKBUILDER_FAILED_TO_PARSE_INCLUSION_AND_EXCLUSION =
      "PipeDataNodeTaskBuilder ?? 'inclusion' ? 'exclusion' ?????{}";
  public static final String PIPEDATANODETASKBUILDER_WHEN_INCLUSION_CONTAINS_DATA_DELETE_REALTIME =
      "PipeDataNodeTaskBuilder?? 'inclusion' ?? 'data.delete' ??'realtime-first' ??? "
          + "'false'??????????????";
  public static final String PIPEDATANODETASKBUILDER_WHEN_INCLUSION_INCLUDES_DATA_DELETE_REALTIME =
      "PipeDataNodeTaskBuilder?? 'inclusion' ?? 'data.delete' ??? 'realtime-first' ??? "
          + "'true' ????????????????";
  public static final String PIPEDATANODETASKBUILDER_WHEN_SOURCE_USES_SNAPSHOT_MODEL_REALTIME =
      "PipeDataNodeTaskBuilder?? source ????????'realtime-first' ??? 'false'?"
          + "??????????????";
  public static final String PIPEDATANODETASKBUILDER_WHEN_SOURCE_USES_SNAPSHOT_MODEL_REALTIME_1 =
      "PipeDataNodeTaskBuilder?? source ????????? 'realtime-first' ??? 'true' "
          + "???????????????";
  public static final String PIPEDATANODETASKBUILDER_WHEN_THE_REALTIME_SYNC_IS_ENABLED =
      "PipeDataNodeTaskBuilder?????????????? tsfile ??? rate limiter ??"
          + "???????????";
  public static final String PIPEDATANODETASKBUILDER_WHEN_THE_REALTIME_SYNC_IS_ENABLED_1 =
      "PipeDataNodeTaskBuilder??????????????? tsfile ??? rate limiter?"
          + "????????????? IO?";
  public static final String PIPEEVENTCOLLECTOR_THE_EVENT_IS_ALREADY_RELEASED_SKIPPING =
      "PipeEventCollector??? {} ??????????";
  public static final String PIPE_CONNECTOR_SUBTASK_WAS_CLOSED_WITHIN_MS =
      "Pipe?connector ??? {} ({}) ? {} ms ????";
  public static final String FAILED_TO_DISCARD_EVENTS_OF_PIPE_IN_CONNECTOR_SUBTASK =
      "Pipe {} ? connector ??? {} ?????????";
  public static final String PIPE_META_NOT_FOUND = "??? pipe ????";
  public static final String PIPE_SINK_SUBTASKS_WITH_ATTRIBUTES_IS_BOUNDED =
      "??? {} ? Pipe sink ?????? sinkExecutor {} ? callbackExecutor {}?";
  public static final String PIPE_SINK_SUBTASK_CLOSE_OPERATION_STILL_RUNNING = "????";
  public static final String
      PIPE_SINK_SUBTASK_CLOSE_OPERATION_WILL_RUN_AFTER_CURRENT_CONNECTOR_OPERATION =
          "???? connector ???????";
  public static final String PIPE_SINK_SUBTASK_CLOSE_TIMED_OUT =
      "?? pipe connector ????? {} ms?{}??????????????{}?";
  public static final String PIPE_SINK_SUBTASK_DELAYED_TO_AVOID_FREQUENT_HANDSHAKES =
      "Pipe sink ??? {} ???????? {} ms?????????????????";
  public static final String PIPE_SKIPPING_TEMPORARY_TSFILE_WHICH_SHOULDN_T =
      "Pipe ????????? TsFile?{}";
  public static final String PULLED_PIPE_META_FROM_CONFIG_NODE_RECOVERING =
      "?? config node ?? pipe ????{}????? ...";
  public static final String FAILED_TO_SHOW_CREATE_PIPE_NOT_EXIST =
      "show create pipe %s ???? pipe ????";
  public static final String FAILED_TO_SHOW_CREATE_TOPIC_NOT_EXIST =
      "show create topic %s ???? topic ????";
  public static final String RECEIVED_PIPE_HEARTBEAT_REQUEST_FROM_CONFIG_NODE =
      "???? config node ? pipe ???? {}?";
  public static final String REGION_NO_TSFILEINSERTIONEVENTS_TO_REPLACE_FOR_SOURCE =
      "Region {}?????? source ?? {} ? TsFileInsertionEvent";
  public static final String REGION_REPLACED_TSFILEINSERTIONEVENTS_WITH =
      "Region {}?? TsFileInsertionEvent {} ??? {}";
  public static final String REGISTEREDTASKCOUNT_0 = "registeredTaskCount ?? 0";
  public static final String REGISTEREDTASKCOUNT_0_1 = "registeredTaskCount ???? 0";
  public static final String REGISTER_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "????? {}?runningTaskCount: {}, registeredTaskCount: {}";
  public static final String REPORT_PIPERUNTIMEEXCEPTION_TO_LOCAL_PIPETASKMETA_EXCEPTION_MESSAGE =
      "??? PipeTaskMeta({}) ?? PipeRuntimeException??????{}";
  public static final String RUNNINGTASKCOUNT_0 = "runningTaskCount ?? 0";
  public static final String RUNNINGTASKCOUNT_0_1 = "runningTaskCount ???? 0";
  public static final String SIMPLEPROGRESSINDEXASSIGNER_STARTED_SUCCESSFULLY_ISSIMPLECONSENSUSENABLE_R =
      "SimpleProgressIndexAssigner ?????isSimpleConsensusEnable?{}?"
          + "rebootTimes?{}";
  public static final String STARTING_SIMPLEPROGRESSINDEXASSIGNER =
      "???? SimpleProgressIndexAssigner ...";
  public static final String START_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "?? pipe DN task {} ????? {} ms";
  public static final String START_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "????? {}?runningTaskCount: {}, registeredTaskCount: {}";
  public static final String STOP_PIPE_DN_TASK_SUCCESSFULLY_WITHIN_MS =
      "?? pipe DN task {} ????? {} ms";
  public static final String STOP_SUBTASK_RUNNINGTASKCOUNT_REGISTEREDTASKCOUNT =
      "????? {}?runningTaskCount: {}, registeredTaskCount: {}";
  public static final String SUBTASK_IS_CLOSED_IGNORE_EXCEPTION =
      "subtask {} ????????";
  public static final String SUBTASK_WORKER_IS_INTERRUPTED = "??????????";
  public static final String SUCCESSFULLY_PERSISTED_ALL_PIPE_S_INFO_TO =
      "????? Pipe ?????? ConfigNode?";
  public static final String THE_EXECUTOR_AND_HAS_BEEN_SUCCESSFULLY_SHUTDOWN =
      "??? {} ? {} ??????";

  // ===================== EVENT =====================

  public static final String DATABASENAMEFROMDATAREGION_IS_NULL = "databaseNameFromDataRegion ??";
  public static final String DECREASE_REFERENCE_COUNT_ERROR = "?????????";
  public static final String DECREASE_REFERENCE_COUNT_FOR_MTREE_SNAPSHOT_OR =
      "?? mTree snapshot {} ? tLog {} ? attribute snapshot {} ????????";
  public static final String DECREASE_REFERENCE_COUNT_FOR_TSFILE_ERROR =
      "Decrease reference count for TsFile {} ???";
  public static final String DO_NOT_HAS_A_COMPLETE_PAGE_BODY =
      "????? page body????";
  public static final String ERROR_WHILE_PARSING_TSFILE_INSERTION_EVENT =
      "?? tsfile insertion event ???";
  public static final String EXCEPTION_OCCURRED_WHEN_DETERMINING_THE_EVENT_TIME =
      "?? PipeInsertNodeTabletInsertionEvent({}) ???????????? [{}, {}] ???"
          + "??????????????? true?";
  public static final String FAILED_TO_ALLOCATE_MEMORY_FOR_PARSING_TSFILE =
      "{}???? TsFile {} ???????tablet ???? {}?"
          + "?????????????? TsFile ???";
  public static final String FAILED_TO_CONSUME_PARSED_TABLET_FROM_TSFILE_KEEP_PARSER =
      "{}??? TsFile {} ???? tablet ???tablet ???? {}????? {}?"
          + "?????????????????";
  public static final String FAILED_TO_BUILD_TABLET = "?? tablet ??";
  public static final String FAILED_TO_CHECK_NEXT = "check next ??";
  public static final String FAILED_TO_CLOSE_TSFILEREADER = "?? TsFileReader ??";
  public static final String FAILED_TO_CLOSE_TSFILESEQUENCEREADER = "?? TsFileSequenceReader ??";
  public static final String FAILED_TO_CREATE_TSFILEINSERTIONDATATABLETITERATOR =
      "?? TsFileInsertionDataTabletIterator ??";
  public static final String FAILED_TO_GET_NEXT_TABLET_INSERTION_EVENT =
      "?? next tablet insertion event ???";
  public static final String FAILED_TO_LOAD_MODIFICATIONS_FROM_TSFILE =
      "? TsFile ?? modifications ???";
  public static final String FAILED_TO_READ_METADATA_FOR_DEVICEID_MEASUREMENT =
      "?? deviceId?{}?measurement?{} ? metadata ????????";
  public static final String FAILED_TO_RECORD_PARSE_END_TIME_FOR =
      "?? parse end time for pipe {} ??";
  public static final String FAILED_TO_RECORD_TABLET_METRICS_FOR_PIPE =
      "?? tablet metrics for pipe {} ??";
  public static final String FOUND_NULL_DEVICEID_REMOVING_ENTRY =
      "?? deviceId ? null??????";
  public static final String INITIALIZE_DATA_CONTAINER_ERROR = "??????????";
  public static final String INSERTNODE_HAS_BEEN_RELEASED = "InsertNode ????";
  public static final String INSERTROWNODE_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "InsertRowNode({}) ?? pattern({}) ????? [{}, {}] ?????? 0?"
          + "??? source event({}) ?????";
  public static final String INSERTTABLETNODE_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "InsertTabletNode({}) ?? pattern({}) ????? [{}, {}] ?????? 0?"
          + "??? source event({}) ?????";
  public static final String INVALID_EVENT_TYPE = "??? event type: ";
  public static final String INVALID_INPUT = "??? input: ";
  public static final String ISGENERATEDBYPIPE_IS_NOT_SUPPORTED =
      "isGeneratedByPipe() ????";
  public static final String MAYEVENTPATHSOVERLAPPEDWITHPATTERN_IS_NOT_SUPPORTED =
      "mayEventPathsOverlappedWithPattern() ????";
  public static final String MAYEVENTTIMEOVERLAPPEDWITHTIMERANGE_IS_NOT_SUPPORTED =
      "mayEventTimeOverlappedWithTimeRange() ????";
  public static final String NO_COMMIT_IDS_FOUND_IN_PIPECOMPACTEDTSFILEINSERTIONEVENT =
      "? PipeCompactedTsFileInsertionEvent ???? commit ID?";
  public static final String PIPECOMPACTEDTSFILEINSERTIONEVENT_DOES_NOT_SUPPORT_EQUALSINIOTCONSENSUSV2 =
      "PipeCompactedTsFileInsertionEvent ??? equalsInIoTConsensusV2.";
  public static final String PIPECOMPACTEDTSFILEINSERTIONEVENT_DOES_NOT_SUPPORT_GETREBOOTTIMES =
      "PipeCompactedTsFileInsertionEvent ??? getRebootTimes.";
  public static final String PIPE_FAILED_TO_GET_DEVICES_FROM_TSFILE =
      "Pipe {}??? devices from TsFile {}, extract it anyway ??";
  public static final String PIPE_SKIPPING_TEMPORARY_TSFILE_S_PARSING_WHICH =
      "Pipe ????????? TsFile ????{}";
  public static final String ROW_CAN_NOT_BE_CUSTOMIZED = "Row ?????";
  public static final String SHALLOWCOPYSELFANDBINDPIPETASKMETAFORPROGRESSREPORT_IS_NOT_SUPPORTED =
      "shallowCopySelfAndBindPipeTaskMetaForProgressReport() ????";
  public static final String SKIPPING_TEMPORARY_TSFILE_S_PROGRESSINDEX_WILL_REPORT =
      "?? temporary TsFile {}'s progressIndex, will report MinimumProgressIndex";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_ROW_BY_ROW =
      "TablePatternParser ??? row by row processing";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_TABLET_PROCESSING =
      "TablePatternParser ??? tablet processing";
  public static final String TABLEPATTERNPARSER_DOES_NOT_SUPPORT_TABLET_PROCESSING_WITH =
      "TablePatternParser ??? tablet processing with collect";
  public static final String TABLET_IS_PARSED_TO_ZERO_ROWS_ACCORDING =
      "Tablet({}) ?? pattern({}) ????? [{}, {}] ?????? 0?"
          + "??? source event({}) ?????";
  public static final String TABLE_MODEL_TSFILE_PARSING_DOES_NOT_SUPPORT =
      "Table model tsfile ????????? ChunkMeta";
  public static final String TEMPORARY_TSFILE_DETECTED_WILL_SKIP_ITS_TRANSFER =
      "????? tsFile {}????????";
  public static final String TSFILE_HAS_INITIALIZED_PIPENAME_CREATION_TIME_PATTERN =
      "TsFile {} ???? {}?pipeName?{}??????{}?pattern?{}?startTime?"
          + "{}, endTime?{}, withMod?{}";
  public static final String UNCOMPRESS_ERROR_UNCOMPRESS_SIZE =
      "???????????";
  public static final String UNSUPPORTED = "???";
  public static final String UNSUPPORTED_NODE_TYPE = "???? node type ";
  public static final String WAIT_FOR_MEMORY_ENOUGH_FOR_PARSING_FOR =
      "?? memory enough???? parsing {} for {} ??";

  // ===================== PROCESSOR =====================

  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_BINARY_INPUT =
      "AbstractSameTypeNumericOperator ??? binary input";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_BOOLEAN_INPUT =
      "AbstractSameTypeNumericOperator ??? boolean input";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_DATE_INPUT =
      "AbstractSameTypeNumericOperator ??? date input";
  public static final String ABSTRACTSAMETYPENUMERICOPERATOR_DOES_NOT_SUPPORT_STRING_INPUT =
      "AbstractSameTypeNumericOperator ??? string input";
  public static final String CHANGINGVALUESAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH =
      "ChangingValueSamplingProcessor ? {} ?????{}?{}, {}?{}, {}?{}?";
  public static final String CLEAN_OUTDATED_INCOMPLETE_COMBINER_PIPENAME_CREATIONTIME_COMBINEID =
      "???????? combiner?pipeName={}, creationTime={}, combineId={}";
  public static final String COMBINEHANDLER_NOT_FOUND_FOR_PIPEID =
      "??? pipeId = ??? CombineHandler";
  public static final String COMBINER_COMBINE_COMPLETED_REGIONID_STATE_RECEIVEDREGIONIDSET_EX =
      "Combiner ?????regionId?{}, state?{}, receivedRegionIdSet?{}, "
          + "expectedRegionIdSet?{}";
  public static final String COMBINER_COMBINE_REGIONID_STATE_RECEIVEDREGIONIDSET_EXPECTEDREGI =
      "Combiner ???regionId?{}, state?{}, receivedRegionIdSet?{}, expectedRegionIdSet?{}";
  public static final String DATA_NODES_ENDPOINTS_FOR_TWO_STAGE_AGGREGATION =
      "???????? DataNode endpoints?{}";
  public static final String DIFFERENT_DATA_TYPE_ENCOUNTERED_IN_ONE_WINDOW =
      "?????????????????????????{}??????{}";
  public static final String ENCOUNTERED_EXCEPTION_WHEN_DESERIALIZING_FROM_PIPETASKMETA =
      "? PipeTaskMeta ?????????";
  public static final String END_POINTS_FOR_TWO_STAGE_AGGREGATION_PIPE =
      "????? pipe?pipeName={}, creationTime={}?? endpoints ???? {}";
  public static final String ERROR_OCCURRED_WHEN_CLOSING_COMBINEHANDLER_ID =
      "?? CombineHandler?id = {}??????";
  public static final String ERROR_OCCURS_WHEN_RECEIVING_REQUEST = "??????????{}?";
  public static final String LOGIN_FAILED_OR_SESSION_TIMED_OUT = "?????????????????";
  public static final String FAILED_TO_CLOSE_IOTDBSYNCCLIENT = "?? IoTDBSyncClient ??";
  public static final String FAILED_TO_CLOSE_OLD_IOTDBSYNCCLIENT = "?? old IoTDBSyncClient ??";
  public static final String FAILED_TO_COMBINE_COUNT = "?? count ???";
  public static final String FAILED_TO_CONSTRUCT_IOTDBSYNCCLIENT = "?? IoTDBSyncClient ??";
  public static final String FAILED_TO_FETCH_COMBINE_RESULT = "?????????";
  public static final String FAILED_TO_FETCH_DATA_NODES = "?? data node ??";
  public static final String FAILED_TO_FETCH_DATA_REGION_IDS = "?? data region id ??";
  public static final String FAILED_TO_RECONSTRUCT_IOTDBSYNCCLIENT_AFTER_FAILURE_TO =
      "?? request {}?watermark = {}?????? IoTDBSyncClient {} ??";
  public static final String FAILED_TO_SEND_REQUEST_WATERMARK_TO =
      "?? request {}?watermark = {}?? {} ??";
  public static final String FAILED_TO_TRIGGER_COMBINE_WATERMARK_COUNT_PROGRESSINDEX =
      "???????watermark={}, count={}, progressIndex={}";
  public static final String EXCEPTION_FAILED_TO_INITIALIZE_STATEPROGRESSINDEX_FROM_PROGRESS_INDEX_ARG_E95617F9 =
      "??????? %s ??? StateProgressIndex?";
  public static final String FAILURE_OCCURRED_WHEN_TRYING_TO_COMMIT_PROGRESS =
      "??????????????timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String FETCHED_DATA_REGION_IDS_AT = "? {} ??? data region id {}";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_BINARY_INPUT =
      "FractionPoweredSumOperator ??? binary input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_BOOLEAN_INPUT =
      "FractionPoweredSumOperator ??? boolean input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_DATE_INPUT =
      "FractionPoweredSumOperator ??? date input";
  public static final String FRACTIONPOWEREDSUMOPERATOR_DOES_NOT_SUPPORT_STRING_INPUT =
      "FractionPoweredSumOperator ??? string input";
  public static final String GLOBAL_COUNT_IS_LESS_THAN_THE_LAST =
      "Global count ??????? count?timestamp={}, count={}";
  public static final String IGNORED_TABLETINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF =
      "??? TabletInsertionEvent is not an instance of PipeInsertNodeTabletInsertionEvent or "
          + "PipeRawTabletInsertionEvent: {}";
  public static final String IGNORED_TSFILEINSERTIONEVENT_IS_EMPTY =
      "????? TsFileInsertionEvent?{}";
  public static final String IGNORED_TSFILEINSERTIONEVENT_IS_NOT_AN_INSTANCE_OF =
      "??? TsFileInsertionEvent is not an instance of PipeTsFileInsertionEvent: {}";
  public static final String ILLEGAL_OUTPUT_SERIES_PATH = "??? output series path: ";
  public static final String NO_DATA_NODES_ENDPOINTS_FETCHED = "?????? data node ? endpoint";
  public static final String NO_EXPECTED_REGION_ID_SET_FETCHED =
      "??????? region id ??";
  public static final String PARTIALPATHLASTOBJECTCACHE_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_FROM_TO =
      "PartialPathLastObjectCache.allocatedMemoryBlock ?? {} ??? {}?";
  public static final String PARTIALPATHLASTOBJECTCACHE_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_FROM_TO =
      "PartialPathLastObjectCache.allocatedMemoryBlock ?? {} ??? {}?";
  public static final String SENDING_REQUEST_WATERMARK_TO = "???? request {}?watermark = {}?? {}";
  public static final String SWINGINGDOORTRENDINGSAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH =
      "SwingingDoorTrendingSamplingProcessor ? {} ?????{}?{}, {}?{}, {}?{}?";
  public static final String THE_ABSTRACT_FORMAL_PROCESSOR_DOES_NOT_SUPPORT = "??????????????";
  public static final String TUMBLINGTIMESAMPLINGPROCESSOR_IN_IS_INITIALIZED_WITH_S =
      "TumblingTimeSamplingProcessor ? {} ?????{}?{}s, {}?{}, {}?{}?";
  public static final String TWOSTAGECOUNTPROCESSOR_CUSTOMIZED_BY_THREAD_PIPENAME_CREATIONTIME_RE =
      "??? {} ???? TwoStageCountProcessor?pipeName={}, creationTime={}, "
          + "regionId={}, outputSeries={}, localCommitProgressIndex={}, localCount={}";
  public static final String TWO_STAGE_AGGREGATE_PIPE_PIPENAME_CREATIONTIME_RELATED =
      "????? pipe?pipeName={}, creationTime={}???? region id {}";
  public static final String TWO_STAGE_AGGREGATE_RECEIVER_IS_EXITING =
      "????? receiver ?????";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID =
      "??????region id = {}, combine id = {}?????timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_1 =
      "??????region id = {}, combine id = {}?????timestamp={}, count={}, "
          + "progressIndex={}";
  public static final String TWO_STAGE_COMBINE_REGION_ID_COMBINE_ID_2 =
      "??????region id = {}, combine id = {}????timestamp={}, count={}, "
          + "progressIndex={}, committed progressIndex={}";
  public static final String UNEXPECTED_STATE_CLASS = "???? state class?";
  public static final String UNKNOWN_COMBINE_RESULT_TYPE = "??? combine result type?";
  public static final String UNKNOWN_REQUEST_TYPE = "??? request type {}?{}?";

  // ===================== SOURCE =====================

  public static final String ALL_DATA_IN_TSFILEEPOCH_WAS_EXTRACTED =
      "TsFileEpoch {} ?????????";
  public static final String BUFFERSIZE_MUST_BE_A_POWER_OF_2 = "bufferSize ??? 2 ??";
  public static final String BUFFERSIZE_MUST_NOT_BE_LESS_THAN_1 =
      "bufferSize ???? 1";
  public static final String CAPTURE_TREE_AND_CAPTURE_TABLE_CAN_NOT =
      "capture.tree ? capture.table ?????? false";
  public static final String DATABASE_NAME_IS_NULL_WHEN_MATCHING_SOURCES =
      "???????? source ?????????";
  public static final String DATA_REGION_INJECTED_WATERMARK_EVENT_WITH_TIMESTAMP =
      "Data region {}??? watermark ???timestamp?{}";
  public static final String DISCARD_TABLET_EVENT_BECAUSE_IT_IS_NOT =
      "?? tablet ?? {}?????????? TsFileEpoch ????? USING_BOTH?";
  public static final String DISRUPTOR_ALREADY_STARTED = "Disruptor ???";
  public static final String DISRUPTOR_SHUTDOWN_COMPLETED = "Disruptor ????";
  public static final String DISRUPTOR_STARTED_WITH_BUFFER_SIZE = "Disruptor ??????????{}";
  public static final String EXCEPTION_DURING_ONSHUTDOWN = "onShutdown() ??????";
  public static final String EXCEPTION_DURING_ONSTART = "onStart() ??????";
  public static final String EXCEPTION_ENCOUNTERED_WHEN_TRIGGERING_SCHEMA_REGION_SNAPSHOT =
      "?? schema region snapshot ??????";
  public static final String EXCEPTION_PROCESSING = "????????{} {}";
  public static final String FAILED_TO_LOAD_SNAPSHOT = "?? snapshot {} ??";
  public static final String FAILED_TO_LOAD_SNAPSHOT_FROM_BYTEBUFFER =
      "?? snapshot from byteBuffer {} ???";
  public static final String FAILED_TO_START_SOURCES = "?? sources ???";
  public static final String HEARTBEAT_EVENT_CAN_NOT_BE_SUPPLIED_BECAUSE =
      "Heartbeat Event {} ?????????????????";
  public static final String EVENT_CAN_NOT_BE_SUPPLIED_BECAUSE_DATA_IS_LOST =
      "Event %s ?????????????????????????????";
  public static final String INTERRUPTED_WAITING_FOR_PROCESSOR_TO_STOP =
      "?? processor ??????";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_PARSING_PRIVILEGE_FOR_TSFILE =
      "???? TsFile %s ??????????";
  public static final String INTERRUPTED_WHEN_WAITING_FOR_CLOSING_TSFILE =
      "?? TsFile %s ???????";
  public static final String PARSE_TSFILE_ERROR_BECAUSE = "?? TsFile %s ??????%s";
  public static final String PARSE_TSFILE_WHEN_CHECKING_PRIVILEGE_ERROR =
      "??????? TsFile %s ??????%s";
  public static final String READ_TSFILE_ERROR = "?? TsFile %s ???";
  public static final String IOTDBSCHEMAREGIONSOURCE_DOES_NOT_SUPPORT_TRANSFERRING_EVENTS_UNDER =
      "IoTDBSchemaRegionSource ???? simple consensus ?????";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_EVENT = "?????? event?";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN = "?????? plan?";
  public static final String NO_EVENT_HANDLER_CONFIGURED = "??? event handler";
  public static final String N_MUST_BE_0 = "n ?? > 0";
  public static final String PIPEREALTIMEDATAREGIONEXTRACTOR_OBSERVED_DATA_REGION_TIME_PARTITION_GROWT =
      "PipeRealtimeDataRegionExtractor({}) ??? data region {} ? time partition ???"
          + "?? time partition id ???{}?";
  public static final String PIPE_AND_IS_NOT_SET_USE_HYBRID =
      "Pipe?'{}'?'{}'?? '{}'?'{}'????????? hybrid ???";
  public static final String PIPE_ASSIGNER_ON_DATA_REGION_SHUTDOWN_INTERNAL =
      "Pipe?data region {} ?? Assigner ? {} ms ??? internal disruptor";
  public static final String PIPE_FAILED_TO_GET_DEVICES_FROM_TSFILE_1 =
      "Pipe {}@{}??? devices from TsFile {}, extract it anyway ??";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR =
      "Pipe {}@{}??? reference count for historical deletion event {}, will discard it ??";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_1 =
      "Pipe {}@{}??? reference count for historical tsfile event {}, will discard it ??";
  public static final String PIPE_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_2 =
      "Pipe {}@{}??? reference count for terminate event, will resend it ??";
  public static final String PIPE_FAILED_TO_PIN_TSFILERESOURCE = "Pipe??? TsFileResource {} ??";
  public static final String PIPE_FAILED_TO_START_TO_EXTRACT_HISTORICAL =
      "Pipe {}@{}??? to extract historical TsFile, storage engine is not ready. Will retry "
          + "later ???";
  public static final String PIPE_FAILED_TO_UNPIN_SKIPPED_HISTORICAL_TSFILERESOURCE =
      "Pipe {}@{}?unpin skipped historical TsFileResource, original path: {} ??";
  public static final String PIPE_FAILED_TO_UNPIN_TSFILERESOURCE_AFTER_CREATING =
      "Pipe {}@{}?unpin TsFileResource after creating event, original path: {} ??";
  public static final String PIPE_FAILED_TO_UNPIN_TSFILERESOURCE_AFTER_DROPPING =
      "Pipe {}@{}?unpin TsFileResource after dropping pipe, original path: {} ??";
  public static final String PIPE_FINISH_TO_EXTRACT_DELETIONS_EXTRACT_DELETIONS =
      "Pipe {}@{}?finish to extract deletions, extract deletions count {}/{}, took {} ms";
  public static final String PIPE_FINISH_TO_EXTRACT_HISTORICAL_TSFILE_EXTRACTED =
      "Pipe {}@{}?finish to extract historical TsFile, extracted sequence file count {}/{}, "
          + "extracted unsequence file count {}/{}, extracted file count {}/{}, took {} ms";
  public static final String PIPE_FINISH_TO_SORT_ALL_EXTRACTED_RESOURCES =
      "Pipe {}@{}?finish to sort all extracted resources, took {} ms";
  public static final String PIPE_HISTORICAL_DATA_EXTRACTION_TIME_RANGE_START =
      "Pipe {}@{}?historical data extraction time range, start time {}({}), end time {}({}), "
          + "sloppy pattern {}, sloppy time range {}, should transfer mod file {}, username: {}, "
          + "skip if no privileges: {}, is forwarding pipe requests: {}";
  public static final String PIPE_IS_SET_TO_FALSE_USE_HEARTBEAT =
      "Pipe?'{}'?'{}'???? false??? heartbeat ?? source?";
  public static final String PIPE_ON_DATA_REGION_SKIP_COMMIT_OF =
      "Pipe {} ? data region {} ??? event {} ??????????? flush?";
  public static final String PIPE_REALTIME_DATA_REGION_SOURCE_IS_INITIALIZED =
      "Pipe {}@{}?realtime data region source is initialized with parameters: {}.";
  public static final String PIPE_RESOURCE_MEETS_MAYTSFILECONTAINUNPROCESSEDDATA_CONDITION_EXTRACT =
      "Pipe {}@{}?resource {} meets mayTsFileContainUnprocessedData condition, extractor "
          + "progressIndex: {}, resource ProgressIndex: {}";
  public static final String PIPE_SET_WATERMARK_INJECTOR_WITH_INTERVAL_MS =
      "Pipe {}@{}?Set watermark injector with interval {} ms.";
  public static final String PIPE_SKIP_HISTORICAL_TSFILE_BECAUSE_REALTIME_SOURCE =
      "Pipe {}@{}?skip historical tsfile {} because realtime source in current task {} has "
          + "already captured it.";
  public static final String PIPE_SNAPSHOT_MODE_IS_ENABLED_USE_HEARTBEAT =
      "Pipe??????????? heartbeat ?? source?";
  public static final String PIPE_STARTED_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}?? {} ms ????? historical source {} and realtime source {}?";
  public static final String PIPE_STARTING_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}?Starting historical source {} and realtime source {}.";
  public static final String PIPE_START_HISTORICAL_SOURCE_AND_REALTIME_SOURCE =
      "Pipe {}@{}?Start historical source {} and realtime source {} ???";
  public static final String PIPE_START_TO_EXTRACT_DELETIONS = "Pipe {}@{}????? deletions";
  public static final String PIPE_START_TO_EXTRACT_HISTORICAL_TSFILE_ORIGINAL =
      "Pipe {}@{}????? historical TsFile, original sequence file count {}, original unSequence "
          + "file count {}, start progress index {}";
  public static final String PIPE_START_TO_FLUSH_DATA_REGION = "Pipe {}@{}????? data region";
  public static final String PIPE_START_TO_SORT_ALL_EXTRACTED_RESOURCES =
      "Pipe {}@{}????? all extracted resources";
  public static final String PIPE_TASK_CANNOTUSETABLETANYMORE_FOR_TSFILE_THE_MEMORY =
      "Pipe task {}@{} ? tsFile {} ?? canNotUseTabletAnyMore?insert node {} ????????? pipe {} ??????event count?{}";
  public static final String PIPE_UNEXPECTED_PROGRESSINDEX_TYPE_FALLBACK_TO_ORIGIN =
      "Pipe {}@{}?unexpected ProgressIndex type {}, fallback to origin {}.";
  public static final String PIPE_UNSUPPORTED_SOURCE_REALTIME_MODE_CREATE_A =
      "Pipe????? source realtime mode: {}, create a hybrid source?";
  public static final String PROCESSOR_INTERRUPTED = "??????";
  public static final String PROCESSOR_INTERRUPTED_UNEXPECTEDLY = "????????????";
  public static final String PROCESSOR_STOPPED = "??????";
  public static final String SET_FOR_HISTORICAL_DELETION_EVENT =
      "[{}]? historical deletion event {} ?? {}";
  public static final String SET_FOR_HISTORICAL_EVENT = "[{}]? historical event {} ?? {}";
  public static final String SET_FOR_REALTIME_EVENT = "[{}]? realtime event {} ?? {}";
  public static final String SOURCES_FILTERED_BY_DATABASE_AND_TABLE_IS =
      "? table model ???? source ??? database ? table ?????????";
  public static final String SOURCES_FILTERED_BY_DEVICE_IS_NULL_WHEN =
      "? tree model ???? source ??? device ?????????";
  public static final String TAKE_SNAPSHOT_ERROR = "???????{}";
  public static final String THE_ASSIGNER_QUEUE_CONTENT_HAS_EXCEEDED_HALF =
      "Assigner ???????????????????????"
          + "regionId?{}, capacity?{}, bufferSize?{}";
  public static final String THE_PIPE_CANNOT_EXTRACT_TABLE_MODEL_DATA =
      "sql dialect ??? tree ??pipe ???? table model ???";
  public static final String THE_PIPE_CANNOT_EXTRACT_TREE_MODEL_DATA =
      "sql dialect ??? table ??pipe ???? tree model ???";
  public static final String THE_REFERENCE_COUNT_OF_THE_EVENT_CANNOT =
      "?? {} ????????????????";
  public static final String THE_REFERENCE_COUNT_OF_THE_REALTIME_EVENT =
      "???? {} ????????????????";
  public static final String TIMED_OUT_WAITING_FOR_PROCESSOR_TO_STOP =
      "?? processor ????";
  public static final String TSFILEEPOCH_NOT_FOUND_FOR_TSFILE_CREATING_A =
      "??? TsFile {} ??? TsFileEpoch???????";
  public static final String WHEN_IS_SET_TO_FALSE_SPECIFYING_AND =
      "? '{}'?'{}'???? false ???? {} ? {} ???";
  public static final String WHEN_IS_SET_TO_TRUE_SPECIFYING_AND =
      "? '{}'?'{}'?'{}'?'{}'???? true ???? {} ? {} ???";
  public static final String WHEN_OR_IS_SPECIFIED_SPECIFYING_OR_IS_INVALID =
      "??? {}?{}?{} ? {} ???? {}?{}?{} ? {} ???";

  // ===================== SINK =====================

  public static final String ACQUIRE_IOPCITEMMGT_SUCCESSFULLY_INTERFACE_ADDRESS =
      "???? IOPCItemMgt! Interface address: {}";
  public static final String ACQUIRE_IOPCSYNCIO_SUCCESSFULLY_INTERFACE_ADDRESS =
      "???? IOPCSyncIO! Interface address: {}";
  public static final String ADDED_EVENT_TO_RETRY_QUEUE = "?? event {} ??? retry queue";
  public static final String BATCH_ID_CREATE_BATCH_DIR_SUCCESSFULLY_BATCH =
      "?? id = {}????? batch dir?batch file dir = {}?";
  public static final String BATCH_ID_DELETE_THE_TSFILE_AFTER_FAILED =
      "?? id = {}??? tablet ? {} ????{} ?? tsfile {}?{}";
  public static final String MESSAGE_MAYBE_THE_TSFILE_NEEDS_TO_BE_DELETED_MANUALLY_342E28E2 =
      "????????? tsfile?";
  public static final String BATCH_ID_FAILED_TO_BUILD_THE_TABLE =
      "?? id = {}??? table model TSFile ????????? Tablet ?????????"
          + "?? Table Schema ?????";
  public static final String BATCH_ID_FAILED_TO_CLOSE_THE_TSFILE =
      "?? id = {}??? tablet ????? tsfile {} ??????{}";
  public static final String BATCH_ID_FAILED_TO_CLOSE_THE_TSFILE_1 =
      "?? id = {}????? batch ??? tsfile {} ??????{}";
  public static final String BATCH_ID_FAILED_TO_CREATE_BATCH_FILE =
      "?? id = {}??? batch file dir {} ???";
  public static final String BATCH_ID_FAILED_TO_DELETE_THE_TSFILE =
      "?? id = {}????? batch ??? tsfile {} ??????{}";
  public static final String BATCH_ID_FAILED_TO_WRITE_TABLETS_INTO =
      "?? id = {}??? tablet ? tsfile ??????{}";
  public static final String BATCH_ID_SEAL_TSFILE_SUCCESSFULLY = "?? id = {}????? tsfile {}?";
  public static final String BATCH_ID_UNSUPPORTED_EVENT_TYPE_WHEN_CONSTRUCTING =
      "?? id = {}??? tsfile batch ??????? {} ?? {}";
  public static final String CANNOT_INCREASE_REFERENCE_COUNT_FOR_EVENT_IGNORE =
      "???? event {} ??????? batch ???";
  public static final String CANNOT_SERIALIZE_BOTH_TABLET_AND_STATEMENT_ARE =
      "??????tablet ? statement ???";
  public static final String CERTIFICATE_DIRECTORY_IS_PLEASE_MOVE_CERTIFICATES_FROM =
      "??????{}?????? reject ????? trusted ?????????";
  public static final String CLIENT_HAS_BEEN_RETURNED_TO_THE_POOL =
      "Client ?????????? handler ??? {}?????? {}?";
  public static final String CLOSED_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTES =
      "??? AsyncPipeDataTransferServiceClientManager for receiver attributes: {}";
  public static final String CREATE_GROUP_SUCCESSFULLY_SERVER_HANDLE_UPDATE_RATE =
      "?? group ???Server handle?{}, update rate?{} ms";
  public static final String DELETENODETRANSFER_NO_EVENT_SUCCESSFULLY_PROCESSED =
      "DeleteNodeTransfer?? {} ? event ?????";
  public static final String DESERIALIZE_PIPEDATA_ERROR_BECAUSE_UNKNOWN_TYPE =
      "???? PipeData ????????? ";
  public static final String DESERIALIZE_PIPEDATA_ERROR_BECAUSE_UNKNOWN_TYPE_1 =
      "???? PipeData ????????? {}?";
  public static final String ERROR_GETTING_OPC_CLIENT = "?? opc client ???";
  public static final String ERROR_PROGID_IS_INVALID_OR_UNREGISTERED_HRESULT =
      "???ProgID ???????(HRESULT=0x";
  public static final String ERROR_RUNNING_OPC_CLIENT = "?? opc client ???";
  public static final String EXCEPTION_OCCURRED_WHEN_PIPETABLEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO =
      "PipeTableModelTsFileBuilderV2 ? tsfile ?? tablet ??????"
          + "?? fallback tsfile builder?{}";
  public static final String EXCEPTION_OCCURRED_WHEN_PIPETREEMODELTSFILEBUILDERV2_WRITING_TABLETS_TO =
      "PipeTreeModelTsFileBuilderV2 ? tsfile ?? tablet ??????"
          + "?? fallback tsfile builder?{}";
  public static final String EXECUTE_STATEMENT_TO_DATABASE_SKIP_BECAUSE_NO =
      "???? {} ???? {} ?????????";
  public static final String FAILED_TO_ACQUIRE_IOPCITEMMGT_ERROR_CODE_0X =
      "?? IOPCItemMgt, error code: 0x ??";
  public static final String FAILED_TO_ACQUIRE_IOPCSYNCIO_ERROR_CODE_0X =
      "?? IOPCSyncIO, error code: 0x ??";
  public static final String FAILED_TO_ADD_ITEM = "?? item ?? ";
  public static final String FAILED_TO_ADD_ITEM_WIN_ERROR_CODE = "?? item ???win ????0x";
  public static final String FAILED_TO_ADJUST_TIMEOUT_WHEN_FAILED_TO =
      "????????????????";
  public static final String FAILED_TO_BORROW_CLIENT_FOR_CACHED_LEADER =
      "? cached leader ?? client {}:{} ???";
  public static final String HANDSHAKE_ERROR_WITH_RECEIVER =
      "???? {}:{} ?????????{}????{}?";
  public static final String HANDSHAKE_ERROR_WITH_RECEIVER_1 =
      "???? {}:{} ?????";
  public static final String HANDSHAKE_ERROR_BY_HANDSHAKE_V2_RETRY_WITH_V1 =
      "?? PipeTransferHandshakeV2Req ???? {}:{} ??????? PipeTransferHandshakeV1Req "
          + "?????";
  public static final String FAILED_TO_BUILD_AND_STARTUP_OPCUASERVER =
      "????? OpcUaServer ??";
  public static final String FAILED_TO_CLOSE_ASYNCPIPEDATATRANSFERSERVICECLIENTMANAGER_FOR_RECEIVER_ATTRIBUTE =
      "?? AsyncPipeDataTransferServiceClientManager for receiver attributes: {} ??";
  public static final String FAILED_TO_CLOSE_CLIENT_AFTER_HANDSHAKE_FAILURE =
      "?? client {}:{} after handshake failure when the manager is closed ???";
  public static final String FAILED_TO_CLOSE_CLIENT_MANAGER = "?? client manager ???";
  public static final String FAILED_TO_CLOSE_FILE_READER_OR_DELETE =
      "????????? file reader ??? tsFile ???";
  public static final String FAILED_TO_CLOSE_FILE_READER_OR_DELETE_1 =
      "????????? file reader ??? tsFile ???";
  public static final String FAILED_TO_CLOSE_FILE_READER_WHEN_SUCCESSFULLY =
      "???? mod ????? file reader ???";
  public static final String FAILED_TO_CLOSE_OR_INVALIDATE_CLIENT_WHEN =
      "connector ???????? client ???Client?{}, Exception?{}";
  public static final String FAILED_TO_CLOSE_TRUSTLISTMANAGER_BECAUSE =
      "?? trustListManager ??????{}?";
  public static final String FAILED_TO_CONNECT_TO_SERVER_ERROR_CODE =
      "?? server ???????0x";
  public static final String FAILED_TO_CONVERT_STATEMENT_TO_TABLET = "? statement ??? tablet ???";
  public static final String FAILED_TO_CONVERT_STATEMENT_TO_TABLET_FOR =
      "????? statement ??? tablet ??";
  public static final String FAILED_TO_CREATE_GROUP_ERROR_CODE_0X = "?? group ???????0x";
  public static final String FAILED_TO_CREATE_NODES_AFTER_TRANSFER_DATA =
      "?? data value ??? node ????????";
  public static final String FAILED_TO_DELETE_BATCH_FILE_THIS_FILE =
      "?? batch file {} ?????????????";
  public static final String FAILED_TO_GET_THE_SIZE_OF_PIPETRANSFERBATCHREQBUILDER =
      "?? PipeTransferBatchReqBuilder ??????? 0?Exception?{}";
  public static final String FAILED_TO_HANDSHAKE = "?????";
  public static final String FAILED_TO_LOG_ERROR_WHEN_FAILED_TO =
      "????????????????";
  public static final String FAILED_TO_PUSH_VALUE_CHANGE_TO_CLIENT =
      "? client ?? value ?????nodeId={}";
  public static final String FAILED_TO_SEND_INITIAL_VALUE_TO_NEW =
      "???????? value ???nodeId={}";
  public static final String FAILED_TO_SERIALIZE_PROGRESS_INDEX = "??? progress index {} ??";
  public static final String FAILED_TO_SHUTDOWN_EXECUTOR = "?? executor {} ???";
  public static final String FAILED_TO_TRANSFER_DATAVALUE = "?? dataValue ??";
  public static final String FAILED_TO_TRANSFER_DATAVALUE_AFTER_SUCCESSFULLY_CREATED =
      "???? node ??? dataValue ??";
  public static final String FAILED_TO_TRANSFER_PIPEDELETENODEEVENT_COMMITTER_KEY_REPLICATE =
      "?? PipeDeleteNodeEvent {} (committer key={}, replicate index={}) ???";
  public static final String FAILED_TO_TRANSFER_SLICE_RETRY_WHOLE_TRANSFER =
      "?? slice ????????{}-{}?????????";
  public static final String FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_COMMITTER_KEY_REPLICATE =
      "?? TabletInsertionEvent {} (committer key={}, replicate index={}) ???";
  public static final String FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_COMMITTER_KEY_COMMIT_ID =
      "?? TabletInsertionEvent {}?committer key={}?commit id={}????";
  public static final String FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_BATCH =
      "?? TabletInsertionEvent ????????????{}??? pipe ???{}";
  public static final String FAILED_TO_TRANSFER_TSFILE_BATCH = "?? tsfile batch ({}) ???";
  public static final String FAILED_TO_TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY =
      "?? tsfile event {} asynchronously ???";
  public static final String FAILED_TO_TRANSFER_TSFILEINSERTIONEVENT_COMMITTER_KEY_COMMIT_ID =
      "?? TsFileInsertionEvent {}?committer key {}?commit id {}????";
  public static final String FAILED_TO_TRANSFER_TSFILEINSERTIONEVENT_BATCHED_TABLE_EVENTS =
      "?? TsFileInsertionEvent {}??? TableInsertionEvent????";
  public static final String FAILED_TO_UPDATE_LEADER_CACHE_FOR_DEVICE =
      "?? leader cache for device {} with endpoint {}:{} ???";
  public static final String FAILED_TO_WRITE = "???? ";
  public static final String FAILED_TO_WRITE_WIN_ERROR_CODE_0X =
      "?????win ????0x";
  public static final String GENERATE_STATEMENT_FROM_TABLET_ERROR = "? tablet {} ?? Statement ???";
  public static final String GOT_AN_ERROR_FROM = "? {}:{} ???? \\\"{}\\\"?";
  public static final String GOT_AN_ERROR_FROM_AN_UNKNOWN_CLIENT =
      "??? client ???? \\\"{}\\\"?";
  public static final String HANDSHAKE_SUCCESSFULLY_WITH_RECEIVER =
      "? receiver {}:{} ?????";
  public static final String ILLEGAL_STATE_WHEN_RETURN_THE_CLIENT_TO =
      "?? client ??????????????????????????";
  public static final String INSERTNODETRANSFER_NO_EVENT_SUCCESSFULLY_PROCESSED =
      "InsertNodeTransfer?? {} ? event ?????";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_HANDSHAKE_RESPONSE =
      "waiting for handshake response ?????";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTConsensusV2AsyncConnector ??? transferring generic event: {}.";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFER_GENERIC_EVENT =
      "IoTConsensusV2AsyncConnector ??? transfer generic event: {}.";
  public static final String IOTCONSENSUSV2ASYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVEN =
      "IoTConsensusV2AsyncConnector ??? PipeTsFileInsertionEvent??????{}?";
  public static final String IOTCONSENSUSV2CONNECTOR_TRANSFERBUFFER_QUEUE_OFFER_IS_INTERRUPTED =
      "IoTConsensusV2Connector transferBuffer ????????";
  public static final String IOTCONSENSUSV2TRANSFERBATCHREQBUILDER_THE_MAX_BATCH_SIZE_IS_ADJUSTED =
      "IoTConsensusV2TransferBatchReqBuilder????????? batch ???? {} ??? {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_NOT_FOUND_IN_TRANSFERBUFFER =
      "IoTConsensusV2-ConsensusGroup-{}?? transferBuffer ???? event-{}??????"
          + "queue size = {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_REPLICATE_INDEX_TRANSFER_FAILED =
      "IoTConsensusV2-ConsensusGroup-{}?Event {} replicate index {} ?????"
          + "?? retry queue ???? event ?????";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_EVENT_REPLICATE_INDEX_TRANSFER_FAILED_1 =
      "IoTConsensusV2-ConsensusGroup-{}?Event {} replicate index {} ?????"
          + "??? retry queue?";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_NO_EVENT_ADDED_TO_CONNECTOR =
      "IoTConsensusV2-ConsensusGroup-{}?? {} ? event-{} ??? connector buffer";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_ONE_EVENT_SUCCESSFULLY_RECEIVED_BY =
      "IoTConsensusV2-ConsensusGroup-{}??? event-{} ?? follower ?????"
          + "????????queue size = {}, limit size = {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_RETRYEVENTQUEUE_IS_NOT_EMPTY_AFTER =
      "IoTConsensusV2-ConsensusGroup-{}?20 ?? retryEventQueue ?????"
          + "retryQueue size?{}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_RETRY_WITH_INTERVAL_FOR_INDEX =
      "IoTConsensusV2-ConsensusGroup-{}???? {} ?? index {} {}";
  public static final String IOTCONSENSUSV2_CONSENSUSGROUP_TRY_TO_REMOVE_EVENT_AFTER =
      "IoTConsensusV2-ConsensusGroup-{}?iotConsensusV2AsyncConnector ???"
          + "???? event-{}???????";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN =
      "IoTConsensusV2-{}??? file reader when failed to transfer file ???";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN_1 =
      "IoTConsensusV2-{}??? file reader when successfully transferred file ???";
  public static final String IOTCONSENSUSV2_FAILED_TO_CLOSE_FILE_READER_WHEN_2 =
      "IoTConsensusV2-{}??? file reader when successfully transferred mod file ???";
  public static final String IOTCONSENSUSV2_FAILED_TO_TRANSFER_TABLETINSERTIONEVENT_BATCH_TOTAL =
      "IoTConsensusV2??? TabletInsertionEvent batch. Total failed events: {}, related pipe "
          + "names: {} ??";
  public static final String IOTCONSENSUSV2_FAILED_TO_TRANSFER_TSFILEINSERTIONEVENT_COMMITTER_KEY =
      "IoTConsensusV2-{}??? TsFileInsertionEvent {} (committer key {}, replicate index {}) ???";
  public static final String IOTCONSENSUSV2_REDIRECT_FILE_POSITION_TO =
      "IoTConsensusV2-{}?Redirect file position to {}.";
  public static final String IOTCONSENSUSV2_SUCCESSFULLY_TRANSFERRED_FILE_COMMITTER_KEY_REPLICATE =
      "IoTConsensusV2-{}????? file {}?committer key={}, replicate index={}??";
  public static final String IOTDBCDCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTAB =
      "IoTDBCDCConnector ??? PipeInsertNodeTabletInsertionEvent ? PipeRawTabletInsertionEvent?";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBDataRegionAirGapConnector ??? transferring generic event: {}.";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_A =
      "IoTDBDataRegionAirGapConnector ??? PipeInsertNodeTabletInsertionEvent ? PipeRawTabletInsertionEvent??? {}?";
  public static final String IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE =
      "IoTDBDataRegionAirGapConnector ??? PipeTsFileInsertionEvent??? {}?";
  public static final String FAILED_TO_LOGIN_TO_RECEIVER_FOR_LEGACY_PIPE_TRANSFER =
      "?? receiver %s:%s for legacy pipe transfer ??????code: %d, message: %s";
  public static final String IOTDBLEGACYPIPECONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBLegacyPipeConnector ??? transferring generic event: {}.";
  public static final String IOTDBLEGACYPIPECONNECTOR_ONLY_SUPPORT_PIPEINSERTNODEINSERTIONEVENT_AND_PIPETABLE =
      "IoTDBLegacyPipeConnector ??? PipeInsertNodeInsertionEvent ? PipeTabletInsertionEvent?";
  public static final String IOTDBLEGACYPIPECONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT =
      "IoTDBLegacyPipeConnector ??? PipeTsFileInsertionEvent?";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBSchemaRegionAirGapSink ???? TabletInsertionEvent?";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBSchemaRegionAirGapSink ???? TsFileInsertionEvent?";
  public static final String IOTDBSCHEMAREGIONAIRGAPSINK_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBSchemaRegionAirGapSink ??? transferring generic event: {}.";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBSchemaRegionConnector ???? TabletInsertionEvent?";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBSchemaRegionConnector ???? TsFileInsertionEvent?";
  public static final String IOTDBSCHEMAREGIONCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBSchemaRegionConnector ??? transferring generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBThriftAsyncConnector ??? transferring generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFER_GENERIC_EVENT =
      "IoTDBThriftAsyncConnector ??? transfer generic event: {}.";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PI =
      "IoTDBThriftAsyncConnector ??? PipeInsertNodeTabletInsertionEvent ? PipeRawTabletInsertionEvent??????{}?";
  public static final String IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVENT =
      "IoTDBThriftAsyncConnector ??? PipeTsFileInsertionEvent??????{}?";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBThriftSyncConnector ??? transferring generic event: {}.";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIP =
      "IoTDBThriftSyncConnector ??? PipeInsertNodeTabletInsertionEvent ? PipeRawTabletInsertionEvent??? {}?";
  public static final String IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE =
      "IoTDBThriftSyncConnector ??? PipeTsFileInsertionEvent??? {}?";
  public static final String LEADERCACHEMANAGER_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_FROM_TO =
      "LeaderCacheManager.allocatedMemoryBlock ?? {} ??? {}?";
  public static final String LEADERCACHEMANAGER_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_FROM_TO =
      "LeaderCacheManager.allocatedMemoryBlock ?? {} ??? {}?";
  public static final String LOADING_KEYSTORE_AT = "??? {} ?? KeyStore";
  public static final String LOADING_KEYSTORE_AT_1 = "??? {}. ?? KeyStore";
  public static final String LOAD_KEYSTORE_FAILED_THE_EXISTING_KEYSTORE_MAY =
      "?? keyStore ????? keyStore ????????????...";
  public static final String NO_OPC_CLIENT_OR_SERVER_IS_SPECIFIED =
      "?? tablet ???? OPC client ? server";
  public static final String OPC_DA_SINK_MUST_RUN_ON_WINDOWS = "opc-da-sink ??? Windows ??????";
  public static final String PIPETABLEMODETSFILEBUILDERV2_DOES_NOT_SUPPORT_TREE_MODEL_TABLET =
      "PipeTableModeTsFileBuilderV2 ??? tree model tablet to build TSFile";
  public static final String PIPETABLEMODETSFILEBUILDER_DOES_NOT_SUPPORT_TREE_MODEL_TABLET =
      "PipeTableModeTsFileBuilder ??? tree model tablet to build TSFile";
  public static final String PIPETREEMODELTSFILEBUILDERV2_DOES_NOT_SUPPORT_TABLE_MODEL_TABLET =
      "PipeTreeModelTsFileBuilderV2 ??? table model tablet to build TSFile";
  public static final String PIPETREEMODELTSFILEBUILDER_DOES_NOT_SUPPORT_TABLE_MODEL_TABLET =
      "PipeTreeModelTsFileBuilder ??? table model tablet to build TSFile";
  public static final String POLLED_EVENT_FROM_RETRY_QUEUE = "? retry queue ?? event {}?";
  public static final String RECEIVED_AN_ERROR_MESSAGE_FROM =
      "? {}:{} ?????? {}";
  public static final String RECEIVED_AN_UNKNOWN_MESSAGE_FROM =
      "? {}:{} ?????? {}";
  public static final String RECEIVED_A_ACK_MESSAGE_FROM = "? {}:{} ?? ack ??";
  public static final String RECEIVED_A_BIND_MESSAGE_FROM = "? {}:{} ?? bind ??";
  public static final String REDIRECT_FILE_POSITION_TO = "??? file position ? {}?";
  public static final String REDIRECT_TO_POSITION_IN_TRANSFERRING_TSFILE =
      "???? position {}?????? TsFile ? {}?";
  public static final String NETWORK_FAILED_TO_RECEIVE_TSFILE_STATUS =
      "???? TsFile %s ??????%s";
  public static final String SECURITY_DIR = "security ???{}";
  public static final String SECURITY_PKI_DIR = "security pki ???{}";
  public static final String
      LOG_OPC_UA_ENDPOINT_SELECTED_CONFIGURED_ARG_ADVERTISED_ARG_EFFECTIVE_ARG_ALLOWENDPOINTREDIRECT_ARG_4FE076CB =
          "??? OPC UA endpoint?configured={}?advertised={}?effective={}?allowEndpointRedirect={}?";
  public static final String SSL_TRUST_STORE_PAIR_REQUIRED_WHEN_SSL_ENABLED =
      "? %s ? %s ? true ?????????????? trust-store ????%s ? %s?%s ? %s?? %s ? %s";
  public static final String SSL_KEY_STORE_PATH_AND_PASSWORD_MUST_BE_SPECIFIED_TOGETHER =
      "SSL key-store ??????????????????%s ? %s?%s ? %s?? %s ? %s";
  public static final String SUCCESSFULLY_ADDED_ITEM = "???? item {}?";
  public static final String SUCCESSFULLY_CONVERTED_PROGID_TO_CLSID =
      "??? progID {} ??? CLSID: {{}}";
  public static final String SUCCESSFULLY_SHUTDOWN_EXECUTOR = "???? executor {}?";
  public static final String SUCCESSFULLY_TRANSFERRED_DELETION_EVENT =
      "???? deletion event {}?";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE = "???? file {}?";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_AND =
      "???? file {}?{} ? {}?";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_BATCHED_TABLEINSERTIONEVENTS_REFERENCE_COUNT =
      "???? file {}??? TableInsertionEvents?????={}??";
  public static final String SUCCESSFULLY_TRANSFERRED_FILE_COMMITTER_KEY_COMMIT_ID =
      "???? file {}?committer key={}, commit id={}, ????={}??";
  public static final String SUCCESSFULLY_TRANSFERRED_SCHEMA_EVENT =
      "???? schema event {}?";
  public static final String SUCCESSFULLY_TRANSFERRED_SCHEMA_REGION_SNAPSHOT_AND =
      "???? schema region ?? {}?{} ? {}?";
  public static final String THE_BATCH_SIZE_LIMIT_HAS_EXPANDED_FROM =
      "batch ?????? {} ??? {}?";
  public static final String THE_BATCH_SIZE_LIMIT_HAS_SHRUNK_FROM =
      "batch ?????? {} ??? {}?";
  public static final String THE_DEFAULT_QUALITY_CAN_ONLY_BE_GOOD =
      "?? quality ??? 'GOOD'?'BAD' ? 'UNCERTAIN'?";
  public static final String THE_EVENT_ACK_IS_NOT_FOUND = "??? event ack {}?";
  public static final String THE_EVENT_CAN_T_BE_TRANSFERRED_TO =
      "?? {} ????? client???????";
  public static final String THE_EVENT_IN_ERROR_IS_NOT_FOUND =
      "?????? event {}?";
  public static final String THE_EVENT_POLLED_FROM_THE_QUEUE_IS =
      "??? poll ?? event ? peek ??????Peeked event?{}?polled event?{}?";
  public static final String THE_FILE_IS_NOT_FOUND_MAY_ALREADY =
      "??? file {}????????";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT =
      "pipe {} ?? drop?event ack {} ?????";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT_1 =
      "pipe {} ?? drop???? event {} ?????";
  public static final String THE_PIPE_WAS_DROPPED_SO_THE_EVENT_2 =
      "pipe {} ?? drop?event {} ?????";
  public static final String THE_QUALITY_VALUE_ONLY_SUPPORTS_BOOLEAN_TYPE =
      "quality ???? boolean ????? true == GOOD?false == BAD?";
  public static final String THE_SCHEMA_REGION_AIR_GAP_CONNECTOR_DOES =
      "The schema region air gap connector ??? transferring single file piece bytes.";
  public static final String THE_SCHEMA_REGION_CONNECTOR_DOES_NOT_SUPPORT =
      "The schema region connector ??? transferring single file piece req.";
  public static final String THE_SECURITY_POLICY_CANNOT_BE_EMPTY =
      "?????????";
  public static final String THE_SECURITY_POLICY_CAN_ONLY_BE_NONE =
      "??????? 'None'?'Basic128Rsa15'?'Basic256'?'Basic256Sha256'?'Aes128_Sha256_RsaOaep' ? 'Aes256_Sha256_RsaPss'?";
  public static final String THE_SEGMENTS_OF_TABLETS_MUST_EXIST =
      "tablet ? segment ????";
  public static final String THE_TABLET_OF_COMMITID_CAN_T_BE =
      "commitId ? {} ? tablet ??? client ?????????";
  public static final String THE_TRANSFER_THREAD_IS_INTERRUPTED = "????????";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN =
      "?? client ? websocket ??????code ? {}???? {}?????????{}";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN_1 =
      "?? client {}:{} ? websocket ??????code ? {}???? {}?????????{}";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_CLIENT_HAS_BEEN_2 =
      "?? client {}:{} ? websocket ??????";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_HAS_BEEN_CLOSED =
      "?? {}:{} ? websocket ?????????? commitId ? {} ? ack ???";
  public static final String THE_WEBSOCKET_CONNECTION_FROM_HAS_BEEN_CLOSED_1 =
      "?? {}:{} ? websocket ?????????? commitId ? {} ? error ???";
  public static final String THE_WEBSOCKET_SERVER_HAS_BEEN_STARTED =
      "websocket server {}:{} ????";
  public static final String THE_WRITTEN_TABLET_TIME_MAY_OVERLAP_OR =
      "??? Tablet ???????? Schema ?????";
  public static final String THIS_CONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLET =
      "? Connector ??? PipeInsertNodeTabletInsertionEvent ? PipeRawTabletInsertionEvent??? {}?";
  public static final String TIMED_OUT_WHEN_WAITING_FOR_CLIENT_HANDSHAKE =
      "?? client ???????";
  public static final String TIOTCONSENSUSV2BATCHTRANSFERRESP_IS_NULL =
      "TIoTConsensusV2BatchTransferResp ??";
  public static final String TIOTCONSENSUSV2TRANSFERRESP_IS_NULL = "TIoTConsensusV2TransferResp ??";
  public static final String TPIPETRANSFERRESP_IS_NULL = "TPipeTransferResp ??";
  public static final String OPC_UA_SINK_MODEL_MUST_BE_CLIENT_SERVER_WHEN_OUTER_OR_WITH_QUALITY =
      "? OPC UA sink ???? server ?? 'with-quality' ??? true ??%s ? %s ??? %s?";
  public static final String WITH_QUALITY_MEASUREMENT_MUST_BE_VALUE_OR_QUALITY_NAME =
      "?? 'with-quality' ????measurement ??? \"value-name\" ? \"quality-name\"?";
  public static final String SESSION_FAILED_TO_CHECK_AUTHORITY_FOR_STATEMENT =
      "Session {}: ?? statement {} ?????username = {}?response = {}?";
  public static final String TRANSFER_REQUEST_BODY_TOO_LARGE_WILL_BE_SLICED =
      "?????????????????{}-{}???????{}????{}";
  public static final String TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY_WAS_INTERRUPTED =
      "???? tsfile event {} ????";
  public static final String UNABLE_TO_CREATE_SECURITY_DIR = "???? security dir: ";
  public static final String UNKNOWN_LOAD_BALANCE_STRATEGY_USE_ROUND_ROBIN =
      "??? load balance strategy: {}, use round-robin strategy instead?";
  public static final String UNSUPPORTED_BATCH_TYPE = "???? batch type {}?";
  public static final String UNSUPPORTED_BATCH_TYPE_WHEN_TRANSFERRING_TABLET_INSERTION =
      "???? batch type {} when transferring tablet insertion event?";
  public static final String UNSUPPORTED_DATATYPE = "???? dataType ";
  public static final String UNSUPPORTED_EVENT_TYPE_WHEN_BUILDING_TRANSFER_REQUEST =
      "???? event {} type {} when building transfer request";
  public static final String WAIT_FOR_RESOURCE_ENOUGH_FOR_SLICING_TSFILE =
      "?? resource enough???? slicing tsfile {} for {} ??";
  public static final String WEBSOCKETCONNECTOR_FAILED_TO_INCREASE_THE_REFERENCE_COUNT =
      "WebsocketConnector ???????????????????????{}?";
  public static final String WEBSOCKETCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTA =
      "WebsocketConnector ??? PipeInsertNodeTabletInsertionEvent ? PipeRawTabletInsertionEvent??????{}?";
  public static final String WEBSOCKETCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVENT =
      "WebsocketConnector ??? PipeTsFileInsertionEvent??????{}?";
  public static final String WHEN_THE_OPC_UA_SINK_POINTS_TO =
      "? OPC UA sink ???? server ????? table model ???";
  public static final String WHEN_THE_OPC_UA_SINK_SETS_WITH =
      "? OPC UA sink ? 'with-quality' ??? true ????? table model ???";
  public static final String WRITEBACKSINK_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLETI =
      "WriteBackSink ??? PipeInsertNodeTabletInsertionEvent ? PipeRawTabletInsertionEvent??? {}?";

  // ===================== RECEIVER =====================

  public static final String ALL_RECEIVERS_RELATED_TO_ARE_RELEASED =
      "? {} ????? Receiver ????";
  public static final String AUTO_CREATE_DATABASE_FAILED_BECAUSE = "???? database failed because: ";
  public static final String CREATE_DATABASE_ERROR_STATEMENT_RESULT_STATUS =
      "?? Database error, statement: {}, result status : {}.";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_FOR_LOADTSFILESTATEMENT =
      "LoadTsFileStatement?{} ??????????????????";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_FOR_STATEMENT =
      "statement?{} ??????????????????";
  public static final String DATABASE_NAME_IS_UNEXPECTEDLY_NULL_SKIP_DATA_TYPE_CONVERSION =
      "Pipe??????????????????";
  public static final String DATA_TYPE_CONVERSION_FOR_LOADTSFILESTATEMENT_IS_SUCCESSFUL =
      "LoadTsFileStatement {} ??????????";
  public static final String DATA_TYPE_MISMATCH_DETECTED_TSSTATUS_FOR_LOADTSFILESTATEMENT =
      "LoadTsFileStatement?{} ???????????TSStatus?{}???????????";
  public static final String DELETE_ERROR_STATEMENT = "?? {} ???statement?{}?";
  public static final String DELETE_RESULT_STATUS = "???????{}?";
  public static final String FAILED_TO_CLOSE_IOTDBAIRGAPRECEIVERAGENT_S_SERVER_SOCKET =
      "?? IoTDBAirGapReceiverAgent's server socket ??";
  public static final String FAILED_TO_CONVERT_DATA_TYPE_FOR_LOADTSFILESTATEMENT =
      "?? data type for LoadTsFileStatement: {} ???";
  public static final String FAILED_TO_EXECUTE_STATEMENT_AFTER_DATA_TYPE =
      "execute statement after data type conversion ???";
  public static final String
      FAILED_TO_EXECUTE_STATEMENT_AFTER_DATA_TYPE_CONVERSION_WITH_EXCEPTION_TYPE =
          "Pipe??????????? statement ????????{}?";
  public static final String FAILED_TO_PARSE_ROW_VALUE_DURING_DATA_TYPE_CONVERSION =
      "Pipe?????????? row value ????????{}?";
  public static final String FAILED_TO_HANDLE_CONFIG_CLIENT_ID_EXIT =
      "?? config client (id = {}) exit ??";
  public static final String FAIL_TO_CREATE_IOTCONSENSUSV2_RECEIVER_FILE_FOLDERS =
      "?? iotConsensusV2 receiver file folders allocation strategy ??????all disks of folders "
          + "are full.";
  public static final String FAIL_TO_CREATE_PIPE_RECEIVER_FILE_FOLDERS =
      "?? pipe receiver file folders allocation strategy ??????all disks of folders are full.";
  public static final String FAIL_TO_INITIATE_FILE_BUFFER_FOLDER_ERROR =
      "??? file buffer folder, Error msg: {} ??";
  public static final String FAIL_TO_LOAD_PIPEDATA_BECAUSE = "?? pipeData ??????{}.";
  public static final String FAIL_TO_RENAME_FILE_TO = "rename file {} to {} ??";
  public static final String INVOKE_HANDSHAKE_METHOD_FROM_CLIENT_IP =
      "? client ip = {} ?? handshake ??";
  public static final String INVOKE_TRANSPORTDATA_METHOD_FROM_CLIENT_IP =
      "? client ip = {} ?? transportData ??";
  public static final String INVOKE_TRANSPORTPIPEDATA_METHOD_FROM_CLIENT_IP =
      "? client ip = {} ?? transportPipeData ??";
  public static final String IOTCONSENSUSV2RECEIVER_THREAD_IS_INTERRUPTED_WHEN_WAITING_FOR =
      "IoTConsensusV2Receiver ????? receiver ?????????????????";
  public static final String IOTCONSENSUSV2_PIPENAME = "IoTConsensusV2-PipeName-{}?{}";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WAITING_IS_INTERRUPTED_ONSYNCEDCOMMITINDEX =
      "IoTConsensusV2-PipeName-{}?current waiting is interrupted. onSyncedCommitIndex: {}. "
          + "Exception: ";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WRITING_FILE_WRITER_IS =
      "IoTConsensusV2-PipeName-{}?Current writing file writer ????????";
  public static final String IOTCONSENSUSV2_PIPENAME_CURRENT_WRITING_FILE_WRITER_WAS =
      "IoTConsensusV2-PipeName-{}?Current writing file writer {} ???.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CLOSE_CURRENT_WRITING =
      "IoTConsensusV2-PipeName-{}??? current writing file writer {} ??????{}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE =
      "IoTConsensusV2-PipeName-{}??? receiver file dir {} ???";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE_1 =
      "IoTConsensusV2-PipeName-{}??? receiver file dir {}. Because parent system dir have been "
          + "deleted due to system concurrently exit ???";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_FILE_2 =
      "IoTConsensusV2-PipeName-{}??? receiver file dir {}. May ??????authority or dir already "
          + "exists etc.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_CREATE_RECEIVER_TSFILEWRITER =
      "IoTConsensusV2-PipeName-{}??? receiver tsFileWriter-{} file dir {} ??";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_DELETE_BECAUSE =
      "IoTConsensusV2-PipeName-{}?{} Failed to delete {}, because {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_GET_BASE_DIRECTORY =
      "IoTConsensusV2-PipeName-{}??? base directory ??";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_LOAD_FILE_FROM =
      "IoTConsensusV2-PipeName-{}??? file {} from req {} ???";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_READ_TSFILE_WHEN =
      "IoTConsensusV2-PipeName-{}??? TsFile when counting points: {} ???";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_RETURN_TSFILEWRITER =
      "IoTConsensusV2-PipeName-{}?return tsFileWriter {} ???";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE =
      "IoTConsensusV2-PipeName-{}??? file {} ??????the file does not exist.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE_1 =
      "IoTConsensusV2-PipeName-{}??? file {} ??????writing file is {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_BECAUSE_2 =
      "IoTConsensusV2-PipeName-{}??? file {} ??????{}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_FROM =
      "IoTConsensusV2-PipeName-{}??? file {} from req {} ???";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_STATUS =
      "IoTConsensusV2-PipeName-{}??? file {}, status is {} ???";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_WHEN =
      "IoTConsensusV2-PipeName-{}??? file {} when check final seal file ??????the length of "
          + "file is not correct. The original file has length {}, but receiver file has length {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_SEAL_FILE_WHEN_1 =
      "IoTConsensusV2-PipeName-{}??? file {} when check non final seal ??????the length of "
          + "file is not correct. The original file has length {}, but receiver file has length {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_FAILED_TO_WRITE_FILE_PIECE =
      "IoTConsensusV2-PipeName-{}??? file piece from req {} ???";
  public static final String IOTCONSENSUSV2_PIPENAME_FILE_OFFSET_RESET_REQUESTED_BY =
      "IoTConsensusV2-PipeName-{}?File offset reset requested by receiver, response status = {}.";
  public static final String IOTCONSENSUSV2_PIPENAME_ILLEGAL_FILE_NAME_WHEN_CHECKING =
      "IoTConsensusV2-PipeName-{}???? file name {} when checking writing file?";
  public static final String IOTCONSENSUSV2_PIPENAME_IS_NOT_EXISTED_NO_NEED =
      "IoTConsensusV2-PipeName-{}?{} {} ?????????";
  public static final String IOTCONSENSUSV2_PIPENAME_NO_EVENT_GET_EXECUTED_AFTER =
      "IoTConsensusV2-PipeName-{}?? {} ? event ??????????? "
          + "receiver syncIndex?{}";
  public static final String IOTCONSENSUSV2_PIPENAME_NO_EVENT_GET_EXECUTED_BECAUSE =
      "IoTConsensusV2-PipeName-{}?? {} ? event ?????? receiver buffer ??? >= "
          + "pipeline??? receiver syncIndex {}, ?? buffer ?? {}";
  public static final String IOTCONSENSUSV2_PIPENAME_PATH_TRAVERSAL_ATTEMPT_DETECTED_FILENAME =
      "IoTConsensusV2-PipeName-{}?Path traversal attempt detected! Filename: {}";
  public static final String IOTCONSENSUSV2_PIPENAME_PROCESS_NO_EVENT_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}?process no.{} event successfully!";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVED_A_DEPRECATED_REQUEST_WHICH =
      "IoTConsensusV2-PipeName-{}??????? request-{}????? {}? ";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_DETECTED_AN_NEWER_PIPETASKRESTARTTIMES =
      "IoTConsensusV2-PipeName-{}?receiver ?????? pipeTaskRestartTimes?"
          + "?? pipe task ????receiver ?????????";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_DETECTED_AN_NEWER_REBOOTTIMES =
      "IoTConsensusV2-PipeName-{}?receiver ?????? rebootTimes?"
          + "?? leader ????receiver ?????????";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_FILE_DIR_WAS_CREATED =
      "IoTConsensusV2-PipeName-{}?Receiver file dir {} ????";
  public static final String IOTCONSENSUSV2_PIPENAME_RECEIVER_THREAD_GET_INTERRUPTED_WHEN =
      "IoTConsensusV2-PipeName-{}?receiver ??????????";
  public static final String IOTCONSENSUSV2_PIPENAME_SEAL_FILE_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}????? file {}?";
  public static final String IOTCONSENSUSV2_PIPENAME_SEAL_FILE_WITH_MODS_SUCCESSFULLY =
      "IoTConsensusV2-PipeName-{}????? file with mods {}?";
  public static final String IOTCONSENSUSV2_PIPENAME_SKIP_LOAD_TSFILE_WHEN_SEALING =
      "IoTConsensusV2-PipeName-{}???????? tsfile-{}???? region "
          + "????????";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_PIECES =
      "IoTConsensusV2-PipeName-{}????? tsFile pieces";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_SEAL =
      "IoTConsensusV2-PipeName-{}????? tsFile seal";
  public static final String IOTCONSENSUSV2_PIPENAME_STARTING_TO_RECEIVE_TSFILE_SEAL_1 =
      "IoTConsensusV2-PipeName-{}????? tsFile seal with mods";
  public static final String IOTCONSENSUSV2_PIPENAME_START_TO_RECEIVE_NO_EVENT =
      "IoTConsensusV2-PipeName-{}????? no.{} event";
  public static final String IOTCONSENSUSV2_PIPENAME_THE_POINT_COUNT_OF_TSFILE =
      "IoTConsensusV2-PipeName-{}?sender ??? TsFile {} ?????? TsFile ???????";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILEWRITER_RETURNED_SELF =
      "IoTConsensusV2-PipeName-{}?tsFileWriter-{} ????";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILEWRITER_ROLL_TO_WRITING_PATH =
      "IoTConsensusV2-PipeName-{}?tsfileWriter-{} ??? writing path {}";
  public static final String IOTCONSENSUSV2_PIPENAME_TSFILE_WRITER_IS_CLEANED_UP =
      "IoTConsensusV2-PipeName-{}?tsfile writer-{} ?????????????????";
  public static final String IOTCONSENSUSV2_PIPENAME_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "IoTConsensusV2-PipeName-{}???? PipeRequestType, response status = {}?";
  public static final String IOTCONSENSUSV2_PIPENAME_WAS_DELETED =
      "IoTConsensusV2-PipeName-{}?{} {} ???.";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_IS_NOT_AVAILABLE =
      "IoTConsensusV2-PipeName-{}?Writing file {} ????Writing file ? null?{}, writing file "
          + "?????{}, writing file writer ??? null?{}?";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_IS_NOT_EXISTED =
      "IoTConsensusV2-PipeName-{}?Writing file {} ????????????????? writing file ? {}?";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_S_OFFSET_IS =
      "IoTConsensusV2-PipeName-{}?Writing file {} ? offset ? {}???? sender ? offset "
          + "? {}?";
  public static final String IOTCONSENSUSV2_PIPENAME_WRITING_FILE_WAS_CREATED_READY =
      "IoTConsensusV2-PipeName-{}?Writing file {} ???????? file piece?";
  public static final String IOTCONSENSUSV2_RECEIVE_ON_THE_FLY_NO_EVENT =
      "IoTConsensusV2-{}?data region ????? on-the-fly ?? {} ? event?????";
  public static final String IOTCONSENSUSV2_TRANSFER_BATCH_HASN_T_BEEN_IMPLEMENTED =
      "IoTConsensusV2 transfer batch ?????";
  public static final String IOTCONSENSUSV2_TSFILEWRITER_SET_NULL_WRITING_FILE =
      "IoTConsensusV2-{}?TsFileWriter-{} ?? writing file ? null";
  public static final String IOTCONSENSUSV2_TSFILEWRITER_SET_NULL_WRITING_FILE_WRITER =
      "IoTConsensusV2-{}?TsFileWriter-{} ?? writing file writer ? null";
  public static final String IOTCONSENSUSV2_UNKNOWN_IOTCONSENSUSV2REQUESTVERSION_RESPONSE_STATUS =
      "IoTConsensusV2???? IoTConsensusV2RequestVersion, response status = {}?";
  public static final String IOTCONSENSUSV2_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "IoTConsensusV2 ??? PipeRequestType?response status = {}?";
  public static final String IOTCONSENSUSV2_WAITING_FOR_THE_PREVIOUS_EVENT_TIMES =
      "IoTConsensusV2-{}?????? event ????? peek {}, ?? id {}";
  public static final String IOTDBAIRGAPRECEIVERAGENT_STARTED =
      "IoTDBAirGapReceiverAgent {} ????";
  public static final String IOTDBAIRGAPRECEIVERAGENT_STOPPED =
      "IoTDBAirGapReceiverAgent {} ????";
  public static final String LOAD_ACTIVE_LISTENING_PIPE_DIR_IS_NOT =
      "??? load active listening pipe dir?";
  public static final String LOAD_PIPEDATA_WITH_SERIALIZE_NUMBER_SUCCESSFULLY =
      "???? serialize number ? {} ? pipeData?";
  public static final String LOAD_TSFILE_ERROR_STATEMENT = "?? TsFile {} ???statement?{}?";
  public static final String LOAD_TSFILE_RESULT_STATUS = "?? TsFile ?????{}?";
  public static final String PARSE_DATABASE_PARTIALPATH_ERROR = "Parse database PartialPath {} ???";
  public static final String PIPE_AIR_GAP_RECEIVER_CHECKSUM_FAILED_EXPECTED =
      "Pipe air gap receiver {}??????????{}, ???{}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_OF =
      "Pipe air gap receiver {} ??????????Socket?{}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_OF_1 =
      "Pipe air gap receiver {} ???????Socket?{}";
  public static final String PIPE_AIR_GAP_RECEIVER_CLOSED_BECAUSE_SOCKET =
      "Pipe air gap receiver {} ? socket ???????Socket?{}";
  public static final String PIPE_AIR_GAP_RECEIVER_EXCEPTION_DURING_HANDLING =
      "Pipe air gap receiver {}???????????Socket?{}";
  public static final String PIPE_AIR_GAP_RECEIVER_HANDLE_DATA_FAILED =
      "Pipe air gap receiver {}???????????{}, req?{}";
  public static final String PIPE_AIR_GAP_RECEIVER_SOCKET_CLOSED_WHEN =
      "Pipe air gap receiver {}?????? socket {} ??????{}";
  public static final String PIPE_AIR_GAP_RECEIVER_STARTED_SOCKET =
      "Pipe air gap receiver {} ????Socket?{}";
  public static final String PIPE_AIR_GAP_RECEIVER_TEMPORARY_UNAVAILABLE_RETRY =
      "Pipe air gap receiver {}???????????? sender ?? FAIL?";
  public static final String PIPE_DATA_TRANSPORT_ERROR = "Pipe ???????{}";
  public static final String PIPE_INSERTING_ROW_CASTING_TYPE_FROM =
      "Pipe??? row????? {} ??? {}?";
  public static final String PIPE_INSERTING_TABLET_CASTING_TYPE_FROM =
      "Pipe??? tablet????? {} ??? {}?";
  public static final String PIPE_INSERTING_TABLET_TO_CASTING_TYPE_FROM =
      "Pipe?? {}.{} ?? tablet????? {} ??? {}?";
  public static final String RECEIVERS_EXECUTOR_IS_CLOSED = "Receivers-{} ? executor ????";
  public static final String RECEIVER_EXIT_SUCCESSFULLY = "Receiver-{} ?????";
  public static final String RECEIVER_ID = "Receiver id = {}?{}";
  public static final String RECEIVER_ID_THE_NUMBER_OF_DEVICE_PATHS =
      "Receiver id = {}?device path ??? statement {} ?? sub-status ????{}?";
  public static final String RECEIVER_ID_UNKNOWN_PIPEREQUESTTYPE_RESPONSE_STATUS =
      "Receiver id = {}???? PipeRequestType?response status = {}?";
  public static final String RECEIVER_ID_UNSUPPORTED_STATEMENT_TYPE_FOR_REDIRECTION =
      "Receiver id = {}????? statement type {} ?? redirection?";
  public static final String RECEIVER_ID_FAILED_TO_CHECK_AUTHORITY_FOR_STATEMENT =
      "Receiver id = {}: ?? statement {} ?????username = {}?response = {}?";
  public static final String RECEIVER_ID_FAILURE_STATUS_WHILE_EXECUTING_STATEMENT =
      "Receiver id = {}: ?? statement {} ????????{}";
  public static final String RECEIVER_ID_EXCEPTION_WHILE_EXECUTING_STATEMENT =
      "Receiver id = {}: ?? statement {} ??????";
  public static final String UNKNOWN_PIPEREQUESTTYPE = "?? PipeRequestType %s?";
  public static final String EXCEPTION_ENCOUNTERED_WHILE_HANDLING_REQUEST =
      "???? %s????? %s ??";
  public static final String RECEIVER_IS_READY = "Receiver-{} ???";
  public static final String RECEIVER_TEMPORARILY_OUT_OF_MEMORY_FORMAT =
      "?? %s ?????????????%d bytes??????%d bytes??????%d bytes?"
          + "???????%d bytes";
  public static final String REGISTER_WITH_INTERVAL_IN_SECONDS_SUCCESSFULLY =
      "???? {}????????{}?";
  public static final String SOCKET_CLOSED_WHEN_EXECUTING_READTILLFULL =
      "?? readTillFull ? socket ???";
  public static final String SOCKET_CLOSED_WHEN_EXECUTING_SKIPTILLENOUGH =
      "?? skipTillEnough ? socket ???";
  public static final String START_LOAD_PIPEDATA_WITH_SERIALIZE_NUMBER_AND =
      "???? serialize number ? {}?type ? {} ? pipeData?value={}";
  public static final String STORAGE_ENGINE_READONLY = "??????";
  public static final String SYNC_START_AT_TO_IS_DONE = "Sync {} ? {} ??? {} ????";
  public static final String THE_IOTCONSENSUSV2_REQUEST_VERSION_IS_DIFFERENT_FROM =
      "iotConsensusV2 ???? {} ? sender ???? {} ???"
          + "receiver ????? sender ?????";
  public static final String THE_START_INDEX_OF_DATA_SYNC_IS =
      "data sync ????? {} ???????????????? 0?";
  public static final String THE_START_INDEX_OF_DATA_SYNC_IS_1 =
      "data sync ????? {} ????????????? {}?";
  public static final String THRIFT_CONNECTION_IS_NOT_ALIVE = "Thrift ??????";
  public static final String TSFILECHECKER_DID_NOT_TERMINATE_WITHIN_S =
      "TsFileChecker ?? {} ????";
  public static final String TSFILECHECKER_THREAD_STILL_DOESN_T_EXIT_AFTER =
      "TsFileChecker ?? {} ? 30 ??????";
  public static final String UNHANDLED_EXCEPTION_DURING_PIPE_AIR_GAP_RECEIVER =
      "pipe air gap receiver ???????????";
  public static final String UNSUPPORTED_DATA_TYPE = "???? data type?";

  // ===================== RESOURCE =====================

  public static final String CANNOT_GET_DATA_REGION_IDS_USE_DEFAULT =
      "???? data region id?????? lock segment ???{}";
  public static final String EXPAND_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock ??? expand callback";
  public static final String EXPAND_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock ??? expand method";
  public static final String FAILED_TO_CACHEDEVICEISALIGNEDMAPIFABSENT_FOR_TSFILE_BECAUSE_MEMORY =
      "cacheDeviceIsAlignedMapIfAbsent for tsfile {} ??????memory usage is high";
  public static final String FAILED_TO_CACHEOBJECTSIFABSENT_FOR_TSFILE_BECAUSE_MEMORY =
      "cacheObjectsIfAbsent for tsfile {} ??????memory usage is high";
  public static final String FAILED_TO_ESTIMATE_SIZE_FOR_INSERTNODE =
      "estimate size for InsertNode: {} ??";
  public static final String FAILED_TO_EXECUTE_THE_EXPAND_CALLBACK =
      "execute the expand callback ???";
  public static final String FAILED_TO_EXECUTE_THE_SHRINK_CALLBACK =
      "execute the shrink callback ???";
  public static final String FAILED_TO_GET_FILE_SIZE_OF_LINKED =
      "?? file size of linked TsFile {}:  ??";
  public static final String FORCEALLOCATEWITHRETRY_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceAllocateWithRetry???????????";
  public static final String FORCEALLOCATE_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceAllocate???????????";
  public static final String FORCERESIZE_CANNOT_RESIZE_A_NULL_OR_RELEASED =
      "forceResize???? null ?????????? resize";
  public static final String FORCERESIZE_INTERRUPTED_WHILE_WAITING_FOR_AVAILABLE_MEMORY =
      "forceResize???????????";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_THE_LOCK = "????????";
  public static final String IS_RELEASED_AFTER_THREAD_INTERRUPTION =
      "{} ???????????";
  public static final String PIPETSFILERESOURCE_CACHED_DEVICEISALIGNEDMAP_FOR_TSFILE =
      "PipeTsFileResource??? tsfile {} ?? deviceIsAlignedMap?";
  public static final String PIPETSFILERESOURCE_CACHED_OBJECTS_FOR_TSFILE =
      "PipeTsFileResource??? tsfile {} ?????";
  public static final String PIPETSFILERESOURCE_CLOSED_TSFILE_AND_CLEANED_UP =
      "PipeTsFileResource???? tsfile {} ??????";
  public static final String PIPETSFILERESOURCE_FAILED_TO_CACHE_OBJECTS_FOR_TSFILE =
      "PipeTsFileResource?? tsfile {} ???????????????";
  public static final String PIPETSFILERESOURCE_FAILED_TO_DELETE_TSFILE_WHEN_CLOSING =
      "PipeTsFileResource?????? tsfile {} ??????{}???????";
  public static final String PIPETSFILERESOURCE_S_REFERENCE_COUNT_IS_DECREASED_TO =
      "PipeTsFileResource ???????? 0 ???";
  public static final String PIPE_HARDLINK_DIR_FOUND_DELETING_IT_RESULT =
      "?? Pipe hardlink ????????{}, ???{}";
  public static final String PIPE_HARDLINK_DIR_FOUND_MOVED_TO_PERIODICAL_DELETE =
      "?? Pipe hardlink ??????? {} ??? {} ???????????";
  public static final String PIPE_STALE_HARDLINK_DIR_FOUND_REGISTERING_PERIODICAL_DELETE =
      "????? Pipe hardlink ????????????????{}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_FINISHED =
      "????????????? Pipe hardlink ?? {}????{}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_PROGRESS =
      "????? Pipe hardlink ???????? {} ?????????{}, ???????{}";
  public static final String PIPE_HARDLINK_DIR_PERIODICAL_DELETE_ALL_FINISHED =
      "??????????????? Pipe hardlink ???";
  public static final String PIPE_HARDLINK_DIR_MOVE_FAILED_DELETING_SYNC =
      "???????? Pipe hardlink ?? {} ??????????";
  public static final String PIPE_HARDLINK_DIR_MOVE_FAILED_SKIPPING_PERIODICAL_DELETE =
      "???????? Pipe hardlink ?? {} ????????????????? Pipe ????";
  public static final String PIPE_SNAPSHOT_DIR_FOUND_DELETING_IT =
      "?? Pipe snapshot ????????{},";
  public static final String SHRINK_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock ??? shrink callback";
  public static final String SHRINK_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK =
      "PipeFixedMemoryBlock ??? shrink method";
  public static final String THE_MEMORY_BLOCK_HAS_BEEN_RELEASED = "???????";
  public static final String THE_MULTIPLE_N_MUST_BE_GREATER_THAN =
      "?? n ???? 0";
  public static final String TRYALLOCATE_ALLOCATED_MEMORY_TOTAL_MEMORY_SIZE_BYTES =
      "tryAllocate???????????? {} ????????? {} ??????????? {} ??????????? {} ??";
  public static final String TRYALLOCATE_FAILED_TO_ALLOCATE_MEMORY_TOTAL_MEMORY =
      "tryAllocate????????????? {} ????????? {} ????????? {} ??";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_IS_NOT_CONSISTENT_WITH =
      "tryExpandAllAndCheckConsistency????????? block ????usedMemorySizeInBytes ? {}???? block ???? {}";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TABLETS_IS_NOT =
      "tryExpandAllAndCheckConsistency?tablet ????????? block ????usedMemorySizeInBytesOfTablets ? {}???? tablet block ???? {}";
  public static final String TRYEXPANDALLANDCHECKCONSISTENCY_MEMORY_USAGE_OF_TSFILES_IS_NOT =
      "tryExpandAllAndCheckConsistency?tsfile ????????? block ????usedMemorySizeInBytesOfTsFiles ? {}???? tsfile block ???? {}";

  // ===================== METRIC =====================

  public static final String FAILED_TO_DEREGISTER_PIPE_ASSIGNER_METRICS_PIPEDATAREGIONASSIGNER =
      "?? pipe assigner metrics, PipeDataRegionAssigner({}) does not exist ??";
  public static final String FAILED_TO_DEREGISTER_PIPE_DATA_REGION_EXTRACTOR =
      "?? pipe data region extractor metrics, IoTDBDataRegionExtractor({}) does not exist ??";
  public static final String FAILED_TO_DEREGISTER_PIPE_DATA_REGION_SINK =
      "?? pipe data region sink metrics, PipeSinkSubtask({}) does not exist ??";
  public static final String FAILED_TO_DEREGISTER_PIPE_REMAINING_EVENT_AND =
      "?? pipe remaining event and time metrics, RemainingEventAndTimeOperator({}) does not "
          + "exist ??";
  public static final String FAILED_TO_DEREGISTER_PIPE_SCHEMA_REGION_CONNECTOR =
      "?? pipe schema region connector metrics, PipeConnectorSubtask({}) does not exist ??";
  public static final String FAILED_TO_DEREGISTER_PIPE_SCHEMA_REGION_SOURCE =
      "?? pipe schema region source metrics, IoTDBSchemaRegionSource({}) does not exist ??";
  public static final String SKIP_DEREGISTER_PIPE_TSFILE_TO_TABLETS =
      "???? pipe tsfile to tablets metrics??? pipeID({}) ???";
  public static final String FAILED_TO_DEREGISTER_SCHEMA_REGION_LISTENER_METRICS =
      "?? schema region listener metrics, SchemaRegionListeningQueue({}) does not exist ??";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR =
      "mark pipe data region extractor heartbeat event, IoTDBDataRegionExtractor({}) does not "
          + "exist ??";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR_1 =
      "mark pipe data region extractor tablet event, IoTDBDataRegionExtractor({}) does not "
          + "exist ??";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_EXTRACTOR_2 =
      "mark pipe data region extractor tsfile event, IoTDBDataRegionExtractor({}) does not "
          + "exist ??";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_SINK =
      "mark pipe data region sink tablet event, PipeSinkSubtask({}) does not exist ??";
  public static final String FAILED_TO_MARK_PIPE_DATA_REGION_SINK_1 =
      "mark pipe data region sink tsfile event, PipeSinkSubtask({}) does not exist ??";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_HEARTBEAT_EVENT =
      "mark pipe processor heartbeat event, PipeProcessorSubtask({}) does not exist ??";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_TABLET_EVENT =
      "mark pipe processor tablet event, PipeProcessorSubtask({}) does not exist ??";
  public static final String FAILED_TO_MARK_PIPE_PROCESSOR_TSFILE_EVENT =
      "mark pipe processor tsfile event, PipeProcessorSubtask({}) does not exist ??";
  public static final String FAILED_TO_MARK_PIPE_REGION_COMMIT_REMAININGEVENTANDTIMEOPERATOR =
      "mark pipe region commit, RemainingEventAndTimeOperator({}) does not exist ??";
  public static final String FAILED_TO_MARK_PIPE_SCHEMA_REGION_WRITE =
      "mark pipe schema region write plan event, PipeConnectorSubtask({}) does not exist ??";
  public static final String FAILED_TO_MARK_PIPE_TSFILE_TO_TABLETS =
      "mark pipe tsfile to tablets invocation, pipeID({}) does not exist ??";
  public static final String FAILED_TO_RECORD_PIPE_TSFILE_TO_TABLETS =
      "?? pipe tsfile to tablets time, pipeID({}) does not exist ??";
  public static final String FAILED_TO_RECORD_TABLET_GENERATED_PIPEID_DOES =
      "?? tablet generated, pipeID({}) does not exist ??";
  public static final String FAILED_TO_SET_RECENT_PROCESSED_TSFILE_EPOCH =
      "?? recent processed tsfile epoch state, PipeRealtimeDataRegionExtractor({}) does not "
          + "exist ??";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_ASSIGNER_METRICS =
      "?? from pipe assigner metrics, assigner map not empty ??";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_DATA_REGION =
      "?? from pipe data region sink metrics, sink map not empty ??";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_EXTRACTOR_METRICS =
      "?? from pipe extractor metrics, extractor map not empty ??";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_PROCESSOR_METRICS =
      "?? from pipe processor metrics, processor map not empty ??";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_REMAINING_EVENT =
      "?? from pipe remaining event and time metrics, RemainingEventAndTimeOperator map not "
          + "empty ??";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION =
      "?? from pipe schema region connector metrics, connector map not empty ??";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION_1 =
      "?? from pipe schema region extractor metrics, extractor map not empty ??";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_SCHEMA_REGION_2 =
      "?? from pipe schema region listener metrics, listening queue map not empty ??";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_TSFILE_TO =
      "?? from pipe tsfile to tablets metrics, pipe map is not empty, pipe: {} ??";

  // ---------------------------------------------------------------------------
  // pipe ? AbstractSameTypeNumericOperator
  // ---------------------------------------------------------------------------
  public static final String UNSUPPORTED_OUTPUT_DATATYPE_FMT = "?????????? %s";

  // ---------------------------------------------------------------------------
  // pipe ? IoTDBDataRegionSource
  // ---------------------------------------------------------------------------
  public static final String ILLEGAL_TREE_PATTERN_FMT = "Pattern \"%s\" ???";

  // ---------------------------------------------------------------------------
  // pipe ? OpcUaServerBuilder
  // ---------------------------------------------------------------------------
  public static final String UNABLE_CREATE_SECURITY_DIR = "?????????";
  public static final String OPC_UA_SECURITY_DIR =
      "?????{}";
  public static final String OPC_UA_SECURITY_PKI_DIR =
      "?? PKI ???{}";
  public static final String
      EXCEPTION_THE_ADVERTISED_HOST_MUST_BE_A_HOSTNAME_OR_IP_ADDRESS_WITHOUT_A_SCHEME_PORT_OR_PATH_6857C67A =
          "advertised host ????? scheme?port ? path ? hostname ? IP ???";
  public static final String
      LOG_ADVERTISED_HOST_ARG_IS_NOT_PRESENT_IN_THE_LOADED_OPC_UA_SERVER_CERTIFICATE_SUBJECT_ALTERNATIVE_NAMES_SECURED_CLIENTS_MAY_REJECT_IT_REPLACE_OR_REGENERATE_THE_CERTIFICATE_AND_ESTABLISH_TRUST_AGAIN_912358AF =
          "advertised host {} ?????? OPC UA server ?? subject alternative names ???????????????"
              + "??????????????????";

  // ---------------------------------------------------------------------------
  // pipe ? PipeDataNodePluginAgent
  // ---------------------------------------------------------------------------
  public static final String PLUGIN_NOT_REGISTERED_FMT = "?? %s ????";

  // ---------------------------------------------------------------------------
  // pipe - WriteBackSink
  // ---------------------------------------------------------------------------
  public static final String TABLE_MODEL_DATABASE_INVALID_FMT =
      "?????? %s ??????? '%s'???? %s???????? %d";
  public static final String TREE_MODEL_DATABASE_INVALID_FMT =
      "?????? %s ???????????????????? %s???????? %d";
  public static final String TARGET_TREE_MODEL_DATABASE_CANNOT_BE_USED_FOR_TABLE_MODEL_EVENTS_FMT =
      "???????? %s ????????????????????? %s ???";
  public static final String FAILED_TO_REWRITE_TREE_MODEL_DATABASE_FMT =
      "???????? %s ??? %s ?????? %s?";

  // ---------------------------------------------------------------------------
  // pipe ? PipeTransferTrackableHandler
  // ---------------------------------------------------------------------------
  public static final String TPIPE_TRANSFER_RESP_IS_NULL_WHEN_TRANSFERRING_SLICE =
      "????? TPipeTransferResp ???";

  private DataNodePipeMessages() {}
  // ---------------------------------------------------------------------------
  // ??????
  // ---------------------------------------------------------------------------
  public static final String PIPE_LOG_SUBSCRIPTION_DETECT_DUPLICATED_PIPETSFILEINSERTIONEVENT_23A4740C =
      "Subscription??????? PipeTsFileInsertionEvent {}?????";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_ECB64624 =
      "Subscription???? topic [{}]?consumer group [{}] ? prefetching queue ??????????????";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_8F561EB2 =
      "Subscription???? topic [{}]?consumer group [{}] ? prefetching queue ?????????????";
  public static final String PIPE_LOG_SUBSCRIPTION_CREATE_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_E7F21F1E =
      "Subscription?????? topic [{}]?consumer group [{}] ? prefetching queue";
  public static final String PIPE_LOG_SUBSCRIPTION_DROP_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_21F313CB =
      "Subscription?????? topic [{}]?consumer group [{}] ? prefetching queue";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_03B89C51 =
      "Subscription???? topic [{}]?consumer group [{}] ? prefetching queue ????????????";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_EA7D450B =
      "Subscription???? topic [{}]?consumer group [{}] ? prefetching queue ???";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_12E69B65 =
      "Subscription???? topic [{}]?consumer group [{}] ? prefetching queue ???";
  public static final String PIPE_LOG_SUBSCRIPTION_PREFETCHING_QUEUE_BOUND_TO_TOPIC_FOR_CONSUMER_C2735402 =
      "Subscription???? topic [{}]?consumer group [{}] ? prefetching queue ???";
  public static final String PIPE_LOG_SUBSCRIPTIONPREFETCHINGTABLETQUEUE_DETECTED_OUTDATED_POLL_C0001CCF =
      "SubscriptionPrefetchingTabletQueue {} ?????? poll ???consumer {}?commit context {}?offset {}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_POLL_CALLED_CONSUMERID_TOPICNAMES_5F1F5175 =
      "ConsensusSubscriptionBroker [{}]??? poll?consumerId={}?topicNames={}?queueCount={}?"
          + "maxBytes={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_POLL_RESULT_CONSUMERID_EVENTSPOLLED_06412726 =
      "ConsensusSubscriptionBroker [{}]?poll ???consumerId={}?eventsPolled={}?eventsNacked={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_REFRESHED_OWNERSHIP_FOR_TOPIC_EB11CF64 =
      "ConsensusSubscriptionBroker [{}]??? topic [{}] ? ownership?consumers={}?regions={}?"
          + "generation={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_STABLE_OWNERSHIP_POLL_ORDER_D40BB7D4 =
      "ConsensusSubscriptionBroker [{}]?topic [{}] ??? ownership poll ???assignedQueueCount={}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_REGION_B40792D9 =
      "Subscription?topic [{}]?Region [{}]?consumer group [{}] ? consensus prefetching queue ??????";
  public static final String PIPE_LOG_SUBSCRIPTION_CREATE_CONSENSUS_PREFETCHING_QUEUE_BOUND_TO_0DBFC05E =
      "Subscription?????? topic [{}]?consumer group [{}] ? consensus prefetching queue?"
          + "consensusGroupId={}?fallbackCommittedRegionProgress={}?tailStartSearchIndex={}?"
          + "initialRuntimeVersion={}?initialActive={}?totalRegionQueues={}";
  public static final String PIPE_LOG_SUBSCRIPTION_CLOSED_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_3A9DDEC5 =
      "Subscription??? Region ?????? topic [{}]?Region [{}]?consumer group [{}] ? consensus "
          + "prefetching queue";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUE_S_BOUND_TO_TOPIC_AB10ED07 =
      "Subscription???? topic [{}]?consumer group [{}] ? consensus prefetching queue ????????????";
  public static final String PIPE_LOG_SUBSCRIPTION_DROP_ALL_CONSENSUS_PREFETCHING_QUEUE_S_BOUND_FCC1B2C4 =
      "Subscription????? {} ???? topic [{}]?consumer group [{}] ? consensus prefetching queue";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_COMMIT_7D8CC39D =
      "ConsensusSubscriptionBroker [{}]?topic [{}] ?????? queue";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_COMMIT_CONTEXT_NOT_FOUND_IN_46DF62A6 =
      "ConsensusSubscriptionBroker [{}]???? commit context {}???? {} ? Region queue?topic [{}]";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEK_6307A90D =
      "ConsensusSubscriptionBroker [{}]?topic [{}] ????? seek ? queue";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_UNSUPPORTED_SEEKTYPE_FOR_TOPIC_EDCA2CF2 =
      "ConsensusSubscriptionBroker [{}]???? seekType {}?topic [{}]";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEK_9AC3890C =
      "ConsensusSubscriptionBroker [{}]?topic [{}] ????? seek(topicProgress) ? queue";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONBROKER_NO_QUEUES_FOR_TOPIC_TO_SEEKAFTER_C6D87BFD =
      "ConsensusSubscriptionBroker [{}]?topic [{}] ????? seekAfter(topicProgress) ? queue";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_PREFETCHING_QUEUES_BOUND_TO_TOPIC_63B37089 =
      "Subscription???? topic [{}]?consumer group [{}] ? consensus prefetching queue ???";
  public static final String PIPE_LOG_SUBSCRIPTIONPREFETCHINGTSFILEQUEUE_DETECTED_OUTDATED_POLL_7E0CE108 =
      "SubscriptionPrefetchingTsFileQueue {} ?????? poll ???consumer {}?commit context {}?writing "
          + "offset {}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_COMMIT_PIPETERMINATEEVENT_36529DC9 =
      "Subscription?SubscriptionPrefetchingQueue {} ?? PipeTerminateEvent {}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_IGNORE_ENRICHEDEVENT_95C6241C =
      "Subscription?SubscriptionPrefetchingQueue {} ? prefetch ???? EnrichedEvent {}?";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_POLL_COMMITTED_8684FF17 =
      "Subscription?SubscriptionPrefetchingQueue {} ? prefetching queue poll ?????? {}?????????"
          + "?????";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_POLL_NON_POLLABLE_644D5D6B =
      "Subscription?SubscriptionPrefetchingQueue {} ? prefetching queue poll ??? poll ?? {}?????????"
          + "?? nack ??????";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_INTERRUPTED_WHILE_F8923826 =
      "Subscription?SubscriptionPrefetchingQueue {} ? poll ????????";
  public static final String PIPE_LOG_SUBSCRIPTION_INCONSISTENT_HEARTBEAT_EVENT_WHEN_PEEKING_BROKEN_BFE1DF6E =
      "Subscription?{} peeking ? heartbeat event ?????????????? {}??? {}?????";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_ONLY_SUPPORT_PREFETCH_F3B33B30 =
      "Subscription?SubscriptionPrefetchingQueue {} ??? prefetch EnrichedEvent??? {}?";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_PREFETCH_TSFILEINSERTIONEVENT_19444D2C =
      "Subscription?SubscriptionPrefetchingQueue {} ? ToTabletIterator ? null ? prefetch "
          + "TsFileInsertionEvent??????????? {}?";
  public static final String PIPE_LOG_FAILED_TO_INCREASE_REFERENCE_COUNT_FOR_WHEN_ON_RETRYABLE_4E10BE3B =
      "? {} ??????????????? TabletInsertionEvent ??? {} ?";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_ON_RETRYABLE_TABLETINSERTIONEVENT_2350D9F7 =
      "?? {} ????? TabletInsertionEvent {} ?????";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_COMMIT_CONTEXT_DOES_NOT_EXIST_0E4EF990 =
      "Subscription?subscription commit context {} ??????????????????prefetching queue?{}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_EVENT_IS_COMMITTED_SUBSCRIPTION_BEE17D7F =
      "Subscription?subscription event {} ????subscription commit context {}?prefetching queue?{}";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_EVENT_IS_NOT_COMMITTABLE_SUBSCRIPTION_8D03A10C =
      "Subscription?subscription event {} ?????subscription commit context {}?prefetching queue?{}";
  public static final String PIPE_LOG_INCONSISTENT_CONSUMER_GROUP_WHEN_ACKING_EVENT_CURRENT_INCOMING_AEE3E90F =
      "acking event ? consumer group ???????{}????{}?consumer id?{}?event commit context?{}?"
          + "prefetching queue?{}??????";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTION_COMMIT_CONTEXT_DOES_NOT_EXIST_DE907E05 =
      "Subscription?subscription commit context [{}] ??????????????????prefetching queue?{}";
  public static final String PIPE_LOG_INCONSISTENT_CONSUMER_GROUP_WHEN_NACKING_EVENT_CURRENT_INCOMING_B0104C41 =
      "nacking event ? consumer group ???????{}????{}?consumer id?{}?event commit context?{}?"
          + "prefetching queue?{}??????";
  public static final String PIPE_LOG_SUBSCRIPTION_SUBSCRIPTIONPREFETCHINGQUEUE_RECYCLE_EVENT_7B120BC3 =
      "Subscription?SubscriptionPrefetchingQueue {} ??????? {}??? nack ????? prefetching queue";
  public static final String PIPE_LOG_SUBSCRIPTION_POISON_MESSAGE_DETECTED_NACKCOUNT_FORCE_ACKING_7528DD6B =
      "Subscription???? poison message?nackCount={}????? {} ? prefetching queue {} ????? ack";
  public static final String PIPE_LOG_SUBSCRIPTION_POISON_MESSAGE_DETECTED_NACKCOUNT_FORCE_ACKING_D984349C =
      "Subscription???? poison message?nackCount={}??? eagerly pollable event {} ? prefetching "
          + "queue {} ????? ack";
  public static final String PIPE_LOG_SUBSCRIPTION_POISON_MESSAGE_DETECTED_NACKCOUNT_FORCE_ACKING_FEF0F0BF =
      "Subscription???? poison message?nackCount={}??? pollable event {} ? prefetching queue {} "
          + "????? ack";
  public static final String PIPE_LOG_SUBSCRIPTION_UNKNOWN_PIPESUBSCRIBEREQUESTVERSION_RESPONSE_56E5D93F =
      "Subscription???? PipeSubscribeRequestVersion????? = {}?";
  public static final String PIPE_LOG_THE_SUBSCRIPTION_REQUEST_VERSION_IS_DIFFERENT_FROM_THE_CLIENT_324A125F =
      "subscription ???? {} ???????? {} ???receiver ????????????";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_IS_A_NO_OP_ON_THIS_DATANODE_BECAUSE_28F7E92B =
      "Subscription?consensus {} ?? DataNode ?????????? queue ????consumerGroup={}?topic={}";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_REFRESHING_CONSENSUS_QUEUE_ORDER_1886704D =
      "SubscriptionBrokerAgent?? topic [{}] ? consensus queue order-mode ??? [{}]";
  public static final String PIPE_LOG_SUBSCRIPTION_UNBOUND_CONSENSUS_PREFETCHING_QUEUE_S_FOR_REMOVED_AC018742 =
      "Subscription???? {} ???? Region [{}] ? consensus prefetching queue";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_SETACTIVEFORREGION_REGIONID_ACTIVE_4AC3A2CB =
      "SubscriptionBrokerAgent?setActiveForRegion regionId={}?active={}";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_SETACTIVEWRITERSFORREGION_REGIONID_48B39B3E =
      "SubscriptionBrokerAgent?setActiveWritersForRegion regionId={}?activeWriterNodeIds={}";
  public static final String PIPE_LOG_SUBSCRIPTIONBROKERAGENT_APPLYRUNTIMESTATEFORREGION_REGIONID_6D8C37A1 =
      "SubscriptionBrokerAgent?applyRuntimeStateForRegion regionId={}?runtimeState={}";
  public static final String PIPE_LOG_SUBSCRIPTION_FAILED_TO_PARSE_CONSENSUS_REGION_ID_FOR_COMMITTED_9F1A50EB =
      "Subscription??? committed progress ? consensus Region id {} ???topic={}?consumerGroup={}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSENSUS_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_E46FCDD9 =
      "Subscription???? consumer group [{}] ? consensus broker ???";
  public static final String PIPE_LOG_SUBSCRIPTION_PIPE_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_E9B60B22 =
      "Subscription???? consumer group [{}] ? pipe broker ???";
  public static final String PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_EXIST_74CAD5BE =
      "Subscription???? consumer group [{}] ? broker ???";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_GROUP_META_CHANGE_DETECTED_TOPICSUNSUBBYGROUP_F6DAF20A =
      "Subscription???? consumer group [{}] meta ???topicsUnsubByGroup={}?newlySubscribedTopics={}";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_HANDLING_SINGLE_CONSUMER_GROUP_META_10E7688C =
      "?? consumer group {} ??? consumer group meta ???????";
  public static final String PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_HAS_ALREADY_0F37997F =
      "Subscription???? consumer group [{}] ? broker ?????? agent {} ? consumer group meta ?????? "
          + "coordinator {} ? meta ??????? broker";
  public static final String PIPE_LOG_SUBSCRIPTION_BROKER_BOUND_TO_CONSUMER_GROUP_DOES_NOT_EXISTED_9F09E4DE =
      "Subscription???? consumer group [{}] ? broker ??????? consumer group meta ?????? agent??????";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_HANDLING_SINGLE_TOPIC_META_CHANGES_43434FC4 =
      "?? topic {} ??? topic meta ???????";
  public static final String PIPE_LOG_PULLED_TOPIC_META_FROM_CONFIG_NODE_RECOVERING_5C4B1AEE =
      "?? ConfigNode ?? topic meta?{}???????";
  public static final String PIPE_LOG_INTERRUPTED_WHILE_SLEEPING_WILL_RETRY_TO_GET_TOPIC_META_976E4BE2 =
      "???????????? ConfigNode ?? topic meta?";
  public static final String PIPE_LOG_PULLED_CONSUMER_GROUP_META_FROM_CONFIG_NODE_RECOVERING_A85B948F =
      "?? ConfigNode ?? consumer group meta?{}???????";
  public static final String PIPE_LOG_INTERRUPTED_WHILE_SLEEPING_WILL_RETRY_TO_GET_CONSUMER_GROUP_7E161F39 =
      "???????????? ConfigNode ?? consumer group meta?";
  public static final String PIPE_LOG_FAILED_TO_GET_TOPIC_META_FROM_CONFIG_NODE_FOR_TIMES_WILL_E8D0B7F8 =
      "? ConfigNode ?? topic meta ??? {} ??????? {} ??";
  public static final String PIPE_LOG_FAILED_TO_GET_CONSUMER_GROUP_META_FROM_CONFIG_NODE_FOR_TIMES_3E4C727C =
      "? ConfigNode ?? consumer group meta ??? {} ??????? {} ??";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_REFRESHED_OF_PROCESSOR_BUFFERED_COMMIT_8C7A352A =
      "Subscription?consumer {} ??? {} ? processor-buffered commit context lease?? {} ?";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_SUCCESSFULLY_WITH_REQUEST_6BC8BFED =
      "Subscription?consumer {} poll {} ??????{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_FULL_COMMIT_CONTEXTS_CFC18359 =
      "Subscription?consumer {} commit?nack?{}??? commit context?{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_FULL_REQUESTED_COMMIT_1E67E8A3 =
      "Subscription?consumer {} commit?nack?{}????? commit context?{}????? commit context?{}?"
          + "????????? commit context?{}";
  public static final String PIPE_LOG_SUBSCRIPTION_REMOVE_CONSUMER_CONFIG_WHEN_HANDLING_EXIT_3827D0E8 =
      "Subscription???????? consumer config {}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_IS_INACTIVE_FOR_MS_EXCEEDING_TIMEOUT_36E06B11 =
      "Subscription?consumer {} ???? {} ms??????? {} ms???????? consumer?";
  public static final String PIPE_LOG_SUBSCRIPTION_THE_CONSUMER_HAS_ALREADY_EXISTED_WHEN_HANDSHAKING_3761AD81 =
      "Subscription???? consumer {} ?????? consumer ???";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_HANDSHAKE_SUCCESSFULLY_DATA_NODE_ID_58DA6A5F =
      "Subscription?consumer {} ?????data node id?{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_UNSUBSCRIBE_SUCCESSFULLY_AA5E0AA9 =
      "Subscription?consumer {} ???? {} ??";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_ACCEPTED_SUCCESSFULLY_58D1C111 =
      "Subscription?consumer {} commit?nack?{}?accepted ???summary?{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_SEEK_TOPIC_TO_TOPICPROGRESS_REGIONCOUNT_41702313 =
      "Subscription?consumer {} ? topic {} seek ? topicProgress?regionCount={}?";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_SEEKAFTER_TOPIC_TO_TOPICPROGRESS_REGIONCOUNT_838584F8 =
      "Subscription?consumer {} ? topic {} seekAfter ? topicProgress?regionCount={}?";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_SEEK_TOPIC_WITH_SEEKTYPE_799FF449 =
      "Subscription?consumer {} ? topic {} ?? seekType={} ?? seek";
  public static final String PIPE_LOG_SUBSCRIPTION_UNSUBSCRIBE_ALL_SUBSCRIBED_TOPICS_BEFORE_CLOSE_BFB787AE =
      "Subscription?????????? topic {}????? consumer {}";
  public static final String PIPE_LOG_SUBSCRIPTION_THE_CONSUMER_DOES_NOT_EXISTED_WHEN_CLOSING_CCB63DCB =
      "Subscription???? consumer {} ???????? consumer?";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_UNSUBSCRIBE_COMPLETED_TOPICS_SUCCESSFULLY_44BAFF55 =
      "Subscription?consumer {} ???? {}???? topic???";
  public static final String PIPE_LOG_SUBSCRIPTION_FAILED_TO_CLOSE_TIMED_OUT_CONSUMER_AFTER_MS_89CC11F1 =
      "Subscription?consumer {} ??? {} ms ?????? consumer ??";
  public static final String PIPE_LOG_SUBSCRIPTION_DETECT_STALE_CONSUMER_CONFIG_WHEN_HANDSHAKING_B0196DB8 =
      "Subscription????????? consumer config?????? consumer config {}??? consumer config ?????? "
          + "consumer config {}?";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_HEARTBEAT_B9EFB1CC =
      "Subscription?????????? consumer config?{}";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_FETCH_ENDPOINTS_FOR_CONSUMER_IN_325B571A =
      "? ConfigNode ??? consumer {} ? endpoints ?????";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBESUBSCRIBEREQ_DF466A30 =
      "Subscription??? PipeSubscribeSubscribeReq ??? consumer config?{}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBEUNSUBSCRIBEREQ_673CE701 =
      "Subscription??? PipeSubscribeUnsubscribeReq ??? consumer config?{}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBEPOLLREQ_6BB9292B =
      "Subscription??? PipeSubscribePollReq ??? consumer config?{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_NULL_RESPONSE_FOR_EVENT_OUTDATED_4CF7FAAA =
      "Subscription?consumer {} ???? {} poll ? null ???outdated?{}?????{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_FOR_EVENT_OUTDATED_FAILED_WITH_0BEFF244 =
      "Subscription?consumer {} poll {} ???event={}?outdated?{}?????{}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBECOMMITREQ_76B28EBB =
      "Subscription??? PipeSubscribeCommitReq ??? consumer config?{}";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_COMMIT_NACK_PARTIALLY_ACCEPTED_REQUESTED_87D0C038 =
      "Subscription?consumer {} commit?nack?{}??? accepted??? summary?{}?accepted summary?{}?"
          + "??????? summary?{}";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_PIPESUBSCRIBECLOSEREQ_717660F8 =
      "Subscription??? PipeSubscribeCloseReq ??? consumer config?{}";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_SEEKING_WITH_REQUEST_6B581543 =
      "???? {} ?? seek ?????";
  public static final String PIPE_LOG_SUBSCRIPTION_MISSING_CONSUMER_CONFIG_WHEN_HANDLING_SUBSCRIPTION_B85D47A4 =
      "Subscription??? subscription seek ????? consumer config?{}";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_CREATING_CONSUMER_IN_CONFIG_5D2E1B97 =
      "???????? {}?? ConfigNode ??? consumer {} ?";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_CLOSING_CONSUMER_IN_CONFIG_NODE_0C2E0CE6 =
      "???????? {}?? ConfigNode ??? consumer {} ?";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_SUBSCRIBING_TOPICS_FOR_CONSUMER_8676DA8A =
      "???????? {}?? ConfigNode ??? topic {} ? consumer {} ?";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_SUBSCRIBING_TOPICS_FOR_CONSUMER_E5D72F10 =
      "? ConfigNode ??? topic {} ? consumer {} ?????";
  public static final String PIPE_LOG_UNEXPECTED_STATUS_CODE_WHEN_UNSUBSCRIBING_TOPICS_FOR_CONSUMER_EFC771F0 =
      "???????? {}?? ConfigNode ?? topic {} ?? consumer {} ????";
  public static final String PIPE_LOG_EXCEPTION_OCCURRED_WHEN_UNSUBSCRIBING_TOPICS_FOR_CONSUMER_FE4B3CEE =
      "? ConfigNode ?? topic {} ?? consumer {} ????????";
  public static final String PIPE_LOG_SUBSCRIPTION_CONSUMER_POLL_EXCESSIVE_PAYLOAD_FOR_EVENT_OUTDATED_2BFF690B =
      "Subscription?consumer {} poll ???? payload {}?event={}?outdated?{}?????{}?????? payload "
          + "????????????";
  public static final String PIPE_LOG_FAILED_TO_UNBIND_FROM_SUBSCRIPTION_PREFETCHING_QUEUE_METRICS_6614388C =
      "?? subscription prefetching queue metrics ???prefetching queue map ??";
  public static final String PIPE_LOG_FAILED_TO_DEREGISTER_SUBSCRIPTION_PREFETCHING_QUEUE_METRICS_F08479A7 =
      "?? subscription prefetching queue metrics ???SubscriptionPrefetchingQueue({}) ???";
  public static final String PIPE_LOG_FAILED_TO_MARK_TRANSFER_EVENT_RATE_SUBSCRIPTIONPREFETCHINGQUEUE_7DEF95B5 =
      "???????????SubscriptionPrefetchingQueue({}) ???";
  public static final String PIPE_LOG_FAILED_TO_UNBIND_FROM_CONSENSUS_SUBSCRIPTION_PREFETCHING_A8F920D9 =
      "?? consensus subscription prefetching queue metrics ???queue map ??";
  public static final String PIPE_LOG_FAILED_TO_DEREGISTER_CONSENSUS_SUBSCRIPTION_PREFETCHING_8B180091 =
      "?? consensus subscription prefetching queue metrics ???ConsensusPrefetchingQueue({}) ???";
  public static final String PIPE_LOG_FAILED_TO_MARK_TRANSFER_EVENT_RATE_CONSENSUSPREFETCHINGQUEUE_FE9B91C3 =
      "???????????ConsensusPrefetchingQueue({}) ???";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTSFILERESPONSE_IS_EMPTY_WHEN_FETCHING_NEXT_DFD60DF1 =
      "??????? SubscriptionEventTsFileResponse {} ??????????";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTSFILERESPONSE_IS_NOT_EMPTY_WHEN_INITIALIZING_C9DE83C9 =
      "???? SubscriptionEventTsFileResponse {} ??????????";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTSFILERESPONSE_IS_EMPTY_WHEN_GENERATING_B8D03E93 =
      "??????? SubscriptionEventTsFileResponse {} ??????????";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTABLETRESPONSE_WAIT_FOR_RESOURCE_ENOUGH_9926289F =
      "SubscriptionEventTabletResponse {} ????????? tablets {} ??";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTABLETRESPONSE_IS_EMPTY_WHEN_FETCHING_NEXT_4464E3F2 =
      "??????? SubscriptionEventTabletResponse {} ??????????";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTTABLETRESPONSE_IS_NOT_EMPTY_WHEN_INITIALIZING_88F075C9 =
      "???? SubscriptionEventTabletResponse {} ??????????";
  public static final String PIPE_LOG_DETECT_LARGE_TABLETS_WITH_BYTE_S_CURRENT_TABLETS_SIZE_BYTE_4D472E38 =
      "???? tablets??? {} byte(s)??? tablets ?? {} byte(s)";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTBINARYCACHE_ALLOCATEDMEMORYBLOCK_HAS_SHRUNK_08F23ADE =
      "SubscriptionEventBinaryCache.allocatedMemoryBlock ?? {} ??? {}?";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTBINARYCACHE_ALLOCATEDMEMORYBLOCK_HAS_EXPANDED_52A971D9 =
      "SubscriptionEventBinaryCache.allocatedMemoryBlock ?? {} ??? {}?";
  public static final String PIPE_LOG_SUBSCRIPTIONEVENTBINARYCACHE_RAISED_AN_EXCEPTION_WHILE_SERIALIZING_F3B698CB =
      "SubscriptionEventBinaryCache ??? CachedSubscriptionPollResponse ??????{}";
  public static final String PIPE_LOG_SUBSCRIPTION_SOMETHING_UNEXPECTED_HAPPENED_WHEN_SERIALIZING_5467B7B6 =
      "Subscription???? CachedSubscriptionPollResponse ????????{}";
  public static final String PIPE_LOG_HAS_BEEN_ITERATED_TIMES_CURRENT_TSFILEINSERTIONEVENT_0939C298 =
      "{} ???? {} ???? TsFileInsertionEvent {}";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_ONLY_SUPPORT_CONVERT_PIPEINSERTNODETABLETINSERTIONEVENT_B888B8AA =
      "SubscriptionPipeTabletEventBatch {} ???? PipeInsertNodeTabletInsertionEvent ? "
          + "PipeRawTabletInsertionEvent ??? tablet??? {}?";
  public static final String
      PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_POSTPONE_EMITTING_SUBSCRIPTION_TABLET_BATCH_FOR_TOPIC_ARG_BECAUSE_TABLE_SCHEMA_ARG_ARG_IS_NOT_AVAILABLE_LOCALLY_996C618D =
          "?????? {} ??? tablet ?????????????? {}.{}";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_UNEXPECTED_TABLET_INSERTION_8FB1B507 =
      "SubscriptionPipeTabletEventBatch???? tablet insertion event {}???????";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_FAILED_TO_INCREASE_THE_595722D8 =
      "SubscriptionPipeTabletEventBatch????? {} ??????????????";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_OVERRIDE_NON_NULL_CURRENTTABLETINSERTIONEVENTSITERATOR_2633B158 =
      "SubscriptionPipeTabletEventBatch {} ?????? null ? "
          + "currentTabletInsertionEventsIterator?????????";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETABLETEVENTBATCH_IGNORE_ENRICHEDEVENT_WHEN_E6BAEACE =
      "SubscriptionPipeTabletEventBatch {} ????? EnrichedEvent {}?????????";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPETSFILEEVENTBATCH_IGNORE_TSFILEINSERTIONEVENT_88189024 =
      "SubscriptionPipeTsFileEventBatch {} ?????? TsFileInsertionEvent {}?";
  public static final String PIPE_LOG_SUBSCRIPTIONPIPEEVENTBATCH_IGNORE_ENRICHEDEVENT_WHEN_BATCHING_E69BE90D =
      "SubscriptionPipeEventBatch {} ?????? EnrichedEvent {}?";
  public static final String PIPE_LOG_CONSENSUS_PREFETCH_EXECUTOR_IS_SHUTDOWN_SKIP_REGISTERING_83E36171 =
      "Consensus prefetch executor ???????? {}";
  public static final String PIPE_LOG_CONSENSUS_PREFETCH_SUBTASK_IS_ALREADY_REGISTERED_419FE7AD =
      "Consensus prefetch subtask {} ???";
  public static final String PIPE_LOG_CONSENSUS_PREFETCH_WORKER_LOOP_EXITS_ABNORMALLY_531EE564 =
      "Consensus prefetch worker loop ????";
  public static final String PIPE_LOG_FAILED_TO_CLOSE_SINK_AFTER_FAILED_TO_INITIALIZE_SINK_IGNORE_CF2E3D90 =
      "sink ???????? sink ?????????";
  public static final String PIPE_LOG_CONSENSUSPREFETCHSUBTASK_UNEXPECTED_ERROR_WHILE_DRIVING_D361F4C2 =
      "ConsensusPrefetchSubtask {}??? queue {} ????????";
  public static final String PIPE_LOG_SUBSCRIPTIONSINKSUBTASK_FOR_CONSENSUS_TOPIC_FAILED_UNEXPECTEDLY_FC41B565 =
      "consensus topic [{}] ? SubscriptionSinkSubtask ?????????????";
  public static final String PIPE_LOG_FAILED_TO_BROADCAST_SUBSCRIPTION_PROGRESS_TO_DATANODE_AT_7024F5B2 =
      "? DataNode {} ?? subscription progress ????? {}?{}";
  public static final String PIPE_LOG_FAILED_TO_BROADCAST_SUBSCRIPTION_PROGRESS_FOR_REGION_DE9074BD =
      "?? Region {} ? subscription progress ???{}";
  public static final String PIPE_LOG_RECEIVED_SUBSCRIPTION_PROGRESS_BROADCAST_CONSUMERGROUPID_CDAEF839 =
      "?? subscription progress ???consumerGroupId={}?topicName={}?regionId={}?physicalTime={}?"
          + "localSeq={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IDEMPOTENT_RE_COMMIT_FOR_30464FC4 =
      "ConsensusSubscriptionCommitState??????? ({},{},{})";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IDEMPOTENT_DIRECT_COMMIT_B093AC01 =
      "ConsensusSubscriptionCommitState??????? ({},{},{})";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_RECOVERED_COMMITTEDREGIONPROGRESS_F6B92C6B =
      "ConsensusSubscriptionCommitManager??? ConfigNode ?? committedRegionProgress={}?"
          + "consumerGroupId={}?topicName={}?regionId={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_COMMIT_FOR_UNKNOWN_751BD2A9 =
      "ConsensusSubscriptionCommitManager??????????consumerGroupId={}?topicName={}?regionId={}?"
          + "writerId={}?writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_DIRECT_COMMIT_D6AD7D96 =
      "ConsensusSubscriptionCommitManager????????????consumerGroupId={}?topicName={}?regionId={}?"
          + "writerId={}?writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_RESET_UNKNOWN_C469052F =
      "ConsensusSubscriptionCommitManager??????????consumerGroupId={}?topicName={}?regionId={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_IGNORE_BROADCAST_WITHOUT_211DE477 =
      "ConsensusSubscriptionCommitManager????? writer ??????consumerGroupId={}?topicName={}?"
          + "regionId={}?writerId={}?writerProgress={}";
  public static final String PIPE_LOG_SKIP_MALFORMED_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_NAME_BB4D75F0 =
      "??????? consensus subscription progress ??? {}";
  public static final String PIPE_LOG_FAILED_TO_RECOVER_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_DF30716B =
      "?? consensus subscription progress ???consumerGroupId={}?topicName={}";
  public static final String PIPE_LOG_FAILED_TO_DELETE_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_51C57096 =
      "?? consensus subscription progress ?? {} ??";
  public static final String PIPE_LOG_FAILED_TO_PERSIST_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_4EA71236 =
      "??? consensus subscription progress ???consumerGroupId={}?topicName={}?regionId={}";
  public static final String PIPE_LOG_FAILED_TO_REWRITE_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_8B230D50 =
      "?? consensus subscription progress ???consumerGroupId={}?topicName={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_FAILED_TO_QUERY_COMMIT_31E47F21 =
      "ConsensusSubscriptionCommitManager?? ConfigNode ?????????consumerGroupId={}?"
          + "topicName={}?regionId={}???={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_FAILED_TO_QUERY_COMMIT_16CFDCD9 =
      "ConsensusSubscriptionCommitManager?? ConfigNode ?????????consumerGroupId={}?"
          + "topicName={}?regionId={}?? 0 ??";
  public static final String PIPE_LOG_FAILED_TO_SERIALIZE_COMMITTED_REGION_PROGRESS_0D8D2129 =
      "??? committed region progress {} ??";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IGNORE_MAPPING_WITHOUT_3E66A74D =
      "ConsensusSubscriptionCommitState????? writer ??? mapping?writerId={}?writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_DUPLICATE_OUTSTANDING_MAPPING_B5B34891 =
      "ConsensusSubscriptionCommitState?slot={} ???? outstanding mapping???={}????={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_OUTSTANDING_SIZE_EXCEEDS_1463BF02 =
      "ConsensusSubscriptionCommitState?outstanding size?{}??????{}??consumers ??????"
          + "committed=({},{}), writerNodeId={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_MISSING_WRITER_IDENTITY_01040357 =
      "ConsensusSubscriptionCommitState?commit ?? writer ???writerId={}?writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_UNKNOWN_KEY_FOR_COMMIT_5F699CFD =
      "ConsensusSubscriptionCommitState?commit ? key ({},{},{}) ??";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_MISSING_WRITER_IDENTITY_BB10A3B1 =
      "ConsensusSubscriptionCommitState?direct commit ?? writer ???writerId={}?writerProgress={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_REJECT_DIRECT_COMMIT_WITHOUT_5B975E49 =
      "ConsensusSubscriptionCommitState??? direct commit?({},{},{}) ?? outstanding mapping";
  public static final String PIPE_LOG_ISCONSENSUSBASEDTOPIC_CHECK_FOR_TOPIC_MODE_RESULT_19EFA0F9 =
      "isConsensusBasedTopic ?? topic [{}]???={}???={}";
  public static final String PIPE_LOG_SET_IOTCONSENSUS_ONNEWPEERCREATED_CALLBACK_FOR_CONSENSUS_0766CE68 =
      "?? IoTConsensus.onNewPeerCreated ????? consensus subscription ????";
  public static final String PIPE_LOG_SET_IOTCONSENSUS_ONPEERREMOVED_CALLBACK_FOR_CONSENSUS_SUBSCRIPTION_21D4D6AC =
      "?? IoTConsensus.onPeerRemoved ????? consensus subscription ??";
  public static final String PIPE_LOG_NEW_DATAREGION_CREATED_CHECKING_CONSUMER_GROUP_S_FOR_AUTO_787C16E9 =
      "? DataRegion {} ???????? {} ? consumer group ????????currentSearchIndex={}";
  public static final String PIPE_LOG_AUTO_BINDING_CONSENSUS_QUEUE_FOR_TOPIC_IN_GROUP_TO_NEW_REGION_86F21649 =
      "? topic [{}]?group [{}] ???? consensus queue ?? Region {}?database={}?"
          + "tailStartSearchIndex={}?hasLocalPersistedState={}?committedRegionProgress={}?"
          + "initialRuntimeVersion={}?initialActive={}?";
  public static final String PIPE_LOG_DATAREGION_BEING_REMOVED_UNBINDING_ALL_CONSENSUS_SUBSCRIPTION_848A29F0 =
      "DataRegion {} ?????????? consensus subscription queue";
  public static final String PIPE_LOG_SETTING_UP_CONSENSUS_SUBSCRIPTIONS_FOR_CONSUMER_GROUP_TOPICS_204374A2 =
      "??? consumer group [{}] ?? consensus subscription?topics={}?consensus group ??={}";
  public static final String PIPE_LOG_SETTING_UP_CONSENSUS_QUEUE_FOR_TOPIC_ISTABLETOPIC_ORDERMODE_4F1CDC66 =
      "??? topic [{}] ?? consensus queue?isTableTopic={}?orderMode={}?config={}";
  public static final String PIPE_LOG_DISCOVERED_CONSENSUS_GROUP_S_FOR_TOPIC_IN_CONSUMER_GROUP_012EE420 =
      "?? {} ? consensus group?topic [{}]?consumer group [{}]?{}";
  public static final String PIPE_LOG_SKIPPING_REGION_DATABASE_FOR_TABLE_TOPIC_DATABASE_KEY_2DA27A84 =
      "?? Region {}?database={}??table topic [{}]?DATABASE_KEY={}?";
  public static final String PIPE_LOG_BINDING_CONSENSUS_PREFETCHING_QUEUE_FOR_TOPIC_IN_CONSUMER_45239EEA =
      "? topic [{}]?consumer group [{}] ? consensus prefetching queue ??? data region consensus "
          + "group [{}]?database={}?tailStartSearchIndex={}?hasLocalPersistedState={}?"
          + "committedRegionProgress={}?initialRuntimeVersion={}?initialActive={}?";
  public static final String PIPE_LOG_TORE_DOWN_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_CONSUMER_GROUP_80B84227 =
      "??? topic [{}]?consumer group [{}] ? consensus subscription";
  public static final String PIPE_LOG_CHECKING_NEW_SUBSCRIPTIONS_IN_CONSUMER_GROUP_FOR_CONSENSUS_4A56D78A =
      "???? consumer group [{}] ? consensus-based topic ?? subscription?{}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_IGNORE_STALE_RUNTIME_STATE_6C36B250 =
      "ConsensusSubscriptionSetupHandler??? Region {} ??? runtime state?incomingRuntimeVersion={}?"
          + "currentRuntimeVersion={}?runtimeState={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_APPLYING_RUNTIME_STATE_1FB8937E =
      "ConsensusSubscriptionSetupHandler??? Region {} ? runtime state?preferred writer {} -> {}?"
          + "runtimeVersion {} -> {}?runtimeState={}";
  public static final String PIPE_LOG_CONSENSUSSUBSCRIPTIONSETUPHANDLER_REGION_PREFERRED_WRITER_46C1A894 =
      "ConsensusSubscriptionSetupHandler?Region {} ? preferred writer ??? {} -> {}?runtimeVersion "
          + "{} -> {}?runtimeState={}?route hint?";
  public static final String PIPE_LOG_FAILED_TO_CHECK_IF_TOPIC_IS_CONSENSUS_BASED_DEFAULTING_TO_ECCE1509 =
      "?? topic [{}] ??? consensus-based ??????? false";
  public static final String PIPE_LOG_SKIPPING_SETUP_OF_CONSENSUS_BASED_SUBSCRIPTIONS_FOR_CONSUMER_46BEE6E4 =
      "?? consumer group [{}] ? consensus-based subscription ????? mode=incremental ??? "
          + "data_region_consensus_protocol_class={}???????? {}???? consensus ???{}?";
  public static final String
      EXCEPTION_SUBSCRIPTION_CANNOT_ARG_CONSENSUS_BASED_TOPIC_S_ARG_IN_CONSUMER_GROUP_ARG_BECAUSE_MODE_INCREMENTAL_ONLY_SUPPORTS_DATA_REGION_CONSENSUS_PROTOCOL_CLASS_ARG_BUT_CURRENT_CONFIGURED_VALUE_IS_ARG_RUNTIME_CONSENSUS_IMPLEMENTATION_ARG_6F21ED67 =
          "Subscription????? %s?consensus-based topic ? %s?consumer group ? [%s]??? "
              + "mode=incremental ??? data_region_consensus_protocol_class=%s???????? %s"
              + "???? consensus ???%s?";
  public static final String PIPE_LOG_TOPIC_CONFIG_NOT_FOUND_FOR_TOPIC_CANNOT_SET_UP_CONSENSUS_A93339CE =
      "??? topic [{}] ???????? consensus queue";
  public static final String PIPE_LOG_NO_LOCAL_IOTCONSENSUS_DATA_REGION_FOUND_FOR_TOPIC_IN_CONSUMER_6FD0600E =
      "topic [{}] ? consumer group [{}] ????? IoTConsensus data region???? data region ?????? "
          + "consensus subscription?";
  public static final String PIPE_LOG_FAILED_TO_TEAR_DOWN_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_F59E8B7C =
      "?? topic [{}]?consumer group [{}] ? consensus subscription ??";
  public static final String PIPE_LOG_FAILED_TO_AUTO_BIND_TOPIC_IN_GROUP_TO_NEW_REGION_5BFD0E7D =
      "? topic [{}]?group [{}] ?????? Region {} ??";
  public static final String PIPE_LOG_FAILED_TO_UNBIND_CONSENSUS_SUBSCRIPTION_QUEUES_FOR_REMOVED_7086F70A =
      "????? Region {} ? consensus subscription queue ??";
  public static final String PIPE_LOG_FAILED_TO_SET_UP_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_IN_CONSUMER_1A30001B =
      "? topic [{}]?consumer group [{}] ?? consensus subscription ??";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_DESERIALIZED_MERGED_INSERTNODE_51FB8295 =
      "ConsensusLogToTabletConverter????????? InsertNode?searchIndex={}?type={}?deviceId={}?"
          + "searchNodeCount={}";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_SEARCHINDEX_CONTAINS_NON_INSERTNODE_CFA9FA49 =
      "ConsensusLogToTabletConverter?searchIndex={} ??? InsertNode PlanNode?{}";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_CONVERTING_INSERTNODE_TYPE_B80428A0 =
      "ConsensusLogToTabletConverter????? InsertNode?type={}?deviceId={}";
  public static final String PIPE_LOG_UNSUPPORTED_INSERTNODE_TYPE_FOR_SUBSCRIPTION_E488EF74 =
      "????? subscription ? InsertNode ???{}";
  public static final String PIPE_LOG_CONSENSUSLOGTOTABLETCONVERTER_FAILED_TO_DESERIALIZE_ICONSENSUSREQUEST_EC1F6BAD =
      "ConsensusLogToTabletConverter????? IConsensusRequest ???type={}??searchIndex={}?{}";
  public static final String PIPE_LOG_INSERTNODE_TYPE_IS_NULL_SKIPPING_CONVERSION_A2F1ADF7 =
      "InsertNode ??? null?????";
  public static final String PIPE_LOG_UNSUPPORTED_DATA_TYPE_C8929F11 =
      "?????????{}";
  public static final String PIPE_LOG_UNSUPPORTED_DATA_TYPE_FOR_COPY_8AD25FE7 =
      "copy ?????????{}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCHING_QUEUE_IS_EMPTY_FOR_22836B5E =
      "ConsensusPrefetchingQueue {}?consumerId={} ? prefetching queue ???pendingEntriesSize={}?"
          + "nextExpected={}?isClosed={}?prefetchInitialized={}?subtaskScheduled={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLLING_QUEUE_SIZE_CONSUMERID_FCA0AAD3 =
      "ConsensusPrefetchingQueue {}??? poll?queue size={}?consumerId={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_DRAINED_ENTRIES_FROM_PENDINGENTRIES_2D4E0BE7 =
      "ConsensusPrefetchingQueue {}?? pendingEntries drain ? {} ????first searchIndex={}?last "
          + "searchIndex={}?nextExpected={}?prefetchingQueueSize={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_TIME_BASED_FLUSH_TABLETS_LINGERED_10A4EBA8 =
      "ConsensusPrefetchingQueue {}??????? flush?{} ? tablet ?? {}ms???={}ms?";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_GAP_DETECTED_EXPECTED_GOT_FILLING_70DD08B3 =
      "ConsensusPrefetchingQueue {}?????????={}???={}?? WAL ?? {} ????";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ACCUMULATE_COMPLETE_BATCHSIZE_FA3F3B41 =
      "ConsensusPrefetchingQueue {}??????batchSize={}?processed={}?skipped={}?lingerTablets={}?"
          + "nextExpected={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SUBSCRIPTION_WAL_READ_ENTRIES_14AA5096 =
      "ConsensusPrefetchingQueue {}?subscription WAL ?? {} ????nextExpectedSearchIndex={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SUBSCRIPTION_WAL_EXHAUSTED_AT_E61AF763 =
      "ConsensusPrefetchingQueue {}?subscription WAL ? {} ????? WAL ? {}??? WAL ????????????";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SKIP_STALE_EVENT_WITH_SEARCHINDEX_07A09B36 =
      "ConsensusPrefetchingQueue {}????????searchIndex ?? [{}, {}]?expectedSeekGeneration={}?"
          + "currentSeekGeneration={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ENQUEUED_EVENT_WITH_TABLETS_SEARCHINDEX_140FDDCB =
      "ConsensusPrefetchingQueue {}?????? {} ? tablet ????searchIndex ?? [{}, {}]?"
          + "prefetchQueueSize={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_REJECT_WITHOUT_WRITER_PROGRESS_D84AA802 =
      "ConsensusPrefetchingQueue {}????? writer progress ? {}?commitContext={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_REJECT_FOR_INACTIVE_QUEUE_COMMITCONTEXT_AE6D382C =
      "ConsensusPrefetchingQueue {}?? queue ?????? {}?commitContext={}?runtimeVersion={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_RECYCLED_TIMED_OUT_EVENT_BACK_5E58639C =
      "ConsensusPrefetchingQueue {}?????? {} ??? prefetching queue";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_INJECTED_WATERMARK_WATERMARKTIMESTAMP_BF373164 =
      "ConsensusPrefetchingQueue {}???? WATERMARK?watermarkTimestamp={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_CREATED_DORMANT_CONSUMERGROUPID_863BC6D6 =
      "ConsensusPrefetchingQueue ????dormant??consumerGroupId={}?topicName={}?orderMode={}?"
          + "consensusGroupId={}?fallbackCommittedRegionProgress={}?fallbackTailSearchIndex={}?"
          + "initialRuntimeVersion={}?initialActive={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCH_INITIALIZED_STARTSEARCHINDEX_69B53EE6 =
      "ConsensusPrefetchingQueue {}?prefetch ?????startSearchIndex={}?progressSource={}?"
          + "recoveryWriterCount={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PERIODIC_STATS_LAG_PENDINGDELTA_WALGAPSKIPPEDENTRIES_9A4E6608 =
      "ConsensusPrefetchingQueue {}：周期统计，lag={}，pendingDelta={}，walDelta={}，pendingTotal={}，"
          + "walTotal={}，walGapSkippedEntries={}，pendingQueueSize={}，prefetchingQueueSize={}，"
          + "inFlightEventsSize={}，realtimeWriterCount={}，walHasNext={}，isActive={}，"
          + "subtaskScheduled={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_WAITING_MS_FOR_WAL_GAP_TO_BECOME_7D91C6C5 =
      "ConsensusPrefetchingQueue {}??? {}ms?? WAL ?? [{}, {}) ???currentNextExpected={}?"
          + "currentWalIndex={}?seekGeneration={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKTOREGIONPROGRESS_WRITERCOUNT_3134A29B =
      "ConsensusPrefetchingQueue {}?seekToRegionProgress writerCount={} -> {}?searchIndex={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKAFTERREGIONPROGRESS_WRITERCOUNT_C6B26D20 =
      "ConsensusPrefetchingQueue {}?seekAfterRegionProgress writerCount={} -> {}?searchIndex={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ABORTED_PENDING_SEEK_DURING_RUNTIME_F9928604 =
      "ConsensusPrefetchingQueue {}????????????? seek({})??? prefetchInitialized {} -> "
          + "{}?seekGeneration {} -> {}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_SCHEDULE_SEEK_BECAUSE_9E407068 =
      "ConsensusPrefetchingQueue {}??? seek({}) ??????{}??? prefetchInitialized {} -> {}?"
          + "seekGeneration {} -> {}";
  public static final String MESSAGE_THE_QUEUE_IS_CLOSING_AC6C2AB4 = "??????";
  public static final String MESSAGE_PREFETCH_RUNTIME_IS_UNAVAILABLE_F1721E89 =
      "????????";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEK_APPLIED_TO_SEARCHINDEX_WRITERCOUNT_FA2C4327 =
      "ConsensusPrefetchingQueue {}?seek({}) ???? searchIndex={}?writerCount={}?seekGeneration={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FLUSHING_LINGERING_TABLETS_DURING_4C4AF235 =
      "ConsensusPrefetchingQueue {}????? flush {} ??? tablet";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ISACTIVE_SET_TO_REGION_EC0AD7BA =
      "ConsensusPrefetchingQueue {}?isActive ??? {}?region={}?";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_RUNTIMEACTIVEWRITERNODEIDS_EFFECTIVEACTIVEWRITERNODEIDS_246519D2 =
      "ConsensusPrefetchingQueue {}?runtimeActiveWriterNodeIds={}?"
          + "effectiveActiveWriterNodeIds={}?region={}?orderMode={}?preferredWriterNodeId={}?";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFERREDWRITERNODEID_SET_TO_EFFECTIVEACTIVEWRITERNODEIDS_B08E8180 =
      "ConsensusPrefetchingQueue {}?preferredWriterNodeId ??? {}?"
          + "effectiveActiveWriterNodeIds={}?region={}?orderMode={}?";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ORDERMODE_SET_TO_EFFECTIVEACTIVEWRITERNODEIDS_CDD3C86E =
      "ConsensusPrefetchingQueue {}?orderMode ??? {}?effectiveActiveWriterNodeIds={}?region={}?"
          + "preferredWriterNodeId={}?runtimeActiveWriterNodeIds={}?";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_APPLIED_RUNTIMEVERSION_36E05B80 =
      "ConsensusPrefetchingQueue {}???? runtimeVersion {}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_APPLIED_RUNTIMESTATE_PREFERREDWRITERNODEID_D845E9D6 =
      "ConsensusPrefetchingQueue {}???? runtimeState={}?preferredWriterNodeId={}";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLL_COMMITTED_EVENT_BROKEN_INVARIANT_E478FA3C =
      "ConsensusPrefetchingQueue {} poll ?????? {}??????????????";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POLL_NON_POLLABLE_EVENT_BROKEN_E9551325 =
      "ConsensusPrefetchingQueue {} poll ??? poll ?? {}??????????? nack";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_INTERRUPTED_WHILE_POLLING_B7CFF5FD =
      "ConsensusPrefetchingQueue {} ? polling ?????";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_READING_SUBSCRIPTION_WAL_A3888AC5 =
      "ConsensusPrefetchingQueue {}??? subscription WAL ??";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_CLOSING_SUBSCRIPTION_WAL_19711C01 =
      "ConsensusPrefetchingQueue {}??? subscription WAL iterator ??";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_COMMIT_CONTEXT_DOES_NOT_EXIST_99B8A8F3 =
      "ConsensusPrefetchingQueue {}?ack ? commit context {} ???";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_EVENT_ALREADY_COMMITTED_AC34E829 =
      "ConsensusPrefetchingQueue {}??? {} ???";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_ADVANCE_COMMIT_FRONTIER_56E606C0 =
      "ConsensusPrefetchingQueue {}??? {} ? commit frontier ??";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_COMMIT_CONTEXT_DOES_NOT_EXIST_05F6C6E0 =
      "ConsensusPrefetchingQueue {}?nack ? commit context {} ???";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKTOREGIONPROGRESS_NOT_SUPPORTED_85477BAB =
      "ConsensusPrefetchingQueue {}???? seekToRegionProgress??? WAL ???";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_SEEKAFTERREGIONPROGRESS_NOT_SUPPORTED_55F36BE8 =
      "ConsensusPrefetchingQueue {}???? seekAfterRegionProgress??? WAL ???";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_READ_WAL_METADATA_FROM_A2ED50D1 =
      "ConsensusPrefetchingQueue {}??? seekToEnd frontier ??? {} ?? WAL metadata ??";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_ERROR_DURING_DEREGISTER_34C332E7 =
      "ConsensusPrefetchingQueue {}???????";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_FAILED_TO_FLUSH_LINGERING_BATCH_F97D8AA7 =
      "ConsensusPrefetchingQueue {}????? flush ?? batch ?????? batch";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_PREFETCH_ROUND_FAILED_TYPE_MESSAGE_63BC909B =
      "ConsensusPrefetchingQueue {}?prefetch ?????type={}?message={}?";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POISON_MESSAGE_DETECTED_NACKCOUNT_3A9255FB =
      "ConsensusPrefetchingQueue {}???? poison message?nackCount={}??????????????? {} ???? ack";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_POISON_MESSAGE_DETECTED_DURING_23159F02 =
      "ConsensusPrefetchingQueue {}?recycle ????? poison message?nackCount={}????? {} ???? ack";
  public static final String PIPE_LOG_PROGRESSWALITERATOR_FAILED_TO_OPEN_NEAR_LIVE_WAL_FILE_RETRYING_5AEB94AC =
      "ProgressWALIterator??? near-live WAL ?? {} ????????????";
  public static final String PIPE_LOG_PROGRESSWALITERATOR_ERROR_READING_WAL_2DB46D41 =
      "ProgressWALIterator??? WAL ??";
  public static final String PIPE_LOG_PROGRESSWALITERATOR_FAILED_TO_OPEN_WAL_FILE_SKIPPING_29CA1092 =
      "ProgressWALIterator：打开 WAL 文件 {} 失败，跳过该文件";
  public static final String PIPE_LOG_PROGRESSWALITERATOR_SKIPPED_UNREADABLE_RETAINED_WAL_FILES_FFC8455E =
      "ProgressWALIterator：跳过了 {} 个无法读取的保留 WAL 文件，directory={}，firstFile={}，"
          + "lastFile={}，firstError={}；这些文件中的历史订阅数据无法重放";
  public static final String PIPE_LOG_CONSENSUSPREFETCHINGQUEUE_WAL_REPLAY_SKIPPED_UNAVAILABLE_SEARCH_INDEXES_B8023B64 =
      "ConsensusPrefetchingQueue {}：WAL 重放跳过了不可用的 searchIndex 区间 [{}, {})，"
          + "skippedEntries={}，totalWalGapSkippedEntries={}；缺失的 WAL 数据可能已在订阅消费前被回收";
  public static final String PIPE_LOG_PIPE_TERMINATE_EVENT_COMMITTED_FOR_HISTORICAL_TRANSFER_CREATIONTIME_9B807B28 =
      "Pipe {}@{}??????????????creationTime?{}?shouldMark?{}?{}";
  public static final String PIPE_LOG_PIPE_HISTORICAL_SOURCE_HAS_SUPPLIED_ALL_EVENTS_EMITTING_8B58DE19 =
      "Pipe {}@{}??? source ?????????????????{}";
  public static final String PIPE_LOG_PIPE_REALTIME_SOURCE_ON_DATA_REGION_LISTENTOTSFILE_LISTENTOINSERTNODE_A02E1552 =
      "Pipe {}@{} {}?DataRegion {} ???? source?listenToTsFile={}?listenToInsertNode={}?"
          + "registeredSourceCount={}?tsFileSourceCount={}?insertNodeSourceCount={}??";
  public static final String PIPE_LOG_INTERRUPTED_WHILE_WAITING_FOR_IN_FLIGHT_PUBLISHES_TO_FINISH_C8E3757B =
      "?? DataRegion {} ?? assigner ??????? publish ????????";
  public static final String PIPE_LOG_SCHEMAREGIONSTATEMACHINE_EXECUTE_READ_PLAN_FRAGMENTINSTANCE_F85A001F =
      "SchemaRegionStateMachine[{}]???? plan?FragmentInstance-{}";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_IS_NO_LONGER_THE_SCHEMA_REGION_LEADER_FD783B3C =
      "???? [nodeId?{}] ??? schema region leader [regionId?{}]?? leader ? [nodeId?{}]";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_IS_NO_LONGER_THE_SCHEMA_REGION_LEADER_12E06F99 =
      "???? [nodeId?{}] ??? schema region leader [regionId?{}]??????????";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_IS_NO_LONGER_THE_SCHEMA_REGION_LEADER_3092822E =
      "???? [nodeId?{}] ??? schema region leader [regionId?{}]?? leader ????????????";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_BECOMES_SCHEMA_REGION_LEADER_REGIONID_46C70A32 =
      "???? [nodeId?{}] ?? schema region leader [regionId?{}]";
  public static final String PIPE_LOG_CURRENT_NODE_NODEID_AS_SCHEMA_REGION_LEADER_REGIONID_IS_F00BFAC5 =
      "???? [nodeId?{}] ?? schema region leader [regionId?{}] ??????";
  public static final String PIPE_LOG_SCHEMA_REGION_LISTENING_QUEUE_LISTEN_TO_SNAPSHOT_FAILED_64845A44 =
      "Schema Region Listening Queue ?? snapshot ??????????????snapshotPaths:{}";
  public static final String PIPE_LOG_WRITE_OPERATION_FAILED_BECAUSE_RETRYTIME_34EFBE99 =
      "??????????{}??????{}?";
  public static final String PIPE_LOG_EXCEPTION_OCCURS_WHEN_TAKING_SNAPSHOT_FOR_IN_48CBDFCC =
      "? {}-{} ? {} ??? snapshot ?????";
  public static final String PIPE_LOG_MEETS_ERROR_WHEN_GETTING_SNAPSHOT_FILES_FOR_9BFA76B9 =
      "?? {}-{} ? snapshot ?????";
  public static final String PIPE_LOG_WRITE_OPERATION_STILL_FAILED_AFTER_RETRY_TIMES_BECAUSE_15EEA702 =
      "??????? {} ?????????{}?";
  public static final String PIPE_LOG_NOW_TRY_TO_DELETE_DIRECTLY_DATABASEPATH_DELETEPATH_A427CD01 =
      "?????????databasePath?{}?deletePath?{}";
  public static final String PIPE_LOG_BATCH_FAILURE_IN_EXECUTING_A_INSERTTABLETNODE_DEVICE_STARTTIME_9A5A70F6 =
      "???? InsertTabletNode ???device?{}?startTime?{}?measurements?{}??????{}";
  public static final String PIPE_LOG_INSERT_ROW_FAILED_DEVICE_TIME_MEASUREMENTS_FAILING_STATUS_63054E8B =
      "??????device?{}?time?{}?measurements?{}??????{}";
  public static final String PIPE_LOG_INSERT_TABLET_FAILED_DEVICE_STARTTIME_MEASUREMENTS_FAILING_B409B2C4 =
      "?? tablet ???device?{}?startTime?{}?measurements?{}??????{}";

  // ---------------------------------------------------------------------------
  // ??????
  // ---------------------------------------------------------------------------
  public static final String PIPE_EXCEPTION_UNSUPPORTED_SUBSCRIPTION_REQUEST_VERSION_D_1E7C211A =
      "???? subscription ???? %d";
  public static final String PIPE_EXCEPTION_PAYLOAD_SIZE_S_BYTE_S_WILL_EXCEED_THE_THRESHOLD_S_BYTE_S_6043B3D8 =
      "payload ?? %s byte(s) ????? %s byte(s)";
  public static final String PIPE_EXCEPTION_INCONSISTENT_READ_LENGTH_BROKEN_INVARIANT_EXPECTED_S_ACTUAL_9203668A =
      "???????????????????%s????%s";
  public static final String PIPE_EXCEPTION_TIMEOUTEXCEPTION_WAITED_S_SECONDS_8B31A3A5 =
      "TimeoutException??? %s ?";
  public static final String PIPE_EXCEPTION_THE_SUBSCRIPTIONCONNECTORSUBTASKMANAGER_ONLY_SUPPORTS_SUBSCRIPTION_CEFFAAA9 =
      "SubscriptionConnectorSubtaskManager ??? subscription-sink?";
  public static final String PIPE_EXCEPTION_FAILED_TO_CONSTRUCT_SUBSCRIPTION_SINK_BECAUSE_OF_S_OR_S_DBA27DC2 =
      "?? subscription sink ??????pipe connector ?????? %s ? %s";
  public static final String PIPE_EXCEPTION_FAILED_TO_GET_PENDINGQUEUE_NO_SUCH_SUBTASK_S_B445404A =
      "?? PendingQueue ??????? subtask?%s";
  public static final String PIPE_EXCEPTION_INVALID_BASE64_URL_COMPONENT_LENGTH_F1F1B6BA =
      "??? base64 URL component ??";
  public static final String PIPE_EXCEPTION_INVALID_CONSENSUS_SUBSCRIPTION_PROGRESS_REGION_COUNT_S_7CE4FD8E =
      "??? consensus subscription progress Region ?? %s";
  public static final String PIPE_EXCEPTION_INVALID_CONSENSUS_SUBSCRIPTION_PROGRESS_PAYLOAD_LENGTH_S_8C145986 =
      "??? consensus subscription progress payload ?? %s";
  public static final String PIPE_EXCEPTION_MALFORMED_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_S_83042847 =
      "????? consensus subscription progress ?? %s";
  public static final String PIPE_EXCEPTION_ILLEGAL_S_S_72D743AA =
      "??? %s=%s";
  public static final String PIPE_EXCEPTION_INTERRUPTED_WHILE_WAITING_FOR_SEEK_APPLICATION_7C7ECAF2 =
      "?? seek ??????";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_RECOVER_FROM_NON_EMPTY_C1B367EF =
      "ConsensusPrefetchingQueue %s?????? WAL ???????????? Region progress ???%s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_INITIALIZE_REPLAY_START_E02DE40E =
      "ConsensusPrefetchingQueue %s????? region progress %s ??? replay ???%s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_SEEKTOREGIONPROGRESS_2746E514 =
      "ConsensusPrefetchingQueue %s????? seekToRegionProgress %s?%s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_CANNOT_SEEKAFTERREGIONPROGRESS_48A500C3 =
      "ConsensusPrefetchingQueue %s????? seekAfterRegionProgress %s?%s";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_IS_CLOSING_WHILE_APPLYING_SEEK_2BB2B431 =
      "ConsensusPrefetchingQueue %s ???? seek ???";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_RUNTIME_STOPPED_BEFORE_SEEK_7BCB4F4B =
      "ConsensusPrefetchingQueue %s ?????? seek(%s) ????";
  public static final String PIPE_EXCEPTION_CONSENSUSPREFETCHINGQUEUE_S_IS_CLOSING_BEFORE_SEEK_APPLIES_F893BB02 =
      "ConsensusPrefetchingQueue %s ? seek ???????";
  public static final String PIPE_EXCEPTION_NO_PRIVILEGE_FOR_SELECT_FOR_USER_S_AT_TABLE_S_S_84B0C299 =
      "?? %s ?? %s.%s ?? SELECT ??";
  public static final String PIPE_EXCEPTION_EXPECTED_BINARY_BYTE_OR_STRING_BUT_WAS_S_7976B10F =
      "?? Binary?byte[] ? String???? %s?";
  public static final String PIPE_EXCEPTION_TIMEOUTEXCEPTION_WAITED_S_SECONDS_FOR_MEMORY_TO_PARSE_TSFILE_0E4EF8FD =
      "TimeoutException??? %s ?????? TsFile ????";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_DATA_TYPE_S_FOR_COLUMN_S_9F870C01 =
      "???? %s ?????? %s";
  public static final String PIPE_EXCEPTION_COLUMN_S_NOT_FOUND_0FA13581 =
      "???? %s";
  public static final String PIPE_EXCEPTION_INSERTNODE_TYPE_S_IS_NOT_SUPPORTED_7DF82B58 =
      "??? InsertNode ?? %s?";
  public static final String PIPE_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_5D5C02E4 =
      "??????? %s?";
  public static final String PIPE_EXCEPTION_FORCEALLOCATEFORTABLET_FAILED_TO_ALLOCATE_BECAUSE_THERE_F878474D =
      "forceAllocateForTablet?????????tablet ???????????? %d bytes?tablet ?????? %d bytes??????? %d "
          + "bytes";
  public static final String PIPE_EXCEPTION_FORCEALLOCATEFORTSFILE_FAILED_TO_ALLOCATE_BECAUSE_THERE_6D614467 =
      "forceAllocateForTsFile?????????tsfile ???????????? %d bytes?tsfile ?????? %d bytes??????? %d "
          + "bytes";
  public static final String PIPE_EXCEPTION_FORCEALLOCATE_FAILED_TO_ALLOCATE_MEMORY_AFTER_D_RETRIES_44EF7AE7 =
      "forceAllocate??? %d ??????????????? %d bytes??????? %d bytes??????? %d bytes";
  public static final String PIPE_EXCEPTION_FORCERESIZE_FAILED_TO_ALLOCATE_MEMORY_AFTER_D_RETRIES_TOTAL_8C6948BC =
      "forceResize??? %d ??????????????? %d bytes??????? %d bytes??????? %d bytes";
  public static final String PIPE_EXCEPTION_FAILED_TO_GET_HARDLINK_OR_COPIED_FILE_IN_PIPE_DIR_FOR_FILE_F009D86E =
      "?? pipe ????? %s ? hardlink ????????????? tsfile?mod ??? resource ??";
  public static final String PIPE_EXCEPTION_PIPEPLANTOSTATEMENTVISITOR_DOES_NOT_SUPPORT_VISITING_GENERAL_452AAA60 =
      "PipePlanToStatementVisitor ??????? plan?PlanNode?%s";
  public static final String PIPE_EXCEPTION_AIRGAP_PAYLOAD_LENGTH_D_EXCEEDS_MAXIMUM_ALLOWED_D_CLOSING_D1712B3D =
      "AirGap payload ???%d?????????%d?????? %s ???";
  public static final String PIPE_EXCEPTION_DETECTED_SUSPICIOUS_NESTED_E_LANGUAGE_PREFIX_CLOSING_CONNECTION_69C76172 =
      "???????? E-Language ??????? %s ???";
  public static final String PIPE_EXCEPTION_AUTO_CREATE_DATABASE_FAILED_S_STATUS_CODE_S_D8EB60FA =
      "??????????%s?????%s";
  public static final String PIPE_EXCEPTION_IOTCONSENSUSV2_PIPENAME_S_FAILED_TO_CREATE_RECEIVER_FILE_DD67E854 =
      "IoTConsensusV2-PipeName-%s??? receiver ???? %s ????????????????????????";
  public static final String PIPE_EXCEPTION_IOTCONSENSUSV2_PIPENAME_S_FAILED_TO_CREATE_RECEIVER_FILE_5ADC430A =
      "IoTConsensusV2-PipeName-%s??? receiver ???? %s ????????????????????";
  public static final String PIPE_EXCEPTION_IOTCONSENSUSV2_PIPENAME_S_FAILED_TO_CREATE_TSFILEWRITER_85EC8DD2 =
      "IoTConsensusV2-PipeName-%s??? tsFileWriter-%d receiver ??????";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_IOTCONSENSUSV2_REQUEST_VERSION_D_E1D94606 =
      "???? iotConsensusV2 ???? %d";
  public static final String PIPE_EXCEPTION_CAN_NOT_EXECUTE_DELETE_STATEMENT_S_3563E8A3 =
      "?????????%s";
  public static final String PIPE_EXCEPTION_CAN_NOT_EXECUTE_LOAD_TSFILE_STATEMENT_S_8CC1A096 =
      "?????? TsFile ???%s";
  public static final String PIPE_EXCEPTION_FAILED_TO_GET_PIPE_TASK_PROGRESS_INDEX_WITH_PIPE_NAME_S_CFE9DE7C =
      "?? pipe ?????????pipe ???%s???? ID?%s?";
  public static final String PIPE_EXCEPTION_EXCEPTION_IN_PIPE_PROCESS_SUBTASK_S_LAST_EVENT_S_ROOT_CAUSE_95B49C24 =
      "pipe ???????subtask?%s????? event?%s????%s";
  public static final String PIPE_EXCEPTION_THE_VISIBILITY_OF_THE_PIPE_S_S_IS_NOT_COMPATIBLE_WITH_THE_30B8BF0A =
      "pipe?%s?%s?????? source?%s?%s?%s??processor?%s?%s?%s?? connector?%s?%s?%s?????????";
  public static final String PIPE_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_WHEN_CONVERT_DATA_AT_CLIENT_405429CC =
      "??????????????? %s";
  public static final String PIPE_EXCEPTION_HANDSHAKE_ERROR_WITH_RECEIVER_S_S_CODE_D_MESSAGE_S_4ED82649 =
      "receiver %s:%s ?????code?%d?message?%s?";
  public static final String PIPE_EXCEPTION_THE_WEBSOCKET_SERVER_HAS_ALREADY_BEEN_CREATED_WITH_PORT_FFC420AE =
      "WebSocket server ????? %d ????? cdc.port ????? %d?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_INSERTION_EVENT_S_703A2E9E =
      "?? tsFile insertion event ????????%s?";
  public static final String PIPE_EXCEPTION_CANNOT_SEND_PIPE_DATA_TO_RECEIVER_S_S_BECAUSE_S_25143D54 =
      "??? receiver %s:%s ?? pipe data????%s?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_EVENT_S_BECAUSE_S_60A63AD7 =
      "?? event %s ???????????%s?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TABLET_INSERTION_EVENT_S_BECAUSE_A6F87EF5 =
      "?? tablet insertion event %s ???????????%s?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_INSERTION_EVENT_S_BECAUSE_BDE61690 =
      "?? tsfile insertion event %s ???????????%s?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_EVENT_S_BECAUSE_S_F36D2A6B =
      "?? tsfile event %s ???????????%s?";
  public static final String PIPE_EXCEPTION_FAILED_TO_TRANSFER_TABLET_INSERTION_EVENT_S_BECAUSE_S_9710318F =
      "?? tablet insertion event %s ??????%s?";
  public static final String PIPE_EXCEPTION_FAILED_TO_TRANSFER_TSFILE_INSERTION_EVENT_S_BECAUSE_S_21AD3263 =
      "?? tsfile insertion event %s ??????%s?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_FILE_S_BECAUSE_S_3C673B7A =
      "???? %s ???????????%s?";
  public static final String PIPE_EXCEPTION_PARAMETERS_IN_SET_S_ARE_NOT_ALLOWED_IN_SKIPIF_AAF177AD =
      "?? %s ?????????? 'skipif' ?";
  public static final String PIPE_EXCEPTION_FAILED_TO_CHECK_PASSWORD_FOR_PIPE_S_0B1A5C73 =
      "?? pipe %s ??????";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_DELETION_S_BECAUSE_S_3B250B4B =
      "?? deletion %s ???????????%s?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TABLET_BATCH_BECAUSE_S_6BEC52E7 =
      "?? tablet batch ???????????%s?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_INSERT_NODE_TABLET_INSERTION_D993C7AB =
      "?? insert node tablet insertion event ???????????%s?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_RAW_TABLET_INSERTION_EVENT_BECAUSE_D8ACEC3C =
      "?? raw tablet insertion event ???????????%s?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_SEAL_FILE_S_BECAUSE_S_DC87F263 =
      "seal ?? %s ???????????%s?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_SCHEMA_REGION_WRITE_PLAN_S_BECAUSE_AEB210C7 =
      "?? schema region write plan %s ???????????%s?";
  public static final String PIPE_EXCEPTION_NETWORK_ERROR_WHEN_SEAL_SNAPSHOT_FILE_S_S_AND_S_BECAUSE_5EF373E6 =
      "seal snapshot ?? %s?%s ? %s ???????????%s?";
  public static final String PIPE_EXCEPTION_FAILED_TO_TRANSFER_SLICE_ORIGIN_REQ_S_S_SLICE_INDEX_D_SLICE_44E1CF32 =
      "?? slice ???Origin req?%s-%s?slice index?%d?slice count?%d????%s";
  public static final String PIPE_EXCEPTION_THE_EXISTING_SERVER_WITH_TCP_PORT_S_AND_HTTPS_PORT_S_S_S_08C076F7 =
      "?? server ? tcp port %s ? https port %s ? %s %s ??? %s %s ????????";
  public static final String PIPE_EXCEPTION_INVALID_KEYSTORE_THE_SERVERPRIVATEKEY_IS_S_F5F3C02F =
      "??? keyStore?serverPrivateKey ? %s";
  public static final String PIPE_EXCEPTION_THE_FOLDER_NODE_FOR_S_DOES_NOT_EXIST_CC0776AE =
      "?? %s ? folder node ????";
  public static final String PIPE_EXCEPTION_THE_NODE_S_DOES_NOT_EXIST_52F98935 =
      "Node %s ????";
  public static final String PIPE_EXCEPTION_THE_EXISTING_SERVER_WITH_NODEURL_S_S_S_S_CONFLICTS_TO_THE_1C06A4F6 =
      "?? server ? nodeUrl %s ? %s %s ??? %s %s ????????";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_S_CONSTRUCTED_FROM_PIPETRANSFERTABLETINSERTNODEREQ_FF5ED1D7 =
      "? PipeTransferTabletInsertNodeReq ???? InsertBaseStatement %s ???";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTNODE_TYPE_S_WHEN_CONSTRUCTING_STATEMENT_FROM_4A055174 =
      "?? insert node ?? statement ????? InsertNode ?? %s?";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_S_CONSTRUCTED_FROM_PIPETRANSFERTABLETBINARYREQV2_06D274D2 =
      "? PipeTransferTabletBinaryReqV2 ???? InsertBaseStatement %s ???";
  public static final String PIPE_EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_S_CONSTRUCTED_FROM_PIPETRANSFERTABLETINSERTNODEREQV2_16F399B6 =
      "? PipeTransferTabletInsertNodeReqV2 ???? InsertBaseStatement %s ???";
  public static final String PIPE_EXCEPTION_FAILED_TO_CREATE_FILE_DIR_FOR_BATCH_S_8FCD9125 =
      "? batch %s ????????";
  public static final String PIPE_EXCEPTION_FAILED_TO_CREATE_BATCH_FILE_DIR_BATCH_ID_S_EA8BE86C =
      "?? batch ????????Batch id = %s?";
  public static final String PIPE_EXCEPTION_PIPETREESTATEMENTTOPLANVISITOR_DOES_NOT_SUPPORT_VISITING_3A4A6524 =
      "PipeTreeStatementToPlanVisitor ??????? statement?Statement?%s";
  public static final String PIPE_EXCEPTION_PIPESTATEMENTTOPLANVISITOR_DOES_NOT_SUPPORT_VISITING_GENERAL_590C6BD7 =
      "PipeStatementToPlanVisitor ??????? statement?Statement?%s";
  public static final String PIPE_EXCEPTION_THE_PATH_PATTERN_S_IS_NOT_VALID_FOR_THE_SOURCE_ONLY_PREFIX_139F93D6 =
      "source ? path pattern %s ?????? prefix ? full path?";
  public static final String PIPE_EXCEPTION_S_S_S_SHOULD_BE_LESS_THAN_OR_EQUAL_TO_S_S_S_0B9726E1 =
      "%s?%s?[%s] ?????? %s?%s?[%s]?";
  public static final String PIPE_EXCEPTION_PARAMETERS_IN_SET_S_ARE_NOT_ALLOWED_IN_REALTIME_LOOSE_RANGE_BACD2475 =
      "?? %s ?????????? 'realtime.loose-range' ?";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_EVENT_TYPE_S_FOR_LOG_REALTIME_EXTRACTOR_S_961C5D2D =
      "event type %s ????? log realtime extractor %s";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_EVENT_TYPE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_9C4F4C82 =
      "event type %s ????? hybrid realtime extractor %s";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_STATE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_43BD62C2 =
      "state %s ????? hybrid realtime extractor %s";
  public static final String PIPE_EXCEPTION_UNSUPPORTED_EVENT_TYPE_S_FOR_HYBRID_REALTIME_EXTRACTOR_S_474BAAC2 =
      "event type %s ???? hybrid realtime extractor %s ???";
  public static final String PIPE_EXCEPTION_PARAMETERS_IN_SET_S_ARE_NOT_ALLOWED_IN_HISTORY_LOOSE_RANGE_0F685D5C =
      "?? %s ?????????? 'history.loose-range' ?";
  public static final String PIPE_EXCEPTION_THE_AGGREGATOR_AND_OUTPUT_NAME_S_IS_INVALID_BC22CF92 =
      "aggregator ? output name %s ???";
  public static final String PIPE_EXCEPTION_THE_NEEDED_INTERMEDIATE_VALUES_S_ARE_NOT_DEFINED_3FF0C52D =
      "?? intermediate values %s ????";
  public static final String PIPE_EXCEPTION_THE_PROCESSOR_S_IS_NOT_A_WINDOWING_PROCESSOR_EA5B59BA =
      "processor %s ?? windowing processor?";
  public static final String PIPE_EXCEPTION_THE_AGGREGATE_PROCESSOR_DOES_NOT_SUPPORT_PROGRESSINDEXTYPE_35351D27 =
      "aggregate processor ??? progressIndexType %s";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_IS_NOT_SUPPORTED_E1A6F05D =
      "????? %s";
  public static final String PIPE_EXCEPTION_THE_OUTPUT_TABLET_DOES_NOT_SUPPORT_COLUMN_TYPE_S_62F3845C =
      "output tablet ??? column type %s";
  public static final String PIPE_EXCEPTION_THE_NEW_DATABASE_NAME_S_IS_INVALID_IT_SHOULD_NOT_CONTAIN_C3AB555E =
      "????? %s ??????? '%s'????? pattern %s???????? %d";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_BOOLEAN_F19CCF75 =
      "?? %s ????? boolean?";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_INT_659069CC =
      "?? %s ????? int?";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_LONG_2D206561 =
      "?? %s ????? long?";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_FLOAT_C15A8A95 =
      "?? %s ????? float?";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_DOUBLE_E577C0D7 =
      "?? %s ????? double?";
  public static final String PIPE_EXCEPTION_THE_TYPE_S_CANNOT_BE_CASTED_TO_STRING_34983FBD =
      "?? %s ????? string?";
  public static final String PIPE_EXCEPTION_UNABLE_TO_CREATE_IOTCONSENSUSV2_DELETION_DIR_AT_S_800EE360 =
      "??? %s ?? iotConsensusV2 deletion dir";
  public static final String PIPE_EXCEPTION_THE_TIMESERIES_S_USED_NEW_TYPE_S_IS_NOT_COMPATIBLE_WITH_455D4D4A =
      "timeseries %s ?????? %s ????? %s ????";
  public static final String PIPE_EXCEPTION_THERE_ARE_TWO_TYPES_OF_PLANNODE_IN_ONE_REQUEST_S_AND_S_30FB3EE5 =
      "????????? PlanNode ???%s ? %s";
  public static final String PIPE_EXCEPTION_THERE_ARE_TWO_TYPES_OF_PLANNODE_IN_ONE_REQUEST_S_AND_SEARCHNODE_F8B4D860 =
      "????????? PlanNode ???%s ? SearchNode";
  public static final String COMPLETE_PAGE_BODY_EXPECTED_ACTUAL_FMT =
      "page body ???????%s????%s";
  public static final String UNCOMPRESS_PAGE_DATA_FAILED_FMT =
      "???????????%s???????%s?page header?%s%s";
  public static final String FAILED_TO_CLOSE_LISTENING_QUEUE_FOR_SCHEMAREGION_BECAUSE_FMT =
      "?? SchemaRegion %s ???????????%s";
  public static final String PIPE_SINK_HEARTBEAT_OR_TRANSFER_FAILED_FMT =
      "PipeConnector?%s(id?%s) heartbeat ?????? generic event ???????????%s";
  public static final String FAILED_TO_ADD_ITEM_WITH_OPC_ERROR_CODE_FMT =
      "?? item %s ???opc ????0x%s";
  public static final String FAILED_TO_WRITE_WITH_VALUE_AND_OPC_ERROR_CODE_FMT =
      "?? %s ?????%s?opc ????0x%s";
  public static final String NO_CERTIFICATE_FOUND =
      "?????";
  public static final String CERTIFICATE_MISSING_APPLICATION_URI =
      "???? application URI";
  public static final String NULL_VALUE =
      "null";
  public static final String INCREASE_REFERENCE_COUNT_ERROR_HOLDER_FMT =
      "?????????Holder Message?%s";
  public static final String DECREASE_REFERENCE_COUNT_ERROR_HOLDER_FMT =
      "?????????Holder Message?%s";
  public static final String INCREASE_REFERENCE_COUNT_TSFILE_OR_MODFILE_ERROR_HOLDER_FMT =
      "? TsFile %s ? modFile %s ?????????Holder Message?%s";
  public static final String DECREASE_REFERENCE_COUNT_TSFILE_ERROR_HOLDER_FMT =
      "? TsFile %s ?????????Holder Message?%s";
  public static final String INCREASE_REFERENCE_COUNT_MTREE_OR_TLOG_ERROR_HOLDER_FMT =
      "? mTree ?? %s ? tLog %s ?????????Holder Message?%s";
  public static final String DECREASE_REFERENCE_COUNT_MTREE_OR_TLOG_ERROR_HOLDER_FMT =
      "? mTree ?? %s ? tLog %s ?????????Holder Message?%s";
  public static final String CONSENSUS_PREFETCHING_QUEUE_CLOSING_BEFORE_SEEK_SCHEDULED_FMT =
      "ConsensusPrefetchingQueue %s ????????? seek(%s)";
  public static final String CONSENSUS_PREFETCHING_QUEUE_RUNTIME_UNAVAILABLE_FOR_SEEK_FMT =
      "ConsensusPrefetchingQueue %s ???? seek(%s)??? prefetch runtime ???";
  public static final String ERROR_PROGID_INVALID_OR_UNREGISTERED_HRESULT_FMT =
      "???ProgID ???????(HRESULT=0x%s)";
  public static final String ERROR_RUNNING_OPC_CLIENT_FMT =
      "?? opc client ???%s?%s";
  public static final String ERROR_GETTING_OPC_CLIENT_FMT =
      "?? opc client ???%s?%s";

  // ---------------------------------------------------------------------------
  // slice A1 ? datanode pipe (leftover literals)
  // ---------------------------------------------------------------------------
  public static final String MESSAGE_FAILED_TO_LOAD_SNAPSHOT_FROM_ARG_9391AA27 =
      "? {} ??????";
  public static final String MESSAGE_PIPE_ARG_ARG_HISTORICAL_TSFILE_SELECTION_SUMMARY_SELECTED_BY_PROGRESS_UNCOVERED_ARG_7B74E18D =
      "Pipe {}@{}??? TsFile ????????????? {}?????/?????? {}?"
          + "???/???? {}??? {}??? {}??????? {}?????? {}??? pipe ?? {}?"
          + "pin ?? {}";
  public static final String EXCEPTION_INVALID_ROW_SIZE_ARG_IN_TABLET_FORMAT_DESERIALIZATION_76405615 =
      "tablet ?????????? %s ???";
  public static final String EXCEPTION_INVALID_SCHEMA_SIZE_ARG_IN_TABLET_FORMAT_DESERIALIZATION_838C5359 =
      "tablet ??????? schema ?? %s ???";
  public static final String EXCEPTION_MISSING_COLUMN_CATEGORY_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_660BD963 =
      "?? tablet ?????????????";
  public static final String EXCEPTION_INVALID_COLUMN_CATEGORY_ARG_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_569FF178 =
      "?? tablet ?????????? %s ???";
  public static final String EXCEPTION_MISSING_TIMESTAMPS_IN_TABLET_FORMAT_DESERIALIZATION_WITH_NON_EMPTY_ROWS_7550129E =
      "tablet ?????????????????";
  public static final String EXCEPTION_MISSING_VALUES_IN_TABLET_FORMAT_DESERIALIZATION_WITH_NON_EMPTY_ROWS_1B9C08D9 =
      "tablet ???????????????";
  public static final String EXCEPTION_MISSING_ARG_FLAG_IN_TABLET_FORMAT_DESERIALIZATION_2F802C0D =
      "tablet ????????? %s ???";
  public static final String EXCEPTION_INVALID_ARG_FLAG_ARG_IN_TABLET_FORMAT_DESERIALIZATION_40FF35AA =
      "tablet ??????? %s ?? %s ???";
  public static final String EXCEPTION_INSUFFICIENT_BYTES_FOR_ARG_IN_TABLET_FORMAT_DESERIALIZATION_EXPECTED_ARG_REMAINING_ARG_3FE76C83 =
      "tablet ??????? %s ????????? %s??? %s?";
  public static final String EXCEPTION_INVALID_BITMAP_SIZE_ARG_IN_TABLET_FORMAT_DESERIALIZATION_832E7C9C =
      "tablet ??????? bitmap ?? %s ???";
  public static final String EXCEPTION_UNSUPPORTED_SCHEMA_PLAN_NODE_9A833E0B =
      "???? schema plan ?? ";
  public static final String EXCEPTION_CANNOT_BUILD_SCHEMA_BATCH_PLAN_NODE_FROM_EMPTY_BATCH_842D9E9B =
      "???? batch ?? schema batch plan ???";
  public static final String EXCEPTION_UNKNOWN_INSERTBASESTATEMENT_ARG_CONSTRUCTED_FROM_PIPETRANSFERTABLETBINARYREQ_20BF2833 =
      "? PipeTransferTabletBinaryReq ????? InsertBaseStatement %s?";
  public static final String EXCEPTION_INVALID_BINARY_REQUEST_BODY_LENGTH_ARG_REMAINING_BODY_LENGTH_ARG_5E21BBFC =
      "??????????? %s???????? %s?";
  public static final String EXCEPTION_FAILED_TO_DESERIALIZE_INSERT_NODE_ARG_ARG_IN_TABLET_BATCH_AT_BODY_POSITION_ARG_WITH_REMAINING_BODY_LENGTH_ARG_EC41A1DD =
      "? tablet batch ? body ?? %s ???? insert ?? %s/%s ?????????? %s?";
  public static final String EXCEPTION_FAILED_TO_DESERIALIZE_RAW_TABLET_ARG_ARG_IN_TABLET_BATCH_AT_BODY_POSITION_ARG_WITH_REMAINING_BODY_LENGTH_ARG_D36919BA =
      "? tablet batch ? body ?? %s ?????? tablet %s/%s ?????????? %s?";
  public static final String EXCEPTION_INSUFFICIENT_BYTES_TO_READ_ARG_IN_TABLET_BATCH_REMAINING_BODY_LENGTH_ARG_343C1B9A =
      "tablet batch ??? %s ?????????????? %s?";
  public static final String EXCEPTION_INVALID_NEGATIVE_ARG_ARG_IN_TABLET_BATCH_89A5F868 =
      "tablet batch ? %s %s ???????";
  public static final String EXCEPTION_FAILED_TO_DESERIALIZE_RAW_TABLET_REQUEST_AT_BODY_POSITION_ARG_WITH_REMAINING_BODY_LENGTH_ARG_45AC3692 =
      "? body ?? %s ?????? tablet ???????????? %s?";
  public static final String EXCEPTION_INCOMPLETE_SCHEMA_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_A23A1C30 =
      "?? tablet ??????? schema ????";
  public static final String EXCEPTION_COLUMN_COUNT_IS_INCONSISTENT_WITH_SCHEMA_COUNT_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_53BA037A =
      "?? tablet ?????????? schema ??????";
  public static final String EXCEPTION_INCOMPLETE_MEASUREMENT_SCHEMA_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_B8DB28A8 =
      "?? tablet ??????? measurement schema ????";
  public static final String EXCEPTION_INCOMPLETE_COLUMN_VALUES_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_269782B9 =
      "?? tablet ?????????????";
  public static final String EXCEPTION_INCOMPLETE_TIMESTAMPS_IN_CURRENT_TABLET_FORMAT_DESERIALIZATION_FE212461 =
      "?? tablet ??????????????";
  public static final String MESSAGE_RECEIVER_ARG_REQUIRES_A_RETRY_THROTTLE_REQUESTS_FOR_ARG_MS_STATUS_ARG_0B3B14F6 =
      "Receiver {} ?????????? {} ms????{}";
  public static final String EXCEPTION_RECEIVER_ARG_HAS_REQUIRED_RETRIES_FOR_MORE_THAN_ARG_MS_PAUSE_REGULAR_RETRIES_AND_PROBE_EVERY_ARG_MS_550475C2 =
      "Receiver %s ??????? %d ms??????????? %d ms ?????";
  public static final String MESSAGE_SUCCESSFULLY_TRANSFERRED_BATCHED_SCHEMA_EVENTS_BATCH_SIZE_ARG_CF2E881C =
      "??????? schema ???batch ?? {}?";
  public static final String EXCEPTION_AUTO_CREATE_TREE_DATABASE_FAILED_ARG_STATUS_CODE_ARG_C6175C27 =
      "???? tree database ???%s?????%s";
  public static final String EXCEPTION_ILLEGAL_TREE_DATABASE_ARG_C805A990 =
      "??? tree database %s?";
  public static final String EXCEPTION_FAILED_TO_GET_PARENT_DIR_OF_8CE21C1D =
      "????????";
  public static final String EXCEPTION_FAILED_TO_PREPARE_NEXT_TABLET_INSERTION_EVENT_70A57827 =
      "????? tablet insertion event ???";
  public static final String EXCEPTION_INVALID_ALIGNED_VALUE_CHUNK_INDEX_ARG_WHILE_THERE_ARE_ARG_TIME_CHUNKS_A7AE6C57 =
      "??? chunk ?? %d ?????? %d ? time chunk?";
  public static final String MESSAGE_FAILED_TO_ROLLBACK_CREATED_REALTIME_PIPE_ARG_STATUS_ARG_CE14334A =
      "?????? realtime pipe {} ??????{}";
  public static final String LOG_REPORTING_PIPE_META_ARG_ISCOMPLETED_ARG_REMAININGEVENTCOUNT_ARG_8F996DF3 =
      "???? pipe meta?%s?isCompleted?%s?remainingEventCount?%s";
  public static final String LOG_REPORTED_ARG_PIPE_METAS_12068FC6 =
      "??? %s ? pipe meta?";
  public static final String MESSAGE_TRANSFER_FILE_ARG_ERROR_RESULT_STATUS_ARG_E565D9FD =
      "???? %s ???????? %s?";

  public static final String EXCEPTION_LEGACY_PIPE_RECEIVER_REQUIRES_A_LOGGED_IN_SESSION_D96219BF =
      "Legacy pipe receiver ?????? session?";
  public static final String EXCEPTION_FAILED_TO_SET_UP_CONSENSUS_SUBSCRIPTION_FOR_TOPIC_ARG_IN_CONSUMER_GROUP_ARG_ARG_A7FA88F3 =
      "??? %s????? %s ?????????%s";
  public static final String EXCEPTION_TOPIC_METADATA_FOR_ARG_IS_UNAVAILABLE_DURING_CONSENSUS_SUBSCRIPTION_SETUP_A1949F20 =
      "???????? topic %s ???????";
  public static final String EXCEPTION_TOPIC_CONFIG_FOR_ARG_IS_UNAVAILABLE_DURING_CONSENSUS_SUBSCRIPTION_SETUP_B94404EE =
      "???????? topic %s ??????";
  public static final String LOG_FAILED_TO_RELEASE_TSFILE_PARSER_MEMORY_FOR_PIPE_ARG_CREATION_TIME_ARG_IN_DATAREGION_ARG_BECAUSE_NO_RESERVATION_EXISTS_BB8321C0 =
      "???? Pipe {}????? {}?? DataRegion {} ?? TsFile ?????????????????";
  public static final String LOG_PIPE_PROCESSOR_WORKER_ARG_HAS_BEEN_PROCESSING_THE_SAME_EVENT_FOR_ARG_MS_PIPE_ARG_DATAREGION_ARG_SUBTASK_ARG_EVENT_ARG_THREAD_STATE_ARG_STACK_ARG_63B40775 =
      "Pipe processor worker {} 已连续处理同一 event {} ms。Pipe：{}，DataRegion：{}，subtask：{}，event：{}，线程状态：{}。栈：{}";
  public static final String LOG_OPC_UA_SERVER_OPERATION_LIMITS_MAXNODESPERWRITE_ARG_MAXNODESPERNODEMANAGEMENT_ARG_5D2BCC90 =
      "OPC UA 服务器操作限制：maxNodesPerWrite={}，maxNodesPerNodeManagement={}";
  public static final String LOG_INTERRUPTED_WHILE_READING_OPC_UA_SERVER_OPERATION_LIMITS_USE_DEFAULTS_MAXNODESPERWRITE_ARG_MAXNODESPERNODEMANAGEMENT_ARG_357D46A4 =
      "读取 OPC UA 服务器操作限制时被中断，使用默认值：maxNodesPerWrite={}，maxNodesPerNodeManagement={}";
  public static final String LOG_FAILED_TO_READ_OPC_UA_SERVER_OPERATION_LIMITS_USE_DEFAULTS_MAXNODESPERWRITE_ARG_MAXNODESPERNODEMANAGEMENT_ARG_65460871 =
      "读取 OPC UA 服务器操作限制失败，使用默认值：maxNodesPerWrite={}，maxNodesPerNodeManagement={}";
}
