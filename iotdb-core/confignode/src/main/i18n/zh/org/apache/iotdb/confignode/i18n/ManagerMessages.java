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

package org.apache.iotdb.confignode.i18n;

public final class ManagerMessages {

  public static final String ACTIVATEDATAALLOTTABLE_ACTIVATE_SERIESPARTITIONSLOT =
      "[ActivateDataAllotTable] Activate SeriesPartitionSlot {} ";
  public static final String AFTER_THIS_SUCCESSFUL_SYNC_IF_PIPETASKINFO_IS_EMPTY_DURING_THIS =
      "After this successful sync, if PipeTaskInfo is empty during this sync and has not been modified afterwards, all subsequent syncs will be skipped";
  public static final String AFTER_THIS_SUCCESSFUL_SYNC_IF_SUBSCRIPTIONINFO_IS_EMPTY_DURING_THIS =
      "After this successful sync, if SubscriptionInfo is empty during this sync and has not been modified afterwards, all subsequent syncs will be skipped";
  public static final String ATTEMPT_TO_REPORT_PIPE_EXCEPTION_TO_A_NULL_PIPETASKMETA =
      "尝试向空的 PipeTaskMeta 上报 pipe 异常。";
  public static final String AUTH_RUN_AUTH_PLAN = "Auth: run auth plan: {}";
  public static final String CLUSTERID = "clusterID: {}";
  public static final String COLLECTING_PIPE_HEARTBEAT_FROM_DATA_NODES =
      "正在从 data nodes 收集 pipe 心跳 {}";
  public static final String CONNECTION_FROM_DATANODE_TO_DATANODE_IS_BROKEN =
      "Connection from DataNode {} to DataNode {} is broken";
  public static final String CONSENSUSGROUPSTATISTICS = "[ConsensusGroupStatistics]\t {}: {} -> {}";
  public static final String CONSENSUSGROUPSTATISTICS_CONSENSUSGROUPSTATISTICSMAP =
      "[ConsensusGroupStatistics] ConsensusGroupStatisticsMap: ";
  public static final String CONSENSUSMANAGER_GETLEADERPEER_BEEN_INTERRUPTED =
      "ConsensusManager getLeaderPeer been interrupted, ";
  public static final String CONSUMER_IN_CONSUMER_GROUP_FAILED_TO_SUBSCRIBE_TOPICS_RESULT_STATUS =
      "Consumer {} in consumer group {} failed to subscribe topics {}. Result status: {}.";
  public static final String CONSUMER_IN_CONSUMER_GROUP_FAILED_TO_UNSUBSCRIBE_TOPICS_RESULT_STATUS =
      "Consumer {} in consumer group {} failed to unsubscribe topics {}. Result status: {}.";
  public static final String CREATEPEERFORCONSENSUSGROUP = "createPeerForConsensusGroup {}...";
  public static final String CREATEREGIONGROUPS_STARTING_TO_CREATE_THE_FOLLOWING_REGIONGROUPS =
      "[CreateRegionGroups] Starting to create the following RegionGroups:";
  public static final String CREATE_DATAPARTITION_FAILED_BECAUSE =
      "Create DataPartition failed because: ";
  public static final String CREATE_SCHEMAPARTITION_FAILED_BECAUSE =
      "Create SchemaPartition failed because: ";
  public static final String DATABASE_DOESN_T_EXIST = "Database: {} doesn't exist";
  public static final String DATABASE_NOT_EXISTS_WHEN_SETUPPARTITIONBALANCER =
      "Database {} not exists when setupPartitionBalancer";
  public static final String DATABASE_NOT_EXISTS_WHEN_UPDATEDATAALLOTTABLE =
      "Database {} not exists when updateDataAllotTable";
  public static final String DATANODELOCATION_IS_NULL_DATANODEID =
      "DataNodeLocation is null, datanodeId {}";
  public static final String DATAREGIONGROUPEXTENSIONPOLICY_DOESN_T_EXIST =
      "DataRegionGroupExtensionPolicy %s doesn't exist.";
  public static final String DECREASE_REFERENCE_COUNT_FOR_SNAPSHOT_ERROR =
      "Decrease reference count for snapshot {} error.";
  public static final String DETECTED_HISTORICAL_PIPE_COMPLETION_REPORT_FROM_DATANODE =
      "检测到来自 DataNode {} 的历史 pipe 完成上报，pipe {}。remainingEventCount: {}, remainingTime: {}, completedDataNodes: {}";
  public static final String DETECTED_COMPLETION_OF_PIPE_STATIC_META_REMOVE_IT =
      "检测到 pipe {} 已完成，static meta: {}，将其移除。";
  public static final String ALL_DATANODES_REPORTED_HISTORICAL_PIPE_COMPLETED =
      "所有 DataNode 均已上报历史 pipe {} 完成。globalRemainingEventCount: {}, globalRemainingTime: {}, staticMeta: {}";
  public static final String DETECT_PIPERUNTIMECRITICALEXCEPTION_FROM_AGENT_STOP_PIPE =
      "检测到 agent 上报 PipeRuntimeCriticalException {}，停止 pipe {}。";
  public static final String DETECT_PIPERUNTIMESINKCRITICALEXCEPTION_FROM_AGENT_STOP_PIPE =
      "检测到 agent 上报 PipeRuntimeSinkCriticalException {}，停止 pipe {}。";
  public static final String ENABLE_SEPARATION_OF_POWERS_IS_NOT_SUPPORTED = "不支持启用权力分离";
  public static final String ENDEXECUTECQ_TIME_RANGE_IS_CURRENT_TIME_IS =
      "[EndExecuteCQ] {}, time range is [{}, {}), current time is {}";
  public static final String ERROR_HAPPENED_WHILE_SHUTTING_DOWN_PREVIOUS_CQ_SCHEDULE_THREAD_POOL =
      "Error happened while shutting down previous cq schedule thread pool.";
  public static final String ERROR_OCCURRED_DURING_CLOSING_PIPECONNECTOR =
      "Error occurred during closing PipeConnector.";
  public static final String ERROR_OCCURRED_DURING_CLOSING_PIPEEXTRACTOR =
      "Error occurred during closing PipeExtractor.";
  public static final String ERROR_OCCURRED_DURING_CLOSING_PIPEPROCESSOR =
      "Error occurred during closing PipeProcessor.";
  public static final String ERROR_WHEN_COUNTING_DATAREGIONGROUPS_IN_DATABASE =
      "Error when counting DataRegionGroups in Database: {}";
  public static final String ERROR_WHEN_COUNTING_SCHEMAREGIONGROUPS_IN_DATABASE =
      "Error when counting SchemaRegionGroups in Database: {}";
  public static final String EVENT_SERVICE_IS_STARTED_SUCCESSFULLY =
      "Event service is started successfully.";
  public static final String EVENT_SERVICE_IS_STOPPED_SUCCESSFULLY =
      "Event service is stopped successfully.";
  public static final String EXCEPTION_ENCOUNTERED_WHEN_TRIGGERING_SCHEMA_REGION_SNAPSHOT =
      "Exception encountered when triggering schema region snapshot.";
  public static final String EXECUTE_CQ_FAILED = "Execute CQ {} failed";
  public static final String EXECUTE_CQ_FAILED_TSSTATUS_IS = "Execute CQ {} failed, TSStatus is {}";
  public static final String EXPECTED_PIPE_HEARTBEAT_NODE_COUNT_IS_FALLBACK_TO_1 =
      "Expected pipe heartbeat node count is {}, fallback to 1.";
  public static final String EXTENDREGION_SUBMIT_ADDREGIONPEERPROCEDURE_SUCCESSFULLY =
      "[ExtendRegion] Submit AddRegionPeerProcedure successfully: {}";
  public static final String EXTEND_REGION_GROUP_FAILED = "Extend region group failed";
  public static final String FAILED_IN_THE_READ_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER =
      "执行共识层读写 API 失败：";
  public static final String FAILED_TO_ACQUIRE_LOCK_WHEN_PARSEHEARTBEAT_FROM_NODE_ID =
      "Failed to acquire lock when parseHeartbeat from node (id={}).";
  public static final String FAILED_TO_ACQUIRE_PIPE_LOCK_FOR_AUTO_RESTART_PIPE_TASK =
      "Failed to acquire pipe lock for auto restart pipe task.";
  public static final String FAILED_TO_ACQUIRE_PIPE_LOCK_FOR_HANDLING_SUCCESSFUL_RESTART =
      "Failed to acquire pipe lock for handling successful restart.";
  public static final String FAILED_TO_ALTER_PIPE_RESULT_STATUS =
      "Failed to alter pipe {}. Result status: {}.";
  public static final String FAILED_TO_CHECK_AND_REPAIR_CONSENSUS_PIPES =
      "Failed to check and repair consensus pipes";
  public static final String FAILED_TO_CHECK_PASSWORD_FOR_PIPE =
      "Failed to check password for pipe %s.";
  public static final String FAILED_TO_CLOSE_CONSUMER_IN_CONSUMER_GROUP_RESULT_STATUS =
      "Failed to close consumer {} in consumer group {}. Result status: {}.";
  public static final String FAILED_TO_CLOSE_EXTRACTOR_AFTER_FAILED_TO_INITIALIZE_EXTRACTOR =
      "初始化 extractor 失败后关闭 extractor 失败，忽略此异常。";
  public static final String FAILED_TO_CLOSE_SINK_AFTER_FAILED_TO_INITIALIZE_IT_IGNORE =
      "初始化 sink 失败后关闭 sink 失败，忽略此异常。";
  public static final String FAILED_TO_COLLECT_COMMITCREATETABLEPLAN =
      "Failed to collect CommitCreateTablePlan";
  public static final String FAILED_TO_COLLECT_PIPE_META_LIST_FROM_CONFIG_NODE_TASK =
      "Failed to collect pipe meta list from config node task agent";
  public static final String FAILED_TO_COLLECT_UNSETTEMPLATEPLAN =
      "Failed to collect UnsetTemplatePlan";
  public static final String FAILED_TO_COLLECT_USER_NAME_FOR_USER_ID =
      "Failed to collect user name for user id {}";
  public static final String FAILED_TO_CREATE_CONSUMER_IN_CONSUMER_GROUP_RESULT_STATUS =
      "Failed to create consumer {} in consumer group {}. Result status: {}.";
  public static final String FAILED_TO_CREATE_PEER_FOR_CONSENSUS_GROUP =
      "Failed to create peer for consensus group";
  public static final String FAILED_TO_CREATE_PIPE_RESULT_STATUS =
      "创建 pipe {} 失败。结果状态：{}。";
  public static final String FAILED_TO_CREATE_SUBTASK_FOR_PIPE_CREATION_TIME =
      "Failed to create subtask for pipe %s, creation time %d";
  public static final String FAILED_TO_CREATE_TOPIC_WITH_ATTRIBUTES_RESULT_STATUS =
      "Failed to create topic {} with attributes {}. Result status: {}.";
  public static final String FAILED_TO_ALTER_TOPIC_THE_TOPIC_IS_NOT_EXISTED =
      "修改 topic %s 失败，该 topic 不存在";
  public static final String FAILED_TO_ALTER_TOPIC_WITH_ATTRIBUTES_RESULT_STATUS =
      "修改 topic {} 失败，属性：{}。结果状态：{}。";
  public static final String OWNER_LEASE_DURATION_BELOW_MIN =
      "创建或修改 topic 失败，owner-lease-duration-ms %s 小于允许的最小值 %s ms。";
  public static final String FAILED_TO_DEEP_COPY_PIPEMETA = "深拷贝 pipeMeta 失败";
  public static final String FAILED_TO_DEREGISTER_PIPE_CONFIG_REGION_CONNECTOR =
      "Failed to deregister pipe config region connector metrics, PipeConfigNodeSubtask({}) does not exist";
  public static final String FAILED_TO_DEREGISTER_PIPE_CONFIG_REGION_EXTRACTOR =
      "Failed to deregister pipe config region extractor metrics, IoTDBConfigRegionExtractor({}) does not exist";
  public static final String FAILED_TO_DEREGISTER_PIPE_REMAINING_TIME_METRICS_REMAININGTIMEOPERATOR_DOES_NOT =
      "Failed to deregister pipe remaining time metrics, RemainingTimeOperator({}) does not exist";
  public static final String FAILED_TO_DEREGISTER_PIPE_TEMPORARY_META_METRICS_PIPETEMPORARYMETA_DOES_NOT =
      "Failed to deregister pipe temporary meta metrics, PipeTemporaryMeta({}) does not exist";
  public static final String FAILED_TO_DROP_PIPE_RESULT_STATUS =
      "删除 pipe {} 失败。结果状态：{}。";
  public static final String FAILED_TO_GET_ALL_PIPE_INFO = "Failed to get all pipe info.";
  public static final String FAILED_TO_GET_ALL_SUBSCRIPTION_INFO =
      "Failed to get all subscription info.";
  public static final String FAILED_TO_GET_ALL_TOPIC_INFO = "Failed to get all topic info.";
  public static final String FAILED_TO_HANDLE_PIPE_META_CHANGES = "处理 pipe 元数据变更失败";
  public static final String FAILED_TO_HANDLE_PIPE_META_CHANGE_RESULT_STATUS =
      "Failed to handle pipe meta change. Result status: {}.";
  public static final String FAILED_TO_LOAD_SNAPSHOT_FROM_BYTEBUFFER =
      "Failed to load snapshot from byteBuffer {}.";
  public static final String FAILED_TO_LOAD_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_A_NORMAL =
      "Failed to load snapshot,snapshot file [{}] is not a normal file.";
  public static final String FAILED_TO_MARK_PIPE_CONFIG_REGION_WRITE_PLAN_EVENT_PIPECONFIGNODESUBTASK =
      "Failed to mark pipe config region write plan event, PipeConfigNodeSubtask({}) does not exist";
  public static final String FAILED_TO_MARK_PIPE_REGION_COMMIT_REMAININGTIMEOPERATOR_DOES_NOT_EXIST =
      "Failed to mark pipe region commit, RemainingTimeOperator({}) does not exist";
  public static final String FAILED_TO_SHOW_SUBSCRIPTION_INFO = "Failed to show subscription info.";
  public static final String FAILED_TO_SHOW_TOPIC_INFO = "Failed to show topic info.";
  public static final String FAILED_TO_START_PIPE_RESULT_STATUS =
      "启动 pipe {} 失败。结果状态：{}。";
  public static final String FAILED_TO_STOP_PIPE_RESULT_STATUS =
      "停止 pipe {} 失败。结果状态：{}。";
  public static final String FAILED_TO_SUBMIT_ASYNC_CONSENSUS_PIPE_CREATION_FOR =
      "Failed to submit async consensus pipe creation for {}: {}";
  public static final String FAILED_TO_SUBMIT_ASYNC_CONSENSUS_PIPE_DROP_FOR =
      "Failed to submit async consensus pipe drop for {}: {}";
  public static final String FAILED_TO_SYNC_CONSUMER_GROUP_META_RESULT_STATUS =
      "Failed to sync consumer group meta. Result status: {}.";
  public static final String FAILED_TO_SYNC_PIPE_META_RESULT_STATUS =
      "同步 pipe 元数据失败。结果状态：{}。";
  public static final String FAILED_TO_SYNC_TEMPLATE_EXTENSION_INFO_TO_DATANODE =
      "Failed to sync template {} extension info to DataNode {}";
  public static final String FAILED_TO_SYNC_TOPIC_META_RESULT_STATUS =
      "Failed to sync topic meta. Result status: {}.";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_CONFIG_REGION_CONNECTOR_METRICS_CONNECTOR =
      "Failed to unbind from pipe config region connector metrics, connector map not empty";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_CONFIG_REGION_EXTRACTOR_METRICS_EXTRACTOR =
      "Failed to unbind from pipe config region extractor metrics, extractor map not empty";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_REMAINING_TIME_METRICS_REMAININGTIMEOPERATOR_MAP =
      "Failed to unbind from pipe remaining time metrics, RemainingTimeOperator map not empty";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_TEMPORARY_META_METRICS_PIPETEMPORARYMETA_MAP =
      "Failed to unbind from pipe temporary meta metrics, PipeTemporaryMeta map not empty";
  public static final String FAILED_TO_UPDATE_PIPE_PROCEDURE_TIMER_PIPEPROCEDURE_DOES_NOT_EXIST =
      "Failed to update pipe procedure timer, PipeProcedure({}) does not exist";
  public static final String FAILED_TO_UPDATE_THE_LAST_EXECUTION_TIME_OF_CQ_BECAUSE =
      "Failed to update the last execution time {} of CQ {}, because {}";
  public static final String FAIL_TO_GET_ALLUDFTABLE = "Fail to get AllUDFTable";
  public static final String FAIL_TO_GET_PIPEPLUGINTABLE = "Fail to get PipePluginTable";
  public static final String FAIL_TO_GET_TRIGGERTABLE = "Fail to get TriggerTable";
  public static final String FAIL_TO_GET_UDFTABLE = "Fail to get UDFTable";
  public static final String FAIL_TO_TRANSFER_BECAUSE_WILL_RETRY =
      "Fail to transfer because {}, will retry";
  public static final String FORCE_UPDATE_NODECACHE_STATUS_CURRENTNANOTIME =
      "Force update NodeCache: status={}, currentNanoTime={}";
  public static final String GETDATAPARTITION_INTERFACE_RECEIVE_PARTITIONSLOTSMAP_RETURN =
      "GetDataPartition interface receive PartitionSlotsMap: {}, return: {}";
  public static final String GETNODEPATHSPARTITION_RECEIVED_PARTIALPATH_LEVEL_PATHPATTERNTREE_RESP =
      "[GetNodePathsPartition]:{}Received PartialPath: {}, Level: {}, PathPatternTree: {}, Resp: {}";
  public static final String GET_OR_CREATE_DATA_PARTITION_RESP_LOG =
      "[GetOrCreateDataPartition]:{}Receive PartitionSlotsMap: {}, Return TDataPartitionTableResp: {}";
  public static final String GETORCREATESCHEMAPARTITION_RECEIVE_DATABASENAMESLOTMAP_RETURN_TSCHEMAPARTITIONTABLERESP =
      "[GetOrCreateSchemaPartition]:{}Receive databaseNameSlotMap: {}, Return TSchemaPartitionTableResp: {}";
  public static final String GETORCREATESCHEMAPARTITION_RECEIVE_PATHPATTERNTREE_RETURN_TSCHEMAPARTITIONTABLERESP =
      "[GetOrCreateSchemaPartition]:{}Receive PathPatternTree: {}, Return TSchemaPartitionTableResp: {}";
  public static final String GETSCHEMAPARTITION_RECEIVE_PATHS_RETURN =
      "GetSchemaPartition receive paths: {}, return: {}";
  public static final String GET_REGION_GROUP_ID_FAIL = "获取区域组 ID 失败";
  public static final String HEARTBEAT_SERVICE_IS_STARTED_SUCCESSFULLY =
      "Heartbeat service is started successfully.";
  public static final String HEARTBEAT_SERVICE_IS_STOPPED_SUCCESSFULLY =
      "Heartbeat service is stopped successfully.";
  public static final String INCORRECT_VERSION_OF = "Incorrect version of ";
  public static final String INIT_CONSENSUSMANAGER_SUCCESSFULLY_WHEN_RESTARTED =
      "Init ConsensusManager successfully when restarted";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_PIPETASKCOORDINATOR_LOCK_CURRENT_THREAD =
      "Interrupted while waiting for PipeTaskCoordinator lock, current thread: {}";
  public static final String INTERRUPT_WHEN_WAIT_FOR_CALCULATING_REGION_PRIORITY =
      "Interrupt when wait for calculating Region priority";
  public static final String INTERRUPT_WHEN_WAIT_FOR_LEADER_ELECTION =
      "Interrupt when wait for leader election";
  public static final String INVALID_EVENT_TYPE = "Invalid event type: ";
  public static final String IOTCONSENSUSV2_LEADER_CHANGED_FAILED_TO_FLUSH_OLD_LEADER_FOR_REGION =
      "[IoTConsensusV2 Leader Changed] Failed to flush old leader {} for region {}";
  public static final String IOTCONSENSUSV2_LEADER_CHANGED_SUCCESSFULLY_FLUSH_OLD_LEADER_FOR_REGION =
      "[IoTConsensusV2 Leader Changed] Successfully flush old leader {} for region {}";
  public static final String IOTDBCONFIGNODERECEIVER_DOES_NOT_SUPPORT_LOAD_FILE_V1 =
      "IoTDBConfigNodeReceiver does not support load file V1.";
  public static final String IOTDBCONFIGREGIONAIRGAPCONNECTOR_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBConfigRegionAirGapConnector can't transfer TabletInsertionEvent.";
  public static final String IOTDBCONFIGREGIONAIRGAPCONNECTOR_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBConfigRegionAirGapConnector can't transfer TsFileInsertionEvent.";
  public static final String IOTDBCONFIGREGIONAIRGAPCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBConfigRegionAirGapConnector does not support transferring generic event: {}.";
  public static final String IOTDBCONFIGREGIONSINK_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBConfigRegionSink can't transfer TabletInsertionEvent.";
  public static final String IOTDBCONFIGREGIONSINK_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBConfigRegionSink can't transfer TsFileInsertionEvent.";
  public static final String IOTDBCONFIGREGIONSINK_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBConfigRegionSink does not support transferring generic event: {}.";
  public static final String IOTDBCONFIGREGIONSOURCE_DOES_NOT_TRANSFERRING_EVENTS_UNDER_SIMPLE_CONSENSUS =
      "IoTDBConfigRegionSource does not transferring events under simple consensus";
  public static final String LEADERBALANCER_FAILED_TO_CHANGE_THE_LEADER_OF_REGION_TO_DATANODE =
      "[LeaderBalancer] Failed to change the leader of Region: {} to DataNode: {}";
  public static final String LEADERBALANCER_REGION_NOT_IN_DATABASEREGIONGROUPMAP =
      "[LeaderBalancer] Region: {} not in databaseRegionGroupMap";
  public static final String LEADERBALANCER_REGION_NOT_IN_REGIONLEADERMAP =
      "[LeaderBalancer] Region: {} not in regionLeaderMap";
  public static final String LEADERBALANCER_REGION_NOT_IN_REGIONLOCATIONMAP =
      "[LeaderBalancer] Region: {} not in regionLocationMap";
  public static final String LEADERBALANCER_REGION_NOT_IN_REGIONSTATISTICSMAP =
      "[LeaderBalancer] Region: {} not in regionStatisticsMap";
  public static final String LEADERBALANCER_THE_FOLLOWING_REGIONGROUPS_LEADER_CANNOT_BE =
      "[LeaderBalancer] The following RegionGroups' leader cannot be selected because their corresponding caches are incomplete: {}";
  public static final String LEADERBALANCER_TRY_TO_CHANGE_THE_LEADER_OF_REGION_TO_DATANODE =
      "[LeaderBalancer] Try to change the leader of Region: {} to DataNode: {} ";
  public static final String LOADSTATISTICS_SERVICE_IS_STARTED_SUCCESSFULLY =
      "LoadStatistics service is started successfully.";
  public static final String LOADSTATISTICS_SERVICE_IS_STOPPED_SUCCESSFULLY =
      "LoadStatistics service is stopped successfully.";
  public static final String MIGRATEREGION_SUBMIT_REGIONMIGRATEPROCEDURE_SUCCESSFULLY_REGION_ORIGIN_DATANODE =
      "[MigrateRegion] Submit RegionMigrateProcedure successfully, Region: {}, Origin DataNode: {}, Dest DataNode: {}, Add Coordinator: {}, Remove Coordinator: {}";
  public static final String MISMATCHED_CRC32_CODE_WHEN_DESERIALIZING_SERVICE_INFO =
      "Mismatched CRC32 code when deserializing service info.";
  public static final String NETWORK_ERROR_WHEN_SEAL_CONFIG_REGION_SNAPSHOT_BECAUSE =
      "Network error when seal config region snapshot %s, because %s.";
  public static final String NETWORK_ERROR_WHEN_TRANSFER_CONFIG_REGION_WRITE_PLAN_BECAUSE =
      "Network error when transfer config region write plan %s, because %s.";
  public static final String NETWORK_ERROR_WHEN_TRANSFER_EVENT_BECAUSE =
      "Network error when transfer event %s, because %s.";
  public static final String NODEMANAGER_START_TO_REMOVE_DATANODE =
      "NodeManager start to remove DataNode {}";
  public static final String NODEMANAGER_SUBMIT_REMOVEAINODEPLAN_FINISHED =
      "NodeManager submit RemoveAINodePlan finished, {}";
  public static final String NODEMANAGER_SUBMIT_REMOVEDATANODEPLAN_FINISHED_REMOVEDATANODEPLAN =
      "NodeManager submit RemoveDataNodePlan finished, removeDataNodePlan: {}";
  public static final String NODESTATISTICS = "[NodeStatistics]\t {}: {} -> {}";
  public static final String NODESTATISTICS_NODESTATISTICSMAP =
      "[NodeStatistics] NodeStatisticsMap: ";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN = "没有传输计划的权限：";
  public static final String NOT_IMPLEMENT_YET = "尚未实现";
  public static final String NO_CORRESPONDING_PIPE_IS_RUNNING_IN_THE_REPORTED_DATAREGION_RUNTIMEMETAFROMAGENT =
      "No corresponding Pipe is running in the reported DataRegion. runtimeMetaFromAgent is null, runtimeMetaFromCoordinator: {}";
  public static final String PARTITIONBALANCER_THE_SERIESSLOT_IN_TIMESLOT_WILL_BE =
      "[PartitionBalancer] The SeriesSlot: {} in TimeSlot: {} will be allocated to DataRegionGroup: {}, because the original target: {} is currently unavailable.";
  public static final String PHIACCRUALDETECTOR_TOPOLOGY_IS_BROKEN_HEARTBEAT_HISTORY_MS =
      "[PhiAccrualDetector] Topology {} is broken, heartbeat history (ms): {}";
  public static final String PHIACCRUALDETECTOR_TOPOLOGY_IS_RECOVERED_HEARTBEAT_HISTORY_MS =
      "[PhiAccrualDetector] Topology {} is recovered, heartbeat history (ms): {}";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_WAS_FAILED_TO_SUBMIT =
      "PipeHandleLeaderChangeProcedure was failed to submit.";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_WAS_SUBMITTED_PROCEDUREID =
      "PipeHandleLeaderChangeProcedure was submitted, procedureId: {}.";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_WAS_FAILED_TO_SUBMIT =
      "PipeHandleMetaChangeProcedure was failed to submit.";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_WAS_SUBMITTED_PROCEDUREID =
      "PipeHandleMetaChangeProcedure was submitted, procedureId: {}.";
  public static final String PIPEHEARTBEAT_IS_STARTED_SUCCESSFULLY =
      "PipeHeartbeat is started successfully.";
  public static final String PIPEHEARTBEAT_IS_STOPPED_SUCCESSFULLY =
      "PipeHeartbeat is stopped successfully.";
  public static final String PIPEMETASYNCER_IS_STARTED_SUCCESSFULLY =
      "PipeMetaSyncer is started successfully.";
  public static final String PIPEMETASYNCER_IS_STOPPED_SUCCESSFULLY =
      "PipeMetaSyncer is stopped successfully.";
  public static final String PIPERUNTIMECONFIGNODEAGENT_STARTED =
      "PipeRuntimeConfigNodeAgent started";
  public static final String PIPERUNTIMECONFIGNODEAGENT_STOPPED =
      "PipeRuntimeConfigNodeAgent stopped";
  public static final String PIPERUNTIMECOORDINATOR_MEETS_ERROR_IN_UPDATING_PIPEMETAKEEPER =
      "PipeRuntimeCoordinator meets error in updating pipeMetaKeeper, ";
  public static final String PIPETASKCOORDINATORLOCK_IS_HELD_BY_ANOTHER_THREAD_SKIP_THIS_ROUND_OF =
      "PipeTaskCoordinatorLock is held by another thread, skip this round of heartbeat to avoid procedure and rpc accumulation as much as possible";
  public static final String PIPETASKCOORDINATORLOCK_IS_HELD_BY_ANOTHER_THREAD_SKIP_THIS_ROUND_OF_2 =
      "PipeTaskCoordinatorLock is held by another thread, skip this round of sync to avoid procedure and rpc accumulation as much as possible";
  public static final String PIPETASKCOORDINATOR_LOCK_ACQUIRED_BY_THREAD =
      "PipeTaskCoordinator lock acquired by thread {}";
  public static final String PIPETASKCOORDINATOR_LOCK_FAILED_TO_ACQUIRE_BY_THREAD_BECAUSE_OF_TIMEOUT =
      "PipeTaskCoordinator lock failed to acquire by thread {} because of timeout";
  public static final String PIPETASKCOORDINATOR_LOCK_RELEASED_BY_THREAD =
      "PipeTaskCoordinator lock released by thread {}";
  public static final String PIPETASKCOORDINATOR_LOCK_WAITING_FOR_THREAD =
      "PipeTaskCoordinator lock waiting for thread {}";
  public static final String PIPE_SNAPSHOT_DIR_FOUND_DELETING_IT =
      "Pipe snapshot dir found, deleting it: {},";
  public static final String PROCEDUREMANAGER_IS_STARTED_SUCCESSFULLY = "ProcedureManager 已成功启动。";
  public static final String PROCEDUREMANAGER_IS_STOPPED_SUCCESSFULLY = "ProcedureManager 已成功停止。";
  public static final String PROCEDURE_DETAILS_ARE = "[{}] procedure details are {}";
  public static final String REBALANCEDATAALLOTTABLE_DATABASE =
      "[ReBalanceDataAllotTable] Database: {}, ";
  public static final String RECEIVED_PIPE_HEARTBEAT_REQUEST_FROM_CONFIG_COORDINATOR =
      "Received pipe heartbeat request {} from config coordinator.";
  public static final String RECEIVER_ID = "Receiver id = {}: {}";
  public static final String RECEIVER_ID_EXCEPTION_ENCOUNTERED_WHILE_EXECUTING_PLAN =
      "Receiver id = {}: Exception encountered while executing plan {}: ";
  public static final String RECEIVER_ID_FAILURE_STATUS_ENCOUNTERED_WHILE_EXECUTING_PLAN =
      "Receiver id = {}: Failure status encountered while executing plan {}: {}";
  public static final String RECEIVER_ID_PERMISSION_CHECK_FAILED_WHILE_EXECUTING_PLAN =
      "Receiver id = {}: Permission check failed while executing plan {}: {}";
  public static final String RECEIVER_ID_UNSUPPORTED_PIPEREQUESTTYPE_ON_CONFIGNODE_RESPONSE_STATUS =
      "Receiver id = {}: Unsupported PipeRequestType on ConfigNode, response status = {}.";
  public static final String RECONSTRUCTREGION_SUBMIT_RECONSTRUCTREGIONPROCEDURE_SUCCESSFULLY =
      "[ReconstructRegion] Submit ReconstructRegionProcedure successfully, {}";
  public static final String REGIONCLEANER_IS_STARTED_SUCCESSFULLY =
      "RegionCleaner is started successfully.";
  public static final String REGIONCLEANER_IS_STOPPED_SUCCESSFULLY =
      "RegionCleaner is stopped successfully.";
  public static final String REGIONELECTION_THE_LEADER_OF_REGIONGROUPS_IS_ELECTED =
      "[RegionElection] The leader of RegionGroups: {} is elected.";
  public static final String REGIONELECTION_THE_LEADER_OF_REGIONGROUPS_IS_NOT_DETERMINED_AFTER_10 =
      "[RegionElection] The leader of RegionGroups: {} is not determined after 10 heartbeat interval. Some function might fail.";
  public static final String REGIONELECTION_WAIT_FOR_LEADER_ELECTION_OF_REGIONGROUPS =
      "[RegionElection] Wait for leader election of RegionGroups: {}";
  public static final String REGIONGROUPSTATISTICS_REGIONGROUP =
      "[RegionGroupStatistics]\t RegionGroup {}: {} -> {}";
  public static final String REGIONGROUPSTATISTICS_REGIONGROUPSTATISTICSMAP =
      "[RegionGroupStatistics] RegionGroupStatisticsMap: ";
  public static final String REGIONGROUPSTATISTICS_REGION_IN_DATANODE =
      "[RegionGroupStatistics]\t Region in DataNode {}: {} -> {}";
  public static final String REGIONGROUPSTATISTICS_REGION_IN_DATANODE_NULL =
      "[RegionGroupStatistics]\t Region in DataNode {}: null -> {}";
  public static final String REGIONGROUPSTATISTICS_REGION_IN_DATANODE_NULL_2 =
      "[RegionGroupStatistics]\t Region in DataNode {}: {} -> null";
  public static final String REGIONGROUPSTATUS_DOESN_T_EXIST =
      "RegionGroupStatus %s doesn't exist.";
  public static final String REGIONPRIORITY = "[RegionPriority]\t {}: {}->{}";
  public static final String REGIONPRIORITY_REGIONPRIORITYMAP =
      "[RegionPriority] RegionPriorityMap: ";
  public static final String REGIONPRIORITY_THE_ROUTING_PRIORITY_OF_REGIONGROUPS_IS_CALCULATED =
      "[RegionPriority] The routing priority of RegionGroups: {} is calculated.";
  public static final String REGIONPRIORITY_THE_ROUTING_PRIORITY_OF_REGIONGROUPS_IS_NOT_DETERMINED_AFTER =
      "[RegionPriority] The routing priority of RegionGroups: {} is not determined after 10 heartbeat interval. Some function might fail.";
  public static final String REGIONPRIORITY_WAIT_FOR_REGION_PRIORITY_UPDATE_OF_REGIONGROUPS =
      "[RegionPriority] Wait for Region priority update of RegionGroups: {}";
  public static final String REGION_ID = "Region id ";
  public static final String REMOVEREGIONPEER_SUBMIT_REMOVEREGIONPEERPROCEDURE_SUCCESSFULLY =
      "[RemoveRegionPeer] Submit RemoveRegionPeerProcedure successfully: {}";
  public static final String REMOVE_REGION_TARGET_DATANODE_NOT_FOUND_WILL_SIMPLY_CLEAN_UP =
      "Remove region: Target DataNode {} not found, will simply clean up the partition table of region {} and do nothing else.";
  public static final String REPORT_PIPERUNTIMEEXCEPTION_TO_LOCAL_PIPETASKMETA_EXCEPTION_MESSAGE =
      "Report PipeRuntimeException to local PipeTaskMeta({}), exception message: {}";
  public static final String RETRYFAILMISSIONS_SERVICE_IS_STARTED_SUCCESSFULLY =
      "RetryFailMissions service is started successfully.";
  public static final String RETRYFAILMISSIONS_SERVICE_IS_STOPPED_SUCCESSFULLY =
      "RetryFailMissions service is stopped successfully.";
  public static final String SERIALIZATION_FAILED_FOR_THE_ALTER_ENCODING_TIME_SERIES_PLAN_IN =
      "Serialization failed for the alter encoding time series plan in pipe transmission, skip transfer";
  public static final String SERIALIZATION_FAILED_FOR_THE_DELETE_LOGICAL_VIEW_PLAN_IN_PIPE =
      "Serialization failed for the delete logical view plan in pipe transmission, skip transfer";
  public static final String SERIALIZATION_FAILED_FOR_THE_DELETE_TIME_SERIES_PLAN_IN_PIPE =
      "Serialization failed for the delete time series plan in pipe transmission, skip transfer";
  public static final String SOMETHING_WRONG_HAPPENED_WHILE_CALLING_CONSENSUS_LAYER_S_CREATELOCALPEER_API =
      "Something wrong happened while calling consensus layer's createLocalPeer API.";
  public static final String SOME_PIPES_NEED_RESTARTING_WILL_RESTART_THEM_AFTER_THIS_SYNC =
      "Some pipes need restarting, will restart them after this sync";
  public static final String STARTEXECUTECQ_EXECUTE_CQ_ON_DATANODE_TIME_RANGE_IS_CURRENT_TIME =
      "[StartExecuteCQ] execute CQ {} on DataNode[{}], time range is [{}, {}), current time is {}";
  public static final String START_TO_ACTIVATE_UDF_IN_UDF_TABLE_ON_CONFIG_NODES =
      "Start to activate UDF [{}] in UDF_Table on Config Nodes";
  public static final String START_TO_ADD_UDF_IN_UDF_TABLE_ON_CONFIG_NODES =
      "Start to add UDF [{}] in UDF_Table on Config Nodes";
  public static final String START_TO_CREATE_REGION_ON_DATANODE =
      "Start to create Region: {} on DataNode: {}";
  public static final String START_TO_CREATE_UDF_ON_DATA_NODES_NEEDTOSAVEJAR =
      "Start to create UDF [{}] on Data Nodes, needToSaveJar[{}]";
  public static final String START_TRANSFER_OF = "Start transfer of {}";
  public static final String STOP_SUBMITTING_CQ_BECAUSE = "Stop submitting CQ {} because {}";
  public static final String STOP_SUBMITTING_CQ_BECAUSE_CURRENT_NODE_IS_NOT_LEADER_OR =
      "Stop submitting CQ {} because current node is not leader or current scheduled thread pool is shut down.";
  public static final String SUBMITTED_ASYNC_CONSENSUS_PIPE_CREATION =
      "Submitted async consensus pipe creation: {}";
  public static final String SUBMITTED_ASYNC_CONSENSUS_PIPE_DROP =
      "Submitted async consensus pipe drop: {}";
  public static final String SUBMIT_REMOVEAINODEPROCEDURE_SUCCESSFULLY =
      "Submit RemoveAINodeProcedure successfully, {}";
  public static final String SUBMIT_REMOVECONFIGNODEPROCEDURE_SUCCESSFULLY =
      "Submit RemoveConfigNodeProcedure successfully: {}";
  public static final String SUBMIT_REMOVEDATANODESPROCEDURE_SUCCESSFULLY =
      "Submit RemoveDataNodesProcedure successfully, {}";
  public static final String SUBSCRIPTIONCOORDINATORLOCK_IS_HELD_BY_ANOTHER_THREAD_SKIP_THIS_ROUND_OF =
      "SubscriptionCoordinatorLock is held by another thread, skip this round of sync to avoid procedure and rpc accumulation as much as possible";
  public static final String SUBSCRIPTIONMETASYNCER_IS_STARTED_SUCCESSFULLY =
      "SubscriptionMetaSyncer is started successfully.";
  public static final String SUBSCRIPTIONMETASYNCER_IS_STOPPED_SUCCESSFULLY =
      "SubscriptionMetaSyncer is stopped successfully.";
  public static final String SUCCESSFULLY_TRANSFERRED_CONFIG_EVENT =
      "Successfully transferred config event {}.";
  public static final String SUCCESSFULLY_TRANSFERRED_CONFIG_REGION_SNAPSHOT =
      "Successfully transferred config region snapshot {}.";
  public static final String THERE_IS_NO_RUNNING_DATANODE_TO_EXECUTE_CQ =
      "There is no RUNNING DataNode to execute CQ {}";
  public static final String THE_CONFIGNODE_WILL_BE_SHUTDOWN_SOON_MARK_IT_AS_UNKNOWN =
      "The ConfigNode-{} will be shutdown soon, mark it as Unknown";
  public static final String THE_CONFIG_REGION_AIR_GAP_CONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING =
      "The config region air gap connector does not support transferring single file piece bytes.";
  public static final String THE_CONFIG_REGION_SINK_DOES_NOT_SUPPORT_TRANSFERRING_SINGLE_FILE =
      "The config region sink does not support transferring single file piece req.";
  public static final String THE_CONFIG_REGION_SNAPSHOTS_CANNOT_BE_PARSED =
      "The config region snapshots %s cannot be parsed.";
  public static final String THE_DATABASE_DOESN_T_EXIST_MAYBE_IT_HAS_BEEN_PRE =
      "The Database: {} doesn't exist. Maybe it has been pre-deleted.";
  public static final String THE_DATANODE_WILL_BE_SHUTDOWN_SOON_MARK_IT_AS_UNKNOWN =
      "The DataNode-{} will be shutdown soon, mark it as Unknown";
  public static final String THE_REMOVENODEREPLICASELECT_METHOD_OF_GREEDYREGIONGROUPALLOCATOR_IS_YET =
      "The removeNodeReplicaSelect method of GreedyRegionGroupAllocator is yet to be implemented.";
  public static final String THE_REMOVENODEREPLICASELECT_METHOD_OF_PARTITEGRAPHPLACEMENTREGIONGROUPALLOCATOR =
      "The removeNodeReplicaSelect method of PartiteGraphPlacementRegionGroupAllocator is yet to be implemented.";
  public static final String THE_REMOVE_DATANODE_REQUEST_CHECK_FAILED_REQ_CHECK_RESULT =
      "The remove DataNode request check failed. req: {}, check result: {}";
  public static final String TOPOLOGY_ASYMMETRIC_NETWORK_PARTITION_FROM_TO =
      "[Topology] Asymmetric network partition from {} to {}";
  public static final String TOPOLOGY_CLUSTER_TOPOLOGY_CHANGED_LATEST =
      "[Topology] Cluster topology changed, latest: {}";
  public static final String TOPOLOGY_PROBING_HAS_STARTED_SUCCESSFULLY =
      "Topology Probing has started successfully";
  public static final String TOPOLOGY_PROBING_HAS_STOPPED_SUCCESSFULLY =
      "Topology Probing has stopped successfully";
  public static final String TOPOLOGY_TOPOLOGY_OF_DATANODE_IS_NOW_TO_DATANODE =
      "[Topology] Topology of DataNode {} is now {} to DataNode {}";
  public static final String UNABLE_TO_PARSE_PATH_WHEN_CHECKING_READ_PRIVILEGE_PATH =
      "Unable to parse path when checking READ privilege, path: {}";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_CREATING_SERVICE_ON_DATANODE =
      "Unexpected error happened while creating Service {} on DataNode {}: ";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_DROPPING_CQ =
      "Unexpected error happened while dropping cq {}: ";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_DROPPING_SERVICE_ON_DATANODE =
      "Unexpected error happened while dropping Service {} on DataNode {}: ";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_FETCHING_CQ_LIST =
      "Unexpected error happened while fetching cq list: ";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_GETTING_USER_DEFINED_SERVICE =
      "Unexpected error happened while getting user-defined Service: ";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_SHOWING_CQ =
      "Unexpected error happened while showing cq: ";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_SHOWING_SERVICE =
      "Unexpected error happened while showing Service: ";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_STARTING_SERVICE_ON_DATANODE =
      "Unexpected error happened while starting Service {} on DataNode {}: ";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_STOPPING_SERVICE_ON_DATANODE =
      "Unexpected error happened while stopping Service {} on DataNode {}: ";
  public static final String UNEXPECTED_INTERRUPTION_DURING_RETRY_CREATING_PEER_FOR_CONSENSUS_GROUP =
      "Unexpected interruption during retry creating peer for consensus group";
  public static final String UNEXPECTED_INTERRUPTION_DURING_RETRY_GETTING_LATEST_REGION_ROUTE_MAP =
      "Unexpected interruption during retry getting latest region route map";
  public static final String UNEXPECTED_INTERRUPTION_DURING_WAITING_FOR_CONFIGNODE_LEADER_READY =
      "Unexpected interruption during waiting for configNode leader ready.";
  public static final String UNEXPECTED_INTERRUPTION_DURING_WAITING_FOR_GET_CLUSTER_ID =
      "Unexpected interruption during waiting for get cluster id.";
  public static final String UNEXPECTED_NON_CREATE_REGION_MAINTAIN_TASK_SKIPPED =
      "Unexpected non-create task in the RegionMaintainer queue; skipping it (the queue only recreates region replicas now, and region deletion is handled by RemoveRegionGroupProcedure).";
  public static final String UNEXPECTED_NULL_PROCEDURE_PARAMETERS_FOR_WAITINGPROCEDUREFINISHED =
      "Unexpected null procedure parameters for waitingProcedureFinished";
  public static final String UNKNOWN_DATAPARTITION_ALLOCATION_STRATEGY_USING_INHERIT_STRATEGY_BY_DEFAULT =
      "Unknown DataPartition allocation strategy {}, using INHERIT strategy by default.";
  public static final String UNKNOWN_TIMEOUTPOLICY = "Unknown TimeoutPolicy: ";
  public static final String UN_PARSE_ABLE_PATH_NAME_ENCOUNTERED_DURING_TEMPLATE_PRIVILEGE_TRIMMING =
      "Un-parse-able path name encountered during template privilege trimming, please check";
  public static final String UPGRADE_CONFIGNODE_CONSENSUS_WAL_DIR_FOR_SIMPLECONSENSUS_FROM_VERSION_1 =
      "upgrade ConfigNode consensus wal dir for SimpleConsensus from version/1.0 to version/1.1 failed, ";
  public static final String WRITE_PARTITION_ALLOCATION_RESULT_FAILED_BECAUSE =
      "Write partition allocation result failed because: {}";

  public static final String CANNOT_SPECIFY_VIEW_PATTERN_TO_MATCH_MORE_THAN_ONE_TREE_DATABASE =
      "不能指定匹配多个树模型数据库的视图模式。";

    public static final String CONFIGNODE_IS_REMOVING = "ConfigNode 正在移除中";
  public static final String REPORTED_PIPE_METAS = "已上报 {} 个 pipe 元数据。";
  public static final String CLUSTERID_HAS_NOT_GENERATED = "clusterId 尚未生成";
  public static final String MIGRATE_THE_SERVICE_ON_THE_REMOVED_DATANODES_FAILED = "在已移除的 DataNode 上迁移服务失败";
  public static final String SERVER_ACCEPTED_THE_REQUEST = "服务器已接受请求";
  public static final String SERVER_REJECTED_THE_REQUEST_MAYBE_REQUESTS_ARE_TOO_MANY = "服务器拒绝了请求，可能请求过多";
  public static final String THERE_IS_ALREADY_ONE_AINODE_IN_THE_CLUSTER = "集群中已存在一个 AINode。";
  public static final String REMOVE_AINODE_FAILED_BECAUSE_THERE_IS_NO_AINODE_IN_THE = "移除 AINode 失败，集群中没有 AINode。";
  public static final String REMOVE_CONFIGNODE_FAILED_DUE_TO_THREAD_INTERRUPTION = "移除 ConfigNode 失败，线程被中断。";
  public static final String REMOVE_CONFIGNODE_FAILED_BECAUSE_THE_CONFIGNODE_NOT_IN_CURRENT_CLUSTER = "移除 ConfigNode 失败，该 ConfigNode 不在当前集群中。";
  public static final String SUCCESSFULLY_REMOVE_CONFIGNODE = "成功移除 ConfigNode。";
  public static final String REMOVE_CONFIGNODE_FAILED_BECAUSE_TRANSFER_CONFIGNODE_LEADER_FAILED = "移除 ConfigNode 失败，转移 ConfigNode leader 失败。";

  private ManagerMessages() {}
}
