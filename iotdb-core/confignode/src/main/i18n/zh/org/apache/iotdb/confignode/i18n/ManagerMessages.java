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
      "[ActivateDataAllotTable] 激活 SeriesPartitionSlot {} ";
  public static final String AFTER_THIS_SUCCESSFUL_SYNC_IF_PIPETASKINFO_IS_EMPTY_DURING_THIS =
      "本次同步成功后，若本次同步期间 PipeTaskInfo 为空且之后未被修改，将跳过后续所有同步";
  public static final String AFTER_THIS_SUCCESSFUL_SYNC_IF_SUBSCRIPTIONINFO_IS_EMPTY_DURING_THIS =
      "本次同步成功后，若本次同步期间 SubscriptionInfo 为空且之后未被修改，将跳过后续所有同步";
  public static final String ATTEMPT_TO_REPORT_PIPE_EXCEPTION_TO_A_NULL_PIPETASKMETA =
      "尝试向空的 PipeTaskMeta 上报 pipe 异常。";
  public static final String AUTH_RUN_AUTH_PLAN = "Auth：执行权限计划：{}";
  public static final String CLUSTERID = "clusterID: {}";
  public static final String COLLECTING_PIPE_HEARTBEAT_FROM_DATA_NODES =
      "正在从 data nodes 收集 pipe 心跳 {}";
  public static final String CONNECTION_FROM_DATANODE_TO_DATANODE_IS_BROKEN =
      "DataNode {} 到 DataNode {} 的连接已断开";
  public static final String CONSENSUSGROUPSTATISTICS = "[ConsensusGroupStatistics]\t {}: {} -> {}";
  public static final String CONSENSUSGROUPSTATISTICS_CONSENSUSGROUPSTATISTICSMAP =
      "[ConsensusGroupStatistics] ConsensusGroupStatisticsMap: ";
  public static final String CONSENSUSMANAGER_GETLEADERPEER_BEEN_INTERRUPTED =
      "ConsensusManager getLeaderPeer 被中断，";
  public static final String CONSUMER_IN_CONSUMER_GROUP_FAILED_TO_SUBSCRIBE_TOPICS_RESULT_STATUS =
      "consumer group {} 中的 consumer {} 订阅 topic {} 失败。结果状态：{}。";
  public static final String CONSUMER_IN_CONSUMER_GROUP_FAILED_TO_UNSUBSCRIBE_TOPICS_RESULT_STATUS =
      "consumer group {} 中的 consumer {} 取消订阅 topic {} 失败。结果状态：{}。";
  public static final String CREATEPEERFORCONSENSUSGROUP = "createPeerForConsensusGroup {}...";
  public static final String CREATEREGIONGROUPS_STARTING_TO_CREATE_THE_FOLLOWING_REGIONGROUPS =
      "[CreateRegionGroups] 开始创建以下 RegionGroup：";
  public static final String CREATE_DATAPARTITION_FAILED_BECAUSE =
      "创建 DataPartition 失败，原因：";
  public static final String CREATE_SCHEMAPARTITION_FAILED_BECAUSE =
      "创建 SchemaPartition 失败，原因：";
  public static final String DATABASE_DOESN_T_EXIST = "Database: {} 不存在";
  public static final String DATABASE_NOT_EXISTS_WHEN_SETUPPARTITIONBALANCER =
      "setupPartitionBalancer 时数据库 {} 不存在";
  public static final String DATABASE_NOT_EXISTS_WHEN_UPDATEDATAALLOTTABLE =
      "updateDataAllotTable 时数据库 {} 不存在";
  public static final String DATANODELOCATION_IS_NULL_DATANODEID =
      "DataNodeLocation 为空，datanodeId {}";
  public static final String DATAREGIONGROUPEXTENSIONPOLICY_DOESN_T_EXIST =
      "DataRegionGroupExtensionPolicy %s 不存在。";
  public static final String DECREASE_REFERENCE_COUNT_FOR_SNAPSHOT_ERROR =
      "减少快照 {} 的引用计数失败。";
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
  public static final String ENABLE_SEPARATION_OF_POWERS_IS_NOT_SUPPORTED = "不支持启用权限分离";
  public static final String ENDEXECUTECQ_TIME_RANGE_IS_CURRENT_TIME_IS =
      "[EndExecuteCQ] {}，时间范围为 [{}, {})，当前时间为 {}";
  public static final String ERROR_HAPPENED_WHILE_SHUTTING_DOWN_PREVIOUS_CQ_SCHEDULE_THREAD_POOL =
      "关闭上一个 CQ 调度线程池时出错。";
  public static final String ERROR_OCCURRED_DURING_CLOSING_PIPECONNECTOR =
      "关闭 PipeConnector 时出错。";
  public static final String ERROR_OCCURRED_DURING_CLOSING_PIPEEXTRACTOR =
      "关闭 PipeExtractor 时出错。";
  public static final String ERROR_OCCURRED_DURING_CLOSING_PIPEPROCESSOR =
      "关闭 PipeProcessor 时出错。";
  public static final String ERROR_WHEN_COUNTING_DATAREGIONGROUPS_IN_DATABASE =
      "统计 Database: {} 中的 DataRegionGroup 数量时出错";
  public static final String ERROR_WHEN_COUNTING_SCHEMAREGIONGROUPS_IN_DATABASE =
      "统计 Database: {} 中的 SchemaRegionGroup 数量时出错";
  public static final String EVENT_SERVICE_IS_STARTED_SUCCESSFULLY =
      "Event 服务已成功启动。";
  public static final String EVENT_SERVICE_IS_STOPPED_SUCCESSFULLY =
      "Event 服务已成功停止。";
  public static final String EXCEPTION_ENCOUNTERED_WHEN_TRIGGERING_SCHEMA_REGION_SNAPSHOT =
      "触发 schema region 快照时遇到异常。";
  public static final String EXECUTE_CQ_FAILED = "执行 CQ {} 失败";
  public static final String EXECUTE_CQ_FAILED_TSSTATUS_IS = "执行 CQ {} 失败，TSStatus 为 {}";
  public static final String EXPECTED_PIPE_HEARTBEAT_NODE_COUNT_IS_FALLBACK_TO_1 =
      "期望的 pipe 心跳节点数为 {}，回退为 1。";
  public static final String EXTENDREGION_SUBMIT_ADDREGIONPEERPROCEDURE_SUCCESSFULLY =
      "[ExtendRegion] 成功提交 AddRegionPeerProcedure：{}";
  public static final String EXTEND_REGION_GROUP_FAILED = "扩展 region group 失败";
  public static final String FAILED_IN_THE_READ_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER =
      "执行共识层读写 API 失败：";
  public static final String FAILED_TO_ACQUIRE_LOCK_WHEN_PARSEHEARTBEAT_FROM_NODE_ID =
      "解析来自节点 (id={}) 的心跳时获取锁失败。";
  public static final String FAILED_TO_ACQUIRE_PIPE_LOCK_FOR_AUTO_RESTART_PIPE_TASK =
      "为自动重启 pipe task 获取 pipe 锁失败。";
  public static final String FAILED_TO_ACQUIRE_PIPE_LOCK_FOR_HANDLING_SUCCESSFUL_RESTART =
      "为处理成功重启获取 pipe 锁失败。";
  public static final String FAILED_TO_ALTER_PIPE_RESULT_STATUS =
      "修改 pipe {} 失败。结果状态：{}。";
  public static final String FAILED_TO_CHECK_AND_REPAIR_CONSENSUS_PIPES =
      "检查并修复 consensus pipe 失败";
  public static final String FAILED_TO_CHECK_PASSWORD_FOR_PIPE =
      "检查 pipe %s 的密码失败。";
  public static final String FAILED_TO_CLOSE_CONSUMER_IN_CONSUMER_GROUP_RESULT_STATUS =
      "关闭 consumer group {} 中的 consumer {} 失败。结果状态：{}。";
  public static final String FAILED_TO_CLOSE_EXTRACTOR_AFTER_FAILED_TO_INITIALIZE_EXTRACTOR =
      "初始化 extractor 失败后关闭 extractor 失败，忽略此异常。";
  public static final String FAILED_TO_CLOSE_SINK_AFTER_FAILED_TO_INITIALIZE_IT_IGNORE =
      "初始化 sink 失败后关闭 sink 失败，忽略此异常。";
  public static final String FAILED_TO_COLLECT_COMMITCREATETABLEPLAN =
      "收集 CommitCreateTablePlan 失败";
  public static final String FAILED_TO_COLLECT_PIPE_META_LIST_FROM_CONFIG_NODE_TASK =
      "从 config node task agent 收集 pipe meta 列表失败";
  public static final String FAILED_TO_COLLECT_UNSETTEMPLATEPLAN =
      "收集 UnsetTemplatePlan 失败";
  public static final String FAILED_TO_COLLECT_USER_NAME_FOR_USER_ID =
      "为用户 id {} 收集用户名失败";
  public static final String FAILED_TO_CREATE_CONSUMER_IN_CONSUMER_GROUP_RESULT_STATUS =
      "在 consumer group {} 中创建 consumer {} 失败。结果状态：{}。";
  public static final String FAILED_TO_CREATE_PEER_FOR_CONSENSUS_GROUP =
      "为 consensus group 创建 peer 失败";
  public static final String FAILED_TO_CREATE_PIPE_RESULT_STATUS =
      "创建 pipe {} 失败。结果状态：{}。";
  public static final String FAILED_TO_CREATE_SUBTASK_FOR_PIPE_CREATION_TIME =
      "为 pipe %s 创建子任务失败，创建时间 %d";
  public static final String FAILED_TO_CREATE_TOPIC_WITH_ATTRIBUTES_RESULT_STATUS =
      "创建带属性 {} 的 topic {} 失败。结果状态：{}。";
  public static final String FAILED_TO_ALTER_TOPIC_THE_TOPIC_IS_NOT_EXISTED =
      "修改 topic %s 失败，该 topic 不存在";
  public static final String FAILED_TO_ALTER_TOPIC_WITH_ATTRIBUTES_RESULT_STATUS =
      "修改 topic {} 失败，属性：{}。结果状态：{}。";
  public static final String OWNER_LEASE_DURATION_BELOW_MIN =
      "创建或修改 topic 失败，owner-lease-duration-ms %s 小于允许的最小值 %s ms。";
  public static final String FAILED_TO_DEEP_COPY_PIPEMETA = "深拷贝 pipeMeta 失败";
  public static final String FAILED_TO_DEREGISTER_PIPE_CONFIG_REGION_CONNECTOR =
      "取消注册 pipe config region connector 指标失败，PipeConfigNodeSubtask({}) 不存在";
  public static final String FAILED_TO_DEREGISTER_PIPE_CONFIG_REGION_EXTRACTOR =
      "取消注册 pipe config region extractor 指标失败，IoTDBConfigRegionExtractor({}) 不存在";
  public static final String FAILED_TO_DEREGISTER_PIPE_REMAINING_TIME_METRICS_REMAININGTIMEOPERATOR_DOES_NOT =
      "取消注册 pipe remaining time 指标失败，RemainingTimeOperator({}) 不存在";
  public static final String FAILED_TO_DEREGISTER_PIPE_TEMPORARY_META_METRICS_PIPETEMPORARYMETA_DOES_NOT =
      "取消注册 pipe temporary meta 指标失败，PipeTemporaryMeta({}) 不存在";
  public static final String FAILED_TO_DROP_PIPE_RESULT_STATUS =
      "删除 pipe {} 失败。结果状态：{}。";
  public static final String FAILED_TO_GET_ALL_PIPE_INFO = "获取全部 pipe 信息失败。";
  public static final String FAILED_TO_GET_ALL_SUBSCRIPTION_INFO =
      "获取全部订阅信息失败。";
  public static final String FAILED_TO_GET_ALL_TOPIC_INFO = "获取全部 topic 信息失败。";
  public static final String FAILED_TO_HANDLE_PIPE_META_CHANGES = "处理 pipe 元数据变更失败";
  public static final String FAILED_TO_HANDLE_PIPE_META_CHANGE_RESULT_STATUS =
      "处理 pipe 元数据变更失败。结果状态：{}。";
  public static final String FAILED_TO_LOAD_SNAPSHOT_FROM_BYTEBUFFER =
      "从 byteBuffer {} 加载快照失败。";
  public static final String FAILED_TO_LOAD_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_A_NORMAL =
      "加载快照失败，快照文件 [{}] 不是普通文件。";
  public static final String FAILED_TO_MARK_PIPE_CONFIG_REGION_WRITE_PLAN_EVENT_PIPECONFIGNODESUBTASK =
      "标记 pipe config region write plan event 失败，PipeConfigNodeSubtask({}) 不存在";
  public static final String FAILED_TO_MARK_PIPE_REGION_COMMIT_REMAININGTIMEOPERATOR_DOES_NOT_EXIST =
      "标记 pipe region commit 失败，RemainingTimeOperator({}) 不存在";
  public static final String FAILED_TO_SHOW_SUBSCRIPTION_INFO = "展示订阅信息失败。";
  public static final String FAILED_TO_SHOW_TOPIC_INFO = "展示 topic 信息失败。";
  public static final String FAILED_TO_START_PIPE_RESULT_STATUS =
      "启动 pipe {} 失败。结果状态：{}。";
  public static final String FAILED_TO_STOP_PIPE_RESULT_STATUS =
      "停止 pipe {} 失败。结果状态：{}。";
  public static final String FAILED_TO_SUBMIT_ASYNC_CONSENSUS_PIPE_CREATION_FOR =
      "为 {} 异步提交 consensus pipe 创建失败：{}";
  public static final String FAILED_TO_SUBMIT_ASYNC_CONSENSUS_PIPE_DROP_FOR =
      "为 {} 异步提交 consensus pipe 删除失败：{}";
  public static final String FAILED_TO_SYNC_CONSUMER_GROUP_META_RESULT_STATUS =
      "同步 consumer group 元数据失败。结果状态：{}。";
  public static final String FAILED_TO_SYNC_PIPE_META_RESULT_STATUS =
      "同步 pipe 元数据失败。结果状态：{}。";
  public static final String FAILED_TO_SYNC_TEMPLATE_EXTENSION_INFO_TO_DATANODE =
      "将模板 {} 的扩展信息同步到 DataNode {} 失败";
  public static final String FAILED_TO_SYNC_TOPIC_META_RESULT_STATUS =
      "同步 topic 元数据失败。结果状态：{}。";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_CONFIG_REGION_CONNECTOR_METRICS_CONNECTOR =
      "从 pipe config region connector 指标解绑失败，connector map 不为空";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_CONFIG_REGION_EXTRACTOR_METRICS_EXTRACTOR =
      "从 pipe config region extractor 指标解绑失败，extractor map 不为空";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_REMAINING_TIME_METRICS_REMAININGTIMEOPERATOR_MAP =
      "从 pipe remaining time 指标解绑失败，RemainingTimeOperator map 不为空";
  public static final String FAILED_TO_UNBIND_FROM_PIPE_TEMPORARY_META_METRICS_PIPETEMPORARYMETA_MAP =
      "从 pipe temporary meta 指标解绑失败，PipeTemporaryMeta map 不为空";
  public static final String FAILED_TO_UPDATE_PIPE_PROCEDURE_TIMER_PIPEPROCEDURE_DOES_NOT_EXIST =
      "更新 pipe procedure timer 失败，PipeProcedure({}) 不存在";
  public static final String FAILED_TO_UPDATE_THE_LAST_EXECUTION_TIME_OF_CQ_BECAUSE =
      "更新 CQ {} 的上次执行时间 {} 失败，原因：{}";
  public static final String FAIL_TO_GET_ALLUDFTABLE = "获取 AllUDFTable 失败";
  public static final String FAIL_TO_GET_PIPEPLUGINTABLE = "获取 PipePluginTable 失败";
  public static final String FAIL_TO_GET_TRIGGERTABLE = "获取 TriggerTable 失败";
  public static final String FAIL_TO_GET_UDFTABLE = "获取 UDFTable 失败";
  public static final String FAIL_TO_TRANSFER_BECAUSE_WILL_RETRY =
      "传输失败，原因：{}，将重试";
  public static final String FORCE_UPDATE_NODECACHE_STATUS_CURRENTNANOTIME =
      "强制更新 NodeCache：status={}, currentNanoTime={}";
  public static final String GETDATAPARTITION_INTERFACE_RECEIVE_PARTITIONSLOTSMAP_RETURN =
      "GetDataPartition 接口收到 PartitionSlotsMap：{}，返回：{}";
  public static final String GETNODEPATHSPARTITION_RECEIVED_PARTIALPATH_LEVEL_PATHPATTERNTREE_RESP =
      "[GetNodePathsPartition]:{}收到 PartialPath: {}, Level: {}, PathPatternTree: {}, Resp: {}";
  public static final String GET_OR_CREATE_DATA_PARTITION_RESP_LOG =
      "[GetOrCreateDataPartition]:{}收到 PartitionSlotsMap: {}, 返回 TDataPartitionTableResp: {}";
  public static final String GETORCREATESCHEMAPARTITION_RECEIVE_DATABASENAMESLOTMAP_RETURN_TSCHEMAPARTITIONTABLERESP =
      "[GetOrCreateSchemaPartition]:{}收到 databaseNameSlotMap: {}, 返回 TSchemaPartitionTableResp: {}";
  public static final String GETORCREATESCHEMAPARTITION_RECEIVE_PATHPATTERNTREE_RETURN_TSCHEMAPARTITIONTABLERESP =
      "[GetOrCreateSchemaPartition]:{}收到 PathPatternTree: {}, 返回 TSchemaPartitionTableResp: {}";
  public static final String GETSCHEMAPARTITION_RECEIVE_PATHS_RETURN =
      "GetSchemaPartition 收到 paths：{}，返回：{}";
  public static final String GET_REGION_GROUP_ID_FAIL = "获取 region group id 失败";
  public static final String HEARTBEAT_SERVICE_IS_STARTED_SUCCESSFULLY =
      "心跳服务已成功启动。";
  public static final String HEARTBEAT_SERVICE_IS_STOPPED_SUCCESSFULLY =
      "心跳服务已成功停止。";
  public static final String INCORRECT_VERSION_OF = "版本不正确：";
  public static final String INIT_CONSENSUSMANAGER_SUCCESSFULLY_WHEN_RESTARTED =
      "重启时成功初始化 ConsensusManager";
  public static final String INTERRUPTED_WHILE_WAITING_FOR_PIPETASKCOORDINATOR_LOCK_CURRENT_THREAD =
      "等待 PipeTaskCoordinator 锁时被中断，当前线程：{}";
  public static final String INTERRUPT_WHEN_WAIT_FOR_CALCULATING_REGION_PRIORITY =
      "等待计算 Region 优先级时被中断";
  public static final String INTERRUPT_WHEN_WAIT_FOR_LEADER_ELECTION =
      "等待 leader 选举时被中断";
  public static final String INVALID_EVENT_TYPE = "无效的事件类型：";
  public static final String IOTCONSENSUSV2_LEADER_CHANGED_FAILED_TO_FLUSH_OLD_LEADER_FOR_REGION =
      "[IoTConsensusV2 Leader Changed] 为 region {} flush 旧 leader {} 失败";
  public static final String IOTCONSENSUSV2_LEADER_CHANGED_SUCCESSFULLY_FLUSH_OLD_LEADER_FOR_REGION =
      "[IoTConsensusV2 Leader Changed] 成功为 region {} flush 旧 leader {}";
  public static final String IOTDBCONFIGNODERECEIVER_DOES_NOT_SUPPORT_LOAD_FILE_V1 =
      "IoTDBConfigNodeReceiver 不支持 load file V1。";
  public static final String IOTDBCONFIGREGIONAIRGAPCONNECTOR_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBConfigRegionAirGapConnector 无法传输 TabletInsertionEvent。";
  public static final String IOTDBCONFIGREGIONAIRGAPCONNECTOR_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBConfigRegionAirGapConnector 无法传输 TsFileInsertionEvent。";
  public static final String IOTDBCONFIGREGIONAIRGAPCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBConfigRegionAirGapConnector 不支持传输通用事件：{}。";
  public static final String IOTDBCONFIGREGIONSINK_CAN_T_TRANSFER_TABLETINSERTIONEVENT =
      "IoTDBConfigRegionSink 无法传输 TabletInsertionEvent。";
  public static final String IOTDBCONFIGREGIONSINK_CAN_T_TRANSFER_TSFILEINSERTIONEVENT =
      "IoTDBConfigRegionSink 无法传输 TsFileInsertionEvent。";
  public static final String IOTDBCONFIGREGIONSINK_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT =
      "IoTDBConfigRegionSink 不支持传输通用事件：{}。";
  public static final String IOTDBCONFIGREGIONSOURCE_DOES_NOT_TRANSFERRING_EVENTS_UNDER_SIMPLE_CONSENSUS =
      "IoTDBConfigRegionSource 在 simple consensus 下不传输事件";
  public static final String LEADERBALANCER_FAILED_TO_CHANGE_THE_LEADER_OF_REGION_TO_DATANODE =
      "[LeaderBalancer] 将 Region: {} 的 leader 切换到 DataNode: {} 失败";
  public static final String LEADERBALANCER_REGION_NOT_IN_DATABASEREGIONGROUPMAP =
      "[LeaderBalancer] Region: {} 不在 databaseRegionGroupMap 中";
  public static final String LEADERBALANCER_REGION_NOT_IN_REGIONLEADERMAP =
      "[LeaderBalancer] Region: {} 不在 regionLeaderMap 中";
  public static final String LEADERBALANCER_REGION_NOT_IN_REGIONLOCATIONMAP =
      "[LeaderBalancer] Region: {} 不在 regionLocationMap 中";
  public static final String LEADERBALANCER_REGION_NOT_IN_REGIONSTATISTICSMAP =
      "[LeaderBalancer] Region: {} 不在 regionStatisticsMap 中";
  public static final String LEADERBALANCER_THE_FOLLOWING_REGIONGROUPS_LEADER_CANNOT_BE =
      "[LeaderBalancer] 以下 RegionGroup 无法选出 leader，因为对应的缓存不完整：{}";
  public static final String LEADERBALANCER_TRY_TO_CHANGE_THE_LEADER_OF_REGION_TO_DATANODE =
      "[LeaderBalancer] 尝试将 Region: {} 的 leader 切换到 DataNode: {} ";
  public static final String LOADSTATISTICS_SERVICE_IS_STARTED_SUCCESSFULLY =
      "LoadStatistics 服务已成功启动。";
  public static final String LOADSTATISTICS_SERVICE_IS_STOPPED_SUCCESSFULLY =
      "LoadStatistics 服务已成功停止。";
  public static final String MIGRATEREGION_SUBMIT_REGIONMIGRATEPROCEDURE_SUCCESSFULLY_REGION_ORIGIN_DATANODE =
      "[MigrateRegion] 成功提交 RegionMigrateProcedure，Region：{}，原 DataNode：{}，目标 DataNode：{}，新增 Coordinator：{}，移除 Coordinator：{}";
  public static final String MISMATCHED_CRC32_CODE_WHEN_DESERIALIZING_SERVICE_INFO =
      "反序列化 service info 时 CRC32 码不匹配。";
  public static final String NETWORK_ERROR_WHEN_SEAL_CONFIG_REGION_SNAPSHOT_BECAUSE =
      "封存 config region 快照 %s 时出现网络错误，原因：%s。";
  public static final String NETWORK_ERROR_WHEN_TRANSFER_CONFIG_REGION_WRITE_PLAN_BECAUSE =
      "传输 config region 写入计划 %s 时出现网络错误，原因：%s。";
  public static final String NETWORK_ERROR_WHEN_TRANSFER_EVENT_BECAUSE =
      "传输事件 %s 时出现网络错误，原因：%s。";
  public static final String NODEMANAGER_START_TO_REMOVE_DATANODE =
      "NodeManager 开始移除 DataNode {}";
  public static final String NODEMANAGER_SUBMIT_REMOVEAINODEPLAN_FINISHED =
      "NodeManager 提交 RemoveAINodePlan 完成，{}";
  public static final String NODEMANAGER_SUBMIT_REMOVEDATANODEPLAN_FINISHED_REMOVEDATANODEPLAN =
      "NodeManager 提交 RemoveDataNodePlan 完成，removeDataNodePlan：{}";
  public static final String NODESTATISTICS = "[NodeStatistics]\t {}: {} -> {}";
  public static final String NODESTATISTICS_NODESTATISTICSMAP =
      "[NodeStatistics] NodeStatisticsMap: ";
  public static final String NOT_HAS_PRIVILEGE_TO_TRANSFER_PLAN = "没有传输计划的权限：";
  public static final String NOT_IMPLEMENT_YET = "尚未实现";
  public static final String NO_CORRESPONDING_PIPE_IS_RUNNING_IN_THE_REPORTED_DATAREGION_RUNTIMEMETAFROMAGENT =
      "上报的 DataRegion 中没有对应的 pipe 在运行。runtimeMetaFromAgent 为空，runtimeMetaFromCoordinator：{}";
  public static final String PARTITIONBALANCER_THE_SERIESSLOT_IN_TIMESLOT_WILL_BE =
      "[PartitionBalancer] TimeSlot：{} 中的 SeriesSlot：{} 将被分配给 DataRegionGroup：{}，因为原目标：{} 当前不可用。";
  public static final String PHIACCRUALDETECTOR_TOPOLOGY_IS_BROKEN_HEARTBEAT_HISTORY_MS =
      "[PhiAccrualDetector] 拓扑 {} 已断开，心跳历史 (ms)：{}";
  public static final String PHIACCRUALDETECTOR_TOPOLOGY_IS_RECOVERED_HEARTBEAT_HISTORY_MS =
      "[PhiAccrualDetector] 拓扑 {} 已恢复，心跳历史 (ms)：{}";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_WAS_FAILED_TO_SUBMIT =
      "PipeHandleLeaderChangeProcedure 提交失败。";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_WAS_SUBMITTED_PROCEDUREID =
      "PipeHandleLeaderChangeProcedure 已提交，procedureId：{}。";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_WAS_FAILED_TO_SUBMIT =
      "PipeHandleMetaChangeProcedure 提交失败。";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_WAS_SUBMITTED_PROCEDUREID =
      "PipeHandleMetaChangeProcedure 已提交，procedureId：{}。";
  public static final String PIPEHEARTBEAT_IS_STARTED_SUCCESSFULLY =
      "PipeHeartbeat 已成功启动。";
  public static final String PIPEHEARTBEAT_IS_STOPPED_SUCCESSFULLY =
      "PipeHeartbeat 已成功停止。";
  public static final String PIPEMETASYNCER_IS_STARTED_SUCCESSFULLY =
      "PipeMetaSyncer 已成功启动。";
  public static final String PIPEMETASYNCER_IS_STOPPED_SUCCESSFULLY =
      "PipeMetaSyncer 已成功停止。";
  public static final String PIPERUNTIMECONFIGNODEAGENT_STARTED =
      "PipeRuntimeConfigNodeAgent 已启动";
  public static final String PIPERUNTIMECONFIGNODEAGENT_STOPPED =
      "PipeRuntimeConfigNodeAgent 已停止";
  public static final String PIPERUNTIMECOORDINATOR_MEETS_ERROR_IN_UPDATING_PIPEMETAKEEPER =
      "PipeRuntimeCoordinator 更新 pipeMetaKeeper 时遇到错误，";
  public static final String PIPETASKCOORDINATORLOCK_IS_HELD_BY_ANOTHER_THREAD_SKIP_THIS_ROUND_OF =
      "PipeTaskCoordinatorLock 被其他线程持有，跳过本轮心跳，以尽量避免 procedure 和 rpc 堆积";
  public static final String PIPETASKCOORDINATORLOCK_IS_HELD_BY_ANOTHER_THREAD_SKIP_THIS_ROUND_OF_2 =
      "PipeTaskCoordinatorLock 被其他线程持有，跳过本轮同步，以尽量避免 procedure 和 rpc 堆积";
  public static final String PIPETASKCOORDINATOR_LOCK_ACQUIRED_BY_THREAD =
      "PipeTaskCoordinator 锁已由线程 {} 获取";
  public static final String PIPETASKCOORDINATOR_LOCK_FAILED_TO_ACQUIRE_BY_THREAD_BECAUSE_OF_TIMEOUT =
      "线程 {} 因超时获取 PipeTaskCoordinator 锁失败";
  public static final String PIPETASKCOORDINATOR_LOCK_RELEASED_BY_THREAD =
      "PipeTaskCoordinator 锁已由线程 {} 释放";
  public static final String PIPETASKCOORDINATOR_LOCK_WAITING_FOR_THREAD =
      "PipeTaskCoordinator 锁正在等待线程 {}";
  public static final String PIPE_SNAPSHOT_DIR_FOUND_DELETING_IT =
      "发现 pipe 快照目录，正在删除：{}，";
  public static final String PROCEDUREMANAGER_IS_STARTED_SUCCESSFULLY = "ProcedureManager 已成功启动。";
  public static final String PROCEDUREMANAGER_IS_STOPPED_SUCCESSFULLY = "ProcedureManager 已成功停止。";
  public static final String PROCEDURE_DETAILS_ARE = "[{}] procedure 详情为 {}";
  public static final String REBALANCEDATAALLOTTABLE_DATABASE =
      "[ReBalanceDataAllotTable] Database：{}，";
  public static final String RECEIVED_PIPE_HEARTBEAT_REQUEST_FROM_CONFIG_COORDINATOR =
      "收到来自 config coordinator 的 pipe 心跳请求 {}。";
  public static final String RECEIVER_ID = "Receiver id = {}: {}";
  public static final String RECEIVER_ID_EXCEPTION_ENCOUNTERED_WHILE_EXECUTING_PLAN =
      "Receiver id = {}: 执行计划 {} 时遇到异常：";
  public static final String RECEIVER_ID_FAILURE_STATUS_ENCOUNTERED_WHILE_EXECUTING_PLAN =
      "Receiver id = {}: 执行计划 {} 时遇到失败状态：{}";
  public static final String RECEIVER_ID_PERMISSION_CHECK_FAILED_WHILE_EXECUTING_PLAN =
      "Receiver id = {}: 执行计划 {} 时权限检查失败：{}";
  public static final String RECEIVER_ID_UNSUPPORTED_PIPEREQUESTTYPE_ON_CONFIGNODE_RESPONSE_STATUS =
      "Receiver id = {}: ConfigNode 上不支持的 PipeRequestType，响应状态 = {}。";
  public static final String RECONSTRUCTREGION_SUBMIT_RECONSTRUCTREGIONPROCEDURE_SUCCESSFULLY =
      "[ReconstructRegion] 成功提交 ReconstructRegionProcedure，{}";
  public static final String REGIONCLEANER_IS_STARTED_SUCCESSFULLY =
      "RegionCleaner 已成功启动。";
  public static final String REGIONCLEANER_IS_STOPPED_SUCCESSFULLY =
      "RegionCleaner 已成功停止。";
  public static final String REGIONELECTION_THE_LEADER_OF_REGIONGROUPS_IS_ELECTED =
      "[RegionElection] RegionGroup：{} 的 leader 已选出。";
  public static final String REGIONELECTION_THE_LEADER_OF_REGIONGROUPS_IS_NOT_DETERMINED_AFTER_10 =
      "[RegionElection] RegionGroup：{} 的 leader 在 10 个心跳间隔后仍未确定，部分功能可能失败。";
  public static final String REGIONELECTION_WAIT_FOR_LEADER_ELECTION_OF_REGIONGROUPS =
      "[RegionElection] 等待 RegionGroup：{} 的 leader 选举";
  public static final String REGIONGROUPSTATISTICS_REGIONGROUP =
      "[RegionGroupStatistics]\t RegionGroup {}: {} -> {}";
  public static final String REGIONGROUPSTATISTICS_REGIONGROUPSTATISTICSMAP =
      "[RegionGroupStatistics] RegionGroupStatisticsMap: ";
  public static final String REGIONGROUPSTATISTICS_REGION_IN_DATANODE =
      "[RegionGroupStatistics]\t DataNode {} 中的 Region：{} -> {}";
  public static final String REGIONGROUPSTATISTICS_REGION_IN_DATANODE_NULL =
      "[RegionGroupStatistics]\t DataNode {} 中的 Region：null -> {}";
  public static final String REGIONGROUPSTATISTICS_REGION_IN_DATANODE_NULL_2 =
      "[RegionGroupStatistics]\t DataNode {} 中的 Region：{} -> null";
  public static final String REGIONGROUPSTATUS_DOESN_T_EXIST =
      "RegionGroupStatus %s 不存在。";
  public static final String REGIONPRIORITY = "[RegionPriority]\t {}: {}->{}";
  public static final String REGIONPRIORITY_REGIONPRIORITYMAP =
      "[RegionPriority] RegionPriorityMap: ";
  public static final String REGIONPRIORITY_THE_ROUTING_PRIORITY_OF_REGIONGROUPS_IS_CALCULATED =
      "[RegionPriority] RegionGroup：{} 的路由优先级已计算完成。";
  public static final String REGIONPRIORITY_THE_ROUTING_PRIORITY_OF_REGIONGROUPS_IS_NOT_DETERMINED_AFTER =
      "[RegionPriority] RegionGroup：{} 的路由优先级在 10 个心跳间隔后仍未确定，部分功能可能失败。";
  public static final String REGIONPRIORITY_WAIT_FOR_REGION_PRIORITY_UPDATE_OF_REGIONGROUPS =
      "[RegionPriority] 等待 RegionGroup：{} 的 Region 优先级更新";
  public static final String REGION_ID = "Region id ";
  public static final String REMOVEREGIONPEER_SUBMIT_REMOVEREGIONPEERPROCEDURE_SUCCESSFULLY =
      "[RemoveRegionPeer] 成功提交 RemoveRegionPeerProcedure：{}";
  public static final String REMOVE_REGION_TARGET_DATANODE_NOT_FOUND_WILL_SIMPLY_CLEAN_UP =
      "移除 region：未找到目标 DataNode {}，将仅清理 region {} 的分区表，不执行其他操作。";
  public static final String REPORT_PIPERUNTIMEEXCEPTION_TO_LOCAL_PIPETASKMETA_EXCEPTION_MESSAGE =
      "向本地 PipeTaskMeta({}) 上报 PipeRuntimeException，异常消息：{}";
  public static final String RETRYFAILMISSIONS_SERVICE_IS_STARTED_SUCCESSFULLY =
      "RetryFailMissions 服务已成功启动。";
  public static final String RETRYFAILMISSIONS_SERVICE_IS_STOPPED_SUCCESSFULLY =
      "RetryFailMissions 服务已成功停止。";
  public static final String SERIALIZATION_FAILED_FOR_THE_ALTER_ENCODING_TIME_SERIES_PLAN_IN =
      "pipe 传输中对 alter encoding time series plan 序列化失败，跳过传输";
  public static final String SERIALIZATION_FAILED_FOR_THE_DELETE_LOGICAL_VIEW_PLAN_IN_PIPE =
      "pipe 传输中对 delete logical view plan 序列化失败，跳过传输";
  public static final String SERIALIZATION_FAILED_FOR_THE_DELETE_TIME_SERIES_PLAN_IN_PIPE =
      "pipe 传输中对 delete time series plan 序列化失败，跳过传输";
  public static final String SOMETHING_WRONG_HAPPENED_WHILE_CALLING_CONSENSUS_LAYER_S_CREATELOCALPEER_API =
      "调用共识层 createLocalPeer API 时发生错误。";
  public static final String SOME_PIPES_NEED_RESTARTING_WILL_RESTART_THEM_AFTER_THIS_SYNC =
      "部分 pipe 需要重启，将在本次同步后重启它们";
  public static final String STARTEXECUTECQ_EXECUTE_CQ_ON_DATANODE_TIME_RANGE_IS_CURRENT_TIME =
      "[StartExecuteCQ] 在 DataNode[{}] 上执行 CQ {}，时间范围为 [{}, {})，当前时间为 {}";
  public static final String START_TO_ACTIVATE_UDF_IN_UDF_TABLE_ON_CONFIG_NODES =
      "开始在 Config Nodes 上激活 UDF_Table 中的 UDF [{}]";
  public static final String START_TO_ADD_UDF_IN_UDF_TABLE_ON_CONFIG_NODES =
      "开始在 Config Nodes 上向 UDF_Table 中添加 UDF [{}]";
  public static final String START_TO_CREATE_REGION_ON_DATANODE =
      "开始在 DataNode: {} 上创建 Region：{}";
  public static final String START_TO_CREATE_UDF_ON_DATA_NODES_NEEDTOSAVEJAR =
      "开始在 Data Nodes 上创建 UDF [{}]，needToSaveJar[{}]";
  public static final String START_TRANSFER_OF = "开始传输 {}";
  public static final String STOP_SUBMITTING_CQ_BECAUSE = "停止提交 CQ {}，原因：{}";
  public static final String STOP_SUBMITTING_CQ_BECAUSE_CURRENT_NODE_IS_NOT_LEADER_OR =
      "停止提交 CQ {}，原因：当前节点不是 leader 或当前调度线程池已关闭。";
  public static final String SUBMITTED_ASYNC_CONSENSUS_PIPE_CREATION =
      "已异步提交 consensus pipe 创建：{}";
  public static final String SUBMITTED_ASYNC_CONSENSUS_PIPE_DROP =
      "已异步提交 consensus pipe 删除：{}";
  public static final String SUBMIT_REMOVEAINODEPROCEDURE_SUCCESSFULLY =
      "成功提交 RemoveAINodeProcedure，{}";
  public static final String SUBMIT_REMOVECONFIGNODEPROCEDURE_SUCCESSFULLY =
      "成功提交 RemoveConfigNodeProcedure：{}";
  public static final String SUBMIT_REMOVEDATANODESPROCEDURE_SUCCESSFULLY =
      "成功提交 RemoveDataNodesProcedure，{}";
  public static final String SUBSCRIPTIONCOORDINATORLOCK_IS_HELD_BY_ANOTHER_THREAD_SKIP_THIS_ROUND_OF =
      "SubscriptionCoordinatorLock 被其他线程持有，跳过本轮同步，以尽量避免 procedure 和 rpc 堆积";
  public static final String SUBSCRIPTIONMETASYNCER_IS_STARTED_SUCCESSFULLY =
      "SubscriptionMetaSyncer 已成功启动。";
  public static final String SUBSCRIPTIONMETASYNCER_IS_STOPPED_SUCCESSFULLY =
      "SubscriptionMetaSyncer 已成功停止。";
  public static final String SUCCESSFULLY_TRANSFERRED_CONFIG_EVENT =
      "成功传输 config event {}。";
  public static final String SUCCESSFULLY_TRANSFERRED_CONFIG_REGION_SNAPSHOT =
      "成功传输 config region 快照 {}。";
  public static final String THERE_IS_NO_RUNNING_DATANODE_TO_EXECUTE_CQ =
      "没有处于 RUNNING 状态的 DataNode 可用于执行 CQ {}";
  public static final String THE_CONFIGNODE_WILL_BE_SHUTDOWN_SOON_MARK_IT_AS_UNKNOWN =
      "ConfigNode-{} 即将关闭，将其标记为 Unknown";
  public static final String THE_CONFIG_REGION_AIR_GAP_CONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING =
      "config region air gap connector 不支持传输单文件分片字节。";
  public static final String THE_CONFIG_REGION_SINK_DOES_NOT_SUPPORT_TRANSFERRING_SINGLE_FILE =
      "config region sink 不支持传输单文件分片请求。";
  public static final String THE_CONFIG_REGION_SNAPSHOTS_CANNOT_BE_PARSED =
      "无法解析 config region 快照 %s。";
  public static final String THE_DATABASE_DOESN_T_EXIST_MAYBE_IT_HAS_BEEN_PRE =
      "Database: {} 不存在，可能已被预删除。";
  public static final String THE_DATANODE_WILL_BE_SHUTDOWN_SOON_MARK_IT_AS_UNKNOWN =
      "DataNode-{} 即将关闭，将其标记为 Unknown";
  public static final String THE_REMOVENODEREPLICASELECT_METHOD_OF_GREEDYREGIONGROUPALLOCATOR_IS_YET =
      "GreedyRegionGroupAllocator 的 removeNodeReplicaSelect 方法尚未实现。";
  public static final String THE_REMOVENODEREPLICASELECT_METHOD_OF_PARTITEGRAPHPLACEMENTREGIONGROUPALLOCATOR =
      "PartiteGraphPlacementRegionGroupAllocator 的 removeNodeReplicaSelect 方法尚未实现。";
  public static final String THE_REMOVE_DATANODE_REQUEST_CHECK_FAILED_REQ_CHECK_RESULT =
      "移除 DataNode 请求检查失败。请求：{}，检查结果：{}";
  public static final String TOPOLOGY_ASYMMETRIC_NETWORK_PARTITION_FROM_TO =
      "[Topology] 从 {} 到 {} 出现非对称网络分区";
  public static final String TOPOLOGY_CLUSTER_TOPOLOGY_CHANGED_LATEST =
      "[Topology] 集群拓扑已变化，最新：{}";
  public static final String TOPOLOGY_PROBING_HAS_STARTED_SUCCESSFULLY =
      "拓扑探测已成功启动";
  public static final String TOPOLOGY_PROBING_HAS_STOPPED_SUCCESSFULLY =
      "拓扑探测已成功停止";
  public static final String TOPOLOGY_TOPOLOGY_OF_DATANODE_IS_NOW_TO_DATANODE =
      "[Topology] DataNode {} 的拓扑现在对 DataNode {} 为 {}";
  public static final String UNABLE_TO_PARSE_PATH_WHEN_CHECKING_READ_PRIVILEGE_PATH =
      "检查 READ 权限时无法解析路径，path：{}";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_CREATING_SERVICE_ON_DATANODE =
      "在 DataNode {} 上创建 Service {} 时发生意外错误：";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_DROPPING_CQ =
      "删除 cq {} 时发生意外错误：";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_DROPPING_SERVICE_ON_DATANODE =
      "在 DataNode {} 上删除 Service {} 时发生意外错误：";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_FETCHING_CQ_LIST =
      "获取 cq 列表时发生意外错误：";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_GETTING_USER_DEFINED_SERVICE =
      "获取用户自定义 Service 时发生意外错误：";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_SHOWING_CQ =
      "展示 cq 时发生意外错误：";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_SHOWING_SERVICE =
      "展示 Service 时发生意外错误：";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_STARTING_SERVICE_ON_DATANODE =
      "在 DataNode {} 上启动 Service {} 时发生意外错误：";
  public static final String UNEXPECTED_ERROR_HAPPENED_WHILE_STOPPING_SERVICE_ON_DATANODE =
      "在 DataNode {} 上停止 Service {} 时发生意外错误：";
  public static final String UNEXPECTED_INTERRUPTION_DURING_RETRY_CREATING_PEER_FOR_CONSENSUS_GROUP =
      "重试为 consensus group 创建 peer 过程中发生意外中断";
  public static final String UNEXPECTED_INTERRUPTION_DURING_RETRY_GETTING_LATEST_REGION_ROUTE_MAP =
      "重试获取最新 region route map 过程中发生意外中断";
  public static final String UNEXPECTED_INTERRUPTION_DURING_WAITING_FOR_CONFIGNODE_LEADER_READY =
      "等待 configNode leader 就绪过程中发生意外中断。";
  public static final String UNEXPECTED_INTERRUPTION_DURING_WAITING_FOR_GET_CLUSTER_ID =
      "等待获取 cluster id 过程中发生意外中断。";
  public static final String UNEXPECTED_NON_CREATE_REGION_MAINTAIN_TASK_SKIPPED =
      "RegionMaintainer 队列中出现意外的非 create 任务；跳过处理（该队列目前仅用于重建 region 副本，region 删除由 RemoveRegionGroupProcedure 处理）。";
  public static final String UNEXPECTED_NULL_PROCEDURE_PARAMETERS_FOR_WAITINGPROCEDUREFINISHED =
      "waitingProcedureFinished 的 procedure 参数为空";
  public static final String UNKNOWN_DATAPARTITION_ALLOCATION_STRATEGY_USING_INHERIT_STRATEGY_BY_DEFAULT =
      "未知的 DataPartition 分配策略 {}，默认使用 INHERIT 策略。";
  public static final String UNKNOWN_TIMEOUTPOLICY = "未知的 TimeoutPolicy：";
  public static final String UN_PARSE_ABLE_PATH_NAME_ENCOUNTERED_DURING_TEMPLATE_PRIVILEGE_TRIMMING =
      "模板权限裁剪时遇到无法解析的路径名，请检查";
  public static final String UPGRADE_CONFIGNODE_CONSENSUS_WAL_DIR_FOR_SIMPLECONSENSUS_FROM_VERSION_1 =
      "将 ConfigNode consensus wal 目录从 version/1.0 升级到 version/1.1 失败，";
  public static final String WRITE_PARTITION_ALLOCATION_RESULT_FAILED_BECAUSE =
      "写入分区分配结果失败，原因：{}";

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
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_WAS_SUBMITTED_PROCEDUREID_ARG_6DBD6075 = "SubscriptionHandleLeaderChangeProcedure 已提交，procedureId：{}。";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_WAS_FAILED_SUBMIT_58FAB03F = "SubscriptionHandleLeaderChangeProcedure 提交失败。";
  public static final String EXCEPTION_INVALID_2928F475 = " 无效";
  public static final String MESSAGE_FAIL_CREATE_TRIGGER_ARG_SIZE_JAR_TOO_LARGE_YOU_CAN_11869523 =
      "创建 trigger[%s] 失败，Jar 包过大，可以在 ConfigNode 上调大属性 'config_node_ratis_log_appender_buffer_size_max' 的值";
  public static final String MESSAGE_FAIL_CREATE_PIPE_PLUGIN_ARG_SIZE_JAR_TOO_LARGE_YOU_D194A893 =
      "创建 pipe plugin[%s] 失败，Jar 包过大，可以在 ConfigNode 上调大属性 'config_node_ratis_log_appender_buffer_size_max' 的值";
  public static final String MESSAGE_FAIL_CREATE_UDF_ARG_SIZE_JAR_TOO_LARGE_YOU_CAN_2F119802 =
      "创建 UDF[%s] 失败，Jar 包过大，可以在 ConfigNode 上调大属性 'config_node_ratis_log_appender_buffer_size_max' 的值";
  public static final String EXCEPTION_FAILED_SERIALIZE_REGION_PROGRESS_1769D6F1 = "序列化 Region 进度失败 ";
  public static final String MESSAGE_CONSENSUSMANAGER_TARGET_CONFIGNODE_NOT_INITIALIZED_4D386066 = "目标 ConfigNode 的 ConsensusManager 未初始化，";
  public static final String MESSAGE_PLEASE_MAKE_SURE_TARGET_CONFIGNODE_HAS_BEEN_STARTED_SUCCESSFULLY_C78201DC = "请确保目标 ConfigNode 已成功启动。";
  public static final String MESSAGE_CONFIG_NODE_CONSENSUS_PROTOCOL_CLASS_D0F437AF = "config_node_consensus_protocol_class";
  public static final String MESSAGE_DATA_REGION_CONSENSUS_PROTOCOL_CLASS_AB025B20 = "data_region_consensus_protocol_class";
  public static final String MESSAGE_SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS_480645EF = "schema_region_consensus_protocol_class";
  public static final String MESSAGE_SERIES_SLOT_NUM_115D9BE0 = "series_slot_num";
  public static final String MESSAGE_SERIES_PARTITION_EXECUTOR_CLASS_AD1B5C24 = "series_partition_executor_class";
  public static final String MESSAGE_TIME_PARTITION_INTERVAL_CE476507 = "time_partition_interval";
  public static final String MESSAGE_SCHEMA_REPLICATION_FACTOR_11DB65B5 = "schema_replication_factor";
  public static final String MESSAGE_DATA_REPLICATION_FACTOR_22465D3B = "data_replication_factor";
  public static final String MESSAGE_SCHEMA_REGION_PER_DATA_NODE_555F29BC = "schema_region_per_data_node";
  public static final String MESSAGE_DATA_REGION_PER_DATA_NODE_C183AAD5 = "data_region_per_data_node";
  public static final String MESSAGE_READ_CONSISTENCY_LEVEL_B12D8D95 = "read_consistency_level";
  public static final String MESSAGE_DISK_SPACE_WARNING_THRESHOLD_19635ACA = "disk_space_warning_threshold";
  public static final String MESSAGE_TIMESTAMP_PRECISION_9591C9C9 = "timestamp_precision";
  public static final String MESSAGE_SCHEMA_ENGINE_MODE_E37ED98C = "schema_engine_mode";
  public static final String MESSAGE_TAG_ATTRIBUTE_TOTAL_SIZE_AF658CFE = "tag_attribute_total_size";
  public static final String MESSAGE_DATABASE_LIMIT_THRESHOLD_45C23274 = "database_limit_threshold";
  public static final String LOG_UNEXPECTED_ERROR_HAPPENED_SETTING_SPACE_QUOTA_DATABASE_ARG_F6ED7586 = "设置数据库 %s 的空间配额时发生意外错误 ";
  public static final String LOG_UNEXPECTED_ERROR_HAPPENED_SETTING_THROTTLE_QUOTA_USER_ARG_C111BE81 = "设置用户 %s 的限流配额时发生意外错误 ";
  public static final String LOG_SCHEMA_TEMPLATE_NEED_TWO_FILES_1E57542A = "schema_template 需要两个文件";
  public static final String LOG_GOT_IOEXCEPTION_DESERIALIZE_USE_ROLE_FILE_TYPE_ARG_1B548759 = "反序列化 use&role 文件时发生 IOException，类型：{}";
  public static final String LOG_GOT_IOEXCEPTION_DESERIALIZE_ROLELIST_1354F29E = "反序列化 roleList 时发生 IOException";
  public static final String LOG_GOT_EXCEPTION_DESERIALIZING_TTL_FILE_F806EB40 = "反序列化 ttl 文件时发生异常";
  public static final String LOG_UNRECOGNIZED_NODE_TYPE_CANNOT_DESERIALIZE_MTREE_GIVEN_BUFFER_5CF3121B = "无法识别节点类型，无法从给定缓冲区反序列化 MTree";
  public static final String LOG_GOT_IOEXCEPTION_CONSTRUCT_DATABASE_TREE_49436621 = "构建数据库树时发生 IOException";
  public static final String LOG_GOT_IOEXCEPTION_DESERIALIZE_TEMPLATE_INFO_49EE617E = "反序列化模板信息时发生 IOException";
  public static final String MESSAGE_MEASUREMENTS_NOT_FOUND_ARG_CANNOT_AUTO_DETECT_980D7D44 = "未找到 %s 的测点，无法自动检测";
  public static final String LOG_FAILED_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_ARG_ALREADY_EXIST_EB2A6093 = "获取快照失败，原因：快照文件 [{}] 已存在。";
  public static final String LOG_FAILED_LOAD_SNAPSHOT_SNAPSHOT_FILE_ARG_NOT_EXIST_8828CFBA = "无法加载快照，快照文件 [{}] 不存在。";
  public static final String LOG_YOU_MAYBE_NEED_RENAME_SIMPLE_DIR_0_0_MANUALLY_2A12C5C9 = "可能需要手动将 simple 目录重命名为 0_0。";
  public static final String LOG_CONFIGNODE_LOCAL_PEER_HAS_ALREADY_BEEN_CREATED_ARG_FA75E88F = "ConfigNode 本地 peer 已创建：{}";
  public static final String LOG_CONFIGNODE_PEER_ARG_HAS_ALREADY_BEEN_ADDED_ARG_A8F958B0 = "ConfigNode peer {} 已添加：{}";
  public static final String LOG_CONFIGNODE_PEER_ARG_HAS_ALREADY_BEEN_REMOVED_ARG_FACD71EE = "ConfigNode peer {} 已移除：{}";
  public static final String MESSAGE_CURRENT_CONFIGNODE_LEADER_BUT_NOT_READY_YET_PLEASE_TRY_AGAIN_F0B10645 = "当前 ConfigNode 是 leader，但共识层尚未就绪。";

  public static final String MESSAGE_CURRENT_CONFIGNODE_LEADER_SERVICE_NOT_READY = "当前 ConfigNode 是 leader，但 leader 服务尚未就绪。";

  public static final String MESSAGE_CURRENT_CONFIGNODE_NOT_LEADER_PLEASE_REDIRECT_NEW_CONFIGNODE_F9AF262D = "当前 ConfigNode 不是 leader，请重定向到新的 ConfigNode。";
  public static final String LOG_FAILED_SYNC_COMMIT_PROGRESS_RESULT_STATUS_ARG_A9E46E80 = "同步提交进度失败。结果状态：{}。";
  public static final String MESSAGE_FAILED_ALTER_DATABASE_DATABASE_2734674F = "修改数据库失败。数据库 ";
  public static final String MESSAGE_DOESN_T_EXIST_EED8C92E = " 不存在。";
  public static final String MESSAGE_FAILED_ALTER_DATABASE_SCHEMAREGIONGROUPNUM_COULD_ONLY_INCREASED_B98229D3 = "修改数据库失败。SchemaRegionGroupNum 只能增加。";
  public static final String MESSAGE_CURRENT_SCHEMAREGIONGROUPNUM_ARG_ALTER_SCHEMAREGIONGROUPNUM_ARG_F7495BC2 = "当前 SchemaRegionGroupNum：%d，待修改的 SchemaRegionGroupNum：%d";
  public static final String MESSAGE_FAILED_ALTER_DATABASE_DATAREGIONGROUPNUM_COULD_ONLY_INCREASED_84283EB5 = "修改数据库失败。DataRegionGroupNum 只能增加。";
  public static final String MESSAGE_CURRENT_DATAREGIONGROUPNUM_ARG_ALTER_DATAREGIONGROUPNUM_ARG_61C6E978 = "当前 DataRegionGroupNum：%d，待修改的 DataRegionGroupNum：%d";
  public static final String MESSAGE_ARG_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_CURRENT_MIN_ARG_REGIONGROUPNUM_ARG_B81D93DF = "%s 应大于等于当前最小 %sRegionGroupNum：%d。";
  public static final String MESSAGE_ARG_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_CURRENT_MAX_ARG_REGIONGROUPNUM_ARG_3D170323 = "%s 应大于等于当前最大 %sRegionGroupNum：%d。";
  public static final String MESSAGE_ARG_SHOULD_BE_GREATER_THAN_OR_EQUAL_TO_ALLOCATED_ARG_REGIONGROUPNUM_ARG_994394A1 = "%s 应大于等于已分配的 %sRegionGroupNum：%d。";
  public static final String MESSAGE_FAILED_CREATE_DATABASE_SCHEMAREPLICATIONFACTOR_SHOULD_POSITIVE_8847F33C = "创建数据库失败。schemaReplicationFactor 应为正数。";
  public static final String MESSAGE_FAILED_CREATE_DATABASE_DATAREPLICATIONFACTOR_SHOULD_POSITIVE_C2565B7E = "创建数据库失败。dataReplicationFactor 应为正数。";
  public static final String MESSAGE_FAILED_CREATE_DATABASE_TIMEPARTITIONORIGIN_SHOULD_NON_NEGATIVE_BD0595C9 = "创建数据库失败。timePartitionOrigin 应为非负数。";
  public static final String MESSAGE_FAILED_CREATE_DATABASE_TIMEPARTITIONINTERVAL_SHOULD_POSITIVE_BB1B473F = "创建数据库失败。timePartitionInterval 应为正数。";
  public static final String MESSAGE_FAILED_CREATE_DATABASE_SCHEMAREGIONGROUPNUM_SHOULD_POSITIVE_8396A2AB = "创建数据库失败。schemaRegionGroupNum 应为正数。";
  public static final String MESSAGE_ACCEPT_NODE_REGISTRATION_4133276A = "接受节点注册。";
  public static final String MESSAGE_ACCEPT_NODE_RESTART_1BC1A8DD = "接受节点重启。";
  public static final String MESSAGE_REJECT_ARG_START_BECAUSE_CLUSTERNAME_CURRENT_ARG_TARGET_CLUSTER_INCONSISTENT_B9E197DB = "拒绝启动 %s。原因：当前 %s 的 ClusterName 与目标集群不一致。";
  public static final String MESSAGE_CLUSTERNAME_CURRENT_NODE_ARG_CLUSTERNAME_TARGET_CLUSTER_ARG_5C34BE8D = "当前节点的 ClusterName：%s，目标集群的 ClusterName：%s。";
  public static final String MESSAGE_1_CHANGE_SEED_CONFIG_NODE_PARAMETER_ARG_JOIN_CORRECT_CLUSTER_5E9D753C = "\t1. 修改 %s 中的 seed_config_node 参数以加入正确的集群。";
  public static final String MESSAGE_2_CHANGE_CLUSTER_NAME_PARAMETER_ARG_MATCH_TARGET_CLUSTER_0A0DB235 = "\n\t2. 修改 %s 中的 cluster_name 参数以匹配目标集群";
  public static final String MESSAGE_REJECT_ARG_REGISTRATION_BECAUSE_FOLLOWING_IP_PORT_ARG_CURRENT_ARG_CB78CC3B =
      "拒绝注册 %s。原因：以下 ip:port：%s（属于当前 %s）与集群中其他已注册节点冲突。";
  public static final String MESSAGE_1_USE_SQL_SHOW_CLUSTER_DETAILS_FIND_OUT_CONFLICT_NODES_A1195AEA = "\t1. 使用 SQL：\"show cluster details\" 找出冲突节点。移除它们后重试启动。";
  public static final String MESSAGE_2_CHANGE_CONFLICT_IP_PORT_CONFIGURATIONS_ARG_FILE_RETRY_START_CF3F08F6 = "\n\t2. 修改 %s 文件中的冲突 ip:port 配置，然后重试启动。";
  public static final String MESSAGE_CLUSTER_ID_HAS_NOT_GENERATED_PLEASE_TRY_AGAIN_LATER_58A1C3F2 = "cluster id 尚未生成，请稍后重试";
  public static final String MESSAGE_REJECT_ARG_RESTART_BECAUSE_CLUSTERNAME_CURRENT_ARG_TARGET_CLUSTER_INCONSISTENT_2075F29D = "拒绝重启 %s。原因：当前 %s 的 ClusterName 与目标集群不一致。";
  public static final String MESSAGE_REJECT_ARG_RESTART_BECAUSE_NODEID_CURRENT_ARG_ARG_AC13EDD5 = "拒绝重启 %s。原因：当前 %s 的 nodeId 为 %d。";
  public static final String MESSAGE_1_DELETE_DATA_DIR_RETRY_86A23473 = "\t1. 删除 \"data\" 目录后重试。";
  public static final String MESSAGE_REJECT_ARG_RESTART_BECAUSE_THERE_NO_CORRESPONDING_ARG_WHOSE_NODEID_455578E9 = "拒绝重启 %s。原因：集群中没有对应的 %s（nodeId=%d）。";
  public static final String MESSAGE_1_MAYBE_YOU_VE_ALREADY_REMOVED_CURRENT_ARG_WHOSE_NODEID_92165504 =
      "\t1. 可能你已经移除了当前 %s（nodeId=%d）。请删除无用的 'data' 目录并重试启动。";
  public static final String MESSAGE_REJECT_ARG_RESTART_BECAUSE_CLUSTERID_CURRENT_ARG_TARGET_CLUSTER_INCONSISTENT_0398A6CE = "拒绝重启 %s。原因：当前 %s 的 clusterId 与目标集群不一致。";
  public static final String MESSAGE_CLUSTERID_CURRENT_NODE_ARG_CLUSTERID_TARGET_CLUSTER_ARG_23C42434 = "当前节点的 ClusterId：%s，目标集群的 ClusterId：%s。";
  public static final String MESSAGE_1_PLEASE_CHECK_IF_NODE_CONFIGURATION_PATH_CORRECT_7FB5D559 = "\t1. 请检查节点配置或路径是否正确。";
  public static final String MESSAGE_REJECT_ARG_RESTART_BECAUSE_INTERNAL_TENDPOINTS_ARG_CAN_T_MODIFIED_A58B99F0 = "拒绝重启 %s。原因：此 %s 的 internal TEndPoints 不能被修改。";
  public static final String MESSAGE_1_PLEASE_KEEP_INTERNAL_TENDPOINTS_NODE_SAME_AS_BEFORE_2FDB2034 = "\t1. 请保持此节点的 internal TEndPoints 与之前一致。";
  public static final String MESSAGE_REMOVE_CONFIGNODE_FAILED_BECAUSE_THERE_ONLY_ONE_CONFIGNODE_CURRENT_CLUSTER_D1273758 = "移除 ConfigNode 失败，原因：当前集群中只有一个 ConfigNode。";
  public static final String MESSAGE_REMOVE_CONFIGNODE_FAILED_BECAUSE_THERE_NO_OTHER_CONFIGNODE_RUNNING_STATUS_C9C43315 = "移除 ConfigNode 失败，原因：当前集群中没有其他处于 Running 状态的 ConfigNode。";
  public static final String MESSAGE_REMOVE_CONFIGNODE_FAILED_BECAUSE_CONFIGNODEGROUP_LEADER_ELECTION_PLEASE_RETRY_3EE602F6 = "移除 ConfigNode 失败，原因：ConfigNodeGroup 正在进行 leader 选举，请重试。";
  public static final String MESSAGE_TRANSFER_CONFIGNODE_LEADER_FAILED_BECAUSE_CAN_NOT_FIND_ANY_RUNNING_1FE4F96D = "转移 ConfigNode leader 失败，原因：找不到任何正在运行的 ConfigNode。";
  public static final String MESSAGE_CONFIGNODE_REMOVED_LEADER_ALREADY_TRANSFER_LEADER_FA6D1603 = "待移除的 ConfigNode 是 leader，已将 Leader 转移到 ";
  public static final String MESSAGE_TARGET_DATANODE_NOT_EXISTED_PLEASE_ENSURE_YOUR_INPUT_QUERYID_CORRECT_AB84CCDF = "目标 DataNode 不存在，请确保输入的 <queryId> 正确";
  public static final String MESSAGE_CREATE_SCHEMAPARTITION_FAILED_BECAUSE_DATABASE_ARG_NOT_EXISTS_D8AE1679 = "创建 SchemaPartition 失败，原因：数据库 %s 不存在";
  public static final String MESSAGE_CREATE_SCHEMAPARTITION_FAILED_BECAUSE_DATABASE_ARG_DOES_NOT_EXIST_2617832C = "创建 SchemaPartition 失败，原因：数据库 %s 不存在";
  public static final String MESSAGE_CREATE_DATAPARTITION_FAILED_BECAUSE_DATABASE_ARG_NOT_EXISTS_F223D5C2 = "创建 DataPartition 失败，原因：数据库 %s 不存在";
  public static final String MESSAGE_CREATE_DATAPARTITION_FAILED_BECAUSE_DATABASE_ARG_DOES_NOT_EXIST_D7A8C1FC = "创建 DataPartition 失败，原因：数据库 %s 不存在";
  public static final String LOG_REGIONGROUP_ARG_SERIESPARTITIONSLOT_COUNT_ARG_30F57B14 = "到 RegionGroup {}，SeriesPartitionSlot 数量：{}";
  public static final String LOG_REGIONGROUPID_ARG_SERIESPARTITIONSLOT_COUNT_ARG_5DAE4B6A = "RegionGroupId：{}，SeriesPartitionSlot 数量：{}";
  public static final String LOG_INCREASE_REFERENCE_COUNT_SNAPSHOT_ARG_ERROR_HOLDER_MESSAGE_ARG_962E8672 = "增加快照 %s 的引用计数失败。Holder 消息：%s";
  public static final String LOG_DECREASE_REFERENCE_COUNT_SNAPSHOT_ARG_ERROR_HOLDER_MESSAGE_ARG_8C7FF9CE = "减少快照 %s 的引用计数失败。Holder 消息：%s";
  public static final String MESSAGE_RECEIVER_CONFIGNODE_HAS_SET_UP_NEW_RECEIVER_SENDER_MUST_RE_77B80C51 =
      "接收端 ConfigNode 已建立新的 receiver，发送端必须重新发送 handshake 请求。";
  public static final String LOG_IGNORE_EXCEPTION_2AC431FA = "忽略此异常。";
  public static final String LOG_REPORTING_PIPE_META_ARG_REMAININGEVENTCOUNT_ARG_ESTIMATEDREMAININGTIME_ARG_E2727CB4 = "上报 pipe meta：{}，remainingEventCount：{}，estimatedRemainingTime：{}";
  public static final String LOG_PIPEMETAFROMAGENT_NULL_PIPEMETAFROMCOORDINATOR_ARG_36C513AE = "pipeMetaFromAgent 为空，pipeMetaFromCoordinator：{}";
  public static final String LOG_DETECTED_HISTORICAL_PIPE_COMPLETION_REPORT_DATANODE_ARG_PIPE_ARG_REMAININGEVENTCOUNT_7E6C52E9 =
      "检测到 DataNode {} 上 pipe {} 的历史 pipe 完成报告。remainingEventCount："
      + "{}，remainingTime：{}，completedDataNodes：{}";
  public static final String LOG_ALL_DATANODES_REPORTED_HISTORICAL_PIPE_ARG_COMPLETED_GLOBALREMAININGEVENTCOUNT_ARG_GLOBALREMAININGTIME_255 =
      "所有 DataNode 均报告历史 pipe {} 已完成。globalRemainingEventCount：{}，"
      + "globalRemainingTime：{}，staticMeta：{}";
  public static final String LOG_UPDATED_PROGRESS_INDEX_PIPE_NAME_ARG_CONSENSUS_GROUP_ID_ARG_DF112F4F = "已更新 (pipe 名称：{}，共识组 id：{}) 的进度索引 ... ";
  public static final String LOG_PROGRESS_INDEX_COORDINATOR_ARG_PROGRESS_INDEX_AGENT_ARG_UPDATED_PROGRESSINDEX_1A22ABC5 = "coordinator 上的进度索引：{}，agent 上的进度索引：{}，更新后的 progressIndex：{}";
  public static final String LOG_DETECT_PIPERUNTIMECONNECTORCRITICALEXCEPTION_ARG_7D198DD7 = "检测到 PipeRuntimeConnectorCriticalException %s ";
  public static final String LOG_AGENT_STOP_PIPE_ARG_42212C21 = "来自 agent，停止 pipe %s。";
  public static final String LOG_CREATEREGIONGROUPS_REGIONGROUP_ARG_BELONGED_DATABASE_ARG_DATANODES_ARG_5270AB6B = "[CreateRegionGroups] RegionGroup：{}，所属数据库：{}，DataNodes：{}";
  public static final String EXCEPTION_DATANODEID_SHOULD_NOT_BE_MINUS_1_HERE_5CB27796 = "dataNodeId 此处不应为 -1";
  public static final String MESSAGE_SUBSCRIPTIONOWNERLEASESYNCER_IS_STARTED_SUCCESSFULLY_09CA6848 = "SubscriptionOwnerLeaseSyncer 已成功启动。";
  public static final String MESSAGE_FAILED_TO_PUSH_SUBSCRIPTION_TOPIC_OWNER_LEASES_TO_DATANODES_EBFBA668 = "向 DataNodes 推送订阅 topic owner 租约失败。";
  public static final String MESSAGE_SUBSCRIPTIONOWNERLEASESYNCER_IS_STOPPED_SUCCESSFULLY_11442F29 = "SubscriptionOwnerLeaseSyncer 已成功停止。";
  public static final String MESSAGE_NO_AVAILABLE_ARG_REGIONGROUP_FOR_DATABASE_ARG_REGIONGROUPS_VISIBLE_IN_PARTITIONINFO_AND_THEIR_LOADCACHE_STATUS_ARG_615F5D49 =
      "数据库 {} 没有可用的 {} RegionGroup。PartitionInfo 中可见的 RegionGroup 及其 LoadCache 状态：{}";

}
