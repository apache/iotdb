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

public final class ProcedureMessages {

  public static final String NEVER_FINISH_PROCEDURE_RAN_AGAIN =
      "AddNeverFinishSubProcedureProcedure 再次运行，这种情况不应发生";
  public static final String ADDREGIONLOCATION_FINISHED_ADD_REGION_TO_RESULT_IS =
      "AddRegionLocation 完成，将 region {} 添加到 {}，结果为 {}";
  public static final String ADDTABLECOLUMN_COSTS_MS = "AddTableColumn-{}.{}-{} costs {}ms";
  public static final String ADD_COLUMN_TO_TABLE = "向表 {}.{} 添加列";
  public static final String ADD_CONFIGNODE_FAILED = "添加 ConfigNode 失败 ";
  public static final String ALTERCONSUMERGROUPPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES =
      "AlterConsumerGroupProcedure: executeFromOperateOnConfigNodes({})";
  public static final String ALTERCONSUMERGROUPPROCEDURE_EXECUTEFROMOPERATEONDATANODES =
      "AlterConsumerGroupProcedure: executeFromOperateOnDataNodes({})";
  public static final String ALTERCONSUMERGROUPPROCEDURE_EXECUTEFROMVALIDATE_TRY_TO_VALIDATE =
      "AlterConsumerGroupProcedure: executeFromValidate, try to validate";
  public static final String ALTERCONSUMERGROUPPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES =
      "AlterConsumerGroupProcedure: rollbackFromOperateOnConfigNodes({})";
  public static final String ALTERCONSUMERGROUPPROCEDURE_ROLLBACKFROMOPERATEONDATANODES =
      "AlterConsumerGroupProcedure: rollbackFromOperateOnDataNodes";
  public static final String ALTERCONSUMERGROUPPROCEDURE_ROLLBACKFROMVALIDATE =
      "AlterConsumerGroupProcedure: rollbackFromValidate";
  public static final String ALTERENCODINGCOMPRESSOR_COSTS_MS =
      "AlterEncodingCompressor-[{}] costs {}ms";
  public static final String ALTERING_COLUMN_IN_ON_CONFIGNODE =
      "在 ConfigNode 上修改表 {}.{} 中的列 {}";
  public static final String ALTERING_TIME_SERIES_DATA_TYPE = "正在修改时间序列 {} 的数据类型";
  public static final String ALTERLOGICALVIEW_COSTS_MS = "AlterLogicalView-[{}] costs {}ms";
  public static final String ALTERPIPEPROCEDUREV2_EXECUTEFROMCALCULATEINFOFORTASK =
      "AlterPipeProcedureV2: executeFromCalculateInfoForTask({})";
  public static final String ALTERPIPEPROCEDUREV2_EXECUTEFROMOPERATEONDATANODES =
      "AlterPipeProcedureV2: executeFromOperateOnDataNodes({})";
  public static final String ALTERPIPEPROCEDUREV2_EXECUTEFROMVALIDATETASK =
      "AlterPipeProcedureV2: executeFromValidateTask({})";
  public static final String ALTERPIPEPROCEDUREV2_EXECUTEFROMWRITECONFIGNODECONSENSUS =
      "AlterPipeProcedureV2: executeFromWriteConfigNodeConsensus({})";
  public static final String ALTERPIPEPROCEDUREV2_ROLLBACKFROMCALCULATEINFOFORTASK =
      "AlterPipeProcedureV2: rollbackFromCalculateInfoForTask({})";
  public static final String ALTERPIPEPROCEDUREV2_ROLLBACKFROMOPERATEONDATANODES =
      "AlterPipeProcedureV2: rollbackFromOperateOnDataNodes({})";
  public static final String ALTERPIPEPROCEDUREV2_ROLLBACKFROMVALIDATETASK =
      "AlterPipeProcedureV2: rollbackFromValidateTask({})";
  public static final String ALTERPIPEPROCEDUREV2_ROLLBACKFROMWRITECONFIGNODECONSENSUS =
      "AlterPipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})";
  public static final String ALTERTABLECOLUMNDATATYPE_COSTS_MS =
      "AlterTableColumnDataType-{}.{}-{} costs {}ms";
  public static final String ALTERTIMESERIESDATATYPE_COSTS_MS =
      "AlterTimeSeriesDataType-{}-[{}] costs {}ms";
  public static final String ALTERTOPICPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES_TRY_TO_ALTER_TOPIC =
      "AlterTopicProcedure: executeFromOperateOnConfigNodes, try to alter topic";
  public static final String ALTERTOPICPROCEDURE_EXECUTEFROMOPERATEONDATANODES =
      "AlterTopicProcedure: executeFromOperateOnDataNodes({})";
  public static final String ALTERTOPICPROCEDURE_EXECUTEFROMVALIDATE =
      "AlterTopicProcedure: executeFromValidate";
  public static final String ALTERTOPICPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES =
      "AlterTopicProcedure: rollbackFromOperateOnConfigNodes({})";
  public static final String ALTERTOPICPROCEDURE_ROLLBACKFROMOPERATEONDATANODES =
      "AlterTopicProcedure: rollbackFromOperateOnDataNodes({})";
  public static final String ALTERTOPICPROCEDURE_ROLLBACKFROMVALIDATE =
      "AlterTopicProcedure: rollbackFromValidate({})";
  public static final String ALTER_ENCODING_COMPRESSOR_IN_SCHEMA_REGIONS_FAILED_FAILURES =
      "在 schema region 中修改编码 %s 和压缩器失败。失败信息：%s";
  public static final String ALTER_ENCODING_COMPRESSOR_IN_SCHEMA_REGION_FOR_TIMESERIES =
      "在 schema region 中修改时间序列 {} 的编码 {} 和压缩器 {}";
  public static final String ALTER_TIMESERIES_DATA_TYPE_TO_IN_SCHEMA_REGIONS_FAILED_FAILURES =
      "在 schema region 中将时间序列 %s 的数据类型修改为 %s 失败。失败信息：%s";
  public static final String ALTER_TIME_SERIES_DATA_TYPE_FAILED =
      "修改时间序列 {} 数据类型失败";
  public static final String ALTER_VIEW = "修改视图 {}";
  public static final String ALTER_VIEW_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN_ALL =
      "修改视图 %s 失败，当 [%s] 时，原因：在 schemaRegion %s 的所有 replicaset 中执行失败。失败节点：%s，状态：%s";
  public static final String AUTHENTICATION_FAILED = "认证失败。";
  public static final String AUTH_PROCEDURE_CLEAN_DATANODE_CACHE_SUCCESSFULLY =
      "Auth procedure：成功清理 datanode 缓存";
  public static final String BEGIN_TO_CHANGE_DATANODE_STATUS_NODESTATUSMAP =
      "{}, 开始修改 DataNode 状态，nodeStatusMap：{}";
  public static final String BEGIN_TO_STOP_DATANODES_AND_KILL_THE_DATANODE_PROCESS =
      "{}, 开始停止 DataNode 并杀死 DataNode 进程：{}";
  public static final String BROADCASTDATANODESTATUSCHANGE_FINISHED_DATANODE =
      "{}, BroadcastDataNodeStatusChange 完成，dataNode：{}";
  public static final String BROADCASTDATANODESTATUSCHANGE_MEETS_ERROR_STATUS_CHANGE_DATANODES_ERROR_DATANODE =
      "{}, BroadcastDataNodeStatusChange 出错，状态变更的 DataNode：{}，出错的 DataNode：{}";
  public static final String BROADCASTDATANODESTATUSCHANGE_START_DATANODE =
      "{}, BroadcastDataNodeStatusChange 开始，dataNode：{}";
  public static final String CALL_CHANGEREGIONLEADER_FAIL_FOR_THE_TIME_WILL_SLEEP_MS =
      "第 {} 次调用 changeRegionLeader 失败，将休眠 {} 毫秒";
  public static final String CANNOT_FIND_DATANODES_CONTAIN_THE_GIVEN_REGION =
      "找不到包含给定 region 的 DataNode：{}";
  public static final String CANNOT_FIND_REGION_REPLICA_NODES_IN_CREATEPEER_REGIONID =
      "{}, 在 createPeer 中找不到 region 副本节点，regionId：{}";
  public static final String CANNOT_FIND_REGION_REPLICA_NODES_REGION =
      "找不到 region 副本节点，region：{}";
  public static final String CATCH_EXCEPTION_WHILE_DESERIALIZING_PROCEDURE_THIS_PROCEDURE_WILL_BE_IGNORED =
      "反序列化 procedure 时捕获异常，该 procedure 将被忽略。";
  public static final String CHANGE_REGION_LEADER_FINISHED_REGIONID_NEWLEADERNODE =
      "{}, 切换 region leader 完成，regionId：{}，newLeaderNode：{}";
  public static final String CHECK_AND_INVALIDATE_COLUMN_IN_WHEN_ALTERING_COLUMN_DATA_TYPE =
      "修改列数据类型时检查并使表 {}.{} 中的列 {} 缓存失效";
  public static final String CHECK_AND_INVALIDATE_COLUMN_IN_WHEN_DROPPING_COLUMN =
      "删除列时检查并使表 {}.{} 中的列 {} 缓存失效";
  public static final String CHECK_AND_INVALIDATE_SERIES_WHEN_ALTERING_TIME_SERIES_DATA_TYPE =
      "修改时间序列数据类型时检查并使序列 {} 缓存失效";
  public static final String CHECK_AND_INVALIDATE_TABLE_WHEN_DROPPING_TABLE =
      "删除表时检查并使表 {}.{} 缓存失效";
  public static final String CHECK_DATANODE_TEMPLATE_ACTIVATION_OF_TEMPLATE_SET_ON =
      "检查 DataNode 上模板 {}（设置在 {} 上）的激活情况";
  public static final String CHECK_TEMPLATE_EXISTENCE_SET_ON_PATH_WHEN_TRY_SETTING_TEMPLATE =
      "尝试设置模板 {} 时检查路径 {} 上已设置的模板是否存在";
  public static final String CHECK_THE_EXISTENCE_OF_TABLE = "检查表 {}.{} 是否存在";
  public static final String CHECK_TIMESERIES_EXISTENCE_UNDER_PATH_WHEN_TRY_SETTING_TEMPLATE =
      "尝试设置模板 {} 时检查路径 {} 下是否存在时间序列";
  public static final String CLEARING_CACHE_AFTER_ALTER_TIME_SERIES_DATA_TYPE =
      "修改时间序列 {} 数据类型后清理缓存";
  public static final String COLUMN_CHECK_FOR_TABLE_WHEN_ADDING_COLUMN =
      "添加列时对表 {}.{} 进行列检查";
  public static final String COLUMN_CHECK_FOR_TABLE_WHEN_RENAMING_COLUMN =
      "重命名列时对表 {}.{} 进行列检查";
  public static final String COLUMN_CHECK_FOR_TABLE_WHEN_RENAMING_TABLE =
      "重命名表时对表 {}.{} 进行列检查";
  public static final String COMMIT_CREATE_TABLE = "提交创建表 {}.{}";
  public static final String COMMIT_RELEASE_INFO_OF_TABLE_WHEN_ADDING_COLUMN =
      "添加列时提交表 {}.{} 的释放信息";
  public static final String COMMIT_RELEASE_INFO_OF_TABLE_WHEN_ALTERING_COLUMN =
      "修改列时提交表 {}.{} 的释放信息";
  public static final String COMMIT_RELEASE_INFO_OF_TABLE_WHEN_RENAMING_COLUMN =
      "重命名列时提交表 {}.{} 的释放信息";
  public static final String COMMIT_RELEASE_INFO_OF_TABLE_WHEN_RENAMING_TABLE =
      "重命名表时提交表 {}.{} 的释放信息";
  public static final String COMMIT_RELEASE_INFO_OF_TABLE_WHEN_SETTING_PROPERTIES =
      "设置属性时提交表 {}.{} 的释放信息";
  public static final String COMMIT_RELEASE_SCHEMAENGINE_TEMPLATE_SET_ON_PATH =
      "提交释放在路径 {} 上设置的 schemaengine 模板 {}";
  public static final String COMMIT_RELEASE_TABLE = "提交释放表 {}.{}";
  public static final String COMMIT_SET_SCHEMAENGINE_TEMPLATE_ON_PATH =
      "提交在路径 {} 上设置 schemaengine 模板 {}";
  public static final String CONSENSUSPIPEGUARDIAN_CONSENSUS_PIPE_IS_STOPPED_RESTARTING_ASYNCHRONOUSLY =
      "[ConsensusPipeGuardian] 共识 pipe [{}] 已停止，正在异步重启";
  public static final String CONSENSUSPIPEGUARDIAN_CONSENSUS_PIPE_MISSING_CREATING_ASYNCHRONOUSLY =
      "[ConsensusPipeGuardian] 共识 pipe [{}] 缺失，正在异步创建";
  public static final String CONSENSUSPIPEGUARDIAN_UNEXPECTED_CONSENSUS_PIPE_EXISTS_DROPPING_ASYNCHRONOUSLY =
      "[ConsensusPipeGuardian] 存在非预期的共识 pipe [{}]，正在异步删除";
  public static final String CONSTRUCT_SCHEMAENGINE_BLACK_LIST_OF_DEVICES_IN =
      "构建 {}.{} 中设备的 schemaEngine 黑名单";
  public static final String CONSTRUCT_SCHEMAENGINE_BLACK_LIST_OF_TEMPLATE_SET_ON =
      "构建模板 {}（设置在 {} 上）的 schemaengine 黑名单";
  public static final String CONSTRUCT_SCHEMAENGINE_BLACK_LIST_OF_TIMESERIES =
      "构建时间序列 {} 的 schemaEngine 黑名单";
  public static final String CONSTRUCT_SCHEMA_BLACK_LIST_WITH_TEMPLATE =
      "使用模板 {} 构建 schema 黑名单";
  public static final String CONSTRUCT_VIEW_SCHEMAENGINE_BLACK_LIST_OF_VIEW =
      "构建视图 {} 的 schemaengine 黑名单";
  public static final String CONSUMERGROUPMETASYNCPROCEDURE_ACQUIRELOCK_SKIP_THE_PROCEDURE_DUE_TO =
      "ConsumerGroupMetaSyncProcedure: acquireLock, skip the procedure due to the last execution time {}";
  public static final String CONSUMERGROUPMETASYNCPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES =
      "ConsumerGroupMetaSyncProcedure: executeFromOperateOnConfigNodes";
  public static final String CONSUMERGROUPMETASYNCPROCEDURE_EXECUTEFROMOPERATEONDATANODES =
      "ConsumerGroupMetaSyncProcedure: executeFromOperateOnDataNodes";
  public static final String CONSUMERGROUPMETASYNCPROCEDURE_EXECUTEFROMVALIDATE =
      "ConsumerGroupMetaSyncProcedure: executeFromValidate";
  public static final String CONSUMERGROUPMETASYNCPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES =
      "ConsumerGroupMetaSyncProcedure: rollbackFromOperateOnConfigNodes";
  public static final String CONSUMERGROUPMETASYNCPROCEDURE_ROLLBACKFROMOPERATEONDATANODES =
      "ConsumerGroupMetaSyncProcedure: rollbackFromOperateOnDataNodes";
  public static final String CONSUMERGROUPMETASYNCPROCEDURE_ROLLBACKFROMVALIDATE =
      "ConsumerGroupMetaSyncProcedure: rollbackFromValidate";
  public static final String CREATEDATABASE_FAIL_TWICE = "createDatabase 失败两次";
  public static final String CREATED_CONSENSUS_PIPE = "{}, 已创建共识 pipe {}";
  public static final String CREATEPIPEPLUGINPROCEDURE_EXECUTEFROMCREATEONCONFIGNODES =
      "CreatePipePluginProcedure: executeFromCreateOnConfigNodes({})";
  public static final String CREATEPIPEPLUGINPROCEDURE_EXECUTEFROMCREATEONDATANODES =
      "CreatePipePluginProcedure: executeFromCreateOnDataNodes({})";
  public static final String CREATEPIPEPLUGINPROCEDURE_EXECUTEFROMLOCK =
      "CreatePipePluginProcedure: executeFromLock({})";
  public static final String CREATEPIPEPLUGINPROCEDURE_EXECUTEFROMUNLOCK =
      "CreatePipePluginProcedure: executeFromUnlock({})";
  public static final String CREATEPIPEPLUGINPROCEDURE_FAILED_IN_STATE_WILL_ROLLBACK =
      "CreatePipePluginProcedure 在状态 {} 处失败，将回滚";
  public static final String CREATEPIPEPLUGINPROCEDURE_ROLLBACKFROMCREATEONCONFIGNODES =
      "CreatePipePluginProcedure: rollbackFromCreateOnConfigNodes({})";
  public static final String CREATEPIPEPLUGINPROCEDURE_ROLLBACKFROMCREATEONDATANODES =
      "CreatePipePluginProcedure: rollbackFromCreateOnDataNodes({})";
  public static final String CREATEPIPEPLUGINPROCEDURE_ROLLBACKFROMLOCK =
      "CreatePipePluginProcedure: rollbackFromLock({})";
  public static final String CREATEPIPEPROCEDUREV2_EXECUTEFROMCALCULATEINFOFORTASK =
      "CreatePipeProcedureV2: executeFromCalculateInfoForTask({})";
  public static final String CREATEPIPEPROCEDUREV2_EXECUTEFROMOPERATEONDATANODES =
      "CreatePipeProcedureV2: executeFromOperateOnDataNodes({})";
  public static final String CREATEPIPEPROCEDUREV2_EXECUTEFROMVALIDATETASK =
      "CreatePipeProcedureV2: executeFromValidateTask({})";
  public static final String CREATEPIPEPROCEDUREV2_EXECUTEFROMWRITECONFIGNODECONSENSUS =
      "CreatePipeProcedureV2: executeFromWriteConfigNodeConsensus({})";
  public static final String CREATEPIPEPROCEDUREV2_ROLLBACKFROMCALCULATEINFOFORTASK =
      "CreatePipeProcedureV2: rollbackFromCalculateInfoForTask({})";
  public static final String CREATEPIPEPROCEDUREV2_ROLLBACKFROMOPERATEONDATANODES =
      "CreatePipeProcedureV2: rollbackFromOperateOnDataNodes({})";
  public static final String CREATEPIPEPROCEDUREV2_ROLLBACKFROMVALIDATETASK =
      "CreatePipeProcedureV2: rollbackFromValidateTask({})";
  public static final String CREATEPIPEPROCEDUREV2_ROLLBACKFROMWRITECONFIGNODECONSENSUS =
      "CreatePipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})";
  public static final String CREATEREGIONGROUPS_ALL_REPLICAS_OF_REGIONGROUP_ARE_CREATED_SUCCESSFULLY =
      "[CreateRegionGroups] RegionGroup：{} 的所有副本均已成功创建！";
  public static final String CREATEREGIONGROUPS_FAILED_TO_CREATE_MOST_OF_REPLICAS_IN_REGIONGROUP_THE =
      "[CreateRegionGroups] RegionGroup：{} 中大多数副本创建失败，该 RegionGroup 中的冗余副本将被删除。";
  public static final String CREATEREGIONGROUPS_FAILED_TO_CREATE_SOME_REPLICAS_OF_REGIONGROUP_BUT_THIS =
      "[CreateRegionGroups] RegionGroup：{} 中部分副本创建失败，但该 RegionGroup 仍可使用。";
  public static final String CREATESUBSCRIPTIONPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES =
      "CreateSubscriptionProcedure: executeFromOperateOnConfigNodes";
  public static final String CREATESUBSCRIPTIONPROCEDURE_EXECUTEFROMOPERATEONDATANODES =
      "CreateSubscriptionProcedure: executeFromOperateOnDataNodes";
  public static final String CREATESUBSCRIPTIONPROCEDURE_EXECUTEFROMVALIDATE =
      "CreateSubscriptionProcedure: executeFromValidate";
  public static final String CREATESUBSCRIPTIONPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES =
      "CreateSubscriptionProcedure: rollbackFromOperateOnConfigNodes";
  public static final String CREATESUBSCRIPTIONPROCEDURE_ROLLBACKFROMOPERATEONDATANODES =
      "CreateSubscriptionProcedure: rollbackFromOperateOnDataNodes";
  public static final String CREATESUBSCRIPTIONPROCEDURE_ROLLBACKFROMVALIDATE =
      "CreateSubscriptionProcedure: rollbackFromValidate";
  public static final String CREATETABLE_COSTS_MS = "CreateTable-{}.{}-{} costs {}ms";
  public static final String CREATETOPICPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES =
      "CreateTopicProcedure: executeFromOperateOnConfigNodes({})";
  public static final String CREATETOPICPROCEDURE_EXECUTEFROMOPERATEONDATANODES =
      "CreateTopicProcedure: executeFromOperateOnDataNodes({})";
  public static final String CREATETOPICPROCEDURE_EXECUTEFROMVALIDATE =
      "CreateTopicProcedure: executeFromValidate";
  public static final String CREATETOPICPROCEDURE_ROLLBACKFROMCREATEONCONFIGNODES =
      "CreateTopicProcedure: rollbackFromCreateOnConfigNodes({})";
  public static final String CREATETOPICPROCEDURE_ROLLBACKFROMCREATEONDATANODES =
      "CreateTopicProcedure: rollbackFromCreateOnDataNodes({})";
  public static final String CREATETOPICPROCEDURE_ROLLBACKFROMVALIDATE =
      "CreateTopicProcedure: rollbackFromValidate({})";
  public static final String DATANODE_IS_SUBMIT_DELETE_OLD_REGION_PEER_WITH_A_SINGLE =
      "{}, DataNode {} 状态为 {}，提交 DELETE_OLD_REGION_PEER（单次 RPC 尝试），由 RemoveRegionPeerProcedure 处理重试。";
  public static final String DEACTIVATETEMPLATE_COSTS_MS = "DeactivateTemplate-[{}] costs {}ms";
  public static final String DEACTIVATE_TEMPLATE_OF = "停用 {} 的模板";
  public static final String DEACTIVATE_TEMPLATE_OF_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN =
      "停用 %s 的模板失败，当 [%s] 时，原因：在所有 replicaset（%s %s）中执行失败。失败信息：%s";
  public static final String DELETEDATABASEPROCEDURE_DELETE_DATABASE =
      "[DeleteDatabaseProcedure] 删除数据库 ";
  public static final String DELETEDATABASEPROCEDURE_DELETE_DATABASESCHEMA_FAILED =
      "[DeleteDatabaseProcedure] 删除 DatabaseSchema 失败";
  public static final String DELETEDATABASEPROCEDURE_INVALIDATE_CACHE_FAILED =
      "[DeleteDatabaseProcedure] 使缓存失效失败";
  public static final String DELETEDATABASEPROCEDURE_STATE_STUCK_AT =
      "[DeleteDatabaseProcedure] 状态卡在 ";
  public static final String DELETEDEVICES_COSTS_MS = "DeleteDevices-[{}] costs {}ms";
  public static final String DELETELOGICALVIEW_COSTS_MS = "DeleteLogicalView-[{}] costs {}ms";
  public static final String DELETETIMESERIES_COSTS_MS = "DeleteTimeSeries-[{}] costs {}ms";
  public static final String DELETE_DATA_OF_DEVICES_IN = "删除 {}.{} 中设备的数据";
  public static final String DELETE_DATA_OF_TEMPLATE_TIMESERIES =
      "删除模板时间序列 {} 的数据";
  public static final String DELETE_DATA_OF_TIMESERIES = "删除时间序列 {} 的数据";
  public static final String DELETE_DEVICES_IN_IN_SCHEMAENGINE =
      "在 schemaEngine 中删除 {}.{} 中的设备";
  public static final String DELETE_TIMESERIES_SCHEMAENGINE_OF =
      "删除时间序列 {} 的 schemaEngine";
  public static final String DELETE_TIME_SERIES_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN =
      "删除时间序列 %s 失败，当 [%s] 时，原因：在所有 replicaset（%s %s）中执行失败。失败信息：%s";
  public static final String DELETE_VIEW_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN_ALL =
      "删除视图 %s 失败，当 [%s] 时，原因：在 schemaRegion %s 的所有 replicaset 中执行失败。失败信息：%s";
  public static final String DELETE_VIEW_SCHEMAENGINE_OF = "删除视图 {} 的 schemaengine";
  public static final String DELETING_DATA_FOR_TABLE = "正在删除表 {}.{} 的数据";
  public static final String DELETING_DEVICES_FOR_TABLE_WHEN_DROPPING_TABLE =
      "删除表时正在删除表 {}.{} 的设备";
  public static final String DESERIALIZE_MEETS_ERROR_IN_CREATEREGIONGROUPSPROCEDURE =
      "在 CreateRegionGroupsProcedure 中反序列化出错";
  public static final String DROPPING_COLUMN_IN_ON_CONFIGNODE =
      "在 ConfigNode 上删除表 {}.{} 中的列 {}";
  public static final String DROPPING_TABLE_ON_CONFIGNODE = "在 ConfigNode 上删除表 {}.{}";
  public static final String DROPPIPEPLUGINPROCEDURE_EXECUTEFROMDROPONCONFIGNODES =
      "DropPipePluginProcedure: executeFromDropOnConfigNodes({})";
  public static final String DROPPIPEPLUGINPROCEDURE_EXECUTEFROMDROPONDATANODES =
      "DropPipePluginProcedure: executeFromDropOnDataNodes({})";
  public static final String DROPPIPEPLUGINPROCEDURE_EXECUTEFROMLOCK =
      "DropPipePluginProcedure: executeFromLock({})";
  public static final String DROPPIPEPLUGINPROCEDURE_EXECUTEFROMUNLOCK =
      "DropPipePluginProcedure: executeFromUnlock({})";
  public static final String DROPPIPEPLUGINPROCEDURE_FAILED_IN_STATE_WILL_ROLLBACK =
      "DropPipePluginProcedure 在状态 {} 处失败，将回滚";
  public static final String DROPPIPEPLUGINPROCEDURE_ROLLBACKFROMDROPONCONFIGNODES =
      "DropPipePluginProcedure: rollbackFromDropOnConfigNodes({})";
  public static final String DROPPIPEPLUGINPROCEDURE_ROLLBACKFROMDROPONDATANODES =
      "DropPipePluginProcedure: rollbackFromDropOnDataNodes({})";
  public static final String DROPPIPEPLUGINPROCEDURE_ROLLBACKFROMLOCK =
      "DropPipePluginProcedure: rollbackFromLock({})";
  public static final String DROPPIPEPROCEDUREV2_EXECUTEFROMCALCULATEINFOFORTASK =
      "DropPipeProcedureV2: executeFromCalculateInfoForTask({})";
  public static final String DROPPIPEPROCEDUREV2_EXECUTEFROMOPERATEONDATANODES =
      "DropPipeProcedureV2: executeFromOperateOnDataNodes({})";
  public static final String DROPPIPEPROCEDUREV2_EXECUTEFROMVALIDATETASK =
      "DropPipeProcedureV2: executeFromValidateTask({})";
  public static final String DROPPIPEPROCEDUREV2_EXECUTEFROMWRITECONFIGNODECONSENSUS =
      "DropPipeProcedureV2: executeFromWriteConfigNodeConsensus({})";
  public static final String DROPPIPEPROCEDUREV2_ROLLBACKFROMCALCULATEINFOFORTASK =
      "DropPipeProcedureV2: rollbackFromCalculateInfoForTask({})";
  public static final String DROPPIPEPROCEDUREV2_ROLLBACKFROMOPERATEONDATANODES =
      "DropPipeProcedureV2: rollbackFromOperateOnDataNodes({})";
  public static final String DROPPIPEPROCEDUREV2_ROLLBACKFROMVALIDATETASK =
      "DropPipeProcedureV2: rollbackFromValidateTask({})";
  public static final String DROPPIPEPROCEDUREV2_ROLLBACKFROMWRITECONFIGNODECONSENSUS =
      "DropPipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})";
  public static final String DROPSUBSCRIPTIONPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES =
      "DropSubscriptionProcedure: executeFromOperateOnConfigNodes";
  public static final String DROPSUBSCRIPTIONPROCEDURE_EXECUTEFROMOPERATEONDATANODES =
      "DropSubscriptionProcedure: executeFromOperateOnDataNodes";
  public static final String DROPSUBSCRIPTIONPROCEDURE_EXECUTEFROMVALIDATE =
      "DropSubscriptionProcedure: executeFromValidate";
  public static final String DROPSUBSCRIPTIONPROCEDURE_ROLLBACKFROMLOCK =
      "DropSubscriptionProcedure: rollbackFromLock";
  public static final String DROPSUBSCRIPTIONPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES =
      "DropSubscriptionProcedure: rollbackFromOperateOnConfigNodes";
  public static final String DROPSUBSCRIPTIONPROCEDURE_ROLLBACKFROMOPERATEONDATANODES =
      "DropSubscriptionProcedure: rollbackFromOperateOnDataNodes";
  public static final String DROPTABLECOLUMN_COSTS_MS = "DropTableColumn-{}.{}-{} costs {}ms";
  public static final String DROPTABLE_COSTS_MS = "DropTable-{}.{}-{} costs {}ms";
  public static final String DROPTOPICPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES =
      "DropTopicProcedure: executeFromOperateOnConfigNodes({})";
  public static final String DROPTOPICPROCEDURE_EXECUTEFROMOPERATEONDATANODES =
      "DropTopicProcedure: executeFromOperateOnDataNodes({})";
  public static final String DROPTOPICPROCEDURE_EXECUTEFROMVALIDATE =
      "DropTopicProcedure: executeFromValidate({})";
  public static final String DROPTOPICPROCEDURE_ROLLBACKFROMCREATEONCONFIGNODES =
      "DropTopicProcedure: rollbackFromCreateOnConfigNodes({})";
  public static final String DROPTOPICPROCEDURE_ROLLBACKFROMCREATEONDATANODES =
      "DropTopicProcedure: rollbackFromCreateOnDataNodes({})";
  public static final String DROPTOPICPROCEDURE_ROLLBACKFROMVALIDATE =
      "DropTopicProcedure: rollbackFromValidate({})";
  public static final String ERROR_IN_DESERIALIZE = "反序列化 {} 出错";
  public static final String ERROR_IN_DESERIALIZE_PROCID_THIS_PROCEDURE_WILL_BE_IGNORED_IT =
      "反序列化 {}（procID {}）出错。该 procedure 将被忽略。它可能属于旧版本，目前无法使用。";
  public static final String EXECUTE_AUTH_PLAN_SUCCESS_TO_INVALIDATE_DATANODES =
      "执行 auth plan {} 成功。使 datanode 缓存失效：{}";
  public static final String EXECUTING_ON_REGION_FOR_COLUMN_IN_WHEN_DROPPING_COLUMN =
      "删除列时在表 {}.{} 中列 {} 对应的 region 上执行";
  public static final String FAILED_TO_ACTIVE_CQ_BECAUSE_OF_NO_SUCH_CQ =
      "激活 CQ {} 失败，原因是没有这样的 CQ：{}";
  public static final String FAILED_TO_ACTIVE_CQ_BECAUSE_THIS_CQ_HAS_ALREADY_BEEN =
      "激活 CQ {} 失败，原因是该 CQ 已处于激活状态";
  public static final String FAILED_TO_ACTIVE_CQ_SUCCESSFULLY_BECAUSE_OF_UNKNOWN_REASONS =
      "因未知原因 {}，激活 CQ {} 失败";
  public static final String FAILED_TO_ALTER_CONSUMER_GROUP_ON_CONFIG_NODES_BECAUSE =
      "在 ConfigNode 上修改 consumer group %s 失败，原因：%s";
  public static final String FAILED_TO_ALTER_CONSUMER_GROUP_ON_DATA_NODES_BECAUSE =
      "在 DataNode 上修改 consumer group（%s -> %s）失败，原因：%s";
  public static final String FAILED_TO_ALTER_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER =
      "修改 pipe {} 失败，详情：{}，元数据将稍后同步。";
  public static final String FAILED_TO_ALTER_TOPIC_ON_CONFIG_NODES_BECAUSE =
      "在 ConfigNode 上修改 topic（%s -> %s）失败，原因：%s";
  public static final String FAILED_TO_ALTER_TOPIC_ON_DATA_NODES_BECAUSE =
      "在 DataNode 上修改 topic（%s -> %s）失败，原因：%s";
  public static final String FAILED_TO_CHANGE_DATANODE_STATUS_DATANODEID_NODESTATUS =
      "{}, 修改 DataNode 状态失败，dataNodeId={}，nodeStatus={}";
  public static final String FAILED_TO_COMMIT_SET_TEMPLATE_ON_PATH_DUE_TO =
      "在路径 {} 上提交设置模板 {} 失败，原因：{}";
  public static final String FAILED_TO_CREATE_CONSENSUS_PIPE =
      "{}, 创建共识 pipe {} 失败：{}";
  public static final String FAILED_TO_CREATE_PIPES_WHEN_CREATING_SUBSCRIPTION_WITH_REQUEST_DETAILS =
      "使用请求 %s 创建订阅时创建 pipes %s 失败，详情：%s，元数据将稍后同步。";
  public static final String FAILED_TO_CREATE_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER =
      "创建 pipe {} 失败，详情：{}，元数据将稍后同步。";
  public static final String FAILED_TO_CREATE_PIPE_PLUGIN_INSTANCE_ON_DATA_NODES =
      "在 DataNode 上创建 pipe plugin 实例 [%s] 失败";
  public static final String FAILED_TO_CREATE_SUBSCRIPTION_WITH_REQUEST_ON_CONFIG_NODES_BECAUSE =
      "在 ConfigNode 上使用请求 %s 创建订阅失败，原因：%s";
  public static final String FAILED_TO_CREATE_TOPIC_ON_CONFIG_NODES_BECAUSE =
      "在 ConfigNode 上创建 topic %s 失败，原因：%s";
  public static final String FAILED_TO_CREATE_TOPIC_ON_DATA_NODES_BECAUSE =
      "在 DataNode 上创建 topic %s 失败，原因：%s";
  public static final String FAILED_TO_DESERIALIZE_DATAPARTITIONTABLES =
      "反序列化 dataPartitionTables 失败";
  public static final String FAILED_TO_DESERIALIZE_FINALDATAPARTITIONTABLES =
      "反序列化 finalDataPartitionTables 失败";
  public static final String FAILED_TO_DO_INACTIVE_ROLLBACK_OF_CQ_BECAUSE_OF_NO =
      "对 CQ {} 执行 [INACTIVE] 回滚失败，原因是没有这样的 CQ：{}";
  public static final String FAILED_TO_DO_INACTIVE_ROLLBACK_OF_CQ_BECAUSE_OF_UNKNOWN =
      "对 CQ {} 执行 [INACTIVE] 回滚失败，原因：未知原因 {}";
  public static final String FAILED_TO_DROP_PIPES_WHEN_DROPPING_SUBSCRIPTION_WITH_REQUEST_BECAUSE =
      "使用请求 %s 删除订阅时删除 pipes %s 失败，原因：%s";
  public static final String FAILED_TO_DROP_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER =
      "删除 pipe {} 失败，详情：{}，元数据将稍后同步。";
  public static final String FAILED_TO_DROP_PIPE_PLUGIN_ON_DATA_NODES =
      "在 DataNode 上删除 pipe plugin %s 失败";
  public static final String FAILED_TO_DROP_SUBSCRIPTION_WITH_REQUEST_ON_CONFIG_NODES_BECAUSE =
      "在 ConfigNode 上使用请求 %s 删除订阅失败，原因：%s";
  public static final String FAILED_TO_DROP_TOPIC_ON_CONFIG_NODES_BECAUSE =
      "在 ConfigNode 上删除 topic %s 失败，原因：%s";
  public static final String FAILED_TO_DROP_TOPIC_ON_DATA_NODES_BECAUSE =
      "在 DataNode 上删除 topic %s 失败，原因：%s";
  public static final String FAILED_TO_EXECUTE_IN_ALL_REPLICASET_OF_SCHEMAREGION_WHEN_CHECKING =
      "在路径 %s 上检查模板时，在 schemaRegion %s 的所有 replicaset 中执行失败。失败信息：%s";
  public static final String FAILED_TO_EXECUTE_IN_ALL_REPLICASET_OF_SCHEMAREGION_WHEN_CHECKING_2 =
      "在 %s 上检查模板 %s 时，在 schemaRegion %s 的所有 replicaset 中执行失败。失败节点：%s";
  public static final String FAILED_TO_EXECUTE_PLAN_BECAUSE =
      "执行 plan {} 失败，原因：{}";
  public static final String FAILED_TO_FOR_TABLE_TO_DATANODE_FAILURE_RESULTS =
      "对表 {}.{} 在 DataNode 上执行 {} 失败，失败结果：{}";
  public static final String FAILED_TO_INIT_CQ_BECAUSE_OF_UNKNOWN_REASONS =
      "初始化 CQ {} 失败，原因：未知原因 {}";
  public static final String FAILED_TO_INIT_CQ_BECAUSE_SUCH_CQ_ALREADY_EXISTS =
      "初始化 CQ {} 失败，原因：该 CQ 已存在";
  public static final String FAILED_TO_INVALIDATE_COLUMN_S_CACHE_OF_TABLE =
      "使表 {}.{} 的 {} 列 {} 缓存失效失败";
  public static final String FAILED_TO_INVALIDATE_SCHEMAENGINE_CACHE_OF_DEVICES_IN_TABLE =
      "使表 {}.{} 中设备的 schemaEngine 缓存失效失败";
  public static final String FAILED_TO_INVALIDATE_SCHEMAENGINE_CACHE_OF_TABLE =
      "使表 {}.{} 的 schemaEngine 缓存失效失败";
  public static final String FAILED_TO_INVALIDATE_SCHEMAENGINE_CACHE_OF_TIMESERIES =
      "使时间序列 {} 的 schemaEngine 缓存失效失败";
  public static final String FAILED_TO_INVALIDATE_SCHEMAENGINE_CACHE_OF_VIEW =
      "使视图 {} 的 schemaengine 缓存失效失败";
  public static final String FAILED_TO_INVALIDATE_SCHEMA_CACHE_OF_TEMPLATE_TIMESERIES =
      "使模板时间序列 {} 的 schema 缓存失效失败";
  public static final String FAILED_TO_INVALIDATE_TEMPLATE_CACHE_OF_TEMPLATE_SET_ON =
      "使模板 {}（设置在 {} 上）的模板缓存失效失败";
  public static final String FAILED_TO_PRE_RELEASE_FOR_TABLE_TO_DATANODE_FAILURE_RESULTS =
      "对表 {}.{} 在 DataNode 上预释放 {} 失败，失败结果：{}";
  public static final String FAILED_TO_PRE_SET_TEMPLATE_ON_PATH_DUE_TO =
      "在路径 {} 上预设置模板 {} 失败，原因：{}";
  public static final String FAILED_TO_PUSH_CONSUMER_GROUP_META_TO_DATANODES_DETAILS =
      "向 DataNode 推送 consumer group 元数据失败，详情：%s";
  public static final String FAILED_TO_PUSH_PIPE_META_LIST_TO_DATA_NODES_WILL =
      "向 DataNode 推送 pipe 元数据列表失败，将稍后重试。";
  public static final String FAILED_TO_PUSH_PIPE_META_TO_DATANODES_DETAILS =
      "向 DataNode 推送 pipe 元数据失败，详情：%s";
  public static final String FAILED_TO_PUSH_TOPIC_META_TO_DATANODES_DETAILS =
      "向 DataNode 推送 topic 元数据失败，详情：%s";
  public static final String FAILED_TO_REMOVE_DATA_NODE_BECAUSE_IT_IS_NOT_IN =
      "移除 DataNode {} 失败，原因：该节点未在运行，且集群配置为单副本";

  public static final String FAILED_TO_REMOVE_DATA_NODE_WOULD_LEAVE_TOO_FEW =
      "无法移除 %d 个 DataNode：集群当前有 %d 个可用 DataNode，且至少需保留 %d 个（max(schema_replication_factor=%d, data_replication_factor=%d)），以保证每个 Region 仍有足够的副本；但本次请求执行后将只剩 %d 个。";
  public static final String FAILED_TO_REMOVE_DATA_NODE_SINGLE_REPLICA_HINT =
      " 单副本下没有其它节点可供迁移 Region，因此必须始终保留至少一个 DataNode。";
  public static final String FAILED_TO_ROLLBACK_ALTER_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED =
      "回滚修改 pipe {} 失败，详情：{}，元数据将稍后同步。";
  public static final String FAILED_TO_ROLLBACK_COMMIT_SET_TEMPLATE_ON_PATH_DUE_TO =
      "回滚在路径 {} 上提交的模板 {} 设置失败，原因：{}";
  public static final String FAILED_TO_ROLLBACK_CREATE_PIPES_WHEN_CREATING_SUBSCRIPTION_WITH_REQUEST =
      "使用请求 %s 创建订阅时回滚创建 pipes 失败，原因：%s";
  public static final String FAILED_TO_ROLLBACK_CREATE_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED =
      "回滚创建 pipe {} 失败，详情：{}，元数据将稍后同步。";
  public static final String FAILED_TO_ROLLBACK_CREATING_SUBSCRIPTION_WITH_REQUEST_ON_CONFIG_NODES =
      "在 ConfigNode 上回滚使用请求 %s 创建订阅失败，原因：%s";
  public static final String FAILED_TO_ROLLBACK_CREATING_TOPIC_ON_CONFIG_NODES_BECAUSE =
      "在 ConfigNode 上回滚创建 topic %s 失败，原因：%s";
  public static final String FAILED_TO_ROLLBACK_CREATING_TOPIC_ON_DATA_NODES_BECAUSE =
      "在 DataNode 上回滚创建 topic %s 失败，原因：%s";
  public static final String FAILED_TO_ROLLBACK_FROM_ALTERING_CONSUMER_GROUP_ON_CONFIG_NODES =
      "在 ConfigNode 上回滚修改 consumer group（%s -> %s）失败，原因：%s";
  public static final String FAILED_TO_ROLLBACK_FROM_ALTERING_CONSUMER_GROUP_ON_DATA_NODES =
      "在 DataNode 上回滚修改 consumer group（%s -> %s）失败，原因：%s";
  public static final String FAILED_TO_ROLLBACK_FROM_ALTERING_TOPIC_ON_CONFIG_NODES_BECAUSE =
      "在 ConfigNode 上回滚修改 topic（%s -> %s）失败，原因：%s";
  public static final String FAILED_TO_ROLLBACK_FROM_ALTERING_TOPIC_ON_DATA_NODES_BECAUSE =
      "在 DataNode 上回滚修改 topic（%s -> %s）失败，原因：%s";
  public static final String FAILED_TO_ROLLBACK_PIPE_PLUGIN_ON_DATA_NODES =
      "在 DataNode 上回滚 pipe plugin [%s] 失败";
  public static final String FAILED_TO_ROLLBACK_PRE_RELEASE_FOR_TABLE_INFO_TO_DATANODE =
      "回滚对表 {}.{} 在 DataNode 上预释放 {} 的信息失败，失败结果：{}";
  public static final String FAILED_TO_ROLLBACK_PRE_RELEASE_TEMPLATE_INFO_OF_TEMPLATE_SET =
      "回滚 DataNode {} 上路径 {} 处模板 {} 的预释放模板信息失败";
  public static final String FAILED_TO_ROLLBACK_PRE_SET_TEMPLATE_ON_PATH_DUE_TO =
      "回滚在路径 {} 上预设置的模板 {} 失败，原因：{}";
  public static final String FAILED_TO_ROLLBACK_PRE_UNSET_TEMPLATE_OPERATION_OF_TEMPLATE_SET =
      "回滚模板 {}（设置在 {} 上）的预取消设置模板操作失败";
  public static final String FAILED_TO_ROLLBACK_START_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED =
      "回滚启动 pipe {} 失败，详情：{}，元数据将稍后同步。";
  public static final String FAILED_TO_ROLLBACK_STOP_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED =
      "回滚停止 pipe {} 失败，详情：{}，元数据将稍后同步。";
  public static final String FAILED_TO_ROLLBACK_TABLE_CREATION =
      "回滚表 {} 的创建失败。{}";
  public static final String FAILED_TO_ROLLBACK_TEMPLATE_CACHE_OF_TEMPLATE_SET_ON =
      "回滚模板 {}（设置在 {} 上）的模板缓存失败";
  public static final String FAILED_TO_SERIALIZE_DATAPARTITIONTABLES =
      "序列化 dataPartitionTables 失败";
  public static final String FAILED_TO_SERIALIZE_FAILEDDATANODE =
      "序列化 failedDataNode 失败";
  public static final String FAILED_TO_SERIALIZE_FINALDATAPARTITIONTABLES =
      "序列化 finalDataPartitionTables 失败";
  public static final String FAILED_TO_SERIALIZE_SKIPDATANODE = "序列化 skipDataNode 失败";
  public static final String FAILED_TO_SET_SCHEMAENGINE_TEMPLATE_ON_PATH_BECAUSE_THERE_S =
      "在路径 %s 上设置 schemaengine 模板 %s 失败，原因：DataNode %s 上存在失败";
  public static final String FAILED_TO_START_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER =
      "启动 pipe {} 失败，详情：{}，元数据将稍后同步。";
  public static final String FAILED_TO_STOP_AINODE_BECAUSE_BUT_THE_REMOVE_PROCESS_WILL =
      "停止 AINode {} 失败，原因：{}，但移除流程将继续。";
  public static final String FAILED_TO_STOP_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER =
      "停止 pipe {} 失败，详情：{}，元数据将稍后同步。";
  public static final String FAILED_TO_SYNC_TABLE_COMMIT_CREATE_INFO_TO_DATANODE_FAILURE =
      "向 DataNode {} 同步表 {}.{} 的 commit-create 信息失败，失败结果：";
  public static final String FAILED_TO_SYNC_TABLE_PRE_CREATE_INFO_TO_DATANODE_FAILURE =
      "向 DataNode 同步表 {}.{} 的 pre-create 信息失败，失败结果：{}";
  public static final String FAILED_TO_SYNC_TABLE_ROLLBACK_CREATE_INFO_TO_DATANODE_FAILURE =
      "向 DataNode {} 同步表 {}.{} 的 rollback-create 信息失败，失败结果：";
  public static final String FAILED_TO_SYNC_TEMPLATE_COMMIT_SET_INFO_ON_PATH_TO =
      "向 DataNode {} 同步路径 {} 上模板 {} 的 commit-set 信息失败";
  public static final String FAILED_TO_SYNC_TEMPLATE_PRE_SET_INFO_ON_PATH_TO =
      "向 DataNode {} 同步路径 {} 上模板 {} 的 pre-set 信息失败";
  public static final String FAILED_TO_UPDATE_PROCEDURE = "更新 procedure {} 失败";
  public static final String FAILED_TO_UPDATE_TTL_CACHE_OF_DATANODE =
      "更新 DataNode 的 ttl 缓存失败。";
  public static final String FAILED_TO_WRITE_DATAPARTITIONTABLE_TO_CONSENSUS_LOG =
      "将 DataPartitionTable 写入共识日志失败";
  public static final String FAIL_IN_CREATECQPROCEDURE = "在 CreateCQProcedure 中失败";
  public static final String FAIL_TO_ACTIVE_TRIGGERINSTANCE_ON_DATA_NODES =
      "在 DataNode 上激活 triggerInstance [%s] 失败";
  public static final String FAIL_TO_CONFIG_NODE_INACTIVE_ROLLBACK_OF_TRIGGER =
      "对 trigger [%s] 执行 [CONFIG_NODE_INACTIVE] 回滚失败";
  public static final String FAIL_TO_CREATE_PIPE_PLUGIN_AFTER_RETRIES =
      "重试 {} 次后创建 pipe plugin [{}] 仍失败";
  public static final String FAIL_TO_CREATE_TRIGGERINSTANCE_ON_DATA_NODES =
      "在 DataNode 上创建 triggerInstance [%s] 失败";
  public static final String FAIL_TO_CREATE_TRIGGER_AT_STATE =
      "在 STATE [%s] 处创建 trigger [%s] 失败";
  public static final String FAIL_TO_DATA_NODE_INACTIVE_ROLLBACK_OF_TRIGGER =
      "对 trigger [%s] 执行 [DATA_NODE_INACTIVE] 回滚失败";
  public static final String FAIL_TO_DROP_PIPE_PLUGIN_AFTER_RETRIES =
      "重试 {} 次后删除 pipe plugin [{}] 仍失败";
  public static final String FAIL_TO_DROP_TRIGGER_AT_STATE =
      "在 STATE [%s] 处删除 trigger [%s] 失败";
  public static final String FAIL_TO_DROP_TRIGGER_ON_DATA_NODES =
      "在 DataNode 上删除 trigger [%s] 失败";
  public static final String FAIL_TO_EXECUTE_PLAN_AT_STATE =
      "在 state[%s] 处执行 plan [%s] 失败";
  public static final String FAIL_TO_REMOVE_AINODE_AT_STATE =
      "在 STATE [%s] 处移除 AINode [%s] 失败，%s";
  public static final String FAIL_TO_REMOVE_AINODE_ON_CONFIG_NODES =
      "在 ConfigNode [%s] 上移除 [%s] 个 AINode 失败";
  public static final String FAIL_WHEN_EXECUTE = "执行 {} 时失败 ";
  public static final String FINISH_INACTIVE_ROLLBACK_OF_CQ_SUCCESSFULLY =
      "成功完成 CQ {} 的 [INACTIVE] 回滚";
  public static final String FINISH_INIT_CQ_SUCCESSFULLY = "成功完成 CQ {} 的初始化";
  public static final String FINISH_SCHEDULING_CQ_SUCCESSFULLY =
      "成功完成 CQ {} 的调度";
  public static final String FORCE_UPDATE_NODECACHE_DATANODEID_NODESTATUS_CURRENTTIME =
      "{}, 强制更新 NodeCache：dataNodeId={}，nodeStatus={}，currentTime={}";
  public static final String FOR_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN_ALL_REPLICASET =
      "[%s] 对 %s.%s 失败，当 [%s] 时，原因：在所有 replicaset（%s %s）中执行失败。失败节点：%s";
  public static final String FOR_FAILED_WHEN_CONSTRUCT_BLACK_LIST_FOR_TABLE_BECAUSE_FAILED =
      "[%s] 对 %s.%s 失败，当为表构建黑名单时，原因：在所有 replicaset（%s %s）中执行失败。失败信息：%s";
  public static final String INVALIDATE_CACHE_OF_DEVICES_IN =
      "使 {}.{} 中设备的缓存失效";
  public static final String INVALIDATE_CACHE_OF_TEMPLATE_SET_ON =
      "使模板 {}（设置在 {} 上）的缓存失效";
  public static final String INVALIDATE_CACHE_OF_TEMPLATE_TIMESERIES =
      "使模板时间序列 {} 的缓存失效";
  public static final String INVALIDATE_CACHE_OF_TIMESERIES = "使时间序列 {} 的缓存失效";
  public static final String INVALIDATE_CACHE_OF_VIEW = "使视图 {} 的缓存失效";
  public static final String INVALIDATE_COLUMN_CACHE_FAILED_FOR_TABLE =
      "使表 %s.%s 的列 %s 缓存失效失败";
  public static final String INVALIDATE_SCHEMAENGINE_CACHE_FAILED = "使 SchemaEngine 缓存失效失败";
  public static final String INVALIDATE_SCHEMA_CACHE_FAILED = "使 schema 缓存失效失败";
  public static final String INVALIDATE_TEMPLATE_CACHE_FAILED = "使模板缓存失效失败";
  public static final String INVALIDATE_VIEW_SCHEMAENGINE_CACHE_FAILED =
      "使视图 schemaengine 缓存失效失败";
  public static final String INVALIDATING_CACHE_FOR_COLUMN_IN_WHEN_DROPPING_COLUMN =
      "删除列时正在使表 {}.{} 中的列 {} 缓存失效";
  public static final String INVALIDATING_CACHE_FOR_TABLE_WHEN_DROPPING_TABLE =
      "删除表时正在使表 {}.{} 缓存失效";
  public static final String INVALID_DATA_TYPE_CANNOT_BE_USED_AS_A_NEW_TYPE =
      "无效的数据类型不能作为新类型使用";
  public static final String IO_ERROR_WHEN_DESERIALIZE_AUTHPLAN =
      "反序列化 authplan 时发生 IO 错误。";
  public static final String IO_ERROR_WHEN_DESERIALIZE_SETTTL_PLAN =
      "反序列化 setTTL plan 时发生 IO 错误。";
  public static final String NO_AVAILABLE_DATANODE_TO_ASSIGN_TASKS = "没有可用的 DataNode 分配任务";
  public static final String NO_DATABASE_LOST_DATA_PARTITION_TABLE_FOR_CONSENSUS_WRITE =
      "没有数据库丢失用于共识写入的数据分区表";
  public static final String NO_DATAPARTITIONTABLE_AVAILABLE_FOR_CONSENSUS_WRITE =
      "没有可用于共识写入的 DataPartitionTable";
  public static final String NO_ENOUGH_DATA_NODE_TO_MIGRATE_REGION =
      "没有足够的 DataNode 用于迁移 region：{}";
  public static final String OPERATION_TIMED_OUT_AFTER = "操作超时，已耗时 ";
  public static final String PARTITION_TABLE_CLEANER_ACTIVATE_TTL_LOG =
      "[PartitionTableCleaner] 定期激活 PartitionTableAutoCleaner，databaseTTL：{}";
  public static final String PARTITIONTABLECLEANER_PERIODICALLY_ACTIVATE_PARTITIONTABLEAUTOCLEANER_FOR =
      "[PartitionTableCleaner] 为 {} 定期激活 PartitionTableAutoCleaner";
  public static final String PARTITIONTABLECLEANER_THE_PARTITIONTABLEAUTOCLEANER_IS_STARTED_WITH_CYCLE_MS =
      "[PartitionTableCleaner] PartitionTableAutoCleaner 已启动，cycle={}ms";
  public static final String PID_ADDREGION_CANNOT_ROLL_BACK_BECAUSE_CANNOT_FIND_THE_CORRECT =
      "[pid{}][AddRegion] 无法回滚，原因：找不到正确的 locations";
  public static final String PID_ADDREGION_IT_APPEARS_THAT_CONSENSUS_WRITE_HAS_NOT_MODIFIED =
      "[pid{}][AddRegion] 共识写入似乎未修改本地分区表。";
  public static final String PID_ADDREGION_RESET_PEER_LIST_PEER_LIST_OF_CONSENSUS_GROUP =
      "[pid{}][AddRegion] 重置 peer list：DataNode {} 上共识组 {} 的 peer list 重置为 {} 失败，可手动重置";
  public static final String PID_ADDREGION_RESET_PEER_LIST_PEER_LIST_OF_CONSENSUS_GROUP_2 =
      "[pid{}][AddRegion] 重置 peer list：DataNode {} 上共识组 {} 的 peer list 已成功重置为 {}";
  public static final String PID_ADDREGION_RESET_PEER_LIST_PEER_LIST_OF_CONSENSUS_GROUP_3 =
      "[pid{}][AddRegion] 重置 peer list：DataNode {} 上共识组 {} 的 peer list 将被重置为 {}";
  public static final String PID_ADDREGION_STARTED_WILL_BE_ADDED_TO_DATANODE =
      "[pid{}][AddRegion] 开始，{} 将被添加到 DataNode {}。";
  public static final String PID_ADDREGION_START_TO_ROLL_BACK_BECAUSE =
      "[pid{}][AddRegion] 开始回滚，原因：{}";
  public static final String PID_ADDREGION_STATE_COMPLETE = "[pid{}][AddRegion] 状态 {} 完成";
  public static final String PID_ADDREGION_STATE_FAILED = "[pid{}][AddRegion] 状态 {} 失败";
  public static final String PID_ADDREGION_SUCCESS_HAS_BEEN_ADDED_TO_DATANODE_PROCEDURE_TOOK =
      "[pid{}][AddRegion] 成功，{} 已添加到 DataNode {}。Procedure 耗时 {}（开始于 {}）。";
  public static final String PID_REMOVEREGIONGROUP_STARTED_WILL_BE_DELETED =
      "[pid{}][RemoveRegionGroup] 开始，region group {} 将从 DataNode {} 上删除。";
  public static final String PID_REMOVEREGIONGROUP_STARTED_REPLICA_WILL_BE_DELETED_FROM_DATANODE =
      "[pid{}][RemoveRegionGroup] region {} 将从 DataNode {} 上删除。";
  public static final String PID_REMOVEREGIONGROUP_STATE_FAILED =
      "[pid{}][RemoveRegionGroup] 状态 {} 失败";
  public static final String PID_REMOVEREGIONGROUP_DELETE_REPLICA_FAILED =
      "[pid{}][RemoveRegionGroup] 删除 region {} 的一个副本失败（第 {} 次尝试），将持续重试直到删除成功。原因：{}";
  public static final String PID_REMOVEREGIONGROUP_SUCCESS_PROCEDURE_TOOK =
      "[pid{}][RemoveRegionGroup] 成功，region group {} 已删除。过程耗时 {}（开始于 {}）。";
  public static final String PID_MIGRATEREGION_STARTED_WILL_BE_MIGRATED_FROM_DATANODE_TO =
      "[pid{}][MigrateRegion] 开始，{} 将从 DataNode {} 迁移到 {}。";
  public static final String PID_MIGRATEREGION_STATE_COMPLETE =
      "[pid{}][MigrateRegion] 状态 {} 完成";
  public static final String PID_MIGRATEREGION_STATE_FAIL = "[pid{}][MigrateRegion] 状态 {} 失败";
  public static final String PID_MIGRATEREGION_SUB_PROCEDURE_ADDREGIONPEERPROCEDURE =
      "[pid{}][MigrateRegion] 子 procedure AddRegionPeerProcedure 失败，RegionMigrateProcedure 将不再继续";
  public static final String PID_MIGRATEREGION_SUCCESS_HAS_BEEN_MIGRATED_FROM_DATANODE_TO_PROCEDURE =
      "[pid{}][MigrateRegion] 成功，{} {} 已从 DataNode {} 迁移到 {}。Procedure 耗时 {}（开始于 {}）。";
  public static final String PID_NOTIFYREGIONMIGRATION_STARTED_REGION_ID_IS =
      "[pid{}][NotifyRegionMigration] 开始，region id 为 {}。";
  public static final String PID_NOTIFYREGIONMIGRATION_STATE_COMPLETE =
      "[pid{}][NotifyRegionMigration] 状态 {} 完成";
  public static final String PID_NOTIFYREGIONMIGRATION_STATE_FAILED =
      "[pid{}][NotifyRegionMigration] 状态 {} 失败";
  public static final String PID_RECONSTRUCTREGION_FAILED_BUT_THE_REGION_HAS_BEEN_REMOVED_FROM =
      "[pid{}][ReconstructRegion] 失败，但 region {} 已从 DataNode {} 移除。请使用 'extend region' 修复。";
  public static final String PID_RECONSTRUCTREGION_STARTED_REGION_ON_DATANODE_WILL_BE_RECONSTRUCTED =
      "[pid{}][ReconstructRegion] 开始，DataNode {}({}) 上的 region {} 将被重建。";
  public static final String PID_RECONSTRUCTREGION_STATE_COMPLETE =
      "[pid{}][ReconstructRegion] 状态 {} 完成";
  public static final String PID_RECONSTRUCTREGION_STATE_FAIL =
      "[pid{}][ReconstructRegion] 状态 {} 失败";
  public static final String PID_RECONSTRUCTREGION_SUB_PROCEDURE_REMOVEREGIONPEERPROCEDURE =
      "[pid{}][ReconstructRegion] 子 procedure RemoveRegionPeerProcedure 失败，ReconstructRegionProcedure 将不再继续";
  public static final String PID_RECONSTRUCTREGION_SUCCESS_REGION_HAS_BEEN_RECONSTRUCTED =
      "[pid{}][ReconstructRegion] 成功，region {} 已在 DataNode {} 上重建。Procedure 耗时 {}（开始于 {}）";
  public static final String PID_REMOVEREGION_DELETE_OLD_REGION_PEER_EXECUTED_FAILED_AFTER_ATTEMPTS =
      "[pid{}][RemoveRegion] DELETE_OLD_REGION_PEER 在 {} 次尝试后仍执行失败，procedure 将继续。请手动删除 region 文件。{}";
  public static final String PID_REMOVEREGION_DELETE_OLD_REGION_PEER_EXECUTED_FAILED_ATTEMPT_WILL =
      "[pid{}][RemoveRegion] DELETE_OLD_REGION_PEER 执行失败（第 {}/{} 次尝试），将在 {} 毫秒后重试。{}";
  public static final String PID_REMOVEREGION_DELETE_OLD_REGION_PEER_TASK_SUBMITTED_FAILED_AFTER =
      "[pid{}][RemoveRegion] DELETE_OLD_REGION_PEER 任务在 {} 次尝试后仍提交失败，procedure 将继续。请手动删除 region 文件。{}";
  public static final String PID_REMOVEREGION_DELETE_OLD_REGION_PEER_TASK_SUBMITTED_FAILED_ATTEMPT =
      "[pid{}][RemoveRegion] DELETE_OLD_REGION_PEER 任务提交失败（第 {}/{} 次尝试），将在 {} 毫秒后重试。{}";
  public static final String PID_REMOVEREGION_EXECUTED_FAILED_CONFIGNODE_BELIEVE_CURRENT_PEER_LIST_OF =
      "[pid{}][RemoveRegion] {} 执行失败，ConfigNode 认为 {} 当前的 peer list 为 {}。Procedure 将继续。请手动清理 peer list。";
  public static final String PID_REMOVEREGION_STARTED_REGION_WILL_BE_REMOVED_FROM_DATANODE =
      "[pid{}][RemoveRegion] 开始，region {} 将从 DataNode {} 移除。";
  public static final String PID_REMOVEREGION_STATE_SUCCESS =
      "[pid{}][RemoveRegion] 状态 {} 成功";
  public static final String PID_REMOVEREGION_SUCCESS_REGION_HAS_BEEN_REMOVED_FROM_DATANODE_PROCEDURE =
      "[pid{}][RemoveRegion] 成功，region {} 已从 DataNode {} 移除。Procedure 耗时 {}（开始于 {}）";
  public static final String PID_REMOVEREGION_TASK_SUBMITTED_FAILED_CONFIGNODE_BELIEVE_CURRENT_PEER_LIST =
      "[pid{}][RemoveRegion] {} 任务提交失败，ConfigNode 认为 {} 当前的 peer list 为 {}。Procedure 将继续。请手动清理 peer list。";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_EXECUTEFROMCALCULATEINFOFORTASK =
      "PipeHandleLeaderChangeProcedure: executeFromCalculateInfoForTask";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_EXECUTEFROMHANDLEONCONFIGNODES =
      "PipeHandleLeaderChangeProcedure: executeFromHandleOnConfigNodes";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_EXECUTEFROMHANDLEONDATANODES =
      "PipeHandleLeaderChangeProcedure: executeFromHandleOnDataNodes";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_EXECUTEFROMVALIDATETASK =
      "PipeHandleLeaderChangeProcedure: executeFromValidateTask";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_ROLLBACKFROMCALCULATEINFOFORTASK =
      "PipeHandleLeaderChangeProcedure: rollbackFromCalculateInfoForTask";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_ROLLBACKFROMCREATEONDATANODES =
      "PipeHandleLeaderChangeProcedure: rollbackFromCreateOnDataNodes";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_ROLLBACKFROMHANDLEONCONFIGNODES =
      "PipeHandleLeaderChangeProcedure: rollbackFromHandleOnConfigNodes";
  public static final String PIPEHANDLELEADERCHANGEPROCEDURE_ROLLBACKFROMVALIDATETASK =
      "PipeHandleLeaderChangeProcedure: rollbackFromValidateTask";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_EXECUTEFROMCALCULATEINFOFORTASK =
      "PipeHandleMetaChangeProcedure: executeFromCalculateInfoForTask";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_EXECUTEFROMHANDLEONDATANODES =
      "PipeHandleMetaChangeProcedure: executeFromHandleOnDataNodes";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_EXECUTEFROMVALIDATETASK =
      "PipeHandleMetaChangeProcedure: executeFromValidateTask";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_EXECUTEFROMWRITECONFIGNODECONSENSUS =
      "PipeHandleMetaChangeProcedure: executeFromWriteConfigNodeConsensus";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_ROLLBACKFROMCALCULATEINFOFORTASK =
      "PipeHandleMetaChangeProcedure: rollbackFromCalculateInfoForTask";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_ROLLBACKFROMOPERATEONDATANODES =
      "PipeHandleMetaChangeProcedure: rollbackFromOperateOnDataNodes";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_ROLLBACKFROMVALIDATETASK =
      "PipeHandleMetaChangeProcedure: rollbackFromValidateTask";
  public static final String PIPEHANDLEMETACHANGEPROCEDURE_ROLLBACKFROMWRITECONFIGNODECONSENSUS =
      "PipeHandleMetaChangeProcedure: rollbackFromWriteConfigNodeConsensus";
  public static final String PIPEMETASYNCPROCEDURE_ACQUIRELOCK_SKIP_THE_PROCEDURE_DUE_TO_THE_LAST_EXECUTION =
      "PipeMetaSyncProcedure: acquireLock, skip the procedure due to the last execution time {}";
  public static final String PIPEMETASYNCPROCEDURE_EXECUTEFROMCALCULATEINFOFORTASK =
      "PipeMetaSyncProcedure: executeFromCalculateInfoForTask";
  public static final String PIPEMETASYNCPROCEDURE_EXECUTEFROMOPERATEONDATANODES =
      "PipeMetaSyncProcedure: executeFromOperateOnDataNodes";
  public static final String PIPEMETASYNCPROCEDURE_EXECUTEFROMVALIDATETASK =
      "PipeMetaSyncProcedure: executeFromValidateTask";
  public static final String PIPEMETASYNCPROCEDURE_EXECUTEFROMWRITECONFIGNODECONSENSUS =
      "PipeMetaSyncProcedure: executeFromWriteConfigNodeConsensus";
  public static final String PIPEMETASYNCPROCEDURE_ROLLBACKFROMCALCULATEINFOFORTASK =
      "PipeMetaSyncProcedure: rollbackFromCalculateInfoForTask";
  public static final String PIPEMETASYNCPROCEDURE_ROLLBACKFROMOPERATEONDATANODES =
      "PipeMetaSyncProcedure: rollbackFromOperateOnDataNodes";
  public static final String PIPEMETASYNCPROCEDURE_ROLLBACKFROMVALIDATETASK =
      "PipeMetaSyncProcedure: rollbackFromValidateTask";
  public static final String PIPEMETASYNCPROCEDURE_ROLLBACKFROMWRITECONFIGNODECONSENSUS =
      "PipeMetaSyncProcedure: rollbackFromWriteConfigNodeConsensus";
  public static final String PIPE_NOT_FOUND_IN_PIPETASKINFO_CAN_NOT_PUSH_ITS_META =
      "在 PipeTaskInfo 中找不到 Pipe {}，无法推送其元数据。";
  public static final String PIPE_PLUGIN_IS_ALREADY_CREATED_AND_ISSETIFNOTEXISTSCONDITION_IS_TRUE_END =
      "Pipe plugin {} 已创建，且 isSetIfNotExistsCondition 为 true，结束 CreatePipePluginProcedure({})";
  public static final String PIPE_PLUGIN_IS_ALREADY_CREATED_END_THE_CREATEPIPEPLUGINPROCEDURE =
      "Pipe plugin {} 已创建，结束 CreatePipePluginProcedure({})";
  public static final String PIPE_PLUGIN_IS_NOT_EXIST_END_THE_DROPPIPEPLUGINPROCEDURE =
      "Pipe plugin {} 不存在，结束 DropPipePluginProcedure({})";
  public static final String PRE_CREATE_TABLE = "预创建表 {}.{}";
  public static final String PRE_CREATE_TABLE_FAILED = "预创建表失败";
  public static final String PRE_RELEASE = "预释放 ";
  public static final String PRE_RELEASE_INFO_FOR_TABLE_WHEN_SETTING_PROPERTIES =
      "设置属性时预释放表 {}.{} 的信息";
  public static final String PRE_RELEASE_INFO_OF_TABLE_WHEN_ADDING_COLUMN =
      "添加列时预释放表 {}.{} 的信息";
  public static final String PRE_RELEASE_INFO_OF_TABLE_WHEN_ALTERING_COLUMN =
      "修改列时预释放表 {}.{} 的信息";
  public static final String PRE_RELEASE_INFO_OF_TABLE_WHEN_RENAMING_COLUMN =
      "重命名列时预释放表 {}.{} 的信息";
  public static final String PRE_RELEASE_INFO_OF_TABLE_WHEN_RENAMING_TABLE =
      "重命名表时预释放表 {}.{} 的信息";
  public static final String PRE_RELEASE_SCHEMAENGINE_TEMPLATE_SET_ON_PATH =
      "预释放在路径 {} 上设置的 schemaengine 模板 {}";
  public static final String PRE_RELEASE_TABLE = "预释放表 {}.{}";
  public static final String PRE_SET_SCHEMAENGINE_TEMPLATE_ON_PATH =
      "在路径 {} 上预设置 schemaengine 模板 {}";
  public static final String PRE_SET_TEMPLATE_FAILED = "预设置模板失败";
  public static final String PROCEDUREID = "ProcedureId {}: {}";
  public static final String PROCEDUREID_ACQUIRED_PIPE_LOCK = "ProcedureId {} 已获取 pipe 锁。";
  public static final String PROCEDUREID_ACQUIRED_SUBSCRIPTION_LOCK =
      "ProcedureId {} 已获取 subscription 锁。";
  public static final String PROCEDUREID_ALL_RETRIES_FAILED_WHEN_TRYING_TO_AT_STATE_WILL =
      "ProcedureId {}：尝试在状态 [{}] 下 {} 时，全部 {} 次重试均失败，将回滚……";
  public static final String PROCEDUREID_ENCOUNTERED_ERROR_WHEN_TRYING_TO_AT_STATE_RETRY =
      "ProcedureId {}：尝试在状态 [{}] 下 {} 时遇到错误，重试 [{}/{}]";
  public static final String PROCEDUREID_FAILED_TO_ACQUIRE_PIPE_LOCK =
      "ProcedureId {} 获取 pipe 锁失败。";
  public static final String PROCEDUREID_FAILED_TO_ACQUIRE_SUBSCRIPTION_LOCK =
      "ProcedureId {} 获取 subscription 锁失败。";
  public static final String PROCEDUREID_FAILED_TO_ROLLBACK_FROM_CALCULATE_INFO_FOR_TASK =
      "ProcedureId {}：从任务计算信息处回滚失败。";
  public static final String PROCEDUREID_FAILED_TO_ROLLBACK_FROM_OPERATE_ON_DATA_NODES =
      "ProcedureId {}：从 DataNode 操作处回滚失败。";
  public static final String PROCEDUREID_FAILED_TO_ROLLBACK_FROM_STATE_BECAUSE =
      "ProcedureId {}：从状态 [{}] 回滚失败，原因：{}";
  public static final String PROCEDUREID_FAILED_TO_ROLLBACK_FROM_VALIDATE_TASK =
      "ProcedureId {}：从 validate task 处回滚失败。";
  public static final String PROCEDUREID_FAILED_TO_ROLLBACK_FROM_WRITE_CONFIG_NODE_CONSENSUS =
      "ProcedureId {}：从写入 ConfigNode 共识处回滚失败。";
  public static final String PROCEDUREID_FAIL_TO_BECAUSE = "ProcedureId %s：%s 失败，原因：%s";
  public static final String PROCEDUREID_INVALID_LOCK_STATE_PIPE_LOCK_WILL_BE_RELEASED =
      "ProcedureId {}：{}。锁状态无效。Pipe 锁将被释放。";
  public static final String PROCEDUREID_INVALID_LOCK_STATE_SUBSCRIPTION_LOCK_WILL_BE_RELEASED =
      "ProcedureId {}：{}。锁状态无效。Subscription 锁将被释放。";
  public static final String PROCEDUREID_INVALID_LOCK_STATE_WITHOUT_ACQUIRING_PIPE_LOCK =
      "ProcedureId {}：{}。锁状态无效。未获取 pipe 锁。";
  public static final String PROCEDUREID_INVALID_LOCK_STATE_WITHOUT_ACQUIRING_SUBSCRIPTION_LOCK =
      "ProcedureId {}：{}。锁状态无效。未获取 subscription 锁。";
  public static final String PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_BE_EXECUTED_WITH =
      "ProcedureId {}：LOCK_ACQUIRED。后续 procedure 应在持有 pipe 锁的情况下执行。";
  public static final String PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_BE_EXECUTED_WITH_2 =
      "ProcedureId {}：LOCK_ACQUIRED。后续 procedure 应在同时持有 subscription 锁和 pipe 锁的情况下执行。";
  public static final String PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_BE_EXECUTED_WITH_3 =
      "ProcedureId {}：LOCK_ACQUIRED。后续 procedure 应在持有 subscription 锁的情况下执行。";
  public static final String PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_NOT_BE_EXECUTED =
      "ProcedureId {}：LOCK_ACQUIRED。后续 procedure 不应在未持有 pipe 锁的情况下执行。";
  public static final String PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_NOT_BE_EXECUTED_2 =
      "ProcedureId {}：LOCK_ACQUIRED。后续 procedure 不应在未持有 subscription 锁的情况下执行。";
  public static final String PROCEDUREID_LOCK_EVENT_WAIT_PIPE_LOCK_WILL_BE_RELEASED =
      "ProcedureId {}：LOCK_EVENT_WAIT。Pipe 锁将被释放。";
  public static final String PROCEDUREID_LOCK_EVENT_WAIT_SUBSCRIPTION_LOCK_WILL_BE_RELEASED =
      "ProcedureId {}：LOCK_EVENT_WAIT。Subscription 锁将被释放。";
  public static final String PROCEDUREID_LOCK_EVENT_WAIT_WITHOUT_ACQUIRING_PIPE_LOCK =
      "ProcedureId {}：LOCK_EVENT_WAIT。未获取 pipe 锁。";
  public static final String PROCEDUREID_LOCK_EVENT_WAIT_WITHOUT_ACQUIRING_SUBSCRIPTION_LOCK =
      "ProcedureId {}：LOCK_EVENT_WAIT。未获取 subscription 锁。";
  public static final String PROCEDUREID_PIPE_LOCK_IS_NOT_ACQUIRED_EXECUTEFROMSTATE_S_EXECUTION_WILL =
      "ProcedureId {}：未获取 pipe 锁，executeFromState 的执行将被跳过。";
  public static final String PROCEDUREID_PIPE_LOCK_IS_NOT_ACQUIRED_ROLLBACKSTATE_S_EXECUTION_WILL =
      "ProcedureId {}：未获取 pipe 锁，rollbackState({}) 的执行将被跳过。";
  public static final String PROCEDUREID_RELEASE_LOCK_NO_NEED_TO_RELEASE_PIPE_LOCK =
      "ProcedureId {} 释放锁。无需释放 pipe 锁。";
  public static final String PROCEDUREID_RELEASE_LOCK_NO_NEED_TO_RELEASE_SUBSCRIPTION_LOCK =
      "ProcedureId {} 释放锁。无需释放 subscription 锁。";
  public static final String PROCEDUREID_RELEASE_LOCK_PIPE_LOCK_WILL_BE_RELEASED =
      "ProcedureId {} 释放锁。Pipe 锁将被释放。";
  public static final String PROCEDUREID_RELEASE_LOCK_SUBSCRIPTION_LOCK_WILL_BE_RELEASED =
      "ProcedureId {} 释放锁。Subscription 锁将被释放。";
  public static final String PROCEDUREID_SUBSCRIPTION_LOCK_IS_NOT_ACQUIRED_EXECUTEFROMSTATE_S_EXECUTION_WILL =
      "ProcedureId {}：未获取 subscription 锁，executeFromState({}) 的执行将被跳过。";
  public static final String PROCEDUREID_SUBSCRIPTION_LOCK_IS_NOT_ACQUIRED_ROLLBACKSTATE_S_EXECUTION_WILL =
      "ProcedureId {}：未获取 subscription 锁，rollbackState({}) 的执行将被跳过。";
  public static final String PROCEDUREID_TRY_TO_ACQUIRE_PIPE_LOCK =
      "ProcedureId {} 尝试获取 pipe 锁。";
  public static final String PROCEDUREID_TRY_TO_ACQUIRE_SUBSCRIPTION_AND_PIPE_LOCK =
      "ProcedureId {} 尝试获取 subscription 锁和 pipe 锁。";
  public static final String PROCEDUREID_TRY_TO_ACQUIRE_SUBSCRIPTION_LOCK =
      "ProcedureId {} 尝试获取 subscription 锁。";
  public static final String PROCEDURE_TYPE = "Procedure 类型 ";
  public static final String REMOVEREGIONLOCATION_REMOVE_REGION_FROM_DATANODE_RESULT_IS =
      "RemoveRegionLocation：从 DataNode {} 移除 region {}，结果为 {}";
  public static final String REMOVEREGIONPEER_STATE_FAILED = "RemoveRegionPeer 状态 {} 失败";
  public static final String REMOVEREGIONPEER_STATE_SUCCESS = "RemoveRegionPeer 状态 {} 成功";
  public static final String REMOVEREGION_RATIS_TRANSFER_LEADER_FAIL_BUT_PROCEDURE_WILL_CONTINUE =
      "[RemoveRegion] Ratis 转换 leader 失败，但 procedure 将继续。";
  public static final String REMOVE_CONFIG_NODE = "移除 ConfigNode";
  public static final String REMOVE_DATA_NODE_FAILED = "移除 DataNode 失败 ";
  public static final String RENAMETABLECOLUMN_COSTS_MS = "RenameTableColumn-{}.{}-{} costs {}ms";
  public static final String RENAMETABLE_COSTS_MS = "RenameTable-{}.{}-{} costs {}ms";
  public static final String RENAME_COLUMN_TO_TABLE_ON_CONFIG_NODE =
      "在 ConfigNode 上重命名表 {}.{} 的列";
  public static final String RETRIEVABLE_ERROR_TRYING_TO_CREATE_CQ_STATE =
      "尝试创建 cq [{}] 时发生可重试错误，状态 [{}]";
  public static final String RETRIEVABLE_ERROR_TRYING_TO_CREATE_PIPE_PLUGIN_STATE =
      "尝试创建 pipe plugin [{}] 时发生可重试错误，状态：{}";
  public static final String RETRIEVABLE_ERROR_TRYING_TO_DROP_PIPE_PLUGIN_STATE =
      "尝试删除 pipe plugin [{}] 时发生可重试错误，状态：{}";
  public static final String RETRIEVABLE_ERROR_TRYING_TO_EXECUTE_PLAN_STATE =
      "尝试执行 plan {} 时发生可重试错误，状态：{}";
  public static final String RETRIEVABLE_ERROR_TRYING_TO_REMOVE_AINODE_STATE =
      "尝试移除 AINode [{}] 时发生可重试错误，状态 [{}]";
  public static final String ROLLBACK_CREATETABLE_COSTS_MS = "Rollback CreateTable-{} costs {}ms.";
  public static final String ROLLBACK_CREATE_TABLE_FAILED = "回滚创建表失败";
  public static final String ROLLBACK_DROPTABLE_COSTS_MS = "Rollback DropTable-{} costs {}ms.";
  public static final String ROLLBACK_PRE_RELEASE = "回滚预释放 ";
  public static final String ROLLBACK_PRE_RELEASE_TEMPLATE_FAILED =
      "回滚预释放模板失败";
  public static final String ROLLBACK_RENAMETABLECOLUMN_COSTS_MS =
      "Rollback RenameTableColumn-{} costs {}ms.";
  public static final String ROLLBACK_RENAMETABLE_COSTS_MS = "Rollback RenameTable-{} costs {}ms.";
  public static final String ROLLBACK_SETTABLEPROPERTIES_COSTS_MS =
      "Rollback SetTableProperties-{} costs {}ms.";
  public static final String ROLLBACK_SETTEMPLATE_COSTS_MS = "Rollback SetTemplate-{} costs {}ms.";
  public static final String ROLLBACK_TEMPLATE_CACHE_FAILED = "回滚模板缓存失败";
  public static final String ROLLBACK_TEMPLATE_PRE_UNSET_FAILED_BECAUSE_OF =
      "回滚预取消设置模板失败，原因";
  public static final String ROLLBACK_UNSET_TEMPLATE_FAILED_AND_THE_CLUSTER_TEMPLATE_INFO_MANAGEMENT =
      "回滚取消设置模板失败，集群模板信息管理已严重损坏。请重新尝试取消设置。";
  public static final String SELECTED_DATANODE_FOR_REGION = "已为 Region {} 选择 DataNode {}";
  public static final String SEND_ACTION_ADDREGIONPEER_FINISHED_REGIONID_RPCDATANODE_DESTDATANODE_STATUS =
      "{}, Send action addRegionPeer 完成，regionId：{}，rpcDataNode：{}，destDataNode：{}，status：{}";
  public static final String SEND_ACTION_CREATENEWREGIONPEER_ERROR_REGIONID_NEWPEERDATANODEID_RESULT =
      "{}, Send action createNewRegionPeer 出错，regionId：{}，newPeerDataNodeId：{}，result：{}";
  public static final String SEND_ACTION_CREATENEWREGIONPEER_FINISHED_REGIONID_NEWPEERDATANODEID =
      "{}, Send action createNewRegionPeer 完成，regionId：{}，newPeerDataNodeId：{}";
  public static final String SEND_ACTION_DELETEOLDREGIONPEER_FINISHED_REGIONID_DATANODEID =
      "{}, Send action deleteOldRegionPeer 完成，regionId：{}，dataNodeId：{}";
  public static final String SEND_ACTION_REMOVEREGIONPEER_FINISHED_REGIONID_RPCDATANODE =
      "{}, Send action removeRegionPeer 完成，regionId：{}，rpcDataNode：{}";
  public static final String SETSCHEMATEMPLATE_COSTS_MS = "SetSchemaTemplate-[{}] costs {}ms";
  public static final String SETTABLEPROPERTIES_COSTS_MS = "SetTableProperties-{}.{}-{} costs {}ms";
  public static final String SETTTL_COSTS_MS = "SetTTL-[{}] costs {}ms";
  public static final String SET_PROPERTIES_TO_TABLE = "设置表 {}.{} 的属性";
  public static final String SET_TEMPLATE_TO_FAILED_WHEN_CHECK_TIME_SERIES_EXISTENCE_ON =
      "将模板 %s 设置到 %s 失败，当 [检查 DataNode 上时间序列是否存在] 时，原因 ";
  public static final String STARTPIPEPROCEDUREV2_EXECUTEFROMCALCULATEINFOFORTASK =
      "StartPipeProcedureV2: executeFromCalculateInfoForTask({})";
  public static final String STARTPIPEPROCEDUREV2_EXECUTEFROMOPERATEONDATANODES =
      "StartPipeProcedureV2: executeFromOperateOnDataNodes({})";
  public static final String STARTPIPEPROCEDUREV2_EXECUTEFROMVALIDATETASK =
      "StartPipeProcedureV2: executeFromValidateTask({})";
  public static final String STARTPIPEPROCEDUREV2_EXECUTEFROMWRITECONFIGNODECONSENSUS =
      "StartPipeProcedureV2: executeFromWriteConfigNodeConsensus({})";
  public static final String STARTPIPEPROCEDUREV2_ROLLBACKFROMCALCULATEINFOFORTASK =
      "StartPipeProcedureV2: rollbackFromCalculateInfoForTask({})";
  public static final String STARTPIPEPROCEDUREV2_ROLLBACKFROMOPERATEONDATANODES =
      "StartPipeProcedureV2: rollbackFromOperateOnDataNodes({})";
  public static final String STARTPIPEPROCEDUREV2_ROLLBACKFROMVALIDATETASK =
      "StartPipeProcedureV2: rollbackFromValidateTask({})";
  public static final String STARTPIPEPROCEDUREV2_ROLLBACKFROMWRITECONFIGNODECONSENSUS =
      "StartPipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})";
  public static final String START_INACTIVE_ROLLBACK_OF_CQ = "开始 CQ {} 的 [INACTIVE] 回滚";
  public static final String START_ROLLBACK_ADD_COLUMN_TO_TABLE_WHEN_ADDING_COLUMN =
      "添加列时开始回滚向表 {}.{} 添加列的操作";
  public static final String START_ROLLBACK_COMMIT_SET_SCHEMAENGINE_TEMPLATE_ON_PATH =
      "开始回滚在路径 {} 上提交设置 schemaengine 模板 {} 的操作";
  public static final String START_ROLLBACK_PRE_CREATE_TABLE =
      "开始回滚预创建表 {}.{}";
  public static final String START_ROLLBACK_PRE_RELEASE_INFO_FOR_TABLE_WHEN_SETTING_PROPERTIES =
      "设置属性时开始回滚预释放表 {}.{} 的信息";
  public static final String START_ROLLBACK_PRE_RELEASE_INFO_OF_TABLE =
      "开始回滚预释放表 {}.{} 的信息";
  public static final String START_ROLLBACK_PRE_RELEASE_SCHEMAENGINE_TEMPLATE_ON_PATH =
      "开始回滚预释放在路径 {} 上设置的 schemaengine 模板 {}";
  public static final String START_ROLLBACK_PRE_RELEASE_TABLE =
      "开始回滚预释放表 {}.{}";
  public static final String START_ROLLBACK_PRE_SET_SCHEMAENGINE_TEMPLATE_ON_PATH =
      "开始回滚在路径 {} 上预设置 schemaengine 模板 {}";
  public static final String START_ROLLBACK_RENAMING_COLUMN_TO_TABLE_ON_CONFIGNODE =
      "在 ConfigNode 上开始回滚重命名表 {}.{} 列的操作";
  public static final String START_ROLLBACK_RENAMING_TABLE_ON_CONFIGNODE =
      "在 ConfigNode 上开始回滚重命名表 {}.{}";
  public static final String START_ROLLBACK_SET_PROPERTIES_TO_TABLE =
      "开始回滚设置表 {}.{} 的属性";
  public static final String STATE_STUCK_AT = "状态卡在 ";
  public static final String STOPPIPEPROCEDUREV2_EXECUTEFROMCALCULATEINFOFORTASK =
      "StopPipeProcedureV2: executeFromCalculateInfoForTask({})";
  public static final String STOPPIPEPROCEDUREV2_EXECUTEFROMOPERATEONDATANODES =
      "StopPipeProcedureV2: executeFromOperateOnDataNodes({})";
  public static final String STOPPIPEPROCEDUREV2_EXECUTEFROMVALIDATETASK =
      "StopPipeProcedureV2: executeFromValidateTask({})";
  public static final String STOPPIPEPROCEDUREV2_EXECUTEFROMWRITECONFIGNODECONSENSUS =
      "StopPipeProcedureV2: executeFromWriteConfigNodeConsensus({})";
  public static final String STOPPIPEPROCEDUREV2_ROLLBACKFROMCALCULATEINFOFORTASK =
      "StopPipeProcedureV2: rollbackFromCalculateInfoForTask({})";
  public static final String STOPPIPEPROCEDUREV2_ROLLBACKFROMOPERATEONDATANODES =
      "StopPipeProcedureV2: rollbackFromOperateOnDataNodes({})";
  public static final String STOPPIPEPROCEDUREV2_ROLLBACKFROMVALIDATETASK =
      "StopPipeProcedureV2: rollbackFromValidateTask({})";
  public static final String STOPPIPEPROCEDUREV2_ROLLBACKFROMWRITECONFIGNODECONSENSUS =
      "StopPipeProcedureV2: rollbackFromWriteConfigNodeConsensus({})";
  public static final String STOP_DATA_NODE_MEETS_ERROR_ERROR_DATANODE =
      "{}, 停止 DataNode 出错，出错的 DataNode：{}";
  public static final String STOP_DATA_NODE_SUCCESS = "{}, 停止 DataNode {} 成功。";
  public static final String SUBMITTED_ASYNC_CONSENSUS_PIPE_CREATION =
      "{}, 已提交异步共识 pipe 创建：{}";
  public static final String SUBSCRIPTION_META_SYNC_PROCEDURE_FINISHED_UPDATING_LAST_SYNC_VERSION =
      "Subscription 元数据同步 procedure 完成，正在更新上次同步版本。";
  public static final String SUCCESSFULLY_OPERATE_WILL_CLEAR_CACHE_TO_THE_DATA_REGIONS_ANYWAY =
      "操作成功，无论如何都将清理 data region 的缓存";
  public static final String SUCCESSFULLY_RESTORED_WILL_SET_MODS_TO_THE_DATA_REGIONS_ANYWAY =
      "成功恢复，无论如何都将为 data region 设置 mods";
  public static final String SUCCESSFULLY_STOPPED_AINODE = "成功停止 AINode {}";
  public static final String TABLE_ALREADY_EXISTS = "表 '%s.%s' 已存在。";
  public static final String TABLE_NOT_EXISTS = "表 '%s.%s' 不存在。";
  public static final String TARGET_DEVICE_TEMPLATE_IS_NOT_ACTIVATED_ON_ANY_PATH_MATCHED =
      "目标设备模板未在匹配给定路径模式的任何路径上激活";
  public static final String TASK_CANNOT_GET_TASK_REPORT_FROM_DATANODE_LAST_REPORT_TIME =
      "{} 任务 {} 无法从 DataNode {} 获取任务报告，上次报告时间为 {} 之前";
  public static final String THE_UPDATED_TABLE_HAS_THE_SAME_PROPERTIES_WITH_THE_ORIGINAL =
      "更新后的表与原表属性相同。跳过该 procedure。";
  public static final String TOPICMETASYNCPROCEDURE_ACQUIRELOCK_SKIP_THE_PROCEDURE_DUE_TO_THE_LAST_EXECUTION =
      "TopicMetaSyncProcedure: acquireLock, skip the procedure due to the last execution time {}";
  public static final String TOPICMETASYNCPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES =
      "TopicMetaSyncProcedure: executeFromOperateOnConfigNodes";
  public static final String TOPICMETASYNCPROCEDURE_EXECUTEFROMOPERATEONDATANODES =
      "TopicMetaSyncProcedure: executeFromOperateOnDataNodes";
  public static final String TOPICMETASYNCPROCEDURE_EXECUTEFROMVALIDATE =
      "TopicMetaSyncProcedure: executeFromValidate";
  public static final String TOPICMETASYNCPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES =
      "TopicMetaSyncProcedure: rollbackFromOperateOnConfigNodes";
  public static final String TOPICMETASYNCPROCEDURE_ROLLBACKFROMOPERATEONDATANODES =
      "TopicMetaSyncProcedure: rollbackFromOperateOnDataNodes";
  public static final String TOPICMETASYNCPROCEDURE_ROLLBACKFROMVALIDATE =
      "TopicMetaSyncProcedure: rollbackFromValidate";
  public static final String UNEXPECTED_FAIL_TSSTATUS_IS = "意外的失败，tsStatus 为 ";
  public static final String UNEXPECTED_STATE = "意外的状态";
  public static final String UNKNOWN_CREATECQSTATE = "未知的 CreateCQState：";
  public static final String UNKNOWN_CREATETRIGGERSTATE = "未知的 CreateTriggerState：";
  public static final String UNKNOWN_DROPTRIGGERSTATE = "未知的 DropTriggerState：";
  public static final String UNKNOWN_LOAD_BALANCE_STRATEGY = "未知的负载均衡策略：";
  public static final String UNKNOWN_PROCEDURE_TYPE = "未知的 Procedure 类型：";
  public static final String UNKNOWN_PROCEDURE_TYPE_2 = "未知的 Procedure 类型：{}";
  public static final String UNKNOWN_STATE = "未知状态：";
  public static final String UNKNOWN_STATE_DURING_EXECUTING_CREATEPIPEPLUGINPROCEDURE =
      "执行 createPipePluginProcedure 期间出现未知状态，%s";
  public static final String UNKNOWN_STATE_DURING_EXECUTING_OPERATEPIPEPROCEDURE =
      "执行 operatePipeProcedure 期间出现未知状态，%s";
  public static final String UNKNOWN_STATE_DURING_EXECUTING_OPERATESUBSCRIPTIONPROCEDURE =
      "执行 operateSubscriptionProcedure 期间出现未知状态，%s";
  public static final String UNKNOWN_STATE_DURING_EXECUTING_REMOVEAINODEPROCEDURE =
      "执行 removeAINodeProcedure 期间出现未知状态，%s";
  public static final String UNKNOWN_STATE_DURING_ROLLBACK_OPERATESUBSCRIPTIONPROCEDURE =
      "回滚 operateSubscriptionProcedure 期间出现未知状态，%s";
  public static final String UNKNOWN_STATE_FOR_ROLLBACK = "回滚时出现未知状态：";
  public static final String UNRECOGNIZED_ADDTABLECOLUMNSTATE = "无法识别的 AddTableColumnState ";
  public static final String UNRECOGNIZED_ALTERTABLECOLUMNDATATYPEPROCEDURE =
      "无法识别的 AlterTableColumnDataTypeProcedure ";
  public static final String UNRECOGNIZED_ALTERTIMESERIESDATATYPEPROCEDURE_STATE =
      "无法识别的 AlterTimeSeriesDataTypeProcedure 状态 ";
  public static final String UNRECOGNIZED_CREATETABLESTATE = "无法识别的 CreateTableState ";
  public static final String UNRECOGNIZED_DROPTABLECOLUMNSTATE =
      "无法识别的 DropTableColumnState ";
  public static final String UNRECOGNIZED_DROPTABLESTATE = "无法识别的 DropTableState ";
  public static final String UNRECOGNIZED_LOG_TYPE = "无法识别的日志类型 ";
  public static final String UNRECOGNIZED_RENAMETABLECOLUMNSTATE =
      "无法识别的 RenameTableColumnState ";
  public static final String UNRECOGNIZED_RENAMETABLESTATE = "无法识别的 RenameTableState ";
  public static final String UNRECOGNIZED_SETTEMPLATESTATE = "无法识别的 SetTemplateState ";
  public static final String UNRECOGNIZED_STATE = "无法识别的状态 ";
  public static final String UNSETTEMPLATE_COSTS_MS = "UnsetTemplate-[{}] costs {}ms";
  public static final String UNSET_TEMPLATE_FROM_FAILED_WHEN_CHECK_DATANODE_TEMPLATE_ACTIVATION_BECAUSE =
      "从 %s 取消设置模板 %s 失败，当 [检查 DataNode 模板激活情况] 时，原因：%s";
  public static final String UNSET_TEMPLATE_ON = "在 {} 上取消设置模板 {}";
  public static final String UNSUPPORTED_ROLL_BACK_STATE = "不支持的回滚 STATE [{}]";
  public static final String UNSUPPORTED_STATE = "不支持的状态：";
  public static final String UPDATE_DATANODE_TTL_CACHE_FAILED = "更新 DataNode ttl 缓存失败";
  public static final String VALIDATE_TABLE_FOR_TABLE_WHEN_SETTING_PROPERTIES =
      "设置属性时校验表 {}.{}";
  public static final String WAITTASKFINISH_RETURNS_PROCESSING_WHICH_MEANS_THE_WAITING_HAS_BEEN_INTERRUPTED =
      "waitTaskFinish() 返回 PROCESSING，表示等待已被中断（ConfigNode 关闭或 leader 切换）；AddRegionPeer 任务仍在协调节点上运行，本 procedure 将停留在 DO_ADD_REGION_PEER，并在恢复后继续轮询";

    public static final String FAILED_TO_CREATE_DATABASE_THE_TTL_SHOULD_BE_NON_NEGATIVE = "创建数据库失败。TTL 不能为负数。";
  public static final String FAILED_TO_CREATE_DATABASE_THE_DATAREGIONGROUPNUM_SHOULD_BE_POSITIVE = "创建数据库失败。dataRegionGroupNum 应为正数。";
  public static final String PID_FAILED_TO_PERSIST_LOCK_STATE_TO_STORE = "pid={} 持久化锁状态到存储失败。";
  public static final String FORCE_WRITE_UNLOCK_STATE_TO_RAFT_FOR_PID = "强制写入解锁状态到 raft，pid={}";
  public static final String PID_FAILED_TO_PERSIST_UNLOCK_STATE_TO_STORE = "pid={} 持久化解锁状态到存储失败。";
  public static final String DIDN_T_HOLD_THE_LOCK_BEFORE_RESTARTING_SKIP_ACQUIRING_LOCK = "{} 重启前未持有锁，跳过获取锁";
  public static final String IS_ALREADY_BYPASSED_SKIP_ACQUIRING_LOCK = "{} 已被绕过，跳过获取锁。";
  public static final String IS_IN_WAITING_STATE_AND_HOLDLOCK_FALSE_SKIP_ACQUIRING_LOCK = "{} 处于 WAITING 状态且 holdLock=false，跳过获取锁。";
  public static final String HELD_THE_LOCK_BEFORE_RESTARTING_CALL_ACQUIRELOCK_TO_RESTORE_IT = "{} 重启前持有锁，调用 acquireLock 恢复。";
  public static final String CHILD_LATCH_INCREMENT_SET = "子锁计数器设置递增 ";
  public static final String CHILD_LATCH_INCREMENT = "子锁计数器递增 ";
  public static final String CHILD_LATCH_DECREMENT = "子锁计数器递减 ";
  public static final String UNEXPECTED_STATE_FOR = "意外状态：{} 对应 {}";
  public static final String OLD_PROCEDURE_DIRECTORY_DETECTED_UPGRADE_BEGINNING = "检测到旧的 procedure 目录，开始升级...";
  public static final String ALREADY_RUNNING = "已在运行中";
  public static final String PROCEDURE_WORKERS_ARE_STARTED = "{} 个 procedure 工作线程已启动。";
  public static final String IS_ALREADY_FINISHED = "{} 已完成。";
  public static final String ROLLBACK_BECAUSE_PARENT_IS_DONE_ROLLEDBACK_PROC_IS = "因父流程已完成/已回滚而回滚，proc 为 {}";
  public static final String ROLLBACK_STACK_IS_NULL_FOR = "{} 的回滚栈为 null";
  public static final String LOCK_EVENT_WAIT_ROLLBACK = "LOCK_EVENT_WAIT 回滚 {}";
  public static final String LOCK_EVENT_WAIT_CAN_T_ROLLBACK_CHILD_RUNNING_FOR = "LOCK_EVENT_WAIT 无法回滚正在运行的子流程 {}";
  public static final String LOCKSTATE_IS = "{} 锁状态为 {}";
  public static final String FINISHED_IN_MS_SUCCESSFULLY = "{} 在 {} 毫秒内成功完成。";
  public static final String PROCEDUREID_WAIT_FOR_LOCK = "procedureId {} 等待锁。";
  public static final String INTERRUPT_DURING_EXECUTION_SUSPEND_OR_RETRY_IT_LATER = "执行期间被中断，稍后挂起或重试。";
  public static final String CODE_BUG = "代码错误：{}";
  public static final String INITIALIZED_SUB_PROCS = "初始化子流程：{}";
  public static final String ADDED_INTO_TIMEOUTEXECUTOR = "已添加到超时执行器 {}";
  public static final String SUB_PROCEDURE_PID_HAS_BEEN_SUBMITTED = "子流程 pid={} 已提交";
  public static final String STORED_CHILDREN = "已存储 {}，子流程 {}";
  public static final String FAILED_TO_UPDATE_SUBPROCS_ON_EXECUTION = "执行时更新子流程失败";
  public static final String STORE_UPDATE = "存储更新 {}";
  public static final String FAILED_TO_DELETE_SUBPROCEDURES_ON_EXECUTION = "执行时删除子流程失败";
  public static final String FAILED_TO_UPDATE_PROCEDURE_ON_EXECUTION = "执行时更新流程失败";
  public static final String ROLLED_BACK_TIME_DURATION_IS = "已回滚 {}，耗时 {}";
  public static final String ROLL_BACK_FAILED_FOR = "{} 回滚失败";
  public static final String INTERRUPTED_EXCEPTION_OCCURRED_FOR = "{} 发生中断异常";
  public static final String CODE_BUG_RUNTIME_EXCEPTION_FOR = "代码错误：{} 发生运行时异常";
  public static final String FAILED_TO_DELETE_PROCEDURE_ON_ROLLBACK = "回滚时删除流程失败";
  public static final String FAILED_TO_UPDATE_PROCEDURE_ON_ROLLBACK = "回滚时更新 procedure 失败";
  public static final String PROCEDURE_WORKER_TERMINATED = "Procedure 工作线程 {} 已终止。";
  public static final String ADDED_NEW_WORKER_THREAD = "添加新工作线程 {}";
  public static final String STOPPING = "正在停止";
  public static final String FAILED_TO_UPDATE_STORE_PROCEDURE = "更新存储流程 {} 失败";
  public static final String IS_STORED = "{} 已存储。";
  public static final String START_TO_CREATE_TRIGGER = "开始创建触发器 [{}]";
  public static final String CREATE_TRIGGER_FAILED = "创建触发器 {} 失败。";
  public static final String START_INIT_ROLLBACK_OF_TRIGGER = "开始 [INIT] 回滚触发器 [{}]";
  public static final String START_VALIDATED_ROLLBACK_OF_TRIGGER = "开始 [VALIDATED] 回滚触发器 [{}]";
  public static final String START_TO_DROP_TRIGGER = "开始删除触发器 [{}]";
  public static final String START_TO_DROP_TRIGGER_ON_DATA_NODES = "开始在 DataNode 上删除触发器 [{}]";
  public static final String START_TO_DROP_TRIGGER_ON_CONFIG_NODES = "开始在 ConfigNode 上删除触发器 [{}]";
  public static final String DROP_TRIGGER_FAILED = "删除触发器 {} 失败。";
  public static final String ERROR_IN_DESERIALIZE_DELETEDATABASEPROCEDURE = "反序列化 DeleteDatabaseProcedure 出错";
  public static final String EXECUTING_CREATE_PEER_ON = "正在执行 CREATE_PEER 于 {}...";
  public static final String SUCCESSFULLY_CREATE_PEER_ON = "成功执行 CREATE_PEER 于 {}";
  public static final String EXECUTING_ADD_PEER = "正在执行 ADD_PEER {}...";
  public static final String SUCCESSFULLY_ADD_PEER = "成功执行 ADD_PEER {}";
  public static final String THE_CONFIGNODE_IS_SUCCESSFULLY_ADDED_TO_THE_CLUSTER = "ConfigNode {} 已成功加入集群";
  public static final String ROLLBACK_CREATE_PEER_FOR = "回滚 CREATE_PEER：{}";
  public static final String ROLLBACK_ADD_PEER_FOR = "回滚 ADD_PEER：{}";
  public static final String ERROR_IN_DESERIALIZE_ADDCONFIGNODEPROCEDURE = "反序列化 AddConfigNodeProcedure 出错";
  public static final String REMOVE_PEER_FOR_CONFIGNODE = "移除 ConfigNode 的 peer：{}";
  public static final String DELETE_PEER_FOR_CONFIGNODE = "删除 ConfigNode 的 peer：{}";
  public static final String STOP_AND_CLEAR_CONFIGNODE = "停止并清理 ConfigNode：{}";
  public static final String ERROR_IN_DESERIALIZE_REMOVECONFIGNODEPROCEDURE = "反序列化 RemoveConfigNodeProcedure 出错";
  public static final String DATAPARTITIONINTEGRITY_ERROR_EXECUTING_STATE = "[DataPartitionIntegrity] 执行状态 {} 出错：{}";
  public static final String COLLECTING_EARLIEST_TIMESLOTS_FROM_ALL_DATANODES = "正在从所有 DataNode 收集最早时间槽...";
  public static final String ANALYZING_MISSING_DATA_PARTITIONS = "正在分析缺失的数据分区...";
  public static final String CHECKING_DATAPARTITIONTABLE_GENERATION_COMPLETION_STATUS = "正在检查 DataPartitionTable 生成完成状态...";
  public static final String MERGING_DATAPARTITIONTABLES_FROM_DATANODES = "正在合并来自 {} 个 DataNode 的 DataPartitionTable...";
  public static final String DATAPARTITIONINTEGRITY_DATAPARTITIONTABLES_MERGE_COMPLETED_SUCCESSFULLY = "[DataPartitionIntegrity] DataPartitionTable 合并成功";
  public static final String WRITING_DATAPARTITIONTABLE_TO_CONSENSUS_LOG = "正在将 DataPartitionTable 写入共识日志...";
  public static final String DATAPARTITIONINTEGRITY_NO_DATABASE_LOST_DATA_PARTITION_TABLE = "[DataPartitionIntegrity] 没有数据库丢失数据分区表";
  public static final String DATAPARTITIONINTEGRITY_DATAPARTITIONTABLE_TO_WRITE_TO_CONSENSUS = "[DataPartitionIntegrity] 待写入共识的 DataPartitionTable";
  public static final String DATAPARTITIONINTEGRITY_FAILED_TO_WRITE_DATAPARTITIONTABLE_TO_CONSENSUS_LOG = "[DataPartitionIntegrity] 写入 DataPartitionTable 到共识日志失败";
  public static final String DATAPARTITIONINTEGRITY_ERROR_WRITING_DATAPARTITIONTABLE_TO_CONSENSUS_LOG = "[DataPartitionIntegrity] 写入 DataPartitionTable 到共识日志出错";
  public static final String DATAPARTITIONINTEGRITY_FAILED_TO_SERIALIZE_SKIPDATANODE = "[DataPartitionIntegrity] 序列化 skipDataNode 失败";
  public static final String DATAPARTITIONINTEGRITY_FAILED_TO_SERIALIZE_FAILEDDATANODE = "[DataPartitionIntegrity] 序列化 failedDataNode 失败";
  public static final String DATAPARTITIONINTEGRITY_FAILED_TO_DESERIALIZE_SKIPDATANODE = "[DataPartitionIntegrity] 反序列化 skipDataNode 失败";
  public static final String DATAPARTITIONINTEGRITY_FAILED_TO_DESERIALIZE_FAILEDDATANODE = "[DataPartitionIntegrity] 反序列化 failedDataNode 失败";
  public static final String DATAPARTITIONINTEGRITY_SKIPPING_EMPTY_BYTEBUFFER_DURING_DESERIALIZATION = "[DataPartitionIntegrity] 反序列化时跳过空 ByteBuffer";
  public static final String NOT_FIND_REGION_REPLICA_NODES_IN_CREATEPEER_REGIONID = "在 createPeer 中未找到 region 副本节点，regionId：";
  public static final String SIMPLECONSENSUS_PROTOCOL_IS_NOT_SUPPORTED_TO_REMOVE_DATA_NODE = "SimpleConsensus 协议不支持移除数据节点";
  public static final String FAILED_TO_REMOVE_ALL_REQUESTED_DATA_NODES = "移除所有请求的数据节点失败";
  public static final String THERE_EXIST_DATA_NODE_IN_REQUEST_BUT_NOT_IN_CLUSTER = "请求中存在不在集群中的 DataNode";

  public static final String FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE = "在共识层执行写入 API 失败，原因：";
  private ProcedureMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_MS_95BA098D = " ms.";
  public static final String LOG_ARG_JOIN_WAIT_GOT_INTERRUPTED_316B5E9F = "{} 的 join 等待被中断";
  public static final String LOG_NO_COMPLETED_PROCEDURES_CLEANUP_50434D91 = "没有已完成的 procedures 需要清理。";
  public static final String LOG_ERROR_DELETING_COMPLETED_PROCEDURES_ARG_1A3A185E = "删除已完成的 procedures {} 时出错。";
  public static final String LOG_EVICT_COMPLETED_ARG_A968A070 = "驱逐已完成的 {}";
  public static final String LOG_EXECUTING_PROCEDURE_SHOULD_RUNNABLE_STATE_BUT_IT_S_NOT_PROCEDURE_7CF42CE8 = "执行中的 procedure 应处于 RUNNABLE 状态，但实际不是。Procedure 为 {}";
  public static final String LOG_FINISHED_SUBPROCEDURE_PID_ARG_RESUME_PROCESSING_PPID_ARG_93ED990B = "subprocedure pid={} 已完成，恢复处理 ppid={}";
  public static final String LOG_HALT_PID_ARG_ACTIVECOUNT_ARG_411F3EBF = "暂停 pid={}, activeCount={}";
  public static final String LOG_EXCEPTION_HAPPENED_WORKER_ARG_EXECUTE_PROCEDURE_ARG_6E3AD27D = "worker {} 执行 procedure {} 时发生异常";
  public static final String LOG_WORKER_STUCK_ARG_ARG_RUN_TIME_ARG_MS_FB612354 = "Worker 卡住 {}({})，运行时间 {} ms";
  public static final String LOG_PROCEDURE_WORKERS_ARG_RUNNING_ARG_RUNNING_STUCK_1565936D = "Procedure workers：{} 正在运行，{} 正在运行且卡住";
  public static final String LOG_PROCEDUREEXECUTOR_THREADGROUP_ARG_CONTAINS_RUNNING_THREADS_WHICH_USED_NON_PROCEDURE_BD865211 = "ProcedureExecutor threadGroup {} 包含被非 procedure 模块使用的运行线程。";
  public static final String LOG_ADD_PROCEDURE_ARG_AS_ARG_TH_ROLLBACK_STEP_C71B2184 = "将 procedure {} 添加为第 {} 个回滚步骤";
  public static final String LOG_STATEMACHINEPROCEDURE_PID_ARG_NOT_SET_NEXT_STATE_BUT_RETURN_HAS_7F93E63F =
      "StateMachineProcedure pid={} 未设置下一状态，却返回 HAS_MORE_STATE。代码可能存在问题，请检查代码。该 procedure 即将被终止：{}";
  public static final String LOG_STATEMACHINEPROCEDURE_PID_ARG_SET_NEXT_STATE_ARG_BUT_RETURN_NO_0CA2D56C = "StateMachineProcedure pid={} 设置下一状态为 {}，但返回 NO_MORE_STATE";
  public static final String LOG_DON_T_ADD_SUCCESSFUL_PROCEDURE_BACK_SCHEDULER_IT_WILL_IGNORED_E015472C = "不要将已成功的 procedure 加回 scheduler，它将被忽略";
  public static final String LOG_SCHEDULER_NOT_RUNNING_6969C9FF = "scheduler 未运行";
  public static final String LOG_SCHEDULER_WAITING_TIME_LEFT_ARG_NANOS_D7717019 = "scheduler 剩余等待时间 {} nanos";
  public static final String LOG_SLEEP_FAILED_CONFIGNODEPROCEDUREENV_BCD470AC = "ConfigNodeProcedureEnv 中 Sleep 失败：";
  public static final String LOG_INVALIDATE_CACHE_FAILED_BECAUSE_DATANODE_ARG_UNKNOWN_4F2D374C = "缓存失效失败，原因：DataNode {} 状态未知";
  public static final String LOG_INVALIDATE_CACHE_FAILED_INVALIDATE_PARTITION_CACHE_STATUS_ARG_INVALIDATE_SCHEMAENGINE_BEB7A065 = "缓存失效失败，分区缓存失效状态为 {}，schemaengine 缓存失效状态为 {}";
  public static final String MESSAGE_REMOVE_CONFIGNODE_FAILED_BECAUSE_UPDATE_CONSENSUSGROUP_PEER_INFORMATION_FAILED_FCE5302B = "移除 ConfigNode 失败，原因：更新 ConsensusGroup peer 信息失败。";
  public static final String MESSAGE_CAN_T_REMOVE_DATANODE_LIMIT_REPLICATION_FACTOR_D960E3A6 = "无法移除 DataNode，原因：受副本因子限制，";
  public static final String MESSAGE_AVAILABLEDATANODESIZE_ARG_MAXREPLICAFACTOR_ARG_MAX_ALLOWED_REMOVED_DATA_NODE_SIZE_FB8C382C = "availableDataNodeSize：%s，maxReplicaFactor：%s，允许移除的最大 DataNode 数量为：%s";
  public static final String EXCEPTION_NOT_SUPPORTED_0A83F963 = " 不支持";
  public static final String LOG_START_ADD_TRIGGER_ARG_TRIGGERTABLE_CONFIG_NODES_NEEDTOSAVEJAR_ARG_0C23D81E = "开始在 Config Nodes 的 TriggerTable 中添加 trigger [{}]，needToSaveJar[{}]";
  public static final String LOG_START_CREATE_TRIGGERINSTANCE_ARG_DATA_NODES_917C3313 = "开始在 Data Nodes 上创建 triggerInstance [{}]";
  public static final String LOG_START_ACTIVE_TRIGGER_ARG_DATA_NODES_A4AB8131 = "开始在 Data Nodes 上激活 trigger [{}]";
  public static final String LOG_START_ACTIVE_TRIGGER_ARG_CONFIG_NODES_153A5D40 = "开始在 Config Nodes 上激活 trigger [{}]";
  public static final String LOG_RETRIEVABLE_ERROR_TRYING_CREATE_TRIGGER_ARG_STATE_ARG_44976C4E = "尝试创建 trigger [{}] 时发生可重试错误，状态 [{}]";
  public static final String LOG_START_CONFIG_NODE_INACTIVE_ROLLBACK_TRIGGER_ARG_536929E5 = "开始 trigger [{}] 的 [CONFIG_NODE_INACTIVE] 回滚";
  public static final String LOG_START_DATA_NODE_INACTIVE_ROLLBACK_TRIGGER_ARG_38C93D64 = "开始 trigger [{}] 的 [DATA_NODE_INACTIVE] 回滚";
  public static final String LOG_RETRIEVABLE_ERROR_TRYING_DROP_TRIGGER_ARG_STATE_ARG_2282AC35 = "尝试删除 trigger [{}] 时发生可重试错误，状态 [{}]";
  public static final String LOG_DELETEDATABASEPROCEDURE_PRE_DELETE_DATABASE_ARG_6A1FEACC = "[DeleteDatabaseProcedure] 预删除数据库：{}";
  public static final String LOG_DELETEDATABASEPROCEDURE_INVALIDATE_CACHE_DATABASE_ARG_299FC9BC = "[DeleteDatabaseProcedure] 使数据库 {} 的缓存失效";
  public static final String LOG_DELETEDATABASEPROCEDURE_DELETE_DATABASESCHEMA_ARG_A49A47AC = "[DeleteDatabaseProcedure] 删除数据库 Schema：{}";
  public static final String LOG_DELETEDATABASEPROCEDURE_SUCCESSFULLY_DELETE_SCHEMAREGION_ARG_ARG_BA0535DA = "[DeleteDatabaseProcedure] 成功删除 SchemaRegion[{}]，位置 {}";
  public static final String LOG_DELETEDATABASEPROCEDURE_FAILED_DELETE_SCHEMAREGION_ARG_ARG_SUBMIT_ASYNC_DELETION_8C3E6DE3 = "[DeleteDatabaseProcedure] 删除 SchemaRegion[{}] 失败，位置 {}。提交异步删除。";
  public static final String LOG_DELETEDATABASEPROCEDURE_DATA_PARTITION_POLICY_TABLE_DATABASE_ARG_CLEARED_7A32E28A = "[DeleteDatabaseProcedure] 数据库 {} 的数据分区策略表已清理。";
  public static final String LOG_DELETEDATABASEPROCEDURE_DATABASE_ARG_DELETED_SUCCESSFULLY_3A4E9202 = "[DeleteDatabaseProcedure] 数据库 {} 已成功删除";
  public static final String LOG_DELETEDATABASEPROCEDURE_RETRIABLE_ERROR_TRYING_DELETE_DATABASE_ARG_STATE_ARG_8167D246 = "[DeleteDatabaseProcedure] 尝试删除数据库 {} 时发生可重试错误，状态 {}";
  public static final String LOG_DELETEDATABASEPROCEDURE_ROLLBACK_PREDELETED_ARG_638F53DA = "[DeleteDatabaseProcedure] 回滚到预删除状态：{}";
  public static final String EXCEPTION_FAILED_DAA6EA2F = " 失败 ";
  public static final String EXCEPTION_FAILED_CHECK_TIME_SERIES_EXISTENCE_ALL_REPLICASET_SCHEMAREGION_ARG_FAILURES_5F668154 = "检查 SchemaRegion %s 的所有 replicaset 中的时间序列是否存在失败。失败信息：%s";
  public static final String LOG_FAILED_ROLLBACK_CONFIGNODE_TTL_STATE_9666EF54 = "无法回滚 ConfigNode ttl 状态。";
  public static final String LOG_FAILED_ROLLBACK_DATANODE_TTL_CACHE_436C008A = "无法回滚 DataNode ttl 缓存。";
  public static final String EXCEPTION_ROLLBACK_CONFIGNODE_TTL_FAILED_6D4FB59A = "回滚 ConfigNode ttl 失败，对象：";
  public static final String EXCEPTION_ROLLBACK_DATANODE_TTL_CACHE_FAILED_AF9C7102 = "回滚 DataNode ttl 缓存失败，对象：";
  public static final String LOG_PLEASE_VERIFY_WHETHER_LEADER_CHANGE_HAS_OCCURRED_DURING_STAGE_9FE68EE3 = "请确认该阶段是否发生 leader 变更。";
  public static final String LOG_IF_LOG_TRIGGERED_WITHOUT_LEADER_CHANGE_IT_INDICATES_POTENTIAL_BUG_32AE71FD =
      "如果未发生 leader 变更却触发该日志，说明分区表可能存在潜在问题。";
  public static final String LOG_SKIP_RECOVERING_SCHEDULE_TASK_CQ_ARG_BECAUSE_ITS_METADATA_UNAVAILABLE_00286802 = "跳过恢复 CQ {} 的调度任务，原因：其元数据不可用。";
  public static final String LOG_PROCEDUREID_ARG_ACQUIRE_LOCK_3FBF9987 = "procedureId {} 获取锁。";
  public static final String LOG_PROCEDUREID_ARG_ACQUIRE_LOCK_FAILED_WILL_WAIT_LOCK_AFTER_FINISHING_3B27278E = "procedureId {} 获取锁失败，将在执行完成后等待锁。";
  public static final String LOG_PROCEDUREID_ARG_RELEASE_LOCK_FF860D6B = "procedureId {} 释放锁。";
  public static final String LOG_RETRIEVABLE_ERROR_TRYING_ADD_CONFIG_NODE_ARG_STATE_ARG_D7285810 = "尝试添加 ConfigNode {} 时发生可重试错误，状态 {}";
  public static final String LOG_RETRIEVABLE_ERROR_TRYING_REMOVE_CONFIG_NODE_ARG_STATE_ARG_3754EBA1 = "尝试移除 ConfigNode {} 时发生可重试错误，状态 {}";
  public static final String LOG_PROCEDUREID_ARG_REMOVEDATANODES_SKIPS_ACQUIRING_LOCK_SINCE_UPPER_LAYER_ENSURES_C7546FF8 = "procedureId {}-RemoveDataNodes 跳过获取锁，因为上层保证串行执行。";
  public static final String LOG_PROCEDUREID_ARG_REMOVEDATANODES_SKIPS_RELEASING_LOCK_SINCE_IT_HASN_T_AED8A3DA = "procedureId {}-RemoveDataNodes 跳过释放锁，因为它没有获取任何锁。";
  public static final String LOG_ARG_CAN_NOT_REMOVE_DATANODE_ARG_495F9F85 = "{}, 不能移除 DataNode {} ";
  public static final String LOG_BECAUSE_NUMBER_DATANODES_LESS_EQUAL_THAN_REGION_REPLICA_NUMBER_DEC0CB38 = "因为 DataNode 数量小于或等于 Region 副本数";
  public static final String LOG_ARG_DATANODE_REGIONS_REMOVED_ARG_216A7DC7 = "{}，待移除的 DataNode Region 为 {}";
  public static final String LOG_RETRIEVABLE_ERROR_TRYING_REMOVE_DATA_NODE_ARG_STATE_ARG_4EFEB850 = "尝试移除 DataNode {} 时发生可重试错误，状态 {}";
  public static final String LOG_SUBMIT_REGIONMIGRATEPROCEDURE_REGIONID_ARG_REMOVEDDATANODE_ARG_DESTDATANODE_ARG_COORDINATORFORADDPEER_ARG_ =
      "提交 RegionMigrateProcedure，regionId {}: removedDataNode={}, destDataNode={},"
      + " coordinatorForAddPeer={}, coordinatorForRemovePeer={}";
  public static final String LOG_ARG_CANNOT_FIND_TARGET_DATANODE_MIGRATE_REGION_ARG_81A78E06 = "{}，找不到用于迁移 Region {} 的目标 DataNode";
  public static final String LOG_ARG_SOME_REGIONS_MIGRATED_FAILED_DATANODE_ARG_MIGRATEDFAILEDREGIONS_ARG_11644841 = "{}，DataNode {} 中部分 Regions 迁移失败，migratedFailedRegions：{}。";
  public static final String LOG_REGIONS_HAVE_BEEN_SUCCESSFULLY_MIGRATED_WILL_NOT_ROLL_BACK_YOU_AE904563 = "已成功迁移的 Regions 不会回滚，之后可以再次提交 RemoveDataNodes 任务。";
  public static final String LOG_ARG_DATANODES_ARG_ALL_REGIONS_MIGRATED_SUCCESSFULLY_START_STOP_THEM_32D56F28 = "{}，DataNodes：{} 的所有 Regions 已成功迁移，开始停止它们。";
  public static final String LOG_ARG_START_ROLL_BACK_DATANODES_STATUS_ARG_05C67270 = "{}，开始回滚 DataNodes 状态：{}";
  public static final String LOG_ARG_ROLL_BACK_DATANODES_STATUS_SUCCESSFULLY_ARG_6773A2DF = "{}，成功回滚 DataNodes 状态：{}";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATANODES_REGISTERED_NO_WAY_COLLECT_EARLIEST_TIMESLOTS_WAITING_7025EB23 =
      "[DataPartitionIntegrity] 没有已注册的 DataNode，无法收集最早的 timeslot，等待它们上线";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_COLLECTED_EARLIEST_TIMESLOTS_DATANODE_ID_ARG_ALREADY_OUT_834B62B9 =
      "[DataPartitionIntegrity] 从 DataNode[id={}] 收集最早 timeslot 失败，已超过最大重试时间";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_COLLECTED_EARLIEST_TIMESLOTS_DATANODE_ID_ARG_RESPONSE_STATUS_B0A31EC4 =
      "[DataPartitionIntegrity] 从 DataNode[id={}] 收集最早 timeslot 失败，响应状态为 {}";
  public static final String LOG_COLLECTED_EARLIEST_TIMESLOTS_DATANODE_ID_ARG_ARG_5CDF2BA6 = "已从 DataNode[id={}] 收集最早 timeslot：{}";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_COLLECT_EARLIEST_TIMESLOTS_DATANODE_ID_ARG_ARG_A211840A = "[DataPartitionIntegrity] 从 DataNode[id={}] 收集最早 timeslot 失败：{}";
  public static final String LOG_COLLECTED_EARLIEST_TIMESLOTS_ARG_DATANODES_ARG_NUMBER_SUCCESSFUL_DATANODES_ARG_1CC129EF = "从 {} 个 DataNode 收集最早 timeslot：{}，成功的 DataNode 数量为 {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_MISSING_DATA_PARTITIONS_DETECTED_NOTHING_NEEDS_REPAIRED_TERMINATING_72F2635F =
      "[DataPartitionIntegrity] 未检测到缺失的数据分区，无需修复，终止 procedure";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATA_PARTITION_TABLE_RELATED_DATABASE_ARG_WAS_FOUND_B5B90613 =
      "[DataPartitionIntegrity] 未从 ConfigNode 找到与数据库 {} 相关的数据分区表，需要修复该问题";
  public static final String LOG_DATAPARTITIONINTEGRITY_DATABASE_ARG_HAS_LOST_TIMESLOT_ARG_ITS_DATA_TABLE_499AF395 =
      "[DataPartitionIntegrity] 数据库 {} 在其数据表分区中丢失 timeslot {}，需要修复该问题";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATABASES_HAVE_LOST_DATA_PARTITIONS_TERMINATING_PROCEDURE_3E718CC3 = "[DataPartitionIntegrity] 没有数据库丢失数据分区，终止 procedure";
  public static final String LOG_DATAPARTITIONINTEGRITY_IDENTIFIED_ARG_DATABASES_HAVE_LOST_DATA_PARTITIONS_WILL_REQUEST_6DEA7502 =
      "[DataPartitionIntegrity] 已识别出 {} 个数据库丢失数据分区，将请求 {} 个 DataNode 生成 DataPartitionTable";
  public static final String LOG_REQUESTING_DATAPARTITIONTABLE_GENERATION_ARG_DATANODES_559F97E8 = "正在请求 {} 个 DataNode 生成 DataPartitionTable...";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATANODES_REGISTERED_NO_WAY_REQUESTED_DATAPARTITIONTABLE_GENERATION_TERMINATING_ =
      "[DataPartitionIntegrity] 没有已注册的 DataNode，无法请求生成 DataPartitionTable，终止 procedure";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_REQUEST_DATAPARTITIONTABLE_GENERATION_DATANODE_ID_ARG_ALREADY_OUT_6B0C9351 =
      "[DataPartitionIntegrity] 从 DataNode[id={}] 请求生成 DataPartitionTable 失败，已超过最大重试时间";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_REQUEST_DATAPARTITIONTABLE_GENERATION_DATANODE_ID_ARG_RESPONSE_STATUS_93012D =
      "[DataPartitionIntegrity] 从 DataNode[id={}] 请求生成 DataPartitionTable 失败，响应状态为 {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_REQUEST_DATAPARTITIONTABLE_GENERATION_DATANODE_ID_ARG_ARG_818B47B8 = "[DataPartitionIntegrity] 从 DataNode[id={}] 请求生成 DataPartitionTable 失败：{}";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_REQUEST_DATAPARTITIONTABLE_GENERATION_HEART_BEAT_DATANODE_ID_ARG_2AB63F12 =
      "[DataPartitionIntegrity] 从 DataNode[id={}] 请求 DataPartitionTable 生成心跳失败，已超过最大重试时间";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_REQUEST_DATAPARTITIONTABLE_GENERATION_HEART_BEAT_DATANODE_ID_ARG_DC1702EF =
      "[DataPartitionIntegrity] 从 DataNode[id={}] 请求 DataPartitionTable 生成心跳失败，状态为 {}，响应状态为 {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_DATANODE_ARG_COMPLETED_DATAPARTITIONTABLE_GENERATION_TERMINATING_HEART_BEAT_59DAAD5 =
      "[DataPartitionIntegrity] DataNode {} 已完成 DataPartitionTable 生成，终止心跳";
  public static final String LOG_DATAPARTITIONINTEGRITY_DATANODE_ARG_STILL_GENERATING_DATAPARTITIONTABLE_63F84C78 = "[DataPartitionIntegrity] DataNode {} 仍在生成 DataPartitionTable";
  public static final String LOG_DATAPARTITIONINTEGRITY_DATANODE_ARG_RETURNED_UNKNOWN_ERROR_CODE_ARG_2DA6A21E = "[DataPartitionIntegrity] DataNode {} 返回未知错误码：{}";
  public static final String LOG_DATAPARTITIONINTEGRITY_ERROR_CHECKING_DATAPARTITIONTABLE_STATUS_DATANODE_ARG_ARG_TERMINATING_HEART_D6EDA91 =
      "[DataPartitionIntegrity] 从 DataNode {} 检查 DataPartitionTable 状态出错：{}，终止心跳";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATAPARTITIONTABLES_MERGE_DATAPARTITIONTABLES_EMPTY_920E3DE6 = "[DataPartitionIntegrity] 没有可合并的 DataPartitionTable，dataPartitionTables 为空";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATA_PARTITION_TABLE_RELATED_DATABASE_ARG_WAS_FOUND_D1698512 =
      "[DataPartitionIntegrity] 未从 ConfigNode 找到与数据库 {} 相关的数据分区表，直接使用 DataNode 的数据分区表";
  public static final String LOG_DATAPARTITIONINTEGRITY_DATAPARTITIONTABLE_SUCCESSFULLY_WRITTEN_CONSENSUS_LOG_2B1634A6 = "[DataPartitionIntegrity] DataPartitionTable 已成功写入共识日志";
  public static final String LOG_DATAPARTITIONINTEGRITY_ARG_SERIALIZE_FAILED_DATANODEID_ARG_967B51AA = "[DataPartitionIntegrity] {} 对 dataNodeId：{} 序列化失败";
  public static final String LOG_DATAPARTITIONINTEGRITY_ARG_SERIALIZE_FINALDATAPARTITIONTABLES_FAILED_7E44DCD8 = "[DataPartitionIntegrity] {} 序列化 finalDataPartitionTables 失败";
  public static final String LOG_DATAPARTITIONINTEGRITY_ARG_DESERIALIZE_FAILED_DATANODEID_ARG_22388A60 = "[DataPartitionIntegrity] {} 对 dataNodeId：{} 反序列化失败";
  public static final String LOG_DATAPARTITIONINTEGRITY_ARG_DESERIALIZE_FINALDATAPARTITIONTABLES_FAILED_7E23E4BD = "[DataPartitionIntegrity] {} 反序列化 finalDataPartitionTables 失败";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_DESERIALIZE_DATABASESCOPEDDATAPARTITIONTABLE_3B6933B5 = "[DataPartitionIntegrity] 反序列化失败 DatabaseScopedDataPartitionTable";
  public static final String EXCEPTION_FAILED_C6FF154E = " 失败";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_EXECUTEFROMVALIDATE_97490577 = "SubscriptionHandleLeaderChangeProcedure: executeFromValidate";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES_D4E8BD37 = "SubscriptionHandleLeaderChangeProcedure: executeFromOperateOnConfigNodes";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_FAILED_PULL_COMMIT_PROGRESS_DATANODE_ARG_STATUS_ARG_8C6DEC4E = "SubscriptionHandleLeaderChangeProcedure：拉取 DataNode {} 的提交进度失败，状态：{}";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_FAILED_WRITE_API_EXECUTING_CONSENSUS_LAYER_56B3832A = "SubscriptionHandleLeaderChangeProcedure：写入 API 执行共识层时失败，原因：";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_EXECUTEFROMOPERATEONDATANODES_0D9F7C98 = "SubscriptionHandleLeaderChangeProcedure: executeFromOperateOnDataNodes";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_IGNORED_FAILED_TOPIC_META_PUSH_DATANODE_ARG_STATUS_ARG_67FC003F = "SubscriptionHandleLeaderChangeProcedure：忽略向 DataNode {} 推送 topic 元数据失败，状态：{}";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_IGNORED_FAILED_CONSUMER_GROUP_META_PUSH_DATANODE_ARG_STATUS_17C948 = "SubscriptionHandleLeaderChangeProcedure：忽略向 DataNode {} 推送 consumer group 元数据失败，状态：{}";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_IGNORED_FAILED_SUBSCRIPTION_RUNTIME_PUSH_UNREADABLE_DATANODE_ARG_S = "SubscriptionHandleLeaderChangeProcedure：忽略向不可读 DataNode {} 推送订阅运行时信息失败，状态：{}";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_ROLLBACKFROMVALIDATE_74B408B7 = "SubscriptionHandleLeaderChangeProcedure: rollbackFromValidate";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES_D4C70763 = "SubscriptionHandleLeaderChangeProcedure: rollbackFromOperateOnConfigNodes";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_ROLLBACKFROMOPERATEONDATANODES_0250F6E9 = "SubscriptionHandleLeaderChangeProcedure: rollbackFromOperateOnDataNodes";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_FAILED_DESERIALIZE_REGION_PROGRESS_KEY_ARG_SUMMARY_ARG_F6935E59 = "SubscriptionHandleLeaderChangeProcedure: 反序列化 Region 进度失败，key={}, summary={}";
  public static final String EXCEPTION_FAILED_PUSH_SUBSCRIPTION_RUNTIME_STATE_READABLE_DATANODES_DURING_LEADER_CHANGE_F37E6F2C = "leader 变更期间向可读 DataNode 推送订阅运行时状态失败，详情：%s";
  public static final String EXCEPTION_FAILED_SERIALIZE_REGION_PROGRESS_1769D6F1 = "序列化 Region 进度失败 ";
  public static final String EXCEPTION_NO_READABLE_DATANODE_AVAILABLE_ACCEPT_SUBSCRIPTION_METADATA_RUNTIME_UPDATES_DURING_22E61621 = "leader 变更期间没有可读 DataNode 可接受订阅元数据/运行时更新";
  public static final String LOG_CREATESUBSCRIPTIONPROCEDURE_TOPIC_ARG_USES_CONSENSUS_SUBSCRIPTION_MODE_031CF049 = "CreateSubscriptionProcedure: topic [{}] 使用共识订阅模式 ";
  public static final String LOG_MODE_ARG_SKIPPING_PIPE_CREATION_5F4D1026 = "(mode={})，跳过创建 pipe";
  public static final String LOG_CREATESUBSCRIPTIONPROCEDURE_CONSENSUS_BASED_TOPICS_ARG_WILL_HANDLED_DATANODE_90A9C2FD = "CreateSubscriptionProcedure：基于共识的 topics {} 将由 DataNode 处理";
  public static final String LOG_VIA_CONSUMER_GROUP_META_PUSH_NO_PIPE_CREATION_NEEDED_D56CFE31 = "通过 consumer group 元数据推送（无需创建 pipe）";
  public static final String LOG_DROPSUBSCRIPTIONPROCEDURE_TOPIC_ARG_USES_CONSENSUS_SUBSCRIPTION_MODE_6962D13C = "DropSubscriptionProcedure: topic [{}] 使用共识订阅模式 ";
  public static final String LOG_MODE_ARG_SKIPPING_PIPE_REMOVAL_133B0CD6 = "(mode={})，跳过移除 pipe";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_ACQUIRELOCK_SKIP_PROCEDURE_LAST_EXECUTION_TIME_ARG_CE3DD247 = "CommitProgressSyncProcedure：acquireLock，因上次执行时间 {} 跳过该 procedure";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_EXECUTEFROMVALIDATE_CF220E1F = "CommitProgressSyncProcedure: executeFromValidate";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES_0DC818CA = "CommitProgressSyncProcedure: executeFromOperateOnConfigNodes";
  public static final String LOG_FAILED_PULL_COMMIT_PROGRESS_DATANODE_ARG_STATUS_ARG_33037B29 = "拉取 DataNode {} 的提交进度失败，状态：{}";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_EXECUTEFROMOPERATEONDATANODES_NO_OP_34420360 = "CommitProgressSyncProcedure: executeFromOperateOnDataNodes（无操作）";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_ROLLBACKFROMVALIDATE_2309D4D2 = "CommitProgressSyncProcedure: rollbackFromValidate";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES_57CB907B = "CommitProgressSyncProcedure: rollbackFromOperateOnConfigNodes";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_ROLLBACKFROMOPERATEONDATANODES_0D2CEB50 = "CommitProgressSyncProcedure: rollbackFromOperateOnDataNodes";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_FAILED_DESERIALIZE_REGION_PROGRESS_KEY_ARG_SUMMARY_ARG_0202F658 = "CommitProgressSyncProcedure: 反序列化 Region 进度失败，key={}, summary={}";
  public static final String EXCEPTION_UNEXPECTED_PARENT_444B4289 = "非预期父节点";
  public static final String LOG_ARG_8393DD4A = "{}";
  public static final String MESSAGE_HALT_PID_ARG_ACTIVECOUNT_ARG_411F3EBF = "停止 pid={}，activeCount={}";
  public static final String MESSAGE_EXCEPTION_HAPPENED_WHEN_WORKER_ARG_EXECUTE_PROCEDURE_ARG_6E3AD27D = "worker {} 执行 procedure {} 时发生异常";
  public static final String EXCEPTION_CANNOT_DERIVE_A_COLLISION_FREE_DELETE_TASKID_PROCID_ARG_DELETETASKSEQ_ARG_EXCEED_THE_71B7046A =
      "无法推导出无冲突的 delete taskId：procId=%d，deleteTaskSeq=%d 超出了 ";
  public static final String EXCEPTION_CANNOT_DERIVE_A_COLLISION_FREE_DELETE_TASKID_PROCID_ARG_DELETETASKSEQ_ARG_EXCEED_THE_ARG_ARG_BIT_BUDGET_015C598D =
      "无法推导出无冲突的 delete taskId：procId=%d，deleteTaskSeq=%d 超出了 %d/%d 位的预算";
  public static final String MESSAGE_FAILED_TO_SHOW_DATAPARTITIONTABLE_INTEGRITY_CHECK_PROGRESS_5EE98694 = "显示 DataPartitionTable 完整性检查进度失败";
  public static final String MESSAGE_ENCOUNTERED_UNEXPECTED_DATAPARTITIONTABLEINTEGRITYCHECKPROCEDURESTATE_ARG_WHEN_SHOWING_PROGRESS_5FA2739F =
      "显示进度时遇到非预期的 DataPartitionTableIntegrityCheckProcedureState {}";
  public static final String MESSAGE_UNEXPECTED_DATAPARTITIONTABLEINTEGRITYCHECKPROCEDURESTATE_ARG_WHEN_SHOWING_PROGRESS_D3C07BA1 =
      "非预期的 DataPartitionTableIntegrityCheckProcedureState {}（显示进度时）";

}
