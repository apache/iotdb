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
      "AddNeverFinishSubProcedureProcedure run again, which should never happen";
  public static final String ADDREGIONLOCATION_FINISHED_ADD_REGION_TO_RESULT_IS =
      "AddRegionLocation finished, add region {} to {}, result is {}";
  public static final String ADDTABLECOLUMN_COSTS_MS = "AddTableColumn-{}.{}-{} costs {}ms";
  public static final String ADD_COLUMN_TO_TABLE = "Add column to table {}.{}";
  public static final String ADD_CONFIGNODE_FAILED = "Add ConfigNode failed ";
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
      "Altering column {} in {}.{} on configNode";
  public static final String ALTERING_TIME_SERIES_DATA_TYPE = "altering time series {} data type";
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
  public static final String
      ALTERTOPICPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES_TRY_TO_ALTER_TOPIC =
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
      "Alter encoding compressor %s in schema regions failed. Failures: %s";
  public static final String ALTER_ENCODING_COMPRESSOR_IN_SCHEMA_REGION_FOR_TIMESERIES =
      "Alter encoding {} & compressor {} in schema region for timeSeries {}";
  public static final String ALTER_TIMESERIES_DATA_TYPE_TO_IN_SCHEMA_REGIONS_FAILED_FAILURES =
      "Alter timeseries %s data type to %s in schema regions failed. Failures: %s";
  public static final String ALTER_TIME_SERIES_DATA_TYPE_FAILED =
      "alter time series {} data type failed";
  public static final String ALTER_VIEW = "Alter view {}";
  public static final String ALTER_VIEW_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN_ALL =
      "Alter view %s failed when [%s] because failed to execute in all replicaset of schemaRegion %s. Failure nodes: %s, statuses: %s";
  public static final String AUTHENTICATION_FAILED = "Authentication failed.";
  public static final String AUTH_PROCEDURE_CLEAN_DATANODE_CACHE_SUCCESSFULLY =
      "Auth procedure: clean datanode cache successfully";
  public static final String BEGIN_TO_CHANGE_DATANODE_STATUS_NODESTATUSMAP =
      "{}, Begin to change DataNode status, nodeStatusMap: {}";
  public static final String BEGIN_TO_STOP_DATANODES_AND_KILL_THE_DATANODE_PROCESS =
      "{}, Begin to stop DataNodes and kill the DataNode process: {}";
  public static final String BROADCASTDATANODESTATUSCHANGE_FINISHED_DATANODE =
      "{}, BroadcastDataNodeStatusChange finished, dataNode: {}";
  public static final String
      BROADCASTDATANODESTATUSCHANGE_MEETS_ERROR_STATUS_CHANGE_DATANODES_ERROR_DATANODE =
          "{}, BroadcastDataNodeStatusChange meets error, status change dataNodes: {}, error datanode: {}";
  public static final String BROADCASTDATANODESTATUSCHANGE_START_DATANODE =
      "{}, BroadcastDataNodeStatusChange start, dataNode: {}";
  public static final String CALL_CHANGEREGIONLEADER_FAIL_FOR_THE_TIME_WILL_SLEEP_MS =
      "Call changeRegionLeader fail for the {} time, will sleep {} ms";
  public static final String CANNOT_FIND_DATANODES_CONTAIN_THE_GIVEN_REGION =
      "Cannot find DataNodes contain the given region: {}";
  public static final String CANNOT_FIND_REGION_REPLICA_NODES_IN_CREATEPEER_REGIONID =
      "{}, Cannot find region replica nodes in createPeer, regionId: {}";
  public static final String CANNOT_FIND_REGION_REPLICA_NODES_REGION =
      "Cannot find region replica nodes, region: {}";
  public static final String
      CATCH_EXCEPTION_WHILE_DESERIALIZING_PROCEDURE_THIS_PROCEDURE_WILL_BE_IGNORED =
          "Catch exception while deserializing procedure, this procedure will be ignored.";
  public static final String CHANGE_REGION_LEADER_FINISHED_REGIONID_NEWLEADERNODE =
      "{}, Change region leader finished, regionId: {}, newLeaderNode: {}";
  public static final String CHECK_AND_INVALIDATE_COLUMN_IN_WHEN_ALTERING_COLUMN_DATA_TYPE =
      "Check and invalidate column {} in {}.{} when altering column data type";
  public static final String CHECK_AND_INVALIDATE_COLUMN_IN_WHEN_DROPPING_COLUMN =
      "Check and invalidate column {} in {}.{} when dropping column";
  public static final String CHECK_AND_INVALIDATE_SERIES_WHEN_ALTERING_TIME_SERIES_DATA_TYPE =
      "Check and invalidate series {} when altering time series data type";
  public static final String CHECK_AND_INVALIDATE_TABLE_WHEN_DROPPING_TABLE =
      "Check and invalidate table {}.{} when dropping table";
  public static final String CHECK_DATANODE_TEMPLATE_ACTIVATION_OF_TEMPLATE_SET_ON =
      "Check DataNode template activation of template {} set on {}";
  public static final String CHECK_TEMPLATE_EXISTENCE_SET_ON_PATH_WHEN_TRY_SETTING_TEMPLATE =
      "Check template existence set on path {} when try setting template {}";
  public static final String CHECK_THE_EXISTENCE_OF_TABLE = "Check the existence of table {}.{}";
  public static final String CHECK_TIMESERIES_EXISTENCE_UNDER_PATH_WHEN_TRY_SETTING_TEMPLATE =
      "Check timeseries existence under path {} when try setting template {}";
  public static final String CLEARING_CACHE_AFTER_ALTER_TIME_SERIES_DATA_TYPE =
      "clearing cache after alter time series {} data type";
  public static final String COLUMN_CHECK_FOR_TABLE_WHEN_ADDING_COLUMN =
      "Column check for table {}.{} when adding column";
  public static final String COLUMN_CHECK_FOR_TABLE_WHEN_RENAMING_COLUMN =
      "Column check for table {}.{} when renaming column";
  public static final String COLUMN_CHECK_FOR_TABLE_WHEN_RENAMING_TABLE =
      "Column check for table {}.{} when renaming table";
  public static final String COMMIT_CREATE_TABLE = "Commit create table {}.{}";
  public static final String COMMIT_RELEASE_INFO_OF_TABLE_WHEN_ADDING_COLUMN =
      "Commit release info of table {}.{} when adding column";
  public static final String COMMIT_RELEASE_INFO_OF_TABLE_WHEN_ALTERING_COLUMN =
      "Commit release info of table {}.{} when altering column";
  public static final String COMMIT_RELEASE_INFO_OF_TABLE_WHEN_RENAMING_COLUMN =
      "Commit release info of table {}.{} when renaming column";
  public static final String COMMIT_RELEASE_INFO_OF_TABLE_WHEN_RENAMING_TABLE =
      "Commit release info of table {}.{} when renaming table";
  public static final String COMMIT_RELEASE_INFO_OF_TABLE_WHEN_SETTING_PROPERTIES =
      "Commit release info of table {}.{} when setting properties";
  public static final String COMMIT_RELEASE_SCHEMAENGINE_TEMPLATE_SET_ON_PATH =
      "Commit release schemaengine template {} set on path {}";
  public static final String COMMIT_RELEASE_TABLE = "Commit release table {}.{}";
  public static final String COMMIT_SET_SCHEMAENGINE_TEMPLATE_ON_PATH =
      "Commit set schemaengine template {} on path {}";
  public static final String
      CONSENSUSPIPEGUARDIAN_CONSENSUS_PIPE_IS_STOPPED_RESTARTING_ASYNCHRONOUSLY =
          "[ConsensusPipeGuardian] consensus pipe [{}] is stopped, restarting asynchronously";
  public static final String CONSENSUSPIPEGUARDIAN_CONSENSUS_PIPE_MISSING_CREATING_ASYNCHRONOUSLY =
      "[ConsensusPipeGuardian] consensus pipe [{}] missing, creating asynchronously";
  public static final String
      CONSENSUSPIPEGUARDIAN_UNEXPECTED_CONSENSUS_PIPE_EXISTS_DROPPING_ASYNCHRONOUSLY =
          "[ConsensusPipeGuardian] unexpected consensus pipe [{}] exists, dropping asynchronously";
  public static final String CONSTRUCT_SCHEMAENGINE_BLACK_LIST_OF_DEVICES_IN =
      "Construct schemaEngine black list of devices in {}.{}";
  public static final String CONSTRUCT_SCHEMAENGINE_BLACK_LIST_OF_TEMPLATE_SET_ON =
      "Construct schemaengine black list of template {} set on {}";
  public static final String CONSTRUCT_SCHEMAENGINE_BLACK_LIST_OF_TIMESERIES =
      "Construct schemaEngine black list of timeSeries {}";
  public static final String CONSTRUCT_SCHEMA_BLACK_LIST_WITH_TEMPLATE =
      "Construct schema black list with template {}";
  public static final String CONSTRUCT_VIEW_SCHEMAENGINE_BLACK_LIST_OF_VIEW =
      "Construct view schemaengine black list of view {}";
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
  public static final String CREATEDATABASE_FAIL_TWICE = "createDatabase fail twice";
  public static final String CREATED_CONSENSUS_PIPE = "{}, Created consensus pipe {}";
  public static final String CREATEPIPEPLUGINPROCEDURE_EXECUTEFROMCREATEONCONFIGNODES =
      "CreatePipePluginProcedure: executeFromCreateOnConfigNodes({})";
  public static final String CREATEPIPEPLUGINPROCEDURE_EXECUTEFROMCREATEONDATANODES =
      "CreatePipePluginProcedure: executeFromCreateOnDataNodes({})";
  public static final String CREATEPIPEPLUGINPROCEDURE_EXECUTEFROMLOCK =
      "CreatePipePluginProcedure: executeFromLock({})";
  public static final String CREATEPIPEPLUGINPROCEDURE_EXECUTEFROMUNLOCK =
      "CreatePipePluginProcedure: executeFromUnlock({})";
  public static final String CREATEPIPEPLUGINPROCEDURE_FAILED_IN_STATE_WILL_ROLLBACK =
      "CreatePipePluginProcedure failed in state {}, will rollback";
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
  public static final String
      CREATEREGIONGROUPS_ALL_REPLICAS_OF_REGIONGROUP_ARE_CREATED_SUCCESSFULLY =
          "[CreateRegionGroups] All replicas of RegionGroup: {} are created successfully!";
  public static final String
      CREATEREGIONGROUPS_FAILED_TO_CREATE_MOST_OF_REPLICAS_IN_REGIONGROUP_THE =
          "[CreateRegionGroups] Failed to create most of replicas in RegionGroup: {}, The redundant replicas in this RegionGroup will be deleted.";
  public static final String
      CREATEREGIONGROUPS_FAILED_TO_CREATE_SOME_REPLICAS_OF_REGIONGROUP_BUT_THIS =
          "[CreateRegionGroups] Failed to create some replicas of RegionGroup: {}, but this RegionGroup can still be used.";
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
      "{}, DataNode {} is {}, submit DELETE_OLD_REGION_PEER with a single RPC attempt and let RemoveRegionPeerProcedure handle retries.";
  public static final String DEACTIVATETEMPLATE_COSTS_MS = "DeactivateTemplate-[{}] costs {}ms";
  public static final String DEACTIVATE_TEMPLATE_OF = "Deactivate template of {}";
  public static final String DEACTIVATE_TEMPLATE_OF_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN =
      "Deactivate template of %s failed when [%s] because failed to execute in all replicaset of %s %s. Failure: %s";
  public static final String DELETEDATABASEPROCEDURE_DELETE_DATABASE =
      "[DeleteDatabaseProcedure] Delete database ";
  public static final String DELETEDATABASEPROCEDURE_DELETE_DATABASESCHEMA_FAILED =
      "[DeleteDatabaseProcedure] Delete DatabaseSchema failed";
  public static final String DELETEDATABASEPROCEDURE_INVALIDATE_CACHE_FAILED =
      "[DeleteDatabaseProcedure] Invalidate cache failed";
  public static final String DELETEDATABASEPROCEDURE_STATE_STUCK_AT =
      "[DeleteDatabaseProcedure] State stuck at ";
  public static final String DELETEDEVICES_COSTS_MS = "DeleteDevices-[{}] costs {}ms";
  public static final String DELETELOGICALVIEW_COSTS_MS = "DeleteLogicalView-[{}] costs {}ms";
  public static final String DELETETIMESERIES_COSTS_MS = "DeleteTimeSeries-[{}] costs {}ms";
  public static final String DELETE_DATA_OF_DEVICES_IN = "Delete data of devices in {}.{}";
  public static final String DELETE_DATA_OF_TEMPLATE_TIMESERIES =
      "Delete data of template timeSeries {}";
  public static final String DELETE_DATA_OF_TIMESERIES = "Delete data of timeSeries {}";
  public static final String DELETE_DEVICES_IN_IN_SCHEMAENGINE =
      "Delete devices in {}.{} in schemaEngine";
  public static final String DELETE_TIMESERIES_SCHEMAENGINE_OF =
      "Delete timeSeries schemaEngine of {}";
  public static final String DELETE_TIME_SERIES_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN =
      "Delete time series %s failed when [%s] because failed to execute in all replicaset of %s %s. Failures: %s";
  public static final String DELETE_VIEW_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN_ALL =
      "Delete view %s failed when [%s] because failed to execute in all replicaset of schemaRegion %s. Failures: %s";
  public static final String DELETE_VIEW_SCHEMAENGINE_OF = "Delete view schemaengine of {}";
  public static final String DELETING_DATA_FOR_TABLE = "Deleting data for table {}.{}";
  public static final String DELETING_DEVICES_FOR_TABLE_WHEN_DROPPING_TABLE =
      "Deleting devices for table {}.{} when dropping table";
  public static final String DESERIALIZE_MEETS_ERROR_IN_CREATEREGIONGROUPSPROCEDURE =
      "Deserialize meets error in CreateRegionGroupsProcedure";
  public static final String DROPPING_COLUMN_IN_ON_CONFIGNODE =
      "Dropping column {} in {}.{} on configNode";
  public static final String DROPPING_TABLE_ON_CONFIGNODE = "Dropping table {}.{} on configNode";
  public static final String DROPPIPEPLUGINPROCEDURE_EXECUTEFROMDROPONCONFIGNODES =
      "DropPipePluginProcedure: executeFromDropOnConfigNodes({})";
  public static final String DROPPIPEPLUGINPROCEDURE_EXECUTEFROMDROPONDATANODES =
      "DropPipePluginProcedure: executeFromDropOnDataNodes({})";
  public static final String DROPPIPEPLUGINPROCEDURE_EXECUTEFROMLOCK =
      "DropPipePluginProcedure: executeFromLock({})";
  public static final String DROPPIPEPLUGINPROCEDURE_EXECUTEFROMUNLOCK =
      "DropPipePluginProcedure: executeFromUnlock({})";
  public static final String DROPPIPEPLUGINPROCEDURE_FAILED_IN_STATE_WILL_ROLLBACK =
      "DropPipePluginProcedure failed in state {}, will rollback";
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
  public static final String ERROR_IN_DESERIALIZE = "Error in deserialize {}";
  public static final String ERROR_IN_DESERIALIZE_PROCID_THIS_PROCEDURE_WILL_BE_IGNORED_IT =
      "Error in deserialize {} (procID {}). This procedure will be ignored. It may belong to old version and cannot be used now.";
  public static final String EXECUTE_AUTH_PLAN_SUCCESS_TO_INVALIDATE_DATANODES =
      "Execute auth plan {} success. To invalidate datanodes: {}";
  public static final String EXECUTING_ON_REGION_FOR_COLUMN_IN_WHEN_DROPPING_COLUMN =
      "Executing on region for column {} in {}.{} when dropping column";
  public static final String FAILED_TO_ACTIVE_CQ_BECAUSE_OF_NO_SUCH_CQ =
      "Failed to active CQ {} because of no such cq: {}";
  public static final String FAILED_TO_ACTIVE_CQ_BECAUSE_THIS_CQ_HAS_ALREADY_BEEN =
      "Failed to active CQ {} because this cq has already been active";
  public static final String FAILED_TO_ACTIVE_CQ_SUCCESSFULLY_BECAUSE_OF_UNKNOWN_REASONS =
      "Failed to active CQ {} successfully because of unknown reasons {}";
  public static final String FAILED_TO_ALTER_CONSUMER_GROUP_ON_CONFIG_NODES_BECAUSE =
      "Failed to alter consumer group %s on config nodes, because %s";
  public static final String FAILED_TO_ALTER_CONSUMER_GROUP_ON_DATA_NODES_BECAUSE =
      "Failed to alter consumer group (%s -> %s) on data nodes, because %s";
  public static final String FAILED_TO_ALTER_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER =
      "Failed to alter pipe {}, details: {}, metadata will be synchronized later.";
  public static final String FAILED_TO_ALTER_TOPIC_ON_CONFIG_NODES_BECAUSE =
      "Failed to alter topic (%s -> %s) on config nodes, because %s";
  public static final String FAILED_TO_ALTER_TOPIC_ON_DATA_NODES_BECAUSE =
      "Failed to alter topic (%s -> %s) on data nodes, because %s";
  public static final String FAILED_TO_CHANGE_DATANODE_STATUS_DATANODEID_NODESTATUS =
      "{}, Failed to change DataNode status, dataNodeId={}, nodeStatus={}";
  public static final String FAILED_TO_COMMIT_SET_TEMPLATE_ON_PATH_DUE_TO =
      "Failed to commit set template {} on path {} due to {}";
  public static final String FAILED_TO_CREATE_CONSENSUS_PIPE =
      "{}, Failed to create consensus pipe {}: {}";
  public static final String
      FAILED_TO_CREATE_PIPES_WHEN_CREATING_SUBSCRIPTION_WITH_REQUEST_DETAILS =
          "Failed to create pipes %s when creating subscription with request %s, details: %s, metadata will be synchronized later.";
  public static final String FAILED_TO_CREATE_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER =
      "Failed to create pipe {}, details: {}, metadata will be synchronized later.";
  public static final String FAILED_TO_CREATE_PIPE_PLUGIN_INSTANCE_ON_DATA_NODES =
      "Failed to create pipe plugin instance [%s] on data nodes";
  public static final String FAILED_TO_CREATE_SUBSCRIPTION_WITH_REQUEST_ON_CONFIG_NODES_BECAUSE =
      "Failed to create subscription with request %s on config nodes, because %s";
  public static final String FAILED_TO_CREATE_TOPIC_ON_CONFIG_NODES_BECAUSE =
      "Failed to create topic %s on config nodes, because %s";
  public static final String FAILED_TO_CREATE_TOPIC_ON_DATA_NODES_BECAUSE =
      "Failed to create topic %s on data nodes, because %s";
  public static final String FAILED_TO_DESERIALIZE_DATAPARTITIONTABLES =
      "Failed to deserialize dataPartitionTables";
  public static final String FAILED_TO_DESERIALIZE_FINALDATAPARTITIONTABLES =
      "Failed to deserialize finalDataPartitionTables";
  public static final String FAILED_TO_DO_INACTIVE_ROLLBACK_OF_CQ_BECAUSE_OF_NO =
      "Failed to do [INACTIVE] rollback of CQ {} because of no such cq: {}";
  public static final String FAILED_TO_DO_INACTIVE_ROLLBACK_OF_CQ_BECAUSE_OF_UNKNOWN =
      "Failed to do [INACTIVE] rollback of CQ {} because of unknown reasons {}";
  public static final String FAILED_TO_DROP_PIPES_WHEN_DROPPING_SUBSCRIPTION_WITH_REQUEST_BECAUSE =
      "Failed to drop pipes %s when dropping subscription with request %s, because %s";
  public static final String FAILED_TO_DROP_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER =
      "Failed to drop pipe {}, details: {}, metadata will be synchronized later.";
  public static final String FAILED_TO_DROP_PIPE_PLUGIN_ON_DATA_NODES =
      "Failed to drop pipe plugin %s on data nodes";
  public static final String FAILED_TO_DROP_SUBSCRIPTION_WITH_REQUEST_ON_CONFIG_NODES_BECAUSE =
      "Failed to drop subscription with request %s on config nodes, because %s";
  public static final String FAILED_TO_DROP_TOPIC_ON_CONFIG_NODES_BECAUSE =
      "Failed to drop topic %s on config nodes, because %s";
  public static final String FAILED_TO_DROP_TOPIC_ON_DATA_NODES_BECAUSE =
      "Failed to drop topic %s on data nodes, because %s";
  public static final String FAILED_TO_EXECUTE_IN_ALL_REPLICASET_OF_SCHEMAREGION_WHEN_CHECKING =
      "Failed to execute in all replicaset of schemaRegion %s when checking templates on path %s. Failures: %s";
  public static final String FAILED_TO_EXECUTE_IN_ALL_REPLICASET_OF_SCHEMAREGION_WHEN_CHECKING_2 =
      "Failed to execute in all replicaset of schemaRegion %s when checking the template %s on %s. Failure nodes: %s";
  public static final String FAILED_TO_EXECUTE_PLAN_BECAUSE =
      "Failed to execute plan {} because {}";
  public static final String FAILED_TO_FOR_TABLE_TO_DATANODE_FAILURE_RESULTS =
      "Failed to {} for table {}.{} to DataNode, failure results: {}";
  public static final String FAILED_TO_INIT_CQ_BECAUSE_OF_UNKNOWN_REASONS =
      "Failed to init CQ {} because of unknown reasons {}";
  public static final String FAILED_TO_INIT_CQ_BECAUSE_SUCH_CQ_ALREADY_EXISTS =
      "Failed to init CQ {} because such cq already exists";
  public static final String FAILED_TO_INVALIDATE_COLUMN_S_CACHE_OF_TABLE =
      "Failed to invalidate {} column {}'s cache of table {}.{}";
  public static final String FAILED_TO_INVALIDATE_SCHEMAENGINE_CACHE_OF_DEVICES_IN_TABLE =
      "Failed to invalidate schemaEngine cache of devices in table {}.{}";
  public static final String FAILED_TO_INVALIDATE_SCHEMAENGINE_CACHE_OF_TABLE =
      "Failed to invalidate schemaEngine cache of table {}.{}";
  public static final String FAILED_TO_INVALIDATE_SCHEMAENGINE_CACHE_OF_TIMESERIES =
      "Failed to invalidate schemaEngine cache of timeSeries {}";
  public static final String FAILED_TO_INVALIDATE_SCHEMAENGINE_CACHE_OF_VIEW =
      "Failed to invalidate schemaengine cache of view {}";
  public static final String FAILED_TO_INVALIDATE_SCHEMA_CACHE_OF_TEMPLATE_TIMESERIES =
      "Failed to invalidate schema cache of template timeSeries {}";
  public static final String FAILED_TO_INVALIDATE_TEMPLATE_CACHE_OF_TEMPLATE_SET_ON =
      "Failed to invalidate template cache of template {} set on {}";
  public static final String FAILED_TO_PRE_RELEASE_FOR_TABLE_TO_DATANODE_FAILURE_RESULTS =
      "Failed to pre-release {} for table {}.{} to DataNode, failure results: {}";
  public static final String FAILED_TO_PRE_SET_TEMPLATE_ON_PATH_DUE_TO =
      "Failed to pre set template {} on path {} due to {}";
  public static final String FAILED_TO_PROVE_DN_IS_FENCED = "Failed to prove DN is fenced";
  public static final String FAILED_TO_PUSH_CONSUMER_GROUP_META_TO_DATANODES_DETAILS =
      "Failed to push consumer group meta to dataNodes, details: %s";
  public static final String FAILED_TO_PUSH_PIPE_META_LIST_TO_DATA_NODES_WILL =
      "Failed to push pipe meta list to data nodes, will retry later.";
  public static final String FAILED_TO_PUSH_PIPE_META_TO_DATANODES_DETAILS =
      "Failed to push pipe meta to dataNodes, details: %s";
  public static final String FAILED_TO_PUSH_TOPIC_META_TO_DATANODES_DETAILS =
      "Failed to push topic meta to dataNodes, details: %s";
  public static final String FAILED_TO_REMOVE_DATA_NODE_BECAUSE_IT_IS_NOT_IN =
      "Failed to remove data node {} because it is not in running and the configuration of cluster is one replication";

  public static final String FAILED_TO_REMOVE_DATA_NODE_WOULD_LEAVE_TOO_FEW =
      "Cannot remove %d DataNode(s): the cluster has %d available DataNode(s) and must retain at least %d of them (max(schema_replication_factor=%d, data_replication_factor=%d)) so that every region keeps enough replicas, but this request would leave only %d.";
  public static final String FAILED_TO_REMOVE_DATA_NODE_SINGLE_REPLICA_HINT =
      " With a single replica there is nowhere to migrate regions to, so at least one DataNode must always remain.";
  public static final String FAILED_TO_ROLLBACK_ALTER_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED =
      "Failed to rollback alter pipe {}, details: {}, metadata will be synchronized later.";
  public static final String FAILED_TO_ROLLBACK_COMMIT_SET_TEMPLATE_ON_PATH_DUE_TO =
      "Failed to rollback commit set template {} on path {} due to {}";
  public static final String
      FAILED_TO_ROLLBACK_CREATE_PIPES_WHEN_CREATING_SUBSCRIPTION_WITH_REQUEST =
          "Failed to rollback create pipes when creating subscription with request %s, because %s";
  public static final String FAILED_TO_ROLLBACK_CREATE_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED =
      "Failed to rollback create pipe {}, details: {}, metadata will be synchronized later.";
  public static final String FAILED_TO_ROLLBACK_CREATING_SUBSCRIPTION_WITH_REQUEST_ON_CONFIG_NODES =
      "Failed to rollback creating subscription with request %s on config nodes, because %s";
  public static final String FAILED_TO_ROLLBACK_CREATING_TOPIC_ON_CONFIG_NODES_BECAUSE =
      "Failed to rollback creating topic %s on config nodes, because %s";
  public static final String FAILED_TO_ROLLBACK_CREATING_TOPIC_ON_DATA_NODES_BECAUSE =
      "Failed to rollback creating topic %s on data nodes, because %s";
  public static final String FAILED_TO_ROLLBACK_FROM_ALTERING_CONSUMER_GROUP_ON_CONFIG_NODES =
      "Failed to rollback from altering consumer group (%s -> %s) on config nodes, because %s";
  public static final String FAILED_TO_ROLLBACK_FROM_ALTERING_CONSUMER_GROUP_ON_DATA_NODES =
      "Failed to rollback from altering consumer group (%s -> %s) on data nodes, because %s";
  public static final String FAILED_TO_ROLLBACK_FROM_ALTERING_TOPIC_ON_CONFIG_NODES_BECAUSE =
      "Failed to rollback from altering topic (%s -> %s) on config nodes, because %s";
  public static final String FAILED_TO_ROLLBACK_FROM_ALTERING_TOPIC_ON_DATA_NODES_BECAUSE =
      "Failed to rollback from altering topic (%s -> %s) on data nodes, because %s";
  public static final String FAILED_TO_ROLLBACK_PIPE_PLUGIN_ON_DATA_NODES =
      "Failed to rollback pipe plugin [%s] on data nodes";
  public static final String FAILED_TO_ROLLBACK_PRE_RELEASE_FOR_TABLE_INFO_TO_DATANODE =
      "Failed to rollback pre-release {} for table {}.{} info to DataNode, failure results: {}";
  public static final String FAILED_TO_PROVE_AN_UNREACHABLE_DN_IS_FENCED = "Failed to prove an unreachable DataNode is fenced";
  public static final String FAILED_TO_ROLLBACK_PRE_RELEASE_TEMPLATE_INFO_OF_TEMPLATE_SET =
      "Failed to rollback pre release template info of template {} set on path {} on DataNode {}";
  public static final String FAILED_TO_ROLLBACK_PRE_SET_TEMPLATE_ON_PATH_DUE_TO =
      "Failed to rollback pre set template {} on path {} due to {}";
  public static final String FAILED_TO_ROLLBACK_PRE_UNSET_TEMPLATE_OPERATION_OF_TEMPLATE_SET =
      "Failed to rollback pre unset template operation of template {} set on {}";
  public static final String FAILED_TO_ROLLBACK_START_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED =
      "Failed to rollback start pipe {}, details: {}, metadata will be synchronized later.";
  public static final String FAILED_TO_ROLLBACK_STOP_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED =
      "Failed to rollback stop pipe {}, details: {}, metadata will be synchronized later.";
  public static final String FAILED_TO_ROLLBACK_TABLE_CREATION =
      "Failed to rollback table creation {}.{}";
  public static final String FAILED_TO_ROLLBACK_TEMPLATE_CACHE_OF_TEMPLATE_SET_ON =
      "Failed to rollback template cache of template {} set on {}";
  public static final String FAILED_TO_SERIALIZE_DATAPARTITIONTABLES =
      "Failed to serialize dataPartitionTables";
  public static final String FAILED_TO_SERIALIZE_FAILEDDATANODE =
      "Failed to serialize failedDataNode";
  public static final String FAILED_TO_SERIALIZE_FINALDATAPARTITIONTABLES =
      "Failed to serialize finalDataPartitionTables";
  public static final String FAILED_TO_SERIALIZE_SKIPDATANODE = "Failed to serialize skipDataNode";
  public static final String FAILED_TO_SET_SCHEMAENGINE_TEMPLATE_ON_PATH_BECAUSE_THERE_S =
      "Failed to set schemaengine template %s on path %s because there's failure on DataNode %s";
  public static final String FAILED_TO_START_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER =
      "Failed to start pipe {}, details: {}, metadata will be synchronized later.";
  public static final String FAILED_TO_STOP_AINODE_BECAUSE_BUT_THE_REMOVE_PROCESS_WILL =
      "Failed to stop AINode {} because {}, but the remove process will continue.";
  public static final String FAILED_TO_STOP_PIPE_DETAILS_METADATA_WILL_BE_SYNCHRONIZED_LATER =
      "Failed to stop pipe {}, details: {}, metadata will be synchronized later.";
  public static final String FAILED_TO_SYNC_TABLE_COMMIT_CREATE_INFO_TO_DATANODE_FAILURE =
      "Failed to sync table {}.{} commit-create info to DataNode {}, failure results: ";
  public static final String FAILED_TO_SYNC_TABLE_PRE_CREATE_INFO_TO_DATANODE_FAILURE =
      "Failed to sync table {}.{} pre-create info to DataNode, failure results: {}";
  public static final String FAILED_TO_SYNC_TABLE_ROLLBACK_CREATE_INFO_TO_DATANODE_FAILURE =
      "Failed to sync table {}.{} rollback-create info to DataNode, failure results: {}";
  public static final String FAILED_TO_SYNC_TEMPLATE_COMMIT_SET_INFO_ON_PATH_TO =
      "Failed to sync template {} commit-set info on path {} to DataNode {}";
  public static final String FAILED_TO_SYNC_TEMPLATE_PRE_SET_INFO_ON_PATH_TO =
      "Failed to sync template {} pre-set info on path {} to DataNode {}";
  public static final String FAILED_TO_UPDATE_PROCEDURE = "Failed to update procedure {}";
  public static final String FAILED_TO_UPDATE_TTL_CACHE_OF_DATANODE =
      "Failed to update ttl cache of dataNode.";
  public static final String FAILED_TO_WRITE_DATAPARTITIONTABLE_TO_CONSENSUS_LOG =
      "Failed to write DataPartitionTable to consensus log";
  public static final String FAIL_IN_CREATECQPROCEDURE = "Fail in CreateCQProcedure";
  public static final String FAIL_TO_ACTIVE_TRIGGERINSTANCE_ON_DATA_NODES =
      "Fail to active triggerInstance [%s] on Data Nodes";
  public static final String FAIL_TO_CONFIG_NODE_INACTIVE_ROLLBACK_OF_TRIGGER =
      "Fail to [CONFIG_NODE_INACTIVE] rollback of trigger [%s]";
  public static final String FAIL_TO_CREATE_PIPE_PLUGIN_AFTER_RETRIES =
      "Fail to create pipe plugin [{}] after {} retries";
  public static final String FAIL_TO_CREATE_TRIGGERINSTANCE_ON_DATA_NODES =
      "Fail to create triggerInstance [%s] on Data Nodes";
  public static final String FAIL_TO_CREATE_TRIGGER_AT_STATE =
      "Fail to create trigger [%s] at STATE [%s]";
  public static final String FAIL_TO_DATA_NODE_INACTIVE_ROLLBACK_OF_TRIGGER =
      "Fail to [DATA_NODE_INACTIVE] rollback of trigger [%s]";
  public static final String FAIL_TO_DROP_PIPE_PLUGIN_AFTER_RETRIES =
      "Fail to drop pipe plugin [{}] after {} retries";
  public static final String FAIL_TO_DROP_TRIGGER_AT_STATE =
      "Fail to drop trigger [%s] at STATE [%s]";
  public static final String FAIL_TO_DROP_TRIGGER_ON_DATA_NODES =
      "Fail to drop trigger [%s] on Data Nodes";
  public static final String FAIL_TO_EXECUTE_PLAN_AT_STATE =
      "Fail to execute plan [%s] at state[%s]";
  public static final String FAIL_TO_REMOVE_AINODE_AT_STATE =
      "Fail to remove AINode [%s] at STATE [%s], %s";
  public static final String FAIL_TO_REMOVE_AINODE_ON_CONFIG_NODES =
      "Fail to remove [%s] AINode on Config Nodes [%s]";
  public static final String FAIL_WHEN_EXECUTE = "Fail when execute {} ";
  public static final String FINISH_INACTIVE_ROLLBACK_OF_CQ_SUCCESSFULLY =
      "Finish [INACTIVE] rollback of CQ {} successfully";
  public static final String FINISH_INIT_CQ_SUCCESSFULLY = "Finish init CQ {} successfully";
  public static final String FINISH_SCHEDULING_CQ_SUCCESSFULLY =
      "Finish Scheduling CQ {} successfully";
  public static final String FORCE_UPDATE_NODECACHE_DATANODEID_NODESTATUS_CURRENTTIME =
      "{}, Force update NodeCache: dataNodeId={}, nodeStatus={}, currentTime={}";
  public static final String FOR_FAILED_WHEN_BECAUSE_FAILED_TO_EXECUTE_IN_ALL_REPLICASET =
      "[%s] for %s.%s failed when [%s] because failed to execute in all replicaset of %s %s. Failure nodes: %s";
  public static final String FOR_FAILED_WHEN_CONSTRUCT_BLACK_LIST_FOR_TABLE_BECAUSE_FAILED =
      "[%s] for %s.%s failed when construct black list for table because failed to execute in all replicaset of %s %s. Failures: %s";
  public static final String INVALIDATE_CACHE_OF_DEVICES_IN =
      "Invalidate cache of devices in {}.{}";
  public static final String INVALIDATE_CACHE_OF_TEMPLATE_SET_ON =
      "Invalidate cache of template {} set on {}";
  public static final String INVALIDATE_CACHE_OF_TEMPLATE_TIMESERIES =
      "Invalidate cache of template timeSeries {}";
  public static final String INVALIDATE_CACHE_OF_TIMESERIES = "Invalidate cache of timeSeries {}";
  public static final String INVALIDATE_CACHE_OF_VIEW = "Invalidate cache of view {}";
  public static final String INVALIDATE_COLUMN_CACHE_FAILED_FOR_TABLE =
      "Invalidate column %s cache failed for table %s.%s";
  public static final String INVALIDATE_SCHEMAENGINE_CACHE_FAILED =
      "Invalidate schemaEngine cache failed";
  public static final String INVALIDATE_SCHEMA_CACHE_FAILED = "Invalidate schema cache failed";
  public static final String INVALIDATE_TEMPLATE_CACHE_FAILED = "Invalidate template cache failed";
  public static final String INVALIDATE_VIEW_SCHEMAENGINE_CACHE_FAILED =
      "Invalidate view schemaengine cache failed";
  public static final String INVALIDATING_CACHE_FOR_COLUMN_IN_WHEN_DROPPING_COLUMN =
      "Invalidating cache for column {} in {}.{} when dropping column";
  public static final String PRE_RELEASE_DELETE_TABLE_WHEN_DROPPING_TABLE =
      "pre release delete table {}.{} when dropping table";
  public static final String COMMIT_RELEASE_DELETE_TABLE_WHEN_DROPPING_TABLE =
      "commit release delete table {}.{} when dropping table";
  public static final String INVALID_DATA_TYPE_CANNOT_BE_USED_AS_A_NEW_TYPE =
      "Invalid data type cannot be used as a new type";
  public static final String IO_ERROR_WHEN_DESERIALIZE_AUTHPLAN =
      "IO error when deserialize authplan.";
  public static final String IO_ERROR_WHEN_DESERIALIZE_SETTTL_PLAN =
      "IO error when deserialize setTTL plan.";
  public static final String NO_AVAILABLE_DATANODE_TO_ASSIGN_TASKS =
      "No available datanode to assign tasks";
  public static final String NO_DATABASE_LOST_DATA_PARTITION_TABLE_FOR_CONSENSUS_WRITE =
      "No database lost data partition table for consensus write";
  public static final String NO_DATAPARTITIONTABLE_AVAILABLE_FOR_CONSENSUS_WRITE =
      "No DataPartitionTable available for consensus write";
  public static final String NO_ENOUGH_DATA_NODE_TO_MIGRATE_REGION =
      "No enough Data node to migrate region: {}";
  public static final String OPERATION_TIMED_OUT_AFTER = "Operation timed out after ";
  public static final String PARTITION_TABLE_CLEANER_ACTIVATE_TTL_LOG =
      "[PartitionTableCleaner] Periodically activate PartitionTableAutoCleaner, databaseTTL: {}";
  public static final String
      PARTITIONTABLECLEANER_PERIODICALLY_ACTIVATE_PARTITIONTABLEAUTOCLEANER_FOR =
          "[PartitionTableCleaner] Periodically activate PartitionTableAutoCleaner for: {}";
  public static final String
      PARTITIONTABLECLEANER_THE_PARTITIONTABLEAUTOCLEANER_IS_STARTED_WITH_CYCLE_MS =
          "[PartitionTableCleaner] The PartitionTableAutoCleaner is started with cycle={}ms";
  public static final String PID_ADDREGION_CANNOT_ROLL_BACK_BECAUSE_CANNOT_FIND_THE_CORRECT =
      "[pid{}][AddRegion] Cannot roll back, because cannot find the correct locations";
  public static final String PID_ADDREGION_IT_APPEARS_THAT_CONSENSUS_WRITE_HAS_NOT_MODIFIED =
      "[pid{}][AddRegion] It appears that consensus write has not modified the local partition table. ";
  public static final String PID_ADDREGION_RESET_PEER_LIST_PEER_LIST_OF_CONSENSUS_GROUP =
      "[pid{}][AddRegion] reset peer list: peer list of consensus group {} on DataNode {} failed to reset to {}, you may manually reset it";
  public static final String PID_ADDREGION_RESET_PEER_LIST_PEER_LIST_OF_CONSENSUS_GROUP_2 =
      "[pid{}][AddRegion] reset peer list: peer list of consensus group {} on DataNode {} has been successfully reset to {}";
  public static final String PID_ADDREGION_RESET_PEER_LIST_PEER_LIST_OF_CONSENSUS_GROUP_3 =
      "[pid{}][AddRegion] reset peer list: peer list of consensus group {} on DataNode {} will be reset to {}";
  public static final String PID_ADDREGION_STARTED_WILL_BE_ADDED_TO_DATANODE =
      "[pid{}][AddRegion] started, {} will be added to DataNode {}.";
  public static final String PID_ADDREGION_START_TO_ROLL_BACK_BECAUSE =
      "[pid{}][AddRegion] Start to roll back, because: {}";
  public static final String PID_ADDREGION_STATE_COMPLETE = "[pid{}][AddRegion] state {} complete";
  public static final String PID_ADDREGION_STATE_FAILED = "[pid{}][AddRegion] state {} failed";
  public static final String PID_ADDREGION_SUCCESS_HAS_BEEN_ADDED_TO_DATANODE_PROCEDURE_TOOK =
      "[pid{}][AddRegion] success, {} has been added to DataNode {}. Procedure took {} (start at {}).";
  public static final String PID_REMOVEREGIONGROUP_STARTED_WILL_BE_DELETED =
      "[pid{}][RemoveRegionGroup] started, region group {} will be deleted from DataNodes {}.";
  public static final String PID_REMOVEREGIONGROUP_STARTED_REPLICA_WILL_BE_DELETED_FROM_DATANODE =
      "[pid{}][RemoveRegionGroup] region {} will be deleted from DataNode {}.";
  public static final String PID_REMOVEREGIONGROUP_STATE_FAILED =
      "[pid{}][RemoveRegionGroup] state {} failed";
  public static final String PID_REMOVEREGIONGROUP_DELETE_REPLICA_FAILED =
      "[pid{}][RemoveRegionGroup] failed to delete a replica of region {} (attempt {}), will keep retrying until it is deleted. reason: {}";
  public static final String PID_REMOVEREGIONGROUP_SUCCESS_PROCEDURE_TOOK =
      "[pid{}][RemoveRegionGroup] success, region group {} has been deleted. Procedure took {} (started at {}).";
  public static final String PID_MIGRATEREGION_STARTED_WILL_BE_MIGRATED_FROM_DATANODE_TO =
      "[pid{}][MigrateRegion] started, {} will be migrated from DataNode {} to {}.";
  public static final String PID_MIGRATEREGION_STATE_COMPLETE =
      "[pid{}][MigrateRegion] state {} complete";
  public static final String PID_MIGRATEREGION_STATE_FAIL = "[pid{}][MigrateRegion] state {} fail";
  public static final String PID_MIGRATEREGION_SUB_PROCEDURE_ADDREGIONPEERPROCEDURE =
      "[pid{}][MigrateRegion] sub-procedure AddRegionPeerProcedure failed, RegionMigrateProcedure will not continue";
  public static final String
      PID_MIGRATEREGION_SUCCESS_HAS_BEEN_MIGRATED_FROM_DATANODE_TO_PROCEDURE =
          "[pid{}][MigrateRegion] success,{} {} has been migrated from DataNode {} to {}. Procedure took {} (started at {}).";
  public static final String PID_NOTIFYREGIONMIGRATION_STARTED_REGION_ID_IS =
      "[pid{}][NotifyRegionMigration] started, region id is {}.";
  public static final String PID_NOTIFYREGIONMIGRATION_STATE_COMPLETE =
      "[pid{}][NotifyRegionMigration] state {} complete";
  public static final String PID_NOTIFYREGIONMIGRATION_STATE_FAILED =
      "[pid{}][NotifyRegionMigration] state {} failed";
  public static final String PID_RECONSTRUCTREGION_FAILED_BUT_THE_REGION_HAS_BEEN_REMOVED_FROM =
      "[pid{}][ReconstructRegion] failed, but the region {} has been removed from DataNode {}. Use 'extend region' to fix this.";
  public static final String
      PID_RECONSTRUCTREGION_STARTED_REGION_ON_DATANODE_WILL_BE_RECONSTRUCTED =
          "[pid{}][ReconstructRegion] started, region {} on DataNode {}({}) will be reconstructed.";
  public static final String PID_RECONSTRUCTREGION_STATE_COMPLETE =
      "[pid{}][ReconstructRegion] state {} complete";
  public static final String PID_RECONSTRUCTREGION_STATE_FAIL =
      "[pid{}][ReconstructRegion] state {} fail";
  public static final String PID_RECONSTRUCTREGION_SUB_PROCEDURE_REMOVEREGIONPEERPROCEDURE =
      "[pid{}][ReconstructRegion] sub-procedure RemoveRegionPeerProcedure failed, ReconstructRegionProcedure will not continue";
  public static final String PID_RECONSTRUCTREGION_SUCCESS_REGION_HAS_BEEN_RECONSTRUCTED =
      "[pid{}][ReconstructRegion] success, region {} has been reconstructed on DataNode {}. Procedure took {} (started at {})";
  public static final String
      PID_REMOVEREGION_DELETE_OLD_REGION_PEER_EXECUTED_FAILED_AFTER_ATTEMPTS =
          "[pid{}][RemoveRegion] DELETE_OLD_REGION_PEER executed failed after {} attempts, procedure will continue. You should manually delete region file. {}";
  public static final String PID_REMOVEREGION_DELETE_OLD_REGION_PEER_EXECUTED_FAILED_ATTEMPT_WILL =
      "[pid{}][RemoveRegion] DELETE_OLD_REGION_PEER executed failed (attempt {}/{}), will retry after {}ms. {}";
  public static final String PID_REMOVEREGION_DELETE_OLD_REGION_PEER_TASK_SUBMITTED_FAILED_AFTER =
      "[pid{}][RemoveRegion] DELETE_OLD_REGION_PEER task submitted failed after {} attempts, procedure will continue. You should manually delete region file. {}";
  public static final String PID_REMOVEREGION_DELETE_OLD_REGION_PEER_TASK_SUBMITTED_FAILED_ATTEMPT =
      "[pid{}][RemoveRegion] DELETE_OLD_REGION_PEER task submitted failed (attempt {}/{}), will retry after {}ms. {}";
  public static final String
      PID_REMOVEREGION_EXECUTED_FAILED_CONFIGNODE_BELIEVE_CURRENT_PEER_LIST_OF =
          "[pid{}][RemoveRegion] {} executed failed, ConfigNode believe current peer list of {} is {}. Procedure will continue. You should manually clear peer list.";
  public static final String PID_REMOVEREGION_STARTED_REGION_WILL_BE_REMOVED_FROM_DATANODE =
      "[pid{}][RemoveRegion] started, region {} will be removed from DataNode {}.";
  public static final String PID_REMOVEREGION_STATE_SUCCESS =
      "[pid{}][RemoveRegion] state {} success";
  public static final String
      PID_REMOVEREGION_SUCCESS_REGION_HAS_BEEN_REMOVED_FROM_DATANODE_PROCEDURE =
          "[pid{}][RemoveRegion] success, region {} has been removed from DataNode {}. Procedure took {} (started at {})";
  public static final String
      PID_REMOVEREGION_TASK_SUBMITTED_FAILED_CONFIGNODE_BELIEVE_CURRENT_PEER_LIST =
          "[pid{}][RemoveRegion] {} task submitted failed, ConfigNode believe current peer list of {} is {}. Procedure will continue. You should manually clear peer list.";
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
  public static final String
      PIPEMETASYNCPROCEDURE_ACQUIRELOCK_SKIP_THE_PROCEDURE_DUE_TO_THE_LAST_EXECUTION =
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
      "Pipe {} not found in PipeTaskInfo, can not push its meta.";
  public static final String
      PIPE_PLUGIN_IS_ALREADY_CREATED_AND_ISSETIFNOTEXISTSCONDITION_IS_TRUE_END =
          "Pipe plugin {} is already created and isSetIfNotExistsCondition is true, end the CreatePipePluginProcedure({})";
  public static final String PIPE_PLUGIN_IS_ALREADY_CREATED_END_THE_CREATEPIPEPLUGINPROCEDURE =
      "Pipe plugin {} is already created, end the CreatePipePluginProcedure({})";
  public static final String PIPE_PLUGIN_IS_NOT_EXIST_END_THE_DROPPIPEPLUGINPROCEDURE =
      "Pipe plugin {} is not exist, end the DropPipePluginProcedure({})";
  public static final String PRE_CREATE_TABLE = "Pre create table {}.{}";
  public static final String PRE_CREATE_TABLE_FAILED = "Pre create table failed";
  public static final String PRE_RELEASE = "Pre-release ";
  public static final String PRE_RELEASE_INFO_FOR_TABLE_WHEN_SETTING_PROPERTIES =
      "Pre release info for table {}.{} when setting properties";
  public static final String PRE_RELEASE_INFO_OF_TABLE_WHEN_ADDING_COLUMN =
      "Pre release info of table {}.{} when adding column";
  public static final String PRE_RELEASE_INFO_OF_TABLE_WHEN_ALTERING_COLUMN =
      "Pre-release info of table {}.{} when altering column";
  public static final String PRE_RELEASE_INFO_OF_TABLE_WHEN_RENAMING_COLUMN =
      "Pre release info of table {}.{} when renaming column";
  public static final String PRE_RELEASE_INFO_OF_TABLE_WHEN_RENAMING_TABLE =
      "Pre release info of table {}.{} when renaming table";
  public static final String PRE_RELEASE_SCHEMAENGINE_TEMPLATE_SET_ON_PATH =
      "Pre release schemaengine template {} set on path {}";
  public static final String PRE_RELEASE_TABLE = "Pre release table {}.{}";
  public static final String PRE_SET_SCHEMAENGINE_TEMPLATE_ON_PATH =
      "Pre set schemaengine template {} on path {}";
  public static final String PRE_SET_TEMPLATE_FAILED = "Pre set template failed";
  public static final String PROCEDUREID = "ProcedureId {}: {}";
  public static final String PROCEDUREID_ACQUIRED_PIPE_LOCK = "ProcedureId {} acquired pipe lock.";
  public static final String PROCEDUREID_ACQUIRED_SUBSCRIPTION_LOCK =
      "ProcedureId {} acquired subscription lock.";
  public static final String PROCEDUREID_ALL_RETRIES_FAILED_WHEN_TRYING_TO_AT_STATE_WILL =
      "ProcedureId {}: All {} retries failed when trying to {} at state [{}], will rollback...";
  public static final String PROCEDUREID_ENCOUNTERED_ERROR_WHEN_TRYING_TO_AT_STATE_RETRY =
      "ProcedureId {}: Encountered error when trying to {} at state [{}], retry [{}/{}]";
  public static final String PROCEDUREID_FAILED_TO_ACQUIRE_PIPE_LOCK =
      "ProcedureId {} failed to acquire pipe lock.";
  public static final String PROCEDUREID_FAILED_TO_ACQUIRE_SUBSCRIPTION_LOCK =
      "ProcedureId {} failed to acquire subscription lock.";
  public static final String PROCEDUREID_FAILED_TO_ROLLBACK_FROM_CALCULATE_INFO_FOR_TASK =
      "ProcedureId {}: Failed to rollback from calculate info for task.";
  public static final String PROCEDUREID_FAILED_TO_ROLLBACK_FROM_OPERATE_ON_DATA_NODES =
      "ProcedureId {}: Failed to rollback from operate on data nodes.";
  public static final String PROCEDUREID_FAILED_TO_ROLLBACK_FROM_STATE_BECAUSE =
      "ProcedureId {}: Failed to rollback from state [{}], because {}";
  public static final String PROCEDUREID_FAILED_TO_ROLLBACK_FROM_VALIDATE_TASK =
      "ProcedureId {}: Failed to rollback from validate task.";
  public static final String PROCEDUREID_FAILED_TO_ROLLBACK_FROM_WRITE_CONFIG_NODE_CONSENSUS =
      "ProcedureId {}: Failed to rollback from write config node consensus.";
  public static final String PROCEDUREID_FAIL_TO_BECAUSE = "ProcedureId %s: Fail to %s because %s";
  public static final String PROCEDUREID_INVALID_LOCK_STATE_PIPE_LOCK_WILL_BE_RELEASED =
      "ProcedureId {}: {}. Invalid lock state. Pipe lock will be released.";
  public static final String PROCEDUREID_INVALID_LOCK_STATE_SUBSCRIPTION_LOCK_WILL_BE_RELEASED =
      "ProcedureId {}: {}. Invalid lock state. Subscription lock will be released.";
  public static final String PROCEDUREID_INVALID_LOCK_STATE_WITHOUT_ACQUIRING_PIPE_LOCK =
      "ProcedureId {}: {}. Invalid lock state. Without acquiring pipe lock.";
  public static final String PROCEDUREID_INVALID_LOCK_STATE_WITHOUT_ACQUIRING_SUBSCRIPTION_LOCK =
      "ProcedureId {}: {}. Invalid lock state. Without acquiring subscription lock.";
  public static final String
      PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_BE_EXECUTED_WITH =
          "ProcedureId {}: LOCK_ACQUIRED. The following procedure should be executed with pipe lock.";
  public static final String
      PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_BE_EXECUTED_WITH_2 =
          "ProcedureId {}: LOCK_ACQUIRED. The following procedure should be executed with subscription and pipe lock.";
  public static final String
      PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_BE_EXECUTED_WITH_3 =
          "ProcedureId {}: LOCK_ACQUIRED. The following procedure should be executed with subscription lock.";
  public static final String
      PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_NOT_BE_EXECUTED =
          "ProcedureId {}: LOCK_ACQUIRED. The following procedure should not be executed without pipe lock.";
  public static final String
      PROCEDUREID_LOCK_ACQUIRED_THE_FOLLOWING_PROCEDURE_SHOULD_NOT_BE_EXECUTED_2 =
          "ProcedureId {}: LOCK_ACQUIRED. The following procedure should not be executed without subscription lock.";
  public static final String PROCEDUREID_LOCK_EVENT_WAIT_PIPE_LOCK_WILL_BE_RELEASED =
      "ProcedureId {}: LOCK_EVENT_WAIT. Pipe lock will be released.";
  public static final String PROCEDUREID_LOCK_EVENT_WAIT_SUBSCRIPTION_LOCK_WILL_BE_RELEASED =
      "ProcedureId {}: LOCK_EVENT_WAIT. Subscription lock will be released.";
  public static final String PROCEDUREID_LOCK_EVENT_WAIT_WITHOUT_ACQUIRING_PIPE_LOCK =
      "ProcedureId {}: LOCK_EVENT_WAIT. Without acquiring pipe lock.";
  public static final String PROCEDUREID_LOCK_EVENT_WAIT_WITHOUT_ACQUIRING_SUBSCRIPTION_LOCK =
      "ProcedureId {}: LOCK_EVENT_WAIT. Without acquiring subscription lock.";
  public static final String
      PROCEDUREID_PIPE_LOCK_IS_NOT_ACQUIRED_EXECUTEFROMSTATE_S_EXECUTION_WILL =
          "ProcedureId {}: Pipe lock is not acquired, executeFromState's execution will be skipped.";
  public static final String PROCEDUREID_PIPE_LOCK_IS_NOT_ACQUIRED_ROLLBACKSTATE_S_EXECUTION_WILL =
      "ProcedureId {}: Pipe lock is not acquired, rollbackState({})'s execution will be skipped.";
  public static final String PROCEDUREID_RELEASE_LOCK_NO_NEED_TO_RELEASE_PIPE_LOCK =
      "ProcedureId {} release lock. No need to release pipe lock.";
  public static final String PROCEDUREID_RELEASE_LOCK_NO_NEED_TO_RELEASE_SUBSCRIPTION_LOCK =
      "ProcedureId {} release lock. No need to release subscription lock.";
  public static final String PROCEDUREID_RELEASE_LOCK_PIPE_LOCK_WILL_BE_RELEASED =
      "ProcedureId {} release lock. Pipe lock will be released.";
  public static final String PROCEDUREID_RELEASE_LOCK_SUBSCRIPTION_LOCK_WILL_BE_RELEASED =
      "ProcedureId {} release lock. Subscription lock will be released.";
  public static final String
      PROCEDUREID_SUBSCRIPTION_LOCK_IS_NOT_ACQUIRED_EXECUTEFROMSTATE_S_EXECUTION_WILL =
          "ProcedureId {}: Subscription lock is not acquired, executeFromState({})'s execution will be skipped.";
  public static final String
      PROCEDUREID_SUBSCRIPTION_LOCK_IS_NOT_ACQUIRED_ROLLBACKSTATE_S_EXECUTION_WILL =
          "ProcedureId {}: Subscription lock is not acquired, rollbackState({})'s execution will be skipped.";
  public static final String PROCEDUREID_TRY_TO_ACQUIRE_PIPE_LOCK =
      "ProcedureId {} try to acquire pipe lock.";
  public static final String PROCEDUREID_TRY_TO_ACQUIRE_SUBSCRIPTION_AND_PIPE_LOCK =
      "ProcedureId {} try to acquire subscription and pipe lock.";
  public static final String PROCEDUREID_TRY_TO_ACQUIRE_SUBSCRIPTION_LOCK =
      "ProcedureId {} try to acquire subscription lock.";
  public static final String PROCEDURE_TYPE = "Procedure type ";
  public static final String REMOVEREGIONLOCATION_REMOVE_REGION_FROM_DATANODE_RESULT_IS =
      "RemoveRegionLocation remove region {} from DataNode {}, result is {}";
  public static final String REMOVEREGIONPEER_STATE_FAILED = "RemoveRegionPeer state {} failed";
  public static final String REMOVEREGIONPEER_STATE_SUCCESS = "RemoveRegionPeer state {} success";
  public static final String REMOVEREGION_RATIS_TRANSFER_LEADER_FAIL_BUT_PROCEDURE_WILL_CONTINUE =
      "[RemoveRegion] Ratis transfer leader fail, but procedure will continue.";
  public static final String REMOVE_CONFIG_NODE = "Remove Config Node";
  public static final String REMOVE_DATA_NODE_FAILED = "Remove Data Node failed ";
  public static final String RENAMETABLECOLUMN_COSTS_MS = "RenameTableColumn-{}.{}-{} costs {}ms";
  public static final String RENAMETABLE_COSTS_MS = "RenameTable-{}.{}-{} costs {}ms";
  public static final String RENAME_COLUMN_TO_TABLE_ON_CONFIG_NODE =
      "Rename column to table {}.{} on config node";
  public static final String RETRIEVABLE_ERROR_TRYING_TO_CREATE_CQ_STATE =
      "Retrievable error trying to create cq [{}], state [{}]";
  public static final String RETRIEVABLE_ERROR_TRYING_TO_CREATE_PIPE_PLUGIN_STATE =
      "Retrievable error trying to create pipe plugin [{}], state: {}";
  public static final String RETRIEVABLE_ERROR_TRYING_TO_DROP_PIPE_PLUGIN_STATE =
      "Retrievable error trying to drop pipe plugin [{}], state: {}";
  public static final String RETRIEVABLE_ERROR_TRYING_TO_EXECUTE_PLAN_STATE =
      "Retrievable error trying to execute plan {}, state: {}";
  public static final String RETRIEVABLE_ERROR_TRYING_TO_REMOVE_AINODE_STATE =
      "Retrievable error trying to remove AINode [{}], state [{}]";
  public static final String ROLLBACK_CREATETABLE_COSTS_MS = "Rollback CreateTable-{} costs {}ms.";
  public static final String ROLLBACK_CREATE_TABLE_FAILED = "Rollback create table failed";
  public static final String ROLLBACK_DROPTABLE_COSTS_MS = "Rollback DropTable-{} costs {}ms.";
  public static final String ROLLBACK_PRE_DELETE_TABLE_FAILED =
      "Rollback pre-delete table %s.%s failed, please manually drop the table by entering sql";
  public static final String ROLLBACK_PRE_RELEASE = "Rollback pre-release ";
  public static final String ROLLBACK_PRE_RELEASE_TEMPLATE_FAILED =
      "Rollback pre release template failed";
  public static final String ROLLBACK_RENAMETABLECOLUMN_COSTS_MS =
      "Rollback RenameTableColumn-{} costs {}ms.";
  public static final String ROLLBACK_RENAMETABLE_COSTS_MS = "Rollback RenameTable-{} costs {}ms.";
  public static final String ROLLBACK_SETTABLEPROPERTIES_COSTS_MS =
      "Rollback SetTableProperties-{} costs {}ms.";
  public static final String ROLLBACK_SETTEMPLATE_COSTS_MS = "Rollback SetTemplate-{} costs {}ms.";
  public static final String ROLLBACK_TEMPLATE_CACHE_FAILED = "Rollback template cache failed";
  public static final String ROLLBACK_TEMPLATE_PRE_UNSET_FAILED_BECAUSE_OF =
      "Rollback template pre unset failed because of";
  public static final String
      ROLLBACK_UNSET_TEMPLATE_FAILED_AND_THE_CLUSTER_TEMPLATE_INFO_MANAGEMENT =
          "Rollback unset template failed and the cluster template info management is strictly broken. Please try unset again.";
  public static final String SELECTED_DATANODE_FOR_REGION = "Selected DataNode {} for Region {}";
  public static final String
      SEND_ACTION_ADDREGIONPEER_FINISHED_REGIONID_RPCDATANODE_DESTDATANODE_STATUS =
          "{}, Send action addRegionPeer finished, regionId: {}, rpcDataNode: {},  destDataNode: {}, status: {}";
  public static final String
      SEND_ACTION_CREATENEWREGIONPEER_ERROR_REGIONID_NEWPEERDATANODEID_RESULT =
          "{}, Send action createNewRegionPeer error, regionId: {}, newPeerDataNodeId: {}, result: {}";
  public static final String SEND_ACTION_CREATENEWREGIONPEER_FINISHED_REGIONID_NEWPEERDATANODEID =
      "{}, Send action createNewRegionPeer finished, regionId: {}, newPeerDataNodeId: {}";
  public static final String SEND_ACTION_DELETEOLDREGIONPEER_FINISHED_REGIONID_DATANODEID =
      "{}, Send action deleteOldRegionPeer finished, regionId: {}, dataNodeId: {}";
  public static final String SEND_ACTION_REMOVEREGIONPEER_FINISHED_REGIONID_RPCDATANODE =
      "{}, Send action removeRegionPeer finished, regionId: {}, rpcDataNode: {}";
  public static final String SETSCHEMATEMPLATE_COSTS_MS = "SetSchemaTemplate-[{}] costs {}ms";
  public static final String SETTABLEPROPERTIES_COSTS_MS = "SetTableProperties-{}.{}-{} costs {}ms";
  public static final String SETTTL_COSTS_MS = "SetTTL-[{}] costs {}ms";
  public static final String SET_PROPERTIES_TO_TABLE = "Set properties to table {}.{}";
  public static final String SET_TEMPLATE_TO_FAILED_WHEN_CHECK_TIME_SERIES_EXISTENCE_ON =
      "Set template %s to %s failed when [check time series existence on DataNode] because ";
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
  public static final String START_INACTIVE_ROLLBACK_OF_CQ = "Start [INACTIVE] rollback of CQ {}";
  public static final String START_ROLLBACK_ADD_COLUMN_TO_TABLE_WHEN_ADDING_COLUMN =
      "Start rollback Add column to table {}.{} when adding column";
  public static final String START_ROLLBACK_COMMIT_SET_SCHEMAENGINE_TEMPLATE_ON_PATH =
      "Start rollback commit set schemaengine template {} on path {}";
  public static final String START_ROLLBACK_PRE_CREATE_TABLE =
      "Start rollback pre create table {}.{}";
  public static final String START_ROLLBACK_PRE_RELEASE_INFO_FOR_TABLE_WHEN_SETTING_PROPERTIES =
      "Start rollback pre release info for table {}.{} when setting properties";
  public static final String START_ROLLBACK_PRE_RELEASE_INFO_OF_TABLE =
      "Start rollback pre release info of table {}.{}";
  public static final String START_ROLLBACK_PRE_RELEASE_SCHEMAENGINE_TEMPLATE_ON_PATH =
      "Start rollback pre release schemaengine template {} on path {}";
  public static final String START_ROLLBACK_PRE_RELEASE_TABLE =
      "Start rollback pre release table {}.{}";
  public static final String START_ROLLBACK_PRE_SET_SCHEMAENGINE_TEMPLATE_ON_PATH =
      "Start rollback pre set schemaengine template {} on path {}";
  public static final String START_ROLLBACK_RENAMING_COLUMN_TO_TABLE_ON_CONFIGNODE =
      "Start rollback Renaming column to table {}.{} on configNode";
  public static final String START_ROLLBACK_RENAMING_TABLE_ON_CONFIGNODE =
      "Start rollback Renaming table {}.{} on configNode";
  public static final String START_ROLLBACK_SET_PROPERTIES_TO_TABLE =
      "Start rollback set properties to table {}.{}";
  public static final String STATE_MACHINE_PROCEDURE_EOF_STATE_SKIP_EXECUTION =
      "StateMachineProcedure pid={} is scheduled with EOF state, skip execution: {}";
  public static final String STATE_STUCK_AT = "State stuck at ";
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
      "{}, Stop Data Node meets error, error datanode: {}";
  public static final String STOP_DATA_NODE_SUCCESS = "{}, Stop Data Node {} success.";
  public static final String SUBMITTED_ASYNC_CONSENSUS_PIPE_CREATION =
      "{}, Submitted async consensus pipe creation: {}";
  public static final String SUBSCRIPTION_META_SYNC_PROCEDURE_FINISHED_UPDATING_LAST_SYNC_VERSION =
      "Subscription meta sync procedure finished, updating last sync version.";
  public static final String SUCCESSFULLY_OPERATE_WILL_CLEAR_CACHE_TO_THE_DATA_REGIONS_ANYWAY =
      "Successfully operate, will clear cache to the data regions anyway";
  public static final String SUCCESSFULLY_RESTORED_WILL_SET_MODS_TO_THE_DATA_REGIONS_ANYWAY =
      "Successfully restored, will set mods to the data regions anyway";
  public static final String SUCCESSFULLY_STOPPED_AINODE = "Successfully stopped AINode {}";
  public static final String TABLE_ALREADY_EXISTS = "Table '%s.%s' already exists.";
  public static final String TABLE_NOT_EXISTS = "Table '%s.%s' not exists.";
  public static final String TARGET_DEVICE_TEMPLATE_IS_NOT_ACTIVATED_ON_ANY_PATH_MATCHED =
      "Target Device Template is not activated on any path matched by given path pattern";
  public static final String TASK_CANNOT_GET_TASK_REPORT_FROM_DATANODE_LAST_REPORT_TIME =
      "{} task {} cannot get task report from DataNode {}, last report time is {} ago";
  public static final String THE_UPDATED_TABLE_HAS_THE_SAME_PROPERTIES_WITH_THE_ORIGINAL =
      "The updated table has the same properties with the original one. Skip the procedure.";
  public static final String
      TOPICMETASYNCPROCEDURE_ACQUIRELOCK_SKIP_THE_PROCEDURE_DUE_TO_THE_LAST_EXECUTION =
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
  public static final String UNEXPECTED_FAIL_TSSTATUS_IS = "Unexpected fail, tsStatus is ";
  public static final String UNEXPECTED_STATE = "Unexpected state";
  public static final String UNKNOWN_CREATECQSTATE = "Unknown CreateCQState: ";
  public static final String UNKNOWN_CREATETRIGGERSTATE = "Unknown CreateTriggerState: ";
  public static final String UNKNOWN_DROPTRIGGERSTATE = "Unknown DropTriggerState: ";
  public static final String UNKNOWN_LOAD_BALANCE_STRATEGY = "Unknown load balance strategy: ";
  public static final String UNKNOWN_PROCEDURE_TYPE = "Unknown Procedure type: ";
  public static final String UNKNOWN_PROCEDURE_TYPE_2 = "Unknown Procedure type: {}";
  public static final String UNKNOWN_STATE = "Unknown state: ";
  public static final String UNKNOWN_STATE_DURING_EXECUTING_CREATEPIPEPLUGINPROCEDURE =
      "Unknown state during executing createPipePluginProcedure, %s";
  public static final String UNKNOWN_STATE_DURING_EXECUTING_OPERATEPIPEPROCEDURE =
      "Unknown state during executing operatePipeProcedure, %s";
  public static final String UNKNOWN_STATE_DURING_EXECUTING_OPERATESUBSCRIPTIONPROCEDURE =
      "Unknown state during executing operateSubscriptionProcedure, %s";
  public static final String UNKNOWN_STATE_DURING_EXECUTING_REMOVEAINODEPROCEDURE =
      "Unknown state during executing removeAINodeProcedure, %s";
  public static final String UNKNOWN_STATE_DURING_ROLLBACK_OPERATESUBSCRIPTIONPROCEDURE =
      "Unknown state during rollback operateSubscriptionProcedure, %s";
  public static final String UNKNOWN_STATE_FOR_ROLLBACK = "Unknown state for rollback: ";
  public static final String UNRECOGNIZED_ADDTABLECOLUMNSTATE = "Unrecognized AddTableColumnState ";
  public static final String UNRECOGNIZED_ALTERTABLECOLUMNDATATYPEPROCEDURE =
      "Unrecognized AlterTableColumnDataTypeProcedure ";
  public static final String UNRECOGNIZED_ALTERTIMESERIESDATATYPEPROCEDURE_STATE =
      "Unrecognized AlterTimeSeriesDataTypeProcedure state ";
  public static final String UNRECOGNIZED_CREATETABLESTATE = "Unrecognized CreateTableState ";
  public static final String UNRECOGNIZED_DROPTABLECOLUMNSTATE =
      "Unrecognized DropTableColumnState ";
  public static final String UNRECOGNIZED_DROPTABLESTATE = "Unrecognized DropTableState ";
  public static final String UNRECOGNIZED_LOG_TYPE = "unrecognized log type ";
  public static final String UNRECOGNIZED_RENAMETABLECOLUMNSTATE =
      "Unrecognized RenameTableColumnState ";
  public static final String UNRECOGNIZED_RENAMETABLESTATE = "Unrecognized RenameTableState ";
  public static final String UNRECOGNIZED_SETTEMPLATESTATE = "Unrecognized SetTemplateState ";
  public static final String UNRECOGNIZED_STATE = "Unrecognized state ";
  public static final String UNSETTEMPLATE_COSTS_MS = "UnsetTemplate-[{}] costs {}ms";
  public static final String
      UNSET_TEMPLATE_FROM_FAILED_WHEN_CHECK_DATANODE_TEMPLATE_ACTIVATION_BECAUSE =
          "Unset template %s from %s failed when [check DataNode template activation] because %s";
  public static final String UNSET_TEMPLATE_ON = "Unset template {} on {}";
  public static final String UNSUPPORTED_ROLL_BACK_STATE = "Unsupported roll back STATE [{}]";
  public static final String UNSUPPORTED_STATE = "Unsupported state: ";
  public static final String UPDATE_DATANODE_TTL_CACHE_FAILED = "Update dataNode ttl cache failed";
  public static final String VALIDATE_TABLE_FOR_TABLE_WHEN_SETTING_PROPERTIES =
      "Validate table for table {}.{} when setting properties";
  public static final String
      WAITTASKFINISH_RETURNS_PROCESSING_WHICH_MEANS_THE_WAITING_HAS_BEEN_INTERRUPTED =
          "waitTaskFinish() returns PROCESSING, which means the waiting has been interrupted (ConfigNode shutdown or leader change); the AddRegionPeer task is still running on the coordinator, this procedure will stay at DO_ADD_REGION_PEER and resume polling after recovery";

  public static final String FAILED_TO_CREATE_DATABASE_THE_TTL_SHOULD_BE_NON_NEGATIVE =
      "Failed to create database. The TTL should be non-negative.";
  public static final String FAILED_TO_CREATE_DATABASE_THE_DATAREGIONGROUPNUM_SHOULD_BE_POSITIVE =
      "Failed to create database. The dataRegionGroupNum should be positive.";
  public static final String PID_FAILED_TO_PERSIST_LOCK_STATE_TO_STORE =
      "pid={} Failed to persist lock state to store.";
  public static final String FORCE_WRITE_UNLOCK_STATE_TO_RAFT_FOR_PID =
      "Force write unlock state to raft for pid={}";
  public static final String PID_FAILED_TO_PERSIST_UNLOCK_STATE_TO_STORE =
      "pid={} Failed to persist unlock state to store.";
  public static final String DIDN_T_HOLD_THE_LOCK_BEFORE_RESTARTING_SKIP_ACQUIRING_LOCK =
      "{} didn't hold the lock before restarting, skip acquiring lock";
  public static final String IS_ALREADY_BYPASSED_SKIP_ACQUIRING_LOCK =
      "{} is already bypassed, skip acquiring lock.";
  public static final String IS_IN_WAITING_STATE_AND_HOLDLOCK_FALSE_SKIP_ACQUIRING_LOCK =
      "{} is in WAITING STATE, and holdLock= false , skip acquiring lock.";
  public static final String HELD_THE_LOCK_BEFORE_RESTARTING_CALL_ACQUIRELOCK_TO_RESTORE_IT =
      "{} held the lock before restarting, call acquireLock to restore it.";
  public static final String CHILD_LATCH_INCREMENT_SET = "CHILD LATCH INCREMENT SET ";
  public static final String CHILD_LATCH_INCREMENT = "CHILD LATCH INCREMENT ";
  public static final String CHILD_LATCH_DECREMENT = "CHILD LATCH DECREMENT ";
  public static final String UNEXPECTED_STATE_FOR = "Unexpected state:{} for {}";
  public static final String OLD_PROCEDURE_DIRECTORY_DETECTED_UPGRADE_BEGINNING =
      "Old procedure directory detected, upgrade beginning...";
  public static final String ALREADY_RUNNING = "Already running";
  public static final String PROCEDURE_WORKERS_ARE_STARTED = "{} procedure workers are started.";
  public static final String IS_ALREADY_FINISHED = "{} is already finished.";
  public static final String ROLLBACK_BECAUSE_PARENT_IS_DONE_ROLLEDBACK_PROC_IS =
      "Rollback because parent is done/rolledback, proc is {}";
  public static final String ROLLBACK_STACK_IS_NULL_FOR = "Rollback stack is null for {}";
  public static final String LOCK_EVENT_WAIT_ROLLBACK = "LOCK_EVENT_WAIT rollback {}";
  public static final String LOCK_EVENT_WAIT_CAN_T_ROLLBACK_CHILD_RUNNING_FOR =
      "LOCK_EVENT_WAIT can't rollback child running for {}";
  public static final String LOCKSTATE_IS = "{} lockstate is {}";
  public static final String FINISHED_IN_MS_SUCCESSFULLY = "{} finished in {}ms successfully.";
  public static final String PROCEDUREID_WAIT_FOR_LOCK = "procedureId {} wait for lock.";
  public static final String INTERRUPT_DURING_EXECUTION_SUSPEND_OR_RETRY_IT_LATER =
      "Interrupt during execution, suspend or retry it later.";
  public static final String CODE_BUG = "CODE-BUG:{}";
  public static final String INITIALIZED_SUB_PROCS = "Initialized sub procs:{}";
  public static final String ADDED_INTO_TIMEOUTEXECUTOR = "Added into timeoutExecutor {}";
  public static final String SUB_PROCEDURE_PID_HAS_BEEN_SUBMITTED =
      "Sub-Procedure pid={} has been submitted";
  public static final String STORED_CHILDREN = "Stored {}, children {}";
  public static final String FAILED_TO_UPDATE_SUBPROCS_ON_EXECUTION =
      "Failed to update subprocs on execution";
  public static final String STORE_UPDATE = "Store update {}";
  public static final String FAILED_TO_DELETE_SUBPROCEDURES_ON_EXECUTION =
      "Failed to delete subprocedures on execution";
  public static final String FAILED_TO_UPDATE_PROCEDURE_ON_EXECUTION =
      "Failed to update procedure on execution";
  public static final String ROLLED_BACK_TIME_DURATION_IS = "Rolled back {}, time duration is {}";
  public static final String ROLL_BACK_FAILED_FOR = "Roll back failed for {}";
  public static final String INTERRUPTED_EXCEPTION_OCCURRED_FOR =
      "Interrupted exception occurred for {}";
  public static final String CODE_BUG_RUNTIME_EXCEPTION_FOR = "CODE-BUG: runtime exception for {}";
  public static final String FAILED_TO_DELETE_PROCEDURE_ON_ROLLBACK =
      "Failed to delete procedure on rollback";
  public static final String FAILED_TO_UPDATE_PROCEDURE_ON_ROLLBACK =
      "Failed to update procedure on rollback";
  public static final String PROCEDURE_WORKER_TERMINATED = "Procedure worker {} terminated.";
  public static final String ADDED_NEW_WORKER_THREAD = "Added new worker thread {}";
  public static final String STOPPING = "Stopping";
  public static final String FAILED_TO_UPDATE_STORE_PROCEDURE =
      "Failed to update store procedure {}";
  public static final String IS_STORED = "{} is stored.";
  public static final String START_TO_CREATE_TRIGGER = "Start to create trigger [{}]";
  public static final String CREATE_TRIGGER_FAILED = "Create trigger {} failed.";
  public static final String START_INIT_ROLLBACK_OF_TRIGGER =
      "Start [INIT] rollback of trigger [{}]";
  public static final String START_VALIDATED_ROLLBACK_OF_TRIGGER =
      "Start [VALIDATED] rollback of trigger [{}]";
  public static final String START_TO_DROP_TRIGGER = "Start to drop trigger [{}]";
  public static final String START_TO_DROP_TRIGGER_ON_DATA_NODES =
      "Start to drop trigger [{}] on Data Nodes";
  public static final String START_TO_DROP_TRIGGER_ON_CONFIG_NODES =
      "Start to drop trigger [{}] on Config Nodes";
  public static final String DROP_TRIGGER_FAILED = "Drop trigger {} failed.";
  public static final String ERROR_IN_DESERIALIZE_DELETEDATABASEPROCEDURE =
      "Error in deserialize DeleteDatabaseProcedure";
  public static final String EXECUTING_CREATE_PEER_ON = "Executing CREATE_PEER on {}...";
  public static final String SUCCESSFULLY_CREATE_PEER_ON = "Successfully CREATE_PEER on {}";
  public static final String EXECUTING_ADD_PEER = "Executing ADD_PEER {}...";
  public static final String SUCCESSFULLY_ADD_PEER = "Successfully ADD_PEER {}";
  public static final String THE_CONFIGNODE_IS_SUCCESSFULLY_ADDED_TO_THE_CLUSTER =
      "The ConfigNode: {} is successfully added to the cluster";
  public static final String ROLLBACK_CREATE_PEER_FOR = "Rollback CREATE_PEER for: {}";
  public static final String ROLLBACK_ADD_PEER_FOR = "Rollback ADD_PEER for: {}";
  public static final String ERROR_IN_DESERIALIZE_ADDCONFIGNODEPROCEDURE =
      "Error in deserialize AddConfigNodeProcedure";
  public static final String REMOVE_PEER_FOR_CONFIGNODE = "Remove peer for ConfigNode: {}";
  public static final String DELETE_PEER_FOR_CONFIGNODE = "Delete peer for ConfigNode: {}";
  public static final String STOP_AND_CLEAR_CONFIGNODE = "Stop and clear ConfigNode: {}";
  public static final String ERROR_IN_DESERIALIZE_REMOVECONFIGNODEPROCEDURE =
      "Error in deserialize RemoveConfigNodeProcedure";
  public static final String DATAPARTITIONINTEGRITY_ERROR_EXECUTING_STATE =
      "[DataPartitionIntegrity] Error executing state {}: {}";
  public static final String COLLECTING_EARLIEST_TIMESLOTS_FROM_ALL_DATANODES =
      "Collecting earliest timeslots from all DataNodes...";
  public static final String ANALYZING_MISSING_DATA_PARTITIONS =
      "Analyzing missing data partitions...";
  public static final String CHECKING_DATAPARTITIONTABLE_GENERATION_COMPLETION_STATUS =
      "Checking DataPartitionTable generation completion status...";
  public static final String MERGING_DATAPARTITIONTABLES_FROM_DATANODES =
      "Merging DataPartitionTables from {} DataNodes...";
  public static final String
      DATAPARTITIONINTEGRITY_DATAPARTITIONTABLES_MERGE_COMPLETED_SUCCESSFULLY =
          "[DataPartitionIntegrity] DataPartitionTables merge completed successfully";
  public static final String WRITING_DATAPARTITIONTABLE_TO_CONSENSUS_LOG =
      "Writing DataPartitionTable to consensus log...";
  public static final String DATAPARTITIONINTEGRITY_NO_DATABASE_LOST_DATA_PARTITION_TABLE =
      "[DataPartitionIntegrity] No database lost data partition table";
  public static final String DATAPARTITIONINTEGRITY_DATAPARTITIONTABLE_TO_WRITE_TO_CONSENSUS =
      "[DataPartitionIntegrity] DataPartitionTable to write to consensus";
  public static final String
      DATAPARTITIONINTEGRITY_FAILED_TO_WRITE_DATAPARTITIONTABLE_TO_CONSENSUS_LOG =
          "[DataPartitionIntegrity] Failed to write DataPartitionTable to consensus log";
  public static final String
      DATAPARTITIONINTEGRITY_ERROR_WRITING_DATAPARTITIONTABLE_TO_CONSENSUS_LOG =
          "[DataPartitionIntegrity] Error writing DataPartitionTable to consensus log";
  public static final String DATAPARTITIONINTEGRITY_FAILED_TO_SERIALIZE_SKIPDATANODE =
      "[DataPartitionIntegrity] Failed to serialize skipDataNode";
  public static final String DATAPARTITIONINTEGRITY_FAILED_TO_SERIALIZE_FAILEDDATANODE =
      "[DataPartitionIntegrity] Failed to serialize failedDataNode";
  public static final String DATAPARTITIONINTEGRITY_FAILED_TO_DESERIALIZE_SKIPDATANODE =
      "[DataPartitionIntegrity] Failed to deserialize skipDataNode";
  public static final String DATAPARTITIONINTEGRITY_FAILED_TO_DESERIALIZE_FAILEDDATANODE =
      "[DataPartitionIntegrity] Failed to deserialize failedDataNode";
  public static final String
      DATAPARTITIONINTEGRITY_SKIPPING_EMPTY_BYTEBUFFER_DURING_DESERIALIZATION =
          "[DataPartitionIntegrity] Skipping empty ByteBuffer during deserialization";
  public static final String NOT_FIND_REGION_REPLICA_NODES_IN_CREATEPEER_REGIONID =
      "Not find region replica nodes in createPeer, regionId: ";
  public static final String SIMPLECONSENSUS_PROTOCOL_IS_NOT_SUPPORTED_TO_REMOVE_DATA_NODE =
      "SimpleConsensus protocol is not supported to remove data node";
  public static final String FAILED_TO_REMOVE_ALL_REQUESTED_DATA_NODES =
      "Failed to remove all requested data nodes";
  public static final String THERE_EXIST_DATA_NODE_IN_REQUEST_BUT_NOT_IN_CLUSTER =
      "there exist Data Node in request but not in cluster";

  public static final String FAILED_IN_THE_WRITE_API_EXECUTING_THE_CONSENSUS_LAYER_DUE =
      "Failed in the write API executing the consensus layer due to: ";

  private ProcedureMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_MS_95BA098D = " ms.";
  public static final String LOG_ARG_JOIN_WAIT_GOT_INTERRUPTED_316B5E9F = "{} join wait got interrupted";
  public static final String LOG_NO_COMPLETED_PROCEDURES_CLEANUP_50434D91 = "No completed procedures to cleanup.";
  public static final String LOG_ERROR_DELETING_COMPLETED_PROCEDURES_ARG_1A3A185E = "Error deleting completed procedures {}.";
  public static final String LOG_EVICT_COMPLETED_ARG_A968A070 = "Evict completed {}";
  public static final String LOG_EXECUTING_PROCEDURE_SHOULD_RUNNABLE_STATE_BUT_IT_S_NOT_PROCEDURE_7CF42CE8 = "The executing procedure should in RUNNABLE state, but it's not. Procedure is {}";
  public static final String LOG_FINISHED_SUBPROCEDURE_PID_ARG_RESUME_PROCESSING_PPID_ARG_93ED990B = "Finished subprocedure pid={}, resume processing ppid={}";
  public static final String LOG_HALT_PID_ARG_ACTIVECOUNT_ARG_411F3EBF = "Halt pid={}, activeCount={}";
  public static final String LOG_EXCEPTION_HAPPENED_WORKER_ARG_EXECUTE_PROCEDURE_ARG_6E3AD27D = "Exception happened when worker {} execute procedure {}";
  public static final String LOG_WORKER_STUCK_ARG_ARG_RUN_TIME_ARG_MS_FB612354 = "Worker stuck {}({}), run time {} ms";
  public static final String LOG_PROCEDURE_WORKERS_ARG_RUNNING_ARG_RUNNING_STUCK_1565936D = "Procedure workers: {} is running, {} is running and stuck";
  public static final String LOG_PROCEDUREEXECUTOR_THREADGROUP_ARG_CONTAINS_RUNNING_THREADS_WHICH_USED_NON_PROCEDURE_BD865211 =
      "ProcedureExecutor threadGroup {} contains running threads which are used by non-procedure"
      + " module.";
  public static final String LOG_ADD_PROCEDURE_ARG_AS_ARG_TH_ROLLBACK_STEP_C71B2184 = "Add procedure {} as the {}th rollback step";
  public static final String LOG_STATEMACHINEPROCEDURE_PID_ARG_NOT_SET_NEXT_STATE_BUT_RETURN_HAS_7F93E63F =
      "StateMachineProcedure pid={} not set next state, but return HAS_MORE_STATE. It is likely that"
      + " there is some problem with the code. Please check the code. This procedure is about to be"
      + " terminated: {}";
  public static final String LOG_STATEMACHINEPROCEDURE_PID_ARG_SET_NEXT_STATE_ARG_BUT_RETURN_NO_0CA2D56C = "StateMachineProcedure pid={} set next state to {}, but return NO_MORE_STATE";
  public static final String LOG_DON_T_ADD_SUCCESSFUL_PROCEDURE_BACK_SCHEDULER_IT_WILL_IGNORED_E015472C = "Don't add a successful procedure back to the scheduler, it will be ignored";
  public static final String LOG_SCHEDULER_NOT_RUNNING_6969C9FF = "the scheduler is not running";
  public static final String LOG_SCHEDULER_WAITING_TIME_LEFT_ARG_NANOS_D7717019 = "the scheduler waiting time left {} nanos";
  public static final String LOG_SLEEP_FAILED_CONFIGNODEPROCEDUREENV_BCD470AC = "Sleep failed in ConfigNodeProcedureEnv: ";
  public static final String LOG_INVALIDATE_CACHE_FAILED_BECAUSE_DATANODE_ARG_UNKNOWN_4F2D374C = "Invalidate cache failed, because DataNode {} is Unknown";
  public static final String LOG_INVALIDATE_CACHE_FAILED_INVALIDATE_PARTITION_CACHE_STATUS_ARG_INVALIDATE_SCHEMAENGINE_BEB7A065 =
      "Invalidate cache failed, invalidate partition cache status is {}, invalidate schemaengine"
      + " cache status is {}";
  public static final String MESSAGE_REMOVE_CONFIGNODE_FAILED_BECAUSE_UPDATE_CONSENSUSGROUP_PEER_INFORMATION_FAILED_FCE5302B = "Remove ConfigNode failed because update ConsensusGroup peer information failed.";
  public static final String MESSAGE_CAN_T_REMOVE_DATANODE_LIMIT_REPLICATION_FACTOR_D960E3A6 = "Can't remove datanode due to the limit of replication factor, ";
  public static final String MESSAGE_AVAILABLEDATANODESIZE_ARG_MAXREPLICAFACTOR_ARG_MAX_ALLOWED_REMOVED_DATA_NODE_SIZE_FB8C382C = "availableDataNodeSize: %s, maxReplicaFactor: %s, max allowed removed Data Node size is: %s";
  public static final String EXCEPTION_NOT_SUPPORTED_0A83F963 = " is not supported";
  public static final String LOG_START_ADD_TRIGGER_ARG_TRIGGERTABLE_CONFIG_NODES_NEEDTOSAVEJAR_ARG_0C23D81E = "Start to add trigger [{}] in TriggerTable on Config Nodes, needToSaveJar[{}]";
  public static final String LOG_START_CREATE_TRIGGERINSTANCE_ARG_DATA_NODES_917C3313 = "Start to create triggerInstance [{}] on Data Nodes";
  public static final String LOG_START_ACTIVE_TRIGGER_ARG_DATA_NODES_A4AB8131 = "Start to active trigger [{}] on Data Nodes";
  public static final String LOG_START_ACTIVE_TRIGGER_ARG_CONFIG_NODES_153A5D40 = "Start to active trigger [{}] on Config Nodes";
  public static final String LOG_RETRIEVABLE_ERROR_TRYING_CREATE_TRIGGER_ARG_STATE_ARG_44976C4E = "Retrievable error trying to create trigger [{}], state [{}]";
  public static final String LOG_START_CONFIG_NODE_INACTIVE_ROLLBACK_TRIGGER_ARG_536929E5 = "Start to [CONFIG_NODE_INACTIVE] rollback of trigger [{}]";
  public static final String LOG_START_DATA_NODE_INACTIVE_ROLLBACK_TRIGGER_ARG_38C93D64 = "Start to [DATA_NODE_INACTIVE] rollback of trigger [{}]";
  public static final String LOG_RETRIEVABLE_ERROR_TRYING_DROP_TRIGGER_ARG_STATE_ARG_2282AC35 = "Retrievable error trying to drop trigger [{}], state [{}]";
  public static final String LOG_DELETEDATABASEPROCEDURE_PRE_DELETE_DATABASE_ARG_6A1FEACC = "[DeleteDatabaseProcedure] Pre delete database: {}";
  public static final String LOG_DELETEDATABASEPROCEDURE_INVALIDATE_CACHE_DATABASE_ARG_299FC9BC = "[DeleteDatabaseProcedure] Invalidate cache of database: {}";
  public static final String LOG_DELETEDATABASEPROCEDURE_DELETE_DATABASESCHEMA_ARG_A49A47AC = "[DeleteDatabaseProcedure] Delete DatabaseSchema: {}";
  public static final String LOG_DELETEDATABASEPROCEDURE_SUCCESSFULLY_DELETE_SCHEMAREGION_ARG_ARG_BA0535DA = "[DeleteDatabaseProcedure] Successfully delete SchemaRegion[{}] on {}";
  public static final String LOG_DELETEDATABASEPROCEDURE_FAILED_DELETE_SCHEMAREGION_ARG_ARG_SUBMIT_ASYNC_DELETION_8C3E6DE3 = "[DeleteDatabaseProcedure] Failed to delete SchemaRegion[{}] on {}. Submit to async deletion.";
  public static final String LOG_DELETEDATABASEPROCEDURE_DATA_PARTITION_POLICY_TABLE_DATABASE_ARG_CLEARED_7A32E28A = "[DeleteDatabaseProcedure] The data partition policy table of database: {} is cleared.";
  public static final String LOG_DELETEDATABASEPROCEDURE_DATABASE_ARG_DELETED_SUCCESSFULLY_3A4E9202 = "[DeleteDatabaseProcedure] Database: {} is deleted successfully";
  public static final String LOG_DELETEDATABASEPROCEDURE_RETRIABLE_ERROR_TRYING_DELETE_DATABASE_ARG_STATE_ARG_8167D246 = "[DeleteDatabaseProcedure] Retriable error trying to delete database {}, state {}";
  public static final String LOG_DELETEDATABASEPROCEDURE_ROLLBACK_PREDELETED_ARG_638F53DA = "[DeleteDatabaseProcedure] Rollback to preDeleted: {}";
  public static final String EXCEPTION_FAILED_DAA6EA2F = " failed ";
  public static final String EXCEPTION_FAILED_CHECK_TIME_SERIES_EXISTENCE_ALL_REPLICASET_SCHEMAREGION_ARG_FAILURES_5F668154 = "failed to check time series existence in all replicaset of schemaRegion %s. Failures: %s";
  public static final String LOG_FAILED_ROLLBACK_CONFIGNODE_TTL_STATE_9666EF54 = "Failed to rollback ConfigNode ttl state.";
  public static final String LOG_FAILED_ROLLBACK_DATANODE_TTL_CACHE_436C008A = "Failed to rollback DataNode ttl cache.";
  public static final String EXCEPTION_ROLLBACK_CONFIGNODE_TTL_FAILED_6D4FB59A = "Rollback ConfigNode ttl failed for ";
  public static final String EXCEPTION_ROLLBACK_DATANODE_TTL_CACHE_FAILED_AF9C7102 = "Rollback dataNode ttl cache failed for ";
  public static final String LOG_PLEASE_VERIFY_WHETHER_LEADER_CHANGE_HAS_OCCURRED_DURING_STAGE_9FE68EE3 = "Please verify whether a leader change has occurred during this stage. ";
  public static final String LOG_IF_LOG_TRIGGERED_WITHOUT_LEADER_CHANGE_IT_INDICATES_POTENTIAL_BUG_32AE71FD =
      "If this log is triggered without a leader change, it indicates a potential bug in the"
      + " partition table.";
  public static final String LOG_SKIP_RECOVERING_SCHEDULE_TASK_CQ_ARG_BECAUSE_ITS_METADATA_UNAVAILABLE_00286802 = "Skip recovering the schedule task of CQ {} because its metadata is unavailable.";
  public static final String LOG_PROCEDUREID_ARG_ACQUIRE_LOCK_3FBF9987 = "procedureId {} acquire lock.";
  public static final String LOG_PROCEDUREID_ARG_ACQUIRE_LOCK_FAILED_WILL_WAIT_LOCK_AFTER_FINISHING_3B27278E = "procedureId {} acquire lock failed, will wait for lock after finishing execution.";
  public static final String LOG_PROCEDUREID_ARG_RELEASE_LOCK_FF860D6B = "procedureId {} release lock.";
  public static final String LOG_RETRIEVABLE_ERROR_TRYING_ADD_CONFIG_NODE_ARG_STATE_ARG_D7285810 = "Retrievable error trying to add config node {}, state {}";
  public static final String LOG_RETRIEVABLE_ERROR_TRYING_REMOVE_CONFIG_NODE_ARG_STATE_ARG_3754EBA1 = "Retrievable error trying to remove config node {}, state {}";
  public static final String LOG_PROCEDUREID_ARG_REMOVEDATANODES_SKIPS_ACQUIRING_LOCK_SINCE_UPPER_LAYER_ENSURES_C7546FF8 =
      "procedureId {}-RemoveDataNodes skips acquiring lock, since upper layer ensures the serial"
      + " execution.";
  public static final String LOG_PROCEDUREID_ARG_REMOVEDATANODES_SKIPS_RELEASING_LOCK_SINCE_IT_HASN_T_AED8A3DA = "procedureId {}-RemoveDataNodes skips releasing lock, since it hasn't acquire any lock.";
  public static final String LOG_ARG_CAN_NOT_REMOVE_DATANODE_ARG_495F9F85 = "{}, Can not remove DataNode {} ";
  public static final String LOG_BECAUSE_NUMBER_DATANODES_LESS_EQUAL_THAN_REGION_REPLICA_NUMBER_DEC0CB38 = "because the number of DataNodes is less or equal than region replica number";
  public static final String LOG_ARG_DATANODE_REGIONS_REMOVED_ARG_216A7DC7 = "{}, DataNode regions to be removed is {}";
  public static final String LOG_RETRIEVABLE_ERROR_TRYING_REMOVE_DATA_NODE_ARG_STATE_ARG_4EFEB850 = "Retrievable error trying to remove data node {}, state {}";
  public static final String LOG_SUBMIT_REGIONMIGRATEPROCEDURE_REGIONID_ARG_REMOVEDDATANODE_ARG_DESTDATANODE_ARG_COORDINATORFORADDPEER_ARG_ =
      "Submit RegionMigrateProcedure for regionId {}: removedDataNode={}, destDataNode={},"
      + " coordinatorForAddPeer={}, coordinatorForRemovePeer={}";
  public static final String LOG_ARG_CANNOT_FIND_TARGET_DATANODE_MIGRATE_REGION_ARG_81A78E06 = "{}, Cannot find target DataNode to migrate the region: {}";
  public static final String LOG_ARG_SOME_REGIONS_MIGRATED_FAILED_DATANODE_ARG_MIGRATEDFAILEDREGIONS_ARG_11644841 = "{}, Some regions are migrated failed in DataNode: {}, migratedFailedRegions: {}.";
  public static final String LOG_REGIONS_HAVE_BEEN_SUCCESSFULLY_MIGRATED_WILL_NOT_ROLL_BACK_YOU_AE904563 =
      "Regions that have been successfully migrated will not roll back, you can submit the"
      + " RemoveDataNodes task again later.";
  public static final String LOG_ARG_DATANODES_ARG_ALL_REGIONS_MIGRATED_SUCCESSFULLY_START_STOP_THEM_32D56F28 = "{}, DataNodes: {} all regions migrated successfully, start to stop them.";
  public static final String LOG_ARG_START_ROLL_BACK_DATANODES_STATUS_ARG_05C67270 = "{}, Start to roll back the DataNodes status: {}";
  public static final String LOG_ARG_ROLL_BACK_DATANODES_STATUS_SUCCESSFULLY_ARG_6773A2DF = "{}, Roll back the DataNodes status successfully: {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATANODES_REGISTERED_NO_WAY_COLLECT_EARLIEST_TIMESLOTS_WAITING_7025EB23 =
      "[DataPartitionIntegrity] No DataNodes registered, no way to collect earliest timeslots,"
      + " waiting for them to go up";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_COLLECTED_EARLIEST_TIMESLOTS_DATANODE_ID_ARG_ALREADY_OUT_834B62B9 =
      "[DataPartitionIntegrity] Failed to collected earliest timeslots from the DataNode[id={}],"
      + " already out of max retry time";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_COLLECTED_EARLIEST_TIMESLOTS_DATANODE_ID_ARG_RESPONSE_STATUS_B0A31EC4 =
      "[DataPartitionIntegrity] Failed to collected earliest timeslots from the DataNode[id={}],"
      + " response status is {}";
  public static final String LOG_COLLECTED_EARLIEST_TIMESLOTS_DATANODE_ID_ARG_ARG_5CDF2BA6 = "Collected earliest timeslots from the DataNode[id={}]: {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_COLLECT_EARLIEST_TIMESLOTS_DATANODE_ID_ARG_ARG_A211840A = "[DataPartitionIntegrity] Failed to collect earliest timeslots from the DataNode[id={}]: {}";
  public static final String LOG_COLLECTED_EARLIEST_TIMESLOTS_ARG_DATANODES_ARG_NUMBER_SUCCESSFUL_DATANODES_ARG_1CC129EF = "Collected earliest timeslots from {} DataNodes: {}, the number of successful DataNodes is {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_MISSING_DATA_PARTITIONS_DETECTED_NOTHING_NEEDS_REPAIRED_TERMINATING_72F2635F =
      "[DataPartitionIntegrity] No missing data partitions detected, nothing needs to be repaired,"
      + " terminating procedure";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATA_PARTITION_TABLE_RELATED_DATABASE_ARG_WAS_FOUND_B5B90613 =
      "[DataPartitionIntegrity] No data partition table related to database {} was found from the"
      + " ConfigNode, and this issue needs to be repaired";
  public static final String LOG_DATAPARTITIONINTEGRITY_DATABASE_ARG_HAS_LOST_TIMESLOT_ARG_ITS_DATA_TABLE_499AF395 =
      "[DataPartitionIntegrity] Database {} has lost timeslot {} in its data table partition, and"
      + " this issue needs to be repaired";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATABASES_HAVE_LOST_DATA_PARTITIONS_TERMINATING_PROCEDURE_3E718CC3 = "[DataPartitionIntegrity] No databases have lost data partitions, terminating procedure";
  public static final String LOG_DATAPARTITIONINTEGRITY_IDENTIFIED_ARG_DATABASES_HAVE_LOST_DATA_PARTITIONS_WILL_REQUEST_6DEA7502 =
      "[DataPartitionIntegrity] Identified {} databases have lost data partitions, will request"
      + " DataPartitionTable generation from {} DataNodes";
  public static final String LOG_REQUESTING_DATAPARTITIONTABLE_GENERATION_ARG_DATANODES_559F97E8 = "Requesting DataPartitionTable generation from {} DataNodes...";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATANODES_REGISTERED_NO_WAY_REQUESTED_DATAPARTITIONTABLE_GENERATION_TERMINATING_ =
      "[DataPartitionIntegrity] No DataNodes registered, no way to requested DataPartitionTable"
      + " generation, terminating procedure";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_REQUEST_DATAPARTITIONTABLE_GENERATION_DATANODE_ID_ARG_ALREADY_OUT_6B0C9351 =
      "[DataPartitionIntegrity] Failed to request DataPartitionTable generation from the"
      + " DataNode[id={}], already out of max retry time";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_REQUEST_DATAPARTITIONTABLE_GENERATION_DATANODE_ID_ARG_RESPONSE_STATUS_93012D =
      "[DataPartitionIntegrity] Failed to request DataPartitionTable generation from the"
      + " DataNode[id={}], response status is {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_REQUEST_DATAPARTITIONTABLE_GENERATION_DATANODE_ID_ARG_ARG_818B47B8 =
      "[DataPartitionIntegrity] Failed to request DataPartitionTable generation from"
      + " DataNode[id={}]: {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_REQUEST_DATAPARTITIONTABLE_GENERATION_HEART_BEAT_DATANODE_ID_ARG_2AB63F12 =
      "[DataPartitionIntegrity] Failed to request DataPartitionTable generation heart beat from the"
      + " DataNode[id={}], already out of max retry time";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_REQUEST_DATAPARTITIONTABLE_GENERATION_HEART_BEAT_DATANODE_ID_ARG_DC1702EF =
      "[DataPartitionIntegrity] Failed to request DataPartitionTable generation heart beat from the"
      + " DataNode[id={}], state is {}, response status is {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_DATANODE_ARG_COMPLETED_DATAPARTITIONTABLE_GENERATION_TERMINATING_HEART_BEAT_59DAAD5 =
      "[DataPartitionIntegrity] DataNode {} completed DataPartitionTable generation, terminating"
      + " heart beat";
  public static final String LOG_DATAPARTITIONINTEGRITY_DATANODE_ARG_STILL_GENERATING_DATAPARTITIONTABLE_63F84C78 = "[DataPartitionIntegrity] DataNode {} still generating DataPartitionTable";
  public static final String LOG_DATAPARTITIONINTEGRITY_DATANODE_ARG_RETURNED_UNKNOWN_ERROR_CODE_ARG_2DA6A21E = "[DataPartitionIntegrity] DataNode {} returned unknown error code: {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_ERROR_CHECKING_DATAPARTITIONTABLE_STATUS_DATANODE_ARG_ARG_TERMINATING_HEART_D6EDA91 =
      "[DataPartitionIntegrity] Error checking DataPartitionTable status from DataNode {}: {},"
      + " terminating heart beat";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATAPARTITIONTABLES_MERGE_DATAPARTITIONTABLES_EMPTY_920E3DE6 = "[DataPartitionIntegrity] No DataPartitionTables to merge, dataPartitionTables is empty";
  public static final String LOG_DATAPARTITIONINTEGRITY_NO_DATA_PARTITION_TABLE_RELATED_DATABASE_ARG_WAS_FOUND_D1698512 =
      "[DataPartitionIntegrity] No data partition table related to database {} was found from the"
      + " ConfigNode, use data partition table of DataNode directly";
  public static final String LOG_DATAPARTITIONINTEGRITY_DATAPARTITIONTABLE_SUCCESSFULLY_WRITTEN_CONSENSUS_LOG_2B1634A6 = "[DataPartitionIntegrity] DataPartitionTable successfully written to consensus log";
  public static final String LOG_DATAPARTITIONINTEGRITY_ARG_SERIALIZE_FAILED_DATANODEID_ARG_967B51AA = "[DataPartitionIntegrity] {} serialize failed for dataNodeId: {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_ARG_SERIALIZE_FINALDATAPARTITIONTABLES_FAILED_7E44DCD8 = "[DataPartitionIntegrity] {} serialize finalDataPartitionTables failed";
  public static final String LOG_DATAPARTITIONINTEGRITY_ARG_DESERIALIZE_FAILED_DATANODEID_ARG_22388A60 = "[DataPartitionIntegrity] {} deserialize failed for dataNodeId: {}";
  public static final String LOG_DATAPARTITIONINTEGRITY_ARG_DESERIALIZE_FINALDATAPARTITIONTABLES_FAILED_7E23E4BD = "[DataPartitionIntegrity] {} deserialize finalDataPartitionTables failed";
  public static final String LOG_DATAPARTITIONINTEGRITY_FAILED_DESERIALIZE_DATABASESCOPEDDATAPARTITIONTABLE_3B6933B5 = "[DataPartitionIntegrity] Failed to deserialize DatabaseScopedDataPartitionTable";
  public static final String EXCEPTION_FAILED_C6FF154E = " failed";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_EXECUTEFROMVALIDATE_97490577 = "SubscriptionHandleLeaderChangeProcedure: executeFromValidate";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES_D4E8BD37 = "SubscriptionHandleLeaderChangeProcedure: executeFromOperateOnConfigNodes";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_FAILED_PULL_COMMIT_PROGRESS_DATANODE_ARG_STATUS_ARG_8C6DEC4E =
      "SubscriptionHandleLeaderChangeProcedure: failed to pull commit progress from DataNode {},"
      + " status: {}";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_FAILED_WRITE_API_EXECUTING_CONSENSUS_LAYER_56B3832A =
      "SubscriptionHandleLeaderChangeProcedure: failed in the write API executing the consensus"
      + " layer due to: ";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_EXECUTEFROMOPERATEONDATANODES_0D9F7C98 = "SubscriptionHandleLeaderChangeProcedure: executeFromOperateOnDataNodes";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_IGNORED_FAILED_TOPIC_META_PUSH_DATANODE_ARG_STATUS_ARG_67FC003F =
      "SubscriptionHandleLeaderChangeProcedure: ignored failed topic meta push to DataNode {},"
      + " status: {}";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_IGNORED_FAILED_CONSUMER_GROUP_META_PUSH_DATANODE_ARG_STATUS_17C948 =
      "SubscriptionHandleLeaderChangeProcedure: ignored failed consumer group meta push to DataNode"
      + " {}, status: {}";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_IGNORED_FAILED_SUBSCRIPTION_RUNTIME_PUSH_UNREADABLE_DATANODE_ARG_S =
      "SubscriptionHandleLeaderChangeProcedure: ignored failed subscription runtime push to"
      + " unreadable DataNode {}, status: {}";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_ROLLBACKFROMVALIDATE_74B408B7 = "SubscriptionHandleLeaderChangeProcedure: rollbackFromValidate";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES_D4C70763 = "SubscriptionHandleLeaderChangeProcedure: rollbackFromOperateOnConfigNodes";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_ROLLBACKFROMOPERATEONDATANODES_0250F6E9 = "SubscriptionHandleLeaderChangeProcedure: rollbackFromOperateOnDataNodes";
  public static final String LOG_SUBSCRIPTIONHANDLELEADERCHANGEPROCEDURE_FAILED_DESERIALIZE_REGION_PROGRESS_KEY_ARG_SUMMARY_ARG_F6935E59 =
      "SubscriptionHandleLeaderChangeProcedure: failed to deserialize region progress, key={},"
      + " summary={}";
  public static final String EXCEPTION_FAILED_PUSH_SUBSCRIPTION_RUNTIME_STATE_READABLE_DATANODES_DURING_LEADER_CHANGE_F37E6F2C =
      "Failed to push subscription runtime state to readable DataNodes during leader change,"
      + " details: %s";
  public static final String EXCEPTION_FAILED_SERIALIZE_REGION_PROGRESS_1769D6F1 = "Failed to serialize region progress ";
  public static final String EXCEPTION_NO_READABLE_DATANODE_AVAILABLE_ACCEPT_SUBSCRIPTION_METADATA_RUNTIME_UPDATES_DURING_22E61621 =
      "No readable DataNode is available to accept subscription metadata/runtime updates during"
      + " leader change";
  public static final String LOG_CREATESUBSCRIPTIONPROCEDURE_TOPIC_ARG_USES_CONSENSUS_SUBSCRIPTION_MODE_031CF049 = "CreateSubscriptionProcedure: topic [{}] uses consensus subscription mode ";
  public static final String LOG_MODE_ARG_SKIPPING_PIPE_CREATION_5F4D1026 = "(mode={}), skipping pipe creation";
  public static final String LOG_CREATESUBSCRIPTIONPROCEDURE_CONSENSUS_BASED_TOPICS_ARG_WILL_HANDLED_DATANODE_90A9C2FD = "CreateSubscriptionProcedure: consensus-based topics {} will be handled by DataNode ";
  public static final String LOG_VIA_CONSUMER_GROUP_META_PUSH_NO_PIPE_CREATION_NEEDED_D56CFE31 = "via consumer group meta push (no pipe creation needed)";
  public static final String LOG_DROPSUBSCRIPTIONPROCEDURE_TOPIC_ARG_USES_CONSENSUS_SUBSCRIPTION_MODE_6962D13C = "DropSubscriptionProcedure: topic [{}] uses consensus subscription mode ";
  public static final String LOG_MODE_ARG_SKIPPING_PIPE_REMOVAL_133B0CD6 = "(mode={}), skipping pipe removal";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_ACQUIRELOCK_SKIP_PROCEDURE_LAST_EXECUTION_TIME_ARG_CE3DD247 = "CommitProgressSyncProcedure: acquireLock, skip the procedure due to the last execution time {}";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_EXECUTEFROMVALIDATE_CF220E1F = "CommitProgressSyncProcedure: executeFromValidate";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_EXECUTEFROMOPERATEONCONFIGNODES_0DC818CA = "CommitProgressSyncProcedure: executeFromOperateOnConfigNodes";
  public static final String LOG_FAILED_PULL_COMMIT_PROGRESS_DATANODE_ARG_STATUS_ARG_33037B29 = "Failed to pull commit progress from DataNode {}, status: {}";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_EXECUTEFROMOPERATEONDATANODES_NO_OP_34420360 = "CommitProgressSyncProcedure: executeFromOperateOnDataNodes (no-op)";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_ROLLBACKFROMVALIDATE_2309D4D2 = "CommitProgressSyncProcedure: rollbackFromValidate";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_ROLLBACKFROMOPERATEONCONFIGNODES_57CB907B = "CommitProgressSyncProcedure: rollbackFromOperateOnConfigNodes";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_ROLLBACKFROMOPERATEONDATANODES_0D2CEB50 = "CommitProgressSyncProcedure: rollbackFromOperateOnDataNodes";
  public static final String LOG_COMMITPROGRESSSYNCPROCEDURE_FAILED_DESERIALIZE_REGION_PROGRESS_KEY_ARG_SUMMARY_ARG_0202F658 = "CommitProgressSyncProcedure: failed to deserialize region progress, key={}, summary={}";
  public static final String EXCEPTION_UNEXPECTED_PARENT_444B4289 = "Unexpected parent";
  public static final String LOG_ARG_8393DD4A = "{}";
  public static final String MESSAGE_HALT_PID_ARG_ACTIVECOUNT_ARG_411F3EBF = "Halt pid={}, activeCount={}";
  public static final String MESSAGE_EXCEPTION_HAPPENED_WHEN_WORKER_ARG_EXECUTE_PROCEDURE_ARG_6E3AD27D = "Exception happened when worker {} execute procedure {}";
  public static final String EXCEPTION_CANNOT_DERIVE_A_COLLISION_FREE_DELETE_TASKID_PROCID_ARG_DELETETASKSEQ_ARG_EXCEED_THE_71B7046A =
      "cannot derive a collision-free delete taskId: procId=%d, deleteTaskSeq=%d exceed the ";
  public static final String EXCEPTION_CANNOT_DERIVE_A_COLLISION_FREE_DELETE_TASKID_PROCID_ARG_DELETETASKSEQ_ARG_EXCEED_THE_ARG_ARG_BIT_BUDGET_015C598D =
      "cannot derive a collision-free delete taskId: procId=%d, deleteTaskSeq=%d exceed the %d/%d-bit budget";
  public static final String MESSAGE_FAILED_TO_SHOW_DATAPARTITIONTABLE_INTEGRITY_CHECK_PROGRESS_5EE98694 = "Failed to show DataPartitionTable integrity check progress";
  public static final String MESSAGE_ENCOUNTERED_UNEXPECTED_DATAPARTITIONTABLEINTEGRITYCHECKPROCEDURESTATE_ARG_WHEN_SHOWING_PROGRESS_5FA2739F =
      "Encountered unexpected DataPartitionTableIntegrityCheckProcedureState {} when showing progress";
  public static final String MESSAGE_UNEXPECTED_DATAPARTITIONTABLEINTEGRITYCHECKPROCEDURESTATE_ARG_WHEN_SHOWING_PROGRESS_D3C07BA1 =
      "Unexpected DataPartitionTableIntegrityCheckProcedureState {} when showing progress";

}
