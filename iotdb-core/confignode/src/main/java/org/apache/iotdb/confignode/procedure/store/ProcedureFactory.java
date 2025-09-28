1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure.store;
1
1import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
1import org.apache.iotdb.confignode.procedure.Procedure;
1import org.apache.iotdb.confignode.procedure.impl.cq.CreateCQProcedure;
1import org.apache.iotdb.confignode.procedure.impl.model.CreateModelProcedure;
1import org.apache.iotdb.confignode.procedure.impl.model.DropModelProcedure;
1import org.apache.iotdb.confignode.procedure.impl.node.AddConfigNodeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.node.RemoveAINodeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.node.RemoveConfigNodeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.plugin.CreatePipePluginProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.plugin.DropPipePluginProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleLeaderChangeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleMetaChangeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeMetaSyncProcedure;
1import org.apache.iotdb.confignode.procedure.impl.pipe.task.AlterPipeProcedureV2;
1import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
1import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
1import org.apache.iotdb.confignode.procedure.impl.pipe.task.StartPipeProcedureV2;
1import org.apache.iotdb.confignode.procedure.impl.pipe.task.StopPipeProcedureV2;
1import org.apache.iotdb.confignode.procedure.impl.region.AddRegionPeerProcedure;
1import org.apache.iotdb.confignode.procedure.impl.region.CreateRegionGroupsProcedure;
1import org.apache.iotdb.confignode.procedure.impl.region.NotifyRegionMigrationProcedure;
1import org.apache.iotdb.confignode.procedure.impl.region.ReconstructRegionProcedure;
1import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrateProcedure;
1import org.apache.iotdb.confignode.procedure.impl.region.RemoveRegionPeerProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.AlterLogicalViewProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.DeactivateTemplateProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.DeleteLogicalViewProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.SetTTLProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.SetTemplateProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.UnsetTemplateProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.AddTableColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.CreateTableProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.DeleteDevicesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.DropTableProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.RenameTableProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.AddViewColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.CreateTableViewProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.DropViewProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewColumnProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.RenameViewProcedure;
1import org.apache.iotdb.confignode.procedure.impl.schema.table.view.SetViewPropertiesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.CreateConsumerProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.DropConsumerProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.runtime.ConsumerGroupMetaSyncProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.subscription.CreateSubscriptionProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.subscription.DropSubscriptionProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.topic.AlterTopicProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.topic.CreateTopicProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.topic.DropTopicProcedure;
1import org.apache.iotdb.confignode.procedure.impl.subscription.topic.runtime.TopicMetaSyncProcedure;
1import org.apache.iotdb.confignode.procedure.impl.sync.AuthOperationProcedure;
1import org.apache.iotdb.confignode.procedure.impl.sync.CreatePipeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.sync.DropPipeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.sync.StartPipeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.sync.StopPipeProcedure;
1import org.apache.iotdb.confignode.procedure.impl.testonly.AddNeverFinishSubProcedureProcedure;
1import org.apache.iotdb.confignode.procedure.impl.testonly.CreateManyDatabasesProcedure;
1import org.apache.iotdb.confignode.procedure.impl.testonly.NeverFinishProcedure;
1import org.apache.iotdb.confignode.procedure.impl.trigger.CreateTriggerProcedure;
1import org.apache.iotdb.confignode.procedure.impl.trigger.DropTriggerProcedure;
1import org.apache.iotdb.confignode.service.ConfigNode;
1
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.io.IOException;
1import java.nio.ByteBuffer;
1
1public class ProcedureFactory implements IProcedureFactory {
1
1  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureFactory.class);
1
1  @Override
1  public Procedure create(ByteBuffer buffer) throws IOException {
1    short typeCode = buffer.getShort();
1    ProcedureType procedureType = ProcedureType.convertToProcedureType(typeCode);
1    if (procedureType == null) {
1      LOGGER.error("unrecognized log type " + typeCode);
1      throw new IOException("unrecognized log type " + typeCode);
1    }
1
1    Procedure procedure;
1    switch (procedureType) {
1      case DELETE_DATABASE_PROCEDURE:
1        procedure = new DeleteDatabaseProcedure(false);
1        break;
1      case ADD_CONFIG_NODE_PROCEDURE:
1        procedure = new AddConfigNodeProcedure();
1        break;
1      case REMOVE_CONFIG_NODE_PROCEDURE:
1        procedure = new RemoveConfigNodeProcedure();
1        break;
1      case REMOVE_DATA_NODE_PROCEDURE:
1        procedure = new RemoveDataNodesProcedure();
1        break;
1      case REGION_MIGRATE_PROCEDURE:
1        procedure = new RegionMigrateProcedure();
1        break;
1      case ADD_REGION_PEER_PROCEDURE:
1        procedure = new AddRegionPeerProcedure();
1        break;
1      case REMOVE_REGION_PEER_PROCEDURE:
1        procedure = new RemoveRegionPeerProcedure();
1        break;
1      case CREATE_REGION_GROUPS:
1        procedure = new CreateRegionGroupsProcedure();
1        break;
1      case RECONSTRUCT_REGION_PROCEDURE:
1        procedure = new ReconstructRegionProcedure();
1        break;
1      case NOTIFY_REGION_MIGRATION_PROCEDURE:
1        procedure = new NotifyRegionMigrationProcedure();
1        break;
1      case DELETE_TIMESERIES_PROCEDURE:
1        procedure = new DeleteTimeSeriesProcedure(false);
1        break;
1      case DELETE_LOGICAL_VIEW_PROCEDURE:
1        procedure = new DeleteLogicalViewProcedure(false);
1        break;
1      case ALTER_LOGICAL_VIEW_PROCEDURE:
1        procedure = new AlterLogicalViewProcedure(false);
1        break;
1      case CREATE_TRIGGER_PROCEDURE:
1        procedure = new CreateTriggerProcedure(false);
1        break;
1      case DROP_TRIGGER_PROCEDURE:
1        procedure = new DropTriggerProcedure(false);
1        break;
1      case CREATE_PIPE_PROCEDURE:
1        procedure = new CreatePipeProcedure();
1        break;
1      case START_PIPE_PROCEDURE:
1        procedure = new StartPipeProcedure();
1        break;
1      case STOP_PIPE_PROCEDURE:
1        procedure = new StopPipeProcedure();
1        break;
1      case DROP_PIPE_PROCEDURE:
1        procedure = new DropPipeProcedure();
1        break;
1      case CREATE_PIPE_PROCEDURE_V2:
1        procedure = new CreatePipeProcedureV2();
1        break;
1      case START_PIPE_PROCEDURE_V2:
1        procedure = new StartPipeProcedureV2();
1        break;
1      case STOP_PIPE_PROCEDURE_V2:
1        procedure = new StopPipeProcedureV2();
1        break;
1      case DROP_PIPE_PROCEDURE_V2:
1        procedure = new DropPipeProcedureV2();
1        break;
1      case ALTER_PIPE_PROCEDURE_V2:
1        procedure = new AlterPipeProcedureV2(ProcedureType.ALTER_PIPE_PROCEDURE_V2);
1        break;
1      case ALTER_PIPE_PROCEDURE_V3:
1        procedure = new AlterPipeProcedureV2(ProcedureType.ALTER_PIPE_PROCEDURE_V3);
1        break;
1      case PIPE_HANDLE_LEADER_CHANGE_PROCEDURE:
1        procedure = new PipeHandleLeaderChangeProcedure();
1        break;
1      case PIPE_META_SYNC_PROCEDURE:
1        procedure = new PipeMetaSyncProcedure();
1        break;
1      case PIPE_HANDLE_META_CHANGE_PROCEDURE:
1        procedure = new PipeHandleMetaChangeProcedure();
1        break;
1      case CREATE_CQ_PROCEDURE:
1        procedure =
1            new CreateCQProcedure(
1                ConfigNode.getInstance().getConfigManager().getCQManager().getExecutor());
1        break;
1      case SET_TEMPLATE_PROCEDURE:
1        procedure = new SetTemplateProcedure(false);
1        break;
1      case DEACTIVATE_TEMPLATE_PROCEDURE:
1        procedure = new DeactivateTemplateProcedure(false);
1        break;
1      case UNSET_TEMPLATE_PROCEDURE:
1        procedure = new UnsetTemplateProcedure(false);
1        break;
1      case CREATE_TABLE_PROCEDURE:
1        procedure = new CreateTableProcedure(false);
1        break;
1      case CREATE_TABLE_VIEW_PROCEDURE:
1        procedure = new CreateTableViewProcedure(false);
1        break;
1      case ADD_TABLE_COLUMN_PROCEDURE:
1        procedure = new AddTableColumnProcedure(false);
1        break;
1      case ADD_VIEW_COLUMN_PROCEDURE:
1        procedure = new AddViewColumnProcedure(false);
1        break;
1      case SET_TABLE_PROPERTIES_PROCEDURE:
1        procedure = new SetTablePropertiesProcedure(false);
1        break;
1      case SET_VIEW_PROPERTIES_PROCEDURE:
1        procedure = new SetViewPropertiesProcedure(false);
1        break;
1      case RENAME_TABLE_COLUMN_PROCEDURE:
1        procedure = new RenameTableColumnProcedure(false);
1        break;
1      case RENAME_VIEW_COLUMN_PROCEDURE:
1        procedure = new RenameViewColumnProcedure(false);
1        break;
1      case DROP_TABLE_COLUMN_PROCEDURE:
1        procedure = new DropTableColumnProcedure(false);
1        break;
1      case DROP_VIEW_COLUMN_PROCEDURE:
1        procedure = new DropViewColumnProcedure(false);
1        break;
1      case DROP_TABLE_PROCEDURE:
1        procedure = new DropTableProcedure(false);
1        break;
1      case DROP_VIEW_PROCEDURE:
1        procedure = new DropViewProcedure(false);
1        break;
1      case DELETE_DEVICES_PROCEDURE:
1        procedure = new DeleteDevicesProcedure(false);
1        break;
1      case RENAME_TABLE_PROCEDURE:
1        procedure = new RenameTableProcedure(false);
1        break;
1      case RENAME_VIEW_PROCEDURE:
1        procedure = new RenameViewProcedure(false);
1        break;
1      case CREATE_PIPE_PLUGIN_PROCEDURE:
1        procedure = new CreatePipePluginProcedure();
1        break;
1      case DROP_PIPE_PLUGIN_PROCEDURE:
1        procedure = new DropPipePluginProcedure();
1        break;
1      case CREATE_MODEL_PROCEDURE:
1        procedure = new CreateModelProcedure();
1        break;
1      case DROP_MODEL_PROCEDURE:
1        procedure = new DropModelProcedure();
1        break;
1      case AUTH_OPERATE_PROCEDURE:
1        procedure = new AuthOperationProcedure(false);
1        break;
1      case PIPE_ENRICHED_DELETE_DATABASE_PROCEDURE:
1        procedure = new DeleteDatabaseProcedure(true);
1        break;
1      case PIPE_ENRICHED_DELETE_TIMESERIES_PROCEDURE:
1        procedure = new DeleteTimeSeriesProcedure(true);
1        break;
1      case PIPE_ENRICHED_DEACTIVATE_TEMPLATE_PROCEDURE:
1        procedure = new DeactivateTemplateProcedure(true);
1        break;
1      case PIPE_ENRICHED_UNSET_TEMPLATE_PROCEDURE:
1        procedure = new UnsetTemplateProcedure(true);
1        break;
1      case PIPE_ENRICHED_SET_TEMPLATE_PROCEDURE:
1        procedure = new SetTemplateProcedure(true);
1        break;
1      case PIPE_ENRICHED_ALTER_LOGICAL_VIEW_PROCEDURE:
1        procedure = new AlterLogicalViewProcedure(true);
1        break;
1      case PIPE_ENRICHED_DELETE_LOGICAL_VIEW_PROCEDURE:
1        procedure = new DeleteLogicalViewProcedure(true);
1        break;
1      case PIPE_ENRICHED_CREATE_TRIGGER_PROCEDURE:
1        procedure = new CreateTriggerProcedure(true);
1        break;
1      case PIPE_ENRICHED_DROP_TRIGGER_PROCEDURE:
1        procedure = new DropTriggerProcedure(true);
1        break;
1      case PIPE_ENRICHED_AUTH_OPERATE_PROCEDURE:
1        procedure = new AuthOperationProcedure(true);
1        break;
1      case PIPE_ENRICHED_CREATE_TABLE_PROCEDURE:
1        procedure = new CreateTableProcedure(true);
1        break;
1      case PIPE_ENRICHED_DROP_TABLE_PROCEDURE:
1        procedure = new DropTableProcedure(true);
1        break;
1      case PIPE_ENRICHED_ADD_TABLE_COLUMN_PROCEDURE:
1        procedure = new AddTableColumnProcedure(true);
1        break;
1      case PIPE_ENRICHED_SET_TABLE_PROPERTIES_PROCEDURE:
1        procedure = new SetTablePropertiesProcedure(true);
1        break;
1      case PIPE_ENRICHED_RENAME_TABLE_COLUMN_PROCEDURE:
1        procedure = new RenameTableColumnProcedure(true);
1        break;
1      case PIPE_ENRICHED_DROP_TABLE_COLUMN_PROCEDURE:
1        procedure = new DropTableColumnProcedure(true);
1        break;
1      case PIPE_ENRICHED_DELETE_DEVICES_PROCEDURE:
1        procedure = new DeleteDevicesProcedure(true);
1        break;
1      case PIPE_ENRICHED_RENAME_TABLE_PROCEDURE:
1        procedure = new RenameTableProcedure(true);
1        break;
1      case PIPE_ENRICHED_CREATE_TABLE_VIEW_PROCEDURE:
1        procedure = new CreateTableViewProcedure(true);
1        break;
1      case PIPE_ENRICHED_ADD_VIEW_COLUMN_PROCEDURE:
1        procedure = new AddViewColumnProcedure(true);
1        break;
1      case PIPE_ENRICHED_DROP_VIEW_COLUMN_PROCEDURE:
1        procedure = new DropViewColumnProcedure(true);
1        break;
1      case PIPE_ENRICHED_SET_VIEW_PROPERTIES_PROCEDURE:
1        procedure = new SetViewPropertiesProcedure(true);
1        break;
1      case PIPE_ENRICHED_DROP_VIEW_PROCEDURE:
1        procedure = new DropViewProcedure(true);
1        break;
1      case PIPE_ENRICHED_RENAME_VIEW_COLUMN_PROCEDURE:
1        procedure = new RenameViewColumnProcedure(true);
1        break;
1      case PIPE_ENRICHED_RENAME_VIEW_PROCEDURE:
1        procedure = new RenameViewProcedure(true);
1        break;
1      case REMOVE_AI_NODE_PROCEDURE:
1        procedure = new RemoveAINodeProcedure();
1        break;
1      case PIPE_ENRICHED_SET_TTL_PROCEDURE:
1        procedure = new SetTTLProcedure(true);
1        break;
1      case SET_TTL_PROCEDURE:
1        procedure = new SetTTLProcedure(false);
1        break;
1      case CREATE_TOPIC_PROCEDURE:
1        procedure = new CreateTopicProcedure();
1        break;
1      case DROP_TOPIC_PROCEDURE:
1        procedure = new DropTopicProcedure();
1        break;
1      case ALTER_TOPIC_PROCEDURE:
1        procedure = new AlterTopicProcedure();
1        break;
1      case TOPIC_META_SYNC_PROCEDURE:
1        procedure = new TopicMetaSyncProcedure();
1        break;
1      case CREATE_SUBSCRIPTION_PROCEDURE:
1        procedure = new CreateSubscriptionProcedure();
1        break;
1      case DROP_SUBSCRIPTION_PROCEDURE:
1        procedure = new DropSubscriptionProcedure();
1        break;
1      case CREATE_CONSUMER_PROCEDURE:
1        procedure = new CreateConsumerProcedure();
1        break;
1      case DROP_CONSUMER_PROCEDURE:
1        procedure = new DropConsumerProcedure();
1        break;
1      case ALTER_CONSUMER_GROUP_PROCEDURE:
1        procedure = new AlterConsumerGroupProcedure();
1        break;
1      case CONSUMER_GROUP_META_SYNC_PROCEDURE:
1        procedure = new ConsumerGroupMetaSyncProcedure();
1        break;
1      case CREATE_MANY_DATABASES_PROCEDURE:
1        procedure = new CreateManyDatabasesProcedure();
1        break;
1      case NEVER_FINISH_PROCEDURE:
1        procedure = new NeverFinishProcedure();
1        break;
1      case ADD_NEVER_FINISH_SUB_PROCEDURE_PROCEDURE:
1        procedure = new AddNeverFinishSubProcedureProcedure();
1        break;
1      default:
1        LOGGER.error("Unknown Procedure type: {}", typeCode);
1        throw new IOException("Unknown Procedure type: " + typeCode);
1    }
1    try {
1      procedure.deserialize(buffer);
1    } catch (ThriftSerDeException e) {
1      LOGGER.warn(
1          "Catch exception while deserializing procedure, this procedure will be ignored.", e);
1      procedure = null;
1    }
1    return procedure;
1  }
1
1  // The "pipeEnriched" is not considered here
1  public static ProcedureType getProcedureType(final Procedure<?> procedure) {
1    if (procedure instanceof DeleteDatabaseProcedure) {
1      return ProcedureType.DELETE_DATABASE_PROCEDURE;
1    } else if (procedure instanceof AddConfigNodeProcedure) {
1      return ProcedureType.ADD_CONFIG_NODE_PROCEDURE;
1    } else if (procedure instanceof RemoveConfigNodeProcedure) {
1      return ProcedureType.REMOVE_CONFIG_NODE_PROCEDURE;
1    } else if (procedure instanceof RemoveDataNodesProcedure) {
1      return ProcedureType.REMOVE_DATA_NODE_PROCEDURE;
1    } else if (procedure instanceof RemoveAINodeProcedure) {
1      return ProcedureType.REMOVE_AI_NODE_PROCEDURE;
1    } else if (procedure instanceof RegionMigrateProcedure) {
1      return ProcedureType.REGION_MIGRATE_PROCEDURE;
1    } else if (procedure instanceof AddRegionPeerProcedure) {
1      return ProcedureType.ADD_REGION_PEER_PROCEDURE;
1    } else if (procedure instanceof RemoveRegionPeerProcedure) {
1      return ProcedureType.REMOVE_REGION_PEER_PROCEDURE;
1    } else if (procedure instanceof CreateRegionGroupsProcedure) {
1      return ProcedureType.CREATE_REGION_GROUPS;
1    } else if (procedure instanceof DeleteTimeSeriesProcedure) {
1      return ProcedureType.DELETE_TIMESERIES_PROCEDURE;
1    } else if (procedure instanceof ReconstructRegionProcedure) {
1      return ProcedureType.RECONSTRUCT_REGION_PROCEDURE;
1    } else if (procedure instanceof NotifyRegionMigrationProcedure) {
1      return ProcedureType.NOTIFY_REGION_MIGRATION_PROCEDURE;
1    } else if (procedure instanceof CreateTriggerProcedure) {
1      return ProcedureType.CREATE_TRIGGER_PROCEDURE;
1    } else if (procedure instanceof DropTriggerProcedure) {
1      return ProcedureType.DROP_TRIGGER_PROCEDURE;
1    } else if (procedure instanceof CreatePipeProcedure) {
1      return ProcedureType.CREATE_PIPE_PROCEDURE;
1    } else if (procedure instanceof StartPipeProcedure) {
1      return ProcedureType.START_PIPE_PROCEDURE;
1    } else if (procedure instanceof StopPipeProcedure) {
1      return ProcedureType.STOP_PIPE_PROCEDURE;
1    } else if (procedure instanceof DropPipeProcedure) {
1      return ProcedureType.DROP_PIPE_PROCEDURE;
1    } else if (procedure instanceof CreateCQProcedure) {
1      return ProcedureType.CREATE_CQ_PROCEDURE;
1    } else if (procedure instanceof SetTemplateProcedure) {
1      return ProcedureType.SET_TEMPLATE_PROCEDURE;
1    } else if (procedure instanceof DeactivateTemplateProcedure) {
1      return ProcedureType.DEACTIVATE_TEMPLATE_PROCEDURE;
1    } else if (procedure instanceof UnsetTemplateProcedure) {
1      return ProcedureType.UNSET_TEMPLATE_PROCEDURE;
1    } else if (procedure instanceof CreateTableViewProcedure) {
1      return ProcedureType.CREATE_TABLE_VIEW_PROCEDURE;
1    } else if (procedure instanceof CreateTableProcedure) {
1      return ProcedureType.CREATE_TABLE_PROCEDURE;
1    } else if (procedure instanceof AddViewColumnProcedure) {
1      return ProcedureType.ADD_VIEW_COLUMN_PROCEDURE;
1    } else if (procedure instanceof AddTableColumnProcedure) {
1      return ProcedureType.ADD_TABLE_COLUMN_PROCEDURE;
1    } else if (procedure instanceof SetViewPropertiesProcedure) {
1      return ProcedureType.SET_VIEW_PROPERTIES_PROCEDURE;
1    } else if (procedure instanceof SetTablePropertiesProcedure) {
1      return ProcedureType.SET_TABLE_PROPERTIES_PROCEDURE;
1    } else if (procedure instanceof RenameViewColumnProcedure) {
1      return ProcedureType.RENAME_VIEW_COLUMN_PROCEDURE;
1    } else if (procedure instanceof RenameTableColumnProcedure) {
1      return ProcedureType.RENAME_TABLE_COLUMN_PROCEDURE;
1    } else if (procedure instanceof DropViewColumnProcedure) {
1      return ProcedureType.DROP_VIEW_COLUMN_PROCEDURE;
1    } else if (procedure instanceof DropTableColumnProcedure) {
1      return ProcedureType.DROP_TABLE_COLUMN_PROCEDURE;
1    } else if (procedure instanceof DropViewProcedure) {
1      return ProcedureType.DROP_VIEW_PROCEDURE;
1    } else if (procedure instanceof DropTableProcedure) {
1      return ProcedureType.DROP_TABLE_PROCEDURE;
1    } else if (procedure instanceof DeleteDevicesProcedure) {
1      return ProcedureType.DELETE_DEVICES_PROCEDURE;
1    } else if (procedure instanceof RenameViewProcedure) {
1      return ProcedureType.RENAME_VIEW_PROCEDURE;
1    } else if (procedure instanceof RenameTableProcedure) {
1      return ProcedureType.RENAME_TABLE_PROCEDURE;
1    } else if (procedure instanceof CreatePipePluginProcedure) {
1      return ProcedureType.CREATE_PIPE_PLUGIN_PROCEDURE;
1    } else if (procedure instanceof DropPipePluginProcedure) {
1      return ProcedureType.DROP_PIPE_PLUGIN_PROCEDURE;
1    } else if (procedure instanceof CreateModelProcedure) {
1      return ProcedureType.CREATE_MODEL_PROCEDURE;
1    } else if (procedure instanceof DropModelProcedure) {
1      return ProcedureType.DROP_MODEL_PROCEDURE;
1    } else if (procedure instanceof CreatePipeProcedureV2) {
1      return ProcedureType.CREATE_PIPE_PROCEDURE_V2;
1    } else if (procedure instanceof StartPipeProcedureV2) {
1      return ProcedureType.START_PIPE_PROCEDURE_V2;
1    } else if (procedure instanceof StopPipeProcedureV2) {
1      return ProcedureType.STOP_PIPE_PROCEDURE_V2;
1    } else if (procedure instanceof DropPipeProcedureV2) {
1      return ProcedureType.DROP_PIPE_PROCEDURE_V2;
1    } else if (procedure instanceof AlterPipeProcedureV2) {
1      return ProcedureType.ALTER_PIPE_PROCEDURE_V2;
1    } else if (procedure instanceof PipeHandleLeaderChangeProcedure) {
1      return ProcedureType.PIPE_HANDLE_LEADER_CHANGE_PROCEDURE;
1    } else if (procedure instanceof PipeMetaSyncProcedure) {
1      return ProcedureType.PIPE_META_SYNC_PROCEDURE;
1    } else if (procedure instanceof PipeHandleMetaChangeProcedure) {
1      return ProcedureType.PIPE_HANDLE_META_CHANGE_PROCEDURE;
1    } else if (procedure instanceof CreateTopicProcedure) {
1      return ProcedureType.CREATE_TOPIC_PROCEDURE;
1    } else if (procedure instanceof DropTopicProcedure) {
1      return ProcedureType.DROP_TOPIC_PROCEDURE;
1    } else if (procedure instanceof AlterTopicProcedure) {
1      return ProcedureType.ALTER_TOPIC_PROCEDURE;
1    } else if (procedure instanceof TopicMetaSyncProcedure) {
1      return ProcedureType.TOPIC_META_SYNC_PROCEDURE;
1    } else if (procedure instanceof CreateSubscriptionProcedure) {
1      return ProcedureType.CREATE_SUBSCRIPTION_PROCEDURE;
1    } else if (procedure instanceof DropSubscriptionProcedure) {
1      return ProcedureType.DROP_SUBSCRIPTION_PROCEDURE;
1    } else if (procedure instanceof CreateConsumerProcedure) {
1      return ProcedureType.CREATE_CONSUMER_PROCEDURE;
1    } else if (procedure instanceof DropConsumerProcedure) {
1      return ProcedureType.DROP_CONSUMER_PROCEDURE;
1    } else if (procedure instanceof AlterConsumerGroupProcedure) {
1      return ProcedureType.ALTER_CONSUMER_GROUP_PROCEDURE;
1    } else if (procedure instanceof ConsumerGroupMetaSyncProcedure) {
1      return ProcedureType.CONSUMER_GROUP_META_SYNC_PROCEDURE;
1    } else if (procedure instanceof DeleteLogicalViewProcedure) {
1      return ProcedureType.DELETE_LOGICAL_VIEW_PROCEDURE;
1    } else if (procedure instanceof AlterLogicalViewProcedure) {
1      return ProcedureType.ALTER_LOGICAL_VIEW_PROCEDURE;
1    } else if (procedure instanceof AuthOperationProcedure) {
1      return ProcedureType.AUTH_OPERATE_PROCEDURE;
1    } else if (procedure instanceof SetTTLProcedure) {
1      return ProcedureType.SET_TTL_PROCEDURE;
1    } else if (procedure instanceof CreateManyDatabasesProcedure) {
1      return ProcedureType.CREATE_MANY_DATABASES_PROCEDURE;
1    } else if (procedure instanceof NeverFinishProcedure) {
1      return ProcedureType.NEVER_FINISH_PROCEDURE;
1    } else if (procedure instanceof AddNeverFinishSubProcedureProcedure) {
1      return ProcedureType.ADD_NEVER_FINISH_SUB_PROCEDURE_PROCEDURE;
1    }
1    throw new UnsupportedOperationException(
1        "Procedure type " + procedure.getClass() + " is not supported");
1  }
1
1  private static class ProcedureFactoryHolder {
1
1    private static final ProcedureFactory INSTANCE = new ProcedureFactory();
1
1    private ProcedureFactoryHolder() {
1      // Empty constructor
1    }
1  }
1
1  public static ProcedureFactory getInstance() {
1    return ProcedureFactoryHolder.INSTANCE;
1  }
1}
1