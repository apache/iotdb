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

package org.apache.iotdb.confignode.procedure.store;

import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.impl.cq.CreateCQProcedure;
import org.apache.iotdb.confignode.procedure.impl.model.CreateModelProcedure;
import org.apache.iotdb.confignode.procedure.impl.model.DropModelProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.AddConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveAINodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveConfigNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.node.RemoveDataNodeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.plugin.CreatePipePluginProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.plugin.DropPipePluginProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleLeaderChangeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeHandleMetaChangeProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.runtime.PipeMetaSyncProcedure;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.AlterPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.CreatePipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.DropPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.StartPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.pipe.task.StopPipeProcedureV2;
import org.apache.iotdb.confignode.procedure.impl.region.AddRegionPeerProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.CreateRegionGroupsProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrateProcedure;
import org.apache.iotdb.confignode.procedure.impl.region.RemoveRegionPeerProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.AlterLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeactivateTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteDatabaseProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteLogicalViewProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.DeleteTimeSeriesProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SetTTLProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.SetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.UnsetTemplateProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AddTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.CreateTableProcedure;
import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.AlterConsumerGroupProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.CreateConsumerProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.DropConsumerProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.consumer.runtime.ConsumerGroupMetaSyncProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.subscription.CreateSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.subscription.DropSubscriptionProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.AlterTopicProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.CreateTopicProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.DropTopicProcedure;
import org.apache.iotdb.confignode.procedure.impl.subscription.topic.runtime.TopicMetaSyncProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.AuthOperationProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.CreatePipeProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.DropPipeProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.StartPipeProcedure;
import org.apache.iotdb.confignode.procedure.impl.sync.StopPipeProcedure;
import org.apache.iotdb.confignode.procedure.impl.testonly.AddNeverFinishSubProcedureProcedure;
import org.apache.iotdb.confignode.procedure.impl.testonly.CreateManyDatabasesProcedure;
import org.apache.iotdb.confignode.procedure.impl.testonly.NeverFinishProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.CreateTriggerProcedure;
import org.apache.iotdb.confignode.procedure.impl.trigger.DropTriggerProcedure;
import org.apache.iotdb.confignode.service.ConfigNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ProcedureFactory implements IProcedureFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureFactory.class);

  @Override
  public Procedure create(ByteBuffer buffer) throws IOException {
    short typeCode = buffer.getShort();
    ProcedureType procedureType = ProcedureType.convertToProcedureType(typeCode);
    if (procedureType == null) {
      LOGGER.error("unrecognized log type " + typeCode);
      throw new IOException("unrecognized log type " + typeCode);
    }

    Procedure procedure;
    switch (procedureType) {
      case DELETE_DATABASE_PROCEDURE:
        procedure = new DeleteDatabaseProcedure(false);
        break;
      case ADD_CONFIG_NODE_PROCEDURE:
        procedure = new AddConfigNodeProcedure();
        break;
      case REMOVE_CONFIG_NODE_PROCEDURE:
        procedure = new RemoveConfigNodeProcedure();
        break;
      case REMOVE_DATA_NODE_PROCEDURE:
        procedure = new RemoveDataNodeProcedure();
        break;
      case REGION_MIGRATE_PROCEDURE:
        procedure = new RegionMigrateProcedure();
        break;
      case ADD_REGION_PEER_PROCEDURE:
        procedure = new AddRegionPeerProcedure();
        break;
      case REMOVE_REGION_PEER_PROCEDURE:
        procedure = new RemoveRegionPeerProcedure();
        break;
      case CREATE_REGION_GROUPS:
        procedure = new CreateRegionGroupsProcedure();
        break;
      case DELETE_TIMESERIES_PROCEDURE:
        procedure = new DeleteTimeSeriesProcedure(false);
        break;
      case DELETE_LOGICAL_VIEW_PROCEDURE:
        procedure = new DeleteLogicalViewProcedure(false);
        break;
      case ALTER_LOGICAL_VIEW_PROCEDURE:
        procedure = new AlterLogicalViewProcedure(false);
        break;
      case CREATE_TRIGGER_PROCEDURE:
        procedure = new CreateTriggerProcedure(false);
        break;
      case DROP_TRIGGER_PROCEDURE:
        procedure = new DropTriggerProcedure(false);
        break;
      case CREATE_PIPE_PROCEDURE:
        procedure = new CreatePipeProcedure();
        break;
      case START_PIPE_PROCEDURE:
        procedure = new StartPipeProcedure();
        break;
      case STOP_PIPE_PROCEDURE:
        procedure = new StopPipeProcedure();
        break;
      case DROP_PIPE_PROCEDURE:
        procedure = new DropPipeProcedure();
        break;
      case CREATE_PIPE_PROCEDURE_V2:
        procedure = new CreatePipeProcedureV2();
        break;
      case START_PIPE_PROCEDURE_V2:
        procedure = new StartPipeProcedureV2();
        break;
      case STOP_PIPE_PROCEDURE_V2:
        procedure = new StopPipeProcedureV2();
        break;
      case DROP_PIPE_PROCEDURE_V2:
        procedure = new DropPipeProcedureV2();
        break;
      case ALTER_PIPE_PROCEDURE_V2:
        procedure = new AlterPipeProcedureV2(ProcedureType.ALTER_PIPE_PROCEDURE_V2);
        break;
      case ALTER_PIPE_PROCEDURE_V3:
        procedure = new AlterPipeProcedureV2(ProcedureType.ALTER_PIPE_PROCEDURE_V3);
        break;
      case PIPE_HANDLE_LEADER_CHANGE_PROCEDURE:
        procedure = new PipeHandleLeaderChangeProcedure();
        break;
      case PIPE_META_SYNC_PROCEDURE:
        procedure = new PipeMetaSyncProcedure();
        break;
      case PIPE_HANDLE_META_CHANGE_PROCEDURE:
        procedure = new PipeHandleMetaChangeProcedure();
        break;
      case CREATE_CQ_PROCEDURE:
        procedure =
            new CreateCQProcedure(
                ConfigNode.getInstance().getConfigManager().getCQManager().getExecutor());
        break;
      case SET_TEMPLATE_PROCEDURE:
        procedure = new SetTemplateProcedure(false);
        break;
      case DEACTIVATE_TEMPLATE_PROCEDURE:
        procedure = new DeactivateTemplateProcedure(false);
        break;
      case UNSET_TEMPLATE_PROCEDURE:
        procedure = new UnsetTemplateProcedure(false);
        break;
      case CREATE_TABLE_PROCEDURE:
        procedure = new CreateTableProcedure();
        break;
      case ADD_TABLE_COLUMN_PROCEDURE:
        procedure = new AddTableColumnProcedure();
        break;
      case SET_TABLE_PROPERTIES_PROCEDURE:
        procedure = new SetTablePropertiesProcedure();
        break;
      case CREATE_PIPE_PLUGIN_PROCEDURE:
        procedure = new CreatePipePluginProcedure();
        break;
      case DROP_PIPE_PLUGIN_PROCEDURE:
        procedure = new DropPipePluginProcedure();
        break;
      case CREATE_MODEL_PROCEDURE:
        procedure = new CreateModelProcedure();
        break;
      case DROP_MODEL_PROCEDURE:
        procedure = new DropModelProcedure();
        break;
      case AUTH_OPERATE_PROCEDURE:
        procedure = new AuthOperationProcedure(false);
        break;
      case PIPE_ENRICHED_DELETE_DATABASE_PROCEDURE:
        procedure = new DeleteDatabaseProcedure(true);
        break;
      case PIPE_ENRICHED_DELETE_TIMESERIES_PROCEDURE:
        procedure = new DeleteTimeSeriesProcedure(true);
        break;
      case PIPE_ENRICHED_DEACTIVATE_TEMPLATE_PROCEDURE:
        procedure = new DeactivateTemplateProcedure(true);
        break;
      case PIPE_ENRICHED_UNSET_TEMPLATE_PROCEDURE:
        procedure = new UnsetTemplateProcedure(true);
        break;
      case PIPE_ENRICHED_SET_TEMPLATE_PROCEDURE:
        procedure = new SetTemplateProcedure(true);
        break;
      case PIPE_ENRICHED_ALTER_LOGICAL_VIEW_PROCEDURE:
        procedure = new AlterLogicalViewProcedure(true);
        break;
      case PIPE_ENRICHED_DELETE_LOGICAL_VIEW_PROCEDURE:
        procedure = new DeleteLogicalViewProcedure(true);
        break;
      case PIPE_ENRICHED_CREATE_TRIGGER_PROCEDURE:
        procedure = new CreateTriggerProcedure(true);
        break;
      case PIPE_ENRICHED_DROP_TRIGGER_PROCEDURE:
        procedure = new DropTriggerProcedure(true);
        break;
      case PIPE_ENRICHED_AUTH_OPERATE_PROCEDURE:
        procedure = new AuthOperationProcedure(true);
        break;
      case REMOVE_AI_NODE_PROCEDURE:
        procedure = new RemoveAINodeProcedure();
        break;
      case PIPE_ENRICHED_SET_TTL_PROCEDURE:
        procedure = new SetTTLProcedure(true);
        break;
      case SET_TTL_PROCEDURE:
        procedure = new SetTTLProcedure(false);
        break;
      case CREATE_TOPIC_PROCEDURE:
        procedure = new CreateTopicProcedure();
        break;
      case DROP_TOPIC_PROCEDURE:
        procedure = new DropTopicProcedure();
        break;
      case ALTER_TOPIC_PROCEDURE:
        procedure = new AlterTopicProcedure();
        break;
      case TOPIC_META_SYNC_PROCEDURE:
        procedure = new TopicMetaSyncProcedure();
        break;
      case CREATE_SUBSCRIPTION_PROCEDURE:
        procedure = new CreateSubscriptionProcedure();
        break;
      case DROP_SUBSCRIPTION_PROCEDURE:
        procedure = new DropSubscriptionProcedure();
        break;
      case CREATE_CONSUMER_PROCEDURE:
        procedure = new CreateConsumerProcedure();
        break;
      case DROP_CONSUMER_PROCEDURE:
        procedure = new DropConsumerProcedure();
        break;
      case ALTER_CONSUMER_GROUP_PROCEDURE:
        procedure = new AlterConsumerGroupProcedure();
        break;
      case CONSUMER_GROUP_META_SYNC_PROCEDURE:
        procedure = new ConsumerGroupMetaSyncProcedure();
        break;
      case CREATE_MANY_DATABASES_PROCEDURE:
        procedure = new CreateManyDatabasesProcedure();
        break;
      case NEVER_FINISH_PROCEDURE:
        procedure = new NeverFinishProcedure();
        break;
      case ADD_NEVER_FINISH_SUB_PROCEDURE_PROCEDURE:
        procedure = new AddNeverFinishSubProcedureProcedure();
        break;
      default:
        LOGGER.error("Unknown Procedure type: {}", typeCode);
        throw new IOException("Unknown Procedure type: " + typeCode);
    }
    try {
      procedure.deserialize(buffer);
    } catch (ThriftSerDeException e) {
      LOGGER.warn(
          "Catch exception while deserializing procedure, this procedure will be ignored.", e);
      procedure = null;
    }
    return procedure;
  }

  public static ProcedureType getProcedureType(Procedure<?> procedure) {
    if (procedure instanceof DeleteDatabaseProcedure) {
      return ProcedureType.DELETE_DATABASE_PROCEDURE;
    } else if (procedure instanceof AddConfigNodeProcedure) {
      return ProcedureType.ADD_CONFIG_NODE_PROCEDURE;
    } else if (procedure instanceof RemoveConfigNodeProcedure) {
      return ProcedureType.REMOVE_CONFIG_NODE_PROCEDURE;
    } else if (procedure instanceof RemoveDataNodeProcedure) {
      return ProcedureType.REMOVE_DATA_NODE_PROCEDURE;
    } else if (procedure instanceof RemoveAINodeProcedure) {
      return ProcedureType.REMOVE_AI_NODE_PROCEDURE;
    } else if (procedure instanceof RegionMigrateProcedure) {
      return ProcedureType.REGION_MIGRATE_PROCEDURE;
    } else if (procedure instanceof AddRegionPeerProcedure) {
      return ProcedureType.ADD_REGION_PEER_PROCEDURE;
    } else if (procedure instanceof RemoveRegionPeerProcedure) {
      return ProcedureType.REMOVE_REGION_PEER_PROCEDURE;
    } else if (procedure instanceof CreateRegionGroupsProcedure) {
      return ProcedureType.CREATE_REGION_GROUPS;
    } else if (procedure instanceof DeleteTimeSeriesProcedure) {
      return ProcedureType.DELETE_TIMESERIES_PROCEDURE;
    } else if (procedure instanceof CreateTriggerProcedure) {
      return ProcedureType.CREATE_TRIGGER_PROCEDURE;
    } else if (procedure instanceof DropTriggerProcedure) {
      return ProcedureType.DROP_TRIGGER_PROCEDURE;
    } else if (procedure instanceof CreatePipeProcedure) {
      return ProcedureType.CREATE_PIPE_PROCEDURE;
    } else if (procedure instanceof StartPipeProcedure) {
      return ProcedureType.START_PIPE_PROCEDURE;
    } else if (procedure instanceof StopPipeProcedure) {
      return ProcedureType.STOP_PIPE_PROCEDURE;
    } else if (procedure instanceof DropPipeProcedure) {
      return ProcedureType.DROP_PIPE_PROCEDURE;
    } else if (procedure instanceof CreateCQProcedure) {
      return ProcedureType.CREATE_CQ_PROCEDURE;
    } else if (procedure instanceof SetTemplateProcedure) {
      return ProcedureType.SET_TEMPLATE_PROCEDURE;
    } else if (procedure instanceof DeactivateTemplateProcedure) {
      return ProcedureType.DEACTIVATE_TEMPLATE_PROCEDURE;
    } else if (procedure instanceof UnsetTemplateProcedure) {
      return ProcedureType.UNSET_TEMPLATE_PROCEDURE;
    } else if (procedure instanceof CreateTableProcedure) {
      return ProcedureType.CREATE_TABLE_PROCEDURE;
    } else if (procedure instanceof AddTableColumnProcedure) {
      return ProcedureType.ADD_TABLE_COLUMN_PROCEDURE;
    } else if (procedure instanceof SetTablePropertiesProcedure) {
      return ProcedureType.SET_TABLE_PROPERTIES_PROCEDURE;
    } else if (procedure instanceof CreatePipePluginProcedure) {
      return ProcedureType.CREATE_PIPE_PLUGIN_PROCEDURE;
    } else if (procedure instanceof DropPipePluginProcedure) {
      return ProcedureType.DROP_PIPE_PLUGIN_PROCEDURE;
    } else if (procedure instanceof CreateModelProcedure) {
      return ProcedureType.CREATE_MODEL_PROCEDURE;
    } else if (procedure instanceof DropModelProcedure) {
      return ProcedureType.DROP_MODEL_PROCEDURE;
    } else if (procedure instanceof CreatePipeProcedureV2) {
      return ProcedureType.CREATE_PIPE_PROCEDURE_V2;
    } else if (procedure instanceof StartPipeProcedureV2) {
      return ProcedureType.START_PIPE_PROCEDURE_V2;
    } else if (procedure instanceof StopPipeProcedureV2) {
      return ProcedureType.STOP_PIPE_PROCEDURE_V2;
    } else if (procedure instanceof DropPipeProcedureV2) {
      return ProcedureType.DROP_PIPE_PROCEDURE_V2;
    } else if (procedure instanceof AlterPipeProcedureV2) {
      return ProcedureType.ALTER_PIPE_PROCEDURE_V2;
    } else if (procedure instanceof PipeHandleLeaderChangeProcedure) {
      return ProcedureType.PIPE_HANDLE_LEADER_CHANGE_PROCEDURE;
    } else if (procedure instanceof PipeMetaSyncProcedure) {
      return ProcedureType.PIPE_META_SYNC_PROCEDURE;
    } else if (procedure instanceof PipeHandleMetaChangeProcedure) {
      return ProcedureType.PIPE_HANDLE_META_CHANGE_PROCEDURE;
    } else if (procedure instanceof CreateTopicProcedure) {
      return ProcedureType.CREATE_TOPIC_PROCEDURE;
    } else if (procedure instanceof DropTopicProcedure) {
      return ProcedureType.DROP_TOPIC_PROCEDURE;
    } else if (procedure instanceof AlterTopicProcedure) {
      return ProcedureType.ALTER_TOPIC_PROCEDURE;
    } else if (procedure instanceof TopicMetaSyncProcedure) {
      return ProcedureType.TOPIC_META_SYNC_PROCEDURE;
    } else if (procedure instanceof CreateSubscriptionProcedure) {
      return ProcedureType.CREATE_SUBSCRIPTION_PROCEDURE;
    } else if (procedure instanceof DropSubscriptionProcedure) {
      return ProcedureType.DROP_SUBSCRIPTION_PROCEDURE;
    } else if (procedure instanceof CreateConsumerProcedure) {
      return ProcedureType.CREATE_CONSUMER_PROCEDURE;
    } else if (procedure instanceof DropConsumerProcedure) {
      return ProcedureType.DROP_CONSUMER_PROCEDURE;
    } else if (procedure instanceof AlterConsumerGroupProcedure) {
      return ProcedureType.ALTER_CONSUMER_GROUP_PROCEDURE;
    } else if (procedure instanceof ConsumerGroupMetaSyncProcedure) {
      return ProcedureType.CONSUMER_GROUP_META_SYNC_PROCEDURE;
    } else if (procedure instanceof DeleteLogicalViewProcedure) {
      return ProcedureType.DELETE_LOGICAL_VIEW_PROCEDURE;
    } else if (procedure instanceof AlterLogicalViewProcedure) {
      return ProcedureType.ALTER_LOGICAL_VIEW_PROCEDURE;
    } else if (procedure instanceof AuthOperationProcedure) {
      return ProcedureType.AUTH_OPERATE_PROCEDURE;
    } else if (procedure instanceof SetTTLProcedure) {
      return ProcedureType.SET_TTL_PROCEDURE;
    } else if (procedure instanceof CreateManyDatabasesProcedure) {
      return ProcedureType.CREATE_MANY_DATABASES_PROCEDURE;
    } else if (procedure instanceof NeverFinishProcedure) {
      return ProcedureType.NEVER_FINISH_PROCEDURE;
    } else if (procedure instanceof AddNeverFinishSubProcedureProcedure) {
      return ProcedureType.ADD_NEVER_FINISH_SUB_PROCEDURE_PROCEDURE;
    }
    throw new UnsupportedOperationException(
        "Procedure type " + procedure.getClass() + " is not supported");
  }

  private static class ProcedureFactoryHolder {

    private static final ProcedureFactory INSTANCE = new ProcedureFactory();

    private ProcedureFactoryHolder() {
      // Empty constructor
    }
  }

  public static ProcedureFactory getInstance() {
    return ProcedureFactoryHolder.INSTANCE;
  }
}
