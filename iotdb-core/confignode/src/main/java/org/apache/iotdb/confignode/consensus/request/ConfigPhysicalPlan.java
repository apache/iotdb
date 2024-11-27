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

package org.apache.iotdb.confignode.consensus.request;

import org.apache.iotdb.commons.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.confignode.consensus.request.read.ainode.GetAINodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.GetModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowModelPlan;
import org.apache.iotdb.confignode.consensus.request.read.subscription.ShowTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RegisterAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RemoveAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.ainode.UpdateAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateClusterIdPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateVersionInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.ActiveCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.AddCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.DropCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.UpdateCQLastExecTimePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.AdjustMaxRegionGroupNumPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.UpdateDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.function.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.DropTableModelFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.DropTreeModelFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.DropModelInNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.model.DropModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.AddRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.RemoveRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeactivateTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteLogicalViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteTimeSeriesPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.AlterPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.OperateMultiplePipesPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.procedure.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetThrottleQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.PollRegionMaintainTaskPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.PollSpecificRegionMaintainTaskPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.AlterConsumerGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.runtime.ConsumerGroupHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterMultipleTopicsPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.AlterTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.CreateTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.DropTopicPlan;
import org.apache.iotdb.confignode.consensus.request.write.subscription.topic.runtime.TopicHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipePlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.GetPipeSinkPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.PreCreatePipePlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.RecordPipeMessagePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.SetPipeStatusPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.PreDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RollbackCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.RollbackPreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.AddTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.DeleteTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerStateInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggersOnTransferNodesPlan;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import org.apache.tsfile.utils.PublicBAOS;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class ConfigPhysicalPlan implements IConsensusRequest {

  private final ConfigPhysicalPlanType type;

  protected ConfigPhysicalPlan(final ConfigPhysicalPlanType type) {
    this.type = type;
  }

  public ConfigPhysicalPlanType getType() {
    return this.type;
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      serializeImpl(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (final IOException e) {
      throw new SerializationRunTimeException(e);
    }
  }

  protected abstract void serializeImpl(final DataOutputStream stream) throws IOException;

  protected abstract void deserializeImpl(final ByteBuffer buffer) throws IOException;

  public int getSerializedSize() throws IOException {
    final PublicBAOS byteArrayOutputStream = new PublicBAOS();
    final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serializeImpl(outputStream);
    return byteArrayOutputStream.size();
  }

  public static class Factory {

    public static ConfigPhysicalPlan create(final ByteBuffer buffer) throws IOException {
      final short planType = buffer.getShort();
      final ConfigPhysicalPlanType configPhysicalPlanType =
          ConfigPhysicalPlanType.convertToConfigPhysicalPlanType(planType);
      if (configPhysicalPlanType == null) {
        throw new IOException("Unrecognized log configPhysicalPlanType: " + planType);
      }

      final ConfigPhysicalPlan plan;
      switch (configPhysicalPlanType) {
        case RegisterDataNode:
          plan = new RegisterDataNodePlan();
          break;
        case UpdateDataNodeConfiguration:
          plan = new UpdateDataNodePlan();
          break;
        case RemoveDataNode:
          plan = new RemoveDataNodePlan();
          break;
        case RegisterAINode:
          plan = new RegisterAINodePlan();
          break;
        case RemoveAINode:
          plan = new RemoveAINodePlan();
          break;
        case GetAINodeConfiguration:
          plan = new GetAINodeConfigurationPlan();
          break;
        case UpdateAINodeConfiguration:
          plan = new UpdateAINodePlan();
          break;
        case CreateDatabase:
          plan = new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase);
          break;
        case AlterDatabase:
          plan = new DatabaseSchemaPlan(ConfigPhysicalPlanType.AlterDatabase);
          break;
        case SetTTL:
          plan = new SetTTLPlan();
          break;
        case SetSchemaReplicationFactor:
          plan = new SetSchemaReplicationFactorPlan();
          break;
        case SetDataReplicationFactor:
          plan = new SetDataReplicationFactorPlan();
          break;
        case SetTimePartitionInterval:
          plan = new SetTimePartitionIntervalPlan();
          break;
        case AdjustMaxRegionGroupNum:
          plan = new AdjustMaxRegionGroupNumPlan();
          break;
        case CreateRegionGroups:
          plan = new CreateRegionGroupsPlan();
          break;
        case UpdateRegionLocation:
          plan = new UpdateRegionLocationPlan();
          break;
        case AddRegionLocation:
          plan = new AddRegionLocationPlan();
          break;
        case RemoveRegionLocation:
          plan = new RemoveRegionLocationPlan();
          break;
        case OfferRegionMaintainTasks:
          plan = new OfferRegionMaintainTasksPlan();
          break;
        case PollRegionMaintainTask:
          plan = new PollRegionMaintainTaskPlan();
          break;
        case PollSpecificRegionMaintainTask:
          plan = new PollSpecificRegionMaintainTaskPlan();
          break;
        case CreateSchemaPartition:
          plan = new CreateSchemaPartitionPlan();
          break;
        case CreateDataPartition:
          plan = new CreateDataPartitionPlan();
          break;
        case DeleteProcedure:
          plan = new DeleteProcedurePlan();
          break;
        case UpdateProcedure:
          plan = new UpdateProcedurePlan();
          break;
        case PreDeleteDatabase:
          plan = new PreDeleteDatabasePlan();
          break;
        case DeleteDatabase:
          plan = new DeleteDatabasePlan();
          break;
        case CreateUserDep:
        case CreateRoleDep:
        case DropUserDep:
        case DropRoleDep:
        case GrantRoleDep:
        case GrantUserDep:
        case GrantRoleToUserDep:
        case RevokeUserDep:
        case RevokeRoleDep:
        case RevokeRoleFromUserDep:
        case UpdateUserDep:
        case CreateUser:
        case CreateRole:
        case DropUser:
        case DropRole:
        case GrantRole:
        case GrantUser:
        case GrantRoleToUser:
        case RevokeUser:
        case RevokeRole:
        case RevokeRoleFromUser:
        case UpdateUser:
        case CreateUserWithRawPassword:
          plan = new AuthorPlan(configPhysicalPlanType);
          break;
        case ApplyConfigNode:
          plan = new ApplyConfigNodePlan();
          break;
        case RemoveConfigNode:
          plan = new RemoveConfigNodePlan();
          break;
        case UpdateVersionInfo:
          plan = new UpdateVersionInfoPlan();
          break;
        case UpdateClusterId:
          plan = new UpdateClusterIdPlan();
          break;
        case CreateFunction:
          plan = new CreateFunctionPlan();
          break;
        case DropTreeModelFunction:
          plan = new DropTreeModelFunctionPlan();
          break;
        case DropTableModelFunction:
          plan = new DropTableModelFunctionPlan();
          break;
        case AddTriggerInTable:
          plan = new AddTriggerInTablePlan();
          break;
        case DeleteTriggerInTable:
          plan = new DeleteTriggerInTablePlan();
          break;
        case UpdateTriggerStateInTable:
          plan = new UpdateTriggerStateInTablePlan();
          break;
        case CreateSchemaTemplate:
          plan = new CreateSchemaTemplatePlan();
          break;
        case SetSchemaTemplate:
          plan = new SetSchemaTemplatePlan();
          break;
        case PreSetSchemaTemplate:
          plan = new PreSetSchemaTemplatePlan();
          break;
        case CommitSetSchemaTemplate:
          plan = new CommitSetSchemaTemplatePlan();
          break;
        case DropSchemaTemplate:
          plan = new DropSchemaTemplatePlan();
          break;
        case PreUnsetTemplate:
          plan = new PreUnsetSchemaTemplatePlan();
          break;
        case RollbackUnsetTemplate:
          plan = new RollbackPreUnsetSchemaTemplatePlan();
          break;
        case UnsetTemplate:
          plan = new UnsetSchemaTemplatePlan();
          break;
        case ExtendSchemaTemplate:
          plan = new ExtendSchemaTemplatePlan();
          break;
        case PreCreateTable:
          plan = new PreCreateTablePlan();
          break;
        case RollbackCreateTable:
          plan = new RollbackCreateTablePlan();
          break;
        case CommitCreateTable:
          plan = new CommitCreateTablePlan();
          break;
        case AddTableColumn:
          plan = new AddTableColumnPlan();
          break;
        case SetTableProperties:
          plan = new SetTablePropertiesPlan();
          break;
        case RenameTableColumn:
          plan = new RenameTableColumnPlan();
          break;
        case PreDeleteTable:
          plan = new PreDeleteTablePlan();
          break;
        case CommitDeleteTable:
          plan = new CommitDeleteTablePlan();
          break;
        case PreDeleteColumn:
          plan = new PreDeleteColumnPlan();
          break;
        case CommitDeleteColumn:
          plan = new CommitDeleteColumnPlan();
          break;
        case CreatePipeSinkV1:
          plan = new CreatePipeSinkPlanV1();
          break;
        case DropPipeSinkV1:
          plan = new DropPipeSinkPlanV1();
          break;
        case GetPipeSinkV1:
          plan = new GetPipeSinkPlanV1();
          break;
        case PreCreatePipeV1:
          plan = new PreCreatePipePlanV1();
          break;
        case SetPipeStatusV1:
          plan = new SetPipeStatusPlanV1();
          break;
        case DropPipeV1:
          plan = new DropPipePlanV1();
          break;
        case RecordPipeMessageV1:
          plan = new RecordPipeMessagePlan();
          break;
        case CreatePipeV2:
          plan = new CreatePipePlanV2();
          break;
        case SetPipeStatusV2:
          plan = new SetPipeStatusPlanV2();
          break;
        case DropPipeV2:
          plan = new DropPipePlanV2();
          break;
        case AlterPipeV2:
          plan = new AlterPipePlanV2();
          break;
        case OperateMultiplePipesV2:
          plan = new OperateMultiplePipesPlanV2();
          break;
        case PipeHandleLeaderChange:
          plan = new PipeHandleLeaderChangePlan();
          break;
        case PipeHandleMetaChange:
          plan = new PipeHandleMetaChangePlan();
          break;
        case PipeEnriched:
          plan = new PipeEnrichedPlan();
          break;
        case CreateTopic:
          plan = new CreateTopicPlan();
          break;
        case DropTopic:
          plan = new DropTopicPlan();
          break;
        case ShowTopic:
          plan = new ShowTopicPlan();
          break;
        case AlterTopic:
          plan = new AlterTopicPlan();
          break;
        case AlterMultipleTopics:
          plan = new AlterMultipleTopicsPlan();
          break;
        case TopicHandleMetaChange:
          plan = new TopicHandleMetaChangePlan();
          break;
        case AlterConsumerGroup:
          plan = new AlterConsumerGroupPlan();
          break;
        case ConsumerGroupHandleMetaChange:
          plan = new ConsumerGroupHandleMetaChangePlan();
          break;
        case PipeUnsetTemplate:
          plan = new PipeUnsetSchemaTemplatePlan();
          break;
        case PipeDeleteTimeSeries:
          plan = new PipeDeleteTimeSeriesPlan();
          break;
        case PipeDeleteLogicalView:
          plan = new PipeDeleteLogicalViewPlan();
          break;
        case PipeDeactivateTemplate:
          plan = new PipeDeactivateTemplatePlan();
          break;
        case UpdateTriggersOnTransferNodes:
          plan = new UpdateTriggersOnTransferNodesPlan();
          break;
        case UpdateTriggerLocation:
          plan = new UpdateTriggerLocationPlan();
          break;
        case ACTIVE_CQ:
          plan = new ActiveCQPlan();
          break;
        case ADD_CQ:
          plan = new AddCQPlan();
          break;
        case DROP_CQ:
          plan = new DropCQPlan();
          break;
        case UPDATE_CQ_LAST_EXEC_TIME:
          plan = new UpdateCQLastExecTimePlan();
          break;
        case CreateModel:
          plan = new CreateModelPlan();
          break;
        case UpdateModelInfo:
          plan = new UpdateModelInfoPlan();
          break;
        case DropModel:
          plan = new DropModelPlan();
          break;
        case ShowModel:
          plan = new ShowModelPlan();
          break;
        case DropModelInNode:
          plan = new DropModelInNodePlan();
          break;
        case GetModelInfo:
          plan = new GetModelInfoPlan();
          break;
        case CreatePipePlugin:
          plan = new CreatePipePluginPlan();
          break;
        case DropPipePlugin:
          plan = new DropPipePluginPlan();
          break;
        case setSpaceQuota:
          plan = new SetSpaceQuotaPlan();
          break;
        case setThrottleQuota:
          plan = new SetThrottleQuotaPlan();
          break;
        default:
          throw new IOException("unknown PhysicalPlan configPhysicalPlanType: " + planType);
      }
      plan.deserializeImpl(buffer);
      return plan;
    }

    private Factory() {
      // empty constructor
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfigPhysicalPlan that = (ConfigPhysicalPlan) o;
    return type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }
}
