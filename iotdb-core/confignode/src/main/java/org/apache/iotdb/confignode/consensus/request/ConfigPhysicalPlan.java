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
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.datanode.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.function.GetFunctionTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.function.GetUDFJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.GetModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowModelPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowTrialPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.CountTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSeriesSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.pipe.task.ShowPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionIdPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTransferringTriggersPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerLocationPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateVersionInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.ActiveCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.AddCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.DropCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.ShowCQPlan;
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
import org.apache.iotdb.confignode.consensus.request.write.function.DropFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.DropModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelStatePlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.procedure.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetThrottleQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.PollRegionMaintainTaskPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.PollSpecificRegionMaintainTaskPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipePlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.GetPipeSinkPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.PreCreatePipePlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.RecordPipeMessagePlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.SetPipeStatusPlanV1;
import org.apache.iotdb.confignode.consensus.request.write.sync.ShowPipePlanV1;
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
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class ConfigPhysicalPlan implements IConsensusRequest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPhysicalPlan.class);

  private final ConfigPhysicalPlanType type;

  protected ConfigPhysicalPlan(ConfigPhysicalPlanType type) {
    this.type = type;
  }

  public ConfigPhysicalPlanType getType() {
    return this.type;
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      serializeImpl(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (IOException e) {
      throw new SerializationRunTimeException(e);
    }
  }

  protected abstract void serializeImpl(DataOutputStream stream) throws IOException;

  protected abstract void deserializeImpl(ByteBuffer buffer) throws IOException;

  public int getSerializedSize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serializeImpl(outputStream);
    return byteArrayOutputStream.size();
  }

  public static class Factory {

    public static ConfigPhysicalPlan create(ByteBuffer buffer) throws IOException {
      short planType = buffer.getShort();
      ConfigPhysicalPlanType configPhysicalPlanType =
          ConfigPhysicalPlanType.convertToConfigPhysicalPlanType(planType);
      if (configPhysicalPlanType == null) {
        throw new IOException("unrecognized log configPhysicalPlanType: " + planType);
      }

      ConfigPhysicalPlan plan;
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
        case GetDataNodeConfiguration:
          plan = new GetDataNodeConfigurationPlan();
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
        case CountDatabase:
          plan = new CountDatabasePlan();
          break;
        case GetDatabase:
          plan = new GetDatabasePlan();
          break;
        case CreateRegionGroups:
          plan = new CreateRegionGroupsPlan();
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
        case GetSchemaPartition:
          plan = new GetSchemaPartitionPlan();
          break;
        case CreateSchemaPartition:
          plan = new CreateSchemaPartitionPlan();
          break;
        case GetOrCreateSchemaPartition:
          plan = new GetOrCreateSchemaPartitionPlan();
          break;
        case GetDataPartition:
          plan = new GetDataPartitionPlan();
          break;
        case CreateDataPartition:
          plan = new CreateDataPartitionPlan();
          break;
        case GetOrCreateDataPartition:
          plan = new GetOrCreateDataPartitionPlan();
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
        case ListUserDep:
        case ListRoleDep:
        case ListUserPrivilegeDep:
        case ListRolePrivilegeDep:
        case ListUserRolesDep:
        case ListRoleUsersDep:
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
        case ListUser:
        case ListRole:
        case ListUserPrivilege:
        case ListRolePrivilege:
        case ListUserRoles:
        case ListRoleUsers:
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
        case CreateFunction:
          plan = new CreateFunctionPlan();
          break;
        case DropFunction:
          plan = new DropFunctionPlan();
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
        case GetTriggerTable:
          plan = new GetTriggerTablePlan();
          break;
        case GetTriggerLocation:
          plan = new GetTriggerLocationPlan();
          break;
        case GetTriggerJar:
          plan = new GetTriggerJarPlan();
          break;
        case CreateSchemaTemplate:
          plan = new CreateSchemaTemplatePlan();
          break;
        case GetAllSchemaTemplate:
          plan = new GetAllSchemaTemplatePlan();
          break;
        case GetSchemaTemplate:
          plan = new GetSchemaTemplatePlan();
          break;
        case CheckTemplateSettable:
          plan = new CheckTemplateSettablePlan();
          break;
        case GetPathsSetTemplate:
          plan = new GetPathsSetTemplatePlan();
          break;
        case GetAllTemplateSetInfo:
          plan = new GetAllTemplateSetInfoPlan();
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
        case GetTemplateSetInfo:
          plan = new GetTemplateSetInfoPlan();
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
        case GetNodePathsPartition:
          plan = new GetNodePathsPartitionPlan();
          break;
        case GetRegionInfoList:
          plan = new GetRegionInfoListPlan();
          break;
        case UpdateRegionLocation:
          plan = new UpdateRegionLocationPlan();
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
        case ShowPipeV1:
          plan = new ShowPipePlanV1();
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
        case ShowPipeV2:
          plan = new ShowPipePlanV2();
          break;
        case PipeHandleLeaderChange:
          plan = new PipeHandleLeaderChangePlan();
          break;
        case PipeHandleMetaChange:
          plan = new PipeHandleMetaChangePlan();
          break;
        case GetRegionId:
          plan = new GetRegionIdPlan();
          break;
        case GetTimeSlotList:
          plan = new GetTimeSlotListPlan();
          break;
        case CountTimeSlotList:
          plan = new CountTimeSlotListPlan();
          break;
        case GetSeriesSlotList:
          plan = new GetSeriesSlotListPlan();
          break;
        case UpdateTriggersOnTransferNodes:
          plan = new UpdateTriggersOnTransferNodesPlan();
          break;
        case UpdateTriggerLocation:
          plan = new UpdateTriggerLocationPlan();
          break;
        case GetTransferringTriggers:
          plan = new GetTransferringTriggersPlan();
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
        case SHOW_CQ:
          plan = new ShowCQPlan();
          break;
        case GetFunctionTable:
          plan = new GetFunctionTablePlan();
          break;
        case GetFunctionJar:
          plan = new GetUDFJarPlan();
          break;
        case CreateModel:
          plan = new CreateModelPlan();
          break;
        case UpdateModelInfo:
          plan = new UpdateModelInfoPlan();
          break;
        case UpdateModelState:
          plan = new UpdateModelStatePlan();
          break;
        case DropModel:
          plan = new DropModelPlan();
          break;
        case ShowModel:
          plan = new ShowModelPlan();
          break;
        case ShowTrial:
          plan = new ShowTrialPlan();
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
        case GetPipePluginTable:
          plan = new GetPipePluginTablePlan();
          break;
        case GetPipePluginJar:
          plan = new GetPipePluginJarPlan();
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
