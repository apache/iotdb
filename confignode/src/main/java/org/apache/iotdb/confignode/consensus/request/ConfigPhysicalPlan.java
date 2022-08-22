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

import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.AdjustMaxRegionGroupCountPlan;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.DropFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.UpdateProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ConfigPhysicalPlan implements IConsensusRequest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPhysicalPlan.class);

  private final ConfigPhysicalPlanType type;

  public ConfigPhysicalPlan(ConfigPhysicalPlanType type) {
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
      LOGGER.error("Unexpected error occurs when serializing this ConfigRequest.", e);
      throw new SerializationRunTimeException(e);
    }
  }

  protected abstract void serializeImpl(DataOutputStream stream) throws IOException;

  protected abstract void deserializeImpl(ByteBuffer buffer) throws IOException;

  public static class Factory {

    public static ConfigPhysicalPlan create(ByteBuffer buffer) throws IOException {
      int typeNum = buffer.getInt();
      if (typeNum >= ConfigPhysicalPlanType.values().length) {
        throw new IOException("unrecognized log type " + typeNum);
      }
      ConfigPhysicalPlanType type = ConfigPhysicalPlanType.values()[typeNum];
      ConfigPhysicalPlan req;
      switch (type) {
        case RegisterDataNode:
          req = new RegisterDataNodePlan();
          break;
        case RemoveDataNode:
          req = new RemoveDataNodePlan();
          break;
        case GetDataNodeConfiguration:
          req = new GetDataNodeConfigurationPlan();
          break;
        case SetStorageGroup:
          req = new SetStorageGroupPlan();
          break;
        case SetTTL:
          req = new SetTTLPlan();
          break;
        case SetSchemaReplicationFactor:
          req = new SetSchemaReplicationFactorPlan();
          break;
        case SetDataReplicationFactor:
          req = new SetDataReplicationFactorPlan();
          break;
        case SetTimePartitionInterval:
          req = new SetTimePartitionIntervalPlan();
          break;
        case AdjustMaxRegionGroupCount:
          req = new AdjustMaxRegionGroupCountPlan();
          break;
        case CountStorageGroup:
          req = new CountStorageGroupPlan();
          break;
        case GetStorageGroup:
          req = new GetStorageGroupPlan();
          break;
        case CreateRegionGroups:
          req = new CreateRegionGroupsPlan();
          break;
        case DeleteRegionGroups:
          req = new DeleteRegionGroupsPlan();
          break;
        case GetSchemaPartition:
          req = new GetSchemaPartitionPlan();
          break;
        case CreateSchemaPartition:
          req = new CreateSchemaPartitionPlan();
          break;
        case GetOrCreateSchemaPartition:
          req = new GetOrCreateSchemaPartitionPlan();
          break;
        case GetDataPartition:
          req = new GetDataPartitionPlan();
          break;
        case CreateDataPartition:
          req = new CreateDataPartitionPlan();
          break;
        case GetOrCreateDataPartition:
          req = new GetOrCreateDataPartitionPlan();
          break;
        case DeleteProcedure:
          req = new DeleteProcedurePlan();
          break;
        case UpdateProcedure:
          req = new UpdateProcedurePlan();
          break;
        case PreDeleteStorageGroup:
          req = new PreDeleteStorageGroupPlan();
          break;
        case DeleteStorageGroup:
          req = new DeleteStorageGroupPlan();
          break;
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
          req = new AuthorPlan(type);
          break;
        case ApplyConfigNode:
          req = new ApplyConfigNodePlan();
          break;
        case RemoveConfigNode:
          req = new RemoveConfigNodePlan();
          break;
        case CreateFunction:
          req = new CreateFunctionPlan();
          break;
        case DropFunction:
          req = new DropFunctionPlan();
          break;
        case CreateSchemaTemplate:
          req = new CreateSchemaTemplatePlan();
          break;
        case GetAllSchemaTemplate:
          req = new GetAllSchemaTemplatePlan();
          break;
        case GetSchemaTemplate:
          req = new GetSchemaTemplatePlan();
          break;
        case CheckTemplateSettable:
          req = new CheckTemplateSettablePlan();
          break;
        case GetPathsSetTemplate:
          req = new GetPathsSetTemplatePlan();
          break;
        case GetAllTemplateSetInfo:
          req = new GetAllTemplateSetInfoPlan();
          break;
        case SetSchemaTemplate:
          req = new SetSchemaTemplatePlan();
          break;
        case GetNodePathsPartition:
          req = new GetNodePathsPartitionPlan();
          break;
        case GetRegionInfoList:
          req = new GetRegionInfoListPlan();
          break;
        case UpdateRegionLocation:
          req = new UpdateRegionLocationPlan();
          break;
        default:
          throw new IOException("unknown PhysicalPlan type: " + typeNum);
      }
      req.deserializeImpl(buffer);
      return req;
    }

    private Factory() {
      // empty constructor
    }
  }
}
