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

import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetConfigNodeConfigurationReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetNodePathsPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.AdjustMaxRegionGroupCountReq;
import org.apache.iotdb.confignode.consensus.request.write.ApplyConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateFunctionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteProcedureReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.DropFunctionReq;
import org.apache.iotdb.confignode.consensus.request.write.PreDeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.consensus.request.write.UpdateProcedureReq;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class ConfigRequest implements IConsensusRequest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigRequest.class);

  private final ConfigRequestType type;

  public ConfigRequest(ConfigRequestType type) {
    this.type = type;
  }

  public ConfigRequestType getType() {
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

    public static ConfigRequest create(ByteBuffer buffer) throws IOException {
      int typeNum = buffer.getInt();
      if (typeNum >= ConfigRequestType.values().length) {
        throw new IOException("unrecognized log type " + typeNum);
      }
      ConfigRequestType type = ConfigRequestType.values()[typeNum];
      ConfigRequest req;
      switch (type) {
        case RegisterDataNode:
          req = new RegisterDataNodeReq();
          break;
        case GetDataNodeInfo:
          req = new GetDataNodeInfoReq();
          break;
        case SetStorageGroup:
          req = new SetStorageGroupReq();
          break;
        case SetTTL:
          req = new SetTTLReq();
          break;
        case SetSchemaReplicationFactor:
          req = new SetSchemaReplicationFactorReq();
          break;
        case SetDataReplicationFactor:
          req = new SetDataReplicationFactorReq();
          break;
        case SetTimePartitionInterval:
          req = new SetTimePartitionIntervalReq();
          break;
        case AdjustMaxRegionGroupCount:
          req = new AdjustMaxRegionGroupCountReq();
          break;
        case CountStorageGroup:
          req = new CountStorageGroupReq();
          break;
        case GetStorageGroup:
          req = new GetStorageGroupReq();
          break;
        case CreateRegionGroups:
          req = new CreateRegionsReq();
          break;
        case DeleteRegions:
          req = new DeleteRegionsReq();
          break;
        case GetSchemaPartition:
          req = new GetSchemaPartitionReq();
          break;
        case CreateSchemaPartition:
          req = new CreateSchemaPartitionReq();
          break;
        case GetOrCreateSchemaPartition:
          req = new GetOrCreateSchemaPartitionReq();
          break;
        case GetDataPartition:
          req = new GetDataPartitionReq();
          break;
        case CreateDataPartition:
          req = new CreateDataPartitionReq();
          break;
        case GetOrCreateDataPartition:
          req = new GetOrCreateDataPartitionReq();
          break;
        case DeleteProcedure:
          req = new DeleteProcedureReq();
          break;
        case UpdateProcedure:
          req = new UpdateProcedureReq();
          break;
        case PreDeleteStorageGroup:
          req = new PreDeleteStorageGroupReq();
          break;
        case DeleteStorageGroup:
          req = new DeleteStorageGroupReq();
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
          req = new AuthorReq(type);
          break;
        case GetConfigNodeConfiguration:
          req = new GetConfigNodeConfigurationReq();
          break;
        case ApplyConfigNode:
          req = new ApplyConfigNodeReq();
          break;
        case RemoveConfigNode:
          req = new RemoveConfigNodeReq();
          break;
        case CreateFunction:
          req = new CreateFunctionReq();
          break;
        case DropFunction:
          req = new DropFunctionReq();
          break;
        case GetNodePathsPartition:
          req = new GetNodePathsPartitionReq();
          break;
        case GetRegionInfoList:
          req = new GetRegionInfoListReq();
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
