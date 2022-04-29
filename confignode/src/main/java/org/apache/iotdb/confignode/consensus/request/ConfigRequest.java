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
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferOverflowException;
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
  public void serializeRequest(ByteBuffer buffer) {
    serialize(buffer);
  }

  public final void serialize(ByteBuffer buffer) {
    buffer.mark();
    try {
      serializeImpl(buffer);
    } catch (UnsupportedOperationException e) {
      // ignore and throw
      throw e;
    } catch (BufferOverflowException e) {
      buffer.reset();
      throw e;
    } catch (Exception e) {
      LOGGER.error(
          "Rollback buffer entry because error occurs when serializing this physical plan.", e);
      buffer.reset();
      throw e;
    }
  }

  protected abstract void serializeImpl(ByteBuffer buffer);

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
        case DeleteStorageGroup:
          req = new DeleteStorageGroupReq();
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
        case CountStorageGroup:
          req = new CountStorageGroupReq();
          break;
        case GetStorageGroup:
          req = new GetStorageGroupReq();
          break;
        case CreateRegions:
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
        case LIST_USER:
        case LIST_ROLE:
        case LIST_USER_PRIVILEGE:
        case LIST_ROLE_PRIVILEGE:
        case LIST_USER_ROLES:
        case LIST_ROLE_USERS:
        case CREATE_USER:
        case CREATE_ROLE:
        case DROP_USER:
        case DROP_ROLE:
        case GRANT_ROLE:
        case GRANT_USER:
        case GRANT_ROLE_TO_USER:
        case REVOKE_USER:
        case REVOKE_ROLE:
        case REVOKE_ROLE_FROM_USER:
        case UPDATE_USER:
          req = new AuthorReq(type);
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
