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
package org.apache.iotdb.confignode.physical;

import org.apache.iotdb.confignode.physical.crud.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.CreateRegionsPlan;
import org.apache.iotdb.confignode.physical.crud.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.AuthorPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.QueryStorageGroupSchemaPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public abstract class PhysicalPlan implements IConsensusRequest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalPlan.class);

  private final PhysicalPlanType type;

  public PhysicalPlan(PhysicalPlanType type) {
    this.type = type;
  }

  public PhysicalPlanType getType() {
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

    public static PhysicalPlan create(ByteBuffer buffer) throws IOException {
      int typeNum = buffer.getInt();
      if (typeNum >= PhysicalPlanType.values().length) {
        throw new IOException("unrecognized log type " + typeNum);
      }
      PhysicalPlanType type = PhysicalPlanType.values()[typeNum];
      PhysicalPlan plan;
      switch (type) {
        case RegisterDataNode:
          plan = new RegisterDataNodePlan();
          break;
        case QueryDataNodeInfo:
          plan = new QueryDataNodeInfoPlan();
          break;
        case SetStorageGroup:
          plan = new SetStorageGroupPlan();
          break;
        case QueryStorageGroupSchema:
          plan = new QueryStorageGroupSchemaPlan();
          break;
        case CreateRegions:
          plan = new CreateRegionsPlan();
          break;
        case GetSchemaPartition:
          plan = new GetOrCreateSchemaPartitionPlan(PhysicalPlanType.GetSchemaPartition);
          break;
        case CreateSchemaPartition:
          plan = new CreateSchemaPartitionPlan();
          break;
        case GetOrCreateSchemaPartition:
          plan = new GetOrCreateSchemaPartitionPlan(PhysicalPlanType.GetOrCreateSchemaPartition);
          break;
        case GetDataPartition:
          plan = new GetOrCreateDataPartitionPlan(PhysicalPlanType.GetDataPartition);
          break;
        case CreateDataPartition:
          plan = new CreateDataPartitionPlan();
          break;
        case GetOrCreateDataPartition:
          plan = new GetOrCreateDataPartitionPlan(PhysicalPlanType.GetOrCreateDataPartition);
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
          plan = new AuthorPlan(type);
          break;
        default:
          throw new IOException("unknown PhysicalPlan type: " + typeNum);
      }
      plan.deserializeImpl(buffer);
      return plan;
    }

    private Factory() {
      // empty constructor
    }
  }
}
