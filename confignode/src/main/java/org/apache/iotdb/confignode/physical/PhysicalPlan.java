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

import org.apache.iotdb.confignode.physical.crud.DataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.SchemaPartitionPlan;
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
    buffer.flip();
  }

  protected abstract void serializeImpl(ByteBuffer buffer);

  protected abstract void deserializeImpl(ByteBuffer buffer);

  public static class Factory {

    public static PhysicalPlan create(ByteBuffer buffer) throws IOException {
      int typeNum = buffer.getInt();
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
        case QueryDataPartition:
          plan = new DataPartitionPlan(PhysicalPlanType.QueryDataPartition);
          break;
        case ApplyDataPartition:
          plan = new DataPartitionPlan(PhysicalPlanType.ApplyDataPartition);
          break;
        case QuerySchemaPartition:
          plan = new SchemaPartitionPlan(PhysicalPlanType.QuerySchemaPartition);
          break;
        case ApplySchemaPartition:
          plan = new SchemaPartitionPlan(PhysicalPlanType.ApplySchemaPartition);
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
