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

package org.apache.iotdb.confignode.consensus.request.write.confignode;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class UpdateNodeStatusPlan extends ConfigPhysicalPlan {

  public enum Operation {
    SET_STOPPED,
    SET_REMOVING,
    CLEAR;

    /** Maps the final node status to a persistence command without applying transition rules. */
    public static Operation fromNodeStatus(NodeStatus status) {
      return switch (status) {
        case Stopped -> SET_STOPPED;
        case Removing -> SET_REMOVING;
        case Running, ReadOnly, Unknown -> CLEAR;
      };
    }
  }

  private int nodeId;

  private Operation operation;

  public UpdateNodeStatusPlan() {
    super(ConfigPhysicalPlanType.UpdateNodeStatus);
  }

  public UpdateNodeStatusPlan(int nodeId, Operation operation) {
    this();
    this.nodeId = nodeId;
    this.operation = operation;
  }

  public int getNodeId() {
    return nodeId;
  }

  public Operation getOperation() {
    return operation;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().getPlanType(), stream);
    ReadWriteIOUtils.write(nodeId, stream);
    ReadWriteIOUtils.write(operation.name(), stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    nodeId = ReadWriteIOUtils.readInt(buffer);
    operation = Operation.valueOf(ReadWriteIOUtils.readString(buffer));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UpdateNodeStatusPlan that = (UpdateNodeStatusPlan) o;
    return nodeId == that.nodeId && operation == that.operation;
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeId, operation);
  }
}
