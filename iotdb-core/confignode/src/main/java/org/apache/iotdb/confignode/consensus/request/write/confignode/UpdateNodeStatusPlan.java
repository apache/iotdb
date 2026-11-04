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

  private int nodeId;

  // A null status removes the persisted record, including its reason.
  private NodeStatus status;

  private String statusReason;

  public UpdateNodeStatusPlan() {
    super(ConfigPhysicalPlanType.UpdateNodeStatus);
  }

  public UpdateNodeStatusPlan(int nodeId, NodeStatus status) {
    this(nodeId, status, null);
  }

  public UpdateNodeStatusPlan(int nodeId, NodeStatus status, String statusReason) {
    this();
    this.nodeId = nodeId;
    // Resolve clearing before serialization so replay does not depend on future persistence rules.
    this.status = status != null && status.isPersistentStatus() ? status : null;
    this.statusReason = this.status == null ? null : statusReason;
  }

  public int getNodeId() {
    return nodeId;
  }

  public NodeStatus getStatus() {
    return status;
  }

  public String getStatusReason() {
    return statusReason;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().getPlanType(), stream);
    ReadWriteIOUtils.write(nodeId, stream);
    ReadWriteIOUtils.write(status == null ? null : status.getStatus(), stream);
    ReadWriteIOUtils.write(statusReason, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    nodeId = ReadWriteIOUtils.readInt(buffer);
    String statusName = ReadWriteIOUtils.readString(buffer);
    status = statusName == null ? null : NodeStatus.parse(statusName);
    statusReason = ReadWriteIOUtils.readString(buffer);
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
    return nodeId == that.nodeId
        && status == that.status
        && Objects.equals(statusReason, that.statusReason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeId, status, statusReason);
  }
}
