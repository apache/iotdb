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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.DeviceViewIntoPathDescriptor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DeviceViewIntoNode extends SingleChildProcessNode {

  private final DeviceViewIntoPathDescriptor deviceViewIntoPathDescriptor;

  public DeviceViewIntoNode(
      PlanNodeId id, DeviceViewIntoPathDescriptor deviceViewIntoPathDescriptor) {
    super(id);
    this.deviceViewIntoPathDescriptor = deviceViewIntoPathDescriptor;
  }

  public DeviceViewIntoNode(
      PlanNodeId id, PlanNode child, DeviceViewIntoPathDescriptor deviceViewIntoPathDescriptor) {
    super(id, child);
    this.deviceViewIntoPathDescriptor = deviceViewIntoPathDescriptor;
  }

  public DeviceViewIntoPathDescriptor getDeviceViewIntoPathDescriptor() {
    return deviceViewIntoPathDescriptor;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.DEVICE_VIEW_INTO;
  }

  @Override
  public PlanNode clone() {
    return new DeviceViewIntoNode(getPlanNodeId(), this.deviceViewIntoPathDescriptor);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ColumnHeaderConstant.selectIntoAlignByDeviceColumnHeaders.stream()
        .map(ColumnHeader::getColumnName)
        .collect(Collectors.toList());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DEVICE_VIEW_INTO.serialize(byteBuffer);
    this.deviceViewIntoPathDescriptor.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DEVICE_VIEW_INTO.serialize(stream);
    this.deviceViewIntoPathDescriptor.serialize(stream);
  }

  public static DeviceViewIntoNode deserialize(ByteBuffer byteBuffer) {
    DeviceViewIntoPathDescriptor deviceViewIntoPathDescriptor =
        DeviceViewIntoPathDescriptor.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DeviceViewIntoNode(planNodeId, deviceViewIntoPathDescriptor);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeviceViewInto(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DeviceViewIntoNode intoNode = (DeviceViewIntoNode) o;
    return deviceViewIntoPathDescriptor.equals(intoNode.deviceViewIntoPathDescriptor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), deviceViewIntoPathDescriptor);
  }

  @Override
  public String toString() {
    return "DeviceViewIntoNode-" + getPlanNodeId();
  }
}
