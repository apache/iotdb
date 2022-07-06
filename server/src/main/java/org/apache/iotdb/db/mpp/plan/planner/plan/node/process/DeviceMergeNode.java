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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DeviceMergeNode extends MultiChildNode {

  // The result output order, which could sort by device and time.
  // The size of this list is 2 and the first OrderBy in this list has higher priority.
  private final List<OrderBy> mergeOrders;

  // the list of selected devices
  private final List<String> devices;

  public DeviceMergeNode(
      PlanNodeId id, List<PlanNode> children, List<OrderBy> mergeOrders, List<String> devices) {
    super(id);
    this.children = children;
    this.mergeOrders = mergeOrders;
    this.devices = devices;
  }

  public DeviceMergeNode(PlanNodeId id, List<OrderBy> mergeOrders, List<String> devices) {
    super(id);
    this.children = new ArrayList<>();
    this.mergeOrders = mergeOrders;
    this.devices = devices;
  }

  public List<OrderBy> getMergeOrders() {
    return mergeOrders;
  }

  public List<String> getDevices() {
    return devices;
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    this.children.add(child);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public PlanNode clone() {
    return new DeviceMergeNode(getPlanNodeId(), getMergeOrders(), getDevices());
  }

  @Override
  public List<String> getOutputColumnNames() {
    return children.stream()
        .map(PlanNode::getOutputColumnNames)
        .flatMap(List::stream)
        .distinct()
        .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeviceMerge(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DEVICE_MERGE.serialize(byteBuffer);
    ReadWriteIOUtils.write(mergeOrders.get(0).ordinal(), byteBuffer);
    ReadWriteIOUtils.write(mergeOrders.get(1).ordinal(), byteBuffer);
    ReadWriteIOUtils.write(devices.size(), byteBuffer);
    for (String deviceName : devices) {
      ReadWriteIOUtils.write(deviceName, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DEVICE_MERGE.serialize(stream);
    ReadWriteIOUtils.write(mergeOrders.get(0).ordinal(), stream);
    ReadWriteIOUtils.write(mergeOrders.get(1).ordinal(), stream);
    ReadWriteIOUtils.write(devices.size(), stream);
    for (String deviceName : devices) {
      ReadWriteIOUtils.write(deviceName, stream);
    }
  }

  public static DeviceMergeNode deserialize(ByteBuffer byteBuffer) {
    List<OrderBy> mergeOrders = new ArrayList<>();
    mergeOrders.add(OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)]);
    mergeOrders.add(OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)]);
    int devicesSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> devices = new ArrayList<>();
    while (devicesSize > 0) {
      devices.add(ReadWriteIOUtils.readString(byteBuffer));
      devicesSize--;
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DeviceMergeNode(planNodeId, mergeOrders, devices);
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
    DeviceMergeNode that = (DeviceMergeNode) o;
    return Objects.equals(mergeOrders, that.mergeOrders)
        && Objects.equals(devices, that.devices)
        && Objects.equals(children, that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), mergeOrders, devices, children);
  }

  @Override
  public String toString() {
    return "DeviceMerge-" + this.getPlanNodeId();
  }
}
