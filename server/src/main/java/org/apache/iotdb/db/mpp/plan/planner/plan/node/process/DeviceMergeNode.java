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
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DeviceMergeNode extends MultiChildProcessNode {

  // The result output order, which could sort by device and time.
  // The size of this list is 2 and the first SortItem in this list has higher priority.
  private final OrderByParameter mergeOrderParameter;

  // the list of selected devices
  private final List<String> devices;

  public DeviceMergeNode(
      PlanNodeId id, OrderByParameter mergeOrderParameter, List<String> devices) {
    super(id);
    this.mergeOrderParameter = mergeOrderParameter;
    this.devices = devices;
  }

  public OrderByParameter getMergeOrderParameter() {
    return mergeOrderParameter;
  }

  public List<String> getDevices() {
    return devices;
  }

  @Override
  public PlanNode clone() {
    return new DeviceMergeNode(getPlanNodeId(), getMergeOrderParameter(), getDevices());
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    throw new UnsupportedOperationException(
        "DeviceMergeNode should have only one local child in single data region.");
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
    mergeOrderParameter.serializeAttributes(byteBuffer);
    ReadWriteIOUtils.write(devices.size(), byteBuffer);
    for (String deviceName : devices) {
      ReadWriteIOUtils.write(deviceName, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DEVICE_MERGE.serialize(stream);
    mergeOrderParameter.serializeAttributes(stream);
    ReadWriteIOUtils.write(devices.size(), stream);
    for (String deviceName : devices) {
      ReadWriteIOUtils.write(deviceName, stream);
    }
  }

  public static DeviceMergeNode deserialize(ByteBuffer byteBuffer) {
    OrderByParameter mergeOrderParameter = OrderByParameter.deserialize(byteBuffer);
    int devicesSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> devices = new ArrayList<>();
    while (devicesSize > 0) {
      devices.add(ReadWriteIOUtils.readString(byteBuffer));
      devicesSize--;
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DeviceMergeNode(planNodeId, mergeOrderParameter, devices);
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
    return Objects.equals(mergeOrderParameter, that.mergeOrderParameter)
        && Objects.equals(devices, that.devices);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), mergeOrderParameter, devices);
  }

  @Override
  public String toString() {
    return "DeviceMerge-" + this.getPlanNodeId();
  }
}
