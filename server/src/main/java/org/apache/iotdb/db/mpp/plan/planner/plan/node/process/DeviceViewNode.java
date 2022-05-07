/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * DeviceViewNode is responsible for constructing a device-based view of a set of series. And output
 * the result with specific order. The order could be 'order by device' or 'order by timestamp'
 *
 * <p>Each output from its children should have the same schema. That means, the columns should be
 * same between these TsBlocks. If the input TsBlock contains n columns, the device-based view will
 * contain n+1 columns where the new column is Device column.
 */
public class DeviceViewNode extends ProcessNode {

  // The result output order, which could sort by device and time.
  // The size of this list is 2 and the first OrderBy in this list has higher priority.
  private final List<OrderBy> mergeOrders;

  // The size devices and children should be the same.
  private final List<String> devices = new ArrayList<>();

  // each child node whose output TsBlock contains the data belonged to one device.
  private final List<PlanNode> children = new ArrayList<>();

  // measurement columns in result output
  private final List<String> measurements;

  public DeviceViewNode(PlanNodeId id, List<OrderBy> mergeOrders, List<String> measurements) {
    super(id);
    this.mergeOrders = mergeOrders;
    this.measurements = measurements;
  }

  public DeviceViewNode(
      PlanNodeId id, List<OrderBy> mergeOrders, List<String> measurements, List<String> devices) {
    super(id);
    this.mergeOrders = mergeOrders;
    this.measurements = measurements;
    this.devices.addAll(devices);
  }

  public void addChildDeviceNode(String deviceName, PlanNode childNode) {
    this.devices.add(deviceName);
    this.children.add(childNode);
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
    return new DeviceViewNode(getPlanNodeId(), mergeOrders, measurements, devices);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return measurements;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeviceView(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DEVICE_VIEW.serialize(byteBuffer);
    ReadWriteIOUtils.write(mergeOrders.get(0).ordinal(), byteBuffer);
    ReadWriteIOUtils.write(mergeOrders.get(1).ordinal(), byteBuffer);
    ReadWriteIOUtils.write(measurements.size(), byteBuffer);
    for (String measurement : measurements) {
      ReadWriteIOUtils.write(measurement, byteBuffer);
    }
    ReadWriteIOUtils.write(devices.size(), byteBuffer);
    for (String deviceName : devices) {
      ReadWriteIOUtils.write(deviceName, byteBuffer);
    }
  }

  public static DeviceViewNode deserialize(ByteBuffer byteBuffer) {
    List<OrderBy> mergeOrders = new ArrayList<>();
    mergeOrders.add(OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)]);
    mergeOrders.add(OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)]);
    int measurementsSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> measurements = new ArrayList<>();
    while (measurementsSize > 0) {
      measurements.add(ReadWriteIOUtils.readString(byteBuffer));
      measurementsSize--;
    }
    int devicesSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> devices = new ArrayList<>();
    while (devicesSize > 0) {
      devices.add(ReadWriteIOUtils.readString(byteBuffer));
      devicesSize--;
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DeviceViewNode(planNodeId, mergeOrders, measurements, devices);
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
    DeviceViewNode that = (DeviceViewNode) o;
    return mergeOrders.equals(that.mergeOrders)
        && devices.equals(that.devices)
        && children.equals(that.children)
        && measurements.equals(that.measurements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), mergeOrders, devices, children, measurements);
  }
}
