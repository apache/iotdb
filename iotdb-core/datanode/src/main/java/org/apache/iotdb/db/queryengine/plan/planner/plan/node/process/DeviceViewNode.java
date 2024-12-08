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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process;

import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * DeviceViewNode is responsible for constructing a device-based view of a set of series. And output
 * the result with specific order. The order could be 'order by device' or 'order by timestamp'
 *
 * <p>Each output from its children should have the same schema. That means, the columns should be
 * same between these TsBlocks. If the input TsBlock contains n columns, the device-based view will
 * contain n+1 columns where the new column is Device column.
 */
public class DeviceViewNode extends MultiChildProcessNode {

  // The result output order, which could sort by device and time.
  // The size of this list is 2 and the first SortItem in this list has higher priority.
  private final OrderByParameter mergeOrderParameter;

  // The size devices and children should be the same.
  private final List<IDeviceID> devices = new ArrayList<>();

  // Device column and measurement columns in result output
  private List<String> outputColumnNames;

  // e.g. [s1,s2,s3] is query, but [s1, s3] exists in device1, then device1 -> [1, 3], s1 is 1 but
  // not 0 because device is the first column
  private final Map<IDeviceID, List<Integer>> deviceToMeasurementIndexesMap;

  public DeviceViewNode(
      PlanNodeId id,
      OrderByParameter mergeOrderParameter,
      List<String> outputColumnNames,
      Map<IDeviceID, List<Integer>> deviceToMeasurementIndexesMap) {
    super(id);
    this.mergeOrderParameter = mergeOrderParameter;
    this.outputColumnNames = outputColumnNames;
    this.deviceToMeasurementIndexesMap = deviceToMeasurementIndexesMap;
  }

  public DeviceViewNode(
      PlanNodeId id,
      OrderByParameter mergeOrderParameter,
      List<String> outputColumnNames,
      List<IDeviceID> devices,
      Map<IDeviceID, List<Integer>> deviceToMeasurementIndexesMap) {
    super(id);
    this.mergeOrderParameter = mergeOrderParameter;
    this.outputColumnNames = outputColumnNames;
    this.devices.addAll(devices);
    this.deviceToMeasurementIndexesMap = deviceToMeasurementIndexesMap;
  }

  public void addChildDeviceNode(IDeviceID deviceName, PlanNode childNode) {
    this.devices.add(deviceName);
    this.children.add(childNode);
  }

  public List<IDeviceID> getDevices() {
    return devices;
  }

  public Map<IDeviceID, List<Integer>> getDeviceToMeasurementIndexesMap() {
    return deviceToMeasurementIndexesMap;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.DEVICE_VIEW;
  }

  @Override
  public PlanNode clone() {
    return new DeviceViewNode(
        getPlanNodeId(),
        mergeOrderParameter,
        outputColumnNames,
        devices,
        deviceToMeasurementIndexesMap);
  }

  public OrderByParameter getMergeOrderParameter() {
    return mergeOrderParameter;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(List<String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeviceView(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DEVICE_VIEW.serialize(byteBuffer);
    mergeOrderParameter.serializeAttributes(byteBuffer);
    ReadWriteIOUtils.write(outputColumnNames.size(), byteBuffer);
    for (String column : outputColumnNames) {
      ReadWriteIOUtils.write(column, byteBuffer);
    }
    ReadWriteIOUtils.write(devices.size(), byteBuffer);
    for (IDeviceID deviceName : devices) {
      deviceName.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(deviceToMeasurementIndexesMap.size(), byteBuffer);
    for (Map.Entry<IDeviceID, List<Integer>> entry : deviceToMeasurementIndexesMap.entrySet()) {
      entry.getKey().serialize(byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().size(), byteBuffer);
      for (Integer index : entry.getValue()) {
        ReadWriteIOUtils.write(index, byteBuffer);
      }
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DEVICE_VIEW.serialize(stream);
    mergeOrderParameter.serializeAttributes(stream);
    ReadWriteIOUtils.write(outputColumnNames.size(), stream);
    for (String column : outputColumnNames) {
      ReadWriteIOUtils.write(column, stream);
    }
    ReadWriteIOUtils.write(devices.size(), stream);
    for (IDeviceID deviceName : devices) {
      deviceName.serialize(stream);
    }
    ReadWriteIOUtils.write(deviceToMeasurementIndexesMap.size(), stream);
    for (Map.Entry<IDeviceID, List<Integer>> entry : deviceToMeasurementIndexesMap.entrySet()) {
      entry.getKey().serialize(stream);
      ReadWriteIOUtils.write(entry.getValue().size(), stream);
      for (Integer index : entry.getValue()) {
        ReadWriteIOUtils.write(index, stream);
      }
    }
  }

  public static DeviceViewNode deserialize(ByteBuffer byteBuffer) {
    OrderByParameter mergeOrderParameter = OrderByParameter.deserialize(byteBuffer);

    int columnSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> outputColumnNames = new ArrayList<>();
    while (columnSize > 0) {
      outputColumnNames.add(ReadWriteIOUtils.readString(byteBuffer));
      columnSize--;
    }

    int devicesSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<IDeviceID> devices = new ArrayList<>();
    while (devicesSize > 0) {
      devices.add(IDeviceID.Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(byteBuffer));
      devicesSize--;
    }

    int mapSize = ReadWriteIOUtils.readInt(byteBuffer);
    Map<IDeviceID, List<Integer>> deviceToMeasurementIndexesMap = new HashMap<>(mapSize);
    while (mapSize > 0) {
      IDeviceID deviceName =
          IDeviceID.Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(byteBuffer);
      int listSize = ReadWriteIOUtils.readInt(byteBuffer);
      List<Integer> indexes = new ArrayList<>(listSize);
      while (listSize > 0) {
        indexes.add(ReadWriteIOUtils.readInt(byteBuffer));
        listSize--;
      }
      deviceToMeasurementIndexesMap.put(deviceName, indexes);
      mapSize--;
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DeviceViewNode(
        planNodeId, mergeOrderParameter, outputColumnNames, devices, deviceToMeasurementIndexesMap);
  }

  @Override
  public void serializeUseTemplate(DataOutputStream stream, TypeProvider typeProvider)
      throws IOException {
    PlanNodeType.DEVICE_VIEW.serialize(stream);
    id.serialize(stream);
    mergeOrderParameter.serializeAttributes(stream);
    ReadWriteIOUtils.write(devices.size(), stream);
    for (IDeviceID deviceName : devices) {
      deviceName.serialize(stream);
    }

    ReadWriteIOUtils.write(getChildren().size(), stream);
    for (PlanNode planNode : getChildren()) {
      planNode.serializeUseTemplate(stream, typeProvider);
    }
  }

  public static DeviceViewNode deserializeUseTemplate(
      ByteBuffer byteBuffer, TypeProvider typeProvider) {
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    OrderByParameter mergeOrderParameter = OrderByParameter.deserialize(byteBuffer);

    int devicesSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<IDeviceID> devices = new ArrayList<>(devicesSize);
    while (devicesSize > 0) {
      devices.add(IDeviceID.Deserializer.DEFAULT_DESERIALIZER.deserializeFrom(byteBuffer));
      devicesSize--;
    }

    return new DeviceViewNode(
        planNodeId,
        mergeOrderParameter,
        typeProvider.getTemplatedInfo().getDeviceViewOutputNames(),
        devices,
        null);
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
    return mergeOrderParameter.equals(that.mergeOrderParameter)
        && devices.equals(that.devices)
        && outputColumnNames.equals(that.outputColumnNames)
        && deviceToMeasurementIndexesMap.equals(that.deviceToMeasurementIndexesMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        mergeOrderParameter,
        devices,
        outputColumnNames,
        deviceToMeasurementIndexesMap);
  }

  @Override
  public String toString() {
    return "DeviceView-" + this.getPlanNodeId();
  }
}
