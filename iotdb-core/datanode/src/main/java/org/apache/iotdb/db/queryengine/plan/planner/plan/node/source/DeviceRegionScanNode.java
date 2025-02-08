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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.source;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DeviceRegionScanNode extends RegionScanNode {
  private Map<PartialPath, DeviceContext> devicePathToContextMap;

  public DeviceRegionScanNode(
      PlanNodeId planNodeId,
      Map<PartialPath, DeviceContext> devicePathToContextMap,
      boolean outputCount,
      TRegionReplicaSet regionReplicaSet) {
    super(planNodeId);
    this.devicePathToContextMap = devicePathToContextMap;
    this.regionReplicaSet = regionReplicaSet;
    this.outputCount = outputCount;
  }

  public Map<PartialPath, DeviceContext> getDevicePathToContextMap() {
    return devicePathToContextMap;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("DeviceRegionScanNode has no children");
  }

  @Override
  public PlanNode clone() {
    return new DeviceRegionScanNode(
        getPlanNodeId(), getDevicePathToContextMap(), isOutputCount(), getRegionReplicaSet());
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputCount
        ? ColumnHeaderConstant.countDevicesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .collect(Collectors.toList())
        : ColumnHeaderConstant.showDevicesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeviceRegionScan(this, context);
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    int size = ReadWriteIOUtils.readInt(buffer);
    Map<PartialPath, DeviceContext> devicePathToContextMap = new HashMap<>();
    for (int i = 0; i < size; i++) {
      PartialPath path = (PartialPath) PathDeserializeUtil.deserialize(buffer);
      devicePathToContextMap.put(path, DeviceContext.deserialize(buffer));
    }
    boolean outputCount = ReadWriteIOUtils.readBool(buffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new DeviceRegionScanNode(planNodeId, devicePathToContextMap, outputCount, null);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DEVICE_REGION_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(devicePathToContextMap.size(), byteBuffer);
    for (Map.Entry<PartialPath, DeviceContext> entry : devicePathToContextMap.entrySet()) {
      entry.getKey().serialize(byteBuffer);
      entry.getValue().serializeAttributes(byteBuffer);
    }
    ReadWriteIOUtils.write(outputCount, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DEVICE_REGION_SCAN.serialize(stream);
    ReadWriteIOUtils.write(devicePathToContextMap.size(), stream);
    for (Map.Entry<PartialPath, DeviceContext> entry : devicePathToContextMap.entrySet()) {
      entry.getKey().serialize(stream);
      entry.getValue().serializeAttributes(stream);
    }
    ReadWriteIOUtils.write(outputCount, stream);
  }

  @Override
  public String toString() {
    return String.format(
        "DeviceRegionScanNode-%s:[DataRegion: %s OutputCount: %s]",
        this.getPlanNodeId(),
        PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()),
        outputCount);
  }

  @Override
  public Set<PartialPath> getDevicePaths() {
    return new HashSet<>(devicePathToContextMap.keySet());
  }

  @Override
  public void addDevicePath(PartialPath devicePath, RegionScanNode node) {
    this.devicePathToContextMap.put(
        devicePath, ((DeviceRegionScanNode) node).devicePathToContextMap.get(devicePath));
  }

  @Override
  public void clearPath() {
    this.devicePathToContextMap = new HashMap<>();
  }

  @Override
  public long getSize() {
    return devicePathToContextMap.size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    DeviceRegionScanNode that = (DeviceRegionScanNode) o;
    return devicePathToContextMap.equals(that.devicePathToContextMap)
        && outputCount == that.isOutputCount();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), devicePathToContextMap, outputCount);
  }
}
