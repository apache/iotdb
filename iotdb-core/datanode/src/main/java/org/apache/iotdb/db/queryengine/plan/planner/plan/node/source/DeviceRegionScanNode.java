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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.common.TimeseriesContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DeviceRegionScanNode extends RegionScanNode {
  private Map<PartialPath, DeviceContext> devicePathToContextMap;
  private Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>> deviceToTimeseriesSchemaInfo;
  private boolean hasInvalidSeries;
  private boolean hasAliasSeries;

  public DeviceRegionScanNode(
      PlanNodeId planNodeId,
      Map<PartialPath, DeviceContext> devicePathToContextMap,
      boolean outputCount,
      TRegionReplicaSet regionReplicaSet) {
    super(planNodeId);
    this.devicePathToContextMap = devicePathToContextMap;
    this.regionReplicaSet = regionReplicaSet;
    this.outputCount = outputCount;
    this.deviceToTimeseriesSchemaInfo = new HashMap<>();
    this.hasInvalidSeries = false;
    this.hasAliasSeries = false;
  }

  public DeviceRegionScanNode(
      PlanNodeId planNodeId,
      Map<PartialPath, DeviceContext> devicePathToContextMap,
      boolean outputCount,
      TRegionReplicaSet regionReplicaSet,
      Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>> deviceToTimeseriesSchemaInfo,
      boolean hasInvalidSeries,
      boolean hasAliasSeries) {
    super(planNodeId);
    this.devicePathToContextMap = new HashMap<>(devicePathToContextMap);
    this.regionReplicaSet = regionReplicaSet;
    this.outputCount = outputCount;
    this.deviceToTimeseriesSchemaInfo = new HashMap<>(deviceToTimeseriesSchemaInfo);
    this.hasInvalidSeries = hasInvalidSeries;
    this.hasAliasSeries = hasAliasSeries;
  }

  public Map<PartialPath, DeviceContext> getDevicePathToContextMap() {
    return devicePathToContextMap;
  }

  public Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>>
      getDeviceToTimeseriesSchemaInfo() {
    return deviceToTimeseriesSchemaInfo;
  }

  public void setDeviceToTimeseriesSchemaInfo(
      Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>> deviceToTimeseriesSchemaInfo) {
    this.deviceToTimeseriesSchemaInfo = deviceToTimeseriesSchemaInfo;
  }

  public boolean hasInvalidSeries() {
    return hasInvalidSeries;
  }

  public void setHasInvalidSeries(boolean hasInvalidSeries) {
    this.hasInvalidSeries = hasInvalidSeries;
  }

  public boolean hasAliasSeries() {
    return hasAliasSeries;
  }

  public void setHasAliasSeries(boolean hasAliasSeries) {
    this.hasAliasSeries = hasAliasSeries;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException(
        DataNodeQueryMessages.DEVICEREGIONSCANNODE_HAS_NO_CHILDREN);
  }

  @Override
  public PlanNode clone() {
    return new DeviceRegionScanNode(
        getPlanNodeId(),
        getDevicePathToContextMap(),
        isOutputCount(),
        getRegionReplicaSet(),
        getDeviceToTimeseriesSchemaInfo(),
        hasInvalidSeries(),
        hasAliasSeries());
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
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitDeviceRegionScan(this, context);
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
    // Deserialize hasInvalidSeries and hasAliasSeries (optional, may not exist for backward
    // compatibility)
    boolean hasInvalidSeries = false;
    boolean hasAliasSeries = false;
    if (buffer.hasRemaining()) {
      try {
        hasInvalidSeries = ReadWriteIOUtils.readBool(buffer);
        if (buffer.hasRemaining()) {
          hasAliasSeries = ReadWriteIOUtils.readBool(buffer);
        }
      } catch (Exception e) {
        // For backward compatibility, if deserialization fails, set to false
        hasInvalidSeries = false;
        hasAliasSeries = false;
      }
    }
    // Deserialize deviceToTimeseriesSchemaInfo (optional, may be null for backward compatibility)
    Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>> deviceToTimeseriesSchemaInfo = null;
    if (buffer.hasRemaining()) {
      try {
        int timeseriesSize = ReadWriteIOUtils.readInt(buffer);
        if (timeseriesSize >= 0) {
          deviceToTimeseriesSchemaInfo = new HashMap<>();
          for (int i = 0; i < timeseriesSize; i++) {
            PartialPath devicePath = (PartialPath) PathDeserializeUtil.deserialize(buffer);

            int pathSize = ReadWriteIOUtils.readInt(buffer);
            Map<PartialPath, List<TimeseriesContext>> measurementToSchemaInfo = new HashMap<>();
            for (int j = 0; j < pathSize; j++) {
              PartialPath path = (PartialPath) PathDeserializeUtil.deserialize(buffer);
              int schemaSize = ReadWriteIOUtils.readInt(buffer);
              List<TimeseriesContext> schemaInfos = new ArrayList<>();
              for (int k = 0; k < schemaSize; k++) {
                schemaInfos.add(TimeseriesContext.deserialize(buffer));
              }
              measurementToSchemaInfo.put(path, schemaInfos);
            }
            deviceToTimeseriesSchemaInfo.put(devicePath, measurementToSchemaInfo);
          }
        }
      } catch (Exception e) {
        // For backward compatibility, if deserialization fails, set to null
        deviceToTimeseriesSchemaInfo = null;
      }
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new DeviceRegionScanNode(
        planNodeId,
        devicePathToContextMap,
        outputCount,
        null,
        deviceToTimeseriesSchemaInfo,
        hasInvalidSeries,
        hasAliasSeries);
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
    ReadWriteIOUtils.write(hasInvalidSeries, byteBuffer);
    ReadWriteIOUtils.write(hasAliasSeries, byteBuffer);
    // Serialize deviceToTimeseriesSchemaInfo (may be null)
    if (deviceToTimeseriesSchemaInfo != null) {
      ReadWriteIOUtils.write(deviceToTimeseriesSchemaInfo.size(), byteBuffer);
      for (Map.Entry<PartialPath, Map<PartialPath, List<TimeseriesContext>>> entry :
          deviceToTimeseriesSchemaInfo.entrySet()) {
        entry.getKey().serialize(byteBuffer);

        ReadWriteIOUtils.write(entry.getValue().size(), byteBuffer);
        for (Map.Entry<PartialPath, List<TimeseriesContext>> timseriesEntry :
            entry.getValue().entrySet()) {
          timseriesEntry.getKey().serialize(byteBuffer);
          ReadWriteIOUtils.write(timseriesEntry.getValue().size(), byteBuffer);
          for (TimeseriesContext timeseriesContext : timseriesEntry.getValue()) {
            timeseriesContext.serializeAttributes(byteBuffer);
          }
        }
      }
    } else {
      ReadWriteIOUtils.write(-1, byteBuffer);
    }
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
    ReadWriteIOUtils.write(hasInvalidSeries, stream);
    ReadWriteIOUtils.write(hasAliasSeries, stream);
    // Serialize deviceToTimeseriesSchemaInfo (may be null)
    if (deviceToTimeseriesSchemaInfo != null) {
      ReadWriteIOUtils.write(deviceToTimeseriesSchemaInfo.size(), stream);
      for (Map.Entry<PartialPath, Map<PartialPath, List<TimeseriesContext>>> entry :
          deviceToTimeseriesSchemaInfo.entrySet()) {
        entry.getKey().serialize(stream);

        ReadWriteIOUtils.write(entry.getValue().size(), stream);
        for (Map.Entry<PartialPath, List<TimeseriesContext>> timseriesEntry :
            entry.getValue().entrySet()) {
          timseriesEntry.getKey().serialize(stream);
          ReadWriteIOUtils.write(timseriesEntry.getValue().size(), stream);
          for (TimeseriesContext timeseriesContext : timseriesEntry.getValue()) {
            timeseriesContext.serializeAttributes(stream);
          }
        }
      }
    } else {
      ReadWriteIOUtils.write(-1, stream);
    }
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
    return hasAliasSeries || hasInvalidSeries
        ? new HashSet<>(deviceToTimeseriesSchemaInfo.keySet())
        : new HashSet<>(devicePathToContextMap.keySet());
  }

  @Override
  public void addDevicePath(PartialPath devicePath, RegionScanNode node) {
    if (((DeviceRegionScanNode) node).deviceToTimeseriesSchemaInfo.get(devicePath) != null) {
      this.deviceToTimeseriesSchemaInfo.put(
          devicePath, ((DeviceRegionScanNode) node).deviceToTimeseriesSchemaInfo.get(devicePath));
    }

    this.devicePathToContextMap.put(
        devicePath, ((DeviceRegionScanNode) node).devicePathToContextMap.get(devicePath));
  }

  @Override
  public void clearPath() {
    this.devicePathToContextMap = new HashMap<>();
    this.deviceToTimeseriesSchemaInfo = new HashMap<>();
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
        && outputCount == that.isOutputCount()
        && hasInvalidSeries == that.hasInvalidSeries
        && hasAliasSeries == that.hasAliasSeries
        && Objects.equals(deviceToTimeseriesSchemaInfo, that.deviceToTimeseriesSchemaInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        devicePathToContextMap,
        outputCount,
        hasInvalidSeries,
        hasAliasSeries,
        deviceToTimeseriesSchemaInfo);
  }
}
