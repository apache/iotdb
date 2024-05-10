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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.common.TimeseriesSchemaInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.file.metadata.PlainDeviceID;
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

public class TimeseriesRegionScanNode extends RegionScanNode {
  // DevicePath -> (MeasurementPath -> TimeseriesSchemaInfo), it will be serialized to
  // timeseriesToSchemaInfo in the end
  Map<PartialPath, Map<PartialPath, List<TimeseriesSchemaInfo>>> deviceToTimeseriesSchemaInfo;
  private Map<PartialPath, List<TimeseriesSchemaInfo>> timeseriesToSchemaInfo;

  public TimeseriesRegionScanNode(
      PlanNodeId planNodeId, boolean outputCount, TRegionReplicaSet regionReplicaSet) {
    super(planNodeId);
    this.regionReplicaSet = regionReplicaSet;
    this.outputCount = outputCount;
  }

  public TimeseriesRegionScanNode(
      PlanNodeId planNodeId,
      Map<PartialPath, List<TimeseriesSchemaInfo>> timeseriesToSchemaInfo,
      boolean outputCount,
      TRegionReplicaSet regionReplicaSet) {
    super(planNodeId);
    this.timeseriesToSchemaInfo = timeseriesToSchemaInfo;
    this.regionReplicaSet = regionReplicaSet;
    this.outputCount = outputCount;
  }

  public Map<PartialPath, List<TimeseriesSchemaInfo>> getTimeseriesToSchemaInfo() {
    return timeseriesToSchemaInfo;
  }

  public void setDeviceToTimeseriesSchemaInfo(
      Map<PartialPath, Map<PartialPath, List<TimeseriesSchemaInfo>>> deviceToTimeseriesSchemaInfo) {
    this.deviceToTimeseriesSchemaInfo = deviceToTimeseriesSchemaInfo;
  }

  public Map<PartialPath, Map<PartialPath, List<TimeseriesSchemaInfo>>>
      getDeviceToTimeseriesSchemaInfo() {
    return deviceToTimeseriesSchemaInfo;
  }

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("TimeseriesRegionScanNode does not support addChild");
  }

  @Override
  public PlanNode clone() {
    Map<PartialPath, List<TimeseriesSchemaInfo>> timeseriesToSchemaInfo =
        getTimeseriesToSchemaInfo();
    TimeseriesRegionScanNode timeseriesRegionScanNode =
        new TimeseriesRegionScanNode(
            getPlanNodeId(), timeseriesToSchemaInfo, isOutputCount(), getRegionReplicaSet());
    if (timeseriesToSchemaInfo == null) {
      timeseriesRegionScanNode.setDeviceToTimeseriesSchemaInfo(deviceToTimeseriesSchemaInfo);
    }
    return timeseriesRegionScanNode;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return outputCount
        ? ColumnHeaderConstant.countTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .collect(Collectors.toList())
        : ColumnHeaderConstant.showTimeSeriesColumnHeaders.stream()
            .map(ColumnHeader::getColumnName)
            .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTimeSeriesRegionScan(this, context);
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    int size = ReadWriteIOUtils.readInt(buffer);
    Map<PartialPath, List<TimeseriesSchemaInfo>> timeseriesToSchemaInfo = new HashMap<>();
    for (int i = 0; i < size; i++) {
      PartialPath path = PartialPath.deserialize(buffer);
      List<TimeseriesSchemaInfo> timeseriesSchemaInfos = new ArrayList<>();
      int schemaSize = ReadWriteIOUtils.readInt(buffer);
      for (int j = 0; j < schemaSize; j++) {
        timeseriesSchemaInfos.add(TimeseriesSchemaInfo.deserialize(buffer));
      }
      timeseriesToSchemaInfo.put(path, timeseriesSchemaInfos);
    }
    boolean outputCount = ReadWriteIOUtils.readBool(buffer);
    return new TimeseriesRegionScanNode(null, timeseriesToSchemaInfo, outputCount, null);
  }

  @TestOnly
  public List<PartialPath> getMeasurementPath() throws IllegalPathException {
    if (deviceToTimeseriesSchemaInfo != null) {
      List<PartialPath> partialPaths = new ArrayList<>();
      for (PartialPath path :
          deviceToTimeseriesSchemaInfo.values().stream()
              .map(Map::keySet)
              .flatMap(Set::stream)
              .collect(Collectors.toList())) {
        if (path instanceof AlignedPath) {
          for (String measurementName : ((AlignedPath) path).getMeasurementList()) {
            partialPaths.add(new PartialPath(new PlainDeviceID(path.getDevice()), measurementName));
          }
        } else {
          partialPaths.add(path);
        }
      }
      return partialPaths;
    }
    return new ArrayList<>(timeseriesToSchemaInfo.keySet());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TIMESERIES_REGION_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(timeseriesToSchemaInfo.size(), byteBuffer);
    for (Map.Entry<PartialPath, Map<PartialPath, List<TimeseriesSchemaInfo>>> entry :
        deviceToTimeseriesSchemaInfo.entrySet()) {
      ReadWriteIOUtils.write(entry.getValue().size(), byteBuffer);
      for (Map.Entry<PartialPath, List<TimeseriesSchemaInfo>> timeseriesSchemaInfoEntry :
          entry.getValue().entrySet()) {
        timeseriesSchemaInfoEntry.getKey().serialize(byteBuffer);
        ReadWriteIOUtils.write(timeseriesSchemaInfoEntry.getValue().size(), byteBuffer);
        for (TimeseriesSchemaInfo timeseriesSchemaInfo : timeseriesSchemaInfoEntry.getValue()) {
          timeseriesSchemaInfo.serializeAttributes(byteBuffer);
        }
      }
    }
    ReadWriteIOUtils.write(outputCount, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TIMESERIES_REGION_SCAN.serialize(stream);
    ReadWriteIOUtils.write(timeseriesToSchemaInfo.size(), stream);
    for (Map.Entry<PartialPath, Map<PartialPath, List<TimeseriesSchemaInfo>>> entry :
        deviceToTimeseriesSchemaInfo.entrySet()) {
      ReadWriteIOUtils.write(entry.getValue().size(), stream);
      for (Map.Entry<PartialPath, List<TimeseriesSchemaInfo>> timeseriesSchemaInfoEntry :
          entry.getValue().entrySet()) {
        timeseriesSchemaInfoEntry.getKey().serialize(stream);
        ReadWriteIOUtils.write(timeseriesSchemaInfoEntry.getValue().size(), stream);
        for (TimeseriesSchemaInfo timeseriesSchemaInfo : timeseriesSchemaInfoEntry.getValue()) {
          timeseriesSchemaInfo.serializeAttributes(stream);
        }
      }
    }
    ReadWriteIOUtils.write(outputCount, stream);
  }

  @Override
  public String toString() {
    return String.format(
        "TimeseriesRegionScanNode-%s:[DataRegion: %s OutputCount: %s]",
        PlanNodeType.TIMESERIES_REGION_SCAN, timeseriesToSchemaInfo, outputCount);
  }

  @Override
  public Set<PartialPath> getDevicePaths() {
    if (deviceToTimeseriesSchemaInfo != null) {
      return new HashSet<>(deviceToTimeseriesSchemaInfo.keySet());
    }
    return timeseriesToSchemaInfo.keySet().stream()
        .map(PartialPath::getDevicePath)
        .collect(Collectors.toSet());
  }

  @Override
  public void addDevicePath(PartialPath devicePath, RegionScanNode node) {
    this.deviceToTimeseriesSchemaInfo.put(
        devicePath,
        ((TimeseriesRegionScanNode) node).getDeviceToTimeseriesSchemaInfo().get(devicePath));
  }

  @Override
  public void clearPath() {
    this.deviceToTimeseriesSchemaInfo = new HashMap<>();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimeseriesRegionScanNode)) {
      return false;
    }
    TimeseriesRegionScanNode that = (TimeseriesRegionScanNode) o;
    if (timeseriesToSchemaInfo != null) {
      return timeseriesToSchemaInfo.equals(that.timeseriesToSchemaInfo)
          && outputCount == that.isOutputCount();
    } else {
      return deviceToTimeseriesSchemaInfo.equals(that.deviceToTimeseriesSchemaInfo)
          && outputCount == that.isOutputCount();
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timeseriesToSchemaInfo, outputCount);
  }
}
