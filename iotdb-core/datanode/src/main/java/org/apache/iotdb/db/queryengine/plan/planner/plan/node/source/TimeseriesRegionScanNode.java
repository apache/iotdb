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
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathType;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.common.TimeseriesContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeseriesRegionScanNode extends RegionScanNode {
  // IDeviceID -> (MeasurementPath -> TimeseriesSchemaInfo)
  private Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>> deviceToTimeseriesSchemaInfo;

  public TimeseriesRegionScanNode(
      PlanNodeId planNodeId, boolean outputCount, TRegionReplicaSet regionReplicaSet) {
    super(planNodeId);
    this.regionReplicaSet = regionReplicaSet;
    this.outputCount = outputCount;
  }

  public TimeseriesRegionScanNode(
      PlanNodeId planNodeId,
      Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>> deviceToTimeseriesSchemaInfo,
      boolean outputCount,
      TRegionReplicaSet regionReplicaSet) {
    super(planNodeId);
    this.deviceToTimeseriesSchemaInfo = deviceToTimeseriesSchemaInfo;
    this.regionReplicaSet = regionReplicaSet;
    this.outputCount = outputCount;
  }

  public void setDeviceToTimeseriesSchemaInfo(
      Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>> deviceToTimeseriesSchemaInfo) {
    this.deviceToTimeseriesSchemaInfo = deviceToTimeseriesSchemaInfo;
  }

  public Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>>
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
    return new TimeseriesRegionScanNode(
        getPlanNodeId(), getDeviceToTimeseriesSchemaInfo(), isOutputCount(), getRegionReplicaSet());
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
    Map<PartialPath, Map<PartialPath, List<TimeseriesContext>>> deviceToTimeseriesSchemaInfo =
        new HashMap<>();
    for (int i = 0; i < size; i++) {

      int nodeSize = ReadWriteIOUtils.readInt(buffer);
      String[] nodes = new String[nodeSize];
      for (int j = 0; j < nodeSize; j++) {
        nodes[j] = ReadWriteIOUtils.readString(buffer);
      }
      PartialPath devicePath = new PartialPath(nodes);

      int pathSize = ReadWriteIOUtils.readInt(buffer);
      Map<PartialPath, List<TimeseriesContext>> measurementToSchemaInfo = new HashMap<>();
      for (int j = 0; j < pathSize; j++) {
        PartialPath path = deserializePartialPath(nodes, buffer);
        int schemaSize = ReadWriteIOUtils.readInt(buffer);
        List<TimeseriesContext> schemaInfos = new ArrayList<>();
        for (int k = 0; k < schemaSize; k++) {
          schemaInfos.add(TimeseriesContext.deserialize(buffer));
        }
        measurementToSchemaInfo.put(path, schemaInfos);
      }
      deviceToTimeseriesSchemaInfo.put(devicePath, measurementToSchemaInfo);
    }
    boolean outputCount = ReadWriteIOUtils.readBool(buffer);
    return new TimeseriesRegionScanNode(
        PlanNodeId.deserialize(buffer), deviceToTimeseriesSchemaInfo, outputCount, null);
  }

  @TestOnly
  public List<PartialPath> getMeasurementPath() {
    return deviceToTimeseriesSchemaInfo.values().stream()
        .map(Map::keySet)
        .flatMap(Set::stream)
        .flatMap(
            path -> {
              if (path instanceof AlignedPath) {
                AlignedPath alignedPath = (AlignedPath) path;
                return alignedPath.getMeasurementList().stream()
                    .map(
                        measurementName ->
                            alignedPath.getDevicePath().concatAsMeasurementPath(measurementName));
              } else {
                return Stream.of(path);
              }
            })
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return String.format(
        "%s[%s]",
        getClass().getSimpleName(),
        deviceToTimeseriesSchemaInfo.entrySet().stream()
            .map(
                entry ->
                    String.format(
                        "%s -> %s",
                        entry.getKey().getFullPath(),
                        entry.getValue().entrySet().stream()
                            .map(
                                entry1 ->
                                    String.format(
                                        "%s -> %s",
                                        entry1.getKey().getFullPath(),
                                        entry1.getValue().stream()
                                            .map(TimeseriesContext::toString)
                                            .collect(Collectors.joining(", "))))
                            .collect(Collectors.joining(", "))))
            .collect(Collectors.joining(", ")));
  }

  @Override
  public Set<PartialPath> getDevicePaths() {
    return new HashSet<>(deviceToTimeseriesSchemaInfo.keySet());
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
  public long getSize() {
    return deviceToTimeseriesSchemaInfo.values().stream().mapToLong(Map::size).sum();
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
    return deviceToTimeseriesSchemaInfo.equals(that.deviceToTimeseriesSchemaInfo)
        && outputCount == that.isOutputCount();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), deviceToTimeseriesSchemaInfo, outputCount);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TIMESERIES_REGION_SCAN.serialize(byteBuffer);
    ReadWriteIOUtils.write(deviceToTimeseriesSchemaInfo.size(), byteBuffer);
    for (Map.Entry<PartialPath, Map<PartialPath, List<TimeseriesContext>>> entry :
        deviceToTimeseriesSchemaInfo.entrySet()) {

      int size = entry.getKey().getNodeLength();
      ReadWriteIOUtils.write(size, byteBuffer);
      String[] nodes = entry.getKey().getNodes();
      for (int i = 0; i < size; i++) {
        ReadWriteIOUtils.write(nodes[i], byteBuffer);
      }

      ReadWriteIOUtils.write(entry.getValue().size(), byteBuffer);
      for (Map.Entry<PartialPath, List<TimeseriesContext>> timseriesEntry :
          entry.getValue().entrySet()) {
        serializeMeasurements(timseriesEntry.getKey(), byteBuffer);
        ReadWriteIOUtils.write(timseriesEntry.getValue().size(), byteBuffer);
        for (TimeseriesContext timeseriesContext : timseriesEntry.getValue()) {
          timeseriesContext.serializeAttributes(byteBuffer);
        }
      }
    }
    ReadWriteIOUtils.write(outputCount, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TIMESERIES_REGION_SCAN.serialize(stream);
    ReadWriteIOUtils.write(deviceToTimeseriesSchemaInfo.size(), stream);
    for (Map.Entry<PartialPath, Map<PartialPath, List<TimeseriesContext>>> entry :
        deviceToTimeseriesSchemaInfo.entrySet()) {

      int size = entry.getKey().getNodeLength();
      ReadWriteIOUtils.write(size, stream);
      String[] nodes = entry.getKey().getNodes();
      for (int i = 0; i < size; i++) {
        ReadWriteIOUtils.write(nodes[i], stream);
      }

      ReadWriteIOUtils.write(entry.getValue().size(), stream);
      for (Map.Entry<PartialPath, List<TimeseriesContext>> timseriesEntry :
          entry.getValue().entrySet()) {
        serializeMeasurements(timseriesEntry.getKey(), stream);
        ReadWriteIOUtils.write(timseriesEntry.getValue().size(), stream);
        for (TimeseriesContext timeseriesContext : timseriesEntry.getValue()) {
          timeseriesContext.serializeAttributes(stream);
        }
      }
    }
    ReadWriteIOUtils.write(outputCount, stream);
  }

  private static PartialPath deserializePartialPath(String[] deviceNodes, ByteBuffer buffer) {
    byte pathType = buffer.get();
    if (pathType == 0) {
      String[] newNodes = Arrays.copyOf(deviceNodes, deviceNodes.length + 1);
      newNodes[deviceNodes.length] = ReadWriteIOUtils.readString(buffer);
      return new MeasurementPath(newNodes);
    } else {
      int size = ReadWriteIOUtils.readInt(buffer);
      List<String> measurements = new ArrayList<>();
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        measurements.add(ReadWriteIOUtils.readString(buffer));
      }
      size = ReadWriteIOUtils.readInt(buffer);
      for (int i = 0; i < size; i++) {
        schemaList.add(MeasurementSchema.deserializeFrom(buffer));
      }
      return new AlignedPath(deviceNodes, measurements, schemaList);
    }
  }

  private void serializeMeasurements(PartialPath path, DataOutputStream stream) throws IOException {
    if (path instanceof MeasurementPath) {
      PathType.Measurement.serialize(stream);
      ReadWriteIOUtils.write(path.getMeasurement(), stream);
    } else if (path instanceof AlignedPath) {
      PathType.Aligned.serialize(stream);
      AlignedPath alignedPath = (AlignedPath) path;

      List<String> measurements = alignedPath.getMeasurementList();
      List<IMeasurementSchema> schemaList = alignedPath.getSchemaList();
      ReadWriteIOUtils.write(measurements.size(), stream);
      for (int i = 0; i < measurements.size(); i++) {
        ReadWriteIOUtils.write(measurements.get(i), stream);
      }
      ReadWriteIOUtils.write(schemaList.size(), stream);
      for (int i = 0; i < schemaList.size(); i++) {
        schemaList.get(i).serializeTo(stream);
      }
    }
  }

  private void serializeMeasurements(PartialPath path, ByteBuffer buffer) {
    if (path instanceof MeasurementPath) {
      PathType.Measurement.serialize(buffer);
      ReadWriteIOUtils.write(path.getMeasurement(), buffer);
    } else if (path instanceof AlignedPath) {
      PathType.Aligned.serialize(buffer);
      AlignedPath alignedPath = (AlignedPath) path;

      List<String> measurements = alignedPath.getMeasurementList();
      List<IMeasurementSchema> schemaList = alignedPath.getSchemaList();
      ReadWriteIOUtils.write(measurements.size(), buffer);
      for (int i = 0; i < measurements.size(); i++) {
        ReadWriteIOUtils.write(measurements.get(i), buffer);
      }
      ReadWriteIOUtils.write(schemaList.size(), buffer);
      for (int i = 0; i < schemaList.size(); i++) {
        schemaList.get(i).serializeTo(buffer);
      }
    }
  }
}
