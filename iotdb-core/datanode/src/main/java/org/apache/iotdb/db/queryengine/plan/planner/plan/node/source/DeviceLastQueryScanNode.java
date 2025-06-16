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
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;
import org.eclipse.jetty.util.StringUtil;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class DeviceLastQueryScanNode extends LastSeriesSourceNode {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DeviceLastQueryScanNode.class);

  public static final List<String> LAST_QUERY_HEADER_COLUMNS =
      ImmutableList.of(
          ColumnHeaderConstant.TIMESERIES,
          ColumnHeaderConstant.VALUE,
          ColumnHeaderConstant.DATATYPE);

  private final PartialPath devicePath;
  private final boolean aligned;
  private final List<IMeasurementSchema> measurementSchemas;

  private final String outputViewPath;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;

  public DeviceLastQueryScanNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean aligned,
      List<IMeasurementSchema> measurementSchemas,
      String outputViewPath) {
    super(id, new AtomicInteger(1));
    this.aligned = aligned;
    this.devicePath = devicePath;
    this.measurementSchemas = measurementSchemas;
    this.outputViewPath = outputViewPath;
  }

  public DeviceLastQueryScanNode(
      PlanNodeId id, MeasurementPath measurementPath, String outputViewPath) {
    super(id, new AtomicInteger(1));
    this.aligned = measurementPath.isUnderAlignedEntity();
    this.devicePath = measurementPath.getDevicePath();
    this.measurementSchemas = Collections.singletonList(measurementPath.getMeasurementSchema());
    this.outputViewPath = outputViewPath;
  }

  public DeviceLastQueryScanNode(PlanNodeId id, AlignedPath alignedPath, String outputViewPath) {
    super(id, new AtomicInteger(1));
    this.aligned = true;
    this.devicePath = alignedPath.getDevicePath();
    this.measurementSchemas = alignedPath.getSchemaList();
    this.outputViewPath = outputViewPath;
  }

  public DeviceLastQueryScanNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean aligned,
      List<IMeasurementSchema> measurementSchemas,
      AtomicInteger dataNodeSeriesScanNum,
      String outputViewPath) {
    super(id, dataNodeSeriesScanNum);
    this.aligned = aligned;
    this.devicePath = devicePath;
    this.measurementSchemas = measurementSchemas;
    this.outputViewPath = outputViewPath;
  }

  public DeviceLastQueryScanNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean aligned,
      List<IMeasurementSchema> measurementSchemas,
      AtomicInteger dataNodeSeriesScanNum,
      String outputViewPath,
      TRegionReplicaSet regionReplicaSet) {
    super(id, dataNodeSeriesScanNum);
    this.devicePath = devicePath;
    this.aligned = aligned;
    this.measurementSchemas = measurementSchemas;
    this.outputViewPath = outputViewPath;
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public void open() throws Exception {}

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  public PartialPath getSeriesPath() {
    return devicePath;
  }

  public boolean isAligned() {
    return this.aligned;
  }

  public String getOutputViewPath() {
    return outputViewPath;
  }

  public String getOutputSymbolForSort() {
    if (outputViewPath != null) {
      return outputViewPath;
    }
    return devicePath.toString();
  }

  @Override
  public void close() throws Exception {}

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("no child is allowed for SeriesScanNode");
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.DEVICE_LAST_QUERY_SCAN;
  }

  @Override
  public PlanNode clone() {
    return new DeviceLastQueryScanNode(
        getPlanNodeId(),
        devicePath,
        aligned,
        measurementSchemas,
        getDataNodeSeriesScanNum(),
        outputViewPath,
        regionReplicaSet);
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return LAST_QUERY_HEADER_COLUMNS;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitDeviceLastQueryScan(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    DeviceLastQueryScanNode that = (DeviceLastQueryScanNode) o;
    return Objects.equals(devicePath, that.devicePath)
        && Objects.equals(aligned, that.aligned)
        && Objects.equals(measurementSchemas, that.measurementSchemas)
        && Objects.equals(outputViewPath, that.outputViewPath)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        devicePath,
        aligned,
        measurementSchemas,
        outputViewPath,
        regionReplicaSet);
  }

  @Override
  public String toString() {
    if (StringUtil.isNotBlank(outputViewPath)) {
      return String.format(
          "DeviceLastQueryScanNode-%s:[Device: %s, Aligned: %s, Measurements: %s, ViewPath: %s, DataRegion: %s]",
          this.getPlanNodeId(),
          this.getDevicePath(),
          this.aligned,
          this.getMeasurementSchemas(),
          this.getOutputViewPath(),
          PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
    } else {
      return String.format(
          "DeviceLastQueryScanNode-%s:[Device: %s, Aligned: %s, Measurements: %s, DataRegion: %s]",
          this.getPlanNodeId(),
          this.getDevicePath(),
          this.aligned,
          this.getMeasurementSchemas(),
          PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
    }
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.DEVICE_LAST_QUERY_SCAN.serialize(byteBuffer);
    devicePath.serialize(byteBuffer);
    ReadWriteIOUtils.write(aligned, byteBuffer);
    ReadWriteIOUtils.write(measurementSchemas.size(), byteBuffer);
    for (IMeasurementSchema measurementSchema : measurementSchemas) {
      if (measurementSchema instanceof MeasurementSchema) {
        ReadWriteIOUtils.write((byte) 0, byteBuffer);
      } else if (measurementSchema instanceof VectorMeasurementSchema) {
        ReadWriteIOUtils.write((byte) 1, byteBuffer);
      }
      measurementSchema.serializeTo(byteBuffer);
    }
    ReadWriteIOUtils.write(getDataNodeSeriesScanNum().get(), byteBuffer);
    ReadWriteIOUtils.write(outputViewPath == null, byteBuffer);
    if (outputViewPath != null) {
      ReadWriteIOUtils.write(outputViewPath, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.DEVICE_LAST_QUERY_SCAN.serialize(stream);
    devicePath.serialize(stream);
    ReadWriteIOUtils.write(aligned, stream);
    ReadWriteIOUtils.write(measurementSchemas.size(), stream);
    for (IMeasurementSchema measurementSchema : measurementSchemas) {
      if (measurementSchema instanceof MeasurementSchema) {
        ReadWriteIOUtils.write((byte) 0, stream);
      } else if (measurementSchema instanceof VectorMeasurementSchema) {
        ReadWriteIOUtils.write((byte) 1, stream);
      }
      measurementSchema.serializeTo(stream);
    }
    ReadWriteIOUtils.write(getDataNodeSeriesScanNum().get(), stream);
    ReadWriteIOUtils.write(outputViewPath == null, stream);
    if (outputViewPath != null) {
      ReadWriteIOUtils.write(outputViewPath, stream);
    }
  }

  public static DeviceLastQueryScanNode deserialize(ByteBuffer byteBuffer) {
    PartialPath devicePath = PartialPath.deserialize(byteBuffer);
    boolean aligned = ReadWriteIOUtils.readBool(byteBuffer);
    int measurementSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>(measurementSize);
    for (int i = 0; i < measurementSize; i++) {
      byte type = ReadWriteIOUtils.readByte(byteBuffer);
      if (type == 0) {
        measurementSchemas.add(MeasurementSchema.deserializeFrom(byteBuffer));
      } else if (type == 1) {
        measurementSchemas.add(VectorMeasurementSchema.deserializeFrom(byteBuffer));
      }
    }

    int dataNodeSeriesScanNum = ReadWriteIOUtils.readInt(byteBuffer);
    boolean isNull = ReadWriteIOUtils.readBool(byteBuffer);
    String outputPathSymbol = isNull ? null : ReadWriteIOUtils.readString(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new DeviceLastQueryScanNode(
        planNodeId,
        devicePath,
        aligned,
        measurementSchemas,
        new AtomicInteger(dataNodeSeriesScanNum),
        outputPathSymbol);
  }

  public PartialPath getDevicePath() {
    return this.devicePath;
  }

  public List<IMeasurementSchema> getMeasurementSchemas() {
    return measurementSchemas;
  }

  @Override
  public PartialPath getPartitionPath() {
    return devicePath;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(id)
        + MemoryEstimationHelper.getEstimatedSizeOfPartialPath(devicePath)
        + measurementSchemas.stream()
            .mapToLong(schema -> RamUsageEstimator.sizeOf(schema.getMeasurementName()))
            .sum()
        + RamUsageEstimator.sizeOf(outputViewPath);
  }
}
