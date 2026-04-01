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
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LastQueryScanNode extends LastSeriesSourceNode {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LastQueryScanNode.class);

  public static final List<String> LAST_QUERY_HEADER_COLUMNS =
      ImmutableList.of(
          ColumnHeaderConstant.TIMESERIES,
          ColumnHeaderConstant.VALUE,
          ColumnHeaderConstant.DATATYPE);

  private final PartialPath devicePath;
  private final boolean aligned;
  private final List<Integer> indexOfMeasurementSchemas;
  // This structure does not need to be serialized or deserialized.
  // It will be set when the current Node is added to the child by the upper LastQueryNode.
  private List<IMeasurementSchema> globalMeasurementSchemaList;

  // Store alias of paths or viewPath in this field.
  private final List<String> outputPaths;
  // Indicate if there is viewPath stored in outputPaths.
  private final boolean isOutputPathForView;
  private final TSDataType outputViewPathType;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;
  private boolean deviceInMultiRegion = false;

  public LastQueryScanNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean aligned,
      List<Integer> indexOfMeasurementSchemas,
      List<String> outputPaths,
      boolean isOutputPathForView,
      TSDataType outputViewPathType,
      List<IMeasurementSchema> globalMeasurementSchemaList) {
    super(id, new AtomicInteger(1));
    this.aligned = aligned;
    this.devicePath = devicePath;
    this.indexOfMeasurementSchemas = indexOfMeasurementSchemas;
    this.outputPaths = outputPaths;
    this.outputViewPathType = outputViewPathType;
    this.globalMeasurementSchemaList = globalMeasurementSchemaList;
    this.isOutputPathForView = isOutputPathForView;
  }

  public LastQueryScanNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean aligned,
      List<Integer> indexOfMeasurementSchemas,
      AtomicInteger dataNodeSeriesScanNum,
      List<String> outputPaths,
      boolean isOutputPathForView,
      TSDataType outputViewPathType) {
    this(
        id,
        devicePath,
        aligned,
        indexOfMeasurementSchemas,
        dataNodeSeriesScanNum,
        outputPaths,
        isOutputPathForView,
        outputViewPathType,
        null);
  }

  public LastQueryScanNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean aligned,
      List<Integer> indexOfMeasurementSchemas,
      AtomicInteger dataNodeSeriesScanNum,
      List<String> outputPaths,
      boolean isOutputPathForView,
      TSDataType outputViewPathType,
      List<IMeasurementSchema> globalMeasurementSchemaList) {
    super(id, dataNodeSeriesScanNum);
    this.aligned = aligned;
    this.devicePath = devicePath;
    this.indexOfMeasurementSchemas = indexOfMeasurementSchemas;
    this.outputPaths = outputPaths;
    this.isOutputPathForView = isOutputPathForView;
    this.outputViewPathType = outputViewPathType;
    this.globalMeasurementSchemaList = globalMeasurementSchemaList;
  }

  public LastQueryScanNode(
      PlanNodeId id,
      PartialPath devicePath,
      boolean aligned,
      List<Integer> indexOfMeasurementSchemas,
      AtomicInteger dataNodeSeriesScanNum,
      List<String> outputPaths,
      boolean isOutputPathForView,
      TSDataType outputViewPathType,
      TRegionReplicaSet regionReplicaSet,
      boolean deviceInMultiRegion,
      List<IMeasurementSchema> globalMeasurementSchemaList) {
    super(id, dataNodeSeriesScanNum);
    this.devicePath = devicePath;
    this.aligned = aligned;
    this.indexOfMeasurementSchemas = indexOfMeasurementSchemas;
    this.outputPaths = outputPaths;
    this.isOutputPathForView = isOutputPathForView;
    this.outputViewPathType = outputViewPathType;
    this.regionReplicaSet = regionReplicaSet;
    this.deviceInMultiRegion = deviceInMultiRegion;
    this.globalMeasurementSchemaList = globalMeasurementSchemaList;
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

  public List<String> getOutputPaths() {
    return outputPaths;
  }

  public TSDataType getOutputViewPathType() {
    return outputViewPathType;
  }

  public boolean isOutputPathForView() {
    return isOutputPathForView;
  }

  public String getOutputSymbolForSort() {
    if (outputPaths != null && outputPaths.size() == 1) {
      return outputPaths.get(0);
    }
    // If outputPaths is null or size > 1, it means there is no view and no alias, just return the
    // device name is ok,
    // because the measurements have been sorted in AnalyzeVisitor if needed.
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
    return PlanNodeType.LAST_QUERY_SCAN;
  }

  @Override
  public PlanNode clone() {
    return new LastQueryScanNode(
        getPlanNodeId(),
        devicePath,
        aligned,
        indexOfMeasurementSchemas,
        getDataNodeSeriesScanNum(),
        outputPaths,
        isOutputPathForView,
        outputViewPathType,
        regionReplicaSet,
        deviceInMultiRegion,
        globalMeasurementSchemaList);
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
    return visitor.visitLastQueryScan(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    LastQueryScanNode that = (LastQueryScanNode) o;
    return Objects.equals(devicePath, that.devicePath)
        && Objects.equals(aligned, that.aligned)
        && Objects.equals(indexOfMeasurementSchemas, that.indexOfMeasurementSchemas)
        && Objects.equals(outputPaths, that.outputPaths)
        && Objects.equals(outputViewPathType, that.outputViewPathType)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        devicePath,
        aligned,
        indexOfMeasurementSchemas,
        outputPaths,
        regionReplicaSet);
  }

  @Override
  public String toString() {
    if (outputPaths != null) {
      return String.format(
          "LastQueryScanNode-%s:[Device: %s, Aligned: %s, Measurements: %s, OutputPaths: %s, DataRegion: %s]",
          this.getPlanNodeId(),
          this.getDevicePath(),
          this.aligned,
          this.getMeasurementSchemas(),
          this.getOutputPaths(),
          PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
    } else {
      return String.format(
          "LastQueryScanNode-%s:[Device: %s, Aligned: %s, Measurements: %s, DataRegion: %s]",
          this.getPlanNodeId(),
          this.getDevicePath(),
          this.aligned,
          this.getMeasurementSchemas(),
          PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
    }
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LAST_QUERY_SCAN.serialize(byteBuffer);
    devicePath.serialize(byteBuffer);
    ReadWriteIOUtils.write(aligned, byteBuffer);
    ReadWriteIOUtils.write(indexOfMeasurementSchemas.size(), byteBuffer);
    for (Integer measurementSchema : indexOfMeasurementSchemas) {
      ReadWriteIOUtils.write(measurementSchema, byteBuffer);
    }
    ReadWriteIOUtils.write(getDataNodeSeriesScanNum().get(), byteBuffer);
    ReadWriteIOUtils.write(outputPaths == null, byteBuffer);
    if (outputPaths != null) {
      int size = outputPaths.size();
      ReadWriteIOUtils.write(size, byteBuffer);
      for (int i = 0; i < size; i++) {
        ReadWriteIOUtils.write(outputPaths.get(i), byteBuffer);
      }
    }
    ReadWriteIOUtils.write(isOutputPathForView, byteBuffer);
    if (isOutputPathForView) {
      ReadWriteIOUtils.write(outputViewPathType, byteBuffer);
    }
    ReadWriteIOUtils.write(deviceInMultiRegion, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LAST_QUERY_SCAN.serialize(stream);
    devicePath.serialize(stream);
    ReadWriteIOUtils.write(aligned, stream);
    ReadWriteIOUtils.write(indexOfMeasurementSchemas.size(), stream);
    for (Integer measurementSchema : indexOfMeasurementSchemas) {
      ReadWriteIOUtils.write(measurementSchema, stream);
    }
    ReadWriteIOUtils.write(getDataNodeSeriesScanNum().get(), stream);
    ReadWriteIOUtils.write(outputPaths == null, stream);
    if (outputPaths != null) {
      int size = outputPaths.size();
      ReadWriteIOUtils.write(size, stream);
      for (int i = 0; i < size; i++) {
        ReadWriteIOUtils.write(outputPaths.get(i), stream);
      }
    }
    ReadWriteIOUtils.write(isOutputPathForView, stream);
    if (isOutputPathForView) {
      ReadWriteIOUtils.write(outputViewPathType, stream);
    }
    ReadWriteIOUtils.write(deviceInMultiRegion, stream);
  }

  public static LastQueryScanNode deserialize(ByteBuffer byteBuffer) {
    PartialPath devicePath = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    boolean aligned = ReadWriteIOUtils.readBool(byteBuffer);
    int measurementSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<Integer> measurementSchemas = new ArrayList<>(measurementSize);
    for (int i = 0; i < measurementSize; i++) {
      measurementSchemas.add(ReadWriteIOUtils.readInt(byteBuffer));
    }

    int dataNodeSeriesScanNum = ReadWriteIOUtils.readInt(byteBuffer);
    boolean isNull = ReadWriteIOUtils.readBool(byteBuffer);
    List<String> outputPaths = null;
    if (!isNull) {
      int size = ReadWriteIOUtils.readInt(byteBuffer);
      outputPaths = new ArrayList<>(size);
      while (size > 0) {
        outputPaths.add(ReadWriteIOUtils.readString(byteBuffer));
        size--;
      }
    }
    boolean isOutputPathForView = ReadWriteIOUtils.readBool(byteBuffer);
    TSDataType dataType = isOutputPathForView ? ReadWriteIOUtils.readDataType(byteBuffer) : null;
    boolean deviceInMultiRegion = ReadWriteIOUtils.readBool(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LastQueryScanNode(
        planNodeId,
        devicePath,
        aligned,
        measurementSchemas,
        new AtomicInteger(dataNodeSeriesScanNum),
        outputPaths,
        isOutputPathForView,
        dataType,
        null,
        deviceInMultiRegion,
        null);
  }

  public void setGlobalMeasurementSchemaList(List<IMeasurementSchema> globalMeasurementSchemaList) {
    this.globalMeasurementSchemaList = globalMeasurementSchemaList;
  }

  public List<IMeasurementSchema> getGlobalMeasurementSchemaList() {
    return globalMeasurementSchemaList;
  }

  public IMeasurementSchema getMeasurementSchema(int idx) {
    int globalIdx = indexOfMeasurementSchemas.get(idx);
    return globalMeasurementSchemaList.get(globalIdx);
  }

  public PartialPath getDevicePath() {
    return this.devicePath;
  }

  public boolean isDeviceInMultiRegion() {
    return deviceInMultiRegion;
  }

  public void setDeviceInMultiRegion(boolean deviceInMultiRegion) {
    this.deviceInMultiRegion = deviceInMultiRegion;
  }

  public List<Integer> getIdxOfMeasurementSchemas() {
    return indexOfMeasurementSchemas;
  }

  public List<IMeasurementSchema> getMeasurementSchemas() {
    return indexOfMeasurementSchemas.stream()
        .map(globalMeasurementSchemaList::get)
        .collect(Collectors.toList());
  }

  @Override
  public PartialPath getPartitionPath() {
    return devicePath;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(id)
        // The memory of each String has been calculated before
        + MemoryEstimationHelper.getEstimatedSizeOfCopiedPartialPath(devicePath)
        + MemoryEstimationHelper.getEstimatedSizeOfIntegerArrayList(indexOfMeasurementSchemas)
        + (outputPaths == null
            ? 0L
            : outputPaths.stream().mapToLong(path -> RamUsageEstimator.sizeOf(path)).sum());
  }
}
