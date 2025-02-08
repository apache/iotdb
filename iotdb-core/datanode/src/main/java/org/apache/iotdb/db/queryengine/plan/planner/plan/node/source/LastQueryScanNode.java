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
import org.apache.iotdb.commons.path.MeasurementPath;
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
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.eclipse.jetty.util.StringUtil;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class LastQueryScanNode extends LastSeriesSourceNode {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LastQueryScanNode.class);

  public static final List<String> LAST_QUERY_HEADER_COLUMNS =
      ImmutableList.of(
          ColumnHeaderConstant.TIMESERIES,
          ColumnHeaderConstant.VALUE,
          ColumnHeaderConstant.DATATYPE);

  // The path of the target series which will be scanned.
  private final MeasurementPath seriesPath;

  private final String outputViewPath;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;

  public LastQueryScanNode(PlanNodeId id, MeasurementPath seriesPath, String outputViewPath) {
    super(id, new AtomicInteger(1));
    this.seriesPath = seriesPath;
    this.outputViewPath = outputViewPath;
  }

  public LastQueryScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      AtomicInteger dataNodeSeriesScanNum,
      String outputViewPath) {
    super(id, dataNodeSeriesScanNum);
    this.seriesPath = seriesPath;
    this.outputViewPath = outputViewPath;
  }

  public LastQueryScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      AtomicInteger dataNodeSeriesScanNum,
      String outputViewPath,
      TRegionReplicaSet regionReplicaSet) {
    super(id, dataNodeSeriesScanNum);
    this.seriesPath = seriesPath;
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

  public MeasurementPath getSeriesPath() {
    return seriesPath;
  }

  public String getOutputViewPath() {
    return outputViewPath;
  }

  public String getOutputSymbolForSort() {
    if (outputViewPath != null) {
      return outputViewPath;
    }
    return seriesPath.getFullPath();
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
        getPlanNodeId(), seriesPath, getDataNodeSeriesScanNum(), outputViewPath, regionReplicaSet);
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
    return Objects.equals(seriesPath, that.seriesPath)
        && Objects.equals(outputViewPath, that.outputViewPath)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), seriesPath, outputViewPath, regionReplicaSet);
  }

  @Override
  public String toString() {
    if (StringUtil.isNotBlank(outputViewPath)) {
      return String.format(
          "LastQueryScanNode-%s:[SeriesPath: %s, ViewPath: %s, DataRegion: %s]",
          this.getPlanNodeId(),
          this.getSeriesPath(),
          this.getOutputViewPath(),
          PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
    } else {
      return String.format(
          "LastQueryScanNode-%s:[SeriesPath: %s, DataRegion: %s]",
          this.getPlanNodeId(),
          this.getSeriesPath(),
          PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
    }
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.LAST_QUERY_SCAN.serialize(byteBuffer);
    seriesPath.serialize(byteBuffer);
    ReadWriteIOUtils.write(getDataNodeSeriesScanNum().get(), byteBuffer);
    ReadWriteIOUtils.write(outputViewPath == null, byteBuffer);
    if (outputViewPath != null) {
      ReadWriteIOUtils.write(outputViewPath, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.LAST_QUERY_SCAN.serialize(stream);
    seriesPath.serialize(stream);
    ReadWriteIOUtils.write(getDataNodeSeriesScanNum().get(), stream);
    ReadWriteIOUtils.write(outputViewPath == null, stream);
    if (outputViewPath != null) {
      ReadWriteIOUtils.write(outputViewPath, stream);
    }
  }

  public static LastQueryScanNode deserialize(ByteBuffer byteBuffer) {
    MeasurementPath partialPath = (MeasurementPath) PathDeserializeUtil.deserialize(byteBuffer);
    int dataNodeSeriesScanNum = ReadWriteIOUtils.readInt(byteBuffer);
    boolean isNull = ReadWriteIOUtils.readBool(byteBuffer);
    String outputPathSymbol = isNull ? null : ReadWriteIOUtils.readString(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new LastQueryScanNode(
        planNodeId, partialPath, new AtomicInteger(dataNodeSeriesScanNum), outputPathSymbol);
  }

  @Override
  public PartialPath getPartitionPath() {
    return getSeriesPath();
  }

  public String outputPathSymbol() {
    if (outputViewPath == null) {
      return seriesPath.getFullPath();
    } else {
      return outputViewPath;
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(id)
        + MemoryEstimationHelper.getEstimatedSizeOfPartialPath(seriesPath)
        + RamUsageEstimator.sizeOf(outputViewPath);
  }
}
