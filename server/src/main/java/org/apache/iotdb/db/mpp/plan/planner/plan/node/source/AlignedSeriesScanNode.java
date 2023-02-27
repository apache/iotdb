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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.source;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AlignedSeriesScanNode extends SeriesSourceNode {

  // The paths of the target series which will be scanned.
  private final AlignedPath alignedPath;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  private Ordering scanOrder = Ordering.ASC;

  // time filter for current series, could be null if doesn't exist
  @Nullable private Filter timeFilter;

  // value filter for current series, could be null if doesn't exist
  @Nullable private Filter valueFilter;

  // Limit for result set. The default value is -1, which means no limit
  private long limit;

  // offset for result set. The default value is 0
  private long offset;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;

  public AlignedSeriesScanNode(PlanNodeId id, AlignedPath alignedPath) {
    super(id);
    this.alignedPath = alignedPath;
  }

  public AlignedSeriesScanNode(PlanNodeId id, AlignedPath alignedPath, Ordering scanOrder) {
    this(id, alignedPath);
    this.scanOrder = scanOrder;
  }

  public AlignedSeriesScanNode(
      PlanNodeId id,
      AlignedPath alignedPath,
      Ordering scanOrder,
      @Nullable Filter timeFilter,
      @Nullable Filter valueFilter,
      long limit,
      long offset,
      TRegionReplicaSet dataRegionReplicaSet) {
    this(id, alignedPath, scanOrder);
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
    this.limit = limit;
    this.offset = offset;
    this.regionReplicaSet = dataRegionReplicaSet;
  }

  public AlignedPath getAlignedPath() {
    return alignedPath;
  }

  public Ordering getScanOrder() {
    return scanOrder;
  }

  @Nullable
  public Filter getTimeFilter() {
    return timeFilter;
  }

  public void setTimeFilter(@Nullable Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  @Nullable
  public Filter getValueFilter() {
    return valueFilter;
  }

  public long getLimit() {
    return limit;
  }

  public long getOffset() {
    return offset;
  }

  public void setLimit(long limit) {
    this.limit = limit;
  }

  public void setOffset(long offset) {
    this.offset = offset;
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

  @Override
  public void close() throws Exception {}

  @Override
  public List<PlanNode> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException("no child is allowed for AlignedSeriesScanNode");
  }

  @Override
  public PlanNode clone() {
    return new AlignedSeriesScanNode(
        getPlanNodeId(),
        getAlignedPath(),
        getScanOrder(),
        getTimeFilter(),
        getValueFilter(),
        getLimit(),
        getOffset(),
        this.regionReplicaSet);
  }

  @Override
  public List<String> getOutputColumnNames() {
    List<String> outputColumnNames = new ArrayList<>();
    String deviceName = alignedPath.getDevice();
    for (String measurement : alignedPath.getMeasurementList()) {
      outputColumnNames.add(deviceName.concat(TsFileConstant.PATH_SEPARATOR + measurement));
    }
    return outputColumnNames;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAlignedSeriesScan(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.ALIGNED_SERIES_SCAN.serialize(byteBuffer);
    alignedPath.serialize(byteBuffer);
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
    if (timeFilter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      timeFilter.serialize(byteBuffer);
    }
    if (valueFilter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      valueFilter.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(limit, byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.ALIGNED_SERIES_SCAN.serialize(stream);
    alignedPath.serialize(stream);
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
    if (timeFilter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      timeFilter.serialize(stream);
    }
    if (valueFilter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      valueFilter.serialize(stream);
    }
    ReadWriteIOUtils.write(limit, stream);
    ReadWriteIOUtils.write(offset, stream);
  }

  public static AlignedSeriesScanNode deserialize(ByteBuffer byteBuffer) {
    AlignedPath alignedPath = (AlignedPath) PathDeserializeUtil.deserialize(byteBuffer);
    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    byte isNull = ReadWriteIOUtils.readByte(byteBuffer);
    Filter timeFilter = null;
    if (isNull == 1) {
      timeFilter = FilterFactory.deserialize(byteBuffer);
    }
    isNull = ReadWriteIOUtils.readByte(byteBuffer);
    Filter valueFilter = null;
    if (isNull == 1) {
      valueFilter = FilterFactory.deserialize(byteBuffer);
    }
    long limit = ReadWriteIOUtils.readLong(byteBuffer);
    long offset = ReadWriteIOUtils.readLong(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AlignedSeriesScanNode(
        planNodeId, alignedPath, scanOrder, timeFilter, valueFilter, limit, offset, null);
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
    AlignedSeriesScanNode that = (AlignedSeriesScanNode) o;
    return limit == that.limit
        && offset == that.offset
        && alignedPath.equals(that.alignedPath)
        && scanOrder == that.scanOrder
        && Objects.equals(timeFilter, that.timeFilter)
        && Objects.equals(valueFilter, that.valueFilter)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        alignedPath,
        scanOrder,
        timeFilter,
        valueFilter,
        limit,
        offset,
        regionReplicaSet);
  }

  public String toString() {
    return String.format(
        "AlignedSeriesScanNode-%s:[SeriesPath: %s, DataRegion: %s]",
        this.getPlanNodeId(),
        this.getAlignedPath().getFormattedString(),
        PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
  }

  @Override
  public PartialPath getPartitionPath() {
    return alignedPath;
  }

  @Override
  public Filter getPartitionTimeFilter() {
    return timeFilter;
  }
}
