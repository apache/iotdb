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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * SeriesScanNode is responsible for read data a specific series. When reading data, the
 * SeriesScanNode can read the raw data batch by batch. And also, it can leverage the filter and
 * other info to decrease the result set.
 *
 * <p>Children type: no child is allowed for SeriesScanNode
 */
public class SeriesScanNode extends SeriesSourceNode {

  // The path of the target series which will be scanned.
  private final MeasurementPath seriesPath;

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

  public SeriesScanNode(PlanNodeId id, MeasurementPath seriesPath) {
    super(id);
    this.seriesPath = seriesPath;
  }

  public SeriesScanNode(PlanNodeId id, MeasurementPath seriesPath, Ordering scanOrder) {
    this(id, seriesPath);
    this.scanOrder = scanOrder;
  }

  public SeriesScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      Ordering scanOrder,
      @Nullable Filter timeFilter,
      @Nullable Filter valueFilter,
      long limit,
      long offset,
      TRegionReplicaSet dataRegionReplicaSet) {
    this(id, seriesPath, scanOrder);
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
    this.limit = limit;
    this.offset = offset;
    this.regionReplicaSet = dataRegionReplicaSet;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public void open() throws Exception {}

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet dataRegion) {
    this.regionReplicaSet = dataRegion;
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

  public Ordering getScanOrder() {
    return scanOrder;
  }

  public void setScanOrder(Ordering scanOrder) {
    this.scanOrder = scanOrder;
  }

  public MeasurementPath getSeriesPath() {
    return seriesPath;
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
    throw new UnsupportedOperationException("no child is allowed for SeriesScanNode");
  }

  @Override
  public PlanNode clone() {
    return new SeriesScanNode(
        getPlanNodeId(),
        getSeriesPath(),
        getScanOrder(),
        getTimeFilter(),
        getValueFilter(),
        getLimit(),
        getOffset(),
        this.regionReplicaSet);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return ImmutableList.of(seriesPath.getFullPath());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSeriesScan(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SERIES_SCAN.serialize(byteBuffer);
    seriesPath.serialize(byteBuffer);
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
    PlanNodeType.SERIES_SCAN.serialize(stream);
    seriesPath.serialize(stream);
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

  public static SeriesScanNode deserialize(ByteBuffer byteBuffer) {
    MeasurementPath partialPath = (MeasurementPath) PathDeserializeUtil.deserialize(byteBuffer);
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
    return new SeriesScanNode(
        planNodeId, partialPath, scanOrder, timeFilter, valueFilter, limit, offset, null);
  }

  @Override
  public String toString() {
    return String.format(
        "SeriesScanNode-%s:[SeriesPath: %s, DataRegion: %s]",
        this.getPlanNodeId(),
        this.getSeriesPath(),
        PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
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
    SeriesScanNode that = (SeriesScanNode) o;
    return limit == that.limit
        && offset == that.offset
        && seriesPath.equals(that.seriesPath)
        && scanOrder == that.scanOrder
        && Objects.equals(timeFilter, that.timeFilter)
        && Objects.equals(valueFilter, that.valueFilter)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        seriesPath,
        scanOrder,
        timeFilter,
        valueFilter,
        limit,
        offset,
        regionReplicaSet);
  }

  @Override
  public PartialPath getPartitionPath() {
    return seriesPath;
  }

  @Override
  public Filter getPartitionTimeFilter() {
    return timeFilter;
  }
}
