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
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * SeriesScanNode is responsible for read data a specific series. When reading data, the
 * SeriesScanNode can read the raw data batch by batch. And also, it can leverage the filter and
 * other info to decrease the result set.
 *
 * <p>Children type: no child is allowed for SeriesScanNode
 */
public class SeriesScanNode extends SourceNode {

  // The path of the target series which will be scanned.
  private final MeasurementPath seriesPath;

  // all the sensors in seriesPath's device of current query
  @Nonnull private final Set<String> allSensors;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  private OrderBy scanOrder = OrderBy.TIMESTAMP_ASC;

  // time filter for current series, could be null if doesn't exist
  @Nullable private Filter timeFilter;

  // value filter for current series, could be null if doesn't exist
  @Nullable private Filter valueFilter;

  // Limit for result set. The default value is -1, which means no limit
  private int limit;

  // offset for result set. The default value is 0
  private int offset;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;

  public SeriesScanNode(
      PlanNodeId id, MeasurementPath seriesPath, @Nonnull Set<String> allSensors) {
    super(id);
    this.seriesPath = seriesPath;
    this.allSensors = allSensors;
  }

  public SeriesScanNode(
      PlanNodeId id, MeasurementPath seriesPath, Set<String> allSensors, OrderBy scanOrder) {
    this(id, seriesPath, allSensors);
    this.scanOrder = scanOrder;
  }

  public SeriesScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      Set<String> allSensors,
      OrderBy scanOrder,
      @Nullable Filter timeFilter,
      @Nullable Filter valueFilter,
      int limit,
      int offset,
      TRegionReplicaSet dataRegionReplicaSet) {
    this(id, seriesPath, allSensors, scanOrder);
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

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  @Nonnull
  public Set<String> getAllSensors() {
    return allSensors;
  }

  public OrderBy getScanOrder() {
    return scanOrder;
  }

  public void setScanOrder(OrderBy scanOrder) {
    this.scanOrder = scanOrder;
  }

  public MeasurementPath getSeriesPath() {
    return seriesPath;
  }

  @Nullable
  public Filter getTimeFilter() {
    return timeFilter;
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
        getAllSensors(),
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
    ReadWriteIOUtils.write(allSensors.size(), byteBuffer);
    for (String sensor : allSensors) {
      ReadWriteIOUtils.write(sensor, byteBuffer);
    }
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
    ThriftCommonsSerDeUtils.writeTRegionReplicaSet(regionReplicaSet, byteBuffer);
  }

  public static SeriesScanNode deserialize(ByteBuffer byteBuffer) {
    MeasurementPath partialPath = (MeasurementPath) PathDeserializeUtil.deserialize(byteBuffer);
    int allSensorSize = ReadWriteIOUtils.readInt(byteBuffer);
    Set<String> allSensors = new HashSet<>();
    for (int i = 0; i < allSensorSize; i++) {
      allSensors.add(ReadWriteIOUtils.readString(byteBuffer));
    }
    OrderBy scanOrder = OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
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
    int limit = ReadWriteIOUtils.readInt(byteBuffer);
    int offset = ReadWriteIOUtils.readInt(byteBuffer);
    TRegionReplicaSet dataRegionReplicaSet =
        ThriftCommonsSerDeUtils.readTRegionReplicaSet(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SeriesScanNode(
        planNodeId,
        partialPath,
        allSensors,
        scanOrder,
        timeFilter,
        valueFilter,
        limit,
        offset,
        dataRegionReplicaSet);
  }

  @Override
  public String toString() {
    return String.format(
        "SeriesScanNode-%s:[SeriesPath: %s, DataRegion: %s]",
        this.getPlanNodeId(), this.getSeriesPath(), this.getRegionReplicaSet());
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
        && allSensors.equals(that.allSensors)
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
        allSensors,
        scanOrder,
        timeFilter,
        valueFilter,
        limit,
        offset,
        regionReplicaSet);
  }
}
