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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
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
import java.util.stream.Collectors;

/**
 * This node is responsible to do the aggregation calculation for one series. It will read the
 * target series and calculate the aggregation result by the aggregation digest or raw data of this
 * series.
 *
 * <p>The aggregation result will be represented as a TsBlock
 *
 * <p>This node will split data of the target series into many groups by time range and do the
 * aggregation calculation for each group. Each result will be one row of the result TsBlock. The
 * timestamp of each row is the start time of the time range group.
 *
 * <p>If there is no time range split parameter, the result TsBlock will only contain one row, which
 * represent the whole aggregation result of this series. And the timestamp will be 0, which is
 * meaningless.
 */
public class SeriesAggregationScanNode extends SeriesAggregationSourceNode {

  // The path of the target series which will be aggregated.
  private final MeasurementPath seriesPath;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;

  public SeriesAggregationScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      List<AggregationDescriptor> aggregationDescriptorList) {
    super(id, AggregationNode.getDeduplicatedDescriptors(aggregationDescriptorList));
    this.seriesPath = seriesPath;
  }

  public SeriesAggregationScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      Ordering scanOrder,
      @Nullable GroupByTimeParameter groupByTimeParameter) {
    this(id, seriesPath, aggregationDescriptorList);
    this.scanOrder = scanOrder;
    this.groupByTimeParameter = groupByTimeParameter;
  }

  public SeriesAggregationScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      Ordering scanOrder,
      @Nullable Filter timeFilter,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      TRegionReplicaSet dataRegionReplicaSet) {
    this(id, seriesPath, aggregationDescriptorList, scanOrder, groupByTimeParameter);
    this.timeFilter = timeFilter;
    this.regionReplicaSet = dataRegionReplicaSet;
  }

  @Override
  public Ordering getScanOrder() {
    return scanOrder;
  }

  @Override
  @Nullable
  public Filter getTimeFilter() {
    return timeFilter;
  }

  public void setTimeFilter(@Nullable Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  @Override
  @Nullable
  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  public MeasurementPath getSeriesPath() {
    return seriesPath;
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
    throw new UnsupportedOperationException("no child is allowed for SeriesAggregateScanNode");
  }

  @Override
  public PlanNode clone() {
    return new SeriesAggregationScanNode(
        getPlanNodeId(),
        getSeriesPath(),
        getAggregationDescriptorList(),
        getScanOrder(),
        getTimeFilter(),
        getGroupByTimeParameter(),
        getRegionReplicaSet());
  }

  @Override
  public List<String> getOutputColumnNames() {
    return aggregationDescriptorList.stream()
        .map(AggregationDescriptor::getOutputColumnNames)
        .flatMap(List::stream)
        .collect(Collectors.toList());
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
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitSeriesAggregationScan(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SERIES_AGGREGATE_SCAN.serialize(byteBuffer);
    seriesPath.serialize(byteBuffer);
    ReadWriteIOUtils.write(aggregationDescriptorList.size(), byteBuffer);
    for (AggregationDescriptor aggregationDescriptor : aggregationDescriptorList) {
      aggregationDescriptor.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
    if (timeFilter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      timeFilter.serialize(byteBuffer);
    }
    if (groupByTimeParameter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      groupByTimeParameter.serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.SERIES_AGGREGATE_SCAN.serialize(stream);
    seriesPath.serialize(stream);
    ReadWriteIOUtils.write(aggregationDescriptorList.size(), stream);
    for (AggregationDescriptor aggregationDescriptor : aggregationDescriptorList) {
      aggregationDescriptor.serialize(stream);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
    if (timeFilter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      timeFilter.serialize(stream);
    }
    if (groupByTimeParameter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      groupByTimeParameter.serialize(stream);
    }
  }

  public static SeriesAggregationScanNode deserialize(ByteBuffer byteBuffer) {
    MeasurementPath partialPath = (MeasurementPath) PathDeserializeUtil.deserialize(byteBuffer);
    int aggregateDescriptorSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<AggregationDescriptor> aggregationDescriptorList = new ArrayList<>();
    for (int i = 0; i < aggregateDescriptorSize; i++) {
      aggregationDescriptorList.add(AggregationDescriptor.deserialize(byteBuffer));
    }
    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    byte isNull = ReadWriteIOUtils.readByte(byteBuffer);
    Filter timeFilter = null;
    if (isNull == 1) {
      timeFilter = FilterFactory.deserialize(byteBuffer);
    }
    isNull = ReadWriteIOUtils.readByte(byteBuffer);
    GroupByTimeParameter groupByTimeParameter = null;
    if (isNull == 1) {
      groupByTimeParameter = GroupByTimeParameter.deserialize(byteBuffer);
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SeriesAggregationScanNode(
        planNodeId,
        partialPath,
        aggregationDescriptorList,
        scanOrder,
        timeFilter,
        groupByTimeParameter,
        null);
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
    SeriesAggregationScanNode that = (SeriesAggregationScanNode) o;
    return seriesPath.equals(that.seriesPath)
        && aggregationDescriptorList.equals(that.aggregationDescriptorList)
        && scanOrder == that.scanOrder
        && Objects.equals(timeFilter, that.timeFilter)
        && Objects.equals(groupByTimeParameter, that.groupByTimeParameter)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        seriesPath,
        aggregationDescriptorList,
        scanOrder,
        timeFilter,
        groupByTimeParameter,
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

  @Override
  public String toString() {
    return String.format(
        "SeriesAggregationScanNode-%s:[SeriesPath: %s, Descriptor: %s, DataRegion: %s]",
        this.getPlanNodeId(),
        this.getSeriesPath(),
        this.getAggregationDescriptorList(),
        PlanNodeUtil.printRegionReplicaSet(this.getRegionReplicaSet()));
  }
}
