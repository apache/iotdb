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
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
public class SeriesAggregationScanNode extends SourceNode {

  // The path of the target series which will be aggregated.
  private final MeasurementPath seriesPath;

  // The list of aggregate functions, each AggregateDescriptor will be output as one column in
  // result TsBlock
  private final List<AggregationDescriptor> aggregationDescriptorList;

  // all the sensors in seriesPath's device of current query
  private final Set<String> allSensors;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  private OrderBy scanOrder = OrderBy.TIMESTAMP_ASC;

  // time filter for current series, could be null if doesn't exist
  @Nullable private Filter timeFilter;

  // The parameter of `group by time`
  // Its value will be null if there is no `group by time` clause,
  @Nullable private GroupByTimeParameter groupByTimeParameter;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;

  public SeriesAggregationScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      Set<String> allSensors) {
    super(id);
    this.seriesPath = seriesPath;
    this.allSensors = allSensors;
    this.aggregationDescriptorList = aggregationDescriptorList;
  }

  public SeriesAggregationScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      Set<String> allSensors,
      OrderBy scanOrder) {
    this(id, seriesPath, aggregationDescriptorList, allSensors);
    this.scanOrder = scanOrder;
  }

  public SeriesAggregationScanNode(
      PlanNodeId id,
      MeasurementPath seriesPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      Set<String> allSensors,
      OrderBy scanOrder,
      @Nullable Filter timeFilter,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      TRegionReplicaSet dataRegionReplicaSet) {
    this(id, seriesPath, aggregationDescriptorList, allSensors, scanOrder);
    this.timeFilter = timeFilter;
    this.groupByTimeParameter = groupByTimeParameter;
    this.regionReplicaSet = dataRegionReplicaSet;
  }

  public OrderBy getScanOrder() {
    return scanOrder;
  }

  public Set<String> getAllSensors() {
    return allSensors;
  }

  @Nullable
  public Filter getTimeFilter() {
    return timeFilter;
  }

  @Nullable
  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  public MeasurementPath getSeriesPath() {
    return seriesPath;
  }

  public List<AggregationDescriptor> getAggregationDescriptorList() {
    return aggregationDescriptorList;
  }

  public List<AggregationType> getAggregateFuncList() {
    return aggregationDescriptorList.stream()
        .map(AggregationDescriptor::getAggregationType)
        .collect(Collectors.toList());
  }

  public void setTimeFilter(Filter timeFilter) {
    this.timeFilter = timeFilter;
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
        getAllSensors(),
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
    return visitor.visitSeriesAggregate(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SERIES_AGGREGATE_SCAN.serialize(byteBuffer);
    seriesPath.serialize(byteBuffer);
    ReadWriteIOUtils.write(aggregationDescriptorList.size(), byteBuffer);
    for (AggregationDescriptor aggregationDescriptor : aggregationDescriptorList) {
      aggregationDescriptor.serialize(byteBuffer);
    }
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
    if (groupByTimeParameter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      groupByTimeParameter.serialize(byteBuffer);
    }
    ThriftCommonsSerDeUtils.writeTRegionReplicaSet(regionReplicaSet, byteBuffer);
  }

  public static SeriesAggregationScanNode deserialize(ByteBuffer byteBuffer) {
    MeasurementPath partialPath = (MeasurementPath) PathDeserializeUtil.deserialize(byteBuffer);
    int aggregateDescriptorSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<AggregationDescriptor> aggregationDescriptorList = new ArrayList<>();
    for (int i = 0; i < aggregateDescriptorSize; i++) {
      aggregationDescriptorList.add(AggregationDescriptor.deserialize(byteBuffer));
    }
    int allSensorsSize = ReadWriteIOUtils.readInt(byteBuffer);
    Set<String> allSensors = new HashSet<>();
    for (int i = 0; i < allSensorsSize; i++) {
      allSensors.add(ReadWriteIOUtils.readString(byteBuffer));
    }
    OrderBy scanOrder = OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
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
    TRegionReplicaSet regionReplicaSet = ThriftCommonsSerDeUtils.readTRegionReplicaSet(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SeriesAggregationScanNode(
        planNodeId,
        partialPath,
        aggregationDescriptorList,
        allSensors,
        scanOrder,
        timeFilter,
        groupByTimeParameter,
        regionReplicaSet);
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
        && allSensors.equals(that.allSensors)
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
        allSensors,
        scanOrder,
        timeFilter,
        groupByTimeParameter,
        regionReplicaSet);
  }
}
