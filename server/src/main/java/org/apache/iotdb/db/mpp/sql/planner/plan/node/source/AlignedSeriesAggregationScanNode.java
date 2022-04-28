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

package org.apache.iotdb.db.mpp.sql.planner.plan.node.source;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.sql.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AlignedSeriesAggregationScanNode extends SourceNode {

  // The paths of the target series which will be aggregated.
  private final AlignedPath alignedPath;

  // The list of aggregate functions, each AggregateDescriptor will be output as one column in
  // result TsBlock
  private final List<AggregationDescriptor> aggregationDescriptorList;

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

  public AlignedSeriesAggregationScanNode(
      PlanNodeId id,
      AlignedPath alignedPath,
      List<AggregationDescriptor> aggregationDescriptorList) {
    super(id);
    this.alignedPath = alignedPath;
    this.aggregationDescriptorList = aggregationDescriptorList;
  }

  public AlignedSeriesAggregationScanNode(
      PlanNodeId id,
      AlignedPath alignedPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      OrderBy scanOrder) {
    this(id, alignedPath, aggregationDescriptorList);
    this.scanOrder = scanOrder;
  }

  public AlignedSeriesAggregationScanNode(
      PlanNodeId id,
      AlignedPath alignedPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      OrderBy scanOrder,
      @Nullable Filter timeFilter,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      TRegionReplicaSet dataRegionReplicaSet) {
    this(id, alignedPath, aggregationDescriptorList, scanOrder);
    this.timeFilter = timeFilter;
    this.groupByTimeParameter = groupByTimeParameter;
    this.regionReplicaSet = dataRegionReplicaSet;
  }

  public AlignedPath getAlignedPath() {
    return alignedPath;
  }

  public List<AggregationDescriptor> getAggregationDescriptorList() {
    return aggregationDescriptorList;
  }

  public OrderBy getScanOrder() {
    return scanOrder;
  }

  @Nullable
  public Filter getTimeFilter() {
    return timeFilter;
  }

  @Nullable
  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
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
    throw new UnsupportedOperationException(
        "no child is allowed for AlignedSeriesAggregationScanNode");
  }

  @Override
  public PlanNode clone() {
    return new AlignedSeriesAggregationScanNode(
        getPlanNodeId(),
        getAlignedPath(),
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
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAlignedSeriesAggregate(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.ALIGNED_SERIES_AGGREGATE_SCAN.serialize(byteBuffer);
    alignedPath.serialize(byteBuffer);
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
    ThriftCommonsSerDeUtils.writeTRegionReplicaSet(regionReplicaSet, byteBuffer);
  }

  public static AlignedSeriesAggregationScanNode deserialize(ByteBuffer byteBuffer) {
    AlignedPath alignedPath = (AlignedPath) PathDeserializeUtil.deserialize(byteBuffer);
    int aggregateDescriptorSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<AggregationDescriptor> aggregationDescriptorList = new ArrayList<>();
    for (int i = 0; i < aggregateDescriptorSize; i++) {
      aggregationDescriptorList.add(AggregationDescriptor.deserialize(byteBuffer));
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
    return new AlignedSeriesAggregationScanNode(
        planNodeId,
        alignedPath,
        aggregationDescriptorList,
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
    AlignedSeriesAggregationScanNode that = (AlignedSeriesAggregationScanNode) o;
    return alignedPath.equals(that.alignedPath)
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
        alignedPath,
        aggregationDescriptorList,
        scanOrder,
        timeFilter,
        groupByTimeParameter,
        regionReplicaSet);
  }
}
