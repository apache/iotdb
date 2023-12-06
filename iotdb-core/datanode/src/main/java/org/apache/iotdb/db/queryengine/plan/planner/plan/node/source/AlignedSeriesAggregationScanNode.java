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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
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

public class AlignedSeriesAggregationScanNode extends SeriesAggregationSourceNode {

  // The paths of the target series which will be aggregated.
  private final AlignedPath alignedPath;

  // The id of DataRegion where the node will run
  private TRegionReplicaSet regionReplicaSet;

  public AlignedSeriesAggregationScanNode(
      PlanNodeId id,
      AlignedPath alignedPath,
      List<AggregationDescriptor> aggregationDescriptorList) {
    super(id, aggregationDescriptorList);
    this.alignedPath = alignedPath;
    this.aggregationDescriptorList =
        AggregationNode.getDeduplicatedDescriptors(aggregationDescriptorList);
  }

  public AlignedSeriesAggregationScanNode(
      PlanNodeId id,
      AlignedPath alignedPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      Ordering scanOrder,
      @Nullable GroupByTimeParameter groupByTimeParameter) {
    this(id, alignedPath, aggregationDescriptorList);
    this.scanOrder = scanOrder;
    this.groupByTimeParameter = groupByTimeParameter;
  }

  public AlignedSeriesAggregationScanNode(
      PlanNodeId id,
      AlignedPath alignedPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      Ordering scanOrder,
      @Nullable Expression pushDownPredicate,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      TRegionReplicaSet dataRegionReplicaSet) {
    this(id, alignedPath, aggregationDescriptorList, scanOrder, groupByTimeParameter);
    this.pushDownPredicate = pushDownPredicate;
    this.regionReplicaSet = dataRegionReplicaSet;
  }

  public AlignedSeriesAggregationScanNode(
      PlanNodeId id,
      AlignedPath alignedPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      Ordering scanOrder,
      boolean outputEndTime,
      @Nullable Expression pushDownPredicate,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      TRegionReplicaSet dataRegionReplicaSet) {
    this(
        id,
        alignedPath,
        aggregationDescriptorList,
        scanOrder,
        pushDownPredicate,
        groupByTimeParameter,
        dataRegionReplicaSet);
    setOutputEndTime(outputEndTime);
  }

  public AlignedPath getAlignedPath() {
    return alignedPath;
  }

  @Override
  public Ordering getScanOrder() {
    return scanOrder;
  }

  @Override
  public void open() throws Exception {
    // Do nothing
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public void setRegionReplicaSet(TRegionReplicaSet regionReplicaSet) {
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public void close() throws Exception {
    // Do nothing
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
        isOutputEndTime(),
        getPushDownPredicate(),
        getGroupByTimeParameter(),
        getRegionReplicaSet());
  }

  @Override
  public List<String> getOutputColumnNames() {
    List<String> outputColumnNames = new ArrayList<>();
    if (isOutputEndTime()) {
      outputColumnNames.add(ColumnHeaderConstant.ENDTIME);
    }
    outputColumnNames.addAll(
        aggregationDescriptorList.stream()
            .map(AggregationDescriptor::getOutputColumnNames)
            .flatMap(List::stream)
            .collect(Collectors.toList()));
    return outputColumnNames;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAlignedSeriesAggregationScan(this, context);
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
    ReadWriteIOUtils.write(isOutputEndTime(), byteBuffer);
    if (pushDownPredicate == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      Expression.serialize(pushDownPredicate, byteBuffer);
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
    PlanNodeType.ALIGNED_SERIES_AGGREGATE_SCAN.serialize(stream);
    alignedPath.serialize(stream);
    ReadWriteIOUtils.write(aggregationDescriptorList.size(), stream);
    for (AggregationDescriptor aggregationDescriptor : aggregationDescriptorList) {
      aggregationDescriptor.serialize(stream);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
    ReadWriteIOUtils.write(isOutputEndTime(), stream);
    if (pushDownPredicate == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      Expression.serialize(pushDownPredicate, stream);
    }
    if (groupByTimeParameter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      groupByTimeParameter.serialize(stream);
    }
  }

  public static AlignedSeriesAggregationScanNode deserialize(ByteBuffer byteBuffer) {
    AlignedPath alignedPath = (AlignedPath) PathDeserializeUtil.deserialize(byteBuffer);
    int aggregateDescriptorSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<AggregationDescriptor> aggregationDescriptorList = new ArrayList<>();
    for (int i = 0; i < aggregateDescriptorSize; i++) {
      aggregationDescriptorList.add(AggregationDescriptor.deserialize(byteBuffer));
    }
    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    boolean outputEndTime = ReadWriteIOUtils.readBool(byteBuffer);
    byte isNull = ReadWriteIOUtils.readByte(byteBuffer);
    Expression pushDownPredicate = null;
    if (isNull == 1) {
      pushDownPredicate = Expression.deserialize(byteBuffer);
    }
    isNull = ReadWriteIOUtils.readByte(byteBuffer);
    GroupByTimeParameter groupByTimeParameter = null;
    if (isNull == 1) {
      groupByTimeParameter = GroupByTimeParameter.deserialize(byteBuffer);
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AlignedSeriesAggregationScanNode(
        planNodeId,
        alignedPath,
        aggregationDescriptorList,
        scanOrder,
        outputEndTime,
        pushDownPredicate,
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
    AlignedSeriesAggregationScanNode that = (AlignedSeriesAggregationScanNode) o;
    return alignedPath.equals(that.alignedPath)
        && Objects.equals(regionReplicaSet, that.regionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), alignedPath, regionReplicaSet);
  }

  @Override
  public PartialPath getPartitionPath() {
    return getAlignedPath();
  }

  @Override
  public String toString() {
    return String.format(
        "AlignedSeriesAggregationScanNode-%s:[SeriesPath: %s, Descriptor: %s, DataRegion: %s]",
        this.getPlanNodeId(),
        this.getAlignedPath().getFormattedString(),
        this.getAggregationDescriptorList(),
        PlanNodeUtil.printRegionReplicaSet(getRegionReplicaSet()));
  }
}
