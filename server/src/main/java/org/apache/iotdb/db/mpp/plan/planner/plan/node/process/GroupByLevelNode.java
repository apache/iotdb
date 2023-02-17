/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This node is responsible for the final aggregation merge operation. It will process the data from
 * TsBlock row by row. For one row, it will rollup the fields which have the same aggregate function
 * and belong to one bucket. Here, that two columns belong to one bucket means the partial paths of
 * device after rolling up in specific level are the same.
 *
 * <p>For example, let's say there are two columns `root.sg.d1.s1` and `root.sg.d2.s1`.
 *
 * <p>If the group by level parameter is [0, 1], then these two columns will belong to one bucket
 * and the bucket name is `root.sg.*.s1`.
 *
 * <p>If the group by level parameter is [0, 2], then these two columns will not belong to one
 * bucket. And the total buckets are `root.*.d1.s1` and `root.*.d2.s1`
 */
public class GroupByLevelNode extends MultiChildProcessNode {

  // The list of aggregate descriptors
  // each GroupByLevelDescriptor will be output as one or two column of result TsBlock
  protected List<CrossSeriesAggregationDescriptor> groupByLevelDescriptors;

  // The parameter of `group by time`.
  // Its value will be null if there is no `group by time` clause.
  @Nullable protected GroupByTimeParameter groupByTimeParameter;

  protected Ordering scanOrder;

  public GroupByLevelNode(
      PlanNodeId id,
      List<PlanNode> children,
      List<CrossSeriesAggregationDescriptor> groupByLevelDescriptors,
      GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder) {
    super(id, children);
    this.groupByLevelDescriptors = groupByLevelDescriptors;
    this.groupByTimeParameter = groupByTimeParameter;
    this.scanOrder = scanOrder;
  }

  public GroupByLevelNode(
      PlanNodeId id,
      List<CrossSeriesAggregationDescriptor> groupByLevelDescriptors,
      GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder) {
    super(id);
    this.groupByLevelDescriptors = groupByLevelDescriptors;
    this.groupByTimeParameter = groupByTimeParameter;
    this.scanOrder = scanOrder;
  }

  @Override
  public PlanNode clone() {
    return new GroupByLevelNode(
        getPlanNodeId(), getGroupByLevelDescriptors(), this.groupByTimeParameter, this.scanOrder);
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new HorizontallyConcatNode(
        new PlanNodeId(String.format("%s-%s", getPlanNodeId(), subNodeId)),
        new ArrayList<>(children.subList(startIndex, endIndex)));
  }

  public List<CrossSeriesAggregationDescriptor> getGroupByLevelDescriptors() {
    return groupByLevelDescriptors;
  }

  public void setGroupByLevelDescriptors(
      List<CrossSeriesAggregationDescriptor> groupByLevelDescriptors) {
    this.groupByLevelDescriptors = groupByLevelDescriptors;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return groupByLevelDescriptors.stream()
        .map(AggregationDescriptor::getOutputColumnNames)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitGroupByLevel(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.GROUP_BY_LEVEL.serialize(byteBuffer);
    ReadWriteIOUtils.write(groupByLevelDescriptors.size(), byteBuffer);
    for (CrossSeriesAggregationDescriptor groupByLevelDescriptor : groupByLevelDescriptors) {
      groupByLevelDescriptor.serialize(byteBuffer);
    }
    if (groupByTimeParameter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      groupByTimeParameter.serialize(byteBuffer);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.GROUP_BY_LEVEL.serialize(stream);
    ReadWriteIOUtils.write(groupByLevelDescriptors.size(), stream);
    for (CrossSeriesAggregationDescriptor groupByLevelDescriptor : groupByLevelDescriptors) {
      groupByLevelDescriptor.serialize(stream);
    }
    if (groupByTimeParameter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      groupByTimeParameter.serialize(stream);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
  }

  public static GroupByLevelNode deserialize(ByteBuffer byteBuffer) {
    int descriptorSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<CrossSeriesAggregationDescriptor> groupByLevelDescriptors = new ArrayList<>();
    while (descriptorSize > 0) {
      groupByLevelDescriptors.add(CrossSeriesAggregationDescriptor.deserialize(byteBuffer));
      descriptorSize--;
    }
    byte isNull = ReadWriteIOUtils.readByte(byteBuffer);
    GroupByTimeParameter groupByTimeParameter = null;
    if (isNull == 1) {
      groupByTimeParameter = GroupByTimeParameter.deserialize(byteBuffer);
    }
    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new GroupByLevelNode(
        planNodeId, groupByLevelDescriptors, groupByTimeParameter, scanOrder);
  }

  @Nullable
  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  public Ordering getScanOrder() {
    return scanOrder;
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
    GroupByLevelNode that = (GroupByLevelNode) o;
    return Objects.equals(groupByLevelDescriptors, that.groupByLevelDescriptors)
        && Objects.equals(groupByTimeParameter, that.groupByTimeParameter)
        && scanOrder == that.scanOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), groupByLevelDescriptors, groupByTimeParameter, scanOrder);
  }

  public String toString() {
    return String.format(
        "GroupByLevelNode-%s: Output: %s, Input: %s",
        getPlanNodeId(), getOutputColumnNames(), groupByLevelDescriptors.size());
  }
}
