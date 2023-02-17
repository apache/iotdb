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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SlidingWindowAggregationNode extends SingleChildProcessNode {

  // The list of aggregate functions, each AggregateDescriptor will be output as one column of
  // result TsBlock
  private List<AggregationDescriptor> aggregationDescriptorList;

  // The parameter of `group by time`.
  private final GroupByTimeParameter groupByTimeParameter;

  protected Ordering scanOrder;

  public SlidingWindowAggregationNode(
      PlanNodeId id,
      List<AggregationDescriptor> aggregationDescriptorList,
      GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder) {
    super(id);
    this.aggregationDescriptorList = aggregationDescriptorList;
    this.groupByTimeParameter = groupByTimeParameter;
    this.scanOrder = scanOrder;
  }

  public SlidingWindowAggregationNode(
      PlanNodeId id,
      PlanNode child,
      List<AggregationDescriptor> aggregationDescriptorList,
      GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder) {
    super(id, child);
    this.aggregationDescriptorList = aggregationDescriptorList;
    this.groupByTimeParameter = groupByTimeParameter;
    this.scanOrder = scanOrder;
  }

  public List<AggregationDescriptor> getAggregationDescriptorList() {
    return aggregationDescriptorList;
  }

  public void setAggregationDescriptorList(List<AggregationDescriptor> aggregationDescriptorList) {
    this.aggregationDescriptorList = aggregationDescriptorList;
  }

  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  public Ordering getScanOrder() {
    return scanOrder;
  }

  @Override
  public PlanNode clone() {
    return new SlidingWindowAggregationNode(
        getPlanNodeId(), getAggregationDescriptorList(), getGroupByTimeParameter(), getScanOrder());
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
    return visitor.visitSlidingWindowAggregation(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.SLIDING_WINDOW_AGGREGATION.serialize(byteBuffer);
    ReadWriteIOUtils.write(aggregationDescriptorList.size(), byteBuffer);
    for (AggregationDescriptor aggregationDescriptor : aggregationDescriptorList) {
      aggregationDescriptor.serialize(byteBuffer);
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
    PlanNodeType.SLIDING_WINDOW_AGGREGATION.serialize(stream);
    ReadWriteIOUtils.write(aggregationDescriptorList.size(), stream);
    for (AggregationDescriptor aggregationDescriptor : aggregationDescriptorList) {
      aggregationDescriptor.serialize(stream);
    }
    if (groupByTimeParameter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      groupByTimeParameter.serialize(stream);
    }
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
  }

  public static SlidingWindowAggregationNode deserialize(ByteBuffer byteBuffer) {
    int descriptorSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<AggregationDescriptor> aggregationDescriptorList = new ArrayList<>();
    while (descriptorSize > 0) {
      aggregationDescriptorList.add(AggregationDescriptor.deserialize(byteBuffer));
      descriptorSize--;
    }
    byte isNull = ReadWriteIOUtils.readByte(byteBuffer);
    GroupByTimeParameter groupByTimeParameter = null;
    if (isNull == 1) {
      groupByTimeParameter = GroupByTimeParameter.deserialize(byteBuffer);
    }
    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new SlidingWindowAggregationNode(
        planNodeId, aggregationDescriptorList, groupByTimeParameter, scanOrder);
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
    SlidingWindowAggregationNode that = (SlidingWindowAggregationNode) o;
    return Objects.equals(aggregationDescriptorList, that.aggregationDescriptorList)
        && Objects.equals(groupByTimeParameter, that.groupByTimeParameter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), aggregationDescriptorList, groupByTimeParameter);
  }

  public String toString() {
    return String.format("SlidingWindowAggregationNode-%s", getPlanNodeId());
  }
}
