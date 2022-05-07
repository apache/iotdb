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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class GroupByTimeNode extends ProcessNode {

  // The list of aggregate functions, each AggregateDescriptor will be output as one column of
  // result TsBlock
  private final List<AggregationDescriptor> aggregationDescriptorList;

  // The parameter of `group by time`.
  // Its value will be null if there is no `group by time` clause.
  @Nullable private final GroupByTimeParameter groupByTimeParameter;

  private List<PlanNode> children;

  public GroupByTimeNode(
      PlanNodeId id,
      List<AggregationDescriptor> aggregationDescriptorList,
      @Nullable GroupByTimeParameter groupByTimeParameter) {
    super(id);
    this.aggregationDescriptorList = aggregationDescriptorList;
    this.groupByTimeParameter = groupByTimeParameter;
    this.children = new ArrayList<>();
  }

  public GroupByTimeNode(
      PlanNodeId id,
      List<PlanNode> children,
      List<AggregationDescriptor> aggregationDescriptorList,
      GroupByTimeParameter groupByTimeParameter) {
    this(id, aggregationDescriptorList, groupByTimeParameter);
    this.children = children;
  }

  public List<AggregationDescriptor> getAggregationDescriptorList() {
    return aggregationDescriptorList;
  }

  @Nullable
  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    this.children.add(child);
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public PlanNode clone() {
    return new GroupByTimeNode(
        getPlanNodeId(), getAggregationDescriptorList(), getGroupByTimeParameter());
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
    return visitor.visitGroupByTime(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.GROUP_BY_TIME.serialize(byteBuffer);
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
  }

  public static GroupByTimeNode deserialize(ByteBuffer byteBuffer) {
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
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new GroupByTimeNode(planNodeId, aggregationDescriptorList, groupByTimeParameter);
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
    GroupByTimeNode that = (GroupByTimeNode) o;
    return Objects.equals(aggregationDescriptorList, that.aggregationDescriptorList)
        && Objects.equals(groupByTimeParameter, that.groupByTimeParameter)
        && Objects.equals(children, that.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), aggregationDescriptorList, groupByTimeParameter, children);
  }
}
