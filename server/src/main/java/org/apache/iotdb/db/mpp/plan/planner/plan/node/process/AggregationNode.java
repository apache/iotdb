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
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderBy;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This node is used to aggregate required series from multiple sources. The source data will be
 * input as a TsBlock, it may be raw data or partial aggregation result. This node will output the
 * final series aggregated result represented by TsBlock.
 */
public class AggregationNode extends MultiChildNode {

  // The list of aggregate functions, each AggregateDescriptor will be output as one or two column
  // of
  // result TsBlock
  protected List<AggregationDescriptor> aggregationDescriptorList;

  // The parameter of `group by time`.
  // Its value will be null if there is no `group by time` clause.
  @Nullable protected GroupByTimeParameter groupByTimeParameter;

  protected OrderBy scanOrder;

  public AggregationNode(
      PlanNodeId id,
      List<AggregationDescriptor> aggregationDescriptorList,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      OrderBy scanOrder) {
    super(id, new ArrayList<>());
    this.aggregationDescriptorList = getDeduplicatedDescriptors(aggregationDescriptorList);
    this.groupByTimeParameter = groupByTimeParameter;
    this.scanOrder = scanOrder;
  }

  public AggregationNode(
      PlanNodeId id,
      List<PlanNode> children,
      List<AggregationDescriptor> aggregationDescriptorList,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      OrderBy scanOrder) {
    this(id, aggregationDescriptorList, groupByTimeParameter, scanOrder);
    this.children = children;
  }

  public List<AggregationDescriptor> getAggregationDescriptorList() {
    return aggregationDescriptorList;
  }

  @Nullable
  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  public OrderBy getScanOrder() {
    return scanOrder;
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
    return new AggregationNode(
        getPlanNodeId(), getAggregationDescriptorList(), getGroupByTimeParameter(), getScanOrder());
  }

  @Override
  public List<String> getOutputColumnNames() {
    return aggregationDescriptorList.stream()
        .map(AggregationDescriptor::getOutputColumnNames)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  public void setAggregationDescriptorList(List<AggregationDescriptor> aggregationDescriptorList) {
    this.aggregationDescriptorList = aggregationDescriptorList;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitAggregation(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.AGGREGATE.serialize(byteBuffer);
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
    PlanNodeType.AGGREGATE.serialize(stream);
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

  public static AggregationNode deserialize(ByteBuffer byteBuffer) {
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
    OrderBy scanOrder = OrderBy.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AggregationNode(
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
    AggregationNode that = (AggregationNode) o;
    return Objects.equals(aggregationDescriptorList, that.aggregationDescriptorList)
        && Objects.equals(groupByTimeParameter, that.groupByTimeParameter)
        && scanOrder == that.scanOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), aggregationDescriptorList, groupByTimeParameter, scanOrder);
  }

  /**
   * If aggregation function COUNT and AVG for one time series appears at the same time, and outputs
   * intermediate result, the output columns will be like | COUNT | COUNT | SUM |. In this
   * situation, one COUNT column is not needed. Therefore, when COUNT(or SUM) appears with AVG and
   * outputs intermediate result(if they output final result, they will be all necessary), we need
   * to REMOVE the COUNT aggregation, and only keep AVG function no matter their appearing order.
   *
   * <p>The related functions include AVG(COUNT AND SUM), FIRST_VALUE(FIRST_VALUE AND MIN_TIME),
   * LAST_VALUE(LAST_VALUE AND MAX_TIME).
   */
  public static List<AggregationDescriptor> getDeduplicatedDescriptors(
      List<AggregationDescriptor> aggregationDescriptors) {
    Map<String, Integer> columnToIndexMap = new HashMap<>();
    boolean[] removedIndexes = new boolean[aggregationDescriptors.size()];
    for (int i = 0; i < aggregationDescriptors.size(); i++) {
      AggregationDescriptor descriptor = aggregationDescriptors.get(i);
      if (descriptor.getStep().isOutputPartial()) {
        List<String> outputColumnNames = descriptor.getOutputColumnNames();
        for (String outputColumn : outputColumnNames) {
          // if encountering repeated column
          if (columnToIndexMap.containsKey(outputColumn)) {
            // if self is double outputs, then remove the former, else remove self
            if (outputColumnNames.size() == 2) {
              removedIndexes[columnToIndexMap.get(outputColumn)] = true;
            } else {
              removedIndexes[i] = true;
            }
          } else {
            columnToIndexMap.put(outputColumn, i);
          }
        }
      }
    }
    List<AggregationDescriptor> deduplicatedDescriptors = new ArrayList<>();
    for (int i = 0; i < aggregationDescriptors.size(); i++) {
      if (!removedIndexes[i]) {
        deduplicatedDescriptors.add(aggregationDescriptors.get(i));
      }
    }
    return deduplicatedDescriptors;
  }

  public String toString() {
    return String.format("AggregationNode-%s", getPlanNodeId());
  }
}
