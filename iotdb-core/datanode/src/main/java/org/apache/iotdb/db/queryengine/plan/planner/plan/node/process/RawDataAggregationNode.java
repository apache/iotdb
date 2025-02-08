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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode.getDeduplicatedDescriptors;

public class RawDataAggregationNode extends SingleChildProcessNode {

  // The list of aggregate functions, each AggregateDescriptor will be output as one or two column
  // of result TsBlock
  protected List<AggregationDescriptor> aggregationDescriptorList;

  // The parameter of `group by time`.
  // Its value will be null if there is no `group by time` clause.
  @Nullable protected GroupByTimeParameter groupByTimeParameter;

  // The parameter of `group by`.
  // Its value will be null if there is no `group by` clause.
  @Nullable protected GroupByParameter groupByParameter;

  // In some situation of `group by` clause, groupByExpression is required.
  // It will be null if the clause doesn't refer to any expression.
  protected Expression groupByExpression;

  protected Ordering scanOrder;

  protected boolean outputEndTime = false;

  public RawDataAggregationNode(
      PlanNodeId id,
      List<AggregationDescriptor> aggregationDescriptorList,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder) {
    super(id);
    this.aggregationDescriptorList = getDeduplicatedDescriptors(aggregationDescriptorList);
    this.groupByTimeParameter = groupByTimeParameter;
    this.scanOrder = scanOrder;
  }

  public RawDataAggregationNode(
      PlanNodeId id,
      PlanNode child,
      List<AggregationDescriptor> aggregationDescriptorList,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder) {
    super(id, child);
    this.aggregationDescriptorList = getDeduplicatedDescriptors(aggregationDescriptorList);
    this.groupByTimeParameter = groupByTimeParameter;
    this.scanOrder = scanOrder;
  }

  public RawDataAggregationNode(
      PlanNodeId id,
      List<AggregationDescriptor> aggregationDescriptorList,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      @Nullable GroupByParameter groupByParameter,
      Expression groupByExpression,
      boolean outputEndTime,
      Ordering scanOrder) {
    super(id);
    this.aggregationDescriptorList = getDeduplicatedDescriptors(aggregationDescriptorList);
    this.groupByTimeParameter = groupByTimeParameter;
    this.scanOrder = scanOrder;
    this.groupByParameter = groupByParameter;
    this.groupByExpression = groupByExpression;
    this.outputEndTime = outputEndTime;
  }

  public RawDataAggregationNode(
      PlanNodeId id,
      PlanNode child,
      List<AggregationDescriptor> aggregationDescriptorList,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      @Nullable GroupByParameter groupByParameter,
      Expression groupByExpression,
      boolean outputEndTime,
      Ordering scanOrder) {
    super(id, child);
    this.aggregationDescriptorList = getDeduplicatedDescriptors(aggregationDescriptorList);
    this.scanOrder = scanOrder;
    this.groupByParameter = groupByParameter;
    this.groupByTimeParameter = groupByTimeParameter;
    this.groupByExpression = groupByExpression;
    this.outputEndTime = outputEndTime;
  }

  // to avoid the repeated invoking of getDeduplicatedDescriptors method
  public RawDataAggregationNode(
      PlanNodeId id,
      PlanNode child,
      List<AggregationDescriptor> deduplicatedAggregationDescriptorList,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      @Nullable GroupByParameter groupByParameter,
      Expression groupByExpression,
      boolean outputEndTime,
      Ordering scanOrder,
      boolean useDeduplicatedDescriptors) {
    super(id, child);
    this.aggregationDescriptorList = deduplicatedAggregationDescriptorList;
    this.scanOrder = scanOrder;
    this.groupByParameter = groupByParameter;
    this.groupByTimeParameter = groupByTimeParameter;
    this.groupByExpression = groupByExpression;
    this.outputEndTime = outputEndTime;
  }

  public List<AggregationDescriptor> getAggregationDescriptorList() {
    return aggregationDescriptorList;
  }

  @Nullable
  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  @Nullable
  public GroupByParameter getGroupByParameter() {
    return groupByParameter;
  }

  public Ordering getScanOrder() {
    return scanOrder;
  }

  public boolean isOutputEndTime() {
    return outputEndTime;
  }

  public void setOutputEndTime(boolean outputEndTime) {
    this.outputEndTime = outputEndTime;
  }

  @Nullable
  public Expression getGroupByExpression() {
    return groupByExpression;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.RAW_DATA_AGGREGATION;
  }

  @Override
  public PlanNode clone() {
    return new RawDataAggregationNode(
        getPlanNodeId(),
        getAggregationDescriptorList(),
        getGroupByTimeParameter(),
        getGroupByParameter(),
        getGroupByExpression(),
        outputEndTime,
        getScanOrder());
  }

  @Override
  public List<String> getOutputColumnNames() {
    List<String> outputColumnNames = new ArrayList<>();
    if (outputEndTime) {
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
    return visitor.visitRawDataAggregation(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.RAW_DATA_AGGREGATION.serialize(byteBuffer);
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
    if (groupByParameter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      groupByParameter.serialize(byteBuffer);
    }
    if (groupByExpression == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      Expression.serialize(groupByExpression, byteBuffer);
    }
    ReadWriteIOUtils.write(outputEndTime, byteBuffer);
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.RAW_DATA_AGGREGATION.serialize(stream);
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
    if (groupByParameter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      groupByParameter.serialize(stream);
    }
    if (groupByExpression == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      Expression.serialize(groupByExpression, stream);
    }
    ReadWriteIOUtils.write(outputEndTime, stream);
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
  }

  public static RawDataAggregationNode deserialize(ByteBuffer byteBuffer) {
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
    isNull = ReadWriteIOUtils.readByte(byteBuffer);
    GroupByParameter groupByParameter = null;
    if (isNull == 1) {
      groupByParameter = GroupByParameter.deserialize(byteBuffer);
    }
    isNull = ReadWriteIOUtils.readByte(byteBuffer);
    Expression groupByExpression = null;
    if (isNull == 1) {
      groupByExpression = Expression.deserialize(byteBuffer);
    }
    boolean outputEndTime = ReadWriteIOUtils.readBool(byteBuffer);
    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new RawDataAggregationNode(
        planNodeId,
        aggregationDescriptorList,
        groupByTimeParameter,
        groupByParameter,
        groupByExpression,
        outputEndTime,
        scanOrder);
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
    RawDataAggregationNode that = (RawDataAggregationNode) o;
    return Objects.equals(aggregationDescriptorList, that.aggregationDescriptorList)
        && Objects.equals(groupByTimeParameter, that.groupByTimeParameter)
        && Objects.equals(groupByParameter, that.groupByParameter)
        && Objects.equals(groupByExpression, that.groupByExpression)
        && Objects.equals(outputEndTime, that.outputEndTime)
        && scanOrder == that.scanOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        aggregationDescriptorList,
        groupByTimeParameter,
        groupByParameter,
        groupByExpression,
        outputEndTime,
        scanOrder);
  }

  public String toString() {
    return String.format("RawDataAggregationNode-%s", getPlanNodeId());
  }
}
