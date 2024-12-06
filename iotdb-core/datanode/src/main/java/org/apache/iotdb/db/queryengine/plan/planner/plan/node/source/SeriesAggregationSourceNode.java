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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class SeriesAggregationSourceNode extends SeriesSourceNode {

  // The list of aggregate functions, each AggregateDescriptor will be output as one column in
  // result TsBlock
  protected List<AggregationDescriptor> aggregationDescriptorList;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  protected Ordering scanOrder = Ordering.ASC;

  // push-downing query filter for current series, could be null if it doesn't exist
  @Nullable protected Expression pushDownPredicate;

  // The parameter of `group by time`
  // Its value will be null if there is no `group by time` clause,
  @Nullable protected GroupByTimeParameter groupByTimeParameter;

  // If the resultSet should contain 'endTime' column in GROUP BY TIME query.
  private boolean outputEndTime = false;

  protected SeriesAggregationSourceNode(
      PlanNodeId id, List<AggregationDescriptor> aggregationDescriptorList) {
    super(id);
    this.aggregationDescriptorList = aggregationDescriptorList;
  }

  public List<AggregationDescriptor> getAggregationDescriptorList() {
    return aggregationDescriptorList;
  }

  public void setAggregationDescriptorList(List<AggregationDescriptor> aggregationDescriptorList) {
    this.aggregationDescriptorList = aggregationDescriptorList;
  }

  public Ordering getScanOrder() {
    return scanOrder;
  }

  @Nullable
  @Override
  public Expression getPushDownPredicate() {
    return pushDownPredicate;
  }

  public void setPushDownPredicate(@Nullable Expression pushDownPredicate) {
    this.pushDownPredicate = pushDownPredicate;
  }

  public boolean isOutputEndTime() {
    return outputEndTime;
  }

  public void setOutputEndTime(boolean outputEndTime) {
    this.outputEndTime = outputEndTime;
  }

  @Nullable
  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
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
    return visitor.visitSeriesAggregationSourceNode(this, context);
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
    SeriesAggregationSourceNode that = (SeriesAggregationSourceNode) o;
    return aggregationDescriptorList.equals(that.aggregationDescriptorList)
        && scanOrder == that.scanOrder
        && Objects.equals(pushDownPredicate, that.pushDownPredicate)
        && Objects.equals(groupByTimeParameter, that.groupByTimeParameter)
        && outputEndTime == that.outputEndTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        aggregationDescriptorList,
        scanOrder,
        outputEndTime,
        pushDownPredicate,
        groupByTimeParameter);
  }
}
