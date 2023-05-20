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

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public abstract class SeriesAggregationSourceNode extends SeriesSourceNode {

  // The list of aggregate functions, each AggregateDescriptor will be output as one column in
  // result TsBlock
  protected List<AggregationDescriptor> aggregationDescriptorList;

  // The order to traverse the data.
  // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
  // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
  protected Ordering scanOrder = Ordering.ASC;

  // time filter for current series, could be null if it doesn't exist
  @Nullable protected Filter timeFilter;

  // push-downing query filter for current series, could be null if it doesn't exist
  @Nullable protected Filter valueFilter;

  // The parameter of `group by time`
  // Its value will be null if there is no `group by time` clause,
  @Nullable protected GroupByTimeParameter groupByTimeParameter;

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
  public Filter getTimeFilter() {
    return timeFilter;
  }

  public void setTimeFilter(@Nullable Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  @Nullable
  public Filter getValueFilter() {
    return valueFilter;
  }

  public void setValueFilter(@Nullable Filter valueFilter) {
    this.valueFilter = valueFilter;
  }

  @Nullable
  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
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
        && Objects.equals(timeFilter, that.timeFilter)
        && Objects.equals(valueFilter, that.valueFilter)
        && Objects.equals(groupByTimeParameter, that.groupByTimeParameter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        aggregationDescriptorList,
        scanOrder,
        timeFilter,
        valueFilter,
        groupByTimeParameter);
  }
}
