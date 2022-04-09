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
package org.apache.iotdb.db.mpp.sql.planner.plan.node.process;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This node is used to aggregate required series from multiple sources. The source data will be
 * input as a TsBlock, it may be raw data or partial aggregation result. This node will output the
 * final series aggregated result represented by TsBlock.
 */
public class AggregateNode extends ProcessNode {
  // The parameter of `group by time`
  // Its value will be null if there is no `group by time` clause,
  private GroupByTimeParameter groupByTimeParameter;

  // The list of aggregation functions, each FunctionExpression will be output as one column of
  // result TsBlock
  // (Currently we only support one series in the aggregation function)
  // TODO: need consider whether it is suitable the aggregation function using FunctionExpression
  private final Map<PartialPath, Set<AggregationType>> aggregateFuncMap;

  private final List<PlanNode> children;
  private List<String> columnNames;

  public AggregateNode(
      PlanNodeId id,
      Map<PartialPath, Set<AggregationType>> aggregateFuncMap,
      List<PlanNode> children) {
    super(id);
    this.aggregateFuncMap = aggregateFuncMap;
    this.children = children;
  }

  @Override
  public List<PlanNode> getChildren() {
    return children;
  }

  @Override
  public void addChild(PlanNode child) {
    throw new NotImplementedException("addChild of AggregateNode is not implemented");
  }

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("Clone of AggregateNode is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return CHILD_COUNT_NO_LIMIT;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return columnNames;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitRowBasedSeriesAggregate(this, context);
  }

  public static AggregateNode deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {}

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AggregateNode that = (AggregateNode) o;
    return Objects.equals(groupByTimeParameter, that.groupByTimeParameter)
        && Objects.equals(aggregateFuncMap, that.aggregateFuncMap)
        && Objects.equals(children, that.children)
        && Objects.equals(columnNames, that.columnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupByTimeParameter, aggregateFuncMap, children, columnNames);
  }
}
