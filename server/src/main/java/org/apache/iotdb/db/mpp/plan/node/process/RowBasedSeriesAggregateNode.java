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
package org.apache.iotdb.db.mpp.plan.node.process;

import org.apache.iotdb.db.mpp.common.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.node.PlanNodeId;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;

import java.util.List;

/**
 * This node is used to aggregate required series by raw data. The raw data will be input as a
 * TsBlock. This node will output the series aggregated result represented by TsBlock Thus, the
 * columns in output TsBlock will be different from input TsBlock.
 */
public class RowBasedSeriesAggregateNode extends ProcessNode {
  // The parameter of `group by time`
  // Its value will be null if there is no `group by time` clause,
  private GroupByTimeParameter groupByTimeParameter;

  // The list of aggregation functions, each FunctionExpression will be output as one column of
  // result TsBlock
  // (Currently we only support one series in the aggregation function)
  // TODO: need consider whether it is suitable the aggregation function using FunctionExpression
  private List<FunctionExpression> aggregateFuncList;

  public RowBasedSeriesAggregateNode(PlanNodeId id) {
    super(id);
  }

  public RowBasedSeriesAggregateNode(PlanNodeId id, List<FunctionExpression> aggregateFuncList) {
    this(id);
    this.aggregateFuncList = aggregateFuncList;
  }

  public RowBasedSeriesAggregateNode(
      PlanNodeId id,
      List<FunctionExpression> aggregateFuncList,
      GroupByTimeParameter groupByTimeParameter) {
    this(id, aggregateFuncList);
    this.groupByTimeParameter = groupByTimeParameter;
  }
}
