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

package org.apache.iotdb.db.queryengine.plan.optimization;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.optimization.base.ColumnInjectionPushDown;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ColumnInjectNode;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.ArrayList;
import java.util.List;

public class PlanNodePushDown implements PlanOptimizer {
  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();

    if (queryStatement.isGroupByTime() && queryStatement.isOutputEndTime()) {
      // When the aggregation with GROUP BY TIME isn't rawDataQuery, there are AggregationNode and
      // SeriesAggregationNode,
      // If it is and has overlap in groupByParameter, there is SlidingWindowNode
      // There will be a ColumnInjectNode on them, so we need to check if it can be pushed down.
      return plan.accept(new NodePushDownVisitor(), new NodePushDownRewriter());
    }
    return plan;
  }

  private static class NodePushDownVisitor extends PlanVisitor<PlanNode, NodePushDownRewriter> {
    @Override
    public PlanNode visitPlan(PlanNode node, NodePushDownRewriter context) {
      List<PlanNode> children = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        children.add(child.accept(this, context));
      }
      return node.cloneWithChildren(children);
    }

    @Override
    public PlanNode visitColumnInject(ColumnInjectNode node, NodePushDownRewriter context) {
      PlanNode child = node.getChild();
      child.accept(this, context);

      if (child instanceof ColumnInjectionPushDown) {
        ((ColumnInjectionPushDown) child).setOutputEndTime(true);
        return child;
      }

      return node;
    }
  }

  private static class NodePushDownRewriter {}
}
