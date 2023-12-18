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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ColumnInjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

/** <b>Optimization phase:</b> Distributed plan planning */
public class ColumnInjectionPushDown implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    if (analysis.getStatement().getType() != StatementType.QUERY) {
      return plan;
    }
    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
    if (queryStatement.isGroupByTime() && queryStatement.isOutputEndTime()) {
      // When the aggregation with GROUP BY TIME isn't rawDataQuery, there are AggregationNode and
      // SeriesAggregationNode,
      // If it is and has overlap in groupByParameter, there is SlidingWindowNode
      // There will be a ColumnInjectNode on them, so we need to check if it can be pushed down.
      return plan.accept(new Rewriter(), new RewriterContext());
    }
    return plan;
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      context.setParent(node);
      context.setMeetColumnInject(false);
      for (PlanNode child : node.getChildren()) {
        child.accept(this, context);
      }
      return node;
    }

    @Override
    public PlanNode visitColumnInject(ColumnInjectNode node, RewriterContext context) {
      PlanNode parent = context.getParent();

      context.setParent(node);
      context.setMeetColumnInject(true);
      node.getChild().accept(this, context);

      if (context.columnInjectPushDown()) {
        return concatParentWithChild(parent, node.getChild());
      }
      return node;
    }

    private PlanNode concatParentWithChild(PlanNode parent, PlanNode child) {
      if (parent != null) {
        ((SingleChildProcessNode) parent).setChild(child);
        return parent;
      } else {
        return child;
      }
    }

    @Override
    public PlanNode visitSeriesAggregationSourceNode(
        SeriesAggregationSourceNode node, RewriterContext context) {
      if (context.meetColumnInject()) {
        node.setOutputEndTime(true);
        context.setColumnInjectPushDown(true);
      }
      // meet leaf node, stop visiting
      return node;
    }

    @Override
    public PlanNode visitSlidingWindowAggregation(
        SlidingWindowAggregationNode node, RewriterContext context) {
      if (context.meetColumnInject()) {
        node.setOutputEndTime(true);
        context.setColumnInjectPushDown(true);
      }
      // stop visiting its child
      return node;
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, RewriterContext context) {
      if (context.meetColumnInject()) {
        node.setOutputEndTime(true);
        context.setColumnInjectPushDown(true);
      }
      // stop visiting its children
      return node;
    }
  }

  private static class RewriterContext {

    private PlanNode parent;

    private boolean meetColumnInject = false;
    private boolean columnInjectPushDown = false;

    public PlanNode getParent() {
      return parent;
    }

    public void setParent(PlanNode parent) {
      this.parent = parent;
    }

    public boolean meetColumnInject() {
      return meetColumnInject;
    }

    public void setMeetColumnInject(boolean meetColumnInject) {
      this.meetColumnInject = meetColumnInject;
    }

    public boolean columnInjectPushDown() {
      return columnInjectPushDown;
    }

    public void setColumnInjectPushDown(boolean columnInjectPushDown) {
      this.columnInjectPushDown = columnInjectPushDown;
    }
  }
}
