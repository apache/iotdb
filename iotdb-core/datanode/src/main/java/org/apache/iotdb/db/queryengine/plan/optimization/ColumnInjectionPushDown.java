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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.utils.Preconditions.checkArgument;

/**
 * <b>Optimization phase:</b> Distributed plan planning.
 *
 * <p><b>Rules:</b>
 * <pre>1.
 *        ColumnInject
 *             |              ->  SeriesAggregationSource
 *  SeriesAggregationSource
 * <pre>2.
 *        ColumnInject
 *             |              ->        Aggregation
 *        Aggregation
 * <pre>3.
 *        ColumnInject
 *             |              ->  SlidingWindowAggregation
 *  SlidingWindowAggregation
 */
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
      for (PlanNode child : node.getChildren()) {
        context.setParent(node);
        child.accept(this, context);
      }
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, RewriterContext context) {
      List<PlanNode> children = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        context.setParent(null);
        children.add(child.accept(this, context));
      }
      return node.cloneWithChildren(children);
    }

    @Override
    public PlanNode visitColumnInject(ColumnInjectNode node, RewriterContext context) {
      PlanNode child = node.getChild();

      boolean columnInjectPushDown = true;
      if (child instanceof SeriesAggregationSourceNode) {
        ((SeriesAggregationSourceNode) child).setOutputEndTime(true);
      } else if (child instanceof SlidingWindowAggregationNode) {
        ((SlidingWindowAggregationNode) child).setOutputEndTime(true);
      } else if (child instanceof AggregationNode) {
        ((AggregationNode) child).setOutputEndTime(true);
      } else {
        columnInjectPushDown = false;
      }

      if (columnInjectPushDown) {
        return concatParentWithChild(context.getParent(), child);
      }
      return node;
    }

    private PlanNode concatParentWithChild(PlanNode parent, PlanNode child) {
      if (parent == null) {
        return child;
      }

      checkArgument(parent instanceof SingleChildProcessNode);
      ((SingleChildProcessNode) parent).setChild(child);
      return parent;
    }
  }

  private static class RewriterContext {

    private PlanNode parent;

    public PlanNode getParent() {
      return parent;
    }

    public void setParent(PlanNode parent) {
      this.parent = parent;
    }
  }
}
