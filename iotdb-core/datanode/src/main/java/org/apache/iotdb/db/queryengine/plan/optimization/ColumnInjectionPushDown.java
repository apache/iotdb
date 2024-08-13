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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TwoChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.ArrayList;
import java.util.List;

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
 *             |              ->     RawDataAggregation
 *     RawDataAggregation
 * <pre>4.
 *        ColumnInject
 *             |              ->  SlidingWindowAggregation
 *  SlidingWindowAggregation
 */
public class ColumnInjectionPushDown implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    if (analysis.getTreeStatement().getType() != StatementType.QUERY) {
      return plan;
    }
    QueryStatement queryStatement = analysis.getQueryStatement();
    if (queryStatement.isGroupByTime() && queryStatement.isOutputEndTime()) {
      // When the aggregation with GROUP BY TIME isn't rawDataQuery, there are AggregationNode and
      // SeriesAggregationNode,
      // If it is and has overlap in groupByParameter, there is SlidingWindowNode
      // There will be a ColumnInjectNode on them, so we need to check if it can be pushed down.
      return plan.accept(new Rewriter(), null);
    }
    return plan;
  }

  private static class Rewriter extends PlanVisitor<PlanNode, Void> {

    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
      // other source node, just return
      return node;
    }

    @Override
    public PlanNode visitSingleChildProcess(SingleChildProcessNode node, Void context) {
      PlanNode rewrittenChild = node.getChild().accept(this, context);
      node.setChild(rewrittenChild);
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, Void context) {
      List<PlanNode> rewrittenChildren = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        rewrittenChildren.add(child.accept(this, context));
      }
      node.setChildren(rewrittenChildren);
      return node;
    }

    @Override
    public PlanNode visitTwoChildProcess(TwoChildProcessNode node, Void context) {
      node.setLeftChild(node.getLeftChild().accept(this, context));
      node.setRightChild(node.getRightChild().accept(this, context));
      return node;
    }

    @Override
    public PlanNode visitColumnInject(ColumnInjectNode node, Void context) {
      PlanNode child = node.getChild();

      boolean columnInjectPushDown = doPushDown(node, child);

      if (columnInjectPushDown) {
        return child;
      }
      return node;
    }

    private boolean doPushDown(PlanNode node, PlanNode child) {
      boolean columnInjectPushDown = true;
      if (child instanceof SeriesAggregationSourceNode) {
        ((SeriesAggregationSourceNode) child).setOutputEndTime(true);
      } else if (child instanceof SlidingWindowAggregationNode) {
        ((SlidingWindowAggregationNode) child).setOutputEndTime(true);
      } else if (child instanceof AggregationNode) {
        ((AggregationNode) child).setOutputEndTime(true);
      } else if (child instanceof RawDataAggregationNode) {
        ((RawDataAggregationNode) child).setOutputEndTime(true);
      } else if (child instanceof ProjectNode) {
        ProjectNode projectNode = (ProjectNode) child;
        boolean pushDownToChild = doPushDown(child, projectNode.getChild());
        if (pushDownToChild) {
          projectNode.setOutputColumnNames(node.getOutputColumnNames());
        }
        return pushDownToChild;
      } else {
        columnInjectPushDown = false;
      }
      return columnInjectPushDown;
    }
  }
}
