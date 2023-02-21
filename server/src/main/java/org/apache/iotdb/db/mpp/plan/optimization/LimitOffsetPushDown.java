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

package org.apache.iotdb.db.mpp.plan.optimization;

import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;

public class LimitOffsetPushDown implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    if (analysis.getStatement().getType() != StatementType.QUERY) {
      return plan;
    }
    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
    if (queryStatement.isAggregationQuery()
        || (!queryStatement.hasLimit() && !queryStatement.hasOffset())) {
      return plan;
    }
    return plan.accept(new Rewriter(), new RewriterContext(analysis));
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      for (PlanNode child : node.getChildren()) {
        child.accept(this, context);
      }
      return node;
    }

    @Override
    public PlanNode visitLimit(LimitNode node, RewriterContext context) {
      PlanNode child = node.getChild();
      PlanNode parent = context.getParent();

      context.setParent(node);
      context.setLimit(node.getLimit());
      child.accept(this, context);

      if (context.isEnablePushDown()) {
        if (parent != null) {
          ((SingleChildProcessNode) parent).setChild(child);
          return parent;
        } else {
          return node.getChild();
        }
      }
      return node;
    }

    @Override
    public PlanNode visitOffset(OffsetNode node, RewriterContext context) {
      PlanNode child = node.getChild();
      PlanNode parent = context.getParent();

      context.setParent(node);
      context.setOffset(node.getOffset());
      child.accept(this, context);

      if (context.isEnablePushDown()) {
        if (parent != null) {
          ((SingleChildProcessNode) parent).setChild(child);
          return parent;
        } else {
          return node.getChild();
        }
      }
      return node;
    }

    @Override
    public PlanNode visitFill(FillNode node, RewriterContext context) {
      FillPolicy fillPolicy = node.getFillDescriptor().getFillPolicy();
      if (fillPolicy == FillPolicy.VALUE) {
        node.getChild().accept(this, context);
      } else {
        context.setEnablePushDown(false);
      }
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriterContext context) {
      context.setEnablePushDown(false);
      return node;
    }

    @Override
    public PlanNode visitTransform(TransformNode node, RewriterContext context) {
      Expression[] outputExpressions = node.getOutputExpressions();
      boolean enablePushDown = true;
      for (Expression expression : outputExpressions) {
        if (!ExpressionAnalyzer.checkIsScalarExpression(expression, context.getAnalysis())) {
          enablePushDown = false;
          break;
        }
      }

      if (enablePushDown) {
        node.getChild().accept(this, context);
      } else {
        context.setEnablePushDown(false);
      }
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, RewriterContext context) {
      context.setEnablePushDown(false);
      return node;
    }

    @Override
    public PlanNode visitSeriesScan(SeriesScanNode node, RewriterContext context) {
      if (context.isEnablePushDown()) {
        node.setLimit(context.getLimit());
        node.setOffset(context.getOffset());
      }
      return node;
    }

    @Override
    public PlanNode visitAlignedSeriesScan(AlignedSeriesScanNode node, RewriterContext context) {
      if (context.isEnablePushDown()) {
        node.setLimit(context.getLimit());
        node.setOffset(context.getOffset());
      }
      return node;
    }
  }

  private static class RewriterContext {
    private long limit;
    private long offset;

    private boolean enablePushDown = true;

    private PlanNode parent;

    private final Analysis analysis;

    public RewriterContext(Analysis analysis) {
      this.analysis = analysis;
    }

    public long getLimit() {
      return limit;
    }

    public void setLimit(long limit) {
      this.limit = limit;
    }

    public long getOffset() {
      return offset;
    }

    public void setOffset(long offset) {
      this.offset = offset;
    }

    public boolean isEnablePushDown() {
      return enablePushDown;
    }

    public void setEnablePushDown(boolean enablePushDown) {
      this.enablePushDown = enablePushDown;
    }

    public PlanNode getParent() {
      return parent;
    }

    public void setParent(PlanNode parent) {
      this.parent = parent;
    }

    public Analysis getAnalysis() {
      return analysis;
    }
  }
}
