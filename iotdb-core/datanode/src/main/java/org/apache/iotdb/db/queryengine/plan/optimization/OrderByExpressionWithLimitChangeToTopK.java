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

package org.apache.iotdb.db.queryengine.plan.optimization;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode.LIMIT_VALUE_USE_TOP_K;

/**
 * Replace `SortNode` to `TopKNode` in these cases:
 * <li>`LimitNode + SortNode` change to `TopKNode`.
 * <li>`LimitNode + OffsetNode + SortNode` change to `LimitNode + OffsetNode + TopKNode(topValue =
 *     limitValue+offsetValue)`.
 * <li>`LimitNode + TransformNode + SortNode` change to `TransformNode + TopKNode`.
 * <li>`LimitNode + OffsetNode + TransformNode + SortNode` change to `LimitNode + OffsetNode +
 *     TransformNode + TopKNode(topValue = limitValue+offsetValue)`.
 */
public class OrderByExpressionWithLimitChangeToTopK implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    if (analysis.getStatement().getType() != StatementType.QUERY) {
      return plan;
    }

    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
    if (queryStatement.isLastQuery() || !queryStatement.hasLimit()) {
      return plan;
    }

    // when align by time, only order by expression can use this optimize rule
    if (!queryStatement.isAlignByDevice() && !queryStatement.hasOrderByExpression()) {
      return plan;
    }

    // align by device,
    // when use TopKNode (because MergeSortNode with LimitNode has been replaced to TopKNode),
    // or order_based_on_device (because it does not need TopKNode),
    // will not use this optimize rule
    if (queryStatement.isAlignByDevice()
        && (analysis.isUseTopKNode()
            || !queryStatement.hasOrderBy()
            || queryStatement.isOrderByBasedOnDevice())) {
      return plan;
    }

    return plan.accept(new Rewriter(), new RewriterContext(context));
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
    public PlanNode visitLimit(LimitNode limitNode, RewriterContext rewriterContext) {
      PlanNode parent = rewriterContext.getParent();

      if (limitNode.getChild() instanceof OffsetNode) {
        rewriterContext.setParent(limitNode);
        limitNode.getChild().accept(this, rewriterContext);
      }

      if (limitNode.getLimit() > LIMIT_VALUE_USE_TOP_K) {
        return limitNode;
      }

      if (limitNode.getChild() instanceof SortNode) {
        SortNode sortNode = (SortNode) limitNode.getChild();
        TopKNode topKNode =
            new TopKNode(
                rewriterContext.getMppQueryContext().getQueryId().genPlanNodeId(),
                (int) limitNode.getLimit(),
                sortNode.getOrderByParameter(),
                sortNode.getOutputColumnNames());
        topKNode.setChildren(sortNode.getChildren());

        if (parent != null) {
          ((SingleChildProcessNode) parent).setChild(topKNode);
          return parent;
        } else {
          return topKNode;
        }
      } else if (limitNode.getChild() instanceof MergeSortNode) {
        MergeSortNode mergeSortNode = (MergeSortNode) limitNode.getChild();
        TopKNode topKNode =
            new TopKNode(
                rewriterContext.getMppQueryContext().getQueryId().genPlanNodeId(),
                (int) limitNode.getLimit(),
                mergeSortNode.getMergeOrderParameter(),
                mergeSortNode.getOutputColumnNames());
        topKNode.setChildren(mergeSortNode.getChildren());

        if (parent != null) {
          ((SingleChildProcessNode) parent).setChild(topKNode);
          return parent;
        } else {
          return topKNode;
        }
      } else if (limitNode.getChild() instanceof TransformNode) {
        TransformNode transformNode = (TransformNode) limitNode.getChild();
        if (transformNode.getChild() instanceof SortNode) {
          SortNode sortNode = (SortNode) transformNode.getChild();
          TopKNode topKNode =
              new TopKNode(
                  rewriterContext.getMppQueryContext().getQueryId().genPlanNodeId(),
                  (int) limitNode.getLimit(),
                  sortNode.getOrderByParameter(),
                  sortNode.getOutputColumnNames());
          topKNode.setChildren(sortNode.getChildren());
          transformNode.setChild(topKNode);

          if (parent != null) {
            ((SingleChildProcessNode) parent).setChild(transformNode);
            return parent;
          } else {
            return transformNode;
          }
        } else if (transformNode.getChild() instanceof MergeSortNode) {
          MergeSortNode mergeSortNode = (MergeSortNode) transformNode.getChild();
          TopKNode topKNode =
              new TopKNode(
                  rewriterContext.getMppQueryContext().getQueryId().genPlanNodeId(),
                  (int) limitNode.getLimit(),
                  mergeSortNode.getMergeOrderParameter(),
                  mergeSortNode.getOutputColumnNames());
          topKNode.setChildren(mergeSortNode.getChildren());
          transformNode.setChild(topKNode);

          if (parent != null) {
            ((SingleChildProcessNode) parent).setChild(transformNode);
            return parent;
          } else {
            return transformNode;
          }
        }
      }

      return limitNode;
    }

    @Override
    public PlanNode visitOffset(OffsetNode offsetNode, RewriterContext rewriterContext) {
      PlanNode parent = rewriterContext.getParent();

      if (!(parent instanceof LimitNode)) {
        return offsetNode;
      }

      LimitNode limitNode = (LimitNode) parent;
      if (limitNode.getLimit() + offsetNode.getOffset() > LIMIT_VALUE_USE_TOP_K) {
        return offsetNode;
      }

      if (offsetNode.getChild() instanceof SortNode) {
        SortNode sortNode = (SortNode) offsetNode.getChild();
        TopKNode topKNode =
            new TopKNode(
                rewriterContext.getMppQueryContext().getQueryId().genPlanNodeId(),
                (int) ((int) ((LimitNode) parent).getLimit() + offsetNode.getOffset()),
                sortNode.getOrderByParameter(),
                sortNode.getOutputColumnNames());
        topKNode.setChildren(sortNode.getChildren());
        offsetNode.setChild(topKNode);
      } else if (offsetNode.getChild() instanceof MergeSortNode) {
        MergeSortNode sortNode = (MergeSortNode) offsetNode.getChild();
        TopKNode topKNode =
            new TopKNode(
                rewriterContext.getMppQueryContext().getQueryId().genPlanNodeId(),
                (int) ((int) ((LimitNode) parent).getLimit() + offsetNode.getOffset()),
                sortNode.getMergeOrderParameter(),
                sortNode.getOutputColumnNames());
        topKNode.setChildren(sortNode.getChildren());
        offsetNode.setChild(topKNode);
      } else if (offsetNode.getChild() instanceof TransformNode) {
        TransformNode transformNode = (TransformNode) offsetNode.getChild();
        if (transformNode.getChild() instanceof SortNode) {
          SortNode sortNode = (SortNode) transformNode.getChild();
          TopKNode topKNode =
              new TopKNode(
                  rewriterContext.getMppQueryContext().getQueryId().genPlanNodeId(),
                  (int) ((int) ((LimitNode) parent).getLimit() + offsetNode.getOffset()),
                  sortNode.getOrderByParameter(),
                  sortNode.getOutputColumnNames());
          topKNode.setChildren(sortNode.getChildren());
          transformNode.setChild(topKNode);
        } else if (transformNode.getChild() instanceof MergeSortNode) {
          MergeSortNode mergeSortNode = (MergeSortNode) transformNode.getChild();
          TopKNode topKNode =
              new TopKNode(
                  rewriterContext.getMppQueryContext().getQueryId().genPlanNodeId(),
                  (int) ((int) ((LimitNode) parent).getLimit() + offsetNode.getOffset()),
                  mergeSortNode.getMergeOrderParameter(),
                  mergeSortNode.getOutputColumnNames());
          topKNode.setChildren(mergeSortNode.getChildren());
          transformNode.setChild(topKNode);
        }
      }

      return offsetNode;
    }
  }

  static class RewriterContext {
    private PlanNode parent;

    private final MPPQueryContext mppQueryContext;

    public RewriterContext(MPPQueryContext mppQueryContext) {
      this.mppQueryContext = mppQueryContext;
    }

    public PlanNode getParent() {
      return parent;
    }

    public void setParent(PlanNode parent) {
      this.parent = parent;
    }

    public MPPQueryContext getMppQueryContext() {
      return this.mppQueryContext;
    }
  }
}
