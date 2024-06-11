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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode.LIMIT_VALUE_USE_TOP_K;
import static org.apache.iotdb.db.queryengine.plan.statement.component.FillPolicy.LINEAR;

/**
 * Replace `SortNode`+`LimitNode` to `TopKNode` and replace `MergeSortNode`+`LimitNode` to
 * `TopKNode` in below cases. Notice that, `TransformNode` below is only used to transform the
 * projection columns, an example query used `TransformNode`: `select s1 from root.** order by s2`.
 *
 * <p>The cases which can use this optimize rule.
 * <li>`LimitNode + SortNode` ==> `TopKNode`.
 * <li>`LimitNode + MergeSortNode` ==> `TopKNode`.
 * <li>`LimitNode + OffsetNode + SortNode` ==> `LimitNode + OffsetNode + TopKNode(where topValue =
 *     limitValue+offsetValue)`.
 * <li>`LimitNode + OffsetNode + MergeSortNode` ==> `LimitNode + OffsetNode + TopKNode(where
 *     topValue = limitValue+offsetValue)`.
 * <li>`LimitNode + TransformNode/FillNode + SortNode` ==> `TransformNode/FillNode + TopKNode`.
 * <li>`LimitNode + TransformNode/FillNode + MergeSortNode` ==> `TransformNode/FillNode + TopKNode`.
 * <li>`LimitNode + OffsetNode + TransformNode/FillNode + SortNode` ==> `LimitNode + OffsetNode +
 *     TransformNode/FillNode + TopKNode(where topValue = limitValue+offsetValue)`.
 * <li>`LimitNode + OffsetNode + TransformNode/FillNode + MergeSortNode` ==> `LimitNode + OffsetNode
 *     + TransformNode/FillNode + TopKNode(where topValue = limitValue+offsetValue)`.
 */
public class OrderByExpressionWithLimitChangeToTopK implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    if (analysis.getTreeStatement().getType() != StatementType.QUERY) {
      return plan;
    }

    QueryStatement queryStatement = analysis.getQueryStatement();
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
      PlanNode newNode = node.clone();
      for (PlanNode child : node.getChildren()) {
        context.setParent(node);
        newNode.addChild(child.accept(this, context));
      }
      return newNode;
    }

    @Override
    public PlanNode visitSingleChildProcess(SingleChildProcessNode node, RewriterContext context) {
      context.setParent(node);
      node.setChild(node.getChild().accept(this, context));
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, RewriterContext context) {
      List<PlanNode> visitedChildren = new ArrayList<>();
      for (int i = 0; i < node.getChildren().size(); i++) {
        context.setParent(node);
        visitedChildren.add(node.getChildren().get(i).accept(this, context));
      }
      node.setChildren(visitedChildren);
      return node;
    }

    @Override
    public PlanNode visitLimit(LimitNode limitNode, RewriterContext rewriterContext) {

      if (limitNode.getChild() instanceof OffsetNode) {
        rewriterContext.setParent(limitNode);
        limitNode.getChild().accept(this, rewriterContext);
      }

      if (limitNode.getLimit() > LIMIT_VALUE_USE_TOP_K) {
        return limitNode;
      }

      if (limitNode.getChild() instanceof SortNode) {
        return rewriterContext.returnSortNode(limitNode);
      } else if (limitNode.getChild() instanceof MergeSortNode) {
        return rewriterContext.returnMergeSortNode(limitNode);
      } else if (limitNode.getChild() instanceof TransformNode
          && !(limitNode.getChild() instanceof FilterNode)) {
        return rewriterContext.returnTransformNodeFillNode(limitNode);
      } else if (limitNode.getChild() instanceof FillNode
          && !LINEAR.equals(
              ((FillNode) limitNode.getChild()).getFillDescriptor().getFillPolicy())) {
        return rewriterContext.returnTransformNodeFillNode(limitNode);
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
        rewriterContext.processSortNode(offsetNode, parent);
      } else if (offsetNode.getChild() instanceof MergeSortNode) {
        rewriterContext.processMergeSortNode(offsetNode, parent);
      } else if (offsetNode.getChild() instanceof TransformNode
          && !(offsetNode.getChild() instanceof FilterNode)) {
        rewriterContext.processTransformNodeFillNode(offsetNode, parent);
      } else if (offsetNode.getChild() instanceof FillNode
          && !LINEAR.equals(
              ((FillNode) offsetNode.getChild()).getFillDescriptor().getFillPolicy())) {
        rewriterContext.processTransformNodeFillNode(offsetNode, parent);
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

    private PlanNode returnSortNode(LimitNode limitNode) {
      SortNode sortNode = (SortNode) limitNode.getChild();
      TopKNode topKNode =
          new TopKNode(
              getMppQueryContext().getQueryId().genPlanNodeId(),
              (int) limitNode.getLimit(),
              sortNode.getOrderByParameter(),
              sortNode.getOutputColumnNames());
      topKNode.setChildren(sortNode.getChildren());

      return topKNode;
    }

    private PlanNode returnMergeSortNode(LimitNode limitNode) {
      MergeSortNode mergeSortNode = (MergeSortNode) limitNode.getChild();
      TopKNode topKNode =
          new TopKNode(
              getMppQueryContext().getQueryId().genPlanNodeId(),
              (int) limitNode.getLimit(),
              mergeSortNode.getMergeOrderParameter(),
              mergeSortNode.getOutputColumnNames());
      topKNode.setChildren(mergeSortNode.getChildren());

      return topKNode;
    }

    private PlanNode returnTransformNodeFillNode(LimitNode limitNode) {
      SingleChildProcessNode singleNode = (SingleChildProcessNode) limitNode.getChild();
      if (singleNode.getChild() instanceof SortNode) {
        SortNode sortNode = (SortNode) singleNode.getChild();
        TopKNode topKNode =
            new TopKNode(
                getMppQueryContext().getQueryId().genPlanNodeId(),
                (int) limitNode.getLimit(),
                sortNode.getOrderByParameter(),
                sortNode.getOutputColumnNames());
        topKNode.setChildren(sortNode.getChildren());
        singleNode.setChild(topKNode);

        return singleNode;
      } else if (singleNode.getChild() instanceof MergeSortNode) {
        MergeSortNode mergeSortNode = (MergeSortNode) singleNode.getChild();
        TopKNode topKNode =
            new TopKNode(
                getMppQueryContext().getQueryId().genPlanNodeId(),
                (int) limitNode.getLimit(),
                mergeSortNode.getMergeOrderParameter(),
                mergeSortNode.getOutputColumnNames());
        topKNode.setChildren(mergeSortNode.getChildren());
        singleNode.setChild(topKNode);

        return singleNode;
      }

      return limitNode;
    }

    private void processSortNode(OffsetNode offsetNode, PlanNode parent) {
      SortNode sortNode = (SortNode) offsetNode.getChild();
      TopKNode topKNode =
          new TopKNode(
              getMppQueryContext().getQueryId().genPlanNodeId(),
              (int) ((int) ((LimitNode) parent).getLimit() + offsetNode.getOffset()),
              sortNode.getOrderByParameter(),
              sortNode.getOutputColumnNames());
      topKNode.setChildren(sortNode.getChildren());
      offsetNode.setChild(topKNode);
    }

    private void processMergeSortNode(OffsetNode offsetNode, PlanNode parent) {
      MergeSortNode sortNode = (MergeSortNode) offsetNode.getChild();
      TopKNode topKNode =
          new TopKNode(
              getMppQueryContext().getQueryId().genPlanNodeId(),
              (int) ((int) ((LimitNode) parent).getLimit() + offsetNode.getOffset()),
              sortNode.getMergeOrderParameter(),
              sortNode.getOutputColumnNames());
      topKNode.setChildren(sortNode.getChildren());
      offsetNode.setChild(topKNode);
    }

    private void processTransformNodeFillNode(OffsetNode offsetNode, PlanNode parent) {
      SingleChildProcessNode singleNode = (SingleChildProcessNode) offsetNode.getChild();
      if (singleNode.getChild() instanceof SortNode) {
        SortNode sortNode = (SortNode) singleNode.getChild();
        TopKNode topKNode =
            new TopKNode(
                getMppQueryContext().getQueryId().genPlanNodeId(),
                (int) ((int) ((LimitNode) parent).getLimit() + offsetNode.getOffset()),
                sortNode.getOrderByParameter(),
                sortNode.getOutputColumnNames());
        topKNode.setChildren(sortNode.getChildren());
        singleNode.setChild(topKNode);
      } else if (singleNode.getChild() instanceof MergeSortNode) {
        MergeSortNode mergeSortNode = (MergeSortNode) singleNode.getChild();
        TopKNode topKNode =
            new TopKNode(
                getMppQueryContext().getQueryId().genPlanNodeId(),
                (int) ((int) ((LimitNode) parent).getLimit() + offsetNode.getOffset()),
                mergeSortNode.getMergeOrderParameter(),
                mergeSortNode.getOutputColumnNames());
        topKNode.setChildren(mergeSortNode.getChildren());
        singleNode.setChild(topKNode);
      }
    }
  }
}
