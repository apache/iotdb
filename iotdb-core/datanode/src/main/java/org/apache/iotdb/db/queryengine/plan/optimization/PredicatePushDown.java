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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.TemplatedInfo;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.InnerTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.LeftOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanSourceNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class PredicatePushDown implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, Analysis analysis, MPPQueryContext context) {
    if (analysis.getStatement().getType() != StatementType.QUERY) {
      return plan;
    }
    QueryStatement queryStatement = (QueryStatement) analysis.getStatement();
    if (queryStatement.isLastQuery() || !analysis.hasValueFilter()) {
      return plan;
    }
    return plan.accept(
        new Rewriter(),
        new RewriterContext(
            context.getQueryId(),
            queryStatement.isAlignByDevice(),
            analysis.isAllDevicesInOneTemplate(),
            context.getTypeProvider().getTemplatedInfo()));
  }

  private static class Rewriter extends PlanVisitor<PlanNode, RewriterContext> {

    @Override
    public PlanNode visitPlan(PlanNode node, RewriterContext context) {
      throw new IllegalArgumentException("Unexpected plan node: " + node);
    }

    @Override
    public PlanNode visitSingleChildProcess(SingleChildProcessNode node, RewriterContext context) {
      PlanNode rewrittenChild = node.getChild().accept(this, context);
      node.setChild(rewrittenChild);
      return node;
    }

    @Override
    public PlanNode visitMultiChildProcess(MultiChildProcessNode node, RewriterContext context) {
      List<PlanNode> rewrittenChildren = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        rewrittenChildren.add(child.accept(this, context));
      }
      node.setChildren(rewrittenChildren);
      return node;
    }

    @Override
    public PlanNode visitFilter(FilterNode node, RewriterContext context) {
      if (fromHaving(node.getPredicate())) {
        return visitSingleChildProcess(node, context);
      }

      context.setPushDownFilterNode(node);
      PlanNode rewrittenChild = node.getChild().accept(this, context);

      boolean enablePushDown = context.isEnablePushDown();
      context.reset();

      if (enablePushDown) {
        return rewrittenChild;
      }
      return node;
    }

    private boolean fromHaving(Expression predicate) {
      List<Expression> aggregations = ExpressionAnalyzer.searchAggregationExpressions(predicate);
      return aggregations != null && !aggregations.isEmpty();
    }

    @Override
    public PlanNode visitFullOuterTimeJoin(FullOuterTimeJoinNode node, RewriterContext context) {
      if (context.hasNotInheritedPredicate()) {
        return node;
      }
      if (context.isBuildPlanUseTemplate()) {
        // only support push down to aligned scan
        return node;
      }

      Expression inheritedPredicate = context.getInheritedPredicate();
      List<Expression> conjuncts = PredicateUtils.extractConjuncts(inheritedPredicate);

      List<PlanNode> children = node.getChildren();

      List<List<Expression>> pushDownConjunctsForEachChild = new ArrayList<>(children.size());
      // empty list for each child at first
      for (int i = 0; i < children.size(); i++) {
        pushDownConjunctsForEachChild.add(new ArrayList<>());
      }

      List<Expression> cannotPushDownConjuncts = new ArrayList<>();
      extractPushDownConjunctsForEachChild(
          conjuncts, children, pushDownConjunctsForEachChild, cannotPushDownConjuncts, context);

      if (cannotPushDownConjuncts.size() == conjuncts.size()) {
        // all conjuncts cannot push down
        return node;
      }

      context.setEnablePushDown(true);

      List<PlanNode> childrenWithPredicate = new ArrayList<>();
      List<PlanNode> childrenWithoutPredicate = new ArrayList<>();
      for (int i = 0; i < children.size(); i++) {
        SeriesScanSourceNode child = (SeriesScanSourceNode) children.get(i);
        if (pushDownConjunctsForEachChild.get(i).isEmpty()) {
          childrenWithoutPredicate.add(child);
        } else {
          child.setPushDownPredicate(
              PredicateUtils.combineConjuncts(pushDownConjunctsForEachChild.get(i)));
          childrenWithPredicate.add(child);
        }
      }

      PlanNode left = planInnerTimeJoin(childrenWithPredicate, node.getMergeOrder(), context);
      PlanNode right =
          planFullOuterTimeJoin(childrenWithoutPredicate, node.getMergeOrder(), context);

      PlanNode resultNode = planLeftOuterTimeJoin(left, right, node.getMergeOrder(), context);

      if (!cannotPushDownConjuncts.isEmpty()) {
        resultNode =
            planFilter(
                resultNode, PredicateUtils.combineConjuncts(cannotPushDownConjuncts), context);
      } else {
        resultNode = planTransform(resultNode, context);
        resultNode = planProject(resultNode, context);
      }
      return resultNode;
    }

    private void extractPushDownConjunctsForEachChild(
        List<Expression> conjuncts,
        List<PlanNode> children,
        List<List<Expression>> pushDownConjunctsForEachChild,
        List<Expression> cannotPushDownConjuncts,
        RewriterContext context) {
      // find the source symbol for each child
      List<PartialPath> sourcePathForEachChild = new ArrayList<>(children.size());
      for (PlanNode child : children) {
        checkArgument(
            child instanceof SeriesScanSourceNode, "Unexpected node type: " + child.getClass());
        sourcePathForEachChild.add(((SeriesScanSourceNode) child).getPartitionPath());
      }

      // distinguish conjuncts that can push down and cannot push down
      for (Expression conjunct : conjuncts) {
        boolean canPushDown = false;
        for (int i = 0; i < sourcePathForEachChild.size(); i++) {
          if (PredicateUtils.predicateCanPushDownToSource(
              conjunct, sourcePathForEachChild.get(i), context.isBuildPlanUseTemplate())) {
            pushDownConjunctsForEachChild.get(i).add(conjunct);
            canPushDown = true;
            break;
          }
        }
        if (!canPushDown) {
          cannotPushDownConjuncts.add(conjunct);
        }
      }
    }

    private PlanNode planInnerTimeJoin(
        List<PlanNode> children, Ordering mergeOrder, RewriterContext context) {
      PlanNode resultNode = null;
      if (children.size() == 1) {
        resultNode = children.get(0);
      } else if (children.size() > 1) {
        resultNode = new InnerTimeJoinNode(context.genPlanNodeId(), children, mergeOrder);
      }
      return resultNode;
    }

    private PlanNode planFullOuterTimeJoin(
        List<PlanNode> children, Ordering mergeOrder, RewriterContext context) {
      PlanNode resultNode = null;
      if (children.size() == 1) {
        resultNode = children.get(0);
      } else if (children.size() > 1) {
        resultNode = new FullOuterTimeJoinNode(context.genPlanNodeId(), mergeOrder, children);
      }
      return resultNode;
    }

    private PlanNode planLeftOuterTimeJoin(
        PlanNode left, PlanNode right, Ordering mergeOrder, RewriterContext context) {
      checkState(left != null || right != null);
      PlanNode resultNode;
      if (left == null) {
        resultNode = right;
      } else if (right == null) {
        resultNode = left;
      } else {
        resultNode = new LeftOuterTimeJoinNode(context.genPlanNodeId(), mergeOrder, left, right);
      }
      return resultNode;
    }

    private PlanNode planFilter(PlanNode child, Expression predicate, RewriterContext context) {
      FilterNode pushDownFilterNode = context.getPushDownFilterNode();
      return new FilterNode(
          context.genPlanNodeId(),
          child,
          pushDownFilterNode.getOutputExpressions(),
          predicate,
          pushDownFilterNode.isKeepNull(),
          pushDownFilterNode.getScanOrder());
    }

    @Override
    public PlanNode visitAlignedSeriesScan(AlignedSeriesScanNode node, RewriterContext context) {
      if (context.hasNotInheritedPredicate()) {
        return node;
      }

      if (!context.isBuildPlanUseTemplate()) {
        return visitSeriesScanSource(node, context);
      }
      TemplatedInfo templatedInfo = context.getTemplatedInfo();
      checkState(templatedInfo != null, "TemplatedInfo should not be null");

      Expression inheritedPredicate = context.getInheritedPredicate();
      if (context.enablePushDownUseTemplate()
          || PredicateUtils.predicateCanPushDownToSource(
              inheritedPredicate, node.getPartitionPath(), context.isBuildPlanUseTemplate())) {
        node.setPushDownPredicate(inheritedPredicate);
        if (!templatedInfo.hasPushDownPredicate()) {
          templatedInfo.setPushDownPredicate(inheritedPredicate);
        }
        context.setEnablePushDownUseTemplate(true);
        context.setEnablePushDown(true);

        return planProject(node, context);
      }

      // cannot push down
      return node;
    }

    @Override
    public PlanNode visitSeriesScan(SeriesScanNode node, RewriterContext context) {
      if (context.hasNotInheritedPredicate()) {
        return node;
      }
      if (!context.isBuildPlanUseTemplate()) {
        return visitSeriesScanSource(node, context);
      }
      // only support push down to aligned scan
      return node;
    }

    @Override
    public PlanNode visitSeriesScanSource(SeriesScanSourceNode node, RewriterContext context) {
      Expression inheritedPredicate = context.getInheritedPredicate();
      if (PredicateUtils.predicateCanPushDownToSource(
          inheritedPredicate, node.getPartitionPath(), context.isBuildPlanUseTemplate())) {
        node.setPushDownPredicate(inheritedPredicate);
        context.setEnablePushDown(true);

        PlanNode resultNode = planTransform(node, context);
        resultNode = planProject(resultNode, context);
        return resultNode;
      }

      // cannot push down
      return node;
    }

    private PlanNode planTransform(PlanNode resultNode, RewriterContext context) {
      FilterNode pushDownFilterNode = context.getPushDownFilterNode();
      Expression[] outputExpressions = pushDownFilterNode.getOutputExpressions();
      boolean needTransform = false;
      for (Expression expression : outputExpressions) {
        if (ExpressionAnalyzer.checkIsNeedTransform(expression)) {
          needTransform = true;
          break;
        }
      }

      if (!needTransform) {
        return resultNode;
      }
      return new TransformNode(
          context.genPlanNodeId(),
          resultNode,
          outputExpressions,
          pushDownFilterNode.isKeepNull(),
          pushDownFilterNode.getScanOrder());
    }

    private PlanNode planProject(PlanNode resultNode, RewriterContext context) {
      FilterNode pushDownFilterNode = context.getPushDownFilterNode();
      if (resultNode instanceof TransformNode) {
        return resultNode;
      }

      if (context.isBuildPlanUseTemplate()) {
        return new ProjectNode(context.genPlanNodeId(), resultNode, null);
      }

      if (context.isAlignByDevice()
          || (pushDownFilterNode.getOutputColumnNames().size()
              != pushDownFilterNode.getChild().getOutputColumnNames().size())) {
        return new ProjectNode(
            context.genPlanNodeId(), resultNode, pushDownFilterNode.getOutputColumnNames());
      }
      return resultNode;
    }
  }

  private static class RewriterContext {

    private final QueryId queryId;
    private final boolean isAlignByDevice;
    private final boolean isBuildPlanUseTemplate;
    private final TemplatedInfo templatedInfo;

    private FilterNode pushDownFilterNode;

    private boolean enablePushDown = false;
    private boolean enablePushDownUseTemplate = false;

    private RewriterContext(
        QueryId queryId,
        boolean isAlignByDevice,
        boolean isBuildPlanUseTemplate,
        TemplatedInfo templatedInfo) {
      this.queryId = queryId;
      this.isAlignByDevice = isAlignByDevice;
      this.isBuildPlanUseTemplate = isBuildPlanUseTemplate;
      this.templatedInfo = templatedInfo;
    }

    public PlanNodeId genPlanNodeId() {
      return queryId.genPlanNodeId();
    }

    public boolean isAlignByDevice() {
      return isAlignByDevice;
    }

    public boolean isBuildPlanUseTemplate() {
      return isBuildPlanUseTemplate;
    }

    public TemplatedInfo getTemplatedInfo() {
      return templatedInfo;
    }

    public FilterNode getPushDownFilterNode() {
      return pushDownFilterNode;
    }

    public void setPushDownFilterNode(FilterNode pushDownFilterNode) {
      this.pushDownFilterNode = pushDownFilterNode;
    }

    public boolean hasNotInheritedPredicate() {
      return pushDownFilterNode == null;
    }

    public Expression getInheritedPredicate() {
      checkState(pushDownFilterNode != null);
      return pushDownFilterNode.getPredicate();
    }

    public boolean isEnablePushDown() {
      return enablePushDown;
    }

    public void setEnablePushDown(boolean enablePushDown) {
      this.enablePushDown = enablePushDown;
    }

    public boolean enablePushDownUseTemplate() {
      return enablePushDownUseTemplate;
    }

    public void setEnablePushDownUseTemplate(boolean enablePushDownUseTemplate) {
      this.enablePushDownUseTemplate = enablePushDownUseTemplate;
    }

    public void reset() {
      this.pushDownFilterNode = null;
      this.enablePushDown = false;
    }
  }
}
