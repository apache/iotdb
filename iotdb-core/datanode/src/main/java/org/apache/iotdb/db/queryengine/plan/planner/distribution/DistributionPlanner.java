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
package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.optimization.ColumnInjectionPushDown;
import org.apache.iotdb.db.queryengine.plan.optimization.LimitOffsetPushDown;
import org.apache.iotdb.db.queryengine.plan.optimization.OrderByExpressionWithLimitChangeToTopK;
import org.apache.iotdb.db.queryengine.plan.optimization.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.planner.IFragmentParallelPlaner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.MultiChildrenSinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;

import org.apache.commons.lang3.Validate;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode.LIMIT_VALUE_USE_TOP_K;

public class DistributionPlanner {
  private final Analysis analysis;
  private final MPPQueryContext context;
  private final LogicalQueryPlan logicalPlan;

  private final List<PlanOptimizer> optimizers;

  public DistributionPlanner(Analysis analysis, LogicalQueryPlan logicalPlan) {
    this.analysis = analysis;
    this.logicalPlan = logicalPlan;
    this.context = logicalPlan.getContext();

    this.optimizers =
        Arrays.asList(
            new LimitOffsetPushDown(),
            new ColumnInjectionPushDown(),
            new OrderByExpressionWithLimitChangeToTopK());
  }

  public PlanNode rewriteSource() {
    SourceRewriter rewriter = new SourceRewriter(this.analysis);
    List<PlanNode> planNodeList =
        rewriter.visit(logicalPlan.getRootNode(), new DistributionPlanContext(context));
    if (planNodeList.size() != 1) {
      throw new IllegalStateException("root node must return only one");
    } else {
      return planNodeList.get(0);
    }
  }

  public PlanNode addExchangeNode(PlanNode root) {
    ExchangeNodeAdder adder = new ExchangeNodeAdder(this.analysis);
    NodeGroupContext nodeGroupContext =
        new NodeGroupContext(context, analysis.getTreeStatement(), root);
    PlanNode newRoot = adder.visit(root, nodeGroupContext);
    adjustUpStream(newRoot, nodeGroupContext);
    return newRoot;
  }

  /**
   * Adjust upStream of exchangeNodes, generate {@link
   * org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode} or {@link
   * org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.ShuffleSinkNode} for the children
   * of ExchangeNodes with Same DataRegion.
   */
  private void adjustUpStream(PlanNode root, NodeGroupContext context) {
    if (!context.hasExchangeNode) {
      return;
    }

    if (analysis.isVirtualSource()) {
      adjustUpStreamHelper(root, context);
      return;
    }

    final boolean needShuffleSinkNode =
        analysis.getTreeStatement() instanceof QueryStatement
            && needShuffleSinkNode((QueryStatement) analysis.getTreeStatement(), context);

    adjustUpStreamHelper(root, new HashMap<>(), needShuffleSinkNode, context);
  }

  private void adjustUpStreamHelper(PlanNode root, NodeGroupContext context) {
    for (PlanNode child : root.getChildren()) {
      adjustUpStreamHelper(child, context);
      if (child instanceof ExchangeNode) {
        ExchangeNode exchangeNode = (ExchangeNode) child;
        MultiChildrenSinkNode newChild =
            new IdentitySinkNode(context.queryContext.getQueryId().genPlanNodeId());
        newChild.addChild(exchangeNode.getChild());
        newChild.addDownStreamChannelLocation(
            new DownStreamChannelLocation(exchangeNode.getPlanNodeId().toString()));
        exchangeNode.setChild(newChild);
        exchangeNode.setIndexOfUpstreamSinkHandle(newChild.getCurrentLastIndex());
      }
    }
  }

  private void adjustUpStreamHelper(
      PlanNode root,
      Map<TRegionReplicaSet, MultiChildrenSinkNode> memo,
      boolean needShuffleSinkNode,
      NodeGroupContext context) {
    for (PlanNode child : root.getChildren()) {
      adjustUpStreamHelper(child, memo, needShuffleSinkNode, context);
      if (child instanceof ExchangeNode) {
        ExchangeNode exchangeNode = (ExchangeNode) child;
        TRegionReplicaSet regionOfChild =
            context.getNodeDistribution(exchangeNode.getChild().getPlanNodeId()).getRegion();
        MultiChildrenSinkNode newChild =
            memo.computeIfAbsent(
                regionOfChild,
                tRegionReplicaSet ->
                    needShuffleSinkNode
                        ? new ShuffleSinkNode(context.queryContext.getQueryId().genPlanNodeId())
                        : new IdentitySinkNode(context.queryContext.getQueryId().genPlanNodeId()));
        newChild.addChild(exchangeNode.getChild());
        newChild.addDownStreamChannelLocation(
            new DownStreamChannelLocation(exchangeNode.getPlanNodeId().toString()));
        exchangeNode.setChild(newChild);
        exchangeNode.setIndexOfUpstreamSinkHandle(newChild.getCurrentLastIndex());
      }
    }
  }

  /** Return true if we need to use ShuffleSinkNode instead of IdentitySinkNode. */
  private boolean needShuffleSinkNode(
      QueryStatement queryStatement, NodeGroupContext nodeGroupContext) {
    OrderByComponent orderByComponent = queryStatement.getOrderByComponent();

    if (nodeGroupContext.isAlignByDevice() && orderByComponent != null) {

      // TopKNode will use IdentityNode but not ShuffleSinkNode
      if (queryStatement.hasLimit()
          && !queryStatement.isOrderByBasedOnDevice()
          && queryStatement.getRowLimit() <= LIMIT_VALUE_USE_TOP_K) {
        return false;
      }

      return !orderByComponent.getSortItemList().isEmpty()
          && (orderByComponent.isBasedOnTime() && !queryStatement.hasOrderByExpression());
    }

    return false;
  }

  public PlanNode optimize(PlanNode rootWithExchange) {
    if (analysis.getTreeStatement() != null && analysis.getTreeStatement().isQuery()) {
      for (PlanOptimizer optimizer : optimizers) {
        rootWithExchange = optimizer.optimize(rootWithExchange, analysis, context);
      }
    }
    return rootWithExchange;
  }

  public SubPlan splitFragment(PlanNode root) {
    FragmentBuilder fragmentBuilder = new FragmentBuilder();
    return fragmentBuilder.splitToSubPlan(root);
  }

  public DistributedQueryPlan planFragments() {
    PlanNode rootAfterRewrite = rewriteSource();

    PlanNode rootWithExchange = addExchangeNode(rootAfterRewrite);
    PlanNode optimizedRootWithExchange = optimize(rootWithExchange);
    if (analysis.getTreeStatement() != null && analysis.getTreeStatement().isQuery()) {
      analysis
          .getRespDatasetHeader()
          .setTreeColumnToTsBlockIndexMap(optimizedRootWithExchange.getOutputColumnNames());
    }

    SubPlan subPlan = splitFragment(optimizedRootWithExchange);
    // Mark the root Fragment of root SubPlan as `root`
    subPlan.getPlanFragment().setRoot(true);

    List<FragmentInstance> fragmentInstances = planFragmentInstances(subPlan);

    // Only execute this step for READ operation
    if (context.getQueryType() == QueryType.READ) {
      setSinkForRootInstance(subPlan, fragmentInstances);
    }

    return new DistributedQueryPlan(subPlan, fragmentInstances);
  }

  // Convert fragment to detailed instance
  // And for parallel-able fragment, clone it into several instances with different params.
  public List<FragmentInstance> planFragmentInstances(SubPlan subPlan) {
    IFragmentParallelPlaner parallelPlaner =
        context.getQueryType() == QueryType.READ
            ? new SimpleFragmentParallelPlanner(subPlan, analysis, context)
            : new WriteFragmentParallelPlanner(subPlan, analysis, context);
    return parallelPlaner.parallelPlan();
  }

  // TODO: (xingtanzjr) Maybe we should handle ResultNode in LogicalPlanner ?
  public void setSinkForRootInstance(SubPlan subPlan, List<FragmentInstance> instances) {
    FragmentInstance rootInstance = null;
    for (FragmentInstance instance : instances) {
      if (instance.getFragment().getId().equals(subPlan.getPlanFragment().getId())) {
        rootInstance = instance;
        break;
      }
    }
    // root should not be null during normal process
    if (rootInstance == null) {
      return;
    }

    IdentitySinkNode sinkNode =
        new IdentitySinkNode(
            context.getQueryId().genPlanNodeId(),
            Collections.singletonList(rootInstance.getFragment().getPlanNodeTree()),
            Collections.singletonList(
                new DownStreamChannelLocation(
                    context.getLocalDataBlockEndpoint(),
                    context.getResultNodeContext().getVirtualFragmentInstanceId().toThrift(),
                    context.getResultNodeContext().getVirtualResultNodeId().getId())));
    context
        .getResultNodeContext()
        .setUpStream(
            rootInstance.getHostDataNode().mPPDataExchangeEndPoint,
            rootInstance.getId(),
            sinkNode.getPlanNodeId());
    rootInstance.getFragment().setPlanNodeTree(sinkNode);
  }

  private PlanFragmentId getNextFragmentId() {
    return this.logicalPlan.getContext().getQueryId().genPlanFragmentId();
  }

  private class FragmentBuilder {

    public SubPlan splitToSubPlan(PlanNode root) {
      SubPlan rootSubPlan = createSubPlan(root);
      Set<PlanNodeId> visitedSinkNode = new HashSet<>();
      splitToSubPlan(root, rootSubPlan, visitedSinkNode);
      return rootSubPlan;
    }

    private void splitToSubPlan(PlanNode root, SubPlan subPlan, Set<PlanNodeId> visitedSinkNode) {
      // TODO: (xingtanzjr) we apply no action for IWritePlanNode currently
      if (root instanceof WritePlanNode) {
        return;
      }
      if (root instanceof ExchangeNode) {
        // We add a FragmentSinkNode for newly created PlanFragment
        ExchangeNode exchangeNode = (ExchangeNode) root;
        Validate.isTrue(
            exchangeNode.getChild() instanceof MultiChildrenSinkNode,
            "child of ExchangeNode must be MultiChildrenSinkNode");
        MultiChildrenSinkNode sinkNode = (MultiChildrenSinkNode) (exchangeNode.getChild());

        // We cut off the subtree to make the ExchangeNode as the leaf node of current PlanFragment
        exchangeNode.cleanChildren();

        // If the SinkNode hasn't visited, build the child SubPlan Tree
        if (!visitedSinkNode.contains(sinkNode.getPlanNodeId())) {
          visitedSinkNode.add(sinkNode.getPlanNodeId());
          SubPlan childSubPlan = createSubPlan(sinkNode);
          splitToSubPlan(sinkNode, childSubPlan, visitedSinkNode);
          subPlan.addChild(childSubPlan);
        }
        return;
      }
      for (PlanNode child : root.getChildren()) {
        splitToSubPlan(child, subPlan, visitedSinkNode);
      }
    }

    private SubPlan createSubPlan(PlanNode root) {
      PlanFragment fragment = new PlanFragment(getNextFragmentId(), root);
      return new SubPlan(fragment);
    }
  }
}
