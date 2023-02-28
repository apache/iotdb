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
package org.apache.iotdb.db.mpp.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.optimization.LimitOffsetPushDown;
import org.apache.iotdb.db.mpp.plan.optimization.PlanOptimizer;
import org.apache.iotdb.db.mpp.plan.planner.IFragmentParallelPlaner;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.MultiChildrenSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.mpp.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;

import org.apache.commons.lang3.Validate;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DistributionPlanner {
  private Analysis analysis;
  private MPPQueryContext context;
  private LogicalQueryPlan logicalPlan;

  private final List<PlanOptimizer> optimizers;

  private int planFragmentIndex = 0;

  public DistributionPlanner(Analysis analysis, LogicalQueryPlan logicalPlan) {
    this.analysis = analysis;
    this.logicalPlan = logicalPlan;
    this.context = logicalPlan.getContext();
    this.optimizers = Collections.singletonList(new LimitOffsetPushDown());
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
        new NodeGroupContext(
            context,
            analysis.getStatement() instanceof QueryStatement
                && (((QueryStatement) analysis.getStatement()).isAlignByDevice()),
            root);
    PlanNode newRoot = adder.visit(root, nodeGroupContext);
    adjustUpStream(newRoot, nodeGroupContext);
    return newRoot;
  }

  /**
   * Adjust upStream of exchangeNodes, generate {@link
   * org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.IdentitySinkNode} or {@link
   * org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.ShuffleSinkNode} for the children of
   * ExchangeNodes with Same DataRegion.
   */
  private void adjustUpStream(PlanNode root, NodeGroupContext context) {
    if (!context.hasExchangeNode) {
      return;
    }

    final boolean needShuffleSinkNode =
        analysis.getStatement() instanceof QueryStatement
            && needShuffleSinkNode((QueryStatement) analysis.getStatement(), context);

    adjustUpStreamHelper(root, new HashMap<>(), needShuffleSinkNode, context);
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
            context.getNodeDistribution(exchangeNode.getChild().getPlanNodeId()).region;
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
    return nodeGroupContext.isAlignByDevice()
        && orderByComponent != null
        && !(orderByComponent.getSortItemList().isEmpty()
            || orderByComponent.getSortItemList().get(0).getSortKey().equals(SortKey.DEVICE));
  }

  public PlanNode optimize(PlanNode rootWithExchange) {
    if (analysis.getStatement() != null && analysis.getStatement().isQuery()) {
      for (PlanOptimizer optimizer : optimizers) {
        rootWithExchange = optimizer.optimize(rootWithExchange, analysis, context);
      }
    }
    return rootWithExchange;
  }

  public SubPlan splitFragment(PlanNode root) {
    FragmentBuilder fragmentBuilder = new FragmentBuilder(context);
    return fragmentBuilder.splitToSubPlan(root);
  }

  public DistributedQueryPlan planFragments() {
    PlanNode rootAfterRewrite = rewriteSource();
    PlanNode rootWithExchange = addExchangeNode(rootAfterRewrite);
    if (analysis.getStatement() != null && analysis.getStatement().isQuery()) {
      analysis
          .getRespDatasetHeader()
          .setColumnToTsBlockIndexMap(rootWithExchange.getOutputColumnNames());
    }
    PlanNode optimizedRootWithExchange = optimize(rootWithExchange);
    SubPlan subPlan = splitFragment(optimizedRootWithExchange);
    // Mark the root Fragment of root SubPlan as `root`
    subPlan.getPlanFragment().setRoot(true);
    List<FragmentInstance> fragmentInstances = planFragmentInstances(subPlan);
    // Only execute this step for READ operation
    if (context.getQueryType() == QueryType.READ) {
      SetSinkForRootInstance(subPlan, fragmentInstances);
    }
    return new DistributedQueryPlan(
        logicalPlan.getContext(), subPlan, subPlan.getPlanFragmentList(), fragmentInstances);
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
  public void SetSinkForRootInstance(SubPlan subPlan, List<FragmentInstance> instances) {
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
    private MPPQueryContext context;

    public FragmentBuilder(MPPQueryContext context) {
      this.context = context;
    }

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
