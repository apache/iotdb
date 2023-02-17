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

import org.apache.commons.lang3.Validate;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.IFragmentParallelPlaner;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.MultiChildrenSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.SinkNode;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.ShowQueriesStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DistributionPlanner {
  private Analysis analysis;
  private MPPQueryContext context;
  private LogicalQueryPlan logicalPlan;

  public DistributionPlanner(Analysis analysis, LogicalQueryPlan logicalPlan) {
    this.analysis = analysis;
    this.logicalPlan = logicalPlan;
    this.context = logicalPlan.getContext();
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
    adjustUpStream(nodeGroupContext);
    return newRoot;
  }

  /**
   * Adjust upStream of exchangeNodes, generate {@link
   * org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.IdentitySinkNode} or {@link
   * org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.ShuffleSinkNode} for the children of
   * ExchangeNodes with Same DataRegion.
   */
  private void adjustUpStream(NodeGroupContext context) {
    if(context.exchangeNodes.isEmpty()) {
      return;
    }

    // group children of ExchangeNodes
    Map<TRegionReplicaSet, List<PlanNode>> nodeGroups = new HashMap<>();
    context.exchangeNodes.forEach(
        exchangeNode ->
            nodeGroups
                .computeIfAbsent(
                    context.getNodeDistribution(exchangeNode.getChild().getPlanNodeId()).region,
                    exchangeNodes -> new ArrayList<>())
                .add(exchangeNode.getChild()));

    // add IdentitySinkNode/ShuffleSinkNode as parent for nodes of each group
    nodeGroups
        .values()
        .forEach(
            planNodeList -> {
              MultiChildrenSinkNode parent =
                  analysis.getStatement() instanceof QueryStatement
                          && analysis.getMergeOrderParameter() != null
                          && !analysis.getMergeOrderParameter().isEmpty()
                          && analysis.getMergeOrderParameter().getSortItemList().get(0).getSortKey()
                              == SortKey.DEVICE
                      ? new IdentitySinkNode(context.queryContext.getQueryId().genPlanNodeId())
                      : new ShuffleSinkNode(context.queryContext.getQueryId().genPlanNodeId());
              parent.addChildren(planNodeList);
              // we put the parent in list to get it quickly by dataRegion of one ExchangeNode
              planNodeList.add(parent);
            });

    // add child for each ExchangeNode,
    // the child is IdentitySinkNode/ShuffleSinkNode we generate last step
    Map<TRegionReplicaSet, Integer> visitedCount = new HashMap<>();

    context.exchangeNodes.forEach(
        exchangeNode -> {
          TRegionReplicaSet regionOfChild = context.getNodeDistribution(exchangeNode.getChild().getPlanNodeId()).region;
          visitedCount.compute(regionOfChild, (region, count) -> (count == null) ? 0 : count + 1);
          List<PlanNode> planNodeList =
              nodeGroups.get(regionOfChild);
          exchangeNode.addChild(planNodeList.get(planNodeList.size() - 1));
          exchangeNode.setIndexOfUpstreamSinkHandle(visitedCount.get(regionOfChild));
        });
  }

  public SubPlan splitFragment(PlanNode root) {
    FragmentBuilder fragmentBuilder = new FragmentBuilder(context);
    return fragmentBuilder.splitToSubPlan(root);
  }

  public DistributedQueryPlan planFragments() {
    PlanNode rootAfterRewrite = rewriteSource();
    PlanNode rootWithExchange = addExchangeNode(rootAfterRewrite);
    if (analysis.getStatement() instanceof QueryStatement
        || analysis.getStatement() instanceof ShowQueriesStatement) {
      analysis
          .getRespDatasetHeader()
          .setColumnToTsBlockIndexMap(rootWithExchange.getOutputColumnNames());
    }
    SubPlan subPlan = splitFragment(rootWithExchange);
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

    FragmentSinkNode sinkNode = new FragmentSinkNode(context.getQueryId().genPlanNodeId());
    sinkNode.setDownStream(
        context.getLocalDataBlockEndpoint(),
        context.getResultNodeContext().getVirtualFragmentInstanceId(),
        context.getResultNodeContext().getVirtualResultNodeId());
    sinkNode.setChild(rootInstance.getFragment().getPlanNodeTree());
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
      splitToSubPlan(root, rootSubPlan);
      return rootSubPlan;
    }

    private void splitToSubPlan(PlanNode root, SubPlan subPlan) {
      // TODO: (xingtanzjr) we apply no action for IWritePlanNode currently
      if (root instanceof WritePlanNode) {
        return;
      }
      if (root instanceof ExchangeNode) {
        // We add a FragmentSinkNode for newly created PlanFragment
        ExchangeNode exchangeNode = (ExchangeNode) root;
        Validate.isTrue(exchangeNode.getChild() instanceof MultiChildrenSinkNode, "check..");
        MultiChildrenSinkNode sinkNode = (MultiChildrenSinkNode) (exchangeNode.getChild());

        // Record the source node info in the ExchangeNode so that we can keep the connection of
        // these nodes/fragments
        exchangeNode.setRemoteSourceNode(sinkNode);
        // We cut off the subtree to make the ExchangeNode as the leaf node of current PlanFragment
        exchangeNode.cleanChildren();

        // Build the child SubPlan Tree
        SubPlan childSubPlan = createSubPlan(sinkNode);
        splitToSubPlan(sinkNode, childSubPlan);

        subPlan.addChild(childSubPlan);
        return;
      }
      for (PlanNode child : root.getChildren()) {
        splitToSubPlan(child, subPlan);
      }
    }

    private SubPlan createSubPlan(PlanNode root) {
      PlanFragment fragment = new PlanFragment(getNextFragmentId(), root);
      return new SubPlan(fragment);
    }
  }
}
