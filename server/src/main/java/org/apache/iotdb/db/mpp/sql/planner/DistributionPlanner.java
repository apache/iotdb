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
package org.apache.iotdb.db.mpp.sql.planner;

import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.sql.planner.plan.IFragmentParallelPlaner;
import org.apache.iotdb.db.mpp.sql.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.PlanFragment;
import org.apache.iotdb.db.mpp.sql.planner.plan.SimpleFragmentParallelPlanner;
import org.apache.iotdb.db.mpp.sql.planner.plan.SubPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.SimplePlanNodeRewriter;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaMergeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.SchemaScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class DistributionPlanner {
  private Analysis analysis;
  private MPPQueryContext context;
  private LogicalQueryPlan logicalPlan;

  private int planFragmentIndex = 0;

  public DistributionPlanner(Analysis analysis, LogicalQueryPlan logicalPlan) {
    this.analysis = analysis;
    this.logicalPlan = logicalPlan;
    this.context = logicalPlan.getContext();
  }

  public PlanNode rewriteSource() {
    SourceRewriter rewriter = new SourceRewriter();
    return rewriter.visit(logicalPlan.getRootNode(), new DistributionPlanContext(context));
  }

  public PlanNode addExchangeNode(PlanNode root) {
    ExchangeNodeAdder adder = new ExchangeNodeAdder();
    return adder.visit(root, new NodeGroupContext(context));
  }

  public SubPlan splitFragment(PlanNode root) {
    FragmentBuilder fragmentBuilder = new FragmentBuilder(context);
    return fragmentBuilder.splitToSubPlan(root);
  }

  public DistributedQueryPlan planFragments() {
    PlanNode rootAfterRewrite = rewriteSource();
    PlanNode rootWithExchange = addExchangeNode(rootAfterRewrite);
    SubPlan subPlan = splitFragment(rootWithExchange);
    List<FragmentInstance> fragmentInstances = planFragmentInstances(subPlan);
    SetSinkForRootInstance(subPlan, fragmentInstances);
    return new DistributedQueryPlan(
        logicalPlan.getContext(), subPlan, subPlan.getPlanFragmentList(), fragmentInstances);
  }

  // Convert fragment to detailed instance
  // And for parallel-able fragment, clone it into several instances with different params.
  public List<FragmentInstance> planFragmentInstances(SubPlan subPlan) {
    IFragmentParallelPlaner parallelPlaner =
        new SimpleFragmentParallelPlanner(subPlan, analysis, context);
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
        context.getHostEndpoint(),
        context.getResultNodeContext().getVirtualFragmentInstanceId(),
        context.getResultNodeContext().getVirtualResultNodeId());
    sinkNode.setChild(rootInstance.getFragment().getRoot());
    context
        .getResultNodeContext()
        .setUpStream(
            rootInstance.getHostEndpoint(), rootInstance.getId(), sinkNode.getPlanNodeId());
    rootInstance.getFragment().setRoot(sinkNode);
  }

  private PlanFragmentId getNextFragmentId() {
    return new PlanFragmentId(this.logicalPlan.getContext().getQueryId(), this.planFragmentIndex++);
  }

  private class SourceRewriter extends SimplePlanNodeRewriter<DistributionPlanContext> {

    // TODO: (xingtanzjr) implement the method visitDeviceMergeNode()
    public PlanNode visitDeviceMerge(TimeJoinNode node, DistributionPlanContext context) {
      return null;
    }

    @Override
    public PlanNode visitMetaMerge(SchemaMergeNode node, DistributionPlanContext context) {
      SchemaMergeNode root = (SchemaMergeNode) node.clone();
      SchemaScanNode seed = (SchemaScanNode) node.getChildren().get(0);
      TreeSet<RegionReplicaSet> schemaRegions =
          new TreeSet<>(Comparator.comparingInt(region -> region.getConsensusGroupId().getId()));
      analysis
          .getSchemaPartitionInfo()
          .getSchemaPartitionMap()
          .forEach(
              (storageGroup, deviceGroup) -> {
                deviceGroup.forEach(
                    (deviceGroupId, schemaRegionReplicaSet) ->
                        schemaRegions.add(schemaRegionReplicaSet));
              });
      int count = schemaRegions.size();
      schemaRegions.forEach(
          region -> {
            SchemaScanNode metaScanNode = (SchemaScanNode) seed.clone();
            metaScanNode.setRegionReplicaSet(region);
            if (count > 1) {
              metaScanNode.setLimit(metaScanNode.getOffset() + metaScanNode.getLimit());
              metaScanNode.setOffset(0);
            }
            root.addChild(metaScanNode);
          });
      return root;
    }

    @Override
    public PlanNode visitTimeJoin(TimeJoinNode node, DistributionPlanContext context) {
      TimeJoinNode root = (TimeJoinNode) node.clone();

      // Step 1: Get all source nodes. For the node which is not source, add it as the child of
      // current TimeJoinNode
      List<SeriesScanNode> sources = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        if (child instanceof SeriesScanNode) {
          // If the child is SeriesScanNode, we need to check whether this node should be seperated
          // into several splits.
          SeriesScanNode handle = (SeriesScanNode) child;
          List<RegionReplicaSet> dataDistribution =
              analysis.getPartitionInfo(handle.getSeriesPath(), handle.getTimeFilter());
          // If the size of dataDistribution is m, this SeriesScanNode should be seperated into m
          // SeriesScanNode.
          for (RegionReplicaSet dataRegion : dataDistribution) {
            SeriesScanNode split = (SeriesScanNode) handle.clone();
            split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
            split.setRegionReplicaSet(dataRegion);
            sources.add(split);
          }
        } else if (child instanceof SeriesAggregateScanNode) {
          // TODO: (xingtanzjr) We should do the same thing for SeriesAggregateScanNode. Consider to
          // make SeriesAggregateScanNode
          // and SeriesScanNode to derived from the same parent Class because they have similar
          // process logic in many scenarios
        } else {
          // In a general logical query plan, the children of TimeJoinNode should only be
          // SeriesScanNode or SeriesAggregateScanNode
          // So this branch should not be touched.
          root.addChild(visit(child, context));
        }
      }

      // Step 2: For the source nodes, group them by the DataRegion.
      Map<RegionReplicaSet, List<SeriesScanNode>> sourceGroup =
          sources.stream().collect(Collectors.groupingBy(SeriesScanNode::getRegionReplicaSet));
      // Step 3: For the source nodes which belong to same data region, add a TimeJoinNode for them
      // and make the
      // new TimeJoinNode as the child of current TimeJoinNode
      // TODO: (xingtanzjr) optimize the procedure here to remove duplicated TimeJoinNode
      sourceGroup.forEach(
          (dataRegion, seriesScanNodes) -> {
            if (seriesScanNodes.size() == 1) {
              root.addChild(seriesScanNodes.get(0));
            } else {
              // We clone a TimeJoinNode from root to make the params to be consistent.
              // But we need to assign a new ID to it
              TimeJoinNode parentOfGroup = (TimeJoinNode) root.clone();
              root.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
              seriesScanNodes.forEach(parentOfGroup::addChild);
              root.addChild(parentOfGroup);
            }
          });

      return root;
    }

    public PlanNode visit(PlanNode node, DistributionPlanContext context) {
      return node.accept(this, context);
    }
  }

  private class DistributionPlanContext {
    private MPPQueryContext queryContext;

    public DistributionPlanContext(MPPQueryContext queryContext) {
      this.queryContext = queryContext;
    }
  }

  private class ExchangeNodeAdder extends PlanVisitor<PlanNode, NodeGroupContext> {
    @Override
    public PlanNode visitPlan(PlanNode node, NodeGroupContext context) {
      // Visit all the children of current node
      List<PlanNode> children =
          node.getChildren().stream()
              .map(child -> child.accept(this, context))
              .collect(toImmutableList());

      // Calculate the node distribution info according to its children

      // Put the node distribution info into context
      // NOTICE: we will only process the PlanNode which has only 1 child here. For the other
      // PlanNode, we need to process
      // them with special method
      context.putNodeDistribution(
          node.getPlanNodeId(),
          new NodeDistribution(NodeDistributionType.SAME_WITH_ALL_CHILDREN, null));

      return node.cloneWithChildren(children);
    }

    @Override
    public PlanNode visitMetaMerge(SchemaMergeNode node, NodeGroupContext context) {
      node.getChildren()
          .forEach(
              child -> {
                visit(child, context);
              });
      NodeDistribution nodeDistribution =
          new NodeDistribution(NodeDistributionType.DIFFERENT_FROM_ALL_CHILDREN);
      PlanNode newNode = node.clone();
      nodeDistribution.region = calculateSchemaRegionByChildren(node.getChildren(), context);
      context.putNodeDistribution(newNode.getPlanNodeId(), nodeDistribution);
      node.getChildren()
          .forEach(
              child -> {
                if (!nodeDistribution.region.equals(
                    context.getNodeDistribution(child.getPlanNodeId()).region)) {
                  ExchangeNode exchangeNode =
                      new ExchangeNode(context.queryContext.getQueryId().genPlanNodeId());
                  exchangeNode.addChild(child);
                  newNode.addChild(exchangeNode);
                } else {
                  newNode.addChild(child);
                }
              });
      return newNode;
    }

    @Override
    public PlanNode visitMetaScan(SchemaScanNode node, NodeGroupContext context) {
      NodeDistribution nodeDistribution = new NodeDistribution(NodeDistributionType.NO_CHILD);
      nodeDistribution.region = node.getRegionReplicaSet();
      context.putNodeDistribution(node.getPlanNodeId(), nodeDistribution);
      return node;
    }

    @Override
    public PlanNode visitSeriesScan(SeriesScanNode node, NodeGroupContext context) {
      context.putNodeDistribution(
          node.getPlanNodeId(),
          new NodeDistribution(NodeDistributionType.NO_CHILD, node.getRegionReplicaSet()));
      return node.clone();
    }

    @Override
    public PlanNode visitSeriesAggregate(SeriesAggregateScanNode node, NodeGroupContext context) {
      context.putNodeDistribution(
          node.getPlanNodeId(),
          new NodeDistribution(NodeDistributionType.NO_CHILD, node.getRegionReplicaSet()));
      return node.clone();
    }

    @Override
    public PlanNode visitTimeJoin(TimeJoinNode node, NodeGroupContext context) {
      TimeJoinNode newNode = (TimeJoinNode) node.clone();
      List<PlanNode> visitedChildren = new ArrayList<>();
      node.getChildren()
          .forEach(
              child -> {
                visitedChildren.add(visit(child, context));
              });

      RegionReplicaSet dataRegion = calculateDataRegionByChildren(visitedChildren, context);
      NodeDistributionType distributionType =
          nodeDistributionIsSame(visitedChildren, context)
              ? NodeDistributionType.SAME_WITH_ALL_CHILDREN
              : NodeDistributionType.SAME_WITH_SOME_CHILD;
      context.putNodeDistribution(
          newNode.getPlanNodeId(), new NodeDistribution(distributionType, dataRegion));

      // If the distributionType of all the children are same, no ExchangeNode need to be added.
      if (distributionType == NodeDistributionType.SAME_WITH_ALL_CHILDREN) {
        newNode.setChildren(visitedChildren);
        return newNode;
      }

      // Otherwise, we need to add ExchangeNode for the child whose DataRegion is different from the
      // parent.
      visitedChildren.forEach(
          child -> {
            if (!dataRegion.equals(context.getNodeDistribution(child.getPlanNodeId()).region)) {
              ExchangeNode exchangeNode =
                  new ExchangeNode(context.queryContext.getQueryId().genPlanNodeId());
              exchangeNode.setChild(child);
              newNode.addChild(exchangeNode);
            } else {
              newNode.addChild(child);
            }
          });
      return newNode;
    }

    private RegionReplicaSet calculateDataRegionByChildren(
        List<PlanNode> children, NodeGroupContext context) {
      // We always make the dataRegion of TimeJoinNode to be the same as its first child.
      // TODO: (xingtanzjr) We need to implement more suitable policies here
      return context.getNodeDistribution(children.get(0).getPlanNodeId()).region;
    }

    private RegionReplicaSet calculateSchemaRegionByChildren(
        List<PlanNode> children, NodeGroupContext context) {
      // We always make the schemaRegion of MetaMergeNode to be the same as its first child.
      return context.getNodeDistribution(children.get(0).getPlanNodeId()).region;
    }

    private boolean nodeDistributionIsSame(List<PlanNode> children, NodeGroupContext context) {
      // The size of children here should always be larger than 0, or our code has Bug.
      NodeDistribution first = context.getNodeDistribution(children.get(0).getPlanNodeId());
      for (int i = 1; i < children.size(); i++) {
        NodeDistribution next = context.getNodeDistribution(children.get(i).getPlanNodeId());
        if (first.region == null || !first.region.equals(next.region)) {
          return false;
        }
      }
      return true;
    }

    public PlanNode visit(PlanNode node, NodeGroupContext context) {
      return node.accept(this, context);
    }
  }

  private class NodeGroupContext {
    private MPPQueryContext queryContext;
    private Map<PlanNodeId, NodeDistribution> nodeDistributionMap;

    public NodeGroupContext(MPPQueryContext queryContext) {
      this.queryContext = queryContext;
      this.nodeDistributionMap = new HashMap<>();
    }

    public void putNodeDistribution(PlanNodeId nodeId, NodeDistribution distribution) {
      this.nodeDistributionMap.put(nodeId, distribution);
    }

    public NodeDistribution getNodeDistribution(PlanNodeId nodeId) {
      return this.nodeDistributionMap.get(nodeId);
    }
  }

  private enum NodeDistributionType {
    SAME_WITH_ALL_CHILDREN,
    SAME_WITH_SOME_CHILD,
    DIFFERENT_FROM_ALL_CHILDREN,
    NO_CHILD,
  }

  private class NodeDistribution {
    private NodeDistributionType type;
    private RegionReplicaSet region;

    private NodeDistribution(NodeDistributionType type, RegionReplicaSet region) {
      this.type = type;
      this.region = region;
    }

    private NodeDistribution(NodeDistributionType type) {
      this.type = type;
    }
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
      if (root instanceof ExchangeNode) {
        // We add a FragmentSinkNode for newly created PlanFragment
        ExchangeNode exchangeNode = (ExchangeNode) root;
        FragmentSinkNode sinkNode = new FragmentSinkNode(context.getQueryId().genPlanNodeId());
        sinkNode.setChild(exchangeNode.getChild());
        sinkNode.setDownStreamPlanNodeId(exchangeNode.getPlanNodeId());
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
