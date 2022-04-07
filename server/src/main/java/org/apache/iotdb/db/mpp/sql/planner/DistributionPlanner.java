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
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.planner.plan.*;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.*;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesScanNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class DistributionPlanner {
  private Analysis analysis;
  private LogicalQueryPlan logicalPlan;

  private int planFragmentIndex = 0;

  public DistributionPlanner(Analysis analysis, LogicalQueryPlan logicalPlan) {
    this.analysis = analysis;
    this.logicalPlan = logicalPlan;
  }

  public PlanNode rewriteSource() {
    SourceRewriter rewriter = new SourceRewriter();
    return rewriter.visit(logicalPlan.getRootNode(), new DistributionPlanContext());
  }

  public PlanNode addExchangeNode(PlanNode root) {
    ExchangeNodeAdder adder = new ExchangeNodeAdder();
    return adder.visit(root, new NodeGroupContext());
  }

  public SubPlan splitFragment(PlanNode root) {
    FragmentBuilder fragmentBuilder = new FragmentBuilder();
    return fragmentBuilder.splitToSubPlan(root);
  }

  public DistributedQueryPlan planFragments() {
    PlanNode rootAfterRewrite = rewriteSource();
    System.out.println("===== Step 2: Partition SourceNode =====");
    System.out.println(PlanNodeUtil.nodeToString(rootAfterRewrite));
    PlanNode rootWithExchange = addExchangeNode(rootAfterRewrite);
    System.out.println("===== Step 3: Add ExchangeNode =====");
    System.out.println(PlanNodeUtil.nodeToString(rootWithExchange));
    SubPlan subPlan = splitFragment(rootWithExchange);
    List<FragmentInstance> fragmentInstances = planFragmentInstances(subPlan);
    return new DistributedQueryPlan(
        logicalPlan.getContext(), subPlan, subPlan.getPlanFragmentList(), fragmentInstances);
  }

  // Convert fragment to detailed instance
  // And for parallel-able fragment, clone it into several instances with different params.
  public List<FragmentInstance> planFragmentInstances(SubPlan subPlan) {
    IFragmentParallelPlaner parallelPlaner = new SimpleFragmentParallelPlanner(subPlan);
    return parallelPlaner.parallelPlan();
  }

  private PlanFragmentId getNextFragmentId() {
    return new PlanFragmentId(this.logicalPlan.getContext().getQueryId(), this.planFragmentIndex++);
  }

  private class SourceRewriter extends SimplePlanNodeRewriter<DistributionPlanContext> {

    // TODO: (xingtanzjr) implement the method visitDeviceMergeNode()
    public PlanNode visitDeviceMerge(TimeJoinNode node, DistributionPlanContext context) {
      return null;
    }

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
            split.setDataRegionReplicaSet(dataRegion);
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
          sources.stream().collect(Collectors.groupingBy(SeriesScanNode::getDataRegionReplicaSet));
      // Step 3: For the source nodes which belong to same data region, add a TimeJoinNode for them
      // and make the
      // new TimeJoinNode as the child of current TimeJoinNode
      // TODO: (xingtanzjr) optimize the procedure here to remove duplicated TimeJoinNode
      sourceGroup.forEach(
          (dataRegion, seriesScanNodes) -> {
            if (seriesScanNodes.size() == 1) {
              root.addChild(seriesScanNodes.get(0));
            } else {
              // We clone a TimeJoinNode from root to make the params to be consistent
              TimeJoinNode parentOfGroup = (TimeJoinNode) root.clone();
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

  private class DistributionPlanContext {}

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

    public PlanNode visitSeriesScan(SeriesScanNode node, NodeGroupContext context) {
      context.putNodeDistribution(
          node.getPlanNodeId(),
          new NodeDistribution(NodeDistributionType.NO_CHILD, node.getDataRegionReplicaSet()));
      return node.clone();
    }

    public PlanNode visitSeriesAggregate(SeriesAggregateScanNode node, NodeGroupContext context) {
      context.putNodeDistribution(
          node.getPlanNodeId(),
          new NodeDistribution(NodeDistributionType.NO_CHILD, node.getDataRegionReplicaSet()));
      return node.clone();
    }

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
            if (!dataRegion.equals(context.getNodeDistribution(child.getPlanNodeId()).dataRegion)) {
              ExchangeNode exchangeNode = new ExchangeNode(PlanNodeIdAllocator.generateId());
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
      return context.getNodeDistribution(children.get(0).getPlanNodeId()).dataRegion;
    }

    private boolean nodeDistributionIsSame(List<PlanNode> children, NodeGroupContext context) {
      // The size of children here should always be larger than 0, or our code has Bug.
      NodeDistribution first = context.getNodeDistribution(children.get(0).getPlanNodeId());
      for (int i = 1; i < children.size(); i++) {
        NodeDistribution next = context.getNodeDistribution(children.get(i).getPlanNodeId());
        if (first.dataRegion == null || !first.dataRegion.equals(next.dataRegion)) {
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
    Map<PlanNodeId, NodeDistribution> nodeDistribution;

    public NodeGroupContext() {
      nodeDistribution = new HashMap<>();
    }

    public void putNodeDistribution(PlanNodeId nodeId, NodeDistribution distribution) {
      this.nodeDistribution.put(nodeId, distribution);
    }

    public NodeDistribution getNodeDistribution(PlanNodeId nodeId) {
      return this.nodeDistribution.get(nodeId);
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
    private RegionReplicaSet dataRegion;

    private NodeDistribution(NodeDistributionType type, RegionReplicaSet dataRegion) {
      this.type = type;
      this.dataRegion = dataRegion;
    }
  }

  private class FragmentBuilder {
    public SubPlan splitToSubPlan(PlanNode root) {
      SubPlan rootSubPlan = createSubPlan(root);
      splitToSubPlan(root, rootSubPlan);
      return rootSubPlan;
    }

    private void splitToSubPlan(PlanNode root, SubPlan subPlan) {
      if (root instanceof ExchangeNode) {
        // We add a FragmentSinkNode for newly created PlanFragment
        ExchangeNode exchangeNode = (ExchangeNode) root;
        FragmentSinkNode sinkNode = new FragmentSinkNode(PlanNodeIdAllocator.generateId());
        sinkNode.setChild(exchangeNode.getChild());
        sinkNode.setDownStreamNode(exchangeNode);
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
