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

package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.AbstractSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaQueryOrderByHeatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TopKNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryTransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class ExchangeNodeAdder extends PlanVisitor<PlanNode, NodeGroupContext> {

  private final Analysis analysis;

  public ExchangeNodeAdder(Analysis analysis) {
    this.analysis = analysis;
  }

  @Override
  public PlanNode visitPlan(PlanNode node, NodeGroupContext context) {
    // TODO: (xingtanzjr) we apply no action for IWritePlanNode currently
    if (node instanceof WritePlanNode) {
      return node;
    }
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
  public PlanNode visitSchemaQueryMerge(SchemaQueryMergeNode node, NodeGroupContext context) {
    return internalVisitSchemaMerge(node, context);
  }

  private PlanNode internalVisitSchemaMerge(
      AbstractSchemaMergeNode node, NodeGroupContext context) {
    node.getChildren().forEach(child -> visit(child, context));
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
                exchangeNode.setChild(child);
                exchangeNode.setOutputColumnNames(child.getOutputColumnNames());
                context.hasExchangeNode = true;
                newNode.addChild(exchangeNode);
              } else {
                newNode.addChild(child);
              }
            });
    return newNode;
  }

  @Override
  public PlanNode visitCountMerge(CountSchemaMergeNode node, NodeGroupContext context) {
    return internalVisitSchemaMerge(node, context);
  }

  @Override
  public PlanNode visitSchemaFetchMerge(SchemaFetchMergeNode node, NodeGroupContext context) {
    return internalVisitSchemaMerge(node, context);
  }

  @Override
  public PlanNode visitSchemaQueryScan(SchemaQueryScanNode node, NodeGroupContext context) {
    return processNoChildSourceNode(node, context);
  }

  @Override
  public PlanNode visitSchemaFetchScan(SchemaFetchScanNode node, NodeGroupContext context) {
    return processNoChildSourceNode(node, context);
  }

  @Override
  public PlanNode visitSeriesScan(SeriesScanNode node, NodeGroupContext context) {
    return processNoChildSourceNode(node, context);
  }

  @Override
  public PlanNode visitAlignedSeriesScan(AlignedSeriesScanNode node, NodeGroupContext context) {
    return processNoChildSourceNode(node, context);
  }

  @Override
  public PlanNode visitLastQueryScan(LastQueryScanNode node, NodeGroupContext context) {
    return processNoChildSourceNode(node, context);
  }

  @Override
  public PlanNode visitAlignedLastQueryScan(
      AlignedLastQueryScanNode node, NodeGroupContext context) {
    return processNoChildSourceNode(node, context);
  }

  @Override
  public PlanNode visitSeriesAggregationScan(
      SeriesAggregationScanNode node, NodeGroupContext context) {
    return processNoChildSourceNode(node, context);
  }

  @Override
  public PlanNode visitAlignedSeriesAggregationScan(
      AlignedSeriesAggregationScanNode node, NodeGroupContext context) {
    return processNoChildSourceNode(node, context);
  }

  private PlanNode processNoChildSourceNode(SourceNode node, NodeGroupContext context) {
    context.putNodeDistribution(
        node.getPlanNodeId(),
        new NodeDistribution(NodeDistributionType.NO_CHILD, node.getRegionReplicaSet()));
    return node.clone();
  }

  @Override
  public PlanNode visitDeviceView(DeviceViewNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitDeviceMerge(DeviceMergeNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitSingleDeviceView(SingleDeviceViewNode node, NodeGroupContext context) {
    return processOneChildNode(node, context);
  }

  @Override
  public PlanNode visitMergeSort(MergeSortNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitTopK(TopKNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitLastQueryMerge(LastQueryMergeNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitLastQueryCollect(LastQueryCollectNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitLastQuery(LastQueryNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitLastQueryTransform(LastQueryTransformNode node, NodeGroupContext context) {
    return processOneChildNode(node, context);
  }

  @Override
  public PlanNode visitTimeJoin(TimeJoinNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitAggregation(AggregationNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitSchemaQueryOrderByHeat(
      SchemaQueryOrderByHeatNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitGroupByLevel(GroupByLevelNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitTransform(TransformNode node, NodeGroupContext context) {
    return processOneChildNode(node, context);
  }

  @Override
  public PlanNode visitFilter(FilterNode node, NodeGroupContext context) {
    return processOneChildNode(node, context);
  }

  @Override
  public PlanNode visitGroupByTag(GroupByTagNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitHorizontallyConcat(HorizontallyConcatNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitSort(SortNode node, NodeGroupContext context) {
    return processOneChildNode(node, context);
  }

  @Override
  public PlanNode visitLimit(LimitNode node, NodeGroupContext context) {
    return processOneChildNode(node, context);
  }

  private PlanNode processMultiChildNode(MultiChildProcessNode node, NodeGroupContext context) {
    if (analysis.isVirtualSource()) {
      return processMultiChildNodeByLocation(node, context);
    }

    MultiChildProcessNode newNode = (MultiChildProcessNode) node.clone();
    List<PlanNode> visitedChildren =
        node.getChildren().stream()
            .map(child -> visit(child, context))
            .collect(Collectors.toList());

    // DataRegion which node locates
    TRegionReplicaSet dataRegion;
    boolean isChildrenDistributionSame = nodeDistributionIsSame(visitedChildren, context);
    NodeDistributionType distributionType =
        isChildrenDistributionSame
            ? NodeDistributionType.SAME_WITH_ALL_CHILDREN
            : NodeDistributionType.SAME_WITH_SOME_CHILD;
    if (context.isAlignByDevice()) {
      // For align by device,
      // if dataRegions of children are the same, we set child's dataRegion to this node,
      // else we set the selected mostlyUsedDataRegion to this node
      dataRegion =
          isChildrenDistributionSame
              ? context.getNodeDistribution(visitedChildren.get(0).getPlanNodeId()).region
              : context.getMostlyUsedDataRegion();
      context.putNodeDistribution(
          newNode.getPlanNodeId(), new NodeDistribution(distributionType, dataRegion));
    } else {
      dataRegion = calculateDataRegionByChildren(visitedChildren, context);
      context.putNodeDistribution(
          newNode.getPlanNodeId(), new NodeDistribution(distributionType, dataRegion));
    }

    // If the distributionType of all the children are same, no ExchangeNode need to be added.
    if (distributionType == NodeDistributionType.SAME_WITH_ALL_CHILDREN) {
      newNode.setChildren(visitedChildren);
      return newNode;
    }

    // optimize `order by time limit N align by device` query,
    // to ensure that the number of ExchangeNode equals to DataRegionNum but not equals to DeviceNum
    if (node instanceof TopKNode) {
      return processTopNode(node, visitedChildren, context, newNode, dataRegion);
    }

    // Otherwise, we need to add ExchangeNode for the child whose DataRegion is different from the
    // parent.
    for (PlanNode child : visitedChildren) {
      if (!dataRegion.equals(context.getNodeDistribution(child.getPlanNodeId()).region)) {
        if (child instanceof SingleDeviceViewNode) {
          ((SingleDeviceViewNode) child).setCacheOutputColumnNames(true);
        }
        ExchangeNode exchangeNode =
            new ExchangeNode(context.queryContext.getQueryId().genPlanNodeId());
        exchangeNode.setChild(child);
        exchangeNode.setOutputColumnNames(child.getOutputColumnNames());
        context.hasExchangeNode = true;
        newNode.addChild(exchangeNode);
      } else {
        newNode.addChild(child);
      }
    }
    return newNode;
  }

  private PlanNode processMultiChildNodeByLocation(
      MultiChildProcessNode node, NodeGroupContext context) {
    MultiChildProcessNode newNode = (MultiChildProcessNode) node.clone();

    List<PlanNode> children = node.getChildren();
    newNode.addChild(children.get(0));
    for (int i = 1; i < children.size(); i++) {
      PlanNode child = children.get(i);
      ExchangeNode exchangeNode =
          new ExchangeNode(context.queryContext.getQueryId().genPlanNodeId());
      exchangeNode.setChild(child);
      exchangeNode.setOutputColumnNames(child.getOutputColumnNames());
      context.hasExchangeNode = true;
      newNode.addChild(exchangeNode);
    }
    return newNode;
  }

  private PlanNode processTopNode(
      MultiChildProcessNode node,
      List<PlanNode> visitedChildren,
      NodeGroupContext context,
      MultiChildProcessNode newNode,
      TRegionReplicaSet dataRegion) {
    TopKNode rootNode = (TopKNode) node;
    Map<TRegionReplicaSet, TopKNode> regionTopKNodeMap = new HashMap<>();
    for (PlanNode child : visitedChildren) {
      TRegionReplicaSet region = context.getNodeDistribution(child.getPlanNodeId()).region;
      regionTopKNodeMap
          .computeIfAbsent(
              region,
              k -> {
                TopKNode childTopKNode =
                    new TopKNode(
                        context.queryContext.getQueryId().genPlanNodeId(),
                        rootNode.getTopValue(),
                        rootNode.getMergeOrderParameter(),
                        rootNode.getOutputColumnNames());
                context.putNodeDistribution(
                    childTopKNode.getPlanNodeId(),
                    new NodeDistribution(NodeDistributionType.SAME_WITH_ALL_CHILDREN, region));
                return childTopKNode;
              })
          .addChild(child);
    }

    for (Map.Entry<TRegionReplicaSet, TopKNode> entry : regionTopKNodeMap.entrySet()) {
      TRegionReplicaSet topKNodeLocatedRegion = entry.getKey();
      TopKNode topKNode = entry.getValue();

      if (!dataRegion.equals(topKNodeLocatedRegion)) {
        ExchangeNode exchangeNode =
            new ExchangeNode(context.queryContext.getQueryId().genPlanNodeId());
        exchangeNode.setChild(topKNode);
        exchangeNode.setOutputColumnNames(topKNode.getOutputColumnNames());
        context.hasExchangeNode = true;
        newNode.addChild(exchangeNode);
      } else {
        newNode.addChild(topKNode);
      }
    }
    return newNode;
  }

  @Override
  public PlanNode visitSlidingWindowAggregation(
      SlidingWindowAggregationNode node, NodeGroupContext context) {
    return processOneChildNode(node, context);
  }

  private PlanNode processOneChildNode(PlanNode node, NodeGroupContext context) {
    PlanNode newNode = node.clone();
    PlanNode child = visit(node.getChildren().get(0), context);
    newNode.addChild(child);
    TRegionReplicaSet dataRegion = context.getNodeDistribution(child.getPlanNodeId()).region;
    context.putNodeDistribution(
        newNode.getPlanNodeId(),
        new NodeDistribution(NodeDistributionType.SAME_WITH_ALL_CHILDREN, dataRegion));
    return newNode;
  }

  private TRegionReplicaSet calculateDataRegionByChildren(
      List<PlanNode> children, NodeGroupContext context) {

    // Step 1: calculate the count of children group by DataRegion.
    Map<TRegionReplicaSet, Long> groupByRegion =
        children.stream()
            .collect(
                Collectors.groupingBy(
                    child -> {
                      TRegionReplicaSet region =
                          context.getNodeDistribution(child.getPlanNodeId()).region;
                      if (region == null
                          && context.getNodeDistribution(child.getPlanNodeId()).type
                              == NodeDistributionType.SAME_WITH_ALL_CHILDREN) {
                        return calculateSchemaRegionByChildren(child.getChildren(), context);
                      }
                      return region;
                    },
                    Collectors.counting()));

    if (groupByRegion.size() == 1) {
      return groupByRegion.keySet().iterator().next();
    }

    // Step 2: return the RegionReplicaSet with max node count
    long maxCount = -1;
    TRegionReplicaSet result = DataPartition.NOT_ASSIGNED;
    for (Map.Entry<TRegionReplicaSet, Long> entry : groupByRegion.entrySet()) {
      TRegionReplicaSet region = entry.getKey();
      if (DataPartition.NOT_ASSIGNED.equals(region)) {
        continue;
      }
      if (region.equals(context.queryContext.getMainFragmentLocatedRegion())) {
        return context.queryContext.getMainFragmentLocatedRegion();
      }
      if (region.equals(context.getMostlyUsedDataRegion())) {
        return region;
      }
      long planNodeCount = entry.getValue();
      if (planNodeCount > maxCount) {
        maxCount = planNodeCount;
        result = region;
      } else if (planNodeCount == maxCount
          && region.getRegionId().getId() < result.getRegionId().getId()) {
        result = region;
      }
    }
    return result;
  }

  private TRegionReplicaSet calculateSchemaRegionByChildren(
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
