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

package org.apache.iotdb.db.mpp.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.AbstractSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LastQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MultiChildNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.DeleteDataNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class ExchangeNodeAdder extends PlanVisitor<PlanNode, NodeGroupContext> {
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
                exchangeNode.setChild(child);
                exchangeNode.setOutputColumnNames(child.getOutputColumnNames());
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
  public PlanNode visitDeleteData(DeleteDataNode node, NodeGroupContext context) {
    context.putNodeDistribution(
        node.getPlanNodeId(),
        new NodeDistribution(NodeDistributionType.NO_CHILD, node.getRegionReplicaSet()));
    return node;
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
  public PlanNode visitLastQueryMerge(LastQueryMergeNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitTimeJoin(TimeJoinNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitRowBasedSeriesAggregate(AggregationNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  @Override
  public PlanNode visitGroupByLevel(GroupByLevelNode node, NodeGroupContext context) {
    return processMultiChildNode(node, context);
  }

  private PlanNode processMultiChildNode(MultiChildNode node, NodeGroupContext context) {
    MultiChildNode newNode = (MultiChildNode) node.clone();
    List<PlanNode> visitedChildren = new ArrayList<>();
    node.getChildren()
        .forEach(
            child -> {
              visitedChildren.add(visit(child, context));
            });

    TRegionReplicaSet dataRegion = calculateDataRegionByChildren(visitedChildren, context);
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
            exchangeNode.setOutputColumnNames(child.getOutputColumnNames());
            newNode.addChild(exchangeNode);
          } else {
            newNode.addChild(child);
          }
        });
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
    // Step 2: return the RegionReplicaSet with max count
    return Collections.max(groupByRegion.entrySet(), Map.Entry.comparingByValue()).getKey();
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
