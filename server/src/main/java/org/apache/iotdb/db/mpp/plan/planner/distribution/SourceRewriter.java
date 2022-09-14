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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.LogicalPlanBuilder;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.SimplePlanNodeRewriter;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MultiChildNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesSourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByLevelDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class SourceRewriter extends SimplePlanNodeRewriter<DistributionPlanContext> {

  private Analysis analysis;

  public SourceRewriter(Analysis analysis) {
    this.analysis = analysis;
  }

  @Override
  public PlanNode visitDeviceView(DeviceViewNode node, DistributionPlanContext context) {
    checkArgument(
        node.getDevices().size() == node.getChildren().size(),
        "size of devices and its children in DeviceViewNode should be same");

    Set<TRegionReplicaSet> relatedDataRegions = new HashSet<>();

    List<DeviceViewSplit> deviceViewSplits = new ArrayList<>();
    // Step 1: constructs DeviceViewSplit
    for (int i = 0; i < node.getDevices().size(); i++) {
      String device = node.getDevices().get(i);
      PlanNode child = node.getChildren().get(i);
      List<TRegionReplicaSet> regionReplicaSets =
          analysis.getPartitionInfo(device, analysis.getGlobalTimeFilter());
      deviceViewSplits.add(new DeviceViewSplit(device, child, regionReplicaSets));
      relatedDataRegions.addAll(regionReplicaSets);
    }

    DeviceMergeNode deviceMergeNode =
        new DeviceMergeNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            node.getMergeOrderParameter(),
            node.getDevices());

    // Step 2: Iterate all partition and create DeviceViewNode for each region
    for (TRegionReplicaSet regionReplicaSet : relatedDataRegions) {
      List<String> devices = new ArrayList<>();
      List<PlanNode> children = new ArrayList<>();
      for (DeviceViewSplit split : deviceViewSplits) {
        if (split.needDistributeTo(regionReplicaSet)) {
          devices.add(split.device);
          children.add(split.buildPlanNodeInRegion(regionReplicaSet, context.queryContext));
        }
      }
      DeviceViewNode regionDeviceViewNode =
          new DeviceViewNode(
              context.queryContext.getQueryId().genPlanNodeId(),
              node.getMergeOrderParameter(),
              node.getOutputColumnNames(),
              node.getDeviceToMeasurementIndexesMap());
      for (int i = 0; i < devices.size(); i++) {
        regionDeviceViewNode.addChildDeviceNode(devices.get(i), children.get(i));
      }
      deviceMergeNode.addChild(regionDeviceViewNode);
    }

    return deviceMergeNode;
  }

  private static class DeviceViewSplit {
    protected String device;
    protected PlanNode root;
    protected Set<TRegionReplicaSet> dataPartitions;

    protected DeviceViewSplit(
        String device, PlanNode root, List<TRegionReplicaSet> dataPartitions) {
      this.device = device;
      this.root = root;
      this.dataPartitions = new HashSet<>();
      this.dataPartitions.addAll(dataPartitions);
    }

    protected PlanNode buildPlanNodeInRegion(
        TRegionReplicaSet regionReplicaSet, MPPQueryContext context) {
      return buildPlanNodeInRegion(this.root, regionReplicaSet, context);
    }

    protected boolean needDistributeTo(TRegionReplicaSet regionReplicaSet) {
      return this.dataPartitions.contains(regionReplicaSet);
    }

    private PlanNode buildPlanNodeInRegion(
        PlanNode root, TRegionReplicaSet regionReplicaSet, MPPQueryContext context) {
      List<PlanNode> children =
          root.getChildren().stream()
              .map(child -> buildPlanNodeInRegion(child, regionReplicaSet, context))
              .collect(Collectors.toList());
      PlanNode newRoot = root.cloneWithChildren(children);
      newRoot.setPlanNodeId(context.getQueryId().genPlanNodeId());
      if (newRoot instanceof SourceNode) {
        ((SourceNode) newRoot).setRegionReplicaSet(regionReplicaSet);
      }
      return newRoot;
    }
  }

  @Override
  public PlanNode visitSchemaQueryMerge(
      SchemaQueryMergeNode node, DistributionPlanContext context) {
    SchemaQueryMergeNode root = (SchemaQueryMergeNode) node.clone();
    SchemaQueryScanNode seed = (SchemaQueryScanNode) node.getChildren().get(0);
    TreeSet<TRegionReplicaSet> schemaRegions =
        new TreeSet<>(Comparator.comparingInt(region -> region.getRegionId().getId()));
    analysis
        .getSchemaPartitionInfo()
        .getSchemaPartitionMap()
        .forEach(
            (storageGroup, deviceGroup) -> {
              deviceGroup.forEach(
                  (deviceGroupId, schemaRegionReplicaSet) ->
                      schemaRegions.add(schemaRegionReplicaSet));
            });
    schemaRegions.forEach(
        region -> {
          SchemaQueryScanNode schemaQueryScanNode = (SchemaQueryScanNode) seed.clone();
          schemaQueryScanNode.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
          schemaQueryScanNode.setRegionReplicaSet(region);
          root.addChild(schemaQueryScanNode);
        });
    return root;
  }

  @Override
  public PlanNode visitCountMerge(CountSchemaMergeNode node, DistributionPlanContext context) {
    CountSchemaMergeNode root = (CountSchemaMergeNode) node.clone();
    SchemaQueryScanNode seed = (SchemaQueryScanNode) node.getChildren().get(0);
    Set<TRegionReplicaSet> schemaRegions = new HashSet<>();
    analysis
        .getSchemaPartitionInfo()
        .getSchemaPartitionMap()
        .forEach(
            (storageGroup, deviceGroup) -> {
              deviceGroup.forEach(
                  (deviceGroupId, schemaRegionReplicaSet) ->
                      schemaRegions.add(schemaRegionReplicaSet));
            });
    schemaRegions.forEach(
        region -> {
          SchemaQueryScanNode schemaQueryScanNode = (SchemaQueryScanNode) seed.clone();
          schemaQueryScanNode.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
          schemaQueryScanNode.setRegionReplicaSet(region);
          root.addChild(schemaQueryScanNode);
        });
    return root;
  }

  // TODO: (xingtanzjr) a temporary way to resolve the distribution of single SeriesScanNode issue
  @Override
  public PlanNode visitSeriesScan(SeriesScanNode node, DistributionPlanContext context) {
    TimeJoinNode timeJoinNode =
        new TimeJoinNode(context.queryContext.getQueryId().genPlanNodeId(), node.getScanOrder());
    return processRawSeriesScan(node, context, timeJoinNode);
  }

  @Override
  public PlanNode visitAlignedSeriesScan(
      AlignedSeriesScanNode node, DistributionPlanContext context) {
    TimeJoinNode timeJoinNode =
        new TimeJoinNode(context.queryContext.getQueryId().genPlanNodeId(), node.getScanOrder());
    return processRawSeriesScan(node, context, timeJoinNode);
  }

  @Override
  public PlanNode visitLastQueryScan(LastQueryScanNode node, DistributionPlanContext context) {
    LastQueryNode mergeNode =
        new LastQueryNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            node.getPartitionTimeFilter(),
            new OrderByParameter());
    return processRawSeriesScan(node, context, mergeNode);
  }

  @Override
  public PlanNode visitAlignedLastQueryScan(
      AlignedLastQueryScanNode node, DistributionPlanContext context) {
    LastQueryNode mergeNode =
        new LastQueryNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            node.getPartitionTimeFilter(),
            new OrderByParameter());
    return processRawSeriesScan(node, context, mergeNode);
  }

  private PlanNode processRawSeriesScan(
      SeriesSourceNode node, DistributionPlanContext context, MultiChildNode parent) {
    List<SeriesSourceNode> sourceNodes = splitSeriesSourceNodeByPartition(node, context);
    if (sourceNodes.size() == 1) {
      return sourceNodes.get(0);
    }
    sourceNodes.forEach(parent::addChild);
    return parent;
  }

  private List<SeriesSourceNode> splitSeriesSourceNodeByPartition(
      SeriesSourceNode node, DistributionPlanContext context) {
    List<SeriesSourceNode> ret = new ArrayList<>();
    List<TRegionReplicaSet> dataDistribution =
        analysis.getPartitionInfo(node.getPartitionPath(), node.getPartitionTimeFilter());
    if (dataDistribution.size() == 1) {
      node.setRegionReplicaSet(dataDistribution.get(0));
      ret.add(node);
      return ret;
    }

    for (TRegionReplicaSet dataRegion : dataDistribution) {
      SeriesSourceNode split = (SeriesSourceNode) node.clone();
      split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
      split.setRegionReplicaSet(dataRegion);
      ret.add(split);
    }
    return ret;
  }

  @Override
  public PlanNode visitSeriesAggregationScan(
      SeriesAggregationScanNode node, DistributionPlanContext context) {
    return processSeriesAggregationSource(node, context);
  }

  @Override
  public PlanNode visitAlignedSeriesAggregationScan(
      AlignedSeriesAggregationScanNode node, DistributionPlanContext context) {
    return processSeriesAggregationSource(node, context);
  }

  private PlanNode processSeriesAggregationSource(
      SeriesAggregationSourceNode node, DistributionPlanContext context) {
    List<TRegionReplicaSet> dataDistribution =
        analysis.getPartitionInfo(node.getPartitionPath(), node.getPartitionTimeFilter());
    if (dataDistribution.size() == 1) {
      node.setRegionReplicaSet(dataDistribution.get(0));
      return node;
    }
    List<AggregationDescriptor> leafAggDescriptorList = new ArrayList<>();
    node.getAggregationDescriptorList()
        .forEach(
            descriptor -> {
              leafAggDescriptorList.add(
                  new AggregationDescriptor(
                      descriptor.getAggregationFuncName(),
                      AggregationStep.PARTIAL,
                      descriptor.getInputExpressions()));
            });
    leafAggDescriptorList.forEach(
        d ->
            LogicalPlanBuilder.updateTypeProviderByPartialAggregation(
                d, context.queryContext.getTypeProvider()));
    List<AggregationDescriptor> rootAggDescriptorList = new ArrayList<>();
    node.getAggregationDescriptorList()
        .forEach(
            descriptor -> {
              rootAggDescriptorList.add(
                  new AggregationDescriptor(
                      descriptor.getAggregationFuncName(),
                      AggregationStep.FINAL,
                      descriptor.getInputExpressions()));
            });

    AggregationNode aggregationNode =
        new AggregationNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            rootAggDescriptorList,
            node.getGroupByTimeParameter(),
            node.getScanOrder());
    for (TRegionReplicaSet dataRegion : dataDistribution) {
      SeriesAggregationScanNode split = (SeriesAggregationScanNode) node.clone();
      split.setAggregationDescriptorList(leafAggDescriptorList);
      split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
      split.setRegionReplicaSet(dataRegion);
      aggregationNode.addChild(split);
    }
    return aggregationNode;
  }

  @Override
  public PlanNode visitSchemaFetchMerge(
      SchemaFetchMergeNode node, DistributionPlanContext context) {
    SchemaFetchMergeNode root = (SchemaFetchMergeNode) node.clone();
    Map<String, Set<TRegionReplicaSet>> storageGroupSchemaRegionMap = new HashMap<>();
    analysis
        .getSchemaPartitionInfo()
        .getSchemaPartitionMap()
        .forEach(
            (storageGroup, deviceGroup) -> {
              storageGroupSchemaRegionMap.put(storageGroup, new HashSet<>());
              deviceGroup.forEach(
                  (deviceGroupId, schemaRegionReplicaSet) ->
                      storageGroupSchemaRegionMap.get(storageGroup).add(schemaRegionReplicaSet));
            });

    for (PlanNode child : node.getChildren()) {
      for (TRegionReplicaSet schemaRegion :
          storageGroupSchemaRegionMap.get(
              ((SchemaFetchScanNode) child).getStorageGroup().getFullPath())) {
        SchemaFetchScanNode schemaFetchScanNode = (SchemaFetchScanNode) child.clone();
        schemaFetchScanNode.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
        schemaFetchScanNode.setRegionReplicaSet(schemaRegion);
        root.addChild(schemaFetchScanNode);
      }
    }
    return root;
  }

  @Override
  public PlanNode visitLastQuery(LastQueryNode node, DistributionPlanContext context) {
    // For last query, we need to keep every FI's root node is LastQueryMergeNode. So we
    // force every region group have a parent node even if there is only 1 child for it.
    context.setForceAddParent(true);
    PlanNode root = processRawMultiChildNode(node, context);
    if (context.queryMultiRegion) {
      PlanNode newRoot = genLastQueryRootNode(node, context);
      root.getChildren().forEach(newRoot::addChild);
      return newRoot;
    } else {
      return root;
    }
  }

  private PlanNode genLastQueryRootNode(LastQueryNode node, DistributionPlanContext context) {
    PlanNodeId id = context.queryContext.getQueryId().genPlanNodeId();
    if (context.oneSeriesInMultiRegion || !node.getMergeOrderParameter().isEmpty()) {
      return new LastQueryMergeNode(id, node.getMergeOrderParameter());
    }
    return new LastQueryCollectNode(id);
  }

  @Override
  public PlanNode visitTimeJoin(TimeJoinNode node, DistributionPlanContext context) {
    // Although some logic is similar between Aggregation and RawDataQuery,
    // we still use separate method to process the distribution planning now
    // to make the planning procedure more clear
    if (isAggregationQuery(node)) {
      return planAggregationWithTimeJoin(node, context);
    }
    return processRawMultiChildNode(node, context);
  }

  private PlanNode processRawMultiChildNode(MultiChildNode node, DistributionPlanContext context) {
    MultiChildNode root = (MultiChildNode) node.clone();
    // Step 1: Get all source nodes. For the node which is not source, add it as the child of
    // current TimeJoinNode
    List<SourceNode> sources = new ArrayList<>();
    for (PlanNode child : node.getChildren()) {
      if (child instanceof SeriesSourceNode) {
        // If the child is SeriesScanNode, we need to check whether this node should be seperated
        // into several splits.
        SeriesSourceNode handle = (SeriesSourceNode) child;
        List<TRegionReplicaSet> dataDistribution =
            analysis.getPartitionInfo(handle.getPartitionPath(), handle.getPartitionTimeFilter());
        if (dataDistribution.size() > 1) {
          // We mark this variable to `true` if there is some series which is distributed in multi
          // DataRegions
          context.setOneSeriesInMultiRegion(true);
        }
        // If the size of dataDistribution is m, this SeriesScanNode should be seperated into m
        // SeriesScanNode.
        for (TRegionReplicaSet dataRegion : dataDistribution) {
          SeriesSourceNode split = (SeriesSourceNode) handle.clone();
          split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
          split.setRegionReplicaSet(dataRegion);
          sources.add(split);
        }
      }
    }
    // Step 2: For the source nodes, group them by the DataRegion.
    Map<TRegionReplicaSet, List<SourceNode>> sourceGroup =
        sources.stream().collect(Collectors.groupingBy(SourceNode::getRegionReplicaSet));

    if (sourceGroup.size() > 1) {
      context.setQueryMultiRegion(true);
    }

    // Step 3: For the source nodes which belong to same data region, add a TimeJoinNode for them
    // and make the
    // new TimeJoinNode as the child of current TimeJoinNode
    // TODO: (xingtanzjr) optimize the procedure here to remove duplicated TimeJoinNode
    final boolean[] addParent = {false};
    sourceGroup.forEach(
        (dataRegion, seriesScanNodes) -> {
          if (seriesScanNodes.size() == 1 && !context.forceAddParent) {
            root.addChild(seriesScanNodes.get(0));
          } else {
            // If there is only one RegionGroup here, we should not create new MultiChildNode as the
            // parent.
            // If the size of RegionGroup is larger than 1, we need to consider the value of
            // `forceAddParent`.
            // If `forceAddParent` is true, we should not create new MultiChildNode as the parent,
            // either.
            // At last, we can use the parameter `addParent[0]` to judge whether to create new
            // MultiChildNode.
            boolean appendToRootDirectly =
                sourceGroup.size() == 1 || (!addParent[0] && !context.forceAddParent);
            if (appendToRootDirectly) {
              seriesScanNodes.forEach(root::addChild);
              addParent[0] = true;
            } else {
              // We clone a TimeJoinNode from root to make the params to be consistent.
              // But we need to assign a new ID to it
              MultiChildNode parentOfGroup = (MultiChildNode) root.clone();
              parentOfGroup.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
              seriesScanNodes.forEach(parentOfGroup::addChild);
              root.addChild(parentOfGroup);
            }
          }
        });

    // Process the other children which are not SeriesSourceNode
    for (PlanNode child : node.getChildren()) {
      if (!(child instanceof SeriesSourceNode)) {
        // In a general logical query plan, the children of TimeJoinNode should only be
        // SeriesScanNode or SeriesAggregateScanNode
        // So this branch should not be touched.
        root.addChild(visit(child, context));
      }
    }
    return root;
  }

  private boolean isAggregationQuery(TimeJoinNode node) {
    for (PlanNode child : node.getChildren()) {
      if (child instanceof SeriesAggregationScanNode
          || child instanceof AlignedSeriesAggregationScanNode) {
        return true;
      }
    }
    return false;
  }

  // This method is only used to process the PlanNodeTree whose root is SlidingWindowAggregationNode
  @Override
  public PlanNode visitSlidingWindowAggregation(
      SlidingWindowAggregationNode node, DistributionPlanContext context) {
    DistributionPlanContext childContext = context.copy().setRoot(false);
    PlanNode child = visit(node.getChild(), childContext);
    PlanNode newRoot = node.clone();
    newRoot.addChild(child);
    return newRoot;
  }

  private PlanNode planAggregationWithTimeJoin(TimeJoinNode root, DistributionPlanContext context) {

    List<SeriesAggregationSourceNode> sources = splitAggregationSourceByPartition(root, context);
    Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>> sourceGroup =
        sources.stream().collect(Collectors.groupingBy(SourceNode::getRegionReplicaSet));

    // construct AggregationDescriptor for AggregationNode
    List<AggregationDescriptor> rootAggDescriptorList = new ArrayList<>();
    for (PlanNode child : root.getChildren()) {
      SeriesAggregationSourceNode handle = (SeriesAggregationSourceNode) child;
      handle
          .getAggregationDescriptorList()
          .forEach(
              descriptor -> {
                rootAggDescriptorList.add(
                    new AggregationDescriptor(
                        descriptor.getAggregationFuncName(),
                        context.isRoot ? AggregationStep.FINAL : AggregationStep.INTERMEDIATE,
                        descriptor.getInputExpressions()));
              });
    }
    rootAggDescriptorList.forEach(
        d ->
            LogicalPlanBuilder.updateTypeProviderByPartialAggregation(
                d, context.queryContext.getTypeProvider()));
    checkArgument(
        sources.size() > 0, "Aggregation sources should not be empty when distribution planning");
    SeriesAggregationSourceNode seed = sources.get(0);
    AggregationNode aggregationNode =
        new AggregationNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            rootAggDescriptorList,
            seed.getGroupByTimeParameter(),
            seed.getScanOrder());

    final boolean[] addParent = {false};
    sourceGroup.forEach(
        (dataRegion, sourceNodes) -> {
          if (sourceNodes.size() == 1) {
            aggregationNode.addChild(sourceNodes.get(0));
          } else {
            if (!addParent[0]) {
              sourceNodes.forEach(aggregationNode::addChild);
              addParent[0] = true;
            } else {
              // We clone a TimeJoinNode from root to make the params to be consistent.
              // But we need to assign a new ID to it
              TimeJoinNode parentOfGroup = (TimeJoinNode) root.clone();
              parentOfGroup.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
              sourceNodes.forEach(parentOfGroup::addChild);
              aggregationNode.addChild(parentOfGroup);
            }
          }
        });

    return aggregationNode;
  }

  @Override
  public PlanNode visitGroupByLevel(GroupByLevelNode root, DistributionPlanContext context) {
    if (shouldUseNaiveAggregation(root)) {
      return defaultRewrite(root, context);
    }
    // Firstly, we build the tree structure for GroupByLevelNode
    List<SeriesAggregationSourceNode> sources = splitAggregationSourceByPartition(root, context);
    Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>> sourceGroup =
        sources.stream().collect(Collectors.groupingBy(SourceNode::getRegionReplicaSet));

    boolean containsSlidingWindow =
        root.getChildren().size() == 1
            && root.getChildren().get(0) instanceof SlidingWindowAggregationNode;

    GroupByLevelNode newRoot =
        containsSlidingWindow
            ? groupSourcesForGroupByLevelWithSlidingWindow(
                root,
                (SlidingWindowAggregationNode) root.getChildren().get(0),
                sourceGroup,
                context)
            : groupSourcesForGroupByLevel(root, sourceGroup, context);

    // Then, we calculate the attributes for GroupByLevelNode in each level
    calculateGroupByLevelNodeAttributes(newRoot, 0, context);
    return newRoot;
  }

  // If the Aggregation Query contains value filter, we need to use the naive query plan
  // for it. That is, do the raw data query and then do the aggregation operation.
  // Currently, the method to judge whether the query should use naive query plan is whether
  // AggregationNode is contained in the PlanNode tree of logical plan.
  private boolean shouldUseNaiveAggregation(PlanNode root) {
    if (root instanceof AggregationNode) {
      return true;
    }
    for (PlanNode child : root.getChildren()) {
      if (shouldUseNaiveAggregation(child)) {
        return true;
      }
    }
    return false;
  }

  private GroupByLevelNode groupSourcesForGroupByLevelWithSlidingWindow(
      GroupByLevelNode root,
      SlidingWindowAggregationNode slidingWindowNode,
      Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>> sourceGroup,
      DistributionPlanContext context) {
    GroupByLevelNode newRoot = (GroupByLevelNode) root.clone();
    List<SlidingWindowAggregationNode> groups = new ArrayList<>();
    sourceGroup.forEach(
        (dataRegion, sourceNodes) -> {
          SlidingWindowAggregationNode parentOfGroup =
              (SlidingWindowAggregationNode) slidingWindowNode.clone();
          parentOfGroup.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
          if (sourceNodes.size() == 1) {
            parentOfGroup.addChild(sourceNodes.get(0));
          } else {
            TimeJoinNode timeJoinNode =
                new TimeJoinNode(
                    context.queryContext.getQueryId().genPlanNodeId(), root.getScanOrder());
            sourceNodes.forEach(timeJoinNode::addChild);
            parentOfGroup.addChild(timeJoinNode);
          }
          groups.add(parentOfGroup);
        });
    for (int i = 0; i < groups.size(); i++) {
      if (i == 0) {
        newRoot.addChild(groups.get(i));
        continue;
      }
      GroupByLevelNode parent = (GroupByLevelNode) root.clone();
      parent.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
      parent.addChild(groups.get(i));
      newRoot.addChild(parent);
    }
    return newRoot;
  }

  private GroupByLevelNode groupSourcesForGroupByLevel(
      GroupByLevelNode root,
      Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>> sourceGroup,
      DistributionPlanContext context) {
    GroupByLevelNode newRoot = (GroupByLevelNode) root.clone();
    final boolean[] addParent = {false};
    sourceGroup.forEach(
        (dataRegion, sourceNodes) -> {
          if (sourceNodes.size() == 1) {
            newRoot.addChild(sourceNodes.get(0));
          } else {
            if (!addParent[0]) {
              sourceNodes.forEach(newRoot::addChild);
              addParent[0] = true;
            } else {
              GroupByLevelNode parentOfGroup = (GroupByLevelNode) root.clone();
              parentOfGroup.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
              sourceNodes.forEach(parentOfGroup::addChild);
              newRoot.addChild(parentOfGroup);
            }
          }
        });
    return newRoot;
  }

  // TODO: (xingtanzjr) consider to implement the descriptor construction in every class
  private void calculateGroupByLevelNodeAttributes(
      PlanNode node, int level, DistributionPlanContext context) {
    if (node == null) {
      return;
    }
    node.getChildren()
        .forEach(child -> calculateGroupByLevelNodeAttributes(child, level + 1, context));

    // Construct all outputColumns from children. Using Set here to avoid duplication
    Set<String> childrenOutputColumns = new HashSet<>();
    node.getChildren().forEach(child -> childrenOutputColumns.addAll(child.getOutputColumnNames()));

    if (node instanceof SlidingWindowAggregationNode) {
      SlidingWindowAggregationNode handle = (SlidingWindowAggregationNode) node;
      List<AggregationDescriptor> descriptorList = new ArrayList<>();
      for (AggregationDescriptor originalDescriptor : handle.getAggregationDescriptorList()) {
        boolean keep = false;
        for (String childColumn : childrenOutputColumns) {
          for (Expression exp : originalDescriptor.getInputExpressions()) {
            if (isAggColumnMatchExpression(childColumn, exp)) {
              keep = true;
            }
          }
        }
        if (keep) {
          descriptorList.add(originalDescriptor);
          LogicalPlanBuilder.updateTypeProviderByPartialAggregation(
              originalDescriptor, context.queryContext.getTypeProvider());
        }
      }
      handle.setAggregationDescriptorList(descriptorList);
    }

    if (node instanceof GroupByLevelNode) {
      GroupByLevelNode handle = (GroupByLevelNode) node;
      // Check every OutputColumn of GroupByLevelNode and set the Expression of corresponding
      // AggregationDescriptor
      List<GroupByLevelDescriptor> descriptorList = new ArrayList<>();
      for (GroupByLevelDescriptor originalDescriptor : handle.getGroupByLevelDescriptors()) {
        Set<Expression> descriptorExpressions = new HashSet<>();
        for (String childColumn : childrenOutputColumns) {
          // If this condition matched, the childColumn should come from GroupByLevelNode
          if (isAggColumnMatchExpression(childColumn, originalDescriptor.getOutputExpression())) {
            descriptorExpressions.add(originalDescriptor.getOutputExpression());
            continue;
          }
          for (Expression exp : originalDescriptor.getInputExpressions()) {
            if (isAggColumnMatchExpression(childColumn, exp)) {
              descriptorExpressions.add(exp);
            }
          }
        }
        if (descriptorExpressions.size() == 0) {
          continue;
        }
        GroupByLevelDescriptor descriptor = originalDescriptor.deepClone();
        descriptor.setStep(level == 0 ? AggregationStep.FINAL : AggregationStep.INTERMEDIATE);
        descriptor.setInputExpressions(new ArrayList<>(descriptorExpressions));
        descriptorList.add(descriptor);
        LogicalPlanBuilder.updateTypeProviderByPartialAggregation(
            descriptor, context.queryContext.getTypeProvider());
      }
      handle.setGroupByLevelDescriptors(descriptorList);
    }
  }

  // TODO: (xingtanzjr) need to confirm the logic when processing UDF
  private boolean isAggColumnMatchExpression(String columnName, Expression expression) {
    if (columnName == null) {
      return false;
    }
    return columnName.contains(expression.getExpressionString());
  }

  private List<SeriesAggregationSourceNode> splitAggregationSourceByPartition(
      PlanNode root, DistributionPlanContext context) {
    // Step 0: get all SeriesAggregationSourceNode in PlanNodeTree
    List<SeriesAggregationSourceNode> rawSources = findAggregationSourceNode(root);
    // Step 1: split SeriesAggregationSourceNode according to data partition
    List<SeriesAggregationSourceNode> sources = new ArrayList<>();
    Map<PartialPath, Integer> regionCountPerSeries = new HashMap<>();
    for (SeriesAggregationSourceNode child : rawSources) {
      List<TRegionReplicaSet> dataDistribution =
          analysis.getPartitionInfo(child.getPartitionPath(), child.getPartitionTimeFilter());
      for (TRegionReplicaSet dataRegion : dataDistribution) {
        SeriesAggregationSourceNode split = (SeriesAggregationSourceNode) child.clone();
        split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
        split.setRegionReplicaSet(dataRegion);
        // Let each split reference different object of AggregationDescriptorList
        split.setAggregationDescriptorList(
            child.getAggregationDescriptorList().stream()
                .map(AggregationDescriptor::deepClone)
                .collect(Collectors.toList()));
        sources.add(split);
      }
      regionCountPerSeries.put(child.getPartitionPath(), dataDistribution.size());
    }

    // Step 2: change the step for each SeriesAggregationSourceNode according to its split count
    for (SeriesAggregationSourceNode source : sources) {
      //        boolean isFinal = regionCountPerSeries.get(source.getPartitionPath()) == 1;
      // TODO: (xingtanzjr) need to optimize this step later. We make it as Partial now.
      boolean isFinal = false;
      source
          .getAggregationDescriptorList()
          .forEach(
              d -> {
                d.setStep(isFinal ? AggregationStep.FINAL : AggregationStep.PARTIAL);
                LogicalPlanBuilder.updateTypeProviderByPartialAggregation(
                    d, context.queryContext.getTypeProvider());
              });
    }
    return sources;
  }

  private List<SeriesAggregationSourceNode> findAggregationSourceNode(PlanNode node) {
    if (node == null) {
      return new ArrayList<>();
    }
    if (node instanceof SeriesAggregationSourceNode) {
      return Collections.singletonList((SeriesAggregationSourceNode) node);
    }
    List<SeriesAggregationSourceNode> ret = new ArrayList<>();
    node.getChildren().forEach(child -> ret.addAll(findAggregationSourceNode(child)));
    return ret;
  }

  public PlanNode visit(PlanNode node, DistributionPlanContext context) {
    return node.accept(this, context);
  }
}
