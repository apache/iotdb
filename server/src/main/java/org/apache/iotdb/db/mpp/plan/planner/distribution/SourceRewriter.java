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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SingleDeviceViewNode;
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
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;

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
import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

public class SourceRewriter extends SimplePlanNodeRewriter<DistributionPlanContext> {

  private final Analysis analysis;

  public SourceRewriter(Analysis analysis) {
    this.analysis = analysis;
  }

  @Override
  public List<PlanNode> visitMergeSort(MergeSortNode node, DistributionPlanContext context) {
    MergeSortNode newRoot = cloneMergeSortNodeWithoutChild(node, context);
    for (int i = 0; i < node.getChildren().size(); i++) {
      List<PlanNode> rewroteNodes = rewrite(node.getChildren().get(i), context);
      rewroteNodes.forEach(newRoot::addChild);
    }
    return Collections.singletonList(newRoot);
  }

  private MergeSortNode cloneMergeSortNodeWithoutChild(
      MergeSortNode node, DistributionPlanContext context) {
    return new MergeSortNode(
        context.queryContext.getQueryId().genPlanNodeId(),
        node.getMergeOrderParameter(),
        node.getOutputColumnNames());
  }

  @Override
  public List<PlanNode> visitSingleDeviceView(
      SingleDeviceViewNode node, DistributionPlanContext context) {

    // Same process logic as visitDeviceView
    if (analysis.isDeviceViewSpecialProcess()) {
      List<PlanNode> rewroteChildren = rewrite(node.getChild(), context);
      if (rewroteChildren.size() != 1) {
        throw new IllegalStateException("SingleDeviceViewNode have only one child");
      }
      node.setChild(rewroteChildren.get(0));
      return Collections.singletonList(node);
    }

    String device = node.getDevice();
    List<TRegionReplicaSet> regionReplicaSets =
        analysis.getPartitionInfo(device, analysis.getGlobalTimeFilter());

    List<PlanNode> singleDeviceViewList = new ArrayList<>();
    for (TRegionReplicaSet tRegionReplicaSet : regionReplicaSets) {
      singleDeviceViewList.add(
          buildSingleDeviceViewNodeInRegion(node, tRegionReplicaSet, context.queryContext));
    }

    return singleDeviceViewList;
  }

  private PlanNode buildSingleDeviceViewNodeInRegion(
      PlanNode root, TRegionReplicaSet regionReplicaSet, MPPQueryContext context) {
    List<PlanNode> children =
        root.getChildren().stream()
            .map(child -> buildSingleDeviceViewNodeInRegion(child, regionReplicaSet, context))
            .collect(Collectors.toList());
    PlanNode newRoot = root.cloneWithChildren(children);
    newRoot.setPlanNodeId(context.getQueryId().genPlanNodeId());
    if (newRoot instanceof SourceNode) {
      ((SourceNode) newRoot).setRegionReplicaSet(regionReplicaSet);
    }
    return newRoot;
  }

  @Override
  public List<PlanNode> visitDeviceView(DeviceViewNode node, DistributionPlanContext context) {
    checkArgument(
        node.getDevices().size() == node.getChildren().size(),
        "size of devices and its children in DeviceViewNode should be same");

    // If the DeviceView is mixed with Function that need to merge data from different Data Region,
    // it should be processed by a special logic.
    // Now the Functions are : all Aggregation Functions and DIFF
    if (analysis.isDeviceViewSpecialProcess()) {
      return processSpecialDeviceView(node, context);
    }

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

    // Step 2: Iterate all partition and create DeviceViewNode for each region
    List<PlanNode> deviceViewNodeList = new ArrayList<>();
    for (TRegionReplicaSet regionReplicaSet : relatedDataRegions) {
      List<String> devices = new ArrayList<>();
      List<PlanNode> children = new ArrayList<>();
      for (DeviceViewSplit split : deviceViewSplits) {
        if (split.needDistributeTo(regionReplicaSet)) {
          devices.add(split.device);
          children.add(split.buildPlanNodeInRegion(regionReplicaSet, context.queryContext));
        }
      }
      DeviceViewNode regionDeviceViewNode = cloneDeviceViewNodeWithoutChild(node, context);
      for (int i = 0; i < devices.size(); i++) {
        regionDeviceViewNode.addChildDeviceNode(devices.get(i), children.get(i));
      }
      deviceViewNodeList.add(regionDeviceViewNode);
    }

    if (deviceViewNodeList.size() == 1) {
      return deviceViewNodeList;
    }

    MergeSortNode mergeSortNode =
        new MergeSortNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            node.getMergeOrderParameter(),
            node.getOutputColumnNames());
    for (PlanNode deviceViewNode : deviceViewNodeList) {
      mergeSortNode.addChild(deviceViewNode);
    }
    return Collections.singletonList(mergeSortNode);
  }

  private List<PlanNode> processSpecialDeviceView(
      DeviceViewNode node, DistributionPlanContext context) {
    DeviceViewNode newRoot = cloneDeviceViewNodeWithoutChild(node, context);
    for (int i = 0; i < node.getDevices().size(); i++) {
      List<PlanNode> rewroteNode = rewrite(node.getChildren().get(i), context);
      for (PlanNode planNode : rewroteNode) {
        newRoot.addChildDeviceNode(node.getDevices().get(i), planNode);
      }
    }
    return Collections.singletonList(newRoot);
  }

  private DeviceViewNode cloneDeviceViewNodeWithoutChild(
      DeviceViewNode node, DistributionPlanContext context) {
    return new DeviceViewNode(
        context.queryContext.getQueryId().genPlanNodeId(),
        node.getMergeOrderParameter(),
        node.getOutputColumnNames(),
        node.getDeviceToMeasurementIndexesMap());
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
  public List<PlanNode> visitSchemaQueryMerge(
      SchemaQueryMergeNode node, DistributionPlanContext context) {
    SchemaQueryMergeNode root = (SchemaQueryMergeNode) node.clone();
    SchemaQueryScanNode seed = (SchemaQueryScanNode) node.getChildren().get(0);
    List<PartialPath> pathPatternList = seed.getPathPatternList();
    if (pathPatternList.size() == 1) {
      // the path pattern overlaps with all storageGroup or storageGroup.**
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
    } else {
      // the path pattern may only overlap with part of storageGroup or storageGroup.**, need filter
      PathPatternTree patternTree = new PathPatternTree();
      for (PartialPath pathPattern : pathPatternList) {
        patternTree.appendPathPattern(pathPattern);
      }
      Map<String, Set<TRegionReplicaSet>> storageGroupSchemaRegionMap = new HashMap<>();
      analysis
          .getSchemaPartitionInfo()
          .getSchemaPartitionMap()
          .forEach(
              (storageGroup, deviceGroup) -> {
                deviceGroup.forEach(
                    (deviceGroupId, schemaRegionReplicaSet) ->
                        storageGroupSchemaRegionMap
                            .computeIfAbsent(storageGroup, k -> new HashSet<>())
                            .add(schemaRegionReplicaSet));
              });

      storageGroupSchemaRegionMap.forEach(
          (storageGroup, schemaRegionSet) -> {
            // extract the patterns overlap with current database
            Set<PartialPath> filteredPathPatternSet = new HashSet<>();
            try {
              PartialPath storageGroupPath = new PartialPath(storageGroup);
              filteredPathPatternSet.addAll(
                  patternTree.getOverlappedPathPatterns(storageGroupPath));
              filteredPathPatternSet.addAll(
                  patternTree.getOverlappedPathPatterns(
                      storageGroupPath.concatNode(MULTI_LEVEL_PATH_WILDCARD)));
            } catch (IllegalPathException ignored) {
              // won't reach here
            }
            List<PartialPath> filteredPathPatternList = new ArrayList<>(filteredPathPatternSet);

            schemaRegionSet.forEach(
                region -> {
                  SchemaQueryScanNode schemaQueryScanNode = (SchemaQueryScanNode) seed.clone();
                  schemaQueryScanNode.setPlanNodeId(
                      context.queryContext.getQueryId().genPlanNodeId());
                  schemaQueryScanNode.setRegionReplicaSet(region);
                  schemaQueryScanNode.setPathPatternList(filteredPathPatternList);
                  root.addChild(schemaQueryScanNode);
                });
          });
    }
    return Collections.singletonList(root);
  }

  @Override
  public List<PlanNode> visitCountMerge(
      CountSchemaMergeNode node, DistributionPlanContext context) {
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
    return Collections.singletonList(root);
  }

  // TODO: (xingtanzjr) a temporary way to resolve the distribution of single SeriesScanNode issue
  @Override
  public List<PlanNode> visitSeriesScan(SeriesScanNode node, DistributionPlanContext context) {
    TimeJoinNode timeJoinNode =
        new TimeJoinNode(context.queryContext.getQueryId().genPlanNodeId(), node.getScanOrder());
    return processRawSeriesScan(node, context, timeJoinNode);
  }

  @Override
  public List<PlanNode> visitAlignedSeriesScan(
      AlignedSeriesScanNode node, DistributionPlanContext context) {
    TimeJoinNode timeJoinNode =
        new TimeJoinNode(context.queryContext.getQueryId().genPlanNodeId(), node.getScanOrder());
    return processRawSeriesScan(node, context, timeJoinNode);
  }

  @Override
  public List<PlanNode> visitLastQueryScan(
      LastQueryScanNode node, DistributionPlanContext context) {
    LastQueryNode mergeNode =
        new LastQueryNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            node.getPartitionTimeFilter(),
            new OrderByParameter());
    return processRawSeriesScan(node, context, mergeNode);
  }

  @Override
  public List<PlanNode> visitAlignedLastQueryScan(
      AlignedLastQueryScanNode node, DistributionPlanContext context) {
    LastQueryNode mergeNode =
        new LastQueryNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            node.getPartitionTimeFilter(),
            new OrderByParameter());
    return processRawSeriesScan(node, context, mergeNode);
  }

  private List<PlanNode> processRawSeriesScan(
      SeriesSourceNode node, DistributionPlanContext context, MultiChildProcessNode parent) {
    List<PlanNode> sourceNodes = splitSeriesSourceNodeByPartition(node, context);
    if (sourceNodes.size() == 1) {
      return sourceNodes;
    }
    sourceNodes.forEach(parent::addChild);
    return Collections.singletonList(parent);
  }

  private List<PlanNode> splitSeriesSourceNodeByPartition(
      SeriesSourceNode node, DistributionPlanContext context) {
    List<PlanNode> ret = new ArrayList<>();
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
  public List<PlanNode> visitSeriesAggregationScan(
      SeriesAggregationScanNode node, DistributionPlanContext context) {
    return processSeriesAggregationSource(node, context);
  }

  @Override
  public List<PlanNode> visitAlignedSeriesAggregationScan(
      AlignedSeriesAggregationScanNode node, DistributionPlanContext context) {
    return processSeriesAggregationSource(node, context);
  }

  private List<PlanNode> processSeriesAggregationSource(
      SeriesAggregationSourceNode node, DistributionPlanContext context) {
    List<TRegionReplicaSet> dataDistribution =
        analysis.getPartitionInfo(node.getPartitionPath(), node.getPartitionTimeFilter());
    if (dataDistribution.size() == 1) {
      node.setRegionReplicaSet(dataDistribution.get(0));
      return Collections.singletonList(node);
    }
    List<AggregationDescriptor> leafAggDescriptorList = new ArrayList<>();
    node.getAggregationDescriptorList()
        .forEach(
            descriptor -> {
              leafAggDescriptorList.add(
                  new AggregationDescriptor(
                      descriptor.getAggregationFuncName(),
                      AggregationStep.PARTIAL,
                      descriptor.getInputExpressions(),
                      descriptor.getInputAttributes()));
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
                      context.isRoot ? AggregationStep.FINAL : AggregationStep.INTERMEDIATE,
                      descriptor.getInputExpressions(),
                      descriptor.getInputAttributes()));
            });

    AggregationNode aggregationNode =
        new AggregationNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            rootAggDescriptorList,
            node.getGroupByTimeParameter(),
            node.getScanOrder());
    for (TRegionReplicaSet dataRegion : dataDistribution) {
      SeriesAggregationSourceNode split = (SeriesAggregationSourceNode) node.clone();
      split.setAggregationDescriptorList(leafAggDescriptorList);
      split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
      split.setRegionReplicaSet(dataRegion);
      aggregationNode.addChild(split);
    }
    return Collections.singletonList(aggregationNode);
  }

  @Override
  public List<PlanNode> visitSchemaFetchMerge(
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
    return Collections.singletonList(root);
  }

  @Override
  public List<PlanNode> visitLastQuery(LastQueryNode node, DistributionPlanContext context) {
    // For last query, we need to keep every FI's root node is LastQueryMergeNode. So we
    // force every region group have a parent node even if there is only 1 child for it.
    context.setForceAddParent(true);
    PlanNode root = processRawMultiChildNode(node, context);
    if (context.queryMultiRegion) {
      PlanNode newRoot = genLastQueryRootNode(node, context);
      // add sort op for each if we add LastQueryMergeNode as root
      if (newRoot instanceof LastQueryMergeNode && node.getMergeOrderParameter().isEmpty()) {
        OrderByParameter orderByParameter =
            new OrderByParameter(
                Collections.singletonList(new SortItem(SortKey.TIMESERIES, Ordering.ASC)));
        addSortForEachLastQueryNode(root, orderByParameter);
      }
      root.getChildren().forEach(newRoot::addChild);
      return Collections.singletonList(newRoot);
    } else {
      return Collections.singletonList(root);
    }
  }

  private void addSortForEachLastQueryNode(PlanNode root, OrderByParameter orderByParameter) {
    if (root instanceof LastQueryNode
        && (root.getChildren().get(0) instanceof LastQueryScanNode
            || root.getChildren().get(0) instanceof AlignedLastQueryScanNode)) {
      LastQueryNode lastQueryNode = (LastQueryNode) root;
      lastQueryNode.setMergeOrderParameter(orderByParameter);
      // sort children node
      lastQueryNode.setChildren(
          lastQueryNode.getChildren().stream()
              .sorted(
                  Comparator.comparing(
                      child -> {
                        String fullPath = "";
                        if (child instanceof LastQueryScanNode) {
                          fullPath = ((LastQueryScanNode) child).getSeriesPath().getFullPath();
                        } else if (child instanceof AlignedLastQueryScanNode) {
                          fullPath = ((AlignedLastQueryScanNode) child).getSeriesPath().getDevice();
                        }
                        return fullPath;
                      }))
              .collect(Collectors.toList()));
    } else {
      for (PlanNode child : root.getChildren()) {
        addSortForEachLastQueryNode(child, orderByParameter);
      }
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
  public List<PlanNode> visitTimeJoin(TimeJoinNode node, DistributionPlanContext context) {
    // Although some logic is similar between Aggregation and RawDataQuery,
    // we still use separate method to process the distribution planning now
    // to make the planning procedure more clear
    if (containsAggregationSource(node)) {
      return planAggregationWithTimeJoin(node, context);
    }
    return Collections.singletonList(processRawMultiChildNode(node, context));
  }

  private PlanNode processRawMultiChildNode(
      MultiChildProcessNode node, DistributionPlanContext context) {
    MultiChildProcessNode root = (MultiChildProcessNode) node.clone();
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
              MultiChildProcessNode parentOfGroup = (MultiChildProcessNode) root.clone();
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
        List<PlanNode> children = visit(child, context);
        children.forEach(root::addChild);
      }
    }
    return root;
  }

  private boolean isAggregationQuery() {
    return ((QueryStatement) analysis.getStatement()).isAggregationQuery();
  }

  private boolean containsAggregationSource(TimeJoinNode node) {
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
  public List<PlanNode> visitSlidingWindowAggregation(
      SlidingWindowAggregationNode node, DistributionPlanContext context) {
    DistributionPlanContext childContext = context.copy().setRoot(false);
    List<PlanNode> children = visit(node.getChild(), childContext);
    PlanNode newRoot = node.clone();
    children.forEach(newRoot::addChild);
    return Collections.singletonList(newRoot);
  }

  private List<PlanNode> planAggregationWithTimeJoin(
      TimeJoinNode root, DistributionPlanContext context) {
    Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>> sourceGroup;

    // construct newRoot
    MultiChildProcessNode newRoot;
    if (context.isRoot) {
      // This node is the root of PlanTree,
      // if the aggregated series have only one region,
      // upstream will give the final aggregate result,
      // the step of this series' aggregator will be `STATIC`
      List<SeriesAggregationSourceNode> sources = new ArrayList<>();
      Map<PartialPath, Integer> regionCountPerSeries = new HashMap<>();
      boolean[] eachSeriesOneRegion = {true};
      sourceGroup =
          splitAggregationSourceByPartition(
              root, context, sources, eachSeriesOneRegion, regionCountPerSeries);

      if (eachSeriesOneRegion[0]) {
        newRoot = new HorizontallyConcatNode(context.queryContext.getQueryId().genPlanNodeId());
      } else {
        List<AggregationDescriptor> rootAggDescriptorList = new ArrayList<>();
        for (PlanNode child : root.getChildren()) {
          SeriesAggregationSourceNode handle = (SeriesAggregationSourceNode) child;
          handle
              .getAggregationDescriptorList()
              .forEach(
                  descriptor ->
                      rootAggDescriptorList.add(
                          new AggregationDescriptor(
                              descriptor.getAggregationFuncName(),
                              regionCountPerSeries.get(handle.getPartitionPath()) == 1
                                  ? AggregationStep.STATIC
                                  : AggregationStep.FINAL,
                              descriptor.getInputExpressions(),
                              descriptor.getInputAttributes())));
        }
        SeriesAggregationSourceNode seed = (SeriesAggregationSourceNode) root.getChildren().get(0);
        newRoot =
            new AggregationNode(
                context.queryContext.getQueryId().genPlanNodeId(),
                rootAggDescriptorList,
                seed.getGroupByTimeParameter(),
                seed.getScanOrder());
      }
    } else {
      // If this node is not the root node of PlanTree,
      // it declares that there have something to do at downstream,
      // the step of this AggregationNode should be `INTERMEDIATE`
      sourceGroup = splitAggregationSourceByPartition(root, context);
      List<AggregationDescriptor> rootAggDescriptorList = new ArrayList<>();
      for (PlanNode child : root.getChildren()) {
        SeriesAggregationSourceNode handle = (SeriesAggregationSourceNode) child;
        handle
            .getAggregationDescriptorList()
            .forEach(
                descriptor ->
                    rootAggDescriptorList.add(
                        new AggregationDescriptor(
                            descriptor.getAggregationFuncName(),
                            AggregationStep.INTERMEDIATE,
                            descriptor.getInputExpressions(),
                            descriptor.getInputAttributes())));
      }
      SeriesAggregationSourceNode seed = (SeriesAggregationSourceNode) root.getChildren().get(0);
      newRoot =
          new AggregationNode(
              context.queryContext.getQueryId().genPlanNodeId(),
              rootAggDescriptorList,
              seed.getGroupByTimeParameter(),
              seed.getScanOrder());
    }

    boolean[] addParent = {false};
    sourceGroup.forEach(
        (dataRegion, sourceNodes) -> {
          if (sourceNodes.size() == 1) {
            newRoot.addChild(sourceNodes.get(0));
          } else {
            if (!addParent[0]) {
              sourceNodes.forEach(newRoot::addChild);
              addParent[0] = true;
            } else {
              HorizontallyConcatNode parentOfGroup =
                  new HorizontallyConcatNode(context.queryContext.getQueryId().genPlanNodeId());
              sourceNodes.forEach(parentOfGroup::addChild);
              newRoot.addChild(parentOfGroup);
            }
          }
        });

    return Collections.singletonList(newRoot);
  }

  @Override
  public List<PlanNode> visitGroupByLevel(GroupByLevelNode root, DistributionPlanContext context) {
    if (shouldUseNaiveAggregation(root)) {
      return defaultRewrite(root, context);
    }
    // Firstly, we build the tree structure for GroupByLevelNode
    Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>> sourceGroup =
        splitAggregationSourceByPartition(root, context);

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
    Map<String, Expression> columnNameToExpression = new HashMap<>();
    for (CrossSeriesAggregationDescriptor originalDescriptor :
        newRoot.getGroupByLevelDescriptors()) {
      for (Expression exp : originalDescriptor.getInputExpressions()) {
        columnNameToExpression.put(exp.getExpressionString(), exp);
      }
      columnNameToExpression.put(
          originalDescriptor.getOutputExpression().getExpressionString(),
          originalDescriptor.getOutputExpression());
    }

    context.setColumnNameToExpression(columnNameToExpression);
    calculateGroupByLevelNodeAttributes(newRoot, 0, context);
    return Collections.singletonList(newRoot);
  }

  @Override
  public List<PlanNode> visitGroupByTag(GroupByTagNode root, DistributionPlanContext context) {
    Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>> sourceGroup =
        splitAggregationSourceByPartition(root, context);

    boolean containsSlidingWindow =
        root.getChildren().size() == 1
            && root.getChildren().get(0) instanceof SlidingWindowAggregationNode;

    // TODO: use 2 phase aggregation to optimize the query
    return Collections.singletonList(
        containsSlidingWindow
            ? groupSourcesForGroupByTagWithSlidingWindow(
                root,
                (SlidingWindowAggregationNode) root.getChildren().get(0),
                sourceGroup,
                context)
            : groupSourcesForGroupByTag(root, sourceGroup, context));
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
            HorizontallyConcatNode verticallyConcatNode =
                new HorizontallyConcatNode(context.queryContext.getQueryId().genPlanNodeId());
            sourceNodes.forEach(verticallyConcatNode::addChild);
            parentOfGroup.addChild(verticallyConcatNode);
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
      List<CrossSeriesAggregationDescriptor> descriptorList = new ArrayList<>();
      Map<String, Expression> columnNameToExpression = context.getColumnNameToExpression();
      Set<Expression> childrenExpressionSet = new HashSet<>();
      for (String childColumn : childrenOutputColumns) {
        Expression childExpression =
            columnNameToExpression.get(
                childColumn.substring(childColumn.indexOf("(") + 1, childColumn.lastIndexOf(")")));
        childrenExpressionSet.add(childExpression);
      }

      for (CrossSeriesAggregationDescriptor originalDescriptor :
          handle.getGroupByLevelDescriptors()) {
        Set<Expression> descriptorExpressions = new HashSet<>();

        if (childrenExpressionSet.contains(originalDescriptor.getOutputExpression())) {
          descriptorExpressions.add(originalDescriptor.getOutputExpression());
        }

        for (Expression exp : originalDescriptor.getInputExpressions()) {
          if (childrenExpressionSet.contains(exp)) {
            descriptorExpressions.add(exp);
          }
        }

        if (descriptorExpressions.isEmpty()) {
          continue;
        }
        CrossSeriesAggregationDescriptor descriptor = originalDescriptor.deepClone();
        descriptor.setStep(level == 0 ? AggregationStep.FINAL : AggregationStep.INTERMEDIATE);
        descriptor.setInputExpressions(new ArrayList<>(descriptorExpressions));
        descriptorList.add(descriptor);
        LogicalPlanBuilder.updateTypeProviderByPartialAggregation(
            descriptor, context.queryContext.getTypeProvider());
      }
      handle.setGroupByLevelDescriptors(descriptorList);
    }
  }

  private GroupByTagNode groupSourcesForGroupByTagWithSlidingWindow(
      GroupByTagNode root,
      SlidingWindowAggregationNode slidingWindowNode,
      Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>> sourceGroup,
      DistributionPlanContext context) {
    GroupByTagNode newRoot = (GroupByTagNode) root.clone();
    sourceGroup.forEach(
        (dataRegion, sourceNodes) -> {
          SlidingWindowAggregationNode parentOfGroup =
              (SlidingWindowAggregationNode) slidingWindowNode.clone();
          parentOfGroup.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
          List<AggregationDescriptor> childDescriptors = new ArrayList<>();
          sourceNodes.forEach(
              n ->
                  n.getAggregationDescriptorList()
                      .forEach(
                          v -> {
                            childDescriptors.add(
                                new AggregationDescriptor(
                                    v.getAggregationFuncName(),
                                    AggregationStep.INTERMEDIATE,
                                    v.getInputExpressions(),
                                    v.getInputAttributes()));
                          }));
          parentOfGroup.setAggregationDescriptorList(childDescriptors);
          if (sourceNodes.size() == 1) {
            parentOfGroup.addChild(sourceNodes.get(0));
          } else {
            HorizontallyConcatNode verticallyConcatNode =
                new HorizontallyConcatNode(context.queryContext.getQueryId().genPlanNodeId());
            sourceNodes.forEach(verticallyConcatNode::addChild);
            parentOfGroup.addChild(verticallyConcatNode);
          }
          newRoot.addChild(parentOfGroup);
        });
    return newRoot;
  }

  private GroupByTagNode groupSourcesForGroupByTag(
      GroupByTagNode root,
      Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>> sourceGroup,
      DistributionPlanContext context) {
    GroupByTagNode newRoot = (GroupByTagNode) root.clone();
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
              HorizontallyConcatNode horizontallyConcatNode =
                  new HorizontallyConcatNode(context.queryContext.getQueryId().genPlanNodeId());
              sourceNodes.forEach(horizontallyConcatNode::addChild);
              newRoot.addChild(horizontallyConcatNode);
            }
          }
        });
    return newRoot;
  }

  // TODO: (xingtanzjr) need to confirm the logic when processing UDF
  private boolean isAggColumnMatchExpression(String columnName, Expression expression) {
    if (columnName == null) {
      return false;
    }
    return columnName.contains(expression.getExpressionString());
  }

  /**
   * This method is used for rewriting second step AggregationNode like GroupByLevelNode,
   * GroupByTagNode and SlidingWindowAggregationNode, so the first step AggregationNode's step will
   * be {@link AggregationStep#PARTIAL}
   */
  private Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>>
      splitAggregationSourceByPartition(PlanNode root, DistributionPlanContext context) {
    // Step 0: get all SeriesAggregationSourceNode in PlanNodeTree
    List<SeriesAggregationSourceNode> rawSources = AggregationNode.findAggregationSourceNode(root);

    // Step 1: construct SeriesAggregationSourceNode for each data region of one Path
    List<SeriesAggregationSourceNode> sources = new ArrayList<>();
    for (SeriesAggregationSourceNode child : rawSources) {
      constructAggregationSourceNodeForPerRegion(context, sources, child);
    }

    // Step 2: change step to PARTIAL for each SeriesAggregationSourceNode and update TypeProvider
    for (SeriesAggregationSourceNode source : sources) {
      source
          .getAggregationDescriptorList()
          .forEach(
              d -> {
                d.setStep(AggregationStep.PARTIAL);
                LogicalPlanBuilder.updateTypeProviderByPartialAggregation(
                    d, context.queryContext.getTypeProvider());
              });
    }

    // Step 3: group all SeriesAggregationSourceNode by each region
    return sources.stream().collect(Collectors.groupingBy(SourceNode::getRegionReplicaSet));
  }

  private Map<TRegionReplicaSet, List<SeriesAggregationSourceNode>>
      splitAggregationSourceByPartition(
          PlanNode root,
          DistributionPlanContext context,
          List<SeriesAggregationSourceNode> sources,
          boolean[] eachSeriesOneRegion,
          Map<PartialPath, Integer> regionCountPerSeries) {
    // Step 0: get all SeriesAggregationSourceNode in PlanNodeTree
    List<SeriesAggregationSourceNode> rawSources = AggregationNode.findAggregationSourceNode(root);

    // Step 1: construct SeriesAggregationSourceNode for each data region of one Path
    for (SeriesAggregationSourceNode child : rawSources) {
      regionCountPerSeries.put(
          child.getPartitionPath(),
          constructAggregationSourceNodeForPerRegion(context, sources, child));
    }

    // Step 2: change step to SINGLE or PARTIAL for each SeriesAggregationSourceNode and update
    // TypeProvider
    for (SeriesAggregationSourceNode source : sources) {
      boolean isSingle = regionCountPerSeries.get(source.getPartitionPath()) == 1;
      source
          .getAggregationDescriptorList()
          .forEach(
              d -> {
                if (isSingle) {
                  d.setStep(AggregationStep.SINGLE);
                } else {
                  eachSeriesOneRegion[0] = false;
                  d.setStep(AggregationStep.PARTIAL);
                  LogicalPlanBuilder.updateTypeProviderByPartialAggregation(
                      d, context.queryContext.getTypeProvider());
                }
              });
    }

    // Step 3: group all SeriesAggregationSourceNode by each region
    return sources.stream().collect(Collectors.groupingBy(SourceNode::getRegionReplicaSet));
  }

  private int constructAggregationSourceNodeForPerRegion(
      DistributionPlanContext context,
      List<SeriesAggregationSourceNode> sources,
      SeriesAggregationSourceNode child) {
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
    return dataDistribution.size();
  }

  public List<PlanNode> visit(PlanNode node, DistributionPlanContext context) {
    return node.accept(this, context);
  }
}
