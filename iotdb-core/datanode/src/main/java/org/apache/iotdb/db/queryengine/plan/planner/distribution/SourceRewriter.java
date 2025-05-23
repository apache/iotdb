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
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.BaseSourceRewriter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ActiveRegionScanMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationMergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.HorizontallyConcatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MultiChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.RawDataAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.FullOuterTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.join.InnerTimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.DeviceRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.RegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.TimeseriesRegionScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.LAST_VALUE;
import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.partition.DataPartition.NOT_ASSIGNED;
import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionTypeAnalyzer.analyzeExpression;
import static org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanBuilder.updateTypeProviderByPartialAggregation;
import static org.apache.iotdb.db.utils.constant.SqlConstant.AVG;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT_IF;
import static org.apache.iotdb.db.utils.constant.SqlConstant.DIFF;
import static org.apache.iotdb.db.utils.constant.SqlConstant.FIRST_VALUE;
import static org.apache.iotdb.db.utils.constant.SqlConstant.TIME_DURATION;

public class SourceRewriter extends BaseSourceRewriter<DistributionPlanContext> {

  private final Analysis analysis;

  private static final OrderByParameter TIME_ASC =
      new OrderByParameter(Collections.singletonList(new SortItem(OrderByKey.TIME, Ordering.ASC)));

  private static final OrderByParameter TIME_DESC =
      new OrderByParameter(Collections.singletonList(new SortItem(OrderByKey.TIME, Ordering.DESC)));

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

    if (analysis.isDeviceViewSpecialProcess()) {
      List<PlanNode> rewroteChildren = rewrite(node.getChild(), context);
      if (rewroteChildren.size() != 1) {
        throw new IllegalStateException("SingleDeviceViewNode have only one child");
      }
      node.setChild(rewroteChildren.get(0));
      return Collections.singletonList(node);
    }

    IDeviceID device = node.getDevice();
    List<TRegionReplicaSet> regionReplicaSets =
        !analysis.useLogicalView()
            ? new ArrayList<>(analysis.getPartitionInfo(device, context.getPartitionTimeFilter()))
            : new ArrayList<>(
                analysis.getPartitionInfo(
                    analysis.getOutputDeviceToQueriedDevicesMap().get(device),
                    context.getPartitionTimeFilter()));

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
    if (node.getDevices().size() != node.getChildren().size()) {
      throw new IllegalArgumentException(
          "size of devices and its children in DeviceViewNode should be same");
    }

    // Step 1: constructs DeviceViewSplits
    Set<TRegionReplicaSet> relatedDataRegions = new HashSet<>();
    List<DeviceViewSplit> deviceViewSplits = new ArrayList<>();
    boolean existDeviceCrossRegion = false;

    for (int i = 0; i < node.getDevices().size(); i++) {
      IDeviceID outputDevice = node.getDevices().get(i);
      PlanNode child = node.getChildren().get(i);
      List<TRegionReplicaSet> regionReplicaSets =
          analysis.useLogicalView()
              ? new ArrayList<>(
                  analysis.getPartitionInfo(
                      analysis.getOutputDeviceToQueriedDevicesMap().get(outputDevice),
                      context.getPartitionTimeFilter()))
              : new ArrayList<>(
                  analysis.getPartitionInfo(outputDevice, context.getPartitionTimeFilter()));
      if (regionReplicaSets.size() > 1 && !existDeviceCrossRegion) {
        existDeviceCrossRegion = true;
        if (analysis.isDeviceViewSpecialProcess() && aggregationCannotUseMergeSort()) {
          return processSpecialDeviceView(node, context);
        }
      }
      deviceViewSplits.add(new DeviceViewSplit(outputDevice, child, regionReplicaSets));
      relatedDataRegions.addAll(regionReplicaSets);
    }

    // Step 2: Iterate all partition and create DeviceViewNode for each region
    List<PlanNode> deviceViewNodeList = new ArrayList<>();
    if (existDeviceCrossRegion) {
      constructDeviceViewNodeListWithCrossRegion(
          deviceViewNodeList, relatedDataRegions, deviceViewSplits, node, context);
    } else {
      constructDeviceViewNodeListWithoutCrossRegion(
          deviceViewNodeList, deviceViewSplits, node, context, analysis);
    }

    // 1. Only one DeviceViewNode, the is no need to use MergeSortNode.
    // 2. for DeviceView+SortNode case, the parent of DeviceViewNode will be SortNode, MergeSortNode
    // will be generated in {@link #visitSort}.
    // 3. for DeviceView+TopKNode case, there is no need MergeSortNode, TopKNode can output the
    // sorted result.
    if (deviceViewNodeList.size() == 1 || analysis.isHasSortNode() || analysis.isUseTopKNode()) {
      return deviceViewNodeList;
    }

    // aggregation and some device cross region, user AggregationMergeSortNode
    // 1. generate old and new measurement idx relationship
    // 2. generate new outputColumns for each subDeviceView
    if (existDeviceCrossRegion && analysis.isDeviceViewSpecialProcess()) {
      Map<Integer, List<Integer>> newMeasurementIdxMap = new HashMap<>();
      List<String> newPartialOutputColumns = new ArrayList<>();
      Set<Expression> deviceViewOutputExpressions = analysis.getDeviceViewOutputExpressions();

      int i = 0, newIdxSum = 0;
      for (Expression expression : deviceViewOutputExpressions) {
        if (i == 0) {
          newPartialOutputColumns.add(expression.getOutputSymbol());
          i++;
          newIdxSum++;
          continue;
        }
        FunctionExpression aggExpression = (FunctionExpression) expression;
        // used for AVG, FIRST_VALUE, LAST_VALUE, TIME_DURATION agg function
        List<String> actualPartialAggregationNames =
            getActualPartialAggregationNames(aggExpression.getFunctionName());
        for (String actualAggName : actualPartialAggregationNames) {
          FunctionExpression partialFunctionExpression =
              new FunctionExpression(
                  actualAggName,
                  aggExpression.getFunctionAttributes(),
                  aggExpression.getExpressions());
          if (actualPartialAggregationNames.size() > 1) {
            TSDataType dataType = analyzeExpression(analysis, partialFunctionExpression);
            context
                .queryContext
                .getTypeProvider()
                .setTreeModelType(partialFunctionExpression.getOutputSymbol(), dataType);
          }
          newPartialOutputColumns.add(partialFunctionExpression.getOutputSymbol());
        }
        newMeasurementIdxMap.put(
            i++,
            actualPartialAggregationNames.size() > 1
                ? Arrays.asList(newIdxSum++, newIdxSum++)
                : Collections.singletonList(newIdxSum++));
      }

      for (IDeviceID device : node.getDevices()) {
        List<Integer> oldMeasurementIdxList = node.getDeviceToMeasurementIndexesMap().get(device);
        List<Integer> newMeasurementIdxList = new ArrayList<>();
        oldMeasurementIdxList.forEach(
            idx -> newMeasurementIdxList.addAll(newMeasurementIdxMap.get(idx)));
        node.getDeviceToMeasurementIndexesMap().put(device, newMeasurementIdxList);
      }

      for (PlanNode planNode : deviceViewNodeList) {
        DeviceViewNode deviceViewNode = (DeviceViewNode) planNode;
        deviceViewNode.setOutputColumnNames(newPartialOutputColumns);
        transferAggregatorsRecursively(planNode, context);
      }

      boolean hasGroupBy =
          analysis.getGroupByTimeParameter() != null || analysis.hasGroupByParameter();
      AggregationMergeSortNode mergeSortNode =
          new AggregationMergeSortNode(
              context.queryContext.getQueryId().genPlanNodeId(),
              node.getMergeOrderParameter(),
              node.getOutputColumnNames(),
              deviceViewOutputExpressions,
              hasGroupBy);
      deviceViewNodeList.forEach(mergeSortNode::addChild);
      return Collections.singletonList(mergeSortNode);
    } else {
      MergeSortNode mergeSortNode =
          new MergeSortNode(
              context.queryContext.getQueryId().genPlanNodeId(),
              node.getMergeOrderParameter(),
              node.getOutputColumnNames());
      deviceViewNodeList.forEach(mergeSortNode::addChild);
      return Collections.singletonList(mergeSortNode);
    }
  }

  /**
   * aggregation align by device, and aggregation is `count_if` or `diff`, or aggregation used with
   * group by parameter (session, variation, count), use the old aggregation logic
   */
  private boolean aggregationCannotUseMergeSort() {
    if (analysis.hasGroupByParameter()) {
      return true;
    }

    for (Expression expression : analysis.getDeviceViewOutputExpressions()) {
      if (expression instanceof FunctionExpression) {
        String functionName = ((FunctionExpression) expression).getFunctionName();
        if (COUNT_IF.equalsIgnoreCase(functionName) || DIFF.equalsIgnoreCase(functionName)) {
          return true;
        }
      }
    }

    return false;
  }

  private void constructDeviceViewNodeListWithCrossRegion(
      List<PlanNode> deviceViewNodeList,
      Set<TRegionReplicaSet> relatedDataRegions,
      List<DeviceViewSplit> deviceViewSplits,
      DeviceViewNode node,
      DistributionPlanContext context) {
    for (TRegionReplicaSet regionReplicaSet : relatedDataRegions) {
      List<IDeviceID> devices = new ArrayList<>();
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
  }

  private void constructDeviceViewNodeListWithoutCrossRegion(
      List<PlanNode> deviceViewNodeList,
      List<DeviceViewSplit> deviceViewSplits,
      DeviceViewNode node,
      DistributionPlanContext context,
      Analysis analysis) {

    Map<TRegionReplicaSet, DeviceViewNode> regionDeviceViewMap = new HashMap<>();
    for (DeviceViewSplit split : deviceViewSplits) {
      if (split.dataPartitions.size() != 1) {
        throw new IllegalStateException(
            "In non-cross data region device-view situation, "
                + "each device should only have on data partition.");
      }
      TRegionReplicaSet region = split.dataPartitions.iterator().next();
      DeviceViewNode regionDeviceViewNode =
          regionDeviceViewMap.computeIfAbsent(
              region,
              k -> {
                DeviceViewNode deviceViewNode = cloneDeviceViewNodeWithoutChild(node, context);
                deviceViewNodeList.add(deviceViewNode);
                return deviceViewNode;
              });
      PlanNode childNode = split.buildPlanNodeInRegion(region, context.queryContext);
      if (analysis.isDeviceViewSpecialProcess()) {
        List<PlanNode> rewriteResult = rewrite(childNode, context);
        if (rewriteResult.size() != 1) {
          throw new IllegalStateException(
              "In non-cross data region aggregation device-view situation, "
                  + "each rewrite child node of DeviceView should only be one.");
        }
        childNode = rewriteResult.get(0);
      }
      regionDeviceViewNode.addChildDeviceNode(split.device, childNode);
    }
  }

  public List<String> getActualPartialAggregationNames(String aggregationType) {
    List<String> outputAggregationNames = new ArrayList<>();
    switch (aggregationType) {
      case AVG:
        outputAggregationNames.add(SqlConstant.COUNT);
        outputAggregationNames.add(SqlConstant.SUM);
        break;
      case FIRST_VALUE:
        outputAggregationNames.add(FIRST_VALUE);
        outputAggregationNames.add(SqlConstant.MIN_TIME);
        break;
      case LAST_VALUE:
        outputAggregationNames.add(SqlConstant.LAST_VALUE);
        outputAggregationNames.add(SqlConstant.MAX_TIME);
        break;
      case TIME_DURATION:
        outputAggregationNames.add(SqlConstant.MAX_TIME);
        outputAggregationNames.add(SqlConstant.MIN_TIME);
        break;
      default:
        // TODO how about UDAF?
        outputAggregationNames.add(aggregationType);
    }
    return outputAggregationNames;
  }

  private void transferAggregatorsRecursively(PlanNode planNode, DistributionPlanContext context) {
    List<AggregationDescriptor> descriptorList = getAggregationDescriptors(planNode);
    if (descriptorList != null) {
      for (AggregationDescriptor descriptor : descriptorList) {
        descriptor.setStep(
            planNode instanceof SlidingWindowAggregationNode
                ? AggregationStep.INTERMEDIATE
                : AggregationStep.PARTIAL);
        updateTypeProviderByPartialAggregation(descriptor, context.queryContext.getTypeProvider());
      }
    }
    planNode.getChildren().forEach(child -> transferAggregatorsRecursively(child, context));
  }

  private List<AggregationDescriptor> getAggregationDescriptors(PlanNode planNode) {
    List<AggregationDescriptor> descriptorList = null;
    if (planNode instanceof SeriesAggregationSourceNode) {
      descriptorList = ((SeriesAggregationSourceNode) planNode).getAggregationDescriptorList();
    } else if (planNode instanceof AggregationNode) {
      descriptorList = ((AggregationNode) planNode).getAggregationDescriptorList();
    } else if (planNode instanceof SlidingWindowAggregationNode) {
      descriptorList = ((SlidingWindowAggregationNode) planNode).getAggregationDescriptorList();
    } else if (planNode instanceof RawDataAggregationNode) {
      descriptorList = ((RawDataAggregationNode) planNode).getAggregationDescriptorList();
    }
    return descriptorList;
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

  @Override
  public List<PlanNode> visitTransform(TransformNode node, DistributionPlanContext context) {
    List<PlanNode> children = rewrite(node.getChild(), context);
    if (children.size() == 1) {
      node.setChild(children.get(0));
      return Collections.singletonList(node);
    }

    // for some query such as `select avg(s1)+avg(s2) from root.** where count(s3)>1 align by
    // device`,
    // the children of TransformNode may be N(N>1) DeviceViewNodes, so need return N TransformNodes
    return children.stream()
        .map(
            child -> {
              TransformNode transformNode = cloneTransformNodeWithOutChild(node, context);
              transformNode.addChild(child);
              return transformNode;
            })
        .collect(Collectors.toList());
  }

  private TransformNode cloneTransformNodeWithOutChild(
      TransformNode node, DistributionPlanContext context) {
    return new TransformNode(
        context.queryContext.getQueryId().genPlanNodeId(),
        node.getOutputExpressions(),
        node.isKeepNull(),
        node.getScanOrder());
  }

  @Override
  public List<PlanNode> visitSort(SortNode node, DistributionPlanContext context) {

    analysis.setHasSortNode(true);

    List<PlanNode> children = rewrite(node.getChild(), context);
    if (children.size() == 1) {
      node.setChild(children.get(0));
      return Collections.singletonList(node);
    }

    MergeSortNode mergeSortNode =
        new MergeSortNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            node.getOrderByParameter(),
            node.getOutputColumnNames());

    for (PlanNode child : children) {
      SortNode sortNode = cloneSortNodeWithOutChild(node, context);
      sortNode.setChild(child);
      mergeSortNode.addChild(sortNode);
    }
    return Collections.singletonList(mergeSortNode);
  }

  private SortNode cloneSortNodeWithOutChild(SortNode node, DistributionPlanContext context) {
    return new SortNode(
        context.queryContext.getQueryId().genPlanNodeId(), node.getOrderByParameter());
  }

  @Override
  public List<PlanNode> visitSchemaQueryMerge(
      SchemaQueryMergeNode node, DistributionPlanContext context) {
    SchemaQueryMergeNode root = (SchemaQueryMergeNode) node.clone();
    SchemaQueryScanNode seed = (SchemaQueryScanNode) node.getChildren().get(0);
    List<PartialPath> pathPatternList = seed.getPathPatternList();
    Set<TRegionReplicaSet> regionsOfSystemDatabase = new HashSet<>();
    if (pathPatternList.size() == 1) {
      // the path pattern overlaps with all storageGroup or storageGroup.**
      TreeSet<TRegionReplicaSet> schemaRegions =
          new TreeSet<>(Comparator.comparingInt(region -> region.getRegionId().getId()));
      analysis
          .getSchemaPartitionInfo()
          .getSchemaPartitionMap()
          .forEach(
              (storageGroup, deviceGroup) -> {
                if (storageGroup.equals(SchemaConstant.SYSTEM_DATABASE)) {
                  deviceGroup.forEach(
                      (deviceGroupId, schemaRegionReplicaSet) ->
                          regionsOfSystemDatabase.add(schemaRegionReplicaSet));
                } else {
                  deviceGroup.forEach(
                      (deviceGroupId, schemaRegionReplicaSet) ->
                          schemaRegions.add(schemaRegionReplicaSet));
                }
              });
      schemaRegions.forEach(
          region ->
              addSchemaSourceNode(
                  root,
                  seed.getPath(),
                  region,
                  context.queryContext.getQueryId().genPlanNodeId(),
                  seed));
      regionsOfSystemDatabase.forEach(
          region ->
              addSchemaSourceNode(
                  root,
                  seed.getPath(),
                  region,
                  context.queryContext.getQueryId().genPlanNodeId(),
                  seed));
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
                if (storageGroup.equals(SchemaConstant.SYSTEM_DATABASE)) {
                  deviceGroup.forEach(
                      (deviceGroupId, schemaRegionReplicaSet) ->
                          regionsOfSystemDatabase.add(schemaRegionReplicaSet));
                } else {
                  deviceGroup.forEach(
                      (deviceGroupId, schemaRegionReplicaSet) ->
                          storageGroupSchemaRegionMap
                              .computeIfAbsent(storageGroup, k -> new HashSet<>())
                              .add(schemaRegionReplicaSet));
                }
              });

      storageGroupSchemaRegionMap.forEach(
          (storageGroup, schemaRegionSet) -> {
            List<PartialPath> filteredPathPatternList =
                filterPathPattern(patternTree, storageGroup);
            schemaRegionSet.forEach(
                region ->
                    addSchemaSourceNode(
                        root,
                        filteredPathPatternList.size() == 1
                            ? filteredPathPatternList.get(0)
                            : seed.getPath(),
                        region,
                        context.queryContext.getQueryId().genPlanNodeId(),
                        seed));
          });
      if (!regionsOfSystemDatabase.isEmpty()) {
        List<PartialPath> filteredPathPatternList =
            filterPathPattern(patternTree, SchemaConstant.SYSTEM_DATABASE);
        regionsOfSystemDatabase.forEach(
            region ->
                addSchemaSourceNode(
                    root,
                    filteredPathPatternList.size() == 1
                        ? filteredPathPatternList.get(0)
                        : seed.getPath(),
                    region,
                    context.queryContext.getQueryId().genPlanNodeId(),
                    seed));
      }
    }
    return Collections.singletonList(root);
  }

  private List<PartialPath> filterPathPattern(PathPatternTree patternTree, String database) {
    // extract the patterns overlap with current database
    Set<PartialPath> filteredPathPatternSet = new HashSet<>();
    try {
      PartialPath storageGroupPath = new PartialPath(database);
      filteredPathPatternSet.addAll(patternTree.getOverlappedPathPatterns(storageGroupPath));
      filteredPathPatternSet.addAll(
          patternTree.getOverlappedPathPatterns(
              storageGroupPath.concatNode(MULTI_LEVEL_PATH_WILDCARD)));
    } catch (IllegalPathException ignored) {
      // won't reach here
    }
    return new ArrayList<>(filteredPathPatternSet);
  }

  private void addSchemaSourceNode(
      SchemaQueryMergeNode root,
      PartialPath pathPattern,
      TRegionReplicaSet schemaRegion,
      PlanNodeId planNodeId,
      SchemaQueryScanNode seed) {
    SchemaQueryScanNode schemaQueryScanNode = (SchemaQueryScanNode) seed.clone();
    schemaQueryScanNode.setPlanNodeId(planNodeId);
    schemaQueryScanNode.setRegionReplicaSet(schemaRegion);
    schemaQueryScanNode.setPath(pathPattern);
    root.addChild(schemaQueryScanNode);
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
            (storageGroup, deviceGroup) ->
                deviceGroup.forEach(
                    (deviceGroupId, schemaRegionReplicaSet) ->
                        schemaRegions.add(schemaRegionReplicaSet)));
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
    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(
            context.queryContext.getQueryId().genPlanNodeId(), node.getScanOrder());
    return processRawSeriesScan(node, context, fullOuterTimeJoinNode);
  }

  @Override
  public List<PlanNode> visitAlignedSeriesScan(
      AlignedSeriesScanNode node, DistributionPlanContext context) {
    FullOuterTimeJoinNode fullOuterTimeJoinNode =
        new FullOuterTimeJoinNode(
            context.queryContext.getQueryId().genPlanNodeId(), node.getScanOrder());
    return processRawSeriesScan(node, context, fullOuterTimeJoinNode);
  }

  @Override
  public List<PlanNode> visitLastQueryScan(
      LastQueryScanNode node, DistributionPlanContext context) {
    LastQueryNode mergeNode =
        new LastQueryNode(context.queryContext.getQueryId().genPlanNodeId(), null, false);
    return processRawSeriesScan(node, context, mergeNode);
  }

  @Override
  public List<PlanNode> visitAlignedLastQueryScan(
      AlignedLastQueryScanNode node, DistributionPlanContext context) {
    LastQueryNode mergeNode =
        new LastQueryNode(context.queryContext.getQueryId().genPlanNodeId(), null, false);
    return processRawSeriesScan(node, context, mergeNode);
  }

  private List<PlanNode> processRegionScan(RegionScanNode node, DistributionPlanContext context) {
    List<PlanNode> planNodeList = splitRegionScanNodeByRegion(node, context);
    if (planNodeList.size() == 1) {
      return planNodeList;
    }

    boolean outputCountInScanNode = node.isOutputCount() && !context.isOneSeriesInMultiRegion();
    ActiveRegionScanMergeNode regionMergeNode =
        new ActiveRegionScanMergeNode(
            context.queryContext.getQueryId().genPlanNodeId(),
            node.isOutputCount(),
            !outputCountInScanNode,
            node.getSize());
    for (PlanNode planNode : planNodeList) {
      ((RegionScanNode) planNode).setOutputCount(outputCountInScanNode);
      regionMergeNode.addChild(planNode);
    }
    return Collections.singletonList(regionMergeNode);
  }

  @Override
  public List<PlanNode> visitDeviceRegionScan(
      DeviceRegionScanNode node, DistributionPlanContext context) {
    return processRegionScan(node, context);
  }

  @Override
  public List<PlanNode> visitTimeSeriesRegionScan(
      TimeseriesRegionScanNode node, DistributionPlanContext context) {
    return processRegionScan(node, context);
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

  private List<PlanNode> splitRegionScanNodeByRegion(
      RegionScanNode node, DistributionPlanContext context) {
    Map<TRegionReplicaSet, RegionScanNode> regionScanNodeMap = new HashMap<>();
    Set<PartialPath> devicesList = node.getDevicePaths();
    boolean isAllDeviceOnlyInOneRegion = true;

    for (PartialPath device : devicesList) {
      List<TRegionReplicaSet> dataDistribution =
          analysis.getPartitionInfoByDevice(device, context.getPartitionTimeFilter());
      isAllDeviceOnlyInOneRegion = isAllDeviceOnlyInOneRegion && dataDistribution.size() == 1;
      for (TRegionReplicaSet dataRegion : dataDistribution) {
        regionScanNodeMap
            .computeIfAbsent(
                dataRegion,
                k -> {
                  RegionScanNode regionScanNode = (RegionScanNode) node.clone();
                  regionScanNode.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
                  regionScanNode.setRegionReplicaSet(dataRegion);
                  regionScanNode.setOutputCount(node.isOutputCount());
                  regionScanNode.clearPath();
                  return regionScanNode;
                })
            .addDevicePath(device, node);
      }
    }

    context.setOneSeriesInMultiRegion(!isAllDeviceOnlyInOneRegion);
    // If there is only one region, return directly
    return new ArrayList<>(regionScanNodeMap.values());
  }

  private List<PlanNode> splitSeriesSourceNodeByPartition(
      SeriesSourceNode node, DistributionPlanContext context) {
    List<PlanNode> ret = new ArrayList<>();
    List<TRegionReplicaSet> dataDistribution =
        analysis.getPartitionInfo(node.getPartitionPath(), context.getPartitionTimeFilter());
    if (dataDistribution.size() == 1) {
      node.setRegionReplicaSet(dataDistribution.get(0));
      ret.add(node);
      return ret;
    }

    for (TRegionReplicaSet dataRegion : dataDistribution) {
      SeriesSourceNode split = (SeriesSourceNode) node.clone();
      split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
      split.setRegionReplicaSet(dataRegion);
      context
          .getQueryContext()
          .reserveMemoryForFrontEnd(
              MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(split));
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
        analysis.getPartitionInfo(node.getPartitionPath(), context.getPartitionTimeFilter());
    if (dataDistribution.size() == 1) {
      node.setRegionReplicaSet(dataDistribution.get(0));
      return Collections.singletonList(node);
    }
    List<AggregationDescriptor> leafAggDescriptorList = new ArrayList<>();
    node.getAggregationDescriptorList()
        .forEach(
            descriptor ->
                leafAggDescriptorList.add(
                    new AggregationDescriptor(
                        descriptor.getAggregationFuncName(),
                        AggregationStep.PARTIAL,
                        descriptor.getInputExpressions(),
                        descriptor.getInputAttributes())));
    leafAggDescriptorList.forEach(
        d -> updateTypeProviderByPartialAggregation(d, context.queryContext.getTypeProvider()));
    List<AggregationDescriptor> rootAggDescriptorList = new ArrayList<>();
    node.getAggregationDescriptorList()
        .forEach(
            descriptor ->
                rootAggDescriptorList.add(
                    new AggregationDescriptor(
                        descriptor.getAggregationFuncName(),
                        context.isRoot ? AggregationStep.FINAL : AggregationStep.INTERMEDIATE,
                        descriptor.getInputExpressions(),
                        descriptor.getInputAttributes())));

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
      context
          .getQueryContext()
          .reserveMemoryForFrontEnd(
              MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(split));
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
    context.setForceAddParent();
    PlanNode root = processRawMultiChildNode(node, context, false);
    if (context.queryMultiRegion) {
      PlanNode newRoot = genLastQueryRootNode(node, context);
      // add sort op for each if we add LastQueryMergeNode as root
      if (newRoot instanceof LastQueryMergeNode && !node.needOrderByTimeseries()) {
        addSortForEachLastQueryNode(root, Ordering.ASC);
      }
      root.getChildren().forEach(newRoot::addChild);
      return Collections.singletonList(newRoot);
    } else {
      return Collections.singletonList(root);
    }
  }

  private void addSortForEachLastQueryNode(PlanNode root, Ordering timeseriesOrdering) {
    if (root instanceof LastQueryNode
        && (root.getChildren().get(0) instanceof LastQueryScanNode
            || root.getChildren().get(0) instanceof AlignedLastQueryScanNode)) {
      LastQueryNode lastQueryNode = (LastQueryNode) root;
      lastQueryNode.setTimeseriesOrdering(timeseriesOrdering);
      // sort children node
      lastQueryNode.setChildren(
          lastQueryNode.getChildren().stream()
              .sorted(
                  Comparator.comparing(
                      child -> {
                        String sortKey = "";
                        if (child instanceof LastQueryScanNode) {
                          sortKey = ((LastQueryScanNode) child).getOutputSymbolForSort();
                        } else if (child instanceof AlignedLastQueryScanNode) {
                          sortKey = ((AlignedLastQueryScanNode) child).getOutputSymbolForSort();
                        }
                        return sortKey;
                      }))
              .collect(Collectors.toList()));
      lastQueryNode
          .getChildren()
          .forEach(
              child -> {
                if (child instanceof AlignedLastQueryScanNode) {
                  // sort the measurements of AlignedPath for LastQueryMergeOperator
                  ((AlignedLastQueryScanNode) child)
                      .getSeriesPath()
                      .sortMeasurement(Comparator.naturalOrder());
                }
              });
    } else {
      for (PlanNode child : root.getChildren()) {
        addSortForEachLastQueryNode(child, timeseriesOrdering);
      }
    }
  }

  private PlanNode genLastQueryRootNode(LastQueryNode node, DistributionPlanContext context) {
    PlanNodeId id = context.queryContext.getQueryId().genPlanNodeId();
    // if the series is from multi regions or order by clause only refer to timeseries, use
    // LastQueryMergeNode
    if (context.oneSeriesInMultiRegion || node.needOrderByTimeseries()) {
      return new LastQueryMergeNode(
          id, node.getTimeseriesOrdering(), node.isContainsLastTransformNode());
    }
    return new LastQueryCollectNode(id, node.isContainsLastTransformNode());
  }

  @Override
  public List<PlanNode> visitInnerTimeJoin(
      InnerTimeJoinNode node, DistributionPlanContext context) {
    // All child nodes should be SourceNode
    List<SeriesSourceNode> seriesScanNodes = new ArrayList<>(node.getChildren().size());
    List<List<List<TTimePartitionSlot>>> sourceTimeRangeList = new ArrayList<>();
    for (PlanNode child : node.getChildren()) {
      if (!(child instanceof SeriesSourceNode)) {
        throw new IllegalStateException(
            "All child nodes of InnerTimeJoinNode should be SeriesSourceNode");
      }
      SeriesSourceNode sourceNode = (SeriesSourceNode) child;
      seriesScanNodes.add(sourceNode);
      sourceTimeRangeList.add(
          analysis.getTimePartitionRange(
              sourceNode.getPartitionPath(), context.getPartitionTimeFilter()));
    }

    List<List<TTimePartitionSlot>> res = splitTimePartition(sourceTimeRangeList);

    List<PlanNode> children = splitInnerTimeJoinNode(res, node, context, seriesScanNodes);

    if (children.size() == 1) {
      return children;
    } else {
      // add merge sort node for InnerTimeJoinNodes
      // TODO add new type of Node, just traverse all child nodes sequentially in the future
      List<String> outputColumnNames = node.getOutputColumnNames();
      MergeSortNode mergeSortNode =
          new MergeSortNode(
              context.queryContext.getQueryId().genPlanNodeId(),
              node.getMergeOrder() == Ordering.ASC ? TIME_ASC : TIME_DESC,
              outputColumnNames);
      // set unified outputColumnNames for each child InnerTimeJoinNode
      children.forEach(
          child -> ((InnerTimeJoinNode) child).setOutputColumnNames(outputColumnNames));

      mergeSortNode.setChildren(children);
      return Collections.singletonList(mergeSortNode);
    }
  }

  // make it as protected just for UT usage
  protected static List<List<TTimePartitionSlot>> splitTimePartition(
      List<List<List<TTimePartitionSlot>>> childTimePartitionList) {
    if (childTimePartitionList.isEmpty()) {
      return Collections.emptyList();
    }
    List<List<TTimePartitionSlot>> res = new ArrayList<>(childTimePartitionList.get(0));
    for (int i = 1, size = childTimePartitionList.size(); i < size && !res.isEmpty(); i++) {
      res = combineTwoTimePartitionList(res, childTimePartitionList.get(i));
    }
    return res;
  }

  private static List<List<TTimePartitionSlot>> combineTwoTimePartitionList(
      List<List<TTimePartitionSlot>> left, List<List<TTimePartitionSlot>> right) {
    int leftIndex = 0;
    int leftSize = left.size();
    int rightIndex = 0;
    int rightSize = right.size();

    List<List<TTimePartitionSlot>> res = new ArrayList<>(Math.max(leftSize, rightSize));

    int previousResIndex = 0;
    res.add(new ArrayList<>());
    int leftCurrentListIndex = 0;
    int rightCurrentListIndex = 0;
    while (leftIndex < leftSize && rightIndex < rightSize) {
      List<TTimePartitionSlot> leftCurrentList = left.get(leftIndex);
      List<TTimePartitionSlot> rightCurrentList = right.get(rightIndex);
      int leftCurrentListSize = leftCurrentList.size();
      int rightCurrentListSize = rightCurrentList.size();
      while (leftCurrentListIndex < leftCurrentListSize
          && rightCurrentListIndex < rightCurrentListSize) {
        // only keep time partition in All SeriesSlot
        if (leftCurrentList.get(leftCurrentListIndex).startTime
            == rightCurrentList.get(rightCurrentListIndex).startTime) {
          // new continuous time range
          if ((leftCurrentListIndex == 0 && leftIndex != 0)
              || (rightCurrentListIndex == 0 && rightIndex != 0)) {
            previousResIndex++;
            res.add(new ArrayList<>());
          }
          res.get(previousResIndex).add(leftCurrentList.get(leftCurrentListIndex));
          leftCurrentListIndex++;
          rightCurrentListIndex++;
        } else if (leftCurrentList.get(leftCurrentListIndex).startTime
            < rightCurrentList.get(rightCurrentListIndex).startTime) {
          leftCurrentListIndex++;
        } else {
          rightCurrentListIndex++;
        }
      }
      if (leftCurrentListIndex == leftCurrentListSize) {
        leftIndex++;
        leftCurrentListIndex = 0;
      }
      if (rightCurrentListIndex == rightCurrentListSize) {
        rightIndex++;
        rightCurrentListIndex = 0;
      }
    }
    return res;
  }

  private List<PlanNode> splitInnerTimeJoinNode(
      List<List<TTimePartitionSlot>> continuousTimeRange,
      InnerTimeJoinNode node,
      DistributionPlanContext context,
      List<SeriesSourceNode> seriesScanNodes) {
    List<PlanNode> subInnerJoinNode = new ArrayList<>(continuousTimeRange.size());
    for (List<TTimePartitionSlot> oneRegion : continuousTimeRange) {
      if (!oneRegion.isEmpty()) {
        // TODO InnerTimeJoinNode's header
        InnerTimeJoinNode innerTimeJoinNode = (InnerTimeJoinNode) node.clone();
        innerTimeJoinNode.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());

        List<Long> timePartitionIds = convertToTimePartitionIds(oneRegion);
        innerTimeJoinNode.setTimePartitions(timePartitionIds);

        // region group id -> parent InnerTimeJoinNode
        Map<Integer, PlanNode> map = new HashMap<>();
        for (SeriesSourceNode sourceNode : seriesScanNodes) {
          TRegionReplicaSet dataRegion =
              analysis.getPartitionInfo(sourceNode.getPartitionPath(), oneRegion.get(0));
          InnerTimeJoinNode parent =
              (InnerTimeJoinNode)
                  map.computeIfAbsent(
                      dataRegion.regionId.id,
                      k -> {
                        InnerTimeJoinNode value = (InnerTimeJoinNode) node.clone();
                        value.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
                        value.setTimePartitions(timePartitionIds);
                        return value;
                      });
          SeriesSourceNode split = (SeriesSourceNode) sourceNode.clone();
          split.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
          split.setRegionReplicaSet(dataRegion);
          parent.addChild(split);
        }

        for (Map.Entry<Integer, PlanNode> entry : map.entrySet()) {
          InnerTimeJoinNode joinNode = (InnerTimeJoinNode) entry.getValue();
          if (joinNode.getChildren().size() == 1) {
            map.put(entry.getKey(), joinNode.getChildren().get(0));
          }
        }

        if (map.size() > 1) {
          map.values().forEach(innerTimeJoinNode::addChild);
          subInnerJoinNode.add(innerTimeJoinNode);
        } else {
          subInnerJoinNode.add(map.values().iterator().next());
        }
      }
    }

    if (subInnerJoinNode.isEmpty()) {
      for (SeriesSourceNode sourceNode : seriesScanNodes) {
        sourceNode.setRegionReplicaSet(NOT_ASSIGNED);
      }
      return Collections.singletonList(node);
    }
    return subInnerJoinNode;
  }

  private List<Long> convertToTimePartitionIds(List<TTimePartitionSlot> timePartitionSlotList) {
    List<Long> res = new ArrayList<>(timePartitionSlotList.size());
    for (TTimePartitionSlot timePartitionSlot : timePartitionSlotList) {
      res.add(TimePartitionUtils.getTimePartitionId(timePartitionSlot.startTime));
    }
    return res;
  }

  @Override
  public List<PlanNode> visitFullOuterTimeJoin(
      FullOuterTimeJoinNode node, DistributionPlanContext context) {
    // Although some logic is similar between Aggregation and RawDataQuery,
    // we still use separate method to process the distribution planning now
    // to make the planning procedure more clear
    if (containsAggregationSource(node)) {
      return planAggregationWithTimeJoin(node, context);
    }
    return Collections.singletonList(processRawMultiChildNode(node, context, true));
  }

  // Only `visitFullOuterTimeJoin` and `visitLastQuery` invoke this method
  private PlanNode processRawMultiChildNode(
      MultiChildProcessNode node, DistributionPlanContext context, boolean isTimeJoin) {
    MultiChildProcessNode root = (MultiChildProcessNode) node.clone();
    Map<TRegionReplicaSet, List<SourceNode>> sourceGroup = groupBySourceNodes(node, context);

    // For the source nodes which belong to same data region, add a TimeJoinNode for them
    // and make the new TimeJoinNode as the child of current TimeJoinNode
    // TODO: (xingtanzjr) optimize the procedure here to remove duplicated TimeJoinNode
    boolean addParent = false;
    for (Map.Entry<TRegionReplicaSet, List<SourceNode>> entry : sourceGroup.entrySet()) {
      TRegionReplicaSet region = entry.getKey();
      List<SourceNode> seriesScanNodes = entry.getValue();
      if (seriesScanNodes.size() == 1 && (!context.isForceAddParent() || isTimeJoin)) {
        root.addChild(seriesScanNodes.get(0));
        continue;
      }
      // If size of RegionGroup = 1, we should not create new MultiChildNode as the parent.
      // If size of RegionGroup > 1, we need to consider the value of `forceAddParent`.
      // If `forceAddParent` is true, we should not create new MultiChildNode as the parent, either.
      // At last, we can use the parameter `addParent` to judge whether to create new
      // MultiChildNode.
      boolean appendToRootDirectly =
          sourceGroup.size() == 1 || (!addParent && !context.isForceAddParent());
      if (appendToRootDirectly) {
        // In non-last query, this code can be reached at most once
        // And we set region as MainFragmentLocatedRegion, the others Region should transfer data to
        // this region
        context.queryContext.setMainFragmentLocatedRegion(region);
        seriesScanNodes.forEach(root::addChild);
        addParent = true;
      } else {
        // We clone a TimeJoinNode from root to make the params to be consistent.
        // But we need to assign a new ID to it
        MultiChildProcessNode parentOfGroup = (MultiChildProcessNode) root.clone();
        parentOfGroup.setPlanNodeId(context.queryContext.getQueryId().genPlanNodeId());
        seriesScanNodes.forEach(parentOfGroup::addChild);
        root.addChild(parentOfGroup);
      }
    }

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

  private Map<TRegionReplicaSet, List<SourceNode>> groupBySourceNodes(
      MultiChildProcessNode node, DistributionPlanContext context) {
    // Step 1: Get all source nodes. For the node which is not source, add it as the child of
    // current TimeJoinNode
    List<SourceNode> sources = new ArrayList<>();
    for (PlanNode child : node.getChildren()) {
      if (child instanceof SeriesSourceNode) {
        // If the child is SeriesScanNode, we need to check whether this node should be seperated
        // into several splits.
        SeriesSourceNode sourceNode = (SeriesSourceNode) child;
        List<TRegionReplicaSet> dataDistribution =
            analysis.getPartitionInfo(
                sourceNode.getPartitionPath(), context.getPartitionTimeFilter());
        if (dataDistribution.size() > 1) {
          // If there is some series which is distributed in multi DataRegions
          context.setOneSeriesInMultiRegion(true);
        }
        // If the size of dataDistribution is N, this SeriesScanNode should be seperated into N
        // SeriesScanNode.
        for (TRegionReplicaSet dataRegion : dataDistribution) {
          SeriesSourceNode split = (SeriesSourceNode) sourceNode.clone();
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

    return sourceGroup;
  }

  private boolean containsAggregationSource(FullOuterTimeJoinNode node) {
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
      FullOuterTimeJoinNode root, DistributionPlanContext context) {
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
    Map<String, List<Expression>> columnNameToExpression = new HashMap<>();
    for (CrossSeriesAggregationDescriptor originalDescriptor :
        newRoot.getGroupByLevelDescriptors()) {
      columnNameToExpression.putAll(originalDescriptor.getGroupedInputStringToExpressionsMap());
      // for example, max_by(root.*.x1, root.*.y1), we have "root.*.x1, root.*.y1" =>
      // [root.*.x1(TimeSeriesOperand), root.*.y1(TimeSeriesOperand)]
      columnNameToExpression.put(
          originalDescriptor.getParametersString(), originalDescriptor.getOutputExpressions());
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

  @Override
  public List<PlanNode> visitLimit(LimitNode node, DistributionPlanContext context) {
    List<PlanNode> result = new ArrayList<>();
    for (PlanNode planNode : rewrite(node.getChild(), context)) {
      LimitNode newNode =
          new LimitNode(
              context.queryContext.getQueryId().genPlanNodeId(), planNode, node.getLimit());
      result.add(newNode);
    }
    return result;
  }

  // If the Aggregation Query contains value filter, we need to use the naive query plan
  // for it. That is, do the raw data query and then do the aggregation operation.
  // Currently, the method to judge whether the query should use naive query plan is whether
  // AggregationNode is contained in the PlanNode tree of logical plan.
  private boolean shouldUseNaiveAggregation(PlanNode root) {
    if (root instanceof RawDataAggregationNode) {
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
    sourceGroup.forEach(
        (dataRegion, sourceNodes) -> {
          if (sourceNodes.size() == 1) {
            newRoot.addChild(sourceNodes.get(0));
          } else {
            if (sourceGroup.size() == 1) {
              sourceNodes.forEach(newRoot::addChild);
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
          for (String groupedInputExpressionsString :
              originalDescriptor.getInputExpressionsAsStringList()) {
            if (isAggColumnMatchExpression(childColumn, groupedInputExpressionsString)) {
              keep = true;
              break;
            }
          }
        }
        if (keep) {
          descriptorList.add(originalDescriptor);
          updateTypeProviderByPartialAggregation(
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
      Map<String, List<Expression>> columnNameToExpression = context.getColumnNameToExpression();
      Map<String, List<Expression>> childrenExpressionMap = new HashMap<>();
      for (String childColumn : childrenOutputColumns) {
        String childInput =
            childColumn.substring(childColumn.indexOf("(") + 1, childColumn.lastIndexOf(")"));
        childrenExpressionMap.put(childInput, columnNameToExpression.get(childInput));
      }

      for (CrossSeriesAggregationDescriptor originalDescriptor :
          handle.getGroupByLevelDescriptors()) {
        Set<Expression> descriptorExpressions = new HashSet<>();

        if (childrenExpressionMap.containsKey(originalDescriptor.getParametersString())) {
          descriptorExpressions.addAll(originalDescriptor.getOutputExpressions());
        }

        if (analysis.useLogicalView()) {
          for (List<Expression> groupedInputExpressions :
              originalDescriptor.getGroupedInputExpressions()) {
            String groupedInputExpressionsString =
                originalDescriptor.getInputString(groupedInputExpressions);
            List<Expression> inputExpressions =
                childrenExpressionMap.get(groupedInputExpressionsString);
            if (inputExpressions != null && !inputExpressions.isEmpty()) {
              descriptorExpressions.addAll(groupedInputExpressions);
            }
          }
        } else {
          for (String groupedInputExpressionString :
              originalDescriptor.getGroupedInputExpressionStrings()) {
            List<Expression> inputExpressions =
                childrenExpressionMap.get(groupedInputExpressionString);
            if (inputExpressions != null && !inputExpressions.isEmpty()) {
              descriptorExpressions.addAll(inputExpressions);
            }
          }
        }

        if (descriptorExpressions.isEmpty()) {
          continue;
        }
        CrossSeriesAggregationDescriptor descriptor = originalDescriptor.deepClone();
        descriptor.setStep(level == 0 ? AggregationStep.FINAL : AggregationStep.INTERMEDIATE);
        descriptor.setInputExpressions(new ArrayList<>(descriptorExpressions));
        descriptorList.add(descriptor);
        updateTypeProviderByPartialAggregation(descriptor, context.queryContext.getTypeProvider());
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
                          v ->
                              childDescriptors.add(
                                  new AggregationDescriptor(
                                      v.getAggregationFuncName(),
                                      AggregationStep.INTERMEDIATE,
                                      v.getInputExpressions(),
                                      v.getInputAttributes()))));
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
    sourceGroup.forEach(
        (dataRegion, sourceNodes) -> {
          if (sourceNodes.size() == 1) {
            newRoot.addChild(sourceNodes.get(0));
          } else {
            if (sourceGroup.size() == 1) {
              sourceNodes.forEach(newRoot::addChild);
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
  private boolean isAggColumnMatchExpression(String columnName, String expression) {
    if (columnName == null) {
      return false;
    }
    return columnName.contains(expression);
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
                updateTypeProviderByPartialAggregation(d, context.queryContext.getTypeProvider());
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
              descriptor -> {
                if (isSingle) {
                  descriptor.setStep(AggregationStep.SINGLE);
                } else {
                  eachSeriesOneRegion[0] = false;
                  descriptor.setStep(AggregationStep.PARTIAL);
                  updateTypeProviderByPartialAggregation(
                      descriptor, context.queryContext.getTypeProvider());
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
        analysis.getPartitionInfo(child.getPartitionPath(), context.getPartitionTimeFilter());
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

  private static class DeviceViewSplit {
    protected IDeviceID device;
    protected PlanNode root;
    protected Set<TRegionReplicaSet> dataPartitions;

    protected DeviceViewSplit(
        IDeviceID device, PlanNode root, List<TRegionReplicaSet> dataPartitions) {
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
}
