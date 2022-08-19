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

package org.apache.iotdb.db.mpp.plan.planner;

import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsConvertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.NodePathsSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.PathsUsingTemplateScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryOrderByHeatNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByLevelDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.apache.commons.lang.Validate;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

public class LogicalPlanBuilder {

  private PlanNode root;

  private final MPPQueryContext context;

  public LogicalPlanBuilder(MPPQueryContext context) {
    this.context = context;
  }

  public PlanNode getRoot() {
    return root;
  }

  public LogicalPlanBuilder withNewRoot(PlanNode newRoot) {
    this.root = newRoot;
    return this;
  }

  public LogicalPlanBuilder planRawDataSource(
      Set<Expression> sourceExpressions, Ordering scanOrder, Filter timeFilter) {
    List<PlanNode> sourceNodeList = new ArrayList<>();
    List<PartialPath> selectedPaths =
        sourceExpressions.stream()
            .map(expression -> ((TimeSeriesOperand) expression).getPath())
            .collect(Collectors.toList());
    List<PartialPath> groupedPaths = MetaUtils.groupAlignedSeries(selectedPaths);
    for (PartialPath path : groupedPaths) {
      if (path instanceof MeasurementPath) { // non-aligned series
        SeriesScanNode seriesScanNode =
            new SeriesScanNode(
                context.getQueryId().genPlanNodeId(), (MeasurementPath) path, scanOrder);
        seriesScanNode.setTimeFilter(timeFilter);
        sourceNodeList.add(seriesScanNode);
      } else if (path instanceof AlignedPath) { // aligned series
        AlignedSeriesScanNode alignedSeriesScanNode =
            new AlignedSeriesScanNode(
                context.getQueryId().genPlanNodeId(), (AlignedPath) path, scanOrder);
        alignedSeriesScanNode.setTimeFilter(timeFilter);
        sourceNodeList.add(alignedSeriesScanNode);
      } else {
        throw new IllegalArgumentException("unexpected path type");
      }
    }

    this.root = convergeWithTimeJoin(sourceNodeList, scanOrder);
    return this;
  }

  public LogicalPlanBuilder planLast(
      Set<Expression> sourceExpressions,
      Filter globalTimeFilter,
      OrderByParameter mergeOrderParameter) {
    List<PlanNode> sourceNodeList = new ArrayList<>();
    for (Expression sourceExpression : sourceExpressions) {
      MeasurementPath selectPath =
          (MeasurementPath) ((TimeSeriesOperand) sourceExpression).getPath();
      if (selectPath.isUnderAlignedEntity()) {
        sourceNodeList.add(
            new AlignedLastQueryScanNode(
                context.getQueryId().genPlanNodeId(), new AlignedPath(selectPath)));
      } else {
        sourceNodeList.add(new LastQueryScanNode(context.getQueryId().genPlanNodeId(), selectPath));
      }
    }

    this.root =
        new LastQueryNode(
            context.getQueryId().genPlanNodeId(),
            sourceNodeList,
            globalTimeFilter,
            mergeOrderParameter);
    return this;
  }

  public LogicalPlanBuilder planAggregationSource(
      Set<Expression> sourceExpressions,
      AggregationStep curStep,
      Ordering scanOrder,
      Filter timeFilter,
      GroupByTimeParameter groupByTimeParameter,
      Set<Expression> aggregationExpressions,
      Map<Expression, Set<Expression>> groupByLevelExpressions,
      TypeProvider typeProvider) {

    boolean needCheckAscending = groupByTimeParameter == null;
    Map<PartialPath, List<AggregationDescriptor>> ascendingAggregations = new HashMap<>();
    Map<PartialPath, List<AggregationDescriptor>> descendingAggregations = new HashMap<>();
    for (Expression sourceExpression : sourceExpressions) {
      createAggregationDescriptor(
          (FunctionExpression) sourceExpression,
          curStep,
          scanOrder,
          needCheckAscending,
          typeProvider,
          ascendingAggregations,
          descendingAggregations);
    }

    List<PlanNode> sourceNodeList =
        constructSourceNodeFromAggregationDescriptors(
            ascendingAggregations,
            descendingAggregations,
            scanOrder,
            timeFilter,
            groupByTimeParameter);

    return convergeAggregationSource(
        sourceNodeList,
        curStep,
        scanOrder,
        groupByTimeParameter,
        aggregationExpressions,
        groupByLevelExpressions);
  }

  public LogicalPlanBuilder planAggregationSourceWithIndexAdjust(
      Set<Expression> sourceExpressions,
      AggregationStep curStep,
      Ordering scanOrder,
      Filter timeFilter,
      GroupByTimeParameter groupByTimeParameter,
      Set<Expression> aggregationExpressions,
      List<Integer> measurementIndexes,
      Map<Expression, Set<Expression>> groupByLevelExpressions,
      TypeProvider typeProvider) {
    checkArgument(
        sourceExpressions.size() == measurementIndexes.size(),
        "Each aggregate should correspond to a column of output.");

    boolean needCheckAscending = groupByTimeParameter == null;
    Map<PartialPath, List<AggregationDescriptor>> ascendingAggregations = new HashMap<>();
    Map<PartialPath, List<AggregationDescriptor>> descendingAggregations = new HashMap<>();
    Map<AggregationDescriptor, Integer> aggregationToMeasurementIndexMap = new HashMap<>();

    int index = 0;
    for (Expression sourceExpression : sourceExpressions) {
      AggregationDescriptor aggregationDescriptor =
          createAggregationDescriptor(
              (FunctionExpression) sourceExpression,
              curStep,
              scanOrder,
              needCheckAscending,
              typeProvider,
              ascendingAggregations,
              descendingAggregations);
      aggregationToMeasurementIndexMap.put(aggregationDescriptor, measurementIndexes.get(index));
      index++;
    }

    List<PlanNode> sourceNodeList =
        constructSourceNodeFromAggregationDescriptors(
            ascendingAggregations,
            descendingAggregations,
            scanOrder,
            timeFilter,
            groupByTimeParameter);

    if (!curStep.isOutputPartial()) {
      // update measurementIndexes
      measurementIndexes.clear();
      measurementIndexes.addAll(
          sourceNodeList.stream()
              .map(
                  planNode ->
                      ((SeriesAggregationSourceNode) planNode).getAggregationDescriptorList())
              .flatMap(List::stream)
              .map(aggregationToMeasurementIndexMap::get)
              .collect(Collectors.toList()));
    }

    return convergeAggregationSource(
        sourceNodeList,
        curStep,
        scanOrder,
        groupByTimeParameter,
        aggregationExpressions,
        groupByLevelExpressions);
  }

  private AggregationDescriptor createAggregationDescriptor(
      FunctionExpression sourceExpression,
      AggregationStep curStep,
      Ordering scanOrder,
      boolean needCheckAscending,
      TypeProvider typeProvider,
      Map<PartialPath, List<AggregationDescriptor>> ascendingAggregations,
      Map<PartialPath, List<AggregationDescriptor>> descendingAggregations) {
    AggregationDescriptor aggregationDescriptor =
        new AggregationDescriptor(
            sourceExpression.getFunctionName(), curStep, sourceExpression.getExpressions());
    if (curStep.isOutputPartial()) {
      updateTypeProviderByPartialAggregation(aggregationDescriptor, typeProvider);
    }
    PartialPath selectPath =
        ((TimeSeriesOperand) sourceExpression.getExpressions().get(0)).getPath();
    if (!needCheckAscending
        || SchemaUtils.isConsistentWithScanOrder(
            aggregationDescriptor.getAggregationType(), scanOrder)) {
      ascendingAggregations
          .computeIfAbsent(selectPath, key -> new ArrayList<>())
          .add(aggregationDescriptor);
    } else {
      descendingAggregations
          .computeIfAbsent(selectPath, key -> new ArrayList<>())
          .add(aggregationDescriptor);
    }
    return aggregationDescriptor;
  }

  private List<PlanNode> constructSourceNodeFromAggregationDescriptors(
      Map<PartialPath, List<AggregationDescriptor>> ascendingAggregations,
      Map<PartialPath, List<AggregationDescriptor>> descendingAggregations,
      Ordering scanOrder,
      Filter timeFilter,
      GroupByTimeParameter groupByTimeParameter) {
    List<PlanNode> sourceNodeList = new ArrayList<>();
    boolean needCheckAscending = groupByTimeParameter == null;
    Map<PartialPath, List<AggregationDescriptor>> groupedAscendingAggregations =
        MetaUtils.groupAlignedAggregations(ascendingAggregations);
    for (Map.Entry<PartialPath, List<AggregationDescriptor>> pathAggregationsEntry :
        groupedAscendingAggregations.entrySet()) {
      sourceNodeList.add(
          createAggregationScanNode(
              pathAggregationsEntry.getKey(),
              pathAggregationsEntry.getValue(),
              scanOrder,
              groupByTimeParameter,
              timeFilter));
    }

    if (needCheckAscending) {
      Map<PartialPath, List<AggregationDescriptor>> groupedDescendingAggregations =
          MetaUtils.groupAlignedAggregations(descendingAggregations);
      for (Map.Entry<PartialPath, List<AggregationDescriptor>> pathAggregationsEntry :
          groupedDescendingAggregations.entrySet()) {
        sourceNodeList.add(
            createAggregationScanNode(
                pathAggregationsEntry.getKey(),
                pathAggregationsEntry.getValue(),
                scanOrder.reverse(),
                null,
                timeFilter));
      }
    }
    return sourceNodeList;
  }

  private LogicalPlanBuilder convergeAggregationSource(
      List<PlanNode> sourceNodeList,
      AggregationStep curStep,
      Ordering scanOrder,
      GroupByTimeParameter groupByTimeParameter,
      Set<Expression> aggregationExpressions,
      Map<Expression, Set<Expression>> groupByLevelExpressions) {
    if (curStep.isOutputPartial()) {
      if (groupByTimeParameter != null && groupByTimeParameter.hasOverlap()) {
        curStep =
            groupByLevelExpressions != null ? AggregationStep.INTERMEDIATE : AggregationStep.FINAL;

        this.root = convergeWithTimeJoin(sourceNodeList, scanOrder);

        this.root =
            createSlidingWindowAggregationNode(
                this.getRoot(), aggregationExpressions, groupByTimeParameter, curStep, scanOrder);

        if (groupByLevelExpressions != null) {
          curStep = AggregationStep.FINAL;
          this.root =
              createGroupByTLevelNode(
                  Collections.singletonList(this.getRoot()),
                  groupByLevelExpressions,
                  curStep,
                  groupByTimeParameter,
                  scanOrder);
        }
      } else {
        if (groupByLevelExpressions != null) {
          curStep = AggregationStep.FINAL;
          this.root =
              createGroupByTLevelNode(
                  sourceNodeList,
                  groupByLevelExpressions,
                  curStep,
                  groupByTimeParameter,
                  scanOrder);
        }
      }
    } else {
      this.root = convergeWithTimeJoin(sourceNodeList, scanOrder);
    }

    return this;
  }

  public static void updateTypeProviderByPartialAggregation(
      AggregationDescriptor aggregationDescriptor, TypeProvider typeProvider) {
    List<AggregationType> splitAggregations =
        SchemaUtils.splitPartialAggregation(aggregationDescriptor.getAggregationType());
    PartialPath path =
        ((TimeSeriesOperand) aggregationDescriptor.getInputExpressions().get(0)).getPath();
    for (AggregationType aggregationType : splitAggregations) {
      String functionName = aggregationType.toString().toLowerCase();
      typeProvider.setType(
          String.format("%s(%s)", functionName, path.getFullPath()),
          SchemaUtils.getSeriesTypeByPath(path, functionName));
    }
  }

  private PlanNode convergeWithTimeJoin(List<PlanNode> sourceNodes, Ordering mergeOrder) {
    PlanNode tmpNode;
    if (sourceNodes.size() == 1) {
      tmpNode = sourceNodes.get(0);
    } else {
      tmpNode = new TimeJoinNode(context.getQueryId().genPlanNodeId(), mergeOrder, sourceNodes);
    }
    return tmpNode;
  }

  public LogicalPlanBuilder planDeviceView(
      Map<String, PlanNode> deviceNameToSourceNodesMap,
      List<String> outputColumnNames,
      Map<String, List<Integer>> deviceToMeasurementIndexesMap,
      Ordering mergeOrder) {
    DeviceViewNode deviceViewNode =
        new DeviceViewNode(
            context.getQueryId().genPlanNodeId(),
            new OrderByParameter(
                Arrays.asList(
                    new SortItem(SortKey.DEVICE, Ordering.ASC),
                    new SortItem(SortKey.TIME, mergeOrder))),
            outputColumnNames,
            deviceToMeasurementIndexesMap);
    for (Map.Entry<String, PlanNode> entry : deviceNameToSourceNodesMap.entrySet()) {
      String deviceName = entry.getKey();
      PlanNode subPlan = entry.getValue();
      deviceViewNode.addChildDeviceNode(deviceName, subPlan);
    }

    this.root = deviceViewNode;
    return this;
  }

  public LogicalPlanBuilder planGroupByLevel(
      Map<Expression, Set<Expression>> groupByLevelExpressions,
      AggregationStep curStep,
      GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder) {
    if (groupByLevelExpressions == null) {
      return this;
    }

    this.root =
        createGroupByTLevelNode(
            Collections.singletonList(this.getRoot()),
            groupByLevelExpressions,
            curStep,
            groupByTimeParameter,
            scanOrder);
    return this;
  }

  public LogicalPlanBuilder planAggregation(
      Set<Expression> aggregationExpressions,
      GroupByTimeParameter groupByTimeParameter,
      AggregationStep curStep,
      TypeProvider typeProvider,
      Ordering scanOrder) {
    if (aggregationExpressions == null) {
      return this;
    }

    List<AggregationDescriptor> aggregationDescriptorList =
        constructAggregationDescriptorList(aggregationExpressions, curStep);
    if (curStep.isOutputPartial()) {
      aggregationDescriptorList.forEach(
          aggregationDescriptor ->
              updateTypeProviderByPartialAggregation(aggregationDescriptor, typeProvider));
    }
    this.root =
        new AggregationNode(
            context.getQueryId().genPlanNodeId(),
            Collections.singletonList(this.getRoot()),
            aggregationDescriptorList,
            groupByTimeParameter,
            scanOrder);
    return this;
  }

  public LogicalPlanBuilder planSlidingWindowAggregation(
      Set<Expression> aggregationExpressions,
      GroupByTimeParameter groupByTimeParameter,
      AggregationStep curStep,
      Ordering scanOrder) {
    if (aggregationExpressions == null) {
      return this;
    }

    this.root =
        createSlidingWindowAggregationNode(
            this.getRoot(), aggregationExpressions, groupByTimeParameter, curStep, scanOrder);
    return this;
  }

  private PlanNode createSlidingWindowAggregationNode(
      PlanNode child,
      Set<Expression> aggregationExpressions,
      GroupByTimeParameter groupByTimeParameter,
      AggregationStep curStep,
      Ordering scanOrder) {
    List<AggregationDescriptor> aggregationDescriptorList =
        constructAggregationDescriptorList(aggregationExpressions, curStep);
    return new SlidingWindowAggregationNode(
        context.getQueryId().genPlanNodeId(),
        child,
        aggregationDescriptorList,
        groupByTimeParameter,
        scanOrder);
  }

  private PlanNode createGroupByTLevelNode(
      List<PlanNode> children,
      Map<Expression, Set<Expression>> groupByLevelExpressions,
      AggregationStep curStep,
      GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder) {
    List<GroupByLevelDescriptor> groupByLevelDescriptors = new ArrayList<>();
    for (Expression groupedExpression : groupByLevelExpressions.keySet()) {
      groupByLevelDescriptors.add(
          new GroupByLevelDescriptor(
              ((FunctionExpression) groupedExpression).getFunctionName(),
              curStep,
              groupByLevelExpressions.get(groupedExpression).stream()
                  .map(Expression::getExpressions)
                  .flatMap(List::stream)
                  .collect(Collectors.toList()),
              groupedExpression.getExpressions().get(0)));
    }
    return new GroupByLevelNode(
        context.getQueryId().genPlanNodeId(),
        children,
        groupByLevelDescriptors,
        groupByTimeParameter,
        scanOrder);
  }

  private SeriesAggregationSourceNode createAggregationScanNode(
      PartialPath selectPath,
      List<AggregationDescriptor> aggregationDescriptorList,
      Ordering scanOrder,
      GroupByTimeParameter groupByTimeParameter,
      Filter timeFilter) {
    if (selectPath instanceof MeasurementPath) { // non-aligned series
      SeriesAggregationScanNode seriesAggregationScanNode =
          new SeriesAggregationScanNode(
              context.getQueryId().genPlanNodeId(),
              (MeasurementPath) selectPath,
              aggregationDescriptorList,
              scanOrder,
              groupByTimeParameter);
      seriesAggregationScanNode.setTimeFilter(timeFilter);
      return seriesAggregationScanNode;
    } else if (selectPath instanceof AlignedPath) { // aligned series
      AlignedSeriesAggregationScanNode alignedSeriesAggregationScanNode =
          new AlignedSeriesAggregationScanNode(
              context.getQueryId().genPlanNodeId(),
              (AlignedPath) selectPath,
              aggregationDescriptorList,
              scanOrder,
              groupByTimeParameter);
      alignedSeriesAggregationScanNode.setTimeFilter(timeFilter);
      return alignedSeriesAggregationScanNode;
    } else {
      throw new IllegalArgumentException("unexpected path type");
    }
  }

  private List<AggregationDescriptor> constructAggregationDescriptorList(
      Set<Expression> aggregationExpressions, AggregationStep curStep) {
    return aggregationExpressions.stream()
        .map(
            expression -> {
              Validate.isTrue(expression instanceof FunctionExpression);
              return new AggregationDescriptor(
                  ((FunctionExpression) expression).getFunctionName(),
                  curStep,
                  expression.getExpressions());
            })
        .collect(Collectors.toList());
  }

  public LogicalPlanBuilder planFilterAndTransform(
      Expression queryFilter,
      Set<Expression> selectExpressions,
      boolean isGroupByTime,
      ZoneId zoneId,
      Ordering scanOrder) {
    if (queryFilter == null) {
      return this;
    }

    this.root =
        new FilterNode(
            context.getQueryId().genPlanNodeId(),
            this.getRoot(),
            selectExpressions.toArray(new Expression[0]),
            queryFilter,
            isGroupByTime,
            zoneId,
            scanOrder);
    return this;
  }

  public LogicalPlanBuilder planTransform(
      Set<Expression> transformExpressions,
      boolean isGroupByTime,
      ZoneId zoneId,
      Ordering scanOrder) {
    boolean needTransform = false;
    for (Expression expression : transformExpressions) {
      if (ExpressionAnalyzer.checkIsNeedTransform(expression)) {
        needTransform = true;
        break;
      }
    }
    if (!needTransform) {
      return this;
    }

    this.root =
        new TransformNode(
            context.getQueryId().genPlanNodeId(),
            this.getRoot(),
            transformExpressions.toArray(new Expression[0]),
            isGroupByTime,
            zoneId,
            scanOrder);
    return this;
  }

  public LogicalPlanBuilder planFill(FillDescriptor fillDescriptor, Ordering scanOrder) {
    if (fillDescriptor == null) {
      return this;
    }

    this.root =
        new FillNode(
            context.getQueryId().genPlanNodeId(), this.getRoot(), fillDescriptor, scanOrder);
    return this;
  }

  public LogicalPlanBuilder planLimit(int rowLimit) {
    if (rowLimit == 0) {
      return this;
    }

    this.root = new LimitNode(context.getQueryId().genPlanNodeId(), this.getRoot(), rowLimit);
    return this;
  }

  public LogicalPlanBuilder planOffset(int rowOffset) {
    if (rowOffset == 0) {
      return this;
    }

    this.root = new OffsetNode(context.getQueryId().genPlanNodeId(), this.getRoot(), rowOffset);
    return this;
  }

  /** Meta Query* */
  public LogicalPlanBuilder planTimeSeriesSchemaSource(
      PartialPath pathPattern,
      String key,
      String value,
      int limit,
      int offset,
      boolean orderByHeat,
      boolean contains,
      boolean prefixPath,
      Map<Integer, Template> templateMap) {
    this.root =
        new TimeSeriesSchemaScanNode(
            context.getQueryId().genPlanNodeId(),
            pathPattern,
            key,
            value,
            limit,
            offset,
            orderByHeat,
            contains,
            prefixPath,
            templateMap);
    return this;
  }

  public LogicalPlanBuilder planDeviceSchemaSource(
      PartialPath pathPattern, int limit, int offset, boolean prefixPath, boolean hasSgCol) {
    this.root =
        new DevicesSchemaScanNode(
            context.getQueryId().genPlanNodeId(), pathPattern, limit, offset, prefixPath, hasSgCol);
    return this;
  }

  public LogicalPlanBuilder planSchemaQueryMerge(boolean orderByHeat) {
    SchemaQueryMergeNode schemaMergeNode =
        new SchemaQueryMergeNode(context.getQueryId().genPlanNodeId(), orderByHeat);
    schemaMergeNode.addChild(this.getRoot());
    this.root = schemaMergeNode;
    return this;
  }

  public LogicalPlanBuilder planSchemaQueryOrderByHeat(PlanNode lastPlanNode) {
    SchemaQueryOrderByHeatNode node =
        new SchemaQueryOrderByHeatNode(context.getQueryId().genPlanNodeId());
    node.addChild(this.getRoot());
    node.addChild(lastPlanNode);
    this.root = node;
    return this;
  }

  public LogicalPlanBuilder planSchemaFetchMerge(List<String> storageGroupList) {
    this.root = new SchemaFetchMergeNode(context.getQueryId().genPlanNodeId(), storageGroupList);
    return this;
  }

  public LogicalPlanBuilder planSchemaFetchSource(
      List<String> storageGroupList,
      PathPatternTree patternTree,
      Map<Integer, Template> templateMap) {
    PartialPath storageGroupPath;
    for (String storageGroup : storageGroupList) {
      try {
        storageGroupPath = new PartialPath(storageGroup);
        PathPatternTree overlappedPatternTree = new PathPatternTree();
        for (PartialPath pathPattern :
            patternTree.getOverlappedPathPatterns(
                storageGroupPath.concatNode(MULTI_LEVEL_PATH_WILDCARD))) {
          overlappedPatternTree.appendPathPattern(pathPattern);
        }
        this.root.addChild(
            new SchemaFetchScanNode(
                context.getQueryId().genPlanNodeId(),
                storageGroupPath,
                overlappedPatternTree,
                templateMap));
      } catch (IllegalPathException e) {
        // definitely won't happen
        throw new RuntimeException(e);
      }
    }
    return this;
  }

  public LogicalPlanBuilder planCountMerge() {
    CountSchemaMergeNode countMergeNode =
        new CountSchemaMergeNode(context.getQueryId().genPlanNodeId());
    countMergeNode.addChild(this.getRoot());
    this.root = countMergeNode;
    return this;
  }

  public LogicalPlanBuilder planDevicesCountSource(PartialPath partialPath, boolean prefixPath) {
    this.root = new DevicesCountNode(context.getQueryId().genPlanNodeId(), partialPath, prefixPath);
    return this;
  }

  public LogicalPlanBuilder planTimeSeriesCountSource(
      PartialPath partialPath, boolean prefixPath, String key, String value, boolean isContains) {
    this.root =
        new TimeSeriesCountNode(
            context.getQueryId().genPlanNodeId(), partialPath, prefixPath, key, value, isContains);
    return this;
  }

  public LogicalPlanBuilder planLevelTimeSeriesCountSource(
      PartialPath partialPath,
      boolean prefixPath,
      int level,
      String key,
      String value,
      boolean isContains) {
    this.root =
        new LevelTimeSeriesCountNode(
            context.getQueryId().genPlanNodeId(),
            partialPath,
            prefixPath,
            level,
            key,
            value,
            isContains);
    return this;
  }

  public LogicalPlanBuilder planNodePathsSchemaSource(PartialPath partialPath, Integer level) {
    this.root =
        new NodePathsSchemaScanNode(context.getQueryId().genPlanNodeId(), partialPath, level);
    return this;
  }

  public LogicalPlanBuilder planNodePathsConvert() {
    NodePathsConvertNode nodePathsConvertNode =
        new NodePathsConvertNode(context.getQueryId().genPlanNodeId());
    nodePathsConvertNode.addChild(this.getRoot());
    this.root = nodePathsConvertNode;
    return this;
  }

  public LogicalPlanBuilder planNodePathsCount() {
    NodePathsCountNode nodePathsCountNode =
        new NodePathsCountNode(context.getQueryId().genPlanNodeId());
    nodePathsCountNode.addChild(this.getRoot());
    this.root = nodePathsCountNode;
    return this;
  }

  public LogicalPlanBuilder planNodeManagementMemoryMerge(Set<TSchemaNode> data) {
    NodeManagementMemoryMergeNode memorySourceNode =
        new NodeManagementMemoryMergeNode(context.getQueryId().genPlanNodeId(), data);
    memorySourceNode.addChild(this.getRoot());
    this.root = memorySourceNode;
    return this;
  }

  public LogicalPlanBuilder planPathsUsingTemplateSource(int templateId) {
    this.root = new PathsUsingTemplateScanNode(context.getQueryId().genPlanNodeId(), templateId);
    return this;
  }
}
