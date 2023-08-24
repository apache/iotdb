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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.ExpressionAnalyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.DevicesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.DevicesSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.LevelTimeSeriesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.LogicalViewSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.NodeManagementMemoryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.NodePathsConvertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.NodePathsCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.NodePathsSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.PathsUsingTemplateScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaFetchMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaFetchScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaQueryMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.SchemaQueryOrderByHeatNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewIntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.GroupByTagNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.IntoNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleDeviceViewNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesAggregationSourceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.ShowQueriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.AggregationStep;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.DeviceViewIntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.IntoPathDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.OrderByParameter;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowQueriesStatement;
import org.apache.iotdb.db.schemaengine.schemaregion.utils.MetaUtils;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import com.google.common.base.Function;
import org.apache.commons.lang3.Validate;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.DEVICE;
import static org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant.ENDTIME;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT_TIME;

public class LogicalPlanBuilder {

  private PlanNode root;

  private final MPPQueryContext context;

  private final Function<Expression, TSDataType> getPreAnalyzedType;

  public LogicalPlanBuilder(Analysis analysis, MPPQueryContext context) {
    this.getPreAnalyzedType = analysis::getType;
    this.context = context;
  }

  public PlanNode getRoot() {
    return root;
  }

  public LogicalPlanBuilder withNewRoot(PlanNode newRoot) {
    this.root = newRoot;
    return this;
  }

  private void updateTypeProvider(Collection<Expression> expressions) {
    if (expressions == null) {
      return;
    }
    expressions.forEach(
        expression -> {
          if (!expression.getExpressionString().equals(DEVICE)
              && !expression.getExpressionString().equals(ENDTIME)) {
            context
                .getTypeProvider()
                .setType(expression.getExpressionString(), getPreAnalyzedType.apply(expression));
          }
        });
  }

  private void updateTypeProviderWithConstantType(List<String> keys, TSDataType dataType) {
    if (keys == null) {
      return;
    }
    keys.forEach(k -> context.getTypeProvider().setType(k, dataType));
  }

  private void updateTypeProviderWithConstantType(String columnName, TSDataType dataType) {
    context.getTypeProvider().setType(columnName, dataType);
  }

  public LogicalPlanBuilder planRawDataSource(
      Set<Expression> sourceExpressions,
      Ordering scanOrder,
      Filter timeFilter,
      boolean lastLevelUseWildcard) {
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
        // TODO: push down value filter
        seriesScanNode.setValueFilter(timeFilter);
        sourceNodeList.add(seriesScanNode);
      } else if (path instanceof AlignedPath) { // aligned series
        AlignedSeriesScanNode alignedSeriesScanNode =
            new AlignedSeriesScanNode(
                context.getQueryId().genPlanNodeId(),
                (AlignedPath) path,
                scanOrder,
                lastLevelUseWildcard);
        alignedSeriesScanNode.setTimeFilter(timeFilter);
        // TODO: push down value filter
        alignedSeriesScanNode.setValueFilter(timeFilter);
        sourceNodeList.add(alignedSeriesScanNode);
      } else {
        throw new IllegalArgumentException("unexpected path type");
      }
    }

    updateTypeProvider(sourceExpressions);

    this.root = convergeWithTimeJoin(sourceNodeList, scanOrder);
    return this;
  }

  public LogicalPlanBuilder planLast(
      Set<Expression> sourceExpressions, Filter globalTimeFilter, Ordering timeseriesOrdering) {
    List<PlanNode> sourceNodeList = new ArrayList<>();

    Map<String, Boolean> deviceAlignedMap = new HashMap<>();
    Map<String, Boolean> deviceExistViewMap = new HashMap<>();
    Map<String, Map<String, Expression>> outputPathToSourceExpressionMap =
        timeseriesOrdering != null
            ? new TreeMap<>(timeseriesOrdering.getStringComparator())
            : new LinkedHashMap<>();
    for (Expression sourceExpression : sourceExpressions) {
      MeasurementPath outputPath =
          (MeasurementPath)
              (sourceExpression.isViewExpression()
                  ? sourceExpression.getViewPath()
                  : ((TimeSeriesOperand) sourceExpression).getPath());
      String outputDevice = outputPath.getDevice();
      outputPathToSourceExpressionMap
          .computeIfAbsent(
              outputDevice,
              k ->
                  timeseriesOrdering != null
                      ? new TreeMap<>(timeseriesOrdering.getStringComparator())
                      : new LinkedHashMap<>())
          .put(outputPath.getMeasurement(), sourceExpression);
      if (!deviceAlignedMap.containsKey(outputDevice)) {
        deviceAlignedMap.put(outputDevice, outputPath.isUnderAlignedEntity());
      }
      deviceExistViewMap.put(
          outputDevice,
          deviceExistViewMap.getOrDefault(outputDevice, false)
              || sourceExpression.isViewExpression());
    }

    for (Map.Entry<String, Map<String, Expression>> deviceMeasurementExpressionEntry :
        outputPathToSourceExpressionMap.entrySet()) {
      String outputDevice = deviceMeasurementExpressionEntry.getKey();
      if (deviceExistViewMap.get(outputDevice)) {
        // exist view
        for (Expression sourceExpression : deviceMeasurementExpressionEntry.getValue().values()) {
          MeasurementPath selectedPath =
              (MeasurementPath) ((TimeSeriesOperand) sourceExpression).getPath();
          if (selectedPath.isUnderAlignedEntity()) { // aligned series
            sourceNodeList.add(
                new AlignedLastQueryScanNode(
                    context.getQueryId().genPlanNodeId(),
                    new AlignedPath(selectedPath),
                    sourceExpression.isViewExpression()
                        ? sourceExpression.getViewPath().getFullPath()
                        : null));
          } else { // non-aligned series
            sourceNodeList.add(
                new LastQueryScanNode(
                    context.getQueryId().genPlanNodeId(),
                    selectedPath,
                    sourceExpression.isViewExpression()
                        ? sourceExpression.getViewPath().getFullPath()
                        : null));
          }
        }
      } else {
        if (deviceAlignedMap.get(outputDevice)) {
          // aligned series
          List<MeasurementPath> measurementPaths =
              deviceMeasurementExpressionEntry.getValue().values().stream()
                  .map(expression -> (MeasurementPath) ((TimeSeriesOperand) expression).getPath())
                  .collect(Collectors.toList());
          AlignedPath alignedPath = new AlignedPath(measurementPaths.get(0).getDevicePath());
          for (MeasurementPath measurementPath : measurementPaths) {
            alignedPath.addMeasurement(measurementPath);
          }
          sourceNodeList.add(
              new AlignedLastQueryScanNode(
                  context.getQueryId().genPlanNodeId(), alignedPath, null));
        } else {
          // non-aligned series
          for (Expression sourceExpression : deviceMeasurementExpressionEntry.getValue().values()) {
            MeasurementPath selectedPath =
                (MeasurementPath) ((TimeSeriesOperand) sourceExpression).getPath();
            sourceNodeList.add(
                new LastQueryScanNode(context.getQueryId().genPlanNodeId(), selectedPath, null));
          }
        }
      }
    }

    this.root =
        new LastQueryNode(
            context.getQueryId().genPlanNodeId(),
            sourceNodeList,
            globalTimeFilter,
            timeseriesOrdering);
    ColumnHeaderConstant.lastQueryColumnHeaders.forEach(
        columnHeader ->
            context
                .getTypeProvider()
                .setType(columnHeader.getColumnName(), columnHeader.getColumnType()));

    return this;
  }

  public LogicalPlanBuilder planAggregationSource(
      AggregationStep curStep,
      Ordering scanOrder,
      Filter timeFilter,
      GroupByTimeParameter groupByTimeParameter,
      Set<Expression> aggregationExpressions,
      Set<Expression> sourceTransformExpressions,
      LinkedHashMap<Expression, Set<Expression>> crossGroupByAggregations,
      List<String> tagKeys,
      Map<List<String>, LinkedHashMap<Expression, List<Expression>>>
          tagValuesToGroupedTimeseriesOperands) {
    boolean needCheckAscending = groupByTimeParameter == null;
    Map<PartialPath, List<AggregationDescriptor>> ascendingAggregations = new HashMap<>();
    Map<PartialPath, List<AggregationDescriptor>> descendingAggregations = new HashMap<>();
    Map<PartialPath, List<AggregationDescriptor>> countTimeAggregations = new HashMap<>();
    for (Expression aggregationExpression : aggregationExpressions) {
      createAggregationDescriptor(
          (FunctionExpression) aggregationExpression,
          curStep,
          scanOrder,
          needCheckAscending,
          ascendingAggregations,
          descendingAggregations,
          countTimeAggregations);
    }

    List<PlanNode> sourceNodeList =
        constructSourceNodeFromAggregationDescriptors(
            ascendingAggregations,
            descendingAggregations,
            countTimeAggregations,
            scanOrder,
            timeFilter,
            groupByTimeParameter);
    updateTypeProvider(aggregationExpressions);
    updateTypeProvider(sourceTransformExpressions);

    return convergeAggregationSource(
        sourceNodeList,
        curStep,
        scanOrder,
        groupByTimeParameter,
        aggregationExpressions,
        crossGroupByAggregations,
        tagKeys,
        tagValuesToGroupedTimeseriesOperands);
  }

  public LogicalPlanBuilder planAggregationSourceWithIndexAdjust(
      AggregationStep curStep,
      Ordering scanOrder,
      Filter timeFilter,
      GroupByTimeParameter groupByTimeParameter,
      Set<Expression> aggregationExpressions,
      Set<Expression> sourceTransformExpressions,
      LinkedHashMap<Expression, Set<Expression>> crossGroupByExpressions,
      List<Integer> deviceViewInputIndexes) {
    checkArgument(
        aggregationExpressions.size() == deviceViewInputIndexes.size(),
        "Each aggregate should correspond to a column of output.");

    boolean needCheckAscending = groupByTimeParameter == null;
    Map<PartialPath, List<AggregationDescriptor>> ascendingAggregations = new HashMap<>();
    Map<PartialPath, List<AggregationDescriptor>> descendingAggregations = new HashMap<>();
    Map<AggregationDescriptor, Integer> aggregationToIndexMap = new HashMap<>();
    Map<PartialPath, List<AggregationDescriptor>> countTimeAggregations = new HashMap<>();

    int index = 0;
    for (Expression aggregationExpression : aggregationExpressions) {
      AggregationDescriptor aggregationDescriptor =
          createAggregationDescriptor(
              (FunctionExpression) aggregationExpression,
              curStep,
              scanOrder,
              needCheckAscending,
              ascendingAggregations,
              descendingAggregations,
              countTimeAggregations);
      aggregationToIndexMap.put(aggregationDescriptor, deviceViewInputIndexes.get(index));
      index++;
    }

    List<PlanNode> sourceNodeList =
        constructSourceNodeFromAggregationDescriptors(
            ascendingAggregations,
            descendingAggregations,
            countTimeAggregations,
            scanOrder,
            timeFilter,
            groupByTimeParameter);
    updateTypeProvider(aggregationExpressions);
    updateTypeProvider(sourceTransformExpressions);

    if (!curStep.isOutputPartial()) {
      // update measurementIndexes
      deviceViewInputIndexes.clear();
      deviceViewInputIndexes.addAll(
          sourceNodeList.stream()
              .map(
                  planNode ->
                      ((SeriesAggregationSourceNode) planNode).getAggregationDescriptorList())
              .flatMap(List::stream)
              .map(aggregationToIndexMap::get)
              .collect(Collectors.toList()));
    }

    return convergeAggregationSource(
        sourceNodeList,
        curStep,
        scanOrder,
        groupByTimeParameter,
        aggregationExpressions,
        crossGroupByExpressions,
        null,
        null);
  }

  private AggregationDescriptor createAggregationDescriptor(
      FunctionExpression sourceExpression,
      AggregationStep curStep,
      Ordering scanOrder,
      boolean needCheckAscending,
      Map<PartialPath, List<AggregationDescriptor>> ascendingAggregations,
      Map<PartialPath, List<AggregationDescriptor>> descendingAggregations,
      Map<PartialPath, List<AggregationDescriptor>> countTimeAggregations) {
    AggregationDescriptor aggregationDescriptor =
        new AggregationDescriptor(
            sourceExpression.getFunctionName(),
            curStep,
            sourceExpression.getExpressions(),
            sourceExpression.getFunctionAttributes());
    if (curStep.isOutputPartial()) {
      updateTypeProviderByPartialAggregation(aggregationDescriptor, context.getTypeProvider());
    }

    if (COUNT_TIME.equalsIgnoreCase(sourceExpression.getFunctionName())) {
      Map<String, Pair<List<String>, List<IMeasurementSchema>>> map = new HashMap<>();
      for (Expression expression : sourceExpression.getCountTimeExpressions()) {
        TimeSeriesOperand ts = (TimeSeriesOperand) expression;
        PartialPath path = ts.getPath();
        Pair<List<String>, List<IMeasurementSchema>> pair =
            map.computeIfAbsent(
                path.getDevice(), k -> new Pair<>(new ArrayList<>(), new ArrayList<>()));
        pair.left.add(path.getMeasurement());
        try {
          pair.right.add(path.getMeasurementSchema());
        } catch (MetadataException ex) {
          throw new RuntimeException(ex);
        }
      }

      for (Map.Entry<String, Pair<List<String>, List<IMeasurementSchema>>> entry : map.entrySet()) {
        String device = entry.getKey();
        Pair<List<String>, List<IMeasurementSchema>> pair = entry.getValue();
        AlignedPath alignedPath = null;
        try {
          alignedPath = new AlignedPath(device, pair.left, pair.right);
        } catch (IllegalPathException e) {
          throw new RuntimeException(e);
        }
        countTimeAggregations.put(alignedPath, Collections.singletonList(aggregationDescriptor));
      }

      return aggregationDescriptor;
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
      Map<PartialPath, List<AggregationDescriptor>> countTimeAggregations,
      Ordering scanOrder,
      Filter timeFilter,
      GroupByTimeParameter groupByTimeParameter) {

    List<PlanNode> sourceNodeList = new ArrayList<>();
    boolean needCheckAscending = groupByTimeParameter == null;
    Map<PartialPath, List<AggregationDescriptor>> groupedAscendingAggregations = null;
    if (!countTimeAggregations.isEmpty()) {
      groupedAscendingAggregations = countTimeAggregations;
    } else {
      groupedAscendingAggregations = MetaUtils.groupAlignedAggregations(ascendingAggregations);
    }

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
      LinkedHashMap<Expression, Set<Expression>> crossGroupByExpressions,
      List<String> tagKeys,
      Map<List<String>, LinkedHashMap<Expression, List<Expression>>>
          tagValuesToGroupedTimeseriesOperands) {
    if (curStep.isOutputPartial()) {
      if (groupByTimeParameter != null && groupByTimeParameter.hasOverlap()) {
        curStep =
            crossGroupByExpressions != null ? AggregationStep.INTERMEDIATE : AggregationStep.FINAL;

        this.root = convergeWithTimeJoin(sourceNodeList, scanOrder);

        this.root =
            createSlidingWindowAggregationNode(
                this.getRoot(), aggregationExpressions, groupByTimeParameter, curStep, scanOrder);

        if (crossGroupByExpressions != null) {
          curStep = AggregationStep.FINAL;
          if (tagKeys != null) {
            this.root =
                createGroupByTagNode(
                    tagKeys,
                    tagValuesToGroupedTimeseriesOperands,
                    crossGroupByExpressions.keySet(),
                    Collections.singletonList(this.getRoot()),
                    curStep,
                    groupByTimeParameter,
                    scanOrder);
          } else {
            this.root =
                createGroupByTLevelNode(
                    Collections.singletonList(this.getRoot()),
                    crossGroupByExpressions,
                    curStep,
                    groupByTimeParameter,
                    scanOrder);
          }
        }
      } else {
        if (tagKeys != null) {
          curStep = AggregationStep.FINAL;
          this.root =
              createGroupByTagNode(
                  tagKeys,
                  tagValuesToGroupedTimeseriesOperands,
                  crossGroupByExpressions.keySet(),
                  sourceNodeList,
                  curStep,
                  groupByTimeParameter,
                  scanOrder);
        } else if (crossGroupByExpressions != null) {
          curStep = AggregationStep.FINAL;
          this.root =
              createGroupByTLevelNode(
                  sourceNodeList,
                  crossGroupByExpressions,
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
    List<TAggregationType> splitAggregations =
        SchemaUtils.splitPartialAggregation(aggregationDescriptor.getAggregationType());
    String inputExpressionStr =
        aggregationDescriptor.getInputExpressions().get(0).getExpressionString();
    for (TAggregationType aggregation : splitAggregations) {
      String functionName = aggregation.toString().toLowerCase();
      TSDataType aggregationType = SchemaUtils.getAggregationType(functionName);
      typeProvider.setType(
          String.format("%s(%s)", functionName, inputExpressionStr),
          aggregationType == null ? typeProvider.getType(inputExpressionStr) : aggregationType);
    }
  }

  public static void updateTypeProviderByPartialAggregation(
      CrossSeriesAggregationDescriptor aggregationDescriptor, TypeProvider typeProvider) {
    List<TAggregationType> splitAggregations =
        SchemaUtils.splitPartialAggregation(aggregationDescriptor.getAggregationType());
    PartialPath path = ((TimeSeriesOperand) aggregationDescriptor.getOutputExpression()).getPath();
    for (TAggregationType aggregationType : splitAggregations) {
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
      Set<Expression> deviceViewOutputExpressions,
      Map<String, List<Integer>> deviceToMeasurementIndexesMap,
      Set<Expression> selectExpression,
      QueryStatement queryStatement) {
    List<String> outputColumnNames =
        deviceViewOutputExpressions.stream()
            .map(Expression::getExpressionString)
            .collect(Collectors.toList());

    List<SortItem> sortItemList = queryStatement.getSortItemList();

    if (sortItemList.isEmpty()) {
      sortItemList = new ArrayList<>();
    }
    if (!queryStatement.isOrderByDevice()) {
      sortItemList.add(new SortItem(OrderByKey.DEVICE, Ordering.ASC));
    }
    if (!queryStatement.isOrderByTime()) {
      sortItemList.add(new SortItem(OrderByKey.TIME, Ordering.ASC));
    }

    OrderByParameter orderByParameter = new OrderByParameter(sortItemList);

    // order by time, device can be optimized by SingleDeviceViewNode and MergeSortNode
    if (queryStatement.isOrderByBasedOnTime() && !queryStatement.hasOrderByExpression()) {
      MergeSortNode mergeSortNode =
          new MergeSortNode(
              context.getQueryId().genPlanNodeId(), orderByParameter, outputColumnNames);
      for (Map.Entry<String, PlanNode> entry : deviceNameToSourceNodesMap.entrySet()) {
        String deviceName = entry.getKey();
        PlanNode subPlan = entry.getValue();
        SingleDeviceViewNode singleDeviceViewNode =
            new SingleDeviceViewNode(
                context.getQueryId().genPlanNodeId(),
                outputColumnNames,
                deviceName,
                deviceToMeasurementIndexesMap.get(deviceName));
        singleDeviceViewNode.addChild(subPlan);
        mergeSortNode.addChild(singleDeviceViewNode);
      }
      this.root = mergeSortNode;
    } else {
      DeviceViewNode deviceViewNode =
          new DeviceViewNode(
              context.getQueryId().genPlanNodeId(),
              orderByParameter,
              outputColumnNames,
              deviceToMeasurementIndexesMap);

      for (Map.Entry<String, PlanNode> entry : deviceNameToSourceNodesMap.entrySet()) {
        String deviceName = entry.getKey();
        PlanNode subPlan = entry.getValue();
        deviceViewNode.addChildDeviceNode(deviceName, subPlan);
      }
      this.root = deviceViewNode;
    }

    context.getTypeProvider().setType(DEVICE, TSDataType.TEXT);
    updateTypeProvider(deviceViewOutputExpressions);

    if (queryStatement.needPushDownSort()) {
      if (selectExpression.size() != deviceViewOutputExpressions.size()) {
        this.root =
            new TransformNode(
                context.getQueryId().genPlanNodeId(),
                root,
                selectExpression.toArray(new Expression[0]),
                queryStatement.isGroupByTime(),
                queryStatement.getSelectComponent().getZoneId(),
                queryStatement.getResultTimeOrder());
      }
    }

    return this;
  }

  public LogicalPlanBuilder planGroupByLevel(
      Map<Expression, Set<Expression>> groupByLevelExpressions,
      GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder) {
    if (groupByLevelExpressions == null) {
      return this;
    }

    this.root =
        createGroupByTLevelNode(
            Collections.singletonList(this.getRoot()),
            groupByLevelExpressions,
            AggregationStep.FINAL,
            groupByTimeParameter,
            scanOrder);
    return this;
  }

  public LogicalPlanBuilder planAggregation(
      Set<Expression> aggregationExpressions,
      Expression groupByExpression,
      GroupByTimeParameter groupByTimeParameter,
      GroupByParameter groupByParameter,
      boolean outputEndTime,
      AggregationStep curStep,
      Ordering scanOrder) {
    if (aggregationExpressions == null) {
      return this;
    }

    List<AggregationDescriptor> aggregationDescriptorList =
        constructAggregationDescriptorList(aggregationExpressions, curStep);
    updateTypeProvider(aggregationExpressions);
    if (curStep.isOutputPartial()) {
      aggregationDescriptorList.forEach(
          aggregationDescriptor ->
              updateTypeProviderByPartialAggregation(
                  aggregationDescriptor, context.getTypeProvider()));
    }
    if (outputEndTime) {
      context.getTypeProvider().setType(ENDTIME, TSDataType.INT64);
    }
    this.root =
        new AggregationNode(
            context.getQueryId().genPlanNodeId(),
            Collections.singletonList(this.getRoot()),
            aggregationDescriptorList,
            groupByTimeParameter,
            groupByParameter,
            groupByExpression,
            outputEndTime,
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
    List<CrossSeriesAggregationDescriptor> groupByLevelDescriptors = new ArrayList<>();
    for (Map.Entry<Expression, Set<Expression>> entry : groupByLevelExpressions.entrySet()) {
      groupByLevelDescriptors.add(
          new CrossSeriesAggregationDescriptor(
              ((FunctionExpression) entry.getKey()).getFunctionName(),
              curStep,
              entry.getValue().stream()
                  .map(Expression::getExpressions)
                  .flatMap(List::stream)
                  .collect(Collectors.toList()),
              entry.getValue().size(),
              ((FunctionExpression) entry.getKey()).getFunctionAttributes(),
              entry.getKey().getExpressions().get(0)));
    }
    updateTypeProvider(groupByLevelExpressions.keySet());
    updateTypeProvider(
        groupByLevelDescriptors.stream()
            .map(CrossSeriesAggregationDescriptor::getOutputExpression)
            .collect(Collectors.toList()));
    return new GroupByLevelNode(
        context.getQueryId().genPlanNodeId(),
        children,
        groupByLevelDescriptors,
        groupByTimeParameter,
        scanOrder);
  }

  private PlanNode createGroupByTagNode(
      List<String> tagKeys,
      Map<List<String>, LinkedHashMap<Expression, List<Expression>>>
          tagValuesToGroupedTimeseriesOperands,
      Collection<Expression> groupByTagOutputExpressions,
      List<PlanNode> children,
      AggregationStep curStep,
      GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder) {
    Map<List<String>, List<CrossSeriesAggregationDescriptor>> tagValuesToAggregationDescriptors =
        new HashMap<>();
    for (List<String> tagValues : tagValuesToGroupedTimeseriesOperands.keySet()) {
      LinkedHashMap<Expression, List<Expression>> groupedTimeseriesOperands =
          tagValuesToGroupedTimeseriesOperands.get(tagValues);
      List<CrossSeriesAggregationDescriptor> aggregationDescriptors = new ArrayList<>();

      // Bind an AggregationDescriptor for each GroupByTagOutputExpression
      for (Expression groupByTagOutputExpression : groupByTagOutputExpressions) {
        boolean added = false;
        for (Expression expression : groupedTimeseriesOperands.keySet()) {
          if (expression.equals(groupByTagOutputExpression)) {
            String functionName = ((FunctionExpression) expression).getFunctionName();
            CrossSeriesAggregationDescriptor aggregationDescriptor =
                new CrossSeriesAggregationDescriptor(
                    functionName,
                    curStep,
                    groupedTimeseriesOperands.get(expression),
                    ((FunctionExpression) expression).getFunctionAttributes(),
                    expression.getExpressions().get(0));
            aggregationDescriptors.add(aggregationDescriptor);
            added = true;
            break;
          }
        }
        if (!added) {
          aggregationDescriptors.add(null);
        }
      }
      tagValuesToAggregationDescriptors.put(tagValues, aggregationDescriptors);
    }

    updateTypeProvider(groupByTagOutputExpressions);
    updateTypeProviderWithConstantType(tagKeys, TSDataType.TEXT);
    return new GroupByTagNode(
        context.getQueryId().genPlanNodeId(),
        children,
        groupByTimeParameter,
        scanOrder,
        tagKeys,
        tagValuesToAggregationDescriptors,
        groupByTagOutputExpressions.stream()
            .map(Expression::getExpressionString)
            .collect(Collectors.toList()));
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
      // TODO: push down value filter
      seriesAggregationScanNode.setValueFilter(timeFilter);
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
      // TODO: push down value filter
      alignedSeriesAggregationScanNode.setValueFilter(timeFilter);
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
                  expression.getExpressions(),
                  ((FunctionExpression) expression).getFunctionAttributes());
            })
        .collect(Collectors.toList());
  }

  public LogicalPlanBuilder planFilterAndTransform(
      Expression filterExpression,
      Set<Expression> selectExpressions,
      boolean isGroupByTime,
      ZoneId zoneId,
      Ordering scanOrder) {
    if (filterExpression == null || selectExpressions.isEmpty()) {
      return this;
    }

    this.root =
        new FilterNode(
            context.getQueryId().genPlanNodeId(),
            this.getRoot(),
            selectExpressions.toArray(new Expression[0]),
            filterExpression,
            isGroupByTime,
            zoneId,
            scanOrder);
    updateTypeProvider(selectExpressions);
    return this;
  }

  public LogicalPlanBuilder planTransform(
      Set<Expression> selectExpressions, boolean isGroupByTime, ZoneId zoneId, Ordering scanOrder) {
    boolean needTransform = false;
    for (Expression expression : selectExpressions) {
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
            selectExpressions.toArray(new Expression[0]),
            isGroupByTime,
            zoneId,
            scanOrder);
    updateTypeProvider(selectExpressions);
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

  public LogicalPlanBuilder planLimit(long rowLimit) {
    if (rowLimit == 0) {
      return this;
    }

    this.root = new LimitNode(context.getQueryId().genPlanNodeId(), this.getRoot(), rowLimit);
    return this;
  }

  public LogicalPlanBuilder planOffset(long rowOffset) {
    if (rowOffset == 0) {
      return this;
    }

    this.root = new OffsetNode(context.getQueryId().genPlanNodeId(), this.getRoot(), rowOffset);
    return this;
  }

  public LogicalPlanBuilder planHavingAndTransform(
      Expression havingExpression,
      Set<Expression> selectExpressions,
      Set<Expression> orderByExpression,
      boolean isGroupByTime,
      ZoneId zoneId,
      Ordering scanOrder) {

    Set<Expression> outputExpressions = new HashSet<>(selectExpressions);
    if (orderByExpression != null) {
      outputExpressions.addAll(orderByExpression);
    }

    if (havingExpression != null) {
      return planFilterAndTransform(
          havingExpression, outputExpressions, isGroupByTime, zoneId, scanOrder);
    } else {
      return planTransform(outputExpressions, isGroupByTime, zoneId, scanOrder);
    }
  }

  public LogicalPlanBuilder planWhereAndSourceTransform(
      Expression whereExpression,
      Set<Expression> sourceTransformExpressions,
      boolean isGroupByTime,
      ZoneId zoneId,
      Ordering scanOrder) {
    if (whereExpression != null) {
      return planFilterAndTransform(
          whereExpression, sourceTransformExpressions, isGroupByTime, zoneId, scanOrder);
    } else {
      return planTransform(sourceTransformExpressions, isGroupByTime, zoneId, scanOrder);
    }
  }

  public LogicalPlanBuilder planDeviceViewInto(
      DeviceViewIntoPathDescriptor deviceViewIntoPathDescriptor) {
    if (deviceViewIntoPathDescriptor == null) {
      return this;
    }

    ColumnHeaderConstant.selectIntoAlignByDeviceColumnHeaders.forEach(
        columnHeader -> {
          updateTypeProviderWithConstantType(
              columnHeader.getColumnName(), columnHeader.getColumnType());
        });
    this.root =
        new DeviceViewIntoNode(
            context.getQueryId().genPlanNodeId(), this.getRoot(), deviceViewIntoPathDescriptor);
    return this;
  }

  public LogicalPlanBuilder planInto(IntoPathDescriptor intoPathDescriptor) {
    if (intoPathDescriptor == null) {
      return this;
    }

    ColumnHeaderConstant.selectIntoColumnHeaders.forEach(
        columnHeader -> {
          updateTypeProviderWithConstantType(
              columnHeader.getColumnName(), columnHeader.getColumnType());
        });
    this.root =
        new IntoNode(context.getQueryId().genPlanNodeId(), this.getRoot(), intoPathDescriptor);
    return this;
  }

  /** Meta Query* */
  public LogicalPlanBuilder planTimeSeriesSchemaSource(
      PartialPath pathPattern,
      SchemaFilter schemaFilter,
      long limit,
      long offset,
      boolean orderByHeat,
      boolean prefixPath,
      Map<Integer, Template> templateMap) {
    this.root =
        new TimeSeriesSchemaScanNode(
            context.getQueryId().genPlanNodeId(),
            pathPattern,
            schemaFilter,
            limit,
            offset,
            orderByHeat,
            prefixPath,
            templateMap);
    return this;
  }

  public LogicalPlanBuilder planDeviceSchemaSource(
      PartialPath pathPattern,
      long limit,
      long offset,
      boolean prefixPath,
      boolean hasSgCol,
      SchemaFilter schemaFilter) {
    this.root =
        new DevicesSchemaScanNode(
            context.getQueryId().genPlanNodeId(),
            pathPattern,
            limit,
            offset,
            prefixPath,
            hasSgCol,
            schemaFilter);
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

  @SuppressWarnings({"checkstyle:Indentation", "checkstyle:CommentsIndentation"})
  public LogicalPlanBuilder planSchemaFetchSource(
      List<String> storageGroupList,
      PathPatternTree patternTree,
      Map<Integer, Template> templateMap,
      boolean withTags) {
    PartialPath storageGroupPath;
    for (String storageGroup : storageGroupList) {
      try {
        storageGroupPath = new PartialPath(storageGroup);
        PathPatternTree overlappedPatternTree = new PathPatternTree();
        for (PartialPath pathPattern :
            patternTree.getOverlappedPathPatterns(
                storageGroupPath.concatNode(MULTI_LEVEL_PATH_WILDCARD))) {
          // pathPattern has been deduplicated, no need to deduplicate again
          overlappedPatternTree.appendFullPath(pathPattern);
        }
        this.root.addChild(
            new SchemaFetchScanNode(
                context.getQueryId().genPlanNodeId(),
                storageGroupPath,
                overlappedPatternTree,
                templateMap,
                withTags));
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
      PartialPath partialPath,
      boolean prefixPath,
      SchemaFilter schemaFilter,
      Map<Integer, Template> templateMap) {
    this.root =
        new TimeSeriesCountNode(
            context.getQueryId().genPlanNodeId(),
            partialPath,
            prefixPath,
            schemaFilter,
            templateMap);
    return this;
  }

  public LogicalPlanBuilder planLevelTimeSeriesCountSource(
      PartialPath partialPath,
      boolean prefixPath,
      int level,
      SchemaFilter schemaFilter,
      Map<Integer, Template> templateMap) {
    this.root =
        new LevelTimeSeriesCountNode(
            context.getQueryId().genPlanNodeId(),
            partialPath,
            prefixPath,
            level,
            schemaFilter,
            templateMap);
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

  public LogicalPlanBuilder planPathsUsingTemplateSource(
      List<PartialPath> pathPatternList, int templateId) {
    this.root =
        new PathsUsingTemplateScanNode(
            context.getQueryId().genPlanNodeId(), pathPatternList, templateId);
    return this;
  }

  public LogicalPlanBuilder planLogicalViewSchemaSource(
      PartialPath pathPattern, SchemaFilter schemaFilter, long limit, long offset) {
    this.root =
        new LogicalViewSchemaScanNode(
            context.getQueryId().genPlanNodeId(), pathPattern, schemaFilter, limit, offset);
    return this;
  }

  private LogicalPlanBuilder planSort(OrderByParameter orderByParameter) {
    if (orderByParameter.isEmpty()) {
      return this;
    }
    this.root = new SortNode(context.getQueryId().genPlanNodeId(), root, orderByParameter);
    return this;
  }

  public LogicalPlanBuilder planShowQueries(Analysis analysis, ShowQueriesStatement statement) {
    List<TDataNodeLocation> dataNodeLocations = analysis.getRunningDataNodeLocations();
    if (dataNodeLocations.size() == 1) {
      this.root =
          planSingleShowQueries(dataNodeLocations.get(0))
              .planFilterAndTransform(
                  analysis.getWhereExpression(),
                  analysis.getSourceExpressions(),
                  false,
                  statement.getZoneId(),
                  Ordering.ASC)
              .planSort(analysis.getMergeOrderParameter())
              .getRoot();
    } else {
      List<String> outputColumns = new ArrayList<>();
      MergeSortNode mergeSortNode =
          new MergeSortNode(
              context.getQueryId().genPlanNodeId(),
              analysis.getMergeOrderParameter(),
              outputColumns);

      dataNodeLocations.forEach(
          dataNodeLocation ->
              mergeSortNode.addChild(
                  this.planSingleShowQueries(dataNodeLocation)
                      .planFilterAndTransform(
                          analysis.getWhereExpression(),
                          analysis.getSourceExpressions(),
                          false,
                          statement.getZoneId(),
                          Ordering.ASC)
                      .planSort(analysis.getMergeOrderParameter())
                      .getRoot()));
      outputColumns.addAll(mergeSortNode.getChildren().get(0).getOutputColumnNames());
      this.root = mergeSortNode;
    }

    ColumnHeaderConstant.showQueriesColumnHeaders.forEach(
        columnHeader ->
            context
                .getTypeProvider()
                .setType(columnHeader.getColumnName(), columnHeader.getColumnType()));
    return this;
  }

  private LogicalPlanBuilder planSingleShowQueries(TDataNodeLocation dataNodeLocation) {
    this.root = new ShowQueriesNode(context.getQueryId().genPlanNodeId(), dataNodeLocation);
    return this;
  }

  public LogicalPlanBuilder planOrderBy(List<SortItem> sortItemList) {
    if (sortItemList.isEmpty()) {
      return this;
    }

    this.root =
        new SortNode(
            context.getQueryId().genPlanNodeId(), root, new OrderByParameter(sortItemList));
    return this;
  }

  public LogicalPlanBuilder planOrderBy(
      Set<Expression> orderByExpressions, List<SortItem> sortItemList) {

    updateTypeProvider(orderByExpressions);
    OrderByParameter orderByParameter = new OrderByParameter(sortItemList);
    if (orderByParameter.isEmpty()) {
      return this;
    }
    this.root = new SortNode(context.getQueryId().genPlanNodeId(), root, orderByParameter);
    return this;
  }

  public LogicalPlanBuilder planOrderBy(
      QueryStatement queryStatement,
      Set<Expression> orderByExpressions,
      Set<Expression> selectExpression) {
    // only the order by clause having expression needs a sortNode
    if (!queryStatement.hasOrderByExpression()) {
      return this;
    }

    updateTypeProvider(orderByExpressions);
    OrderByParameter orderByParameter = new OrderByParameter(queryStatement.getSortItemList());
    if (orderByParameter.isEmpty()) {
      return this;
    }
    this.root = new SortNode(context.getQueryId().genPlanNodeId(), root, orderByParameter);

    if (root.getOutputColumnNames().size() != selectExpression.size()) {
      this.root =
          new TransformNode(
              context.getQueryId().genPlanNodeId(),
              root,
              selectExpression.toArray(new Expression[0]),
              queryStatement.isGroupByTime(),
              queryStatement.getSelectComponent().getZoneId(),
              queryStatement.getResultTimeOrder());
    }
    return this;
  }
}
