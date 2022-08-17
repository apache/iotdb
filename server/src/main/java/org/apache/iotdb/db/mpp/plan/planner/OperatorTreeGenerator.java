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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaCache;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.aggregation.slidingwindow.SlidingWindowAggregatorFactory;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.exchange.ISinkHandle;
import org.apache.iotdb.db.mpp.execution.exchange.ISourceHandle;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.AggregationOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.DeviceViewOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.FillOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.FilterAndProjectOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.LinearFillOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.OffsetOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.RawDataAggregationOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.SlidingWindowAggregationOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.TransformOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.IFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.constant.BinaryConstantFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.constant.BooleanConstantFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.constant.DoubleConstantFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.constant.FloatConstantFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.constant.IntConstantFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.constant.LongConstantFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.identity.IdentityFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.identity.IdentityLinearFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.linear.DoubleLinearFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.linear.FloatLinearFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.linear.IntLinearFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.linear.LongLinearFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.previous.BinaryPreviousFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.previous.BooleanPreviousFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.previous.DoublePreviousFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.previous.FloatPreviousFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.previous.IntPreviousFill;
import org.apache.iotdb.db.mpp.execution.operator.process.fill.previous.LongPreviousFill;
import org.apache.iotdb.db.mpp.execution.operator.process.join.RowBasedTimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.TimeJoinOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.ColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.DescTimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.MultiColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.NonOverlappedMultiColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.mpp.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryCollectOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQuerySortOperator;
import org.apache.iotdb.db.mpp.execution.operator.process.last.LastQueryUtil;
import org.apache.iotdb.db.mpp.execution.operator.process.last.UpdateLastCacheOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.CountMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.DevicesCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.DevicesSchemaScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.LevelTimeSeriesCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.NodeManageMemoryMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.NodePathsConvertOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.NodePathsCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.NodePathsSchemaScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.PathsUsingTemplateScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaFetchMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaFetchScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaQueryMergeOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.SchemaQueryOrderByHeatOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.TimeSeriesCountOperator;
import org.apache.iotdb.db.mpp.execution.operator.schema.TimeSeriesSchemaScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.AlignedSeriesAggregationScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.AlignedSeriesScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesAggregationScanOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ColumnTransformerVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
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
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.SchemaQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesCountNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.metedata.read.TimeSeriesSchemaScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.AggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.DeviceViewNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FillNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.FilterNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.GroupByLevelNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.LimitNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.OffsetNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SlidingWindowAggregationNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.SortNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TimeJoinNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.TransformNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryCollectNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryMergeNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedLastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.LastQueryScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesAggregationScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.FillDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByLevelDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.OutputColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.FillPolicy;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.db.mpp.plan.statement.component.SortKey;
import org.apache.iotdb.db.mpp.plan.statement.literal.Literal;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.filter.operator.Gt;
import org.apache.iotdb.tsfile.read.filter.operator.GtEq;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.mpp.plan.constant.DataNodeEndPoints.isSameNode;

/** This Visitor is responsible for transferring PlanNode Tree to Operator Tree */
public class OperatorTreeGenerator extends PlanVisitor<Operator, LocalExecutionPlanContext> {

  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
      MPPDataExchangeService.getInstance().getMPPDataExchangeManager();

  private static final DataNodeSchemaCache DATA_NODE_SCHEMA_CACHE =
      DataNodeSchemaCache.getInstance();

  private static final TimeComparator ASC_TIME_COMPARATOR = new AscTimeComparator();

  private static final TimeComparator DESC_TIME_COMPARATOR = new DescTimeComparator();

  private static final IdentityFill IDENTITY_FILL = new IdentityFill();

  private static final IdentityLinearFill IDENTITY_LINEAR_FILL = new IdentityLinearFill();

  private static final Comparator<Binary> ASC_BINARY_COMPARATOR = Comparator.naturalOrder();

  private static final Comparator<Binary> DESC_BINARY_COMPARATOR = Comparator.reverseOrder();

  @Override
  public Operator visitPlan(PlanNode node, LocalExecutionPlanContext context) {
    throw new UnsupportedOperationException("should call the concrete visitXX() method");
  }

  @Override
  public Operator visitSeriesScan(SeriesScanNode node, LocalExecutionPlanContext context) {
    PartialPath seriesPath = node.getSeriesPath();
    boolean ascending = node.getScanOrder() == Ordering.ASC;
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SeriesScanOperator.class.getSimpleName());

    SeriesScanOperator seriesScanOperator =
        new SeriesScanOperator(
            node.getPlanNodeId(),
            seriesPath,
            context.getAllSensors(seriesPath.getDevice(), seriesPath.getMeasurement()),
            seriesPath.getSeriesType(),
            operatorContext,
            node.getTimeFilter(),
            node.getValueFilter(),
            ascending);

    context.addSourceOperator(seriesScanOperator);
    context.addPath(seriesPath);
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return seriesScanOperator;
  }

  @Override
  public Operator visitAlignedSeriesScan(
      AlignedSeriesScanNode node, LocalExecutionPlanContext context) {
    AlignedPath seriesPath = node.getAlignedPath();
    boolean ascending = node.getScanOrder() == Ordering.ASC;
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AlignedSeriesScanOperator.class.getSimpleName());

    AlignedSeriesScanOperator seriesScanOperator =
        new AlignedSeriesScanOperator(
            node.getPlanNodeId(),
            seriesPath,
            operatorContext,
            node.getTimeFilter(),
            node.getValueFilter(),
            ascending);

    context.addSourceOperator(seriesScanOperator);
    context.addPath(seriesPath);
    context
        .getTimeSliceAllocator()
        .recordExecutionWeight(operatorContext, seriesPath.getColumnNum());
    return seriesScanOperator;
  }

  @Override
  public Operator visitAlignedSeriesAggregationScan(
      AlignedSeriesAggregationScanNode node, LocalExecutionPlanContext context) {
    AlignedPath seriesPath = node.getAlignedPath();
    boolean ascending = node.getScanOrder() == Ordering.ASC;
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AlignedSeriesAggregationScanOperator.class.getSimpleName());
    List<Aggregator> aggregators = new ArrayList<>();
    for (AggregationDescriptor descriptor : node.getAggregationDescriptorList()) {
      checkArgument(
          descriptor.getInputExpressions().size() == 1,
          "descriptor's input expression size is not 1");
      checkArgument(
          descriptor.getInputExpressions().get(0) instanceof TimeSeriesOperand,
          "descriptor's input expression is not TimeSeriesOperand");
      String inputSeries =
          ((TimeSeriesOperand) (descriptor.getInputExpressions().get(0)))
              .getPath()
              .getMeasurement();
      int seriesIndex = seriesPath.getMeasurementList().indexOf(inputSeries);
      TSDataType seriesDataType =
          seriesPath.getMeasurementSchema().getSubMeasurementsTSDataTypeList().get(seriesIndex);
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  descriptor.getAggregationType(), seriesDataType, ascending),
              descriptor.getStep(),
              Collections.singletonList(new InputLocation[] {new InputLocation(0, seriesIndex)})));
    }

    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        new AlignedSeriesAggregationScanOperator(
            node.getPlanNodeId(),
            seriesPath,
            operatorContext,
            aggregators,
            node.getTimeFilter(),
            ascending,
            node.getGroupByTimeParameter());

    context.addSourceOperator(seriesAggregationScanOperator);
    context.addPath(seriesPath);
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, aggregators.size());
    return seriesAggregationScanOperator;
  }

  @Override
  public Operator visitSchemaQueryOrderByHeat(
      SchemaQueryOrderByHeatNode node, LocalExecutionPlanContext context) {
    List<Operator> children =
        node.getChildren().stream().map(n -> n.accept(this, context)).collect(Collectors.toList());

    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaQueryOrderByHeatOperator.class.getSimpleName());

    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new SchemaQueryOrderByHeatOperator(operatorContext, children);
  }

  @Override
  public Operator visitSchemaQueryScan(
      SchemaQueryScanNode node, LocalExecutionPlanContext context) {
    if (node instanceof TimeSeriesSchemaScanNode) {
      return visitTimeSeriesSchemaScan((TimeSeriesSchemaScanNode) node, context);
    } else if (node instanceof DevicesSchemaScanNode) {
      return visitDevicesSchemaScan((DevicesSchemaScanNode) node, context);
    } else if (node instanceof DevicesCountNode) {
      return visitDevicesCount((DevicesCountNode) node, context);
    } else if (node instanceof TimeSeriesCountNode) {
      return visitTimeSeriesCount((TimeSeriesCountNode) node, context);
    } else if (node instanceof LevelTimeSeriesCountNode) {
      return visitLevelTimeSeriesCount((LevelTimeSeriesCountNode) node, context);
    } else if (node instanceof NodePathsSchemaScanNode) {
      return visitNodePathsSchemaScan((NodePathsSchemaScanNode) node, context);
    } else if (node instanceof PathsUsingTemplateScanNode) {
      return visitPathsUsingTemplateScan((PathsUsingTemplateScanNode) node, context);
    }
    return visitPlan(node, context);
  }

  @Override
  public Operator visitTimeSeriesSchemaScan(
      TimeSeriesSchemaScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TimeSeriesSchemaScanOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new TimeSeriesSchemaScanOperator(
        node.getPlanNodeId(),
        operatorContext,
        node.getLimit(),
        node.getOffset(),
        node.getPath(),
        node.getKey(),
        node.getValue(),
        node.isContains(),
        node.isOrderByHeat(),
        node.isPrefixPath(),
        node.getTemplateMap());
  }

  @Override
  public Operator visitDevicesSchemaScan(
      DevicesSchemaScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                DevicesSchemaScanOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new DevicesSchemaScanOperator(
        node.getPlanNodeId(),
        operatorContext,
        node.getLimit(),
        node.getOffset(),
        node.getPath(),
        node.isPrefixPath(),
        node.isHasSgCol());
  }

  @Override
  public Operator visitSchemaQueryMerge(
      SchemaQueryMergeNode node, LocalExecutionPlanContext context) {
    List<Operator> children =
        node.getChildren().stream().map(n -> n.accept(this, context)).collect(Collectors.toList());
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaQueryMergeOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new SchemaQueryMergeOperator(node.getPlanNodeId(), operatorContext, children);
  }

  @Override
  public Operator visitCountMerge(CountSchemaMergeNode node, LocalExecutionPlanContext context) {
    List<Operator> children =
        node.getChildren().stream().map(n -> n.accept(this, context)).collect(Collectors.toList());
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                CountMergeOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new CountMergeOperator(node.getPlanNodeId(), operatorContext, children);
  }

  @Override
  public Operator visitDevicesCount(DevicesCountNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                DevicesCountOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new DevicesCountOperator(
        node.getPlanNodeId(), operatorContext, node.getPath(), node.isPrefixPath());
  }

  @Override
  public Operator visitTimeSeriesCount(
      TimeSeriesCountNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TimeSeriesCountOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new TimeSeriesCountOperator(
        node.getPlanNodeId(),
        operatorContext,
        node.getPath(),
        node.isPrefixPath(),
        node.getKey(),
        node.getValue(),
        node.isContains());
  }

  @Override
  public Operator visitLevelTimeSeriesCount(
      LevelTimeSeriesCountNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                LevelTimeSeriesCountOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new LevelTimeSeriesCountOperator(
        node.getPlanNodeId(),
        operatorContext,
        node.getPath(),
        node.isPrefixPath(),
        node.getLevel(),
        node.getKey(),
        node.getValue(),
        node.isContains());
  }

  @Override
  public Operator visitNodePathsSchemaScan(
      NodePathsSchemaScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                NodePathsSchemaScanOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new NodePathsSchemaScanOperator(
        node.getPlanNodeId(), operatorContext, node.getPrefixPath(), node.getLevel());
  }

  @Override
  public Operator visitNodeManagementMemoryMerge(
      NodeManagementMemoryMergeNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                NodeManageMemoryMergeOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new NodeManageMemoryMergeOperator(operatorContext, node.getData(), child);
  }

  @Override
  public Operator visitNodePathConvert(
      NodePathsConvertNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                NodePathsConvertOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new NodePathsConvertOperator(operatorContext, child);
  }

  @Override
  public Operator visitNodePathsCount(NodePathsCountNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                NodePathsCountOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new NodePathsCountOperator(operatorContext, child);
  }

  @Override
  public Operator visitSeriesAggregationScan(
      SeriesAggregationScanNode node, LocalExecutionPlanContext context) {
    PartialPath seriesPath = node.getSeriesPath();
    boolean ascending = node.getScanOrder() == Ordering.ASC;
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SeriesAggregationScanOperator.class.getSimpleName());

    List<Aggregator> aggregators = new ArrayList<>();
    node.getAggregationDescriptorList()
        .forEach(
            o ->
                aggregators.add(
                    new Aggregator(
                        AccumulatorFactory.createAccumulator(
                            o.getAggregationType(),
                            node.getSeriesPath().getSeriesType(),
                            ascending),
                        o.getStep())));
    SeriesAggregationScanOperator aggregateScanOperator =
        new SeriesAggregationScanOperator(
            node.getPlanNodeId(),
            seriesPath,
            context.getAllSensors(seriesPath.getDevice(), seriesPath.getMeasurement()),
            operatorContext,
            aggregators,
            node.getTimeFilter(),
            ascending,
            node.getGroupByTimeParameter());

    context.addSourceOperator(aggregateScanOperator);
    context.addPath(seriesPath);
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, aggregators.size());
    return aggregateScanOperator;
  }

  @Override
  public Operator visitDeviceView(DeviceViewNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                DeviceViewOperator.class.getSimpleName());
    List<Operator> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(Collectors.toList());
    List<List<Integer>> deviceColumnIndex =
        node.getDevices().stream()
            .map(deviceName -> node.getDeviceToMeasurementIndexesMap().get(deviceName))
            .collect(Collectors.toList());
    List<TSDataType> outputColumnTypes = getOutputColumnTypes(node, context.getTypeProvider());

    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new DeviceViewOperator(
        operatorContext, node.getDevices(), children, deviceColumnIndex, outputColumnTypes);
  }

  @Override
  public Operator visitDeviceMerge(DeviceMergeNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                DeviceMergeOperator.class.getSimpleName());
    List<Operator> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(Collectors.toList());
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    TimeSelector selector = null;
    TimeComparator timeComparator = null;
    for (SortItem sortItem : node.getMergeOrderParameter().getSortItemList()) {
      if (sortItem.getSortKey() == SortKey.TIME) {
        Ordering ordering = sortItem.getOrdering();
        if (ordering == Ordering.ASC) {
          selector = new TimeSelector(node.getChildren().size() << 1, true);
          timeComparator = ASC_TIME_COMPARATOR;
        } else {
          selector = new TimeSelector(node.getChildren().size() << 1, false);
          timeComparator = DESC_TIME_COMPARATOR;
        }
        break;
      }
    }

    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new DeviceMergeOperator(
        operatorContext, node.getDevices(), children, dataTypes, selector, timeComparator);
  }

  @Override
  public Operator visitFill(FillNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    return getFillOperator(node, context, child);
  }

  private ProcessOperator getFillOperator(
      FillNode node, LocalExecutionPlanContext context, Operator child) {
    FillDescriptor descriptor = node.getFillDescriptor();
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());
    int inputColumns = inputDataTypes.size();
    FillPolicy fillPolicy = descriptor.getFillPolicy();
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                FillOperator.class.getSimpleName());
    switch (fillPolicy) {
      case VALUE:
        Literal literal = descriptor.getFillValue();
        context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
        return new FillOperator(
            operatorContext, getConstantFill(inputColumns, inputDataTypes, literal), child);
      case PREVIOUS:
        context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
        return new FillOperator(
            operatorContext, getPreviousFill(inputColumns, inputDataTypes), child);
      case LINEAR:
        context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
        return new LinearFillOperator(
            operatorContext, getLinearFill(inputColumns, inputDataTypes), child);
      default:
        throw new IllegalArgumentException("Unknown fill policy: " + fillPolicy);
    }
  }

  private IFill[] getConstantFill(
      int inputColumns, List<TSDataType> inputDataTypes, Literal literal) {
    IFill[] constantFill = new IFill[inputColumns];
    for (int i = 0; i < inputColumns; i++) {
      if (!literal.isDataTypeConsistency(inputDataTypes.get(i))) {
        constantFill[i] = IDENTITY_FILL;
        continue;
      }
      switch (inputDataTypes.get(i)) {
        case BOOLEAN:
          constantFill[i] = new BooleanConstantFill(literal.getBoolean());
          break;
        case TEXT:
          constantFill[i] = new BinaryConstantFill(literal.getBinary());
          break;
        case INT32:
          constantFill[i] = new IntConstantFill(literal.getInt());
          break;
        case INT64:
          constantFill[i] = new LongConstantFill(literal.getLong());
          break;
        case FLOAT:
          constantFill[i] = new FloatConstantFill(literal.getFloat());
          break;
        case DOUBLE:
          constantFill[i] = new DoubleConstantFill(literal.getDouble());
          break;
        default:
          throw new IllegalArgumentException("Unknown data type: " + inputDataTypes.get(i));
      }
    }
    return constantFill;
  }

  private IFill[] getPreviousFill(int inputColumns, List<TSDataType> inputDataTypes) {
    IFill[] previousFill = new IFill[inputColumns];
    for (int i = 0; i < inputColumns; i++) {
      switch (inputDataTypes.get(i)) {
        case BOOLEAN:
          previousFill[i] = new BooleanPreviousFill();
          break;
        case TEXT:
          previousFill[i] = new BinaryPreviousFill();
          break;
        case INT32:
          previousFill[i] = new IntPreviousFill();
          break;
        case INT64:
          previousFill[i] = new LongPreviousFill();
          break;
        case FLOAT:
          previousFill[i] = new FloatPreviousFill();
          break;
        case DOUBLE:
          previousFill[i] = new DoublePreviousFill();
          break;
        default:
          throw new IllegalArgumentException("Unknown data type: " + inputDataTypes.get(i));
      }
    }
    return previousFill;
  }

  private ILinearFill[] getLinearFill(int inputColumns, List<TSDataType> inputDataTypes) {
    ILinearFill[] linearFill = new ILinearFill[inputColumns];
    for (int i = 0; i < inputColumns; i++) {
      switch (inputDataTypes.get(i)) {
        case INT32:
          linearFill[i] = new IntLinearFill();
          break;
        case INT64:
          linearFill[i] = new LongLinearFill();
          break;
        case FLOAT:
          linearFill[i] = new FloatLinearFill();
          break;
        case DOUBLE:
          linearFill[i] = new DoubleLinearFill();
          break;
        case BOOLEAN:
        case TEXT:
          linearFill[i] = IDENTITY_LINEAR_FILL;
          break;
        default:
          throw new IllegalArgumentException("Unknown data type: " + inputDataTypes.get(i));
      }
    }
    return linearFill;
  }

  @Override
  public Operator visitTransform(TransformNode node, LocalExecutionPlanContext context) {
    final OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TransformOperator.class.getSimpleName());
    final Operator inputOperator = generateOnlyChildOperator(node, context);
    final List<TSDataType> inputDataTypes = getInputColumnTypes(node, context.getTypeProvider());
    final Map<String, List<InputLocation>> inputLocations = makeLayout(node);
    final Expression[] projectExpressions = node.getOutputExpressions();
    final TypeProvider typeProvider = context.getTypeProvider();

    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);

    boolean hasNonMappableUDF = false;
    for (Expression expression : projectExpressions) {
      if (!expression.isMappable(typeProvider)) {
        hasNonMappableUDF = true;
        break;
      }
    }

    // Use FilterAndProject Operator when project expressions are all mappable
    if (!hasNonMappableUDF) {
      // init project UDTFContext
      UDTFContext projectContext = new UDTFContext(node.getZoneId());
      projectContext.constructUdfExecutors(projectExpressions);

      List<ColumnTransformer> projectOutputTransformerList = new ArrayList<>();
      Map<Expression, ColumnTransformer> projectExpressionColumnTransformerMap = new HashMap<>();

      // records LeafColumnTransformer of project expressions
      List<LeafColumnTransformer> projectLeafColumnTransformerList = new ArrayList<>();

      ColumnTransformerVisitor visitor = new ColumnTransformerVisitor();
      ColumnTransformerVisitor.ColumnTransformerVisitorContext projectColumnTransformerContext =
          new ColumnTransformerVisitor.ColumnTransformerVisitorContext(
              projectContext,
              typeProvider,
              projectLeafColumnTransformerList,
              inputLocations,
              projectExpressionColumnTransformerMap,
              ImmutableMap.of(),
              ImmutableList.of(),
              inputDataTypes,
              inputLocations.size());

      for (Expression expression : projectExpressions) {
        projectOutputTransformerList.add(
            visitor.process(expression, projectColumnTransformerContext));
      }

      return new FilterAndProjectOperator(
          operatorContext,
          inputOperator,
          inputDataTypes,
          ImmutableList.of(),
          null,
          ImmutableList.of(),
          projectLeafColumnTransformerList,
          projectOutputTransformerList,
          false,
          false);
    }

    try {
      return new TransformOperator(
          operatorContext,
          inputOperator,
          inputDataTypes,
          inputLocations,
          node.getOutputExpressions(),
          node.isKeepNull(),
          node.getZoneId(),
          context.getTypeProvider(),
          node.getScanOrder() == Ordering.ASC);
    } catch (QueryProcessException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Operator visitFilter(FilterNode node, LocalExecutionPlanContext context) {
    final Expression filterExpression = node.getPredicate();
    final TypeProvider typeProvider = context.getTypeProvider();

    // check whether predicate contains Non-Mappable UDF
    if (!filterExpression.isMappable(typeProvider)) {
      throw new UnsupportedOperationException("Filter can not contain Non-Mappable UDF");
    }

    final Expression[] projectExpressions = node.getOutputExpressions();
    final Operator inputOperator = generateOnlyChildOperator(node, context);
    final Map<String, List<InputLocation>> inputLocations = makeLayout(node);
    final List<TSDataType> inputDataTypes = getInputColumnTypes(node, context.getTypeProvider());
    final List<TSDataType> filterOutputDataTypes = new ArrayList<>(inputDataTypes);
    final OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                FilterAndProjectOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);

    boolean hasNonMappableUDF = false;
    for (Expression expression : projectExpressions) {
      if (!expression.isMappable(typeProvider)) {
        hasNonMappableUDF = true;
        break;
      }
    }

    // init UDTFContext;
    UDTFContext filterContext = new UDTFContext(node.getZoneId());
    filterContext.constructUdfExecutors(new Expression[] {filterExpression});

    // records LeafColumnTransformer of filter
    List<LeafColumnTransformer> filterLeafColumnTransformerList = new ArrayList<>();

    // records common ColumnTransformer between filter and project expressions
    List<ColumnTransformer> commonTransformerList = new ArrayList<>();

    // records LeafColumnTransformer of project expressions
    List<LeafColumnTransformer> projectLeafColumnTransformerList = new ArrayList<>();

    // records subexpression -> ColumnTransformer for filter
    Map<Expression, ColumnTransformer> filterExpressionColumnTransformerMap = new HashMap<>();

    ColumnTransformerVisitor visitor = new ColumnTransformerVisitor();

    ColumnTransformerVisitor.ColumnTransformerVisitorContext filterColumnTransformerContext =
        new ColumnTransformerVisitor.ColumnTransformerVisitorContext(
            filterContext,
            typeProvider,
            filterLeafColumnTransformerList,
            inputLocations,
            filterExpressionColumnTransformerMap,
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            0);

    ColumnTransformer filterOutputTransformer =
        visitor.process(filterExpression, filterColumnTransformerContext);

    List<ColumnTransformer> projectOutputTransformerList = new ArrayList<>();

    Map<Expression, ColumnTransformer> projectExpressionColumnTransformerMap = new HashMap<>();

    // init project transformer when project expressions are all mappable
    if (!hasNonMappableUDF) {
      // init project UDTFContext
      UDTFContext projectContext = new UDTFContext(node.getZoneId());
      projectContext.constructUdfExecutors(projectExpressions);

      ColumnTransformerVisitor.ColumnTransformerVisitorContext projectColumnTransformerContext =
          new ColumnTransformerVisitor.ColumnTransformerVisitorContext(
              projectContext,
              typeProvider,
              projectLeafColumnTransformerList,
              inputLocations,
              projectExpressionColumnTransformerMap,
              filterExpressionColumnTransformerMap,
              commonTransformerList,
              filterOutputDataTypes,
              inputLocations.size());

      for (Expression expression : projectExpressions) {
        projectOutputTransformerList.add(
            visitor.process(expression, projectColumnTransformerContext));
      }
    }

    Operator filter =
        new FilterAndProjectOperator(
            operatorContext,
            inputOperator,
            filterOutputDataTypes,
            filterLeafColumnTransformerList,
            filterOutputTransformer,
            commonTransformerList,
            projectLeafColumnTransformerList,
            projectOutputTransformerList,
            hasNonMappableUDF,
            true);

    // Project expressions don't contain Non-Mappable UDF, TransformOperator is not needed
    if (!hasNonMappableUDF) {
      return filter;
    }

    // has Non-Mappable UDF, we wrap a TransformOperator for further calculation
    try {
      final OperatorContext transformContext =
          context
              .getInstanceContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  TransformOperator.class.getSimpleName());
      context.getTimeSliceAllocator().recordExecutionWeight(transformContext, 1);
      return new TransformOperator(
          transformContext,
          filter,
          inputDataTypes,
          inputLocations,
          projectExpressions,
          node.isKeepNull(),
          node.getZoneId(),
          context.getTypeProvider(),
          node.getScanOrder() == Ordering.ASC);
    } catch (QueryProcessException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Operator visitGroupByLevel(GroupByLevelNode node, LocalExecutionPlanContext context) {
    checkArgument(
        node.getGroupByLevelDescriptors().size() >= 1,
        "GroupByLevel descriptorList cannot be empty");
    List<Operator> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(Collectors.toList());
    boolean ascending = node.getScanOrder() == Ordering.ASC;
    List<Aggregator> aggregators = new ArrayList<>();
    Map<String, List<InputLocation>> layout = makeLayout(node);
    for (GroupByLevelDescriptor descriptor : node.getGroupByLevelDescriptors()) {
      List<InputLocation[]> inputLocationList = calcInputLocationList(descriptor, layout);
      TSDataType seriesDataType =
          context
              .getTypeProvider()
              // get the type of first inputExpression
              .getType(descriptor.getInputExpressions().get(0).getExpressionString());
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  descriptor.getAggregationType(), seriesDataType, ascending),
              descriptor.getStep(),
              inputLocationList));
    }
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AggregationOperator.class.getSimpleName());

    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, aggregators.size());
    return new AggregationOperator(
        operatorContext, aggregators, children, ascending, node.getGroupByTimeParameter(), false);
  }

  @Override
  public Operator visitSlidingWindowAggregation(
      SlidingWindowAggregationNode node, LocalExecutionPlanContext context) {
    checkArgument(
        node.getAggregationDescriptorList().size() >= 1,
        "Aggregation descriptorList cannot be empty");
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SlidingWindowAggregationOperator.class.getSimpleName());
    Operator child = node.getChild().accept(this, context);
    boolean ascending = node.getScanOrder() == Ordering.ASC;
    List<Aggregator> aggregators = new ArrayList<>();
    Map<String, List<InputLocation>> layout = makeLayout(node);
    for (AggregationDescriptor descriptor : node.getAggregationDescriptorList()) {
      List<InputLocation[]> inputLocationList = calcInputLocationList(descriptor, layout);
      aggregators.add(
          SlidingWindowAggregatorFactory.createSlidingWindowAggregator(
              descriptor.getAggregationType(),
              context
                  .getTypeProvider()
                  // get the type of first inputExpression
                  .getType(descriptor.getInputExpressions().get(0).toString()),
              ascending,
              inputLocationList,
              descriptor.getStep()));
    }

    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, aggregators.size());
    return new SlidingWindowAggregationOperator(
        operatorContext, aggregators, child, ascending, node.getGroupByTimeParameter());
  }

  @Override
  public Operator visitLimit(LimitNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                LimitOperator.class.getSimpleName());

    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new LimitOperator(operatorContext, node.getLimit(), child);
  }

  @Override
  public Operator visitOffset(OffsetNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                OffsetOperator.class.getSimpleName());

    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new OffsetOperator(operatorContext, node.getOffset(), child);
  }

  @Override
  public Operator visitAggregation(AggregationNode node, LocalExecutionPlanContext context) {
    checkArgument(
        node.getAggregationDescriptorList().size() >= 1,
        "Aggregation descriptorList cannot be empty");
    List<Operator> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(Collectors.toList());
    boolean ascending = node.getScanOrder() == Ordering.ASC;
    List<Aggregator> aggregators = new ArrayList<>();
    Map<String, List<InputLocation>> layout = makeLayout(node);
    for (AggregationDescriptor descriptor : node.getAggregationDescriptorList()) {
      List<InputLocation[]> inputLocationList = calcInputLocationList(descriptor, layout);
      aggregators.add(
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  descriptor.getAggregationType(),
                  context
                      .getTypeProvider()
                      // get the type of first inputExpression
                      .getType(descriptor.getInputExpressions().get(0).toString()),
                  ascending),
              descriptor.getStep(),
              inputLocationList));
    }
    boolean inputRaw = node.getAggregationDescriptorList().get(0).getStep().isInputRaw();
    if (inputRaw) {
      checkArgument(children.size() == 1, "rawDataAggregateOperator can only accept one input");
      OperatorContext operatorContext =
          context
              .getInstanceContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  RawDataAggregationOperator.class.getSimpleName());
      context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, aggregators.size());
      return new RawDataAggregationOperator(
          operatorContext, aggregators, children.get(0), ascending, node.getGroupByTimeParameter());
    } else {
      OperatorContext operatorContext =
          context
              .getInstanceContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  AggregationOperator.class.getSimpleName());
      context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, aggregators.size());
      return new AggregationOperator(
          operatorContext, aggregators, children, ascending, node.getGroupByTimeParameter(), true);
    }
  }

  private List<InputLocation[]> calcInputLocationList(
      AggregationDescriptor descriptor, Map<String, List<InputLocation>> layout) {
    List<List<String>> inputColumnNames = descriptor.getInputColumnNamesList();
    List<InputLocation[]> inputLocationList = new ArrayList<>();

    for (List<String> inputColumnNamesOfOneInput : inputColumnNames) {
      // it may include double parts
      List<List<InputLocation>> inputLocationParts = new ArrayList<>();
      inputColumnNamesOfOneInput.forEach(o -> inputLocationParts.add(layout.get(o)));
      for (int i = 0; i < inputLocationParts.get(0).size(); i++) {
        if (inputColumnNamesOfOneInput.size() == 1) {
          inputLocationList.add(new InputLocation[] {inputLocationParts.get(0).get(i)});
        } else {
          inputLocationList.add(
              new InputLocation[] {
                inputLocationParts.get(0).get(i), inputLocationParts.get(1).get(i)
              });
        }
      }
    }
    return inputLocationList;
  }

  @Override
  public Operator visitSort(SortNode node, LocalExecutionPlanContext context) {
    return super.visitSort(node, context);
  }

  @Override
  public Operator visitTimeJoin(TimeJoinNode node, LocalExecutionPlanContext context) {
    List<Operator> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(Collectors.toList());
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TimeJoinOperator.class.getSimpleName());
    TimeComparator timeComparator =
        node.getMergeOrder() == Ordering.ASC ? ASC_TIME_COMPARATOR : DESC_TIME_COMPARATOR;
    List<OutputColumn> outputColumns = generateOutputColumns(node);
    List<ColumnMerger> mergers = createColumnMergers(outputColumns, timeComparator);
    List<TSDataType> outputColumnTypes = getOutputColumnTypes(node, context.getTypeProvider());

    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new RowBasedTimeJoinOperator(
        operatorContext,
        children,
        node.getMergeOrder(),
        outputColumnTypes,
        mergers,
        timeComparator);
  }

  private List<OutputColumn> generateOutputColumns(TimeJoinNode node) {
    // TODO we should also sort the InputLocation for each column if they are not overlapped
    return makeLayout(node).values().stream()
        .map(inputLocations -> new OutputColumn(inputLocations, inputLocations.size() > 1))
        .collect(Collectors.toList());
  }

  private List<ColumnMerger> createColumnMergers(
      List<OutputColumn> outputColumns, TimeComparator timeComparator) {
    List<ColumnMerger> mergers = new ArrayList<>(outputColumns.size());
    for (OutputColumn outputColumn : outputColumns) {
      ColumnMerger merger;
      // only has one input column
      if (outputColumn.isSingleInputColumn()) {
        merger = new SingleColumnMerger(outputColumn.getSourceLocation(0), timeComparator);
      } else if (outputColumn.isOverlapped()) {
        // has more than one input columns but time of these input columns is overlapped
        merger = new MultiColumnMerger(outputColumn.getSourceLocations());
      } else {
        // has more than one input columns and time of these input columns is not overlapped
        merger =
            new NonOverlappedMultiColumnMerger(outputColumn.getSourceLocations(), timeComparator);
      }
      mergers.add(merger);
    }
    return mergers;
  }

  @Override
  public Operator visitExchange(ExchangeNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ExchangeOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 0);

    FragmentInstanceId localInstanceId = context.getInstanceContext().getId();
    FragmentInstanceId remoteInstanceId = node.getUpstreamInstanceId();

    TEndPoint upstreamEndPoint = node.getUpstreamEndpoint();
    ISourceHandle sourceHandle =
        isSameNode(upstreamEndPoint)
            ? MPP_DATA_EXCHANGE_MANAGER.createLocalSourceHandle(
                localInstanceId.toThrift(),
                node.getPlanNodeId().getId(),
                remoteInstanceId.toThrift(),
                context.getInstanceContext()::failed)
            : MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
                localInstanceId.toThrift(),
                node.getPlanNodeId().getId(),
                upstreamEndPoint,
                remoteInstanceId.toThrift(),
                context.getInstanceContext()::failed);
    return new ExchangeOperator(operatorContext, sourceHandle, node.getUpstreamPlanNodeId());
  }

  @Override
  public Operator visitFragmentSink(FragmentSinkNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);

    FragmentInstanceId localInstanceId = context.getInstanceContext().getId();
    FragmentInstanceId targetInstanceId = node.getDownStreamInstanceId();
    TEndPoint downStreamEndPoint = node.getDownStreamEndpoint();

    checkArgument(
        MPP_DATA_EXCHANGE_MANAGER != null, "MPP_DATA_EXCHANGE_MANAGER should not be null");

    ISinkHandle sinkHandle =
        isSameNode(downStreamEndPoint)
            ? MPP_DATA_EXCHANGE_MANAGER.createLocalSinkHandle(
                localInstanceId.toThrift(),
                targetInstanceId.toThrift(),
                node.getDownStreamPlanNodeId().getId(),
                context.getInstanceContext())
            : MPP_DATA_EXCHANGE_MANAGER.createSinkHandle(
                localInstanceId.toThrift(),
                downStreamEndPoint,
                targetInstanceId.toThrift(),
                node.getDownStreamPlanNodeId().getId(),
                context.getInstanceContext());
    context.setSinkHandle(sinkHandle);
    return child;
  }

  @Override
  public Operator visitSchemaFetchMerge(
      SchemaFetchMergeNode node, LocalExecutionPlanContext context) {
    List<Operator> children =
        node.getChildren().stream().map(n -> n.accept(this, context)).collect(Collectors.toList());
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaFetchMergeOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new SchemaFetchMergeOperator(operatorContext, children, node.getStorageGroupList());
  }

  @Override
  public Operator visitSchemaFetchScan(
      SchemaFetchScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaFetchScanOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new SchemaFetchScanOperator(
        node.getPlanNodeId(),
        operatorContext,
        node.getPatternTree(),
        node.getTemplateMap(),
        ((SchemaDriverContext) (context.getInstanceContext().getDriverContext()))
            .getSchemaRegion());
  }

  @Override
  public Operator visitLastQueryScan(LastQueryScanNode node, LocalExecutionPlanContext context) {
    PartialPath seriesPath = node.getSeriesPath().transformToPartialPath();
    TimeValuePair timeValuePair = DATA_NODE_SCHEMA_CACHE.getLastCache(seriesPath);
    if (timeValuePair == null) { // last value is not cached
      return createUpdateLastCacheOperator(node, context, node.getSeriesPath());
    } else if (!LastQueryUtil.satisfyFilter(
        context.getLastQueryTimeFilter(), timeValuePair)) { // cached last value is not satisfied

      boolean isFilterGtOrGe =
          (context.getLastQueryTimeFilter() instanceof Gt
              || context.getLastQueryTimeFilter() instanceof GtEq);
      // time filter is not > or >=, we still need to read from disk
      if (!isFilterGtOrGe) {
        return createUpdateLastCacheOperator(node, context, node.getSeriesPath());
      } else { // otherwise, we just ignore it and return null
        return null;
      }
    } else { //  cached last value is satisfied, put it into LastCacheScanOperator
      context.addCachedLastValue(timeValuePair, seriesPath.getFullPath());
      return null;
    }
  }

  private UpdateLastCacheOperator createUpdateLastCacheOperator(
      LastQueryScanNode node, LocalExecutionPlanContext context, MeasurementPath fullPath) {
    SeriesAggregationScanOperator lastQueryScan = createLastQueryScanOperator(node, context);

    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                UpdateLastCacheOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new UpdateLastCacheOperator(
        operatorContext,
        lastQueryScan,
        fullPath,
        node.getSeriesPath().getSeriesType(),
        DATA_NODE_SCHEMA_CACHE,
        context.isNeedUpdateLastCache());
  }

  private SeriesAggregationScanOperator createLastQueryScanOperator(
      LastQueryScanNode node, LocalExecutionPlanContext context) {
    MeasurementPath seriesPath = node.getSeriesPath();
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SeriesAggregationScanOperator.class.getSimpleName());

    // last_time, last_value
    List<Aggregator> aggregators = LastQueryUtil.createAggregators(seriesPath.getSeriesType());

    SeriesAggregationScanOperator seriesAggregationScanOperator =
        new SeriesAggregationScanOperator(
            node.getPlanNodeId(),
            seriesPath,
            context.getAllSensors(seriesPath.getDevice(), seriesPath.getMeasurement()),
            operatorContext,
            aggregators,
            context.getLastQueryTimeFilter(),
            false,
            null);
    context.addSourceOperator(seriesAggregationScanOperator);
    context.addPath(seriesPath);
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, aggregators.size());
    return seriesAggregationScanOperator;
  }

  @Override
  public Operator visitAlignedLastQueryScan(
      AlignedLastQueryScanNode node, LocalExecutionPlanContext context) {
    PartialPath seriesPath = node.getSeriesPath().transformToPartialPath();
    TimeValuePair timeValuePair = DATA_NODE_SCHEMA_CACHE.getLastCache(seriesPath);
    if (timeValuePair == null) { // last value is not cached
      return createUpdateLastCacheOperator(
          node, context, node.getSeriesPath().getMeasurementPath());
    } else if (!LastQueryUtil.satisfyFilter(
        context.getLastQueryTimeFilter(), timeValuePair)) { // cached last value is not satisfied

      boolean isFilterGtOrGe =
          (context.getLastQueryTimeFilter() instanceof Gt
              || context.getLastQueryTimeFilter() instanceof GtEq);
      // time filter is not > or >=, we still need to read from disk
      if (!isFilterGtOrGe) {
        return createUpdateLastCacheOperator(
            node, context, node.getSeriesPath().getMeasurementPath());
      } else { // otherwise, we just ignore it and return null
        return null;
      }
    } else { //  cached last value is satisfied, put it into LastCacheScanOperator
      context.addCachedLastValue(timeValuePair, seriesPath.getFullPath());
      return null;
    }
  }

  private UpdateLastCacheOperator createUpdateLastCacheOperator(
      AlignedLastQueryScanNode node, LocalExecutionPlanContext context, MeasurementPath fullPath) {
    AlignedSeriesAggregationScanOperator lastQueryScan = createLastQueryScanOperator(node, context);

    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                UpdateLastCacheOperator.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new UpdateLastCacheOperator(
        operatorContext,
        lastQueryScan,
        fullPath,
        node.getSeriesPath().getSchemaList().get(0).getType(),
        DATA_NODE_SCHEMA_CACHE,
        context.isNeedUpdateLastCache());
  }

  private AlignedSeriesAggregationScanOperator createLastQueryScanOperator(
      AlignedLastQueryScanNode node, LocalExecutionPlanContext context) {
    AlignedPath seriesPath = node.getSeriesPath();
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AlignedSeriesAggregationScanOperator.class.getSimpleName());

    // last_time, last_value
    List<Aggregator> aggregators =
        LastQueryUtil.createAggregators(seriesPath.getSchemaList().get(0).getType());
    AlignedSeriesAggregationScanOperator seriesAggregationScanOperator =
        new AlignedSeriesAggregationScanOperator(
            node.getPlanNodeId(),
            seriesPath,
            operatorContext,
            aggregators,
            context.getLastQueryTimeFilter(),
            false,
            null);
    context.addSourceOperator(seriesAggregationScanOperator);
    context.addPath(seriesPath);
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, aggregators.size());
    return seriesAggregationScanOperator;
  }

  @Override
  public Operator visitLastQuery(LastQueryNode node, LocalExecutionPlanContext context) {

    List<SortItem> sortItemList = node.getMergeOrderParameter().getSortItemList();
    checkArgument(
        sortItemList.isEmpty()
            || (sortItemList.size() == 1 && sortItemList.get(0).getSortKey() == SortKey.TIMESERIES),
        "Last query only support order by timeseries asc/desc");

    context.setLastQueryTimeFilter(node.getTimeFilter());
    context.setNeedUpdateLastCache(LastQueryUtil.needUpdateCache(node.getTimeFilter()));

    List<UpdateLastCacheOperator> operatorList =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .filter(Objects::nonNull)
            .map(o -> (UpdateLastCacheOperator) o)
            .collect(Collectors.toList());

    List<Pair<TimeValuePair, Binary>> cachedLastValueAndPathList =
        context.getCachedLastValueAndPathList();

    int initSize = cachedLastValueAndPathList != null ? cachedLastValueAndPathList.size() : 0;
    // no order by clause
    if (sortItemList.isEmpty()) {
      TsBlockBuilder builder = LastQueryUtil.createTsBlockBuilder(initSize);
      for (int i = 0; i < initSize; i++) {
        TimeValuePair timeValuePair = cachedLastValueAndPathList.get(i).left;
        LastQueryUtil.appendLastValue(
            builder,
            timeValuePair.getTimestamp(),
            cachedLastValueAndPathList.get(i).right,
            timeValuePair.getValue().getStringValue(),
            timeValuePair.getValue().getDataType().name());
      }
      OperatorContext operatorContext =
          context
              .getInstanceContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  LastQueryOperator.class.getSimpleName());
      context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
      return new LastQueryOperator(operatorContext, operatorList, builder);
    } else {
      // order by timeseries
      Comparator<Binary> comparator =
          sortItemList.get(0).getOrdering() == Ordering.ASC
              ? ASC_BINARY_COMPARATOR
              : DESC_BINARY_COMPARATOR;
      // sort values from last cache
      if (initSize > 0) {
        cachedLastValueAndPathList.sort(Comparator.comparing(Pair::getRight, comparator));
      }

      TsBlockBuilder builder = LastQueryUtil.createTsBlockBuilder(initSize);
      for (int i = 0; i < initSize; i++) {
        TimeValuePair timeValuePair = cachedLastValueAndPathList.get(i).left;
        LastQueryUtil.appendLastValue(
            builder,
            timeValuePair.getTimestamp(),
            cachedLastValueAndPathList.get(i).right,
            timeValuePair.getValue().getStringValue(),
            timeValuePair.getValue().getDataType().name());
      }

      OperatorContext operatorContext =
          context
              .getInstanceContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  LastQuerySortOperator.class.getSimpleName());
      context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
      return new LastQuerySortOperator(operatorContext, builder.build(), operatorList, comparator);
    }
  }

  @Override
  public Operator visitLastQueryMerge(LastQueryMergeNode node, LocalExecutionPlanContext context) {
    List<Operator> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(Collectors.toList());
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TimeJoinOperator.class.getSimpleName());

    List<SortItem> items = node.getMergeOrderParameter().getSortItemList();
    Comparator<Binary> comparator =
        (items.isEmpty() || items.get(0).getOrdering() == Ordering.ASC)
            ? ASC_BINARY_COMPARATOR
            : DESC_BINARY_COMPARATOR;

    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new LastQueryMergeOperator(operatorContext, children, comparator);
  }

  @Override
  public Operator visitLastQueryCollect(
      LastQueryCollectNode node, LocalExecutionPlanContext context) {
    List<Operator> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(Collectors.toList());
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TimeJoinOperator.class.getSimpleName());

    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new LastQueryCollectOperator(operatorContext, children);
  }

  private Map<String, List<InputLocation>> makeLayout(PlanNode node) {
    Map<String, List<InputLocation>> outputMappings = new LinkedHashMap<>();
    int tsBlockIndex = 0;
    for (PlanNode childNode : node.getChildren()) {
      int valueColumnIndex = 0;
      for (String columnName : childNode.getOutputColumnNames()) {
        outputMappings
            .computeIfAbsent(columnName, key -> new ArrayList<>())
            .add(new InputLocation(tsBlockIndex, valueColumnIndex));
        valueColumnIndex++;
      }
      tsBlockIndex++;
    }
    return outputMappings;
  }

  private List<TSDataType> getInputColumnTypes(PlanNode node, TypeProvider typeProvider) {
    return node.getChildren().stream()
        .map(PlanNode::getOutputColumnNames)
        .flatMap(List::stream)
        .map(typeProvider::getType)
        .collect(Collectors.toList());
  }

  private List<TSDataType> getOutputColumnTypes(PlanNode node, TypeProvider typeProvider) {
    return node.getOutputColumnNames().stream()
        .map(typeProvider::getType)
        .collect(Collectors.toList());
  }

  private Operator generateOnlyChildOperator(PlanNode node, LocalExecutionPlanContext context) {
    List<Operator> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(Collectors.toList());
    Validate.isTrue(children.size() == 1);
    return children.get(0);
  }

  public Operator visitPathsUsingTemplateScan(
      PathsUsingTemplateScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getInstanceContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                PathsUsingTemplateScanNode.class.getSimpleName());
    context.getTimeSliceAllocator().recordExecutionWeight(operatorContext, 1);
    return new PathsUsingTemplateScanOperator(
        node.getPlanNodeId(), operatorContext, node.getTemplateId());
  }
}
