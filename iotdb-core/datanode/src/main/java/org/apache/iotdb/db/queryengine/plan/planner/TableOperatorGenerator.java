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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITableTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.TableDateBinTimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.TableSingleTimeWindowIterator;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.operator.ExplainAnalyzeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.CollectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.EnforceSingleRowOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.FilterAndProjectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.OffsetOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.PreviousFillWithGroupOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableFillOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableLinearFillOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableLinearFillWithGroupOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableMergeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableStreamSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableTopKOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.IFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.BinaryConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.BooleanConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.DoubleConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.FloatConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.IntConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.fill.constant.LongConstantFill;
import org.apache.iotdb.db.queryengine.execution.operator.process.gapfill.GapFillWGroupWMoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.gapfill.GapFillWGroupWoMoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.gapfill.GapFillWoGroupWMoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.gapfill.GapFillWoGroupWoMoOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.SimpleNestedLoopCrossJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator.JoinKeyComparatorFactory;
import org.apache.iotdb.db.queryengine.execution.operator.schema.CountMergeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaCountOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaQueryScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.DevicePredicateFilter;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.SchemaSourceFactory;
import org.apache.iotdb.db.queryengine.execution.operator.sink.IdentitySinkOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.InformationSchemaTableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.MergeSortFullOuterJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.MergeSortInnerJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableAggregationTableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.GroupedAggregator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.HashAggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.StreamingAggregationOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.StreamingHashAggregationOperator;
import org.apache.iotdb.db.queryengine.execution.relational.ColumnTransformerBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertPredicateToTimeFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToBlobLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToBooleanLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToDateLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToDoubleLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToFloatLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToInt32LiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToInt64LiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToStringLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.CastToTimestampLiteralVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.BinaryType;
import org.apache.tsfile.read.common.type.BlobType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.MEASUREMENT;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.getAggregationTypeByFuncName;
import static org.apache.iotdb.db.queryengine.common.DataNodeEndPoints.isSameNode;
import static org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.InformationSchemaContentSupplierFactory.getSupplier;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.TIME_COLUMN_TEMPLATE;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AccumulatorFactory.createAccumulator;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.AccumulatorFactory.createGroupedAccumulator;
import static org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.convertPredicateToFilter;
import static org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator.IDENTITY_FILL;
import static org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator.UNKNOWN_DATATYPE;
import static org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator.getLinearFill;
import static org.apache.iotdb.db.queryengine.plan.planner.OperatorTreeGenerator.getPreviousFill;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GlobalTimePredicateExtractVisitor.isTimeColumn;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.utils.constant.SqlConstant.AVG;
import static org.apache.iotdb.db.utils.constant.SqlConstant.COUNT;
import static org.apache.iotdb.db.utils.constant.SqlConstant.EXTREME;
import static org.apache.iotdb.db.utils.constant.SqlConstant.FIRST_AGGREGATION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.FIRST_BY_AGGREGATION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.LAST_AGGREGATION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.LAST_BY_AGGREGATION;
import static org.apache.iotdb.db.utils.constant.SqlConstant.MAX;
import static org.apache.iotdb.db.utils.constant.SqlConstant.MIN;
import static org.apache.iotdb.db.utils.constant.SqlConstant.SUM;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

/** This Visitor is responsible for transferring Table PlanNode Tree to Table Operator Tree. */
public class TableOperatorGenerator extends PlanVisitor<Operator, LocalExecutionPlanContext> {

  private final Metadata metadata;

  public TableOperatorGenerator(Metadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public Operator visitPlan(PlanNode node, LocalExecutionPlanContext context) {
    throw new UnsupportedOperationException("should call the concrete visitXX() method");
  }

  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
      MPPDataExchangeService.getInstance().getMPPDataExchangeManager();

  @Override
  public Operator visitIdentitySink(IdentitySinkNode node, LocalExecutionPlanContext context) {
    context.addExchangeSumNum(1);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                IdentitySinkOperator.class.getSimpleName());

    checkArgument(
        MPP_DATA_EXCHANGE_MANAGER != null, "MPP_DATA_EXCHANGE_MANAGER should not be null");
    FragmentInstanceId localInstanceId = context.getInstanceContext().getId();
    DownStreamChannelIndex downStreamChannelIndex = new DownStreamChannelIndex(0);
    ISinkHandle sinkHandle =
        MPP_DATA_EXCHANGE_MANAGER.createShuffleSinkHandle(
            node.getDownStreamChannelLocationList(),
            downStreamChannelIndex,
            ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
            localInstanceId.toThrift(),
            node.getPlanNodeId().getId(),
            context.getInstanceContext());
    sinkHandle.setMaxBytesCanReserve(context.getMaxBytesOneHandleCanReserve());
    context.getDriverContext().setSink(sinkHandle);

    if (node.getChildren().size() == 1) {
      Operator child = node.getChildren().get(0).accept(this, context);
      List<Operator> children = new ArrayList<>(1);
      children.add(child);
      return new IdentitySinkOperator(
          operatorContext, children, downStreamChannelIndex, sinkHandle);
    } else {
      throw new IllegalStateException(
          "IdentitySinkNode should only have one child in table model.");
    }
  }

  @Override
  public Operator visitTableExchange(ExchangeNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ExchangeOperator.class.getSimpleName());

    FragmentInstanceId localInstanceId = context.getInstanceContext().getId();
    FragmentInstanceId remoteInstanceId = node.getUpstreamInstanceId();

    TEndPoint upstreamEndPoint = node.getUpstreamEndpoint();
    boolean isSameNode = isSameNode(upstreamEndPoint);
    ISourceHandle sourceHandle =
        isSameNode
            ? MPP_DATA_EXCHANGE_MANAGER.createLocalSourceHandleForFragment(
                localInstanceId.toThrift(),
                node.getPlanNodeId().getId(),
                node.getUpstreamPlanNodeId().getId(),
                remoteInstanceId.toThrift(),
                node.getIndexOfUpstreamSinkHandle(),
                context.getInstanceContext()::failed)
            : MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
                localInstanceId.toThrift(),
                node.getPlanNodeId().getId(),
                node.getIndexOfUpstreamSinkHandle(),
                upstreamEndPoint,
                remoteInstanceId.toThrift(),
                context.getInstanceContext()::failed);
    if (!isSameNode) {
      context.addExchangeSumNum(1);
    }
    sourceHandle.setMaxBytesCanReserve(context.getMaxBytesOneHandleCanReserve());
    ExchangeOperator exchangeOperator =
        new ExchangeOperator(operatorContext, sourceHandle, node.getUpstreamPlanNodeId());
    context.addExchangeOperator(exchangeOperator);
    return exchangeOperator;
  }

  @Override
  public Operator visitDeviceTableScan(
      DeviceTableScanNode node, LocalExecutionPlanContext context) {

    List<Symbol> outputColumnNames = node.getOutputSymbols();
    int outputColumnCount = outputColumnNames.size();
    List<ColumnSchema> columnSchemas = new ArrayList<>(outputColumnCount);
    int[] columnsIndexArray = new int[outputColumnCount];
    Map<Symbol, ColumnSchema> columnSchemaMap = node.getAssignments();
    Map<Symbol, Integer> idAndAttributeColumnsIndexMap = node.getIdAndAttributeIndexMap();
    List<String> measurementColumnNames = new ArrayList<>();
    Map<String, Integer> measurementColumnsIndexMap = new HashMap<>();
    String timeColumnName = null;
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    int measurementColumnCount = 0;
    int idx = 0;
    for (Symbol columnName : outputColumnNames) {
      ColumnSchema schema =
          requireNonNull(columnSchemaMap.get(columnName), columnName + " is null");

      switch (schema.getColumnCategory()) {
        case ID:
        case ATTRIBUTE:
          columnsIndexArray[idx++] =
              requireNonNull(
                  idAndAttributeColumnsIndexMap.get(columnName), columnName + " is null");
          columnSchemas.add(schema);
          break;
        case MEASUREMENT:
          columnsIndexArray[idx++] = measurementColumnCount;
          measurementColumnCount++;
          measurementColumnNames.add(schema.getName());
          measurementSchemas.add(
              new MeasurementSchema(schema.getName(), getTSDataType(schema.getType())));
          columnSchemas.add(schema);
          measurementColumnsIndexMap.put(columnName.getName(), measurementColumnCount - 1);
          break;
        case TIME:
          columnsIndexArray[idx++] = -1;
          columnSchemas.add(schema);
          timeColumnName = columnName.getName();
          break;
        default:
          throw new IllegalArgumentException(
              "Unexpected column category: " + schema.getColumnCategory());
      }
    }

    Set<Symbol> outputSet = new HashSet<>(outputColumnNames);
    for (Map.Entry<Symbol, ColumnSchema> entry : node.getAssignments().entrySet()) {
      if (!outputSet.contains(entry.getKey())
          && entry.getValue().getColumnCategory() == MEASUREMENT) {
        measurementColumnCount++;
        measurementColumnNames.add(entry.getValue().getName());
        measurementSchemas.add(
            new MeasurementSchema(
                entry.getValue().getName(), getTSDataType(entry.getValue().getType())));
        measurementColumnsIndexMap.put(entry.getKey().getName(), measurementColumnCount - 1);
      } else if (entry.getValue().getColumnCategory() == TIME) {
        timeColumnName = entry.getKey().getName();
      }
    }

    SeriesScanOptions.Builder scanOptionsBuilder =
        node.getTimePredicate().isPresent()
            ? getSeriesScanOptionsBuilder(context, node.getTimePredicate().get())
            : new SeriesScanOptions.Builder();
    scanOptionsBuilder.withPushDownLimit(node.getPushDownLimit());
    scanOptionsBuilder.withPushDownOffset(node.getPushDownOffset());
    scanOptionsBuilder.withPushLimitToEachDevice(node.isPushLimitToEachDevice());
    scanOptionsBuilder.withAllSensors(new HashSet<>(measurementColumnNames));

    Expression pushDownPredicate = node.getPushDownPredicate();
    if (pushDownPredicate != null) {
      scanOptionsBuilder.withPushDownFilter(
          convertPredicateToFilter(
              pushDownPredicate, measurementColumnsIndexMap, columnSchemaMap, timeColumnName));
    }

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableScanOperator.class.getSimpleName());

    int maxTsBlockLineNum = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    if (context.getTypeProvider().getTemplatedInfo() != null) {
      maxTsBlockLineNum =
          (int)
              Math.min(
                  context.getTypeProvider().getTemplatedInfo().getLimitValue(), maxTsBlockLineNum);
    }

    Set<String> allSensors = new HashSet<>(measurementColumnNames);
    // for time column
    allSensors.add("");
    TableScanOperator tableScanOperator =
        new TableScanOperator(
            operatorContext,
            node.getPlanNodeId(),
            columnSchemas,
            columnsIndexArray,
            node.getDeviceEntries(),
            node.getScanOrder(),
            scanOptionsBuilder.build(),
            measurementColumnNames,
            allSensors,
            measurementSchemas,
            maxTsBlockLineNum);

    ((DataDriverContext) context.getDriverContext()).addSourceOperator(tableScanOperator);

    for (int i = 0, size = node.getDeviceEntries().size(); i < size; i++) {
      AlignedFullPath alignedPath =
          constructAlignedPath(
              node.getDeviceEntries().get(i),
              measurementColumnNames,
              measurementSchemas,
              allSensors);
      ((DataDriverContext) context.getDriverContext()).addPath(alignedPath);
    }

    context.getDriverContext().setInputDriver(true);

    return tableScanOperator;
  }

  public static Map<Symbol, List<InputLocation>> makeLayout(final List<PlanNode> children) {
    final Map<Symbol, List<InputLocation>> outputMappings = new LinkedHashMap<>();
    int tsBlockIndex = 0;
    for (final PlanNode childNode : children) {
      int valueColumnIndex = 0;
      for (final Symbol columnName : childNode.getOutputSymbols()) {
        outputMappings
            .computeIfAbsent(columnName, key -> new ArrayList<>())
            .add(new InputLocation(tsBlockIndex, valueColumnIndex));
        valueColumnIndex++;
      }
      tsBlockIndex++;
    }
    return outputMappings;
  }

  private ImmutableMap<Symbol, Integer> makeLayoutFromOutputSymbols(List<Symbol> outputSymbols) {
    ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
    int channel = 0;
    for (Symbol symbol : outputSymbols) {
      outputMappings.put(symbol, channel);
      channel++;
    }
    return outputMappings.buildOrThrow();
  }

  private SeriesScanOptions.Builder getSeriesScanOptionsBuilder(
      LocalExecutionPlanContext context, @NotNull Expression timePredicate) {
    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();

    Filter timeFilter = timePredicate.accept(new ConvertPredicateToTimeFilterVisitor(), null);
    context.getDriverContext().getFragmentInstanceContext().setTimeFilterForTableModel(timeFilter);
    // time filter may be stateful, so we need to copy it
    scanOptionsBuilder.withGlobalTimeFilter(timeFilter.copy());

    return scanOptionsBuilder;
  }

  @Override
  public Operator visitInformationSchemaTableScan(
      InformationSchemaTableScanNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                InformationSchemaTableScanOperator.class.getSimpleName());

    List<TSDataType> dataTypes =
        node.getOutputSymbols().stream()
            .map(symbol -> getTSDataType(context.getTypeProvider().getTableModelType(symbol)))
            .collect(Collectors.toList());

    return new InformationSchemaTableScanOperator(
        operatorContext,
        node.getPlanNodeId(),
        getSupplier(node.getQualifiedObjectName().getObjectName(), dataTypes));
  }

  @Override
  public Operator visitFilter(FilterNode node, LocalExecutionPlanContext context) {
    TypeProvider typeProvider = context.getTypeProvider();
    Optional<Expression> predicate = Optional.of(node.getPredicate());
    Operator inputOperator = node.getChild().accept(this, context);
    List<TSDataType> inputDataTypes = getInputColumnTypes(node, typeProvider);
    Map<Symbol, List<InputLocation>> inputLocations = makeLayout(node.getChildren());

    return constructFilterAndProjectOperator(
        predicate,
        inputOperator,
        node.getOutputSymbols().stream().map(Symbol::toSymbolReference).toArray(Expression[]::new),
        inputDataTypes,
        inputLocations,
        node.getPlanNodeId(),
        context);
  }

  private Operator constructFilterAndProjectOperator(
      Optional<Expression> predicate,
      Operator inputOperator,
      Expression[] projectExpressions,
      List<TSDataType> inputDataTypes,
      Map<Symbol, List<InputLocation>> inputLocations,
      PlanNodeId planNodeId,
      LocalExecutionPlanContext context) {

    final List<TSDataType> filterOutputDataTypes = new ArrayList<>(inputDataTypes);

    // records LeafColumnTransformer of filter
    List<LeafColumnTransformer> filterLeafColumnTransformerList = new ArrayList<>();

    // records subexpression -> ColumnTransformer for filter
    Map<Expression, ColumnTransformer> filterExpressionColumnTransformerMap = new HashMap<>();

    ColumnTransformerBuilder visitor = new ColumnTransformerBuilder();

    ColumnTransformer filterOutputTransformer =
        predicate
            .map(
                p -> {
                  ColumnTransformerBuilder.Context filterColumnTransformerContext =
                      new ColumnTransformerBuilder.Context(
                          context.getDriverContext().getFragmentInstanceContext().getSessionInfo(),
                          filterLeafColumnTransformerList,
                          inputLocations,
                          filterExpressionColumnTransformerMap,
                          ImmutableMap.of(),
                          ImmutableList.of(),
                          ImmutableList.of(),
                          0,
                          context.getTypeProvider(),
                          metadata);

                  return visitor.process(p, filterColumnTransformerContext);
                })
            .orElse(null);

    // records LeafColumnTransformer of project expressions
    List<LeafColumnTransformer> projectLeafColumnTransformerList = new ArrayList<>();

    List<ColumnTransformer> projectOutputTransformerList = new ArrayList<>();

    Map<Expression, ColumnTransformer> projectExpressionColumnTransformerMap = new HashMap<>();

    // records common ColumnTransformer between filter and project expressions
    List<ColumnTransformer> commonTransformerList = new ArrayList<>();

    ColumnTransformerBuilder.Context projectColumnTransformerContext =
        new ColumnTransformerBuilder.Context(
            context.getDriverContext().getFragmentInstanceContext().getSessionInfo(),
            projectLeafColumnTransformerList,
            inputLocations,
            projectExpressionColumnTransformerMap,
            filterExpressionColumnTransformerMap,
            commonTransformerList,
            filterOutputDataTypes,
            inputLocations.size(),
            context.getTypeProvider(),
            metadata);

    for (Expression expression : projectExpressions) {
      projectOutputTransformerList.add(
          visitor.process(expression, projectColumnTransformerContext));
    }

    final OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                planNodeId,
                FilterAndProjectOperator.class.getSimpleName());

    // Project expressions don't contain Non-Mappable UDF, TransformOperator is not needed
    return new FilterAndProjectOperator(
        operatorContext,
        inputOperator,
        filterOutputDataTypes,
        filterLeafColumnTransformerList,
        filterOutputTransformer,
        commonTransformerList,
        projectLeafColumnTransformerList,
        projectOutputTransformerList,
        false,
        predicate.isPresent());
  }

  @Override
  public Operator visitProject(ProjectNode node, LocalExecutionPlanContext context) {
    TypeProvider typeProvider = context.getTypeProvider();
    Optional<Expression> predicate;
    Operator inputOperator;
    List<TSDataType> inputDataTypes;
    Map<Symbol, List<InputLocation>> inputLocations;
    if (node.getChild() instanceof FilterNode) {
      FilterNode filterNode = (FilterNode) node.getChild();
      predicate = Optional.of(filterNode.getPredicate());
      inputOperator = filterNode.getChild().accept(this, context);
      inputDataTypes = getInputColumnTypes(filterNode, typeProvider);
      inputLocations = makeLayout(filterNode.getChildren());
    } else {
      predicate = Optional.empty();
      inputOperator = node.getChild().accept(this, context);
      inputDataTypes = getInputColumnTypes(node, typeProvider);
      inputLocations = makeLayout(node.getChildren());
    }

    return constructFilterAndProjectOperator(
        predicate,
        inputOperator,
        node.getAssignments().getMap().values().toArray(new Expression[0]),
        inputDataTypes,
        inputLocations,
        node.getPlanNodeId(),
        context);
  }

  private List<TSDataType> getInputColumnTypes(PlanNode node, TypeProvider typeProvider) {
    // ignore "time" column
    return node.getChildren().stream()
        .map(PlanNode::getOutputSymbols)
        .flatMap(List::stream)
        .map(s -> getTSDataType(typeProvider.getTableModelType(s)))
        .collect(Collectors.toList());
  }

  @Override
  public Operator visitGapFill(GapFillNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());
    int timeColumnIndex = getColumnIndex(node.getGapFillColumn(), node.getChild());
    if (node.getGapFillGroupingKeys().isEmpty()) { // without group keys
      if (node.getMonthDuration() == 0) { // without month interval
        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    GapFillWoGroupWoMoOperator.class.getSimpleName());
        return new GapFillWoGroupWoMoOperator(
            operatorContext,
            child,
            timeColumnIndex,
            node.getStartTime(),
            node.getEndTime(),
            inputDataTypes,
            node.getNonMonthDuration());
      } else { // with month interval
        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    GapFillWoGroupWMoOperator.class.getSimpleName());
        return new GapFillWoGroupWMoOperator(
            operatorContext,
            child,
            timeColumnIndex,
            node.getStartTime(),
            node.getEndTime(),
            inputDataTypes,
            node.getMonthDuration(),
            context.getZoneId());
      }

    } else { // with group keys
      Set<Integer> groupingKeysIndexSet = new HashSet<>();
      Comparator<SortKey> groupKeyComparator =
          genFillGroupKeyComparator(
              node.getGapFillGroupingKeys(), node, inputDataTypes, groupingKeysIndexSet);
      if (node.getMonthDuration() == 0) { // without month interval
        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    GapFillWGroupWoMoOperator.class.getSimpleName());
        return new GapFillWGroupWoMoOperator(
            operatorContext,
            child,
            timeColumnIndex,
            node.getStartTime(),
            node.getEndTime(),
            groupKeyComparator,
            inputDataTypes,
            groupingKeysIndexSet,
            node.getNonMonthDuration());
      } else { // with month interval
        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    GapFillWGroupWMoOperator.class.getSimpleName());
        return new GapFillWGroupWMoOperator(
            operatorContext,
            child,
            timeColumnIndex,
            node.getStartTime(),
            node.getEndTime(),
            groupKeyComparator,
            inputDataTypes,
            groupingKeysIndexSet,
            node.getMonthDuration(),
            context.getZoneId());
      }
    }
  }

  @Override
  public Operator visitPreviousFill(PreviousFillNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);

    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());
    int inputColumnCount = inputDataTypes.size();
    int helperColumnIndex = -1;
    if (node.getHelperColumn().isPresent()) {
      helperColumnIndex = getColumnIndex(node.getHelperColumn().get(), node.getChild());
    }
    IFill[] fillArray =
        getPreviousFill(
            inputColumnCount,
            inputDataTypes,
            node.getTimeBound().orElse(null),
            context.getZoneId());

    if (node.getGroupingKeys().isPresent()) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  PreviousFillWithGroupOperator.class.getSimpleName());
      return new PreviousFillWithGroupOperator(
          operatorContext,
          fillArray,
          child,
          helperColumnIndex,
          genFillGroupKeyComparator(
              node.getGroupingKeys().get(), node, inputDataTypes, new HashSet<>()),
          inputDataTypes);
    } else {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  TableFillOperator.class.getSimpleName());
      return new TableFillOperator(operatorContext, fillArray, child, helperColumnIndex);
    }
  }

  // used by fill and gapfill
  private Comparator<SortKey> genFillGroupKeyComparator(
      List<Symbol> groupingKeys,
      SingleChildProcessNode node,
      List<TSDataType> inputDataTypes,
      Set<Integer> groupKeysIndex) {
    int groupKeysCount = groupingKeys.size();
    List<SortOrder> sortOrderList = new ArrayList<>(groupKeysCount);
    List<Integer> groupItemIndexList = new ArrayList<>(groupKeysCount);
    List<TSDataType> groupItemDataTypeList = new ArrayList<>(groupKeysCount);
    Map<Symbol, Integer> columnIndex =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());
    for (Symbol symbol : groupingKeys) {
      // sort order for fill_group should always be ASC_NULLS_LAST, it should be same as
      // QueryPlanner.fillGroup
      sortOrderList.add(ASC_NULLS_LAST);
      int index = columnIndex.get(symbol);
      groupItemIndexList.add(index);
      groupItemDataTypeList.add(inputDataTypes.get(index));
    }
    groupKeysIndex.addAll(groupItemIndexList);
    return getComparatorForTable(sortOrderList, groupItemIndexList, groupItemDataTypeList);
  }

  // index starts from 0
  private int getColumnIndex(Symbol symbol, PlanNode node) {
    String name = symbol.getName();
    int channel = 0;
    for (Symbol columnName : node.getOutputSymbols()) {
      if (columnName.getName().equals(name)) {
        return channel;
      }
      channel++;
    }
    throw new IllegalStateException(
        String.format("Found no column %s in %s", symbol, node.getOutputSymbols()));
  }

  @Override
  public Operator visitLinearFill(LinearFillNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);

    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());
    int inputColumnCount = inputDataTypes.size();
    int helperColumnIndex = getColumnIndex(node.getHelperColumn(), node.getChild());
    ILinearFill[] fillArray = getLinearFill(inputColumnCount, inputDataTypes);

    if (node.getGroupingKeys().isPresent()) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  TableLinearFillWithGroupOperator.class.getSimpleName());
      return new TableLinearFillWithGroupOperator(
          operatorContext,
          fillArray,
          child,
          helperColumnIndex,
          genFillGroupKeyComparator(
              node.getGroupingKeys().get(), node, inputDataTypes, new HashSet<>()),
          inputDataTypes);
    } else {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  TableLinearFillOperator.class.getSimpleName());
      return new TableLinearFillOperator(operatorContext, fillArray, child, helperColumnIndex);
    }
  }

  @Override
  public Operator visitValueFill(ValueFillNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableFillOperator.class.getSimpleName());
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTypeProvider());
    int inputColumnCount = inputDataTypes.size();
    Literal filledValue = node.getFilledValue();
    return new TableFillOperator(
        operatorContext,
        getValueFill(inputColumnCount, inputDataTypes, filledValue, context),
        child,
        -1);
  }

  private IFill[] getValueFill(
      int inputColumnCount,
      List<TSDataType> inputDataTypes,
      Literal filledValue,
      LocalExecutionPlanContext context) {
    IFill[] constantFill = new IFill[inputColumnCount];
    for (int i = 0; i < inputColumnCount; i++) {
      switch (inputDataTypes.get(i)) {
        case BOOLEAN:
          Boolean bool = filledValue.accept(new CastToBooleanLiteralVisitor(), null);
          if (bool == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new BooleanConstantFill(bool);
          }
          break;
        case TEXT:
        case STRING:
          Binary binary =
              filledValue.accept(new CastToStringLiteralVisitor(TSFileConfig.STRING_CHARSET), null);
          if (binary == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new BinaryConstantFill(binary);
          }
          break;
        case BLOB:
          Binary blob = filledValue.accept(new CastToBlobLiteralVisitor(), null);
          if (blob == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new BinaryConstantFill(blob);
          }
          break;
        case INT32:
          Integer intValue = filledValue.accept(new CastToInt32LiteralVisitor(), null);
          if (intValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new IntConstantFill(intValue);
          }
          break;
        case DATE:
          Integer dateValue = filledValue.accept(new CastToDateLiteralVisitor(), null);
          if (dateValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new IntConstantFill(dateValue);
          }
          break;
        case INT64:
          Long longValue = filledValue.accept(new CastToInt64LiteralVisitor(), null);
          if (longValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new LongConstantFill(longValue);
          }
          break;
        case TIMESTAMP:
          Long timestampValue =
              filledValue.accept(new CastToTimestampLiteralVisitor(context.getZoneId()), null);
          if (timestampValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new LongConstantFill(timestampValue);
          }
          break;
        case FLOAT:
          Float floatValue = filledValue.accept(new CastToFloatLiteralVisitor(), null);
          if (floatValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new FloatConstantFill(floatValue);
          }
          break;
        case DOUBLE:
          Double doubleValue = filledValue.accept(new CastToDoubleLiteralVisitor(), null);
          if (doubleValue == null) {
            constantFill[i] = IDENTITY_FILL;
          } else {
            constantFill[i] = new DoubleConstantFill(doubleValue);
          }
          break;
        default:
          throw new IllegalArgumentException(UNKNOWN_DATATYPE + inputDataTypes.get(i));
      }
    }
    return constantFill;
  }

  @Override
  public Operator visitLimit(LimitNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                LimitOperator.class.getSimpleName());

    return new LimitOperator(operatorContext, node.getCount(), child);
  }

  @Override
  public Operator visitOffset(OffsetNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                OffsetOperator.class.getSimpleName());

    return new OffsetOperator(operatorContext, node.getCount(), child);
  }

  @Override
  public Operator visitOutput(OutputNode node, LocalExecutionPlanContext context) {
    return node.getChild().accept(this, context);
  }

  @Override
  public Operator visitCollect(CollectNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                CollectOperator.class.getSimpleName());
    List<Operator> children = new ArrayList<>(node.getChildren().size());
    for (PlanNode child : node.getChildren()) {
      children.add(this.process(child, context));
    }
    return new CollectOperator(operatorContext, children);
  }

  @Override
  public Operator visitMergeSort(MergeSortNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableMergeSortOperator.class.getSimpleName());
    List<Operator> children = new ArrayList<>(node.getChildren().size());
    for (PlanNode child : node.getChildren()) {
      children.add(this.process(child, context));
    }
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTypeProvider());

    return new TableMergeSortOperator(
        operatorContext,
        children,
        dataTypes,
        getComparatorForTable(
            node.getOrderingScheme().getOrderingList(), sortItemIndexList, sortItemDataTypeList));
  }

  @Override
  public Operator visitSort(SortNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableSortOperator.class.getSimpleName());
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTypeProvider());

    String filePrefix =
        IoTDBDescriptor.getInstance().getConfig().getSortTmpDir()
            + File.separator
            + operatorContext.getDriverContext().getFragmentInstanceContext().getId().getFullId()
            + File.separator
            + operatorContext.getDriverContext().getPipelineId()
            + File.separator;

    context.getDriverContext().setHaveTmpFile(true);
    context.getDriverContext().getFragmentInstanceContext().setMayHaveTmpFile(true);

    Operator child = node.getChild().accept(this, context);

    return new TableSortOperator(
        operatorContext,
        child,
        dataTypes,
        filePrefix,
        getComparatorForTable(
            node.getOrderingScheme().getOrderingList(), sortItemIndexList, sortItemDataTypeList));
  }

  @Override
  public Operator visitTopK(TopKNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableTopKOperator.class.getSimpleName());
    List<Operator> children = new ArrayList<>(node.getChildren().size());
    for (PlanNode child : node.getChildren()) {
      children.add(this.process(child, context));
    }
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTypeProvider());
    return new TableTopKOperator(
        operatorContext,
        children,
        dataTypes,
        getComparatorForTable(
            node.getOrderingScheme().getOrderingList(), sortItemIndexList, sortItemDataTypeList),
        (int) node.getCount(),
        node.isChildrenDataInOrder());
  }

  private List<TSDataType> getOutputColumnTypes(PlanNode node, TypeProvider typeProvider) {
    return node.getOutputSymbols().stream()
        .map(s -> getTSDataType(typeProvider.getTableModelType(s)))
        .collect(Collectors.toList());
  }

  private void genSortInformation(
      List<Symbol> outputSymbols,
      OrderingScheme orderingScheme,
      List<Integer> sortItemIndexList,
      List<TSDataType> sortItemDataTypeList,
      TypeProvider typeProvider) {
    Map<Symbol, Integer> columnIndex = new HashMap<>();
    int index = 0;
    for (Symbol symbol : outputSymbols) {
      columnIndex.put(symbol, index++);
    }
    orderingScheme
        .getOrderBy()
        .forEach(
            sortItem -> {
              Integer i = columnIndex.get(sortItem);
              if (i == null) {
                throw new IllegalStateException(
                    String.format(
                        "Sort Item %s is not included in children's output columns", sortItem));
              }
              sortItemIndexList.add(i);
              sortItemDataTypeList.add(getTSDataType(typeProvider.getTableModelType(sortItem)));
            });
  }

  @Override
  public Operator visitStreamSort(StreamSortNode node, LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableStreamSortOperator.class.getSimpleName());
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTypeProvider());

    String filePrefix =
        IoTDBDescriptor.getInstance().getConfig().getSortTmpDir()
            + File.separator
            + operatorContext.getDriverContext().getFragmentInstanceContext().getId().getFullId()
            + File.separator
            + operatorContext.getDriverContext().getPipelineId()
            + File.separator;

    context.getDriverContext().setHaveTmpFile(true);
    context.getDriverContext().getFragmentInstanceContext().setMayHaveTmpFile(true);

    Operator child = node.getChild().accept(this, context);

    return new TableStreamSortOperator(
        operatorContext,
        child,
        dataTypes,
        filePrefix,
        getComparatorForTable(
            node.getOrderingScheme().getOrderingList(), sortItemIndexList, sortItemDataTypeList),
        getComparatorForTable(
            node.getOrderingScheme()
                .getOrderingList()
                .subList(0, node.getStreamCompareKeyEndIndex() + 1),
            sortItemIndexList.subList(0, node.getStreamCompareKeyEndIndex() + 1),
            sortItemDataTypeList.subList(0, node.getStreamCompareKeyEndIndex() + 1)),
        TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber());
  }

  @Override
  public Operator visitJoin(JoinNode node, LocalExecutionPlanContext context) {
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTypeProvider());

    Operator leftChild = node.getLeftChild().accept(this, context);
    Operator rightChild = node.getRightChild().accept(this, context);

    ImmutableMap<Symbol, Integer> leftColumnNamesMap =
        makeLayoutFromOutputSymbols(node.getLeftChild().getOutputSymbols());
    int[] leftOutputSymbolIdx = new int[node.getLeftOutputSymbols().size()];
    for (int i = 0; i < leftOutputSymbolIdx.length; i++) {
      Integer index = leftColumnNamesMap.get(node.getLeftOutputSymbols().get(i));
      if (index == null) {
        throw new IllegalStateException(
            "Left child of JoinNode doesn't contain LeftOutputSymbol "
                + node.getLeftOutputSymbols().get(i));
      }
      leftOutputSymbolIdx[i] = index;
    }

    ImmutableMap<Symbol, Integer> rightColumnNamesMap =
        makeLayoutFromOutputSymbols(node.getRightChild().getOutputSymbols());
    int[] rightOutputSymbolIdx = new int[node.getRightOutputSymbols().size()];
    for (int i = 0; i < rightOutputSymbolIdx.length; i++) {
      Integer index = rightColumnNamesMap.get(node.getRightOutputSymbols().get(i));
      if (index == null) {
        throw new IllegalStateException(
            "Right child of JoinNode doesn't contain RightOutputSymbol "
                + node.getLeftOutputSymbols().get(i));
      }
      rightOutputSymbolIdx[i] = index;
    }

    // cross join does not need time column
    if (node.isCrossJoin()) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  SimpleNestedLoopCrossJoinOperator.class.getSimpleName());
      return new SimpleNestedLoopCrossJoinOperator(
          operatorContext,
          leftChild,
          rightChild,
          leftOutputSymbolIdx,
          rightOutputSymbolIdx,
          dataTypes);
    }

    Integer leftJoinKeyPosition = leftColumnNamesMap.get(node.getCriteria().get(0).getLeft());
    if (leftJoinKeyPosition == null) {
      throw new IllegalStateException("Left child of JoinNode doesn't contain left join key.");
    }

    Integer rightJoinKeyPosition = rightColumnNamesMap.get(node.getCriteria().get(0).getRight());
    if (rightJoinKeyPosition == null) {
      throw new IllegalStateException("Right child of JoinNode doesn't contain right join key.");
    }

    Type leftJoinKeyType =
        context.getTypeProvider().getTableModelType(node.getCriteria().get(0).getLeft());

    checkArgument(
        leftJoinKeyType
            == context.getTypeProvider().getTableModelType(node.getCriteria().get(0).getRight()),
        "Join key type mismatch.");

    if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.INNER) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  MergeSortInnerJoinOperator.class.getSimpleName());
      return new MergeSortInnerJoinOperator(
          operatorContext,
          leftChild,
          leftJoinKeyPosition,
          leftOutputSymbolIdx,
          rightChild,
          rightJoinKeyPosition,
          rightOutputSymbolIdx,
          JoinKeyComparatorFactory.getComparator(leftJoinKeyType, true),
          dataTypes,
          leftJoinKeyType);
    } else if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.FULL) {
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  MergeSortFullOuterJoinOperator.class.getSimpleName());
      return new MergeSortFullOuterJoinOperator(
          operatorContext,
          leftChild,
          leftJoinKeyPosition,
          leftOutputSymbolIdx,
          rightChild,
          rightJoinKeyPosition,
          rightOutputSymbolIdx,
          JoinKeyComparatorFactory.getComparator(leftJoinKeyType, true),
          dataTypes,
          leftJoinKeyType,
          buildUpdateLastRowFunction(leftJoinKeyType.getTypeEnum()));
    }

    throw new IllegalStateException("Unsupported join type: " + node.getJoinType());
  }

  private BiFunction<Column, Integer, TsBlock> buildUpdateLastRowFunction(TypeEnum type) {
    switch (type) {
      case INT32:
      case DATE:
        return (column, rowIndex) ->
            new TsBlock(
                1,
                TIME_COLUMN_TEMPLATE,
                new IntColumn(1, Optional.empty(), new int[] {column.getInt(rowIndex)}));
      case INT64:
      case TIMESTAMP:
        return (column, rowIndex) ->
            new TsBlock(
                1,
                TIME_COLUMN_TEMPLATE,
                new LongColumn(1, Optional.empty(), new long[] {column.getLong(rowIndex)}));
      case FLOAT:
        return (column, rowIndex) ->
            new TsBlock(
                1,
                TIME_COLUMN_TEMPLATE,
                new FloatColumn(1, Optional.empty(), new float[] {column.getFloat(rowIndex)}));
      case DOUBLE:
        return (column, rowIndex) ->
            new TsBlock(
                1,
                TIME_COLUMN_TEMPLATE,
                new DoubleColumn(1, Optional.empty(), new double[] {column.getDouble(rowIndex)}));
      case BOOLEAN:
        return (column, rowIndex) ->
            new TsBlock(
                1,
                TIME_COLUMN_TEMPLATE,
                new BooleanColumn(
                    1, Optional.empty(), new boolean[] {column.getBoolean(rowIndex)}));
      case STRING:
      case TEXT:
      case BLOB:
        return (column, rowIndex) ->
            new TsBlock(
                1,
                TIME_COLUMN_TEMPLATE,
                new BinaryColumn(1, Optional.empty(), new Binary[] {column.getBinary(rowIndex)}));
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }

  @Override
  public Operator visitEnforceSingleRow(
      EnforceSingleRowNode node, LocalExecutionPlanContext context) {
    Operator child = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                EnforceSingleRowOperator.class.getSimpleName());

    return new EnforceSingleRowOperator(operatorContext, child);
  }

  @Override
  public Operator visitCountMerge(
      final CountSchemaMergeNode node, final LocalExecutionPlanContext context) {
    final OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                CountMergeOperator.class.getSimpleName());
    final List<Operator> children = new ArrayList<>(node.getChildren().size());
    for (final PlanNode child : node.getChildren()) {
      children.add(this.process(child, context));
    }
    return new CountMergeOperator(operatorContext, children);
  }

  @Override
  public Operator visitTableDeviceFetch(
      final TableDeviceFetchNode node, final LocalExecutionPlanContext context) {
    final OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaQueryScanOperator.class.getSimpleName());
    return new SchemaQueryScanOperator<>(
        node.getPlanNodeId(),
        operatorContext,
        SchemaSourceFactory.getTableDeviceFetchSource(
            node.getDatabase(),
            node.getTableName(),
            node.getDeviceIdList(),
            node.getColumnHeaderList()));
  }

  @Override
  public Operator visitTableDeviceQueryScan(
      final TableDeviceQueryScanNode node, final LocalExecutionPlanContext context) {
    // Query scan use filterNode directly
    final SchemaQueryScanOperator<IDeviceSchemaInfo> operator =
        new SchemaQueryScanOperator<>(
            node.getPlanNodeId(),
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    SchemaQueryScanOperator.class.getSimpleName()),
            SchemaSourceFactory.getTableDeviceQuerySource(
                node.getDatabase(),
                node.getTableName(),
                node.getIdDeterminedFilterList(),
                node.getColumnHeaderList(),
                null));
    operator.setLimit(node.getLimit());
    return operator;
  }

  @Override
  public Operator visitTableDeviceQueryCount(
      final TableDeviceQueryCountNode node, final LocalExecutionPlanContext context) {
    final String database = node.getDatabase();
    final String tableName = node.getTableName();
    final List<ColumnHeader> columnHeaderList = node.getColumnHeaderList();

    // In "count" we have to reuse filter operator per "next"
    final List<LeafColumnTransformer> filterLeafColumnTransformerList = new ArrayList<>();
    return new SchemaCountOperator<>(
        node.getPlanNodeId(),
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                SchemaCountOperator.class.getSimpleName()),
        SchemaSourceFactory.getTableDeviceQuerySource(
            database,
            node.getTableName(),
            node.getIdDeterminedFilterList(),
            columnHeaderList,
            Objects.nonNull(node.getIdFuzzyPredicate())
                ? new DevicePredicateFilter(
                    filterLeafColumnTransformerList,
                    new ColumnTransformerBuilder()
                        .process(
                            node.getIdFuzzyPredicate(),
                            new ColumnTransformerBuilder.Context(
                                context
                                    .getDriverContext()
                                    .getFragmentInstanceContext()
                                    .getSessionInfo(),
                                filterLeafColumnTransformerList,
                                makeLayout(Collections.singletonList(node)),
                                new HashMap<>(),
                                ImmutableMap.of(),
                                ImmutableList.of(),
                                ImmutableList.of(),
                                0,
                                context.getTypeProvider(),
                                metadata)),
                    database,
                    tableName,
                    columnHeaderList)
                : null));
  }

  @Override
  public Operator visitAggregation(AggregationNode node, LocalExecutionPlanContext context) {

    Operator child = node.getChild().accept(this, context);

    if (node.getGroupingKeys().isEmpty()) {
      return planGlobalAggregation(node, child, context.getTypeProvider(), context);
    }

    return planGroupByAggregation(node, child, context.getTypeProvider(), context);
  }

  private Operator planGlobalAggregation(
      AggregationNode node,
      Operator child,
      TypeProvider typeProvider,
      LocalExecutionPlanContext context) {
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AggregationOperator.class.getSimpleName());
    Map<Symbol, AggregationNode.Aggregation> aggregationMap = node.getAggregations();
    ImmutableList.Builder<TableAggregator> aggregatorBuilder = new ImmutableList.Builder<>();
    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());

    node.getOutputSymbols()
        .forEach(
            symbol ->
                aggregatorBuilder.add(
                    buildAggregator(
                        childLayout,
                        symbol,
                        aggregationMap.get(symbol),
                        node.getStep(),
                        typeProvider,
                        true,
                        null)));
    return new AggregationOperator(operatorContext, child, aggregatorBuilder.build());
  }

  // timeColumnName will only be set for AggTableScan.
  private TableAggregator buildAggregator(
      Map<Symbol, Integer> childLayout,
      Symbol symbol,
      AggregationNode.Aggregation aggregation,
      AggregationNode.Step step,
      TypeProvider typeProvider,
      boolean scanAscending,
      String timeColumnName) {
    List<Integer> argumentChannels = new ArrayList<>();
    for (Expression argument : aggregation.getArguments()) {
      Symbol argumentSymbol = Symbol.from(argument);
      argumentChannels.add(childLayout.get(argumentSymbol));
    }

    String functionName = aggregation.getResolvedFunction().getSignature().getName();
    List<TSDataType> originalArgumentTypes =
        aggregation.getResolvedFunction().getSignature().getArgumentTypes().stream()
            .map(InternalTypeManager::getTSDataType)
            .collect(Collectors.toList());
    TableAccumulator accumulator =
        createAccumulator(
            functionName,
            getAggregationTypeByFuncName(functionName),
            originalArgumentTypes,
            aggregation.getArguments(),
            Collections.emptyMap(),
            scanAscending,
            timeColumnName);

    return new TableAggregator(
        accumulator,
        step,
        getTSDataType(typeProvider.getTableModelType(symbol)),
        argumentChannels,
        OptionalInt.empty());
  }

  private Operator planGroupByAggregation(
      AggregationNode node,
      Operator child,
      TypeProvider typeProvider,
      LocalExecutionPlanContext context) {
    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());

    List<Integer> groupByChannels = getChannelsForSymbols(node.getGroupingKeys(), childLayout);
    List<Type> groupByTypes =
        node.getGroupingKeys().stream()
            .map(typeProvider::getTableModelType)
            .collect(toImmutableList());

    if (node.isStreamable()) {
      if (groupByTypes.size() == node.getPreGroupedSymbols().size()) {
        ImmutableList.Builder<TableAggregator> aggregatorBuilder = new ImmutableList.Builder<>();
        node.getAggregations()
            .forEach(
                (k, v) ->
                    aggregatorBuilder.add(
                        buildAggregator(
                            childLayout, k, v, node.getStep(), typeProvider, true, null)));

        OperatorContext operatorContext =
            context
                .getDriverContext()
                .addOperatorContext(
                    context.getNextOperatorId(),
                    node.getPlanNodeId(),
                    StreamingAggregationOperator.class.getSimpleName());
        return new StreamingAggregationOperator(
            operatorContext,
            child,
            groupByTypes,
            groupByChannels,
            genGroupKeyComparator(groupByTypes, groupByChannels),
            aggregatorBuilder.build(),
            Long.MAX_VALUE,
            false,
            Long.MAX_VALUE);
      }

      ImmutableList.Builder<GroupedAggregator> aggregatorBuilder = new ImmutableList.Builder<>();
      node.getAggregations()
          .forEach(
              (k, v) ->
                  aggregatorBuilder.add(
                      buildGroupByAggregator(childLayout, k, v, node.getStep(), typeProvider)));

      Set<Symbol> preGroupedKeys = ImmutableSet.copyOf(node.getPreGroupedSymbols());
      List<Symbol> groupingKeys = node.getGroupingKeys();
      ImmutableList.Builder<Type> preGroupedTypesBuilder = new ImmutableList.Builder<>();
      ImmutableList.Builder<Integer> preGroupedChannelsBuilder = new ImmutableList.Builder<>();
      ImmutableList.Builder<Integer> preGroupedIndexInResultBuilder = new ImmutableList.Builder<>();
      ImmutableList.Builder<Type> unPreGroupedTypesBuilder = new ImmutableList.Builder<>();
      ImmutableList.Builder<Integer> unPreGroupedChannelsBuilder = new ImmutableList.Builder<>();
      ImmutableList.Builder<Integer> unPreGroupedIndexInResultBuilder =
          new ImmutableList.Builder<>();
      for (int i = 0; i < groupByTypes.size(); i++) {
        if (preGroupedKeys.contains(groupingKeys.get(i))) {
          preGroupedTypesBuilder.add(groupByTypes.get(i));
          preGroupedChannelsBuilder.add(groupByChannels.get(i));
          preGroupedIndexInResultBuilder.add(i);
        } else {
          unPreGroupedTypesBuilder.add(groupByTypes.get(i));
          unPreGroupedChannelsBuilder.add(groupByChannels.get(i));
          unPreGroupedIndexInResultBuilder.add(i);
        }
      }

      List<Integer> preGroupedChannels = preGroupedChannelsBuilder.build();
      OperatorContext operatorContext =
          context
              .getDriverContext()
              .addOperatorContext(
                  context.getNextOperatorId(),
                  node.getPlanNodeId(),
                  StreamingHashAggregationOperator.class.getSimpleName());
      return new StreamingHashAggregationOperator(
          operatorContext,
          child,
          preGroupedChannels,
          preGroupedIndexInResultBuilder.build(),
          unPreGroupedTypesBuilder.build(),
          unPreGroupedChannelsBuilder.build(),
          unPreGroupedIndexInResultBuilder.build(),
          genGroupKeyComparator(preGroupedTypesBuilder.build(), preGroupedChannels),
          aggregatorBuilder.build(),
          node.getStep(),
          64,
          Long.MAX_VALUE,
          false,
          Long.MAX_VALUE);
    }

    ImmutableList.Builder<GroupedAggregator> aggregatorBuilder = new ImmutableList.Builder<>();
    node.getAggregations()
        .forEach(
            (k, v) ->
                aggregatorBuilder.add(
                    buildGroupByAggregator(childLayout, k, v, node.getStep(), typeProvider)));
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                HashAggregationOperator.class.getSimpleName());

    return new HashAggregationOperator(
        operatorContext,
        child,
        groupByTypes,
        groupByChannels,
        aggregatorBuilder.build(),
        node.getStep(),
        64,
        Long.MAX_VALUE,
        false,
        Long.MAX_VALUE);
  }

  private Comparator<SortKey> genGroupKeyComparator(
      List<Type> groupTypes, List<Integer> groupByChannels) {
    return getComparatorForTable(
        // SortOrder is not sensitive here, the comparator is just used to judge equality.
        groupTypes.stream().map(k -> ASC_NULLS_LAST).collect(Collectors.toList()),
        groupByChannels,
        groupTypes.stream().map(InternalTypeManager::getTSDataType).collect(Collectors.toList()));
  }

  private static List<Integer> getChannelsForSymbols(
      List<Symbol> symbols, Map<Symbol, Integer> layout) {
    ImmutableList.Builder<Integer> builder = ImmutableList.builder();
    for (Symbol symbol : symbols) {
      builder.add(layout.get(symbol));
    }
    return builder.build();
  }

  private GroupedAggregator buildGroupByAggregator(
      Map<Symbol, Integer> childLayout,
      Symbol symbol,
      AggregationNode.Aggregation aggregation,
      AggregationNode.Step step,
      TypeProvider typeProvider) {
    List<Integer> argumentChannels = new ArrayList<>();
    for (Expression argument : aggregation.getArguments()) {
      Symbol argumentSymbol = Symbol.from(argument);
      argumentChannels.add(childLayout.get(argumentSymbol));
    }

    String functionName = aggregation.getResolvedFunction().getSignature().getName();
    List<TSDataType> originalArgumentTypes =
        aggregation.getResolvedFunction().getSignature().getArgumentTypes().stream()
            .map(InternalTypeManager::getTSDataType)
            .collect(Collectors.toList());
    GroupedAccumulator accumulator =
        createGroupedAccumulator(
            functionName,
            getAggregationTypeByFuncName(functionName),
            originalArgumentTypes,
            Collections.emptyList(),
            Collections.emptyMap(),
            true);

    return new GroupedAggregator(
        accumulator,
        step,
        getTSDataType(typeProvider.getTableModelType(symbol)),
        argumentChannels,
        OptionalInt.empty());
  }

  @Override
  public Operator visitAggregationTableScan(
      AggregationTableScanNode node, LocalExecutionPlanContext context) {

    List<TableAggregator> aggregators = new ArrayList<>(node.getAggregations().size());
    Map<Symbol, Integer> columnLayout = new HashMap<>(node.getAggregations().size());

    int distinctArgumentCount = node.getAssignments().size();
    int aggregationsCount = node.getAggregations().size();
    List<Integer> aggColumnIndexes = new ArrayList<>();
    int channel = 0;
    int idx = -1;
    int measurementColumnCount = 0;
    Map<Symbol, Integer> idAndAttributeColumnsIndexMap = node.getIdAndAttributeIndexMap();
    Map<Symbol, ColumnSchema> columnSchemaMap = node.getAssignments();
    List<ColumnSchema> columnSchemas = new ArrayList<>(aggregationsCount);
    int[] columnsIndexArray = new int[distinctArgumentCount];
    List<String> measurementColumnNames = new ArrayList<>();
    Map<String, Integer> measurementColumnsIndexMap = new HashMap<>();
    String timeColumnName = null;
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      AggregationNode.Aggregation aggregation = entry.getValue();

      for (Expression argument : aggregation.getArguments()) {
        idx++;
        Symbol symbol = Symbol.from(argument);
        ColumnSchema schema = requireNonNull(columnSchemaMap.get(symbol), symbol + " is null");
        switch (schema.getColumnCategory()) {
          case ID:
          case ATTRIBUTE:
            if (!columnLayout.containsKey(symbol)) {
              columnsIndexArray[channel] =
                  requireNonNull(idAndAttributeColumnsIndexMap.get(symbol), symbol + " is null");
              columnSchemas.add(schema);
            }
            break;
          case MEASUREMENT:
            if (!columnLayout.containsKey(symbol)) {
              columnsIndexArray[channel] = measurementColumnCount;
              measurementColumnCount++;
              measurementColumnNames.add(schema.getName());
              measurementSchemas.add(
                  new MeasurementSchema(schema.getName(), getTSDataType(schema.getType())));
              columnSchemas.add(schema);
              measurementColumnsIndexMap.put(symbol.getName(), measurementColumnCount - 1);
            }
            break;
          case TIME:
            if (!columnLayout.containsKey(symbol)) {
              columnsIndexArray[channel] = -1;
              columnSchemas.add(schema);
              timeColumnName = symbol.getName();
            }
            break;
          default:
            throw new IllegalArgumentException(
                "Unexpected column category: " + schema.getColumnCategory());
        }

        if (!columnLayout.containsKey(symbol)) {
          aggColumnIndexes.add(channel);
          columnLayout.put(symbol, channel++);
        } else {
          aggColumnIndexes.add(columnLayout.get(symbol));
        }
      }
    }

    for (Map.Entry<Symbol, ColumnSchema> entry : node.getAssignments().entrySet()) {
      if (!columnLayout.containsKey(entry.getKey())
          && entry.getValue().getColumnCategory() == MEASUREMENT) {
        measurementColumnCount++;
        measurementColumnNames.add(entry.getValue().getName());
        measurementSchemas.add(
            new MeasurementSchema(
                entry.getValue().getName(), getTSDataType(entry.getValue().getType())));
        measurementColumnsIndexMap.put(entry.getKey().getName(), measurementColumnCount - 1);
      } else if (entry.getValue().getColumnCategory() == TIME) {
        timeColumnName = entry.getKey().getName();
      }
    }

    boolean[] ret = checkStatisticAndScanOrder(node, timeColumnName);
    boolean canUseStatistic = ret[0];
    boolean scanAscending = ret[1];

    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      aggregators.add(
          buildAggregator(
              columnLayout,
              entry.getKey(),
              entry.getValue(),
              node.getStep(),
              context.getTypeProvider(),
              scanAscending,
              timeColumnName));
    }

    ITableTimeRangeIterator timeRangeIterator = null;
    List<ColumnSchema> groupingKeySchemas = null;
    int[] groupingKeyIndex = null;
    if (!node.getGroupingKeys().isEmpty()) {
      groupingKeySchemas = new ArrayList<>(node.getGroupingKeys().size());
      groupingKeyIndex = new int[node.getGroupingKeys().size()];
      for (int i = 0; i < node.getGroupingKeys().size(); i++) {
        Symbol groupingKey = node.getGroupingKeys().get(i);

        if (idAndAttributeColumnsIndexMap.containsKey(groupingKey)) {
          groupingKeySchemas.add(columnSchemaMap.get(groupingKey));
          groupingKeyIndex[i] = idAndAttributeColumnsIndexMap.get(groupingKey);
        } else {
          if (node.getProjection() != null
              && !node.getProjection().getMap().isEmpty()
              && node.getProjection().contains(groupingKey)) {
            FunctionCall dateBinFunc = (FunctionCall) node.getProjection().get(groupingKey);
            List<Expression> arguments = dateBinFunc.getArguments();
            DateBinFunctionColumnTransformer dateBinTransformer =
                new DateBinFunctionColumnTransformer(
                    TIMESTAMP,
                    ((LongLiteral) arguments.get(0)).getParsedValue(),
                    ((LongLiteral) arguments.get(1)).getParsedValue(),
                    null,
                    ((LongLiteral) arguments.get(3)).getParsedValue(),
                    context.getZoneId());
            timeRangeIterator = new TableDateBinTimeRangeIterator(dateBinTransformer);
          } else {
            throw new IllegalStateException(
                "grouping key must be ID or Attribute in AggregationTableScan");
          }
        }
      }
    }
    if (timeRangeIterator == null) {
      if (node.getGroupingKeys().isEmpty()) {
        // global aggregation, has no group by, output init value if no data
        timeRangeIterator =
            new TableSingleTimeWindowIterator(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
      } else {
        // aggregation with group by, only has data the result will not be empty
        timeRangeIterator = new TableSingleTimeWindowIterator();
      }
    }

    final OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                TableAggregationTableScanOperator.class.getSimpleName());
    SeriesScanOptions.Builder scanOptionsBuilder =
        node.getTimePredicate().isPresent()
            ? getSeriesScanOptionsBuilder(context, node.getTimePredicate().get())
            : new SeriesScanOptions.Builder();
    scanOptionsBuilder.withPushDownLimit(node.getPushDownLimit());
    scanOptionsBuilder.withPushDownOffset(node.getPushDownOffset());
    scanOptionsBuilder.withPushLimitToEachDevice(node.isPushLimitToEachDevice());
    scanOptionsBuilder.withAllSensors(new HashSet<>(measurementColumnNames));
    Expression pushDownPredicate = node.getPushDownPredicate();
    if (pushDownPredicate != null) {
      scanOptionsBuilder.withPushDownFilter(
          convertPredicateToFilter(
              pushDownPredicate, measurementColumnsIndexMap, columnSchemaMap, timeColumnName));
    }

    Set<String> allSensors = new HashSet<>(measurementColumnNames);
    // for time column
    allSensors.add("");
    TableAggregationTableScanOperator aggTableScanOperator =
        new TableAggregationTableScanOperator(
            node.getPlanNodeId(),
            operatorContext,
            columnSchemas,
            columnsIndexArray,
            node.getDeviceEntries(),
            scanAscending ? Ordering.ASC : Ordering.DESC,
            scanOptionsBuilder.build(),
            measurementColumnNames,
            allSensors,
            measurementSchemas,
            TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber(),
            measurementColumnCount,
            aggregators,
            groupingKeySchemas,
            groupingKeyIndex,
            timeRangeIterator,
            scanAscending,
            calculateMaxAggregationResultSize(),
            canUseStatistic,
            aggColumnIndexes);

    ((DataDriverContext) context.getDriverContext()).addSourceOperator(aggTableScanOperator);

    for (int i = 0, size = node.getDeviceEntries().size(); i < size; i++) {
      AlignedFullPath alignedPath =
          constructAlignedPath(
              node.getDeviceEntries().get(i),
              measurementColumnNames,
              measurementSchemas,
              allSensors);
      ((DataDriverContext) context.getDriverContext()).addPath(alignedPath);
    }

    context.getDriverContext().setInputDriver(true);

    return aggTableScanOperator;
  }

  @Override
  public Operator visitExplainAnalyze(ExplainAnalyzeNode node, LocalExecutionPlanContext context) {
    Operator operator = node.getChild().accept(this, context);
    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                ExplainAnalyzeOperator.class.getSimpleName());
    return new ExplainAnalyzeOperator(
        operatorContext, operator, node.getQueryId(), node.isVerbose(), node.getTimeout());
  }

  private boolean[] checkStatisticAndScanOrder(
      AggregationTableScanNode node, String timeColumnName) {
    boolean canUseStatistic = true;
    int ascendingCount = 0, descendingCount = 0;

    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
      AggregationNode.Aggregation aggregation = entry.getValue();
      String funcName = aggregation.getResolvedFunction().getSignature().getName();
      Symbol argument = Symbol.from(aggregation.getArguments().get(0));
      Type argumentType = node.getAssignments().get(argument).getType();

      switch (funcName) {
        case COUNT:
        case AVG:
        case SUM:
        case EXTREME:
          break;
        case MAX:
        case MIN:
          if (BlobType.BLOB.equals(argumentType)
              || BinaryType.TEXT.equals(argumentType)
              || BooleanType.BOOLEAN.equals(argumentType)) {
            canUseStatistic = false;
          }
          break;
        case FIRST_AGGREGATION:
        case LAST_AGGREGATION:
        case LAST_BY_AGGREGATION:
        case FIRST_BY_AGGREGATION:
          if (FIRST_AGGREGATION.equals(funcName) || FIRST_BY_AGGREGATION.equals(funcName)) {
            ascendingCount++;
          } else {
            descendingCount++;
          }

          // first/last/first_by/last_by aggregation with BLOB type can not use statistics
          if (BlobType.BLOB.equals(argumentType)) {
            canUseStatistic = false;
            break;
          }

          // only last_by(time, x) or last_by(x,time) can use statistic
          if ((LAST_BY_AGGREGATION.equals(funcName) || FIRST_BY_AGGREGATION.equals(funcName))
              && !isTimeColumn(aggregation.getArguments().get(0), timeColumnName)
              && !isTimeColumn(aggregation.getArguments().get(1), timeColumnName)) {
            canUseStatistic = false;
          }
          break;
        default:
          canUseStatistic = false;
      }
    }

    boolean isAscending = node.getScanOrder().isAscending();
    boolean groupByDateBin = node.getProjection() != null && !node.getProjection().isEmpty();
    // only in non-groupByDateBin situation can change the scan order
    if (!groupByDateBin) {
      if (ascendingCount >= descendingCount) {
        node.setScanOrder(Ordering.ASC);
        isAscending = true;
      } else {
        node.setScanOrder(Ordering.DESC);
        isAscending = false;
      }
    }
    return new boolean[] {canUseStatistic, isAscending};
  }

  public static long calculateMaxAggregationResultSize(
      // List<? extends AggregationDescriptor> aggregationDescriptors,
      // ITimeRangeIterator timeRangeIterator
      ) {
    // TODO perfect max aggregation result size logic
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

    //    long timeValueColumnsSizePerLine = TimeColumn.SIZE_IN_BYTES_PER_POSITION;
    //    for (AggregationDescriptor descriptor : aggregationDescriptors) {
    //      List<TSDataType> outPutDataTypes =
    //              descriptor.getOutputColumnNames().stream()
    //                      .map(typeProvider::getTableModelType)
    //                      .collect(Collectors.toList());
    //      for (TSDataType tsDataType : outPutDataTypes) {
    //        timeValueColumnsSizePerLine += getOutputColumnSizePerLine(tsDataType);
    //      }
    //    }
    //
    //    return Math.min(
    //            TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes(),
    //            Math.min(
    //                    TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber(),
    //                    timeRangeIterator.getTotalIntervalNum())
    //                    * timeValueColumnsSizePerLine);
  }
}
