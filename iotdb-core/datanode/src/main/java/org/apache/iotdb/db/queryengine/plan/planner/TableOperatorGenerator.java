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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.CollectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.FilterAndProjectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.OffsetOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableMergeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableStreamSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableTopKOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.CountMergeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaCountOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.SchemaQueryScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.DevicePredicateFilter;
import org.apache.iotdb.db.queryengine.execution.operator.schema.source.SchemaSourceFactory;
import org.apache.iotdb.db.queryengine.execution.operator.sink.IdentitySinkOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.execution.relational.ColumnTransformerBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.ConvertPredicateToTimeFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.MEASUREMENT;
import static org.apache.iotdb.db.queryengine.common.DataNodeEndPoints.isSameNode;
import static org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;
import static org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.convertPredicateToFilter;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

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

    Operator child = node.getChildren().get(0).accept(this, context);
    List<Operator> children = new ArrayList<>(1);
    children.add(child);
    return new IdentitySinkOperator(operatorContext, children, downStreamChannelIndex, sinkHandle);
  }

  @Override
  public Operator visitExchange(ExchangeNode node, LocalExecutionPlanContext context) {
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
  public Operator visitTableScan(TableScanNode node, LocalExecutionPlanContext context) {

    List<Symbol> outputColumnNames = node.getOutputSymbols();
    int outputColumnCount = outputColumnNames.size();
    List<ColumnSchema> columnSchemas = new ArrayList<>(outputColumnCount);
    int[] columnsIndexArray = new int[outputColumnCount];
    Map<Symbol, ColumnSchema> columnSchemaMap = node.getAssignments();
    Map<Symbol, Integer> idAndAttributeColumnsIndexMap = node.getIdAndAttributeIndexMap();
    List<String> measurementColumnNames = new ArrayList<>();
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
          measurementColumnNames.add(columnName.getName());
          measurementSchemas.add(
              new MeasurementSchema(schema.getName(), getTSDataType(schema.getType())));
          columnSchemas.add(schema);
          break;
        case TIME:
          columnsIndexArray[idx++] = -1;
          columnSchemas.add(schema);
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
        measurementColumnNames.add(entry.getKey().getName());
        measurementSchemas.add(
            new MeasurementSchema(
                entry.getValue().getName(), getTSDataType(entry.getValue().getType())));
      }
    }

    SeriesScanOptions.Builder scanOptionsBuilder =
        node.getTimePredicate()
            .map(timePredicate -> getSeriesScanOptionsBuilder(context, timePredicate))
            .orElse(new SeriesScanOptions.Builder());
    scanOptionsBuilder.withPushDownLimit(node.getPushDownLimit());
    scanOptionsBuilder.withPushDownOffset(node.getPushDownOffset());
    scanOptionsBuilder.withPushLimitToEachDevice(node.isPushLimitToEachDevice());
    scanOptionsBuilder.withAllSensors(new HashSet<>(measurementColumnNames));

    Expression pushDownPredicate = node.getPushDownPredicate();
    if (pushDownPredicate != null) {
      scanOptionsBuilder.withPushDownFilter(
          convertPredicateToFilter(pushDownPredicate, measurementColumnNames, columnSchemaMap));
    }

    OperatorContext operatorContext =
        context
            .getDriverContext()
            .addOperatorContext(
                context.getNextOperatorId(),
                node.getPlanNodeId(),
                AlignedSeriesScanOperator.class.getSimpleName());

    int maxTsBlockLineNum = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();
    if (context.getTypeProvider().getTemplatedInfo() != null) {
      maxTsBlockLineNum =
          (int)
              Math.min(
                  context.getTypeProvider().getTemplatedInfo().getLimitValue(), maxTsBlockLineNum);
    }

    TableScanOperator tableScanOperator =
        new TableScanOperator(
            operatorContext,
            node.getPlanNodeId(),
            columnSchemas,
            columnsIndexArray,
            measurementColumnCount,
            node.getDeviceEntries(),
            node.getScanOrder(),
            scanOptionsBuilder.build(),
            measurementColumnNames,
            measurementSchemas,
            maxTsBlockLineNum);

    ((DataDriverContext) context.getDriverContext()).addSourceOperator(tableScanOperator);

    for (int i = 0, size = node.getDeviceEntries().size(); i < size; i++) {
      AlignedFullPath alignedPath =
          constructAlignedPath(
              node.getDeviceEntries().get(i), measurementColumnNames, measurementSchemas);
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
                StreamSortNode.class.getSimpleName());
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
    throw new IllegalStateException("JoinOperator is not implemented currently.");
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
    operator.setOffset(node.getOffset());
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
    throw new UnsupportedOperationException("Agg-BE not supported");
  }

  @Override
  public Operator visitAggregationTableScan(
      AggregationTableScanNode node, LocalExecutionPlanContext context) {
    throw new UnsupportedOperationException("Agg-BE not supported");
  }
}
