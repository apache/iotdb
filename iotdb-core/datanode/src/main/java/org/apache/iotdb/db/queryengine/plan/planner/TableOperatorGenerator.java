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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.FilterAndProjectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.LimitOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.OffsetOperator;
import org.apache.iotdb.db.queryengine.execution.operator.sink.IdentitySinkOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.execution.relational.ColumnTransformerBuilder;
import org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.relational.sql.tree.Expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;
import static org.apache.iotdb.db.queryengine.plan.analyze.PredicateUtils.convertPredicateToFilter;
import static org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand.TIMESTAMP_EXPRESSION_STRING;
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

    // List<Operator> children = dealWithConsumeChildrenOneByOneNode(node, context);
    Operator child = node.getChildren().get(0).accept(this, context);
    List<Operator> children = new ArrayList<>(1);
    children.add(child);
    return new IdentitySinkOperator(operatorContext, children, downStreamChannelIndex, sinkHandle);
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
    boolean hasTimeColumn = false;
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
          hasTimeColumn = true;
          // columnsIndexArray[i] = -1;
          break;
        default:
          throw new IllegalArgumentException(
              "Unexpected column category: " + schema.getColumnCategory());
      }
    }

    int[] newColumnsIndexArray = new int[outputColumnCount - 1];
    if (hasTimeColumn) {
      System.arraycopy(columnsIndexArray, 0, newColumnsIndexArray, 0, outputColumnCount - 1);
    }

    SeriesScanOptions.Builder scanOptionsBuilder = getSeriesScanOptionsBuilder(context);
    scanOptionsBuilder.withPushDownLimit(node.getPushDownLimit());
    scanOptionsBuilder.withPushDownOffset(node.getPushDownOffset());
    scanOptionsBuilder.withAllSensors(new HashSet<>(measurementColumnNames));

    Expression pushDownPredicate = node.getPushDownPredicate();
    boolean predicateCanPushIntoScan = canPushIntoScan(pushDownPredicate);
    if (pushDownPredicate != null && predicateCanPushIntoScan) {
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
            newColumnsIndexArray,
            measurementColumnCount,
            node.getDeviceEntries(),
            node.getScanOrder(),
            scanOptionsBuilder.build(),
            measurementColumnNames,
            measurementSchemas,
            maxTsBlockLineNum);

    ((DataDriverContext) context.getDriverContext()).addSourceOperator(tableScanOperator);

    for (int i = 0, size = node.getDeviceEntries().size(); i < size; i++) {
      AlignedPath alignedPath =
          constructAlignedPath(
              node.getDeviceEntries().get(i), measurementColumnNames, measurementSchemas);
      ((DataDriverContext) context.getDriverContext()).addPath(alignedPath);
    }

    context.getDriverContext().setInputDriver(true);

    if (!predicateCanPushIntoScan) {

      return constructFilterAndProjectOperator(
          Optional.of(pushDownPredicate),
          tableScanOperator,
          node.getOutputSymbols().stream()
              .filter(symbol -> !TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(symbol.getName()))
              .map(Symbol::toSymbolReference)
              .toArray(Expression[]::new),
          tableScanOperator.getResultDataTypes(),
          makeLayout(Collections.singletonList(node)),
          node.getPlanNodeId(),
          context);
    }
    return tableScanOperator;
  }

  private Map<Symbol, List<InputLocation>> makeLayout(List<PlanNode> children) {
    Map<Symbol, List<InputLocation>> outputMappings = new LinkedHashMap<>();
    int tsBlockIndex = 0;
    for (PlanNode childNode : children) {

      int valueColumnIndex = 0;
      for (Symbol columnName : childNode.getOutputSymbols()) {
        if (!TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(columnName.getName())) {
          outputMappings
              .computeIfAbsent(columnName, key -> new ArrayList<>())
              .add(new InputLocation(tsBlockIndex, valueColumnIndex));
          valueColumnIndex++;
        } else {
          outputMappings
              .computeIfAbsent(columnName, key -> new ArrayList<>())
              .add(new InputLocation(tsBlockIndex, -1));
        }
      }
      tsBlockIndex++;
    }
    return outputMappings;
  }

  private SeriesScanOptions.Builder getSeriesScanOptionsBuilder(LocalExecutionPlanContext context) {
    SeriesScanOptions.Builder scanOptionsBuilder = new SeriesScanOptions.Builder();

    Filter globalTimeFilter = context.getGlobalTimeFilter();
    if (globalTimeFilter != null) {
      // time filter may be stateful, so we need to copy it
      scanOptionsBuilder.withGlobalTimeFilter(globalTimeFilter.copy());
    }

    return scanOptionsBuilder;
  }

  private boolean canPushIntoScan(Expression pushDownPredicate) {
    return pushDownPredicate == null || PredicateUtils.predicateCanPushIntoScan(pushDownPredicate);
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
        node.getOutputSymbols().stream()
            .filter(symbol -> !TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(symbol.getName()))
            .map(Symbol::toSymbolReference)
            .toArray(Expression[]::new),
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
            inputLocations.size() - 1,
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
        node.getAssignments().getMap().entrySet().stream()
            .filter(
                entry -> !TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(entry.getKey().getName()))
            .map(Map.Entry::getValue)
            .toArray(Expression[]::new),
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
        .filter(s -> !TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(s.getName()))
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
  public Operator visitMergeSort(MergeSortNode node, LocalExecutionPlanContext context) {
    return super.visitMergeSort(node, context);
  }

  @Override
  public Operator visitOutput(OutputNode node, LocalExecutionPlanContext context) {
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
        node.getOutputSymbols().stream()
            .filter(e -> !TIMESTAMP_EXPRESSION_STRING.equalsIgnoreCase(e.getName()))
            .map(Symbol::toSymbolReference)
            .toArray(Expression[]::new),
        inputDataTypes,
        inputLocations,
        node.getPlanNodeId(),
        context);
  }

  @Override
  public Operator visitSort(SortNode node, LocalExecutionPlanContext context) {
    return super.visitSort(node, context);
  }

  @Override
  public Operator visitTopK(TopKNode node, LocalExecutionPlanContext context) {
    return super.visitTopK(node, context);
  }
}
