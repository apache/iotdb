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

package org.apache.iotdb.calc.plan.planner;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.AssignUniqueIdOperator;
import org.apache.iotdb.calc.execution.operator.process.CollectOperator;
import org.apache.iotdb.calc.execution.operator.process.EnforceSingleRowOperator;
import org.apache.iotdb.calc.execution.operator.process.FilterAndProjectOperator;
import org.apache.iotdb.calc.execution.operator.process.LimitOperator;
import org.apache.iotdb.calc.execution.operator.process.MappingCollectOperator;
import org.apache.iotdb.calc.execution.operator.process.OffsetOperator;
import org.apache.iotdb.calc.execution.operator.process.PatternRecognitionOperator;
import org.apache.iotdb.calc.execution.operator.process.PreviousFillWithGroupOperator;
import org.apache.iotdb.calc.execution.operator.process.TableFillOperator;
import org.apache.iotdb.calc.execution.operator.process.TableLinearFillOperator;
import org.apache.iotdb.calc.execution.operator.process.TableLinearFillWithGroupOperator;
import org.apache.iotdb.calc.execution.operator.process.TableMergeSortOperator;
import org.apache.iotdb.calc.execution.operator.process.TableSortOperator;
import org.apache.iotdb.calc.execution.operator.process.TableStreamSortOperator;
import org.apache.iotdb.calc.execution.operator.process.TableTopKOperator;
import org.apache.iotdb.calc.execution.operator.process.ValuesOperator;
import org.apache.iotdb.calc.execution.operator.process.fill.IFill;
import org.apache.iotdb.calc.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.BinaryConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.BooleanConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.DoubleConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.FloatConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.IntConstantFill;
import org.apache.iotdb.calc.execution.operator.process.fill.constant.LongConstantFill;
import org.apache.iotdb.calc.execution.operator.process.function.TableFunctionLeafOperator;
import org.apache.iotdb.calc.execution.operator.process.function.TableFunctionOperator;
import org.apache.iotdb.calc.execution.operator.process.gapfill.GapFillWGroupWMoOperator;
import org.apache.iotdb.calc.execution.operator.process.gapfill.GapFillWGroupWoMoOperator;
import org.apache.iotdb.calc.execution.operator.process.gapfill.GapFillWoGroupWMoOperator;
import org.apache.iotdb.calc.execution.operator.process.gapfill.GapFillWoGroupWoMoOperator;
import org.apache.iotdb.calc.execution.operator.process.join.SimpleNestedLoopCrossJoinOperator;
import org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.JoinKeyComparatorFactory;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.LogicalIndexNavigation;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.PatternAggregationTracker;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.PatternAggregator;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.PatternVariableRecognizer;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.PhysicalAggregationPointer;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.PhysicalValueAccessor;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.PhysicalValuePointer;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.expression.Computation;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.expression.PatternExpressionComputation;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.matcher.IrRowPatternToProgramRewriter;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.matcher.Matcher;
import org.apache.iotdb.calc.execution.operator.process.rowpattern.matcher.Program;
import org.apache.iotdb.calc.execution.operator.process.window.RowNumberOperator;
import org.apache.iotdb.calc.execution.operator.process.window.TableWindowOperator;
import org.apache.iotdb.calc.execution.operator.process.window.TopKRankingOperator;
import org.apache.iotdb.calc.execution.operator.process.window.function.WindowFunction;
import org.apache.iotdb.calc.execution.operator.process.window.function.WindowFunctionFactory;
import org.apache.iotdb.calc.execution.operator.process.window.function.aggregate.AggregationWindowFunction;
import org.apache.iotdb.calc.execution.operator.process.window.function.aggregate.WindowAggregator;
import org.apache.iotdb.calc.execution.operator.process.window.partition.frame.FrameInfo;
import org.apache.iotdb.calc.execution.operator.source.relational.AsofMergeSortInnerJoinOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.AsofMergeSortLeftJoinOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.MarkDistinctOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.MergeSortFullOuterJoinOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.MergeSortInnerJoinOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.MergeSortLeftJoinOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.MergeSortSemiJoinOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AggregationOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.LastByDescAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.LastDescAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.TableAggregator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAccumulator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.GroupedAggregator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.HashAggregationOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.StreamingAggregationOperator;
import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.grouped.StreamingHashAggregationOperator;
import org.apache.iotdb.calc.execution.relational.ColumnTransformerBuilder;
import org.apache.iotdb.calc.plan.relational.metadata.ITypeMetadata;
import org.apache.iotdb.calc.plan.relational.planner.CastToBlobLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToBooleanLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToDateLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToDoubleLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToFloatLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToInt32LiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToInt64LiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToStringLiteralVisitor;
import org.apache.iotdb.calc.plan.relational.planner.CastToTimestampLiteralVisitor;
import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.calc.utils.datastructure.SortKey;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.analyze.ITableTypeProvider;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.ICoreQueryPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.commons.queryengine.plan.relational.function.BoundSignature;
import org.apache.iotdb.commons.queryengine.plan.relational.function.FunctionKind;
import org.apache.iotdb.commons.queryengine.plan.relational.function.ITableFunctionFactory;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AssignUniqueId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MarkDistinctNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.Measure;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.AggregationLabelSet;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.AggregationValuePointer;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.ClassifierValuePointer;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.IrLabel;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.LogicalIndexPointer;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.MatchNumberValuePointer;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.ScalarValuePointer;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.rowpattern.ValuePointer;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.calc.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.apache.iotdb.calc.execution.operator.process.rowpattern.PhysicalValuePointer.CLASSIFIER;
import static org.apache.iotdb.calc.execution.operator.process.rowpattern.PhysicalValuePointer.MATCH_NUMBER;
import static org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AccumulatorFactory.createAccumulator;
import static org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AccumulatorFactory.createBuiltinAccumulator;
import static org.apache.iotdb.calc.execution.operator.source.relational.aggregation.AccumulatorFactory.createGroupedAccumulator;
import static org.apache.iotdb.calc.plan.planner.CommonOperatorUtils.IDENTITY_FILL;
import static org.apache.iotdb.calc.plan.planner.CommonOperatorUtils.UNKNOWN_DATATYPE;
import static org.apache.iotdb.calc.plan.planner.CommonOperatorUtils.getLinearFill;
import static org.apache.iotdb.calc.plan.planner.CommonOperatorUtils.getPreviousFill;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.FIRST_AGGREGATION;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.FIRST_BY_AGGREGATION;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.LAST_AGGREGATION;
import static org.apache.iotdb.calc.utils.constant.SqlConstant.LAST_BY_AGGREGATION;
import static org.apache.iotdb.commons.queryengine.execution.operator.source.relational.aggregation.grouped.hash.GroupByHash.DEFAULT_GROUP_NUMBER;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_FIRST;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder.DESC_NULLS_FIRST;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder.DESC_NULLS_LAST;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.node.RowsPerMatch.ONE;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SkipToPosition.LAST;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinAggregationFunction.getAggregationTypeByFuncName;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;

/** This Visitor is responsible for transferring Table PlanNode Tree to Table Operator Tree. */
public abstract class TableOperatorGenerator<
        C extends ITableOperatorGeneratorContext, M extends ITypeMetadata & ITableFunctionFactory>
    implements ICoreQueryPlanVisitor<Operator, C> {

  protected final M metadata;

  public TableOperatorGenerator(M metadata) {
    this.metadata = metadata;
  }

  @Override
  public Operator visitPlan(PlanNode node, C context) {
    throw new UnsupportedOperationException("should call the concrete visitXX() method");
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

  protected ImmutableMap<Symbol, Integer> makeLayoutFromOutputSymbols(List<Symbol> outputSymbols) {
    if (outputSymbols == null) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
    int channel = 0;
    for (Symbol symbol : outputSymbols) {
      outputMappings.put(symbol, channel);
      channel++;
    }
    return outputMappings.buildOrThrow();
  }

  @Override
  public Operator visitFilter(FilterNode node, C context) {
    ITableTypeProvider typeProvider = context.getTableTypeProvider();
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

  protected Operator constructFilterAndProjectOperator(
      Optional<Expression> predicate,
      Operator inputOperator,
      Expression[] projectExpressions,
      List<TSDataType> inputDataTypes,
      Map<Symbol, List<InputLocation>> inputLocations,
      PlanNodeId planNodeId,
      C context) {

    final List<TSDataType> filterOutputDataTypes = new ArrayList<>(inputDataTypes);

    // records LeafColumnTransformer of filter
    List<LeafColumnTransformer> filterLeafColumnTransformerList = new ArrayList<>();

    // records subexpression -> ColumnTransformer for filter
    Map<Expression, ColumnTransformer> filterExpressionColumnTransformerMap = new HashMap<>();

    ColumnTransformerBuilder visitor = new ColumnTransformerBuilder();

    SessionInfo sessionInfo = getSessionInfo(context);
    ColumnTransformer filterOutputTransformer =
        predicate
            .map(
                p -> {
                  ColumnTransformerBuilder.Context filterColumnTransformerContext =
                      new ColumnTransformerBuilder.Context(
                          sessionInfo,
                          filterLeafColumnTransformerList,
                          inputLocations,
                          filterExpressionColumnTransformerMap,
                          ImmutableMap.of(),
                          ImmutableList.of(),
                          ImmutableList.of(),
                          0,
                          context.getTableTypeProvider(),
                          metadata,
                          context.getMemoryReservationManager());

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
            sessionInfo,
            projectLeafColumnTransformerList,
            inputLocations,
            projectExpressionColumnTransformerMap,
            filterExpressionColumnTransformerMap,
            commonTransformerList,
            filterOutputDataTypes,
            inputLocations.size(),
            context.getTableTypeProvider(),
            metadata,
            context.getMemoryReservationManager());

    for (Expression expression : projectExpressions) {
      projectOutputTransformerList.add(
          visitor.process(expression, projectColumnTransformerContext));
    }

    final CommonOperatorContext operatorContext =
        addOperatorContext(context, planNodeId, FilterAndProjectOperator.class.getSimpleName());

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
  public Operator visitProject(ProjectNode node, C context) {
    ITableTypeProvider typeProvider = context.getTableTypeProvider();
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

  private List<TSDataType> getInputColumnTypes(PlanNode node, ITableTypeProvider typeProvider) {
    // ignore "time" column
    return node.getChildren().stream()
        .map(PlanNode::getOutputSymbols)
        .flatMap(List::stream)
        .map(s -> getTSDataType(typeProvider.getTableModelType(s)))
        .collect(Collectors.toList());
  }

  @Override
  public Operator visitGapFill(GapFillNode node, C context) {
    Operator child = node.getChild().accept(this, context);
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTableTypeProvider());
    int timeColumnIndex = getColumnIndex(node.getGapFillColumn(), node.getChild());
    if (node.getGapFillGroupingKeys().isEmpty()) { // without group keys
      if (node.getMonthDuration() == 0) { // without month interval
        CommonOperatorContext operatorContext =
            addOperatorContext(
                context, node.getPlanNodeId(), GapFillWoGroupWoMoOperator.class.getSimpleName());
        return new GapFillWoGroupWoMoOperator(
            operatorContext,
            child,
            timeColumnIndex,
            node.getStartTime(),
            node.getEndTime(),
            inputDataTypes,
            node.getNonMonthDuration());
      } else { // with month interval
        CommonOperatorContext operatorContext =
            addOperatorContext(
                context, node.getPlanNodeId(), GapFillWoGroupWMoOperator.class.getSimpleName());
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
        CommonOperatorContext operatorContext =
            addOperatorContext(
                context, node.getPlanNodeId(), GapFillWGroupWoMoOperator.class.getSimpleName());
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
        CommonOperatorContext operatorContext =
            addOperatorContext(
                context, node.getPlanNodeId(), GapFillWGroupWMoOperator.class.getSimpleName());
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
  public Operator visitPreviousFill(PreviousFillNode node, C context) {
    Operator child = node.getChild().accept(this, context);

    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTableTypeProvider());
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
      CommonOperatorContext operatorContext =
          addOperatorContext(
              context, node.getPlanNodeId(), PreviousFillWithGroupOperator.class.getSimpleName());
      return new PreviousFillWithGroupOperator(
          operatorContext,
          fillArray,
          child,
          helperColumnIndex,
          genFillGroupKeyComparator(
              node.getGroupingKeys().get(), node, inputDataTypes, new HashSet<>()),
          inputDataTypes);
    } else {
      CommonOperatorContext operatorContext =
          addOperatorContext(
              context, node.getPlanNodeId(), TableFillOperator.class.getSimpleName());
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
  public Operator visitLinearFill(LinearFillNode node, C context) {
    Operator child = node.getChild().accept(this, context);

    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTableTypeProvider());
    int inputColumnCount = inputDataTypes.size();
    int helperColumnIndex = getColumnIndex(node.getHelperColumn(), node.getChild());
    ILinearFill[] fillArray = getLinearFill(inputColumnCount, inputDataTypes);

    if (node.getGroupingKeys().isPresent()) {
      CommonOperatorContext operatorContext =
          addOperatorContext(
              context,
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
      CommonOperatorContext operatorContext =
          addOperatorContext(
              context, node.getPlanNodeId(), TableLinearFillOperator.class.getSimpleName());
      return new TableLinearFillOperator(operatorContext, fillArray, child, helperColumnIndex);
    }
  }

  @Override
  public Operator visitValueFill(ValueFillNode node, C context) {
    Operator child = node.getChild().accept(this, context);
    CommonOperatorContext operatorContext =
        addOperatorContext(context, node.getPlanNodeId(), TableFillOperator.class.getSimpleName());
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTableTypeProvider());
    int inputColumnCount = inputDataTypes.size();
    Literal filledValue = node.getFilledValue();
    return new TableFillOperator(
        operatorContext,
        getValueFill(inputColumnCount, inputDataTypes, filledValue, context),
        child,
        -1);
  }

  private IFill[] getValueFill(
      int inputColumnCount, List<TSDataType> inputDataTypes, Literal filledValue, C context) {
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
  public Operator visitLimit(LimitNode node, C context) {
    Operator child = node.getChild().accept(this, context);
    CommonOperatorContext operatorContext =
        addOperatorContext(context, node.getPlanNodeId(), LimitOperator.class.getSimpleName());

    return new LimitOperator(operatorContext, node.getCount(), child);
  }

  @Override
  public Operator visitOffset(OffsetNode node, C context) {
    Operator child = node.getChild().accept(this, context);
    CommonOperatorContext operatorContext =
        addOperatorContext(context, node.getPlanNodeId(), OffsetOperator.class.getSimpleName());

    return new OffsetOperator(operatorContext, node.getCount(), child);
  }

  @Override
  public Operator visitOutput(OutputNode node, C context) {
    return node.getChild().accept(this, context);
  }

  @Override
  public Operator visitCollect(CollectNode node, C context) {
    CommonOperatorContext operatorContext =
        addOperatorContext(context, node.getPlanNodeId(), CollectOperator.class.getSimpleName());
    List<Operator> children = new ArrayList<>(node.getChildren().size());
    for (PlanNode child : node.getChildren()) {
      children.add(this.process(child, context));
    }
    return new CollectOperator(operatorContext, children);
  }

  @Override
  public Operator visitMergeSort(MergeSortNode node, C context) {
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), TableMergeSortOperator.class.getSimpleName());
    List<Operator> children = new ArrayList<>(node.getChildren().size());
    for (PlanNode child : node.getChildren()) {
      children.add(this.process(child, context));
    }
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTableTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTableTypeProvider());

    return new TableMergeSortOperator(
        operatorContext,
        children,
        dataTypes,
        getComparatorForTable(
            node.getOrderingScheme().getOrderingList(), sortItemIndexList, sortItemDataTypeList));
  }

  @Override
  public Operator visitSort(SortNode node, C context) {
    CommonOperatorContext operatorContext =
        addOperatorContext(context, node.getPlanNodeId(), TableSortOperator.class.getSimpleName());
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTableTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTableTypeProvider());

    Operator child = node.getChild().accept(this, context);

    return new TableSortOperator(
        operatorContext,
        child,
        dataTypes,
        getSortTmpDir(operatorContext),
        getComparatorForTable(
            node.getOrderingScheme().getOrderingList(), sortItemIndexList, sortItemDataTypeList));
  }

  protected abstract String getSortTmpDir(CommonOperatorContext operatorContext);

  protected abstract CommonOperatorContext addOperatorContext(
      C context, PlanNodeId planNodeId, String operatorType);

  @Override
  public Operator visitTopK(TopKNode node, C context) {
    CommonOperatorContext operatorContext =
        addOperatorContext(context, node.getPlanNodeId(), TableTopKOperator.class.getSimpleName());
    List<Operator> children = new ArrayList<>(node.getChildren().size());
    for (PlanNode child : node.getChildren()) {
      children.add(this.process(child, context));
    }
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTableTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTableTypeProvider());
    return new TableTopKOperator(
        operatorContext,
        children,
        dataTypes,
        getComparatorForTable(
            node.getOrderingScheme().getOrderingList(), sortItemIndexList, sortItemDataTypeList),
        (int) node.getCount(),
        node.isChildrenDataInOrder());
  }

  protected List<TSDataType> getOutputColumnTypes(PlanNode node, ITableTypeProvider typeProvider) {
    return node.getOutputSymbols().stream()
        .map(s -> getTSDataType(typeProvider.getTableModelType(s)))
        .collect(Collectors.toList());
  }

  protected void genSortInformation(
      List<Symbol> outputSymbols,
      OrderingScheme orderingScheme,
      List<Integer> sortItemIndexList,
      List<TSDataType> sortItemDataTypeList,
      ITableTypeProvider typeProvider) {
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
  public Operator visitStreamSort(StreamSortNode node, C context) {
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), TableStreamSortOperator.class.getSimpleName());
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTableTypeProvider());
    int sortItemsCount = node.getOrderingScheme().getOrderBy().size();

    List<Integer> sortItemIndexList = new ArrayList<>(sortItemsCount);
    List<TSDataType> sortItemDataTypeList = new ArrayList<>(sortItemsCount);
    genSortInformation(
        node.getOutputSymbols(),
        node.getOrderingScheme(),
        sortItemIndexList,
        sortItemDataTypeList,
        context.getTableTypeProvider());

    Operator child = node.getChild().accept(this, context);

    return new TableStreamSortOperator(
        operatorContext,
        child,
        dataTypes,
        getSortTmpDir(operatorContext),
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
  public Operator visitGroup(GroupNode node, C context) {
    if (node.getPartitionKeyCount() == 0) {
      SortNode sortNode =
          new SortNode(
              node.getPlanNodeId(), node.getChild(), node.getOrderingScheme(), false, false);
      return visitSort(sortNode, context);
    } else {
      StreamSortNode streamSortNode =
          new StreamSortNode(
              node.getPlanNodeId(),
              node.getChild(),
              node.getOrderingScheme(),
              false,
              false,
              node.getPartitionKeyCount() - 1);
      return visitStreamSort(streamSortNode, context);
    }
  }

  @Override
  public Operator visitJoin(JoinNode node, C context) {
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTableTypeProvider());

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
      CommonOperatorContext operatorContext =
          addOperatorContext(
              context,
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

    semanticCheckForJoin(node);

    JoinNode.AsofJoinClause asofJoinClause = node.getAsofCriteria().orElse(null);
    int equiSize = node.getCriteria().size();
    int size = equiSize + (asofJoinClause == null ? 0 : 1);
    int[] leftJoinKeyPositions = new int[size];
    for (int i = 0; i < equiSize; i++) {
      Integer leftJoinKeyPosition = leftColumnNamesMap.get(node.getCriteria().get(i).getLeft());
      if (leftJoinKeyPosition == null) {
        throw new IllegalStateException("Left child of JoinNode doesn't contain left join key.");
      }
      leftJoinKeyPositions[i] = leftJoinKeyPosition;
    }

    List<Type> joinKeyTypes = new ArrayList<>(size);
    int[] rightJoinKeyPositions = new int[size];
    for (int i = 0; i < equiSize; i++) {
      Integer rightJoinKeyPosition = rightColumnNamesMap.get(node.getCriteria().get(i).getRight());
      if (rightJoinKeyPosition == null) {
        throw new IllegalStateException("Right child of JoinNode doesn't contain right join key.");
      }
      rightJoinKeyPositions[i] = rightJoinKeyPosition;

      Type leftJoinKeyType =
          context.getTableTypeProvider().getTableModelType(node.getCriteria().get(i).getLeft());
      checkIfJoinKeyTypeMatches(
          leftJoinKeyType,
          context.getTableTypeProvider().getTableModelType(node.getCriteria().get(i).getRight()));
      joinKeyTypes.add(leftJoinKeyType);
    }

    if (asofJoinClause != null) {
      Integer leftAsofJoinKeyPosition = leftColumnNamesMap.get(asofJoinClause.getLeft());
      if (leftAsofJoinKeyPosition == null) {
        throw new IllegalStateException(
            "Left child of JoinNode doesn't contain left ASOF main join key.");
      }
      leftJoinKeyPositions[equiSize] = leftAsofJoinKeyPosition;
      Integer rightAsofJoinKeyPosition = rightColumnNamesMap.get(asofJoinClause.getRight());
      if (rightAsofJoinKeyPosition == null) {
        throw new IllegalStateException(
            "Right child of JoinNode doesn't contain right ASOF main join key.");
      }
      rightJoinKeyPositions[equiSize] = rightAsofJoinKeyPosition;

      if (context.getTableTypeProvider().getTableModelType(asofJoinClause.getLeft()) != TIMESTAMP) {
        throw new IllegalStateException("Type of left ASOF Join key is not TIMESTAMP");
      }
      if (context.getTableTypeProvider().getTableModelType(asofJoinClause.getRight())
          != TIMESTAMP) {
        throw new IllegalStateException("Type of right ASOF Join key is not TIMESTAMP");
      }

      ComparisonExpression.Operator asofOperator = asofJoinClause.getOperator();

      if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.INNER) {
        CommonOperatorContext operatorContext =
            addOperatorContext(
                context,
                node.getPlanNodeId(),
                AsofMergeSortInnerJoinOperator.class.getSimpleName());
        return new AsofMergeSortInnerJoinOperator(
            operatorContext,
            leftChild,
            leftJoinKeyPositions,
            leftOutputSymbolIdx,
            rightChild,
            rightJoinKeyPositions,
            rightOutputSymbolIdx,
            JoinKeyComparatorFactory.getAsofComparators(
                joinKeyTypes,
                asofOperator == ComparisonExpression.Operator.LESS_THAN_OR_EQUAL
                    || asofOperator == ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                !asofJoinClause.isOperatorContainsGreater()),
            dataTypes);
      } else if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.LEFT) {
        CommonOperatorContext operatorContext =
            addOperatorContext(
                context, node.getPlanNodeId(), AsofMergeSortLeftJoinOperator.class.getSimpleName());
        return new AsofMergeSortLeftJoinOperator(
            operatorContext,
            leftChild,
            leftJoinKeyPositions,
            leftOutputSymbolIdx,
            rightChild,
            rightJoinKeyPositions,
            rightOutputSymbolIdx,
            JoinKeyComparatorFactory.getAsofComparators(
                joinKeyTypes,
                asofOperator == ComparisonExpression.Operator.LESS_THAN_OR_EQUAL
                    || asofOperator == ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                !asofJoinClause.isOperatorContainsGreater()),
            dataTypes);
      } else {
        throw new IllegalStateException("Unsupported ASOF join type: " + node.getJoinType());
      }
    }

    if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.INNER) {
      CommonOperatorContext operatorContext =
          addOperatorContext(
              context, node.getPlanNodeId(), MergeSortInnerJoinOperator.class.getSimpleName());
      return new MergeSortInnerJoinOperator(
          operatorContext,
          leftChild,
          leftJoinKeyPositions,
          leftOutputSymbolIdx,
          rightChild,
          rightJoinKeyPositions,
          rightOutputSymbolIdx,
          JoinKeyComparatorFactory.getComparators(joinKeyTypes, true),
          dataTypes);
    } else if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.FULL) {
      CommonOperatorContext operatorContext =
          addOperatorContext(
              context, node.getPlanNodeId(), MergeSortFullOuterJoinOperator.class.getSimpleName());
      return new MergeSortFullOuterJoinOperator(
          operatorContext,
          leftChild,
          leftJoinKeyPositions,
          leftOutputSymbolIdx,
          rightChild,
          rightJoinKeyPositions,
          rightOutputSymbolIdx,
          JoinKeyComparatorFactory.getComparators(joinKeyTypes, true),
          dataTypes,
          joinKeyTypes.stream().map(this::buildUpdateLastRowFunction).collect(Collectors.toList()));
    } else if (requireNonNull(node.getJoinType()) == JoinNode.JoinType.LEFT) {
      CommonOperatorContext operatorContext =
          addOperatorContext(
              context, node.getPlanNodeId(), MergeSortLeftJoinOperator.class.getSimpleName());
      return new MergeSortLeftJoinOperator(
          operatorContext,
          leftChild,
          leftJoinKeyPositions,
          leftOutputSymbolIdx,
          rightChild,
          rightJoinKeyPositions,
          rightOutputSymbolIdx,
          JoinKeyComparatorFactory.getComparators(joinKeyTypes, true),
          dataTypes);
    }

    throw new IllegalStateException("Unsupported join type: " + node.getJoinType());
  }

  protected void semanticCheckForJoin(JoinNode node) {
    try {
      checkArgument(
          !node.getFilter().isPresent() || node.getFilter().get().equals(TRUE_LITERAL),
          String.format(
              "Filter is not supported in %s. Filter is %s.",
              node.getJoinType(), node.getFilter().map(Expression::toString).orElse("null")));
      checkArgument(
          !node.getCriteria().isEmpty() || node.getAsofCriteria().isPresent(),
          String.format("%s must have join keys.", node.getJoinType()));
    } catch (IllegalArgumentException e) {
      throw new SemanticException(e.getMessage());
    }
  }

  protected BiFunction<Column, Integer, Column> buildUpdateLastRowFunction(Type joinKeyType) {
    switch (joinKeyType.getTypeEnum()) {
      case INT32:
        return (inputColumn, rowIndex) ->
            new IntColumn(
                1, Optional.empty(), new int[] {inputColumn.getInt(rowIndex)}, TSDataType.INT32);
      case DATE:
        return (inputColumn, rowIndex) ->
            new IntColumn(
                1, Optional.empty(), new int[] {inputColumn.getInt(rowIndex)}, TSDataType.DATE);
      case INT64:
      case TIMESTAMP:
        return (inputColumn, rowIndex) ->
            new LongColumn(1, Optional.empty(), new long[] {inputColumn.getLong(rowIndex)});
      case FLOAT:
        return (inputColumn, rowIndex) ->
            new FloatColumn(1, Optional.empty(), new float[] {inputColumn.getFloat(rowIndex)});
      case DOUBLE:
        return (inputColumn, rowIndex) ->
            new DoubleColumn(1, Optional.empty(), new double[] {inputColumn.getDouble(rowIndex)});
      case BOOLEAN:
        return (inputColumn, rowIndex) ->
            new BooleanColumn(
                1, Optional.empty(), new boolean[] {inputColumn.getBoolean(rowIndex)});
      case STRING:
      case TEXT:
      case BLOB:
        return (inputColumn, rowIndex) ->
            new BinaryColumn(1, Optional.empty(), new Binary[] {inputColumn.getBinary(rowIndex)});
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + joinKeyType);
    }
  }

  @Override
  public Operator visitSemiJoin(SemiJoinNode node, C context) {
    List<TSDataType> dataTypes = getOutputColumnTypes(node, context.getTableTypeProvider());

    Operator leftChild = node.getLeftChild().accept(this, context);
    Operator rightChild = node.getRightChild().accept(this, context);

    ImmutableMap<Symbol, Integer> sourceColumnNamesMap =
        makeLayoutFromOutputSymbols(node.getSource().getOutputSymbols());
    List<Symbol> sourceOutputSymbols = node.getSource().getOutputSymbols();
    int[] sourceOutputSymbolIdx = new int[node.getSource().getOutputSymbols().size()];
    for (int i = 0; i < sourceOutputSymbolIdx.length; i++) {
      Integer index = sourceColumnNamesMap.get(sourceOutputSymbols.get(i));
      checkNotNull(index, "Source of SemiJoinNode doesn't contain sourceOutputSymbol.");
      sourceOutputSymbolIdx[i] = index;
    }

    ImmutableMap<Symbol, Integer> filteringSourceColumnNamesMap =
        makeLayoutFromOutputSymbols(node.getRightChild().getOutputSymbols());

    Integer sourceJoinKeyPosition = sourceColumnNamesMap.get(node.getSourceJoinSymbol());
    checkNotNull(sourceJoinKeyPosition, "Source of SemiJoinNode doesn't contain sourceJoinSymbol.");

    Integer filteringSourceJoinKeyPosition =
        filteringSourceColumnNamesMap.get(node.getFilteringSourceJoinSymbol());
    checkNotNull(
        filteringSourceJoinKeyPosition,
        "FilteringSource of SemiJoinNode doesn't contain filteringSourceJoinSymbol.");

    Type sourceJoinKeyType =
        context.getTableTypeProvider().getTableModelType(node.getSourceJoinSymbol());

    checkIfJoinKeyTypeMatches(
        sourceJoinKeyType,
        context.getTableTypeProvider().getTableModelType(node.getFilteringSourceJoinSymbol()));

    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), MergeSortSemiJoinOperator.class.getSimpleName());
    return new MergeSortSemiJoinOperator(
        operatorContext,
        leftChild,
        sourceJoinKeyPosition,
        sourceOutputSymbolIdx,
        rightChild,
        filteringSourceJoinKeyPosition,
        JoinKeyComparatorFactory.getComparator(sourceJoinKeyType, true),
        dataTypes);
  }

  protected void checkIfJoinKeyTypeMatches(Type leftJoinKeyType, Type rightJoinKeyType) {
    if (leftJoinKeyType != rightJoinKeyType) {
      throw new SemanticException(
          "Join key type mismatch. Left join key type: "
              + leftJoinKeyType
              + ", right join key type: "
              + rightJoinKeyType);
    }
  }

  @Override
  public Operator visitEnforceSingleRow(EnforceSingleRowNode node, C context) {
    Operator child = node.getChild().accept(this, context);
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), EnforceSingleRowOperator.class.getSimpleName());

    return new EnforceSingleRowOperator(operatorContext, child);
  }

  @Override
  public Operator visitAssignUniqueId(AssignUniqueId node, C context) {
    Operator child = node.getChild().accept(this, context);
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), EnforceSingleRowOperator.class.getSimpleName());

    return new AssignUniqueIdOperator(operatorContext, child);
  }

  @Override
  public Operator visitAggregation(AggregationNode node, C context) {

    Operator child = node.getChild().accept(this, context);

    if (node.getGroupingKeys().isEmpty()) {
      return planGlobalAggregation(node, child, context.getTableTypeProvider(), context);
    }

    return planGroupByAggregation(node, child, context.getTableTypeProvider(), context);
  }

  private Operator planGlobalAggregation(
      AggregationNode node, Operator child, ITableTypeProvider typeProvider, C context) {
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), AggregationOperator.class.getSimpleName());
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
                        false,
                        null,
                        Collections.emptySet())));
    return new AggregationOperator(operatorContext, child, aggregatorBuilder.build());
  }

  // timeColumnName and measurementColumnNames will only be set for AggTableScan.
  protected TableAggregator buildAggregator(
      Map<Symbol, Integer> childLayout,
      Symbol symbol,
      AggregationNode.Aggregation aggregation,
      AggregationNode.Step step,
      ITableTypeProvider typeProvider,
      boolean scanAscending,
      boolean isAggTableScan,
      String timeColumnName,
      Set<String> measurementColumnNames) {
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
            isAggTableScan,
            timeColumnName,
            measurementColumnNames,
            aggregation.isDistinct());

    OptionalInt maskChannel = OptionalInt.empty();
    if (aggregation.hasMask()) {
      maskChannel = OptionalInt.of(childLayout.get(aggregation.getMask().get()));
    }

    return new TableAggregator(
        accumulator,
        step,
        getTSDataType(typeProvider.getTableModelType(symbol)),
        argumentChannels,
        maskChannel);
  }

  protected Operator planGroupByAggregation(
      AggregationNode node, Operator child, ITableTypeProvider typeProvider, C context) {
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
                            childLayout,
                            k,
                            v,
                            node.getStep(),
                            typeProvider,
                            true,
                            false,
                            null,
                            Collections.emptySet())));

        CommonOperatorContext operatorContext =
            addOperatorContext(
                context, node.getPlanNodeId(), StreamingAggregationOperator.class.getSimpleName());
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
      CommonOperatorContext operatorContext =
          addOperatorContext(
              context,
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
          DEFAULT_GROUP_NUMBER,
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
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), HashAggregationOperator.class.getSimpleName());

    return new HashAggregationOperator(
        operatorContext,
        child,
        groupByTypes,
        groupByChannels,
        aggregatorBuilder.build(),
        node.getStep(),
        DEFAULT_GROUP_NUMBER,
        Long.MAX_VALUE,
        false,
        Long.MAX_VALUE);
  }

  protected Comparator<SortKey> genGroupKeyComparator(
      List<Type> groupTypes, List<Integer> groupByChannels) {
    return getComparatorForTable(
        // SortOrder is not sensitive here, the comparator is just used to judge equality.
        groupTypes.stream().map(k -> ASC_NULLS_LAST).collect(Collectors.toList()),
        groupByChannels,
        groupTypes.stream().map(InternalTypeManager::getTSDataType).collect(Collectors.toList()));
  }

  protected static List<Integer> getChannelsForSymbols(
      List<Symbol> symbols, Map<Symbol, Integer> layout) {
    ImmutableList.Builder<Integer> builder = ImmutableList.builder();
    for (Symbol symbol : symbols) {
      builder.add(layout.get(symbol));
    }
    return builder.build();
  }

  protected GroupedAggregator buildGroupByAggregator(
      Map<Symbol, Integer> childLayout,
      Symbol symbol,
      AggregationNode.Aggregation aggregation,
      AggregationNode.Step step,
      ITableTypeProvider typeProvider) {
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
            true,
            aggregation.isDistinct());

    OptionalInt maskChannel = OptionalInt.empty();
    if (aggregation.hasMask()) {
      maskChannel = OptionalInt.of(childLayout.get(aggregation.getMask().get()));
    }

    return new GroupedAggregator(
        accumulator,
        step,
        getTSDataType(typeProvider.getTableModelType(symbol)),
        argumentChannels,
        maskChannel);
  }

  @Override
  public Operator visitTableFunctionProcessor(TableFunctionProcessorNode node, C context) {
    TableFunction tableFunction = metadata.getTableFunction(node.getName());
    TableFunctionProcessorProvider processorProvider =
        tableFunction.getProcessorProvider(node.getTableFunctionHandle());
    if (node.getChildren().isEmpty()) {
      List<TSDataType> outputDataTypes =
          node.getOutputSymbols().stream()
              .map(context.getTableTypeProvider()::getTableModelType)
              .map(InternalTypeManager::getTSDataType)
              .collect(Collectors.toList());
      CommonOperatorContext operatorContext =
          addOperatorContext(
              context, node.getPlanNodeId(), TableFunctionLeafOperator.class.getSimpleName());
      return new TableFunctionLeafOperator(operatorContext, processorProvider, outputDataTypes);
    } else {
      Operator operator = node.getChild().accept(this, context);
      CommonOperatorContext operatorContext =
          addOperatorContext(
              context, node.getPlanNodeId(), TableFunctionOperator.class.getSimpleName());

      List<TSDataType> inputDataTypes =
          node.getChild().getOutputSymbols().stream()
              .map(context.getTableTypeProvider()::getTableModelType)
              .map(InternalTypeManager::getTSDataType)
              .collect(Collectors.toList());

      List<TSDataType> outputDataTypes =
          node.getOutputSymbols().stream()
              .map(context.getTableTypeProvider()::getTableModelType)
              .map(InternalTypeManager::getTSDataType)
              .collect(Collectors.toList());

      int properChannelCount = node.getProperOutputs().size();
      Optional<TableFunctionNode.PassThroughSpecification> passThroughSpecification =
          node.getPassThroughSpecification();

      Map<Symbol, Integer> childLayout =
          makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());
      List<Integer> requiredChannels =
          getChannelsForSymbols(node.getRequiredSymbols(), childLayout);
      List<Integer> passThroughChannels =
          passThroughSpecification
              .map(
                  passThrough ->
                      getChannelsForSymbols(
                          passThrough.getColumns().stream()
                              .map(TableFunctionNode.PassThroughColumn::getSymbol)
                              .collect(Collectors.toList()),
                          childLayout))
              .orElse(Collections.emptyList());
      List<Integer> partitionChannels;
      if (node.getDataOrganizationSpecification().isPresent()) {
        partitionChannels =
            getChannelsForSymbols(
                node.getDataOrganizationSpecification().get().getPartitionBy(), childLayout);
      } else {
        partitionChannels = Collections.emptyList();
      }
      return new TableFunctionOperator(
          operatorContext,
          processorProvider,
          operator,
          inputDataTypes,
          outputDataTypes,
          properChannelCount,
          requiredChannels,
          passThroughChannels,
          passThroughSpecification
              .map(TableFunctionNode.PassThroughSpecification::isDeclaredAsPassThrough)
              .orElse(false),
          partitionChannels,
          node.isRequireRecordSnapshot());
    }
  }

  private PatternAggregator buildPatternAggregator(
      ResolvedFunction resolvedFunction,
      List<Map.Entry<Expression, Type>> arguments,
      List<Integer> argumentChannels,
      PatternAggregationTracker patternAggregationTracker) {
    String functionName = resolvedFunction.getSignature().getName();
    List<TSDataType> originalArgumentTypes =
        resolvedFunction.getSignature().getArgumentTypes().stream()
            .map(InternalTypeManager::getTSDataType)
            .collect(Collectors.toList());

    TableAccumulator accumulator =
        createBuiltinAccumulator(getAggregationTypeByFuncName(functionName), originalArgumentTypes);

    BoundSignature signature = resolvedFunction.getSignature();

    return new PatternAggregator(
        signature, accumulator, argumentChannels, patternAggregationTracker);
  }

  @Override
  public Operator visitPatternRecognition(PatternRecognitionNode node, C context) {
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), PatternRecognitionOperator.class.getSimpleName());

    Operator child = node.getChild().accept(this, context);

    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());

    List<Symbol> partitionBySymbols = node.getPartitionBy();
    List<Integer> partitionChannels =
        ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, childLayout));

    List<Integer> sortChannels = ImmutableList.of();
    List<SortOrder> sortOrder = ImmutableList.of();

    if (node.getOrderingScheme().isPresent()) {
      OrderingScheme orderingScheme = node.getOrderingScheme().get();
      sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), childLayout);
      sortOrder = orderingScheme.getOrderingList();
    }

    // The output order for pattern recognition operation is defined as follows:
    // - for ONE ROW PER MATCH: partition by symbols, then measures,
    // - for ALL ROWS PER MATCH: partition by symbols, order by symbols, measures, remaining input
    // symbols.

    // all output column types of the input table
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTableTypeProvider());

    // input channels to be passed directly to output, excluding MEASURES columns
    ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
    // output dataTypes, used to construct the output TsBlock, including MEASURES columns
    ImmutableList.Builder<TSDataType> outputDataTypes = ImmutableList.builder();

    if (node.getRowsPerMatch() == ONE) {
      // ONE ROW PER MATCH: partition columns, MEASURES

      // add all partition columns
      outputChannels.addAll(partitionChannels);
      for (int i = 0; i < partitionBySymbols.size(); i++) {
        Symbol symbol = partitionBySymbols.get(i);
        // obtain the absolute index of the symbol in the base table through `childLayout`
        outputDataTypes.add(inputDataTypes.get(childLayout.get(symbol)));
      }
    } else {
      // ALL ROWS PER MATCH: all input columns, MEASURES

      outputChannels.addAll(
          IntStream.range(0, inputDataTypes.size()).boxed().collect(toImmutableList()));
      outputDataTypes.addAll(inputDataTypes);
    }

    // add MEASURES columns
    for (Map.Entry<Symbol, Measure> measure : node.getMeasures().entrySet()) {
      outputDataTypes.add(getTSDataType(measure.getValue().getType()));
    }

    // prepare structures specific to PatternRecognitionNode
    // 1. establish a two-way mapping of IrLabels to `int`
    List<IrLabel> primaryLabels = ImmutableList.copyOf(node.getVariableDefinitions().keySet());
    ImmutableList.Builder<String> labelNamesBuilder = ImmutableList.builder();
    ImmutableMap.Builder<IrLabel, Integer> mappingBuilder = ImmutableMap.builder();
    for (int i = 0; i < primaryLabels.size(); i++) {
      IrLabel label = primaryLabels.get(i);
      labelNamesBuilder.add(label.getName());
      mappingBuilder.put(label, i);
    }
    Map<IrLabel, Integer> mapping = mappingBuilder.buildOrThrow();
    List<String> labelNames = labelNamesBuilder.build();

    // 2. rewrite pattern to program
    Program program = IrRowPatternToProgramRewriter.rewrite(node.getPattern(), mapping);

    // 3. DEFINE: prepare patternVariableComputation (PatternVariableRecognizer is to be
    // instantiated once per partition)

    // during pattern matching, each thread will have a list of aggregations necessary for label
    // evaluations.
    // the list of aggregations for a thread will be produced at thread creation time from this
    // supplier list, respecting the order.
    // pointers in LabelEvaluator and ThreadEquivalence will access aggregations by position in
    // list.
    int matchAggregationIndex = 0;
    ImmutableList.Builder<PatternAggregator> variableRecognizerAggregatorBuilder =
        ImmutableList.builder();
    List<PatternAggregator> variableRecognizerAggregators = ImmutableList.of();

    ImmutableList.Builder<PatternVariableRecognizer.PatternVariableComputation> evaluationsBuilder =
        ImmutableList.builder();

    for (Map.Entry<IrLabel, ExpressionAndValuePointers> entry :
        node.getVariableDefinitions().entrySet()) {
      String variableName = entry.getKey().getName();
      ExpressionAndValuePointers expressionAndValuePointers = entry.getValue();

      // convert the `ValuePointer` in the `Assignment` to `PhysicalValueAccessor`
      List<PhysicalValueAccessor> valueAccessors = new ArrayList<>();
      for (ExpressionAndValuePointers.Assignment assignment :
          expressionAndValuePointers.getAssignments()) {
        ValuePointer pointer = assignment.getValuePointer();
        if (pointer instanceof MatchNumberValuePointer) {
          valueAccessors.add(
              new PhysicalValuePointer(MATCH_NUMBER, INT64, LogicalIndexNavigation.NO_OP));
        } else if (pointer instanceof ClassifierValuePointer) {
          ClassifierValuePointer classifierPointer = (ClassifierValuePointer) pointer;
          valueAccessors.add(
              new PhysicalValuePointer(
                  CLASSIFIER,
                  STRING,
                  new LogicalIndexNavigation(classifierPointer.getLogicalIndexPointer(), mapping)));
        } else if (pointer instanceof ScalarValuePointer) {
          ScalarValuePointer scalarPointer = (ScalarValuePointer) pointer;
          valueAccessors.add(
              new PhysicalValuePointer(
                  getOnlyElement(
                      getChannelsForSymbols(
                          ImmutableList.of(scalarPointer.getInputSymbol()), childLayout)),
                  context.getTableTypeProvider().getTableModelType(scalarPointer.getInputSymbol()),
                  new LogicalIndexNavigation(scalarPointer.getLogicalIndexPointer(), mapping)));
        } else if (pointer instanceof AggregationValuePointer) {
          AggregationValuePointer aggregationPointer = (AggregationValuePointer) pointer;

          ResolvedFunction resolvedFunction = aggregationPointer.getFunction();

          ImmutableList.Builder<Map.Entry<Expression, Type>> builder = ImmutableList.builder();
          List<Type> signatureTypes = resolvedFunction.getSignature().getArgumentTypes();
          for (int i = 0; i < aggregationPointer.getArguments().size(); i++) {
            builder.add(
                new AbstractMap.SimpleEntry<>(
                    aggregationPointer.getArguments().get(i), signatureTypes.get(i)));
          }
          List<Map.Entry<Expression, Type>> arguments = builder.build();

          List<Integer> valueChannels = new ArrayList<>();

          for (Map.Entry<Expression, Type> argumentWithType : arguments) {
            Expression argument = argumentWithType.getKey();
            valueChannels.add(childLayout.get(Symbol.from(argument)));
          }

          AggregationLabelSet labelSet = aggregationPointer.getSetDescriptor();
          Set<Integer> labels =
              labelSet.getLabels().stream().map(mapping::get).collect(Collectors.toSet());
          PatternAggregationTracker patternAggregationTracker =
              new PatternAggregationTracker(
                  labels, aggregationPointer.getSetDescriptor().isRunning());

          PatternAggregator variableRecognizerAggregator =
              buildPatternAggregator(
                  resolvedFunction, arguments, valueChannels, patternAggregationTracker);

          variableRecognizerAggregatorBuilder.add(variableRecognizerAggregator);

          valueAccessors.add(new PhysicalAggregationPointer(matchAggregationIndex));
          matchAggregationIndex++;
        }
      }

      variableRecognizerAggregators = variableRecognizerAggregatorBuilder.build();

      // transform the symbolic expression tree in the logical planning stage into a parametric
      // expression tree
      Computation computation = Computation.ComputationParser.parse(expressionAndValuePointers);

      // construct a `PatternVariableComputation` object, where valueAccessors is a parameter list
      // and computation is a parametric expression tree, encapsulating the computation logic
      PatternVariableRecognizer.PatternVariableComputation patternVariableComputation =
          new PatternVariableRecognizer.PatternVariableComputation(
              valueAccessors, computation, ImmutableList.of(), labelNames);

      evaluationsBuilder.add(patternVariableComputation);
    }

    // 4. MEASURES: prepare measures computations
    ImmutableList.Builder<PatternExpressionComputation> measureComputationsBuilder =
        ImmutableList.builder();

    matchAggregationIndex = 0;
    ImmutableList.Builder<PatternAggregator> measurePatternAggregatorBuilder =
        ImmutableList.builder();
    List<PatternAggregator> measurePatternAggregators = ImmutableList.of();

    for (Measure measure : node.getMeasures().values()) {
      ExpressionAndValuePointers expressionAndValuePointers =
          measure.getExpressionAndValuePointers();

      // convert the `ValuePointer` in the `Assignment` to `PhysicalValueAccessor`
      List<PhysicalValueAccessor> valueAccessors = new ArrayList<>();
      for (ExpressionAndValuePointers.Assignment assignment :
          expressionAndValuePointers.getAssignments()) {
        ValuePointer pointer = assignment.getValuePointer();
        if (pointer instanceof MatchNumberValuePointer) {
          valueAccessors.add(
              new PhysicalValuePointer(MATCH_NUMBER, INT64, LogicalIndexNavigation.NO_OP));
        } else if (pointer instanceof ClassifierValuePointer) {
          ClassifierValuePointer classifierPointer = (ClassifierValuePointer) pointer;
          valueAccessors.add(
              new PhysicalValuePointer(
                  CLASSIFIER,
                  STRING,
                  new LogicalIndexNavigation(classifierPointer.getLogicalIndexPointer(), mapping)));
        } else if (pointer instanceof ScalarValuePointer) {
          ScalarValuePointer scalarPointer = (ScalarValuePointer) pointer;
          valueAccessors.add(
              new PhysicalValuePointer(
                  getOnlyElement(
                      getChannelsForSymbols(
                          ImmutableList.of(scalarPointer.getInputSymbol()), childLayout)),
                  context.getTableTypeProvider().getTableModelType(scalarPointer.getInputSymbol()),
                  new LogicalIndexNavigation(scalarPointer.getLogicalIndexPointer(), mapping)));
        } else if (pointer instanceof AggregationValuePointer) {
          AggregationValuePointer aggregationPointer = (AggregationValuePointer) pointer;

          ResolvedFunction resolvedFunction = aggregationPointer.getFunction();

          ImmutableList.Builder<Map.Entry<Expression, Type>> builder = ImmutableList.builder();
          List<Type> signatureTypes = resolvedFunction.getSignature().getArgumentTypes();
          for (int i = 0; i < aggregationPointer.getArguments().size(); i++) {
            builder.add(
                new AbstractMap.SimpleEntry<>(
                    aggregationPointer.getArguments().get(i), signatureTypes.get(i)));
          }
          List<Map.Entry<Expression, Type>> arguments = builder.build();

          List<Integer> valueChannels = new ArrayList<>();

          for (Map.Entry<Expression, Type> argumentWithType : arguments) {
            Expression argument = argumentWithType.getKey();
            valueChannels.add(childLayout.get(Symbol.from(argument)));
          }

          AggregationLabelSet labelSet = aggregationPointer.getSetDescriptor();
          Set<Integer> labels =
              labelSet.getLabels().stream().map(mapping::get).collect(Collectors.toSet());
          PatternAggregationTracker patternAggregationTracker =
              new PatternAggregationTracker(
                  labels, aggregationPointer.getSetDescriptor().isRunning());

          PatternAggregator measurePatternAggregator =
              buildPatternAggregator(
                  resolvedFunction, arguments, valueChannels, patternAggregationTracker);

          measurePatternAggregatorBuilder.add(measurePatternAggregator);

          valueAccessors.add(new PhysicalAggregationPointer(matchAggregationIndex));
          matchAggregationIndex++;
        }
      }

      measurePatternAggregators = measurePatternAggregatorBuilder.build();

      // transform the symbolic expression tree in the logical planning stage into a parametric
      // expression tree
      Computation computation = Computation.ComputationParser.parse(expressionAndValuePointers);

      // construct a `PatternExpressionComputation` object, where valueAccessors is a parameter
      // list and computation is a parametric expression tree, encapsulating the computation logic.
      PatternExpressionComputation measureComputation =
          new PatternExpressionComputation(valueAccessors, computation, measurePatternAggregators);

      measureComputationsBuilder.add(measureComputation);
    }

    // 5. prepare SKIP TO navigation
    Optional<LogicalIndexNavigation> skipToNavigation = Optional.empty();
    if (!node.getSkipToLabels().isEmpty()) {
      boolean last = node.getSkipToPosition().equals(LAST);
      skipToNavigation =
          Optional.of(
              new LogicalIndexNavigation(
                  new LogicalIndexPointer(node.getSkipToLabels(), last, false, 0, 0), mapping));
    }

    return new PatternRecognitionOperator(
        operatorContext,
        child,
        inputDataTypes,
        outputDataTypes.build(),
        outputChannels.build(),
        partitionChannels,
        sortChannels,
        node.getRowsPerMatch(),
        node.getSkipToPosition(),
        skipToNavigation,
        new Matcher(program, variableRecognizerAggregators),
        evaluationsBuilder.build(),
        measurePatternAggregators,
        measureComputationsBuilder.build(),
        labelNames);
  }

  /**
   * Checks if the ordering column in aggregations matches the time column. only check for
   * FIRST/LAST/FIRST_BY/LAST_BY
   */
  protected boolean checkOrderColumnIsTime(
      Map<Symbol, AggregationNode.Aggregation> aggregations, String timeColumnName) {

    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregations.entrySet()) {
      String functionName =
          entry.getValue().getResolvedFunction().getSignature().getName().toLowerCase();
      List<Expression> arguments = entry.getValue().getArguments();
      Expression lastParam = entry.getValue().getArguments().get(arguments.size() - 1);

      switch (functionName) {
        case FIRST_AGGREGATION:
        case LAST_AGGREGATION:
        case FIRST_BY_AGGREGATION:
        case LAST_BY_AGGREGATION:
          if (!((SymbolReference) lastParam).getName().equalsIgnoreCase(timeColumnName)) {
            return false;
          }
          break;
      }
    }
    return true;
  }

  protected boolean canUseLastRowOptimize(List<TableAggregator> aggregators) {
    for (TableAggregator aggregator : aggregators) {
      if (aggregator.getAccumulator() instanceof LastDescAccumulator) {
        if (!((LastDescAccumulator) aggregator.getAccumulator()).isTimeColumn()) {
          return false;
        }
      } else if (aggregator.getAccumulator() instanceof LastByDescAccumulator) {
        if (!((LastByDescAccumulator) aggregator.getAccumulator()).yIsTimeColumn()) {
          return false;
        }
      } else {
        return false;
      }
    }
    return true;
  }

  protected boolean canUseLastValuesOptimize(List<TableAggregator> aggregators) {
    for (TableAggregator aggregator : aggregators) {
      if (aggregator.getAccumulator() instanceof LastByDescAccumulator) {
        // cannot optimize when x is Measurement or y is not Measurement
        if (((LastByDescAccumulator) aggregator.getAccumulator()).xIsMeasurementColumn()
            || !((LastByDescAccumulator) aggregator.getAccumulator()).yIsMeasurementColumn()) {
          return false;
        }
      } else if (!(aggregator.getAccumulator() instanceof LastDescAccumulator)) {
        return false;
      }
    }
    return true;
  }

  protected enum OptimizeType {
    LAST_ROW,
    LAST_VALUES,
    NOOP
  }

  @Override
  public Operator visitMarkDistinct(MarkDistinctNode node, C context) {
    Operator child = node.getChild().accept(this, context);
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), MarkDistinctOperator.class.getSimpleName());

    ITableTypeProvider typeProvider = context.getTableTypeProvider();
    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());

    return new MarkDistinctOperator(
        operatorContext,
        child,
        node.getChild().getOutputSymbols().stream()
            .map(typeProvider::getTableModelType)
            .collect(Collectors.toList()),
        node.getDistinctSymbols().stream().map(childLayout::get).collect(Collectors.toList()),
        Optional.empty());
  }

  @Override
  public Operator visitWindowFunction(WindowNode node, C context) {
    ITableTypeProvider typeProvider = context.getTableTypeProvider();
    Operator child = node.getChild().accept(this, context);
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), TableWindowOperator.class.getSimpleName());

    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());

    // Partition channel
    List<Symbol> partitionBySymbols = node.getSpecification().getPartitionBy();
    List<Integer> partitionChannels =
        ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, childLayout));

    // Sort channel
    List<Integer> sortChannels = ImmutableList.of();
    List<SortOrder> sortOrder = ImmutableList.of();
    if (node.getSpecification().getOrderingScheme().isPresent()) {
      OrderingScheme orderingScheme = node.getSpecification().getOrderingScheme().get();
      sortChannels = getChannelsForSymbols(orderingScheme.getOrderBy(), childLayout);
      sortOrder = orderingScheme.getOrderingList();
    }

    // Output channel
    ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
    List<TSDataType> outputDataTypes = new ArrayList<>();
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTableTypeProvider());
    for (int i = 0; i < inputDataTypes.size(); i++) {
      outputChannels.add(i);
      outputDataTypes.add(inputDataTypes.get(i));
    }

    // Window functions
    List<FrameInfo> frameInfoList = new ArrayList<>();
    List<WindowFunction> windowFunctions = new ArrayList<>();
    List<Symbol> windowFunctionOutputSymbols = new ArrayList<>();
    List<TSDataType> windowFunctionOutputDataTypes = new ArrayList<>();
    for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
      // Create FrameInfo
      WindowNode.Frame frame = entry.getValue().getFrame();

      Optional<Integer> frameStartChannel = Optional.empty();
      if (frame.getStartValue().isPresent()) {
        frameStartChannel = Optional.ofNullable(childLayout.get(frame.getStartValue().get()));
      }
      Optional<Integer> frameEndChannel = Optional.empty();
      if (frame.getEndValue().isPresent()) {
        frameEndChannel = Optional.ofNullable(childLayout.get(frame.getEndValue().get()));
      }

      Optional<Integer> sortKeyChannel = Optional.empty();
      Optional<SortOrder> ordering = Optional.empty();
      if (node.getSpecification().getOrderingScheme().isPresent()) {
        sortKeyChannel = Optional.of(sortChannels.get(0));
        if (sortOrder.get(0).isNullsFirst()) {
          if (sortOrder.get(0).isAscending()) {
            ordering = Optional.of(ASC_NULLS_FIRST);
          } else {
            ordering = Optional.of(DESC_NULLS_FIRST);
          }
        } else {
          if (sortOrder.get(0).isAscending()) {
            ordering = Optional.of(ASC_NULLS_LAST);
          } else {
            ordering = Optional.of(DESC_NULLS_LAST);
          }
        }
      }
      FrameInfo frameInfo =
          new FrameInfo(
              frame.getType(),
              frame.getStartType(),
              frameStartChannel,
              frame.getEndType(),
              frameEndChannel,
              sortKeyChannel,
              ordering);
      frameInfoList.add(frameInfo);

      // Arguments
      WindowNode.Function function = entry.getValue();
      ResolvedFunction resolvedFunction = function.getResolvedFunction();
      List<Integer> argumentChannels = new ArrayList<>();
      for (Expression argument : function.getArguments()) {
        Symbol argumentSymbol = Symbol.from(argument);
        argumentChannels.add(childLayout.get(argumentSymbol));
      }

      // Return value
      Type returnType = resolvedFunction.getSignature().getReturnType();
      windowFunctionOutputDataTypes.add(getTSDataType(returnType));

      // Window function
      Symbol symbol = entry.getKey();
      WindowFunction windowFunction;
      FunctionKind functionKind = resolvedFunction.getFunctionKind();
      if (functionKind == FunctionKind.AGGREGATE) {
        WindowAggregator tableWindowAggregator =
            buildWindowAggregator(symbol, function, typeProvider, argumentChannels);
        windowFunction = new AggregationWindowFunction(tableWindowAggregator);
      } else if (functionKind == FunctionKind.WINDOW) {
        String functionName = function.getResolvedFunction().getSignature().getName();
        windowFunction =
            WindowFunctionFactory.createBuiltinWindowFunction(
                functionName, argumentChannels, function.isIgnoreNulls());
      } else {
        throw new UnsupportedOperationException("Unsupported function kind: " + functionKind);
      }

      windowFunctions.add(windowFunction);
      windowFunctionOutputSymbols.add(symbol);
    }

    // Compute layout
    ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
    for (Symbol symbol : node.getChild().getOutputSymbols()) {
      outputMappings.put(symbol, childLayout.get(symbol));
    }
    int channel = inputDataTypes.size();

    for (Symbol symbol : windowFunctionOutputSymbols) {
      outputMappings.put(symbol, channel);
      channel++;
    }

    outputDataTypes.addAll(windowFunctionOutputDataTypes);
    return new TableWindowOperator(
        operatorContext,
        child,
        inputDataTypes,
        outputDataTypes,
        outputChannels.build(),
        windowFunctions,
        frameInfoList,
        partitionChannels,
        sortChannels);
  }

  private WindowAggregator buildWindowAggregator(
      Symbol symbol,
      WindowNode.Function function,
      ITableTypeProvider typeProvider,
      List<Integer> argumentChannels) {
    // Create accumulator first
    String functionName = function.getResolvedFunction().getSignature().getName();
    List<TSDataType> originalArgumentTypes =
        function.getResolvedFunction().getSignature().getArgumentTypes().stream()
            .map(InternalTypeManager::getTSDataType)
            .collect(Collectors.toList());
    TableAccumulator accumulator =
        createBuiltinAccumulator(getAggregationTypeByFuncName(functionName), originalArgumentTypes);

    // Create aggregator by accumulator
    return new WindowAggregator(
        accumulator, getTSDataType(typeProvider.getTableModelType(symbol)), argumentChannels);
  }

  @Override
  public Operator visitUnion(UnionNode node, C context) {
    List<Operator> children =
        node.getChildren().stream()
            .map(child -> child.accept(this, context))
            .collect(Collectors.toList());
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), MappingCollectOperator.class.getSimpleName());

    int size = children.size();
    List<List<Integer>> mappings = new ArrayList<>(size);
    List<Symbol> unionOutputs = node.getOutputSymbols();
    ListMultimap<Symbol, Symbol> outputToInputs = node.getSymbolMapping();
    for (int i = 0; i < size; i++) {
      Map<Symbol, Integer> childOutputs =
          makeLayoutFromOutputSymbols(node.getChildren().get(i).getOutputSymbols());
      int finalI = i;
      mappings.add(
          unionOutputs.stream()
              .map(symbol -> childOutputs.get(outputToInputs.get(symbol).get(finalI)))
              .collect(Collectors.toList()));
    }
    return new MappingCollectOperator(operatorContext, children, mappings);
  }

  @Override
  public Operator visitValuesNode(ValuesNode node, C context) {
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), MappingCollectOperator.class.getSimpleName());

    // Currently we only support empty values operator
    assert node.getRowCount() == 0;
    return new ValuesOperator(operatorContext, ImmutableList.of());
  }

  @Override
  public Operator visitRowNumber(RowNumberNode node, C context) {
    Operator child = node.getChild().accept(this, context);
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), MappingCollectOperator.class.getSimpleName());

    List<Symbol> partitionBySymbols = node.getPartitionBy();
    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());
    List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, childLayout);
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTableTypeProvider());

    ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
    for (int i = 0; i < inputDataTypes.size(); i++) {
      outputChannels.add(i);
    }

    // compute the layout of the output from the window operator
    ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
    outputMappings.putAll(childLayout);

    // row number function goes in the last channel
    int channel = inputDataTypes.size();
    outputMappings.put(node.getRowNumberSymbol(), channel);

    return new RowNumberOperator(
        operatorContext,
        child,
        inputDataTypes,
        outputChannels.build(),
        partitionChannels,
        node.getMaxRowCountPerPartition(),
        10_000);
  }

  @Override
  public Operator visitTopKRanking(TopKRankingNode node, C context) {
    Operator child = node.getChild().accept(this, context);
    CommonOperatorContext operatorContext =
        addOperatorContext(
            context, node.getPlanNodeId(), MappingCollectOperator.class.getSimpleName());

    List<Symbol> partitionBySymbols = node.getSpecification().getPartitionBy();
    Map<Symbol, Integer> childLayout =
        makeLayoutFromOutputSymbols(node.getChild().getOutputSymbols());
    List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, childLayout);
    List<TSDataType> inputDataTypes =
        getOutputColumnTypes(node.getChild(), context.getTableTypeProvider());
    List<TSDataType> partitionTypes =
        partitionChannels.stream().map(inputDataTypes::get).collect(toImmutableList());

    List<Symbol> orderBySymbols = new ArrayList<>();
    Optional<OrderingScheme> orderingScheme = node.getSpecification().getOrderingScheme();
    if (orderingScheme.isPresent()) {
      orderBySymbols = orderingScheme.get().getOrderBy();
    }

    List<SortOrder> sortOrder = new ArrayList<>();
    List<Integer> sortChannels = getChannelsForSymbols(orderBySymbols, childLayout);
    if (orderingScheme.isPresent()) {
      sortOrder =
          orderBySymbols.stream()
              .map(symbol -> orderingScheme.get().getOrdering(symbol))
              .collect(toImmutableList());
    }

    ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
    for (int i = 0; i < inputDataTypes.size(); i++) {
      outputChannels.add(i);
    }

    // compute the layout of the output from the window operator
    ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
    outputMappings.putAll(childLayout);

    if (!node.isPartial() || !partitionChannels.isEmpty()) {
      // ranking function goes in the last channel
      int channel = inputDataTypes.size();
      outputMappings.put(node.getRankingSymbol(), channel);
    }

    return new TopKRankingOperator(
        operatorContext,
        child,
        node.getRankingType(),
        inputDataTypes,
        outputChannels.build(),
        partitionChannels,
        partitionTypes,
        sortChannels,
        sortOrder,
        node.getMaxRankingPerPartition(),
        node.isPartial(),
        Optional.empty(),
        1000,
        Optional.empty());
  }

  protected abstract SessionInfo getSessionInfo(C context);
}
