/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis.GroupingSetAnalysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.FieldId;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GapFillStartAndEndTimeExtractVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Aggregation;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Fill;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FrameBound;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.MeasureDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Offset;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QueryBody;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.VariableDefinition;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WindowFrame;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.tsfile.read.common.type.Type;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isNumericType;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingTranslator.sortItemToSortOrder;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanBuilder.newPlanBuilder;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ScopeAware.scopeAwareKey;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator.GROUP_KEY_SUFFIX;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GapFillStartAndEndTimeExtractVisitor.CAN_NOT_INFER_TIME_RANGE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.groupingSets;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleAggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WindowFrame.Type.GROUPS;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WindowFrame.Type.RANGE;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WindowFrame.Type.ROWS;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toSqlType;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.NodeUtils.getSortItemsFromOrderBy;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;

public class QueryPlanner {
  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final MPPQueryContext queryContext;
  private final QueryId queryIdAllocator;
  private final SessionInfo session;
  private final SubqueryPlanner subqueryPlanner;
  private final Optional<TranslationMap> outerContext;
  private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

  // private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
  // private final SubqueryPlanner subqueryPlanner;

  public QueryPlanner(
      Analysis analysis,
      SymbolAllocator symbolAllocator,
      MPPQueryContext queryContext,
      Optional<TranslationMap> outerContext,
      SessionInfo session,
      Map<NodeRef<Node>, RelationPlan> recursiveSubqueries) {
    requireNonNull(analysis, "analysis is null");
    requireNonNull(symbolAllocator, "symbolAllocator is null");
    requireNonNull(queryContext, "queryContext is null");
    requireNonNull(outerContext, "outerContext is null");
    requireNonNull(session, "session is null");
    requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
    this.queryContext = queryContext;
    this.queryIdAllocator = queryContext.getQueryId();
    this.session = session;
    this.outerContext = outerContext;
    this.subqueryPlanner =
        new SubqueryPlanner(
            analysis, symbolAllocator, queryContext, outerContext, session, recursiveSubqueries);
    this.recursiveSubqueries = recursiveSubqueries;
  }

  public RelationPlan plan(Query query) {
    PlanBuilder builder = planQueryBody(query.getQueryBody());

    builder = fill(builder, query.getFill());

    // TODO result is :input[0], :input[1], :input[2]
    List<Analysis.SelectExpression> selectExpressions = analysis.getSelectExpressions(query);
    List<Expression> outputs =
        selectExpressions.stream()
            .map(Analysis.SelectExpression::getExpression)
            .collect(toImmutableList());

    List<Expression> orderBy = analysis.getOrderByExpressions(query);
    if (!orderBy.isEmpty()) {
      builder =
          builder.appendProjections(
              Iterables.concat(orderBy, outputs), symbolAllocator, queryContext);
    }
    Optional<OrderingScheme> orderingScheme =
        orderingScheme(builder, query.getOrderBy(), analysis.getOrderByExpressions(query));
    builder = sort(builder, orderingScheme);
    builder = offset(builder, query.getOffset());
    builder = limit(builder, query.getLimit(), orderingScheme);
    builder = builder.appendProjections(outputs, symbolAllocator, queryContext);

    return new RelationPlan(
        builder.getRoot(),
        analysis.getScope(query),
        computeOutputs(builder, outputs),
        outerContext);
  }

  public RelationPlan plan(QuerySpecification node) {
    PlanBuilder builder = planFrom(node);

    builder = filter(builder, analysis.getWhere(node), node);
    Expression wherePredicate = null;
    if (builder.getRoot() instanceof FilterNode) {
      wherePredicate = ((FilterNode) builder.getRoot()).getPredicate();
    }

    Symbol timeColumnForGapFill = null;
    FunctionCall gapFillColumn = analysis.getGapFill(node);
    if (gapFillColumn != null) {
      timeColumnForGapFill = builder.translate((Expression) gapFillColumn.getChildren().get(2));
    }
    builder = aggregate(builder, node);
    builder = filter(builder, analysis.getHaving(node), node);
    builder =
        planWindowFunctions(node, builder, ImmutableList.copyOf(analysis.getWindowFunctions(node)));

    if (gapFillColumn != null) {
      if (wherePredicate == null) {
        throw new SemanticException(CAN_NOT_INFER_TIME_RANGE);
      }
      builder =
          gapFill(
              builder,
              timeColumnForGapFill,
              gapFillColumn,
              analysis.getGapFillGroupingKeys(node),
              wherePredicate);
    }

    List<Analysis.SelectExpression> selectExpressions = analysis.getSelectExpressions(node);
    List<Expression> expressions =
        selectExpressions.stream()
            .map(Analysis.SelectExpression::getExpression)
            .collect(toImmutableList());
    builder = subqueryPlanner.handleSubqueries(builder, expressions, analysis.getSubqueries(node));

    if (hasExpressionsToUnfold(selectExpressions)) {
      // pre-project the folded expressions to preserve any non-deterministic semantics of functions
      // that might be referenced
      builder = builder.appendProjections(expressions, symbolAllocator, queryContext);
    }

    List<Expression> outputs = outputExpressions(selectExpressions);

    if (node.getFill().isPresent()) {
      // Add projections for the outputs of SELECT, but stack them on top of the ones from the FROM
      // clause so both are visible
      // when resolving the ORDER BY clause.
      builder = builder.appendProjections(outputs, symbolAllocator, queryContext);
      // The new scope is the composite of the fields from the FROM and SELECT clause (local nested
      // scopes). Fields from the bottom of
      // the scope stack need to be placed first to match the expected layout for nested scopes.
      List<Symbol> newFields = new ArrayList<>(builder.getTranslations().getFieldSymbolsList());

      outputs.stream().map(builder::translate).forEach(newFields::add);

      builder = builder.withScope(analysis.getScope(node.getFill().get()), newFields);
      builder = fill(builder, node.getFill());
    }

    if (node.getOrderBy().isPresent()) {
      // ORDER BY requires outputs of SELECT to be visible.
      // For queries with aggregation, it also requires grouping keys and translated aggregations.
      if (analysis.isAggregation(node)) {
        // Add projections for aggregations required by ORDER BY. After this step, grouping keys and
        // translated
        // aggregations are visible.
        List<Expression> orderByAggregates =
            analysis.getOrderByAggregates(node.getOrderBy().orElse(null));
        builder = builder.appendProjections(orderByAggregates, symbolAllocator, queryContext);
      }

      // Add projections for the outputs of SELECT, but stack them on top of the ones from the FROM
      // clause so both are visible
      // when resolving the ORDER BY clause.
      builder = builder.appendProjections(outputs, symbolAllocator, queryContext);

      // The new scope is the composite of the fields from the FROM and SELECT clause (local nested
      // scopes). Fields from the bottom of
      // the scope stack need to be placed first to match the expected layout for nested scopes.
      List<Symbol> newFields = new ArrayList<>(builder.getTranslations().getFieldSymbolsList());

      outputs.stream().map(builder::translate).forEach(newFields::add);

      builder = builder.withScope(analysis.getScope(node.getOrderBy().orElse(null)), newFields);
      builder =
          planWindowFunctions(
              node,
              builder,
              ImmutableList.copyOf(analysis.getOrderByWindowFunctions(node.getOrderBy().get())));
      analysis.setSortNode(true);
    }

    List<Expression> orderBy = analysis.getOrderByExpressions(node);
    if (!orderBy.isEmpty() || node.getSelect().isDistinct()) {
      builder =
          builder.appendProjections(
              Iterables.concat(orderBy, outputs), symbolAllocator, queryContext);
    }

    builder = distinct(builder, node, outputs);
    Optional<OrderingScheme> orderingScheme =
        orderingScheme(builder, node.getOrderBy(), analysis.getOrderByExpressions(node));
    builder = sort(builder, orderingScheme);
    builder = offset(builder, node.getOffset());
    builder = limit(builder, node.getLimit(), orderingScheme);

    builder = builder.appendProjections(outputs, symbolAllocator, queryContext);

    return new RelationPlan(
        builder.getRoot(), analysis.getScope(node), computeOutputs(builder, outputs), outerContext);
  }

  private PlanBuilder planWindowFunctions(
      Node node, PlanBuilder subPlan, List<FunctionCall> windowFunctions) {
    if (windowFunctions.isEmpty()) {
      return subPlan;
    }

    Map<Analysis.ResolvedWindow, List<FunctionCall>> functions =
        scopeAwareDistinct(subPlan, windowFunctions).stream()
            .collect(Collectors.groupingBy(analysis::getWindow));

    for (Map.Entry<Analysis.ResolvedWindow, List<FunctionCall>> entry : functions.entrySet()) {
      Analysis.ResolvedWindow window = entry.getKey();
      List<FunctionCall> functionCalls = entry.getValue();

      // Pre-project inputs.
      // Predefined window parts (specified in WINDOW clause) can only use source symbols, and no
      // output symbols.
      // It matters in case when this window planning takes place in ORDER BY clause, where both
      // source and output
      // symbols are visible.
      // This issue is solved by analyzing window definitions in the source scope. After analysis,
      // the expressions
      // are recorded as belonging to the source scope, and consequentially source symbols will be
      // used to plan them.
      ImmutableList.Builder<Expression> inputsBuilder =
          ImmutableList.<Expression>builder()
              .addAll(window.getPartitionBy())
              .addAll(
                  getSortItemsFromOrderBy(window.getOrderBy()).stream()
                      .map(SortItem::getSortKey)
                      .iterator());

      if (window.getFrame().isPresent()) {
        WindowFrame frame = window.getFrame().get();
        frame.getStart().getValue().ifPresent(inputsBuilder::add);

        if (frame.getEnd().isPresent()) {
          frame.getEnd().get().getValue().ifPresent(inputsBuilder::add);
        }
      }

      for (FunctionCall windowFunction : functionCalls) {
        inputsBuilder.addAll(new ArrayList<>(windowFunction.getArguments()));
      }

      List<Expression> inputs = inputsBuilder.build();

      subPlan = subqueryPlanner.handleSubqueries(subPlan, inputs, analysis.getSubqueries(node));
      subPlan = subPlan.appendProjections(inputs, symbolAllocator, queryContext);

      // Add projection to coerce inputs to their site-specific types.
      // This is important because the same lexical expression may need to be coerced
      // in different ways if it's referenced by multiple arguments to the window function.
      // For example, given v::integer,
      //    avg(v) OVER (ORDER BY v)
      // Needs to be rewritten as
      //    avg(CAST(v AS double)) OVER (ORDER BY v)
      PlanAndMappings coercions =
          coerce(subPlan, inputs, analysis, queryIdAllocator, symbolAllocator);
      subPlan = coercions.getSubPlan();

      // For frame of type RANGE, append casts and functions necessary for frame bound calculations
      Optional<Symbol> frameStart = Optional.empty();
      Optional<Symbol> frameEnd = Optional.empty();
      Optional<Symbol> sortKeyCoercedForFrameStartComparison = Optional.empty();
      Optional<Symbol> sortKeyCoercedForFrameEndComparison = Optional.empty();

      if (window.getFrame().isPresent() && window.getFrame().get().getType() == RANGE) {
        Optional<Expression> startValue = window.getFrame().get().getStart().getValue();
        Optional<Expression> endValue =
            window.getFrame().get().getEnd().flatMap(FrameBound::getValue);
        // record sortKey coercions for reuse
        Map<Type, Symbol> sortKeyCoercions = new HashMap<>();

        // process frame start
        FrameBoundPlanAndSymbols plan =
            planFrameBound(subPlan, coercions, startValue, window, sortKeyCoercions);
        subPlan = plan.getSubPlan();
        frameStart = plan.getFrameBoundSymbol();
        sortKeyCoercedForFrameStartComparison = plan.getSortKeyCoercedForFrameBoundComparison();

        // process frame end
        plan = planFrameBound(subPlan, coercions, endValue, window, sortKeyCoercions);
        subPlan = plan.getSubPlan();
        frameEnd = plan.getFrameBoundSymbol();
        sortKeyCoercedForFrameEndComparison = plan.getSortKeyCoercedForFrameBoundComparison();
      } else if (window.getFrame().isPresent()
          && (window.getFrame().get().getType() == ROWS
              || window.getFrame().get().getType() == GROUPS)) {
        Optional<Expression> startValue = window.getFrame().get().getStart().getValue();
        Optional<Expression> endValue =
            window.getFrame().get().getEnd().flatMap(FrameBound::getValue);

        // process frame start
        FrameOffsetPlanAndSymbol plan = planFrameOffset(subPlan, startValue, coercions);
        subPlan = plan.getSubPlan();
        frameStart = plan.getFrameOffsetSymbol();

        // process frame end
        plan = planFrameOffset(subPlan, endValue, coercions);
        subPlan = plan.getSubPlan();
        frameEnd = plan.getFrameOffsetSymbol();
      } else if (window.getFrame().isPresent()) {
        throw new IllegalArgumentException(
            "unexpected window frame type: " + window.getFrame().get().getType());
      }

      subPlan =
          planWindow(
              subPlan,
              functionCalls,
              window,
              coercions,
              frameStart,
              sortKeyCoercedForFrameStartComparison,
              frameEnd,
              sortKeyCoercedForFrameEndComparison);
    }

    return subPlan;
  }

  private PlanBuilder planWindow(
      PlanBuilder subPlan,
      List<FunctionCall> windowFunctions,
      Analysis.ResolvedWindow window,
      PlanAndMappings coercions,
      Optional<Symbol> frameStartSymbol,
      Optional<Symbol> sortKeyCoercedForFrameStartComparison,
      Optional<Symbol> frameEndSymbol,
      Optional<Symbol> sortKeyCoercedForFrameEndComparison) {
    WindowFrame.Type frameType = WindowFrame.Type.RANGE;
    FrameBound.Type frameStartType = FrameBound.Type.UNBOUNDED_PRECEDING;
    FrameBound.Type frameEndType = FrameBound.Type.CURRENT_ROW;

    Optional<Expression> frameStartExpression = Optional.empty();
    Optional<Expression> frameEndExpression = Optional.empty();

    if (window.getFrame().isPresent()) {
      WindowFrame frame = window.getFrame().get();
      frameType = frame.getType();

      frameStartType = frame.getStart().getType();
      frameStartExpression = frame.getStart().getValue();

      if (frame.getEnd().isPresent()) {
        frameEndType = frame.getEnd().get().getType();
        frameEndExpression = frame.getEnd().get().getValue();
      }
    }

    DataOrganizationSpecification specification =
        planWindowSpecification(window.getPartitionBy(), window.getOrderBy(), coercions::get);

    // Rewrite frame bounds in terms of pre-projected inputs
    WindowNode.Frame frame =
        new WindowNode.Frame(
            frameType,
            frameStartType,
            frameStartSymbol,
            sortKeyCoercedForFrameStartComparison,
            frameEndType,
            frameEndSymbol,
            sortKeyCoercedForFrameEndComparison,
            frameStartExpression,
            frameEndExpression);

    ImmutableMap.Builder<ScopeAware<Expression>, Symbol> mappings = ImmutableMap.builder();
    ImmutableMap.Builder<Symbol, WindowNode.Function> functions = ImmutableMap.builder();

    for (FunctionCall windowFunction : windowFunctions) {
      Symbol newSymbol =
          symbolAllocator.newSymbol(windowFunction, analysis.getType(windowFunction));

      FunctionCall.NullTreatment nullTreatment =
          windowFunction.getNullTreatment().orElse(FunctionCall.NullTreatment.RESPECT);

      WindowNode.Function function =
          new WindowNode.Function(
              analysis.getResolvedFunction(windowFunction),
              windowFunction.getArguments().stream()
                  .map(argument -> coercions.get(argument).toSymbolReference())
                  .collect(toImmutableList()),
              frame,
              nullTreatment == FunctionCall.NullTreatment.IGNORE);

      functions.put(newSymbol, function);
      mappings.put(scopeAwareKey(windowFunction, analysis, subPlan.getScope()), newSymbol);
    }

    // Create GroupNode
    List<Symbol> sortSymbols = new ArrayList<>();
    Map<Symbol, SortOrder> sortOrderings = new HashMap<>();
    for (Symbol symbol : specification.getPartitionBy()) {
      sortSymbols.add(symbol);
      sortOrderings.put(symbol, ASC_NULLS_LAST);
    }
    int sortKeyOffset = sortSymbols.size();
    specification
        .getOrderingScheme()
        .ifPresent(
            orderingScheme -> {
              for (Symbol symbol : orderingScheme.getOrderBy()) {
                if (!sortOrderings.containsKey(symbol)) {
                  sortSymbols.add(symbol);
                  sortOrderings.put(symbol, orderingScheme.getOrdering(symbol));
                }
              }
            });

    PlanBuilder planBuilder = null;
    if (!sortSymbols.isEmpty()) {
      GroupNode groupNode =
          new GroupNode(
              queryIdAllocator.genPlanNodeId(),
              subPlan.getRoot(),
              new OrderingScheme(sortSymbols, sortOrderings),
              sortKeyOffset);
      planBuilder =
          new PlanBuilder(
              subPlan.getTranslations().withAdditionalMappings(mappings.buildOrThrow()), groupNode);
    }

    // create window node
    return new PlanBuilder(
        subPlan.getTranslations().withAdditionalMappings(mappings.buildOrThrow()),
        new WindowNode(
            queryIdAllocator.genPlanNodeId(),
            planBuilder != null ? planBuilder.getRoot() : subPlan.getRoot(),
            specification,
            functions.buildOrThrow(),
            Optional.empty(),
            ImmutableSet.of(),
            0));
  }

  public static DataOrganizationSpecification planWindowSpecification(
      List<Expression> partitionBy,
      Optional<OrderBy> orderBy,
      Function<Expression, Symbol> expressionRewrite) {
    // Rewrite PARTITION BY
    ImmutableList.Builder<Symbol> partitionBySymbols = ImmutableList.builder();
    for (Expression expression : partitionBy) {
      partitionBySymbols.add(expressionRewrite.apply(expression));
    }

    // Rewrite ORDER BY
    LinkedHashMap<Symbol, SortOrder> orderings = new LinkedHashMap<>();
    for (SortItem item : getSortItemsFromOrderBy(orderBy)) {
      Symbol symbol = expressionRewrite.apply(item.getSortKey());
      // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
      orderings.putIfAbsent(symbol, sortItemToSortOrder(item));
    }

    Optional<OrderingScheme> orderingScheme = Optional.empty();
    if (!orderings.isEmpty()) {
      orderingScheme =
          Optional.of(new OrderingScheme(ImmutableList.copyOf(orderings.keySet()), orderings));
    }

    return new DataOrganizationSpecification(partitionBySymbols.build(), orderingScheme);
  }

  private FrameBoundPlanAndSymbols planFrameBound(
      PlanBuilder subPlan,
      PlanAndMappings coercions,
      Optional<Expression> frameOffset,
      Analysis.ResolvedWindow window,
      Map<Type, Symbol> sortKeyCoercions) {
    // We don't need frameBoundCalculationFunction
    if (!frameOffset.isPresent()) {
      return new FrameBoundPlanAndSymbols(subPlan, Optional.empty(), Optional.empty());
    }

    // Append filter to validate offset values. They mustn't be negative or null.
    Symbol offsetSymbol = coercions.get(frameOffset.get());
    Expression zeroOffset = zeroOfType(symbolAllocator.getTypes().getTableModelType(offsetSymbol));
    Expression predicate =
        new IfExpression(
            new ComparisonExpression(
                GREATER_THAN_OR_EQUAL, offsetSymbol.toSymbolReference(), zeroOffset),
            TRUE_LITERAL,
            new Cast(new NullLiteral(), toSqlType(BOOLEAN)));
    subPlan =
        subPlan.withNewRoot(
            new FilterNode(queryIdAllocator.genPlanNodeId(), subPlan.getRoot(), predicate));

    // Then, coerce the sortKey so that we can add / subtract the offset.
    // Note: for that we cannot rely on the usual mechanism of using the coerce() method. The
    // coerce() method can only handle one coercion for a node,
    // while the sortKey node might require several different coercions, e.g. one for frame start
    // and one for frame end.
    Expression sortKey =
        Iterables.getOnlyElement(window.getOrderBy().get().getSortItems()).getSortKey();
    Symbol sortKeyCoercedForFrameBoundCalculation = coercions.get(sortKey);
    Optional<Type> coercion = frameOffset.map(analysis::getSortKeyCoercionForFrameBoundCalculation);
    if (coercion.isPresent()) {
      Type expectedType = coercion.get();
      Symbol alreadyCoerced = sortKeyCoercions.get(expectedType);
      if (alreadyCoerced != null) {
        sortKeyCoercedForFrameBoundCalculation = alreadyCoerced;
      } else {
        Expression cast =
            new Cast(
                coercions.get(sortKey).toSymbolReference(),
                toSqlType(expectedType),
                false,
                analysis.getType(sortKey).equals(expectedType));
        sortKeyCoercedForFrameBoundCalculation = symbolAllocator.newSymbol(cast, expectedType);
        sortKeyCoercions.put(expectedType, sortKeyCoercedForFrameBoundCalculation);
        subPlan =
            subPlan.withNewRoot(
                new ProjectNode(
                    queryIdAllocator.genPlanNodeId(),
                    subPlan.getRoot(),
                    Assignments.builder()
                        .putIdentities(subPlan.getRoot().getOutputSymbols())
                        .put(sortKeyCoercedForFrameBoundCalculation, cast)
                        .build()));
      }
    }

    // Finally, coerce the sortKey to the type of frameBound so that the operator can perform
    // comparisons on them
    Optional<Symbol> sortKeyCoercedForFrameBoundComparison = Optional.of(coercions.get(sortKey));
    coercion = frameOffset.map(analysis::getSortKeyCoercionForFrameBoundComparison);
    if (coercion.isPresent()) {
      Type expectedType = coercion.get();
      Symbol alreadyCoerced = sortKeyCoercions.get(expectedType);
      if (alreadyCoerced != null) {
        sortKeyCoercedForFrameBoundComparison = Optional.of(alreadyCoerced);
      } else {
        Expression cast =
            new Cast(
                coercions.get(sortKey).toSymbolReference(),
                toSqlType(expectedType),
                false,
                analysis.getType(sortKey).equals(expectedType));
        Symbol castSymbol = symbolAllocator.newSymbol(cast, expectedType);
        sortKeyCoercions.put(expectedType, castSymbol);
        subPlan =
            subPlan.withNewRoot(
                new ProjectNode(
                    queryIdAllocator.genPlanNodeId(),
                    subPlan.getRoot(),
                    Assignments.builder()
                        .putIdentities(subPlan.getRoot().getOutputSymbols())
                        .put(castSymbol, cast)
                        .build()));
        sortKeyCoercedForFrameBoundComparison = Optional.of(castSymbol);
      }
    }

    Symbol frameBoundSymbol = coercions.get(frameOffset.get());
    return new FrameBoundPlanAndSymbols(
        subPlan, Optional.of(frameBoundSymbol), sortKeyCoercedForFrameBoundComparison);
  }

  private FrameOffsetPlanAndSymbol planFrameOffset(
      PlanBuilder subPlan, Optional<Expression> frameOffset, PlanAndMappings coercions) {
    if (!frameOffset.isPresent()) {
      return new FrameOffsetPlanAndSymbol(subPlan, Optional.empty());
    }

    // Report error if frame offsets are literals and they are negative or null
    if (frameOffset.get() instanceof LongLiteral) {
      long frameOffsetValue = ((LongLiteral) frameOffset.get()).getParsedValue();
      if (frameOffsetValue < 0) {
        throw new SemanticException("Window frame offset value must not be negative or null");
      }
    }

    Symbol offsetSymbol = frameOffset.map(coercions::get).get();
    Type offsetType = symbolAllocator.getTypes().getTableModelType(offsetSymbol);

    // Append filter to validate offset values. They mustn't be negative or null.
    Expression zeroOffset = zeroOfType(offsetType);
    Expression predicate =
        new IfExpression(
            new ComparisonExpression(
                GREATER_THAN_OR_EQUAL, offsetSymbol.toSymbolReference(), zeroOffset),
            TRUE_LITERAL,
            new Cast(new NullLiteral(), toSqlType(BOOLEAN)));
    subPlan =
        subPlan.withNewRoot(
            new FilterNode(queryIdAllocator.genPlanNodeId(), subPlan.getRoot(), predicate));

    return new FrameOffsetPlanAndSymbol(subPlan, Optional.of(offsetSymbol));
  }

  private static Expression zeroOfType(Type type) {
    if (isNumericType(type)) {
      return new Cast(new LongLiteral("0"), toSqlType(type));
    }
    throw new IllegalArgumentException("unexpected type: " + type);
  }

  private static boolean hasExpressionsToUnfold(List<Analysis.SelectExpression> selectExpressions) {
    return selectExpressions.stream()
        .map(Analysis.SelectExpression::getUnfoldedExpressions)
        .anyMatch(Optional::isPresent);
  }

  private static List<Expression> outputExpressions(
      List<Analysis.SelectExpression> selectExpressions) {
    ImmutableList.Builder<Expression> result = ImmutableList.builder();
    for (Analysis.SelectExpression selectExpression : selectExpressions) {
      if (selectExpression.getUnfoldedExpressions().isPresent()) {
        result.addAll(selectExpression.getUnfoldedExpressions().get());
      } else {
        result.add(selectExpression.getExpression());
      }
    }
    return result.build();
  }

  public PlanNode plan(Delete node) {
    // implement delete logic
    return null;
  }

  private static List<Symbol> computeOutputs(
      PlanBuilder builder, List<Expression> outputExpressions) {
    ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
    for (Expression expression : outputExpressions) {
      outputSymbols.add(builder.translate(expression));
    }
    return outputSymbols.build();
  }

  private PlanBuilder planQueryBody(QueryBody queryBody) {
    RelationPlan relationPlan =
        new RelationPlanner(
                analysis, symbolAllocator, queryContext, outerContext, session, recursiveSubqueries)
            .process(queryBody, null);

    return newPlanBuilder(relationPlan, analysis);
  }

  private PlanBuilder planFrom(QuerySpecification node) {
    if (node.getFrom().isPresent()) {
      RelationPlan relationPlan =
          new RelationPlanner(
                  analysis,
                  symbolAllocator,
                  queryContext,
                  outerContext,
                  session,
                  recursiveSubqueries)
              .process(node.getFrom().orElse(null), null);
      return newPlanBuilder(relationPlan, analysis);
    } else {
      throw new IllegalStateException("From clause must not by empty");
    }
  }

  private PlanBuilder filter(PlanBuilder subPlan, Expression predicate, Node node) {
    if (predicate == null) {
      return subPlan;
    }

    subPlan = subqueryPlanner.handleSubqueries(subPlan, predicate, analysis.getSubqueries(node));
    return subPlan.withNewRoot(
        new FilterNode(
            queryIdAllocator.genPlanNodeId(), subPlan.getRoot(), subPlan.rewrite(predicate)));
  }

  private PlanBuilder aggregate(PlanBuilder subPlan, QuerySpecification node) {
    if (!analysis.isAggregation(node)) {
      return subPlan;
    }

    ImmutableList.Builder<Expression> inputBuilder = ImmutableList.builder();
    analysis.getAggregates(node).stream()
        .map(FunctionCall::getArguments)
        .flatMap(List::stream)
        // .filter(expression -> !(expression instanceof LambdaExpression)) // lambda expression is
        // generated at execution time
        .forEach(inputBuilder::add);

    /*analysis.getAggregates(node).stream()
            .map(FunctionCall::getOrderBy)
            .map(NodeUtils::getSortItemsFromOrderBy)
            .flatMap(List::stream)
            .map(SortItem::getSortKey)
            .forEach(inputBuilder::add);

    // filter expressions need to be projected first
    analysis.getAggregates(node).stream()
            .map(FunctionCall::getFilter)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(inputBuilder::add);*/

    GroupingSetAnalysis groupingSetAnalysis = analysis.getGroupingSets(node);
    inputBuilder.addAll(groupingSetAnalysis.getComplexExpressions());

    List<Expression> inputs = inputBuilder.build();
    subPlan = subqueryPlanner.handleSubqueries(subPlan, inputs, analysis.getSubqueries(node));
    subPlan = subPlan.appendProjections(inputs, symbolAllocator, queryContext);

    Function<Expression, Expression> rewrite = subPlan.getTranslations()::rewrite;

    GroupingSetsPlan groupingSets = planGroupingSets(subPlan, node, groupingSetAnalysis);

    return planAggregation(
        groupingSets.getSubPlan(),
        groupingSets.getGroupingSets(),
        groupingSets.getGroupIdSymbol(),
        analysis.getAggregates(node),
        rewrite);
  }

  private GroupingSetsPlan planGroupingSets(
      PlanBuilder subPlan, QuerySpecification node, GroupingSetAnalysis groupingSetAnalysis) {
    Map<Symbol, Symbol> groupingSetMappings = new LinkedHashMap<>();

    // Compute a set of artificial columns that will contain the values of the original columns
    // filtered by whether the column is included in the grouping set
    // This will become the basis for the scope for any column references
    Symbol[] fields = new Symbol[subPlan.getTranslations().getFieldSymbolsList().size()];
    for (FieldId field : groupingSetAnalysis.getAllFields()) {
      Symbol input = subPlan.getTranslations().getFieldSymbolsList().get(field.getFieldIndex());
      Symbol output = symbolAllocator.newSymbol(input, GROUP_KEY_SUFFIX);
      fields[field.getFieldIndex()] = output;
      groupingSetMappings.put(output, input);
    }

    Map<ScopeAware<Expression>, Symbol> complexExpressions = new LinkedHashMap<>();
    for (Expression expression : groupingSetAnalysis.getComplexExpressions()) {
      if (!complexExpressions.containsKey(
          scopeAwareKey(expression, analysis, subPlan.getScope()))) {
        Symbol input = subPlan.translate(expression);
        Symbol output =
            symbolAllocator.newSymbol(expression, analysis.getType(expression), GROUP_KEY_SUFFIX);
        complexExpressions.put(scopeAwareKey(expression, analysis, subPlan.getScope()), output);
        groupingSetMappings.put(output, input);
      }
    }

    // For the purpose of "distinct", we need to canonicalize column references that may have
    // varying
    // syntactic forms (e.g., "t.a" vs "a"). Thus we need to enumerate grouping sets based on the
    // underlying
    // fieldId associated with each column reference expression.

    // The catch is that simple group-by expressions can be arbitrary expressions (this is a
    // departure from the SQL specification).
    // But, they don't affect the number of grouping sets or the behavior of "distinct" . We can
    // compute all the candidate
    // grouping sets in terms of fieldId, dedup as appropriate and then cross-join them with the
    // complex expressions.

    // This tracks the grouping sets before complex expressions are considered.
    // It's also used to compute the descriptors needed to implement grouping()
    List<Set<FieldId>> columnOnlyGroupingSets = enumerateGroupingSets(groupingSetAnalysis);
    if (node.getGroupBy().isPresent() && node.getGroupBy().get().isDistinct()) {
      columnOnlyGroupingSets =
          columnOnlyGroupingSets.stream().distinct().collect(toImmutableList());
    }

    // translate from FieldIds to Symbols
    List<List<Symbol>> sets =
        columnOnlyGroupingSets.stream()
            .map(
                set ->
                    set.stream()
                        .map(FieldId::getFieldIndex)
                        .map(index -> fields[index])
                        .collect(toImmutableList()))
            .collect(toImmutableList());

    // combine (cartesian product) with complex expressions
    List<List<Symbol>> groupingSets =
        sets.stream()
            .map(
                set ->
                    ImmutableList.<Symbol>builder()
                        .addAll(set)
                        .addAll(complexExpressions.values())
                        .build())
            .collect(toImmutableList());

    // Generate GroupIdNode (multiple grouping sets) or ProjectNode (single grouping set)
    PlanNode groupId;
    Optional<Symbol> groupIdSymbol = Optional.empty();
    checkArgument(groupingSets.size() == 1, "Only support one groupingSet now");

    Assignments.Builder assignments = Assignments.builder();
    assignments.putIdentities(subPlan.getRoot().getOutputSymbols());
    groupingSetMappings.forEach((key, value) -> assignments.put(key, value.toSymbolReference()));

    groupId =
        new ProjectNode(queryIdAllocator.genPlanNodeId(), subPlan.getRoot(), assignments.build());

    subPlan =
        new PlanBuilder(
            subPlan.getTranslations().withNewMappings(complexExpressions, Arrays.asList(fields)),
            groupId);

    return new GroupingSetsPlan(subPlan, columnOnlyGroupingSets, groupingSets, groupIdSymbol);
  }

  private PlanBuilder planAggregation(
      PlanBuilder subPlan,
      List<List<Symbol>> groupingSets,
      Optional<Symbol> groupIdSymbol,
      List<FunctionCall> aggregates,
      Function<Expression, Expression> rewrite) {
    ImmutableList.Builder<AggregationAssignment> aggregateMappingBuilder = ImmutableList.builder();

    // deduplicate based on scope-aware equality
    for (FunctionCall function : scopeAwareDistinct(subPlan, aggregates)) {
      Symbol symbol = symbolAllocator.newSymbol(function, analysis.getType(function));

      // TODO: for ORDER BY arguments, rewrite them such that they match the actual arguments to the
      // function. This is necessary to maintain the semantics of DISTINCT + ORDER BY,
      //   which requires that ORDER BY be a subset of arguments
      //   What can happen currently is that if the argument requires a coercion, the argument will
      // take a different input that the ORDER BY clause, which is undefined behavior
      Aggregation aggregation =
          new Aggregation(
              analysis.getResolvedFunction(function),
              function.getArguments().stream().map(rewrite).collect(Collectors.toList()),
              function.isDistinct(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      aggregateMappingBuilder.add(new AggregationAssignment(symbol, function, aggregation));
    }
    List<AggregationAssignment> aggregateMappings = aggregateMappingBuilder.build();

    ImmutableSet.Builder<Integer> globalGroupingSets = ImmutableSet.builder();
    for (int i = 0; i < groupingSets.size(); i++) {
      if (groupingSets.get(i).isEmpty()) {
        globalGroupingSets.add(i);
      }
    }

    ImmutableList.Builder<Symbol> groupingKeys = ImmutableList.builder();
    groupingSets.stream().flatMap(List::stream).distinct().forEach(groupingKeys::add);
    groupIdSymbol.ifPresent(groupingKeys::add);

    AggregationNode aggregationNode =
        new AggregationNode(
            queryIdAllocator.genPlanNodeId(),
            subPlan.getRoot(),
            aggregateMappings.stream()
                .collect(
                    toImmutableMap(
                        AggregationAssignment::getSymbol, AggregationAssignment::getRewritten)),
            groupingSets(groupingKeys.build(), groupingSets.size(), globalGroupingSets.build()),
            ImmutableList.of(),
            AggregationNode.Step.SINGLE,
            Optional.empty(),
            groupIdSymbol);

    return new PlanBuilder(
        subPlan
            .getTranslations()
            .withAdditionalMappings(
                aggregateMappings.stream()
                    .collect(
                        toImmutableMap(
                            assignment ->
                                scopeAwareKey(
                                    assignment.getAstExpression(), analysis, subPlan.getScope()),
                            AggregationAssignment::getSymbol))),
        aggregationNode);
  }

  private <T extends Expression> List<T> scopeAwareDistinct(
      PlanBuilder subPlan, List<T> expressions) {
    return expressions.stream()
        .map(function -> scopeAwareKey(function, analysis, subPlan.getScope()))
        .distinct()
        .map(ScopeAware::getNode)
        .collect(toImmutableList());
  }

  private static List<Set<FieldId>> enumerateGroupingSets(GroupingSetAnalysis groupingSetAnalysis) {
    List<List<Set<FieldId>>> partialSets = new ArrayList<>();

    for (List<Set<FieldId>> cube : groupingSetAnalysis.getCubes()) {
      List<Set<FieldId>> sets =
          Sets.powerSet(ImmutableSet.copyOf(cube)).stream()
              .map(set -> set.stream().flatMap(Collection::stream).collect(toImmutableSet()))
              .collect(toImmutableList());

      partialSets.add(sets);
    }

    for (List<Set<FieldId>> rollup : groupingSetAnalysis.getRollups()) {
      List<Set<FieldId>> sets =
          IntStream.rangeClosed(0, rollup.size())
              .mapToObj(
                  prefixLength ->
                      rollup.subList(0, prefixLength).stream()
                          .flatMap(Collection::stream)
                          .collect(toImmutableSet()))
              .collect(toImmutableList());

      partialSets.add(sets);
    }

    partialSets.addAll(groupingSetAnalysis.getOrdinarySets());

    if (partialSets.isEmpty()) {
      return ImmutableList.of(ImmutableSet.of());
    }

    // compute the cross product of the partial sets
    List<Set<FieldId>> allSets = new ArrayList<>();
    partialSets.get(0).stream().map(ImmutableSet::copyOf).forEach(allSets::add);

    for (int i = 1; i < partialSets.size(); i++) {
      List<Set<FieldId>> groupingSets = partialSets.get(i);
      List<Set<FieldId>> oldGroupingSetsCrossProduct = ImmutableList.copyOf(allSets);
      allSets.clear();
      for (Set<FieldId> existingSet : oldGroupingSetsCrossProduct) {
        for (Set<FieldId> groupingSet : groupingSets) {
          Set<FieldId> concatenatedSet =
              ImmutableSet.<FieldId>builder().addAll(existingSet).addAll(groupingSet).build();
          allSets.add(concatenatedSet);
        }
      }
    }

    return allSets;
  }

  public static List<Expression> extractPatternRecognitionExpressions(
      List<VariableDefinition> variableDefinitions, List<MeasureDefinition> measureDefinitions) {
    ImmutableList.Builder<Expression> expressions = ImmutableList.builder();

    variableDefinitions.stream().map(VariableDefinition::getExpression).forEach(expressions::add);

    measureDefinitions.stream().map(MeasureDefinition::getExpression).forEach(expressions::add);

    return expressions.build();
  }

  public static Expression coerceIfNecessary(
      Analysis analysis, Expression original, Expression rewritten) {
    Type coercion = analysis.getCoercion(original);
    if (coercion == null) {
      return rewritten;
    } else {
      throw new RuntimeException("Coercion result in analysis only can be empty");
    }
  }

  /**
   * Creates a projection with any additional coercions by identity of the provided expressions.
   *
   * @return the new subplan and a mapping of each expression to the symbol representing the
   *     coercion or an existing symbol if a coercion wasn't needed
   */
  public static PlanAndMappings coerce(
      PlanBuilder subPlan,
      List<Expression> expressions,
      Analysis analysis,
      QueryId idAllocator,
      SymbolAllocator symbolAllocator) {
    Assignments.Builder assignments = Assignments.builder();
    assignments.putIdentities(subPlan.getRoot().getOutputSymbols());

    Map<NodeRef<Expression>, Symbol> mappings = new HashMap<>();
    for (Expression expression : expressions) {
      Type coercion = analysis.getCoercion(expression);

      // expressions may be repeated, for example, when resolving ordinal references in a GROUP BY
      // clause
      if (!mappings.containsKey(NodeRef.of(expression))) {
        if (coercion != null) {
          Symbol symbol = symbolAllocator.newSymbol(expression, coercion);

          assignments.put(
              symbol,
              new Cast(
                  subPlan.rewrite(expression),
                  // TODO(beyyes) transfer toSqlType(coercion) method,
                  null,
                  false));

          mappings.put(NodeRef.of(expression), symbol);
        } else {
          mappings.put(NodeRef.of(expression), subPlan.translate(expression));
        }
      }
    }

    subPlan =
        subPlan.withNewRoot(
            new ProjectNode(idAllocator.genPlanNodeId(), subPlan.getRoot(), assignments.build()));

    return new PlanAndMappings(subPlan, mappings);
  }

  public static OrderingScheme translateOrderingScheme(
      List<SortItem> items, Function<Expression, Symbol> coercions) {
    List<Symbol> coerced =
        items.stream().map(SortItem::getSortKey).map(coercions).collect(toImmutableList());

    ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
    Map<Symbol, SortOrder> orders = new HashMap<>();
    for (int i = 0; i < coerced.size(); i++) {
      Symbol symbol = coerced.get(i);
      // for multiple sort items based on the same expression, retain the first one:
      // ORDER BY x DESC, x ASC, y --> ORDER BY x DESC, y
      if (!orders.containsKey(symbol)) {
        symbols.add(symbol);
        orders.put(symbol, OrderingTranslator.sortItemToSortOrder(items.get(i)));
      }
    }

    return new OrderingScheme(symbols.build(), orders);
  }

  private PlanBuilder gapFill(
      PlanBuilder subPlan,
      @Nonnull Symbol timeColumn,
      @Nonnull FunctionCall gapFillColumn,
      @Nonnull List<Expression> gapFillGroupingKeys,
      @Nonnull Expression wherePredicate) {
    Symbol gapFillColumnSymbol = subPlan.translate(gapFillColumn);
    List<Symbol> groupingKeys = new ArrayList<>(gapFillGroupingKeys.size());
    subPlan = fillGroup(subPlan, gapFillGroupingKeys, groupingKeys, gapFillColumnSymbol);

    int monthDuration = (int) ((LongLiteral) gapFillColumn.getChildren().get(0)).getParsedValue();
    long nonMonthDuration = ((LongLiteral) gapFillColumn.getChildren().get(1)).getParsedValue();
    long origin = ((LongLiteral) gapFillColumn.getChildren().get(3)).getParsedValue();
    long[] startAndEndTime =
        getStartTimeAndEndTimeOfGapFill(
            timeColumn, wherePredicate, origin, monthDuration, nonMonthDuration);
    return subPlan.withNewRoot(
        new GapFillNode(
            queryIdAllocator.genPlanNodeId(),
            subPlan.getRoot(),
            startAndEndTime[0],
            startAndEndTime[1],
            monthDuration,
            nonMonthDuration,
            gapFillColumnSymbol,
            groupingKeys));
  }

  // both gapFillColumnSymbol and wherePredicate have already been translated.
  private long[] getStartTimeAndEndTimeOfGapFill(
      Symbol timeColumn,
      Expression wherePredicate,
      long origin,
      int monthDuration,
      long nonMonthDuration) {
    GapFillStartAndEndTimeExtractVisitor.Context context =
        new GapFillStartAndEndTimeExtractVisitor.Context();
    GapFillStartAndEndTimeExtractVisitor visitor =
        new GapFillStartAndEndTimeExtractVisitor(timeColumn);
    if (!Boolean.TRUE.equals(wherePredicate.accept(visitor, context))) {
      throw new SemanticException(CAN_NOT_INFER_TIME_RANGE);
    } else {
      return context.getTimeRange(
          origin, monthDuration, nonMonthDuration, queryContext.getZoneId());
    }
  }

  private PlanBuilder fill(PlanBuilder subPlan, Optional<Fill> fill) {
    if (!fill.isPresent()) {
      return subPlan;
    }

    List<Symbol> groupingKeys = null;
    switch (fill.get().getFillMethod()) {
      case PREVIOUS:
        Analysis.PreviousFillAnalysis previousFillAnalysis =
            (Analysis.PreviousFillAnalysis) analysis.getFill(fill.get());
        Symbol previousFillHelperColumn = null;
        if (previousFillAnalysis.getFieldReference().isPresent()) {
          previousFillHelperColumn =
              subPlan.translate(previousFillAnalysis.getFieldReference().get());
        }

        if (previousFillAnalysis.getGroupingKeys().isPresent()) {
          List<FieldReference> fieldReferenceList = previousFillAnalysis.getGroupingKeys().get();
          groupingKeys = new ArrayList<>(fieldReferenceList.size());
          subPlan = fillGroup(subPlan, fieldReferenceList, groupingKeys, previousFillHelperColumn);
        }

        return subPlan.withNewRoot(
            new PreviousFillNode(
                queryIdAllocator.genPlanNodeId(),
                subPlan.getRoot(),
                previousFillAnalysis.getTimeBound().orElse(null),
                previousFillHelperColumn,
                groupingKeys));
      case LINEAR:
        Analysis.LinearFillAnalysis linearFillAnalysis =
            (Analysis.LinearFillAnalysis) analysis.getFill(fill.get());
        Symbol helperColumn = subPlan.translate(linearFillAnalysis.getFieldReference());
        if (linearFillAnalysis.getGroupingKeys().isPresent()) {
          List<FieldReference> fieldReferenceList = linearFillAnalysis.getGroupingKeys().get();
          groupingKeys = new ArrayList<>(fieldReferenceList.size());
          subPlan = fillGroup(subPlan, fieldReferenceList, groupingKeys, helperColumn);
        }
        return subPlan.withNewRoot(
            new LinearFillNode(
                queryIdAllocator.genPlanNodeId(), subPlan.getRoot(), helperColumn, groupingKeys));
      case CONSTANT:
        Analysis.ValueFillAnalysis valueFillAnalysis =
            (Analysis.ValueFillAnalysis) analysis.getFill(fill.get());
        return subPlan.withNewRoot(
            new ValueFillNode(
                queryIdAllocator.genPlanNodeId(),
                subPlan.getRoot(),
                valueFillAnalysis.getFilledValue()));
      default:
        throw new IllegalArgumentException("Unknown fill method: " + fill.get().getFillMethod());
    }
  }

  // used for gapfill and GROUP_FILL in fill clause
  private PlanBuilder fillGroup(
      PlanBuilder subPlan,
      List<? extends Expression> groupingExpressions,
      List<Symbol> groupingKeys,
      @Nullable Symbol timeColumn) {
    ImmutableList.Builder<Symbol> orderBySymbols = ImmutableList.builder();
    Map<Symbol, SortOrder> orderings = new HashMap<>();
    for (Expression expression : groupingExpressions) {
      Symbol symbol = subPlan.translate(expression);
      orderings.computeIfAbsent(
          symbol,
          k -> {
            groupingKeys.add(k);
            orderBySymbols.add(k);
            // sort order for fill_group should always be ASC_NULLS_LAST, it should be same as
            // TableOperatorGenerator
            return ASC_NULLS_LAST;
          });
    }
    if (timeColumn != null) {
      orderings.computeIfAbsent(
          timeColumn,
          k -> {
            orderBySymbols.add(k);
            // sort order for fill_group should always be ASC_NULLS_LAST
            return ASC_NULLS_LAST;
          });
    }
    OrderingScheme orderingScheme = new OrderingScheme(orderBySymbols.build(), orderings);
    analysis.setSortNode(true);
    return subPlan.withNewRoot(
        new SortNode(
            queryIdAllocator.genPlanNodeId(), subPlan.getRoot(), orderingScheme, false, false));
  }

  private PlanBuilder distinct(
      PlanBuilder subPlan, QuerySpecification node, List<Expression> expressions) {
    if (node.getSelect().isDistinct()) {
      List<Symbol> symbols =
          expressions.stream().map(subPlan::translate).collect(Collectors.toList());

      return subPlan.withNewRoot(
          singleAggregation(
              queryIdAllocator.genPlanNodeId(),
              subPlan.getRoot(),
              ImmutableMap.of(),
              singleGroupingSet(symbols)));
    }

    return subPlan;
  }

  private Optional<OrderingScheme> orderingScheme(
      PlanBuilder subPlan, Optional<OrderBy> orderBy, List<Expression> orderByExpressions) {
    if (!orderBy.isPresent() || (analysis.isOrderByRedundant(orderBy.get()))) {
      return Optional.empty();
    }

    Iterator<SortItem> sortItems = orderBy.get().getSortItems().iterator();

    ImmutableList.Builder<Symbol> orderBySymbols = ImmutableList.builder();
    Map<Symbol, SortOrder> orderings = new HashMap<>();
    for (Expression fieldOrExpression : orderByExpressions) {
      Symbol symbol = subPlan.translate(fieldOrExpression);

      SortItem sortItem = sortItems.next();
      if (!orderings.containsKey(symbol)) {
        orderBySymbols.add(symbol);
        orderings.put(symbol, sortItemToSortOrder(sortItem));
      }
    }
    return Optional.of(new OrderingScheme(orderBySymbols.build(), orderings));
  }

  private PlanBuilder sort(PlanBuilder subPlan, Optional<OrderingScheme> orderingScheme) {
    if (!orderingScheme.isPresent()) {
      return subPlan;
    }

    return subPlan.withNewRoot(
        new SortNode(
            queryIdAllocator.genPlanNodeId(),
            subPlan.getRoot(),
            orderingScheme.get(),
            false,
            false));
  }

  private PlanBuilder offset(PlanBuilder subPlan, Optional<Offset> offset) {
    if (!offset.isPresent()) {
      return subPlan;
    }

    return subPlan.withNewRoot(
        new OffsetNode(
            queryIdAllocator.genPlanNodeId(), subPlan.getRoot(), analysis.getOffset(offset.get())));
  }

  private PlanBuilder limit(
      PlanBuilder subPlan, Optional<Node> limit, Optional<OrderingScheme> orderingScheme) {
    if (limit.isPresent() && analysis.getLimit(limit.get()).isPresent()) {
      Optional<OrderingScheme> tiesResolvingScheme = Optional.empty();

      return subPlan.withNewRoot(
          new LimitNode(
              queryIdAllocator.genPlanNodeId(),
              subPlan.getRoot(),
              analysis.getLimit(limit.get()).getAsLong(),
              tiesResolvingScheme));
    }
    return subPlan;
  }

  private static class GroupingSetsPlan {
    private final PlanBuilder subPlan;
    private final List<Set<FieldId>> columnOnlyGroupingSets;
    private final List<List<Symbol>> groupingSets;
    private final Optional<Symbol> groupIdSymbol;

    public GroupingSetsPlan(
        PlanBuilder subPlan,
        List<Set<FieldId>> columnOnlyGroupingSets,
        List<List<Symbol>> groupingSets,
        Optional<Symbol> groupIdSymbol) {
      this.columnOnlyGroupingSets = columnOnlyGroupingSets;
      this.groupingSets = groupingSets;
      this.groupIdSymbol = groupIdSymbol;
      this.subPlan = subPlan;
    }

    public PlanBuilder getSubPlan() {
      return subPlan;
    }

    public List<Set<FieldId>> getColumnOnlyGroupingSets() {
      return columnOnlyGroupingSets;
    }

    public List<List<Symbol>> getGroupingSets() {
      return groupingSets;
    }

    public Optional<Symbol> getGroupIdSymbol() {
      return groupIdSymbol;
    }
  }

  public static class PlanAndMappings {
    private final PlanBuilder subPlan;
    private final Map<NodeRef<Expression>, Symbol> mappings;

    public PlanAndMappings(PlanBuilder subPlan, Map<NodeRef<Expression>, Symbol> mappings) {
      this.subPlan = subPlan;
      this.mappings = ImmutableMap.copyOf(mappings);
    }

    public PlanBuilder getSubPlan() {
      return subPlan;
    }

    public Symbol get(Expression expression) {
      return tryGet(expression)
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      format(
                          "No mapping for expression: %s (%s)",
                          expression, System.identityHashCode(expression))));
    }

    public Optional<Symbol> tryGet(Expression expression) {
      Symbol result = mappings.get(NodeRef.of(expression));

      if (result != null) {
        return Optional.of(result);
      }

      return Optional.empty();
    }
  }

  private static class AggregationAssignment {
    private final Symbol symbol;
    private final Expression astExpression;
    private final AggregationNode.Aggregation aggregation;

    public AggregationAssignment(
        Symbol symbol, Expression astExpression, AggregationNode.Aggregation aggregation) {
      this.astExpression = astExpression;
      this.symbol = symbol;
      this.aggregation = aggregation;
    }

    public Symbol getSymbol() {
      return symbol;
    }

    public Expression getAstExpression() {
      return astExpression;
    }

    public Aggregation getRewritten() {
      return aggregation;
    }
  }

  private static class FrameBoundPlanAndSymbols {
    private final PlanBuilder subPlan;
    private final Optional<Symbol> frameBoundSymbol;
    private final Optional<Symbol> sortKeyCoercedForFrameBoundComparison;

    public FrameBoundPlanAndSymbols(
        PlanBuilder subPlan,
        Optional<Symbol> frameBoundSymbol,
        Optional<Symbol> sortKeyCoercedForFrameBoundComparison) {
      this.subPlan = subPlan;
      this.frameBoundSymbol = frameBoundSymbol;
      this.sortKeyCoercedForFrameBoundComparison = sortKeyCoercedForFrameBoundComparison;
    }

    public PlanBuilder getSubPlan() {
      return subPlan;
    }

    public Optional<Symbol> getFrameBoundSymbol() {
      return frameBoundSymbol;
    }

    public Optional<Symbol> getSortKeyCoercedForFrameBoundComparison() {
      return sortKeyCoercedForFrameBoundComparison;
    }
  }

  private static class FrameOffsetPlanAndSymbol {
    private final PlanBuilder subPlan;
    private final Optional<Symbol> frameOffsetSymbol;

    public FrameOffsetPlanAndSymbol(PlanBuilder subPlan, Optional<Symbol> frameOffsetSymbol) {
      this.subPlan = subPlan;
      this.frameOffsetSymbol = frameOffsetSymbol;
    }

    public PlanBuilder getSubPlan() {
      return subPlan;
    }

    public Optional<Symbol> getFrameOffsetSymbol() {
      return frameOffsetSymbol;
    }
  }
}
