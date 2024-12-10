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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Fill;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Offset;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QueryBody;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem;

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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingTranslator.sortItemToSortOrder;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanBuilder.newPlanBuilder;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ScopeAware.scopeAwareKey;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_LAST;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator.GROUP_KEY_SUFFIX;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.GapFillStartAndEndTimeExtractVisitor.CAN_NOT_INFER_TIME_RANGE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.groupingSets;

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
      analysis.setSortNode(true);
    }

    List<Expression> orderBy = analysis.getOrderByExpressions(node);
    if (!orderBy.isEmpty()) {
      builder =
          builder.appendProjections(
              Iterables.concat(orderBy, outputs), symbolAllocator, queryContext);
    }

    Optional<OrderingScheme> orderingScheme =
        orderingScheme(builder, node.getOrderBy(), analysis.getOrderByExpressions(node));
    builder = sort(builder, orderingScheme);
    builder = offset(builder, node.getOffset());
    builder = limit(builder, node.getLimit(), orderingScheme);

    builder = builder.appendProjections(outputs, symbolAllocator, queryContext);

    return new RelationPlan(
        builder.getRoot(), analysis.getScope(node), computeOutputs(builder, outputs), outerContext);
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
}
