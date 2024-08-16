/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Offset;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QueryBody;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SortItem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingTranslator.sortItemToSortOrder;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanBuilder.newPlanBuilder;

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
        builder.getRoot(), analysis.getScope(query), computeOutputs(builder, outputs));
  }

  public RelationPlan plan(QuerySpecification node) {
    PlanBuilder builder = planFrom(node);

    builder = filter(builder, analysis.getWhere(node));

    List<Analysis.SelectExpression> selectExpressions = analysis.getSelectExpressions(node);

    if (hasExpressionsToUnfold(selectExpressions)) {
      List<Expression> expressions =
          selectExpressions.stream()
              .map(Analysis.SelectExpression::getExpression)
              .collect(toImmutableList());

      // pre-project the folded expressions to preserve any non-deterministic semantics of functions
      // that might be referenced
      builder = builder.appendProjections(expressions, symbolAllocator, queryContext);
    }

    List<Expression> outputs = outputExpressions(selectExpressions);
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
        builder.getRoot(), analysis.getScope(node), computeOutputs(builder, outputs));

    // TODO handle aggregate, having, distinct, subQuery later
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

  private PlanBuilder filter(PlanBuilder subPlan, Expression predicate) {
    if (predicate == null) {
      return subPlan;
    }

    // planBuilder = subqueryPlanner.handleSubqueries(subPlan, predicate,
    // analysis.getSubqueries(node));
    return subPlan.withNewRoot(
        new FilterNode(
            queryIdAllocator.genPlanNodeId(), subPlan.getRoot(), subPlan.rewrite(predicate)));
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
}
