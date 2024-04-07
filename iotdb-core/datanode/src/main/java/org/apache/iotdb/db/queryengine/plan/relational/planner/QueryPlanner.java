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

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.relational.sql.tree.Delete;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.Node;
import org.apache.iotdb.db.relational.sql.tree.Offset;
import org.apache.iotdb.db.relational.sql.tree.OrderBy;
import org.apache.iotdb.db.relational.sql.tree.Query;
import org.apache.iotdb.db.relational.sql.tree.QuerySpecification;
import org.apache.iotdb.db.relational.sql.tree.SortItem;
import org.apache.iotdb.tsfile.read.common.type.Type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingTranslator.sortItemToSortOrder;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanBuilder.newPlanBuilder;

class QueryPlanner {
  private static final int MAX_BIGINT_PRECISION = 19;
  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final QueryId idAllocator;
  // private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
  // private final PlannerContext plannerContext;
  private final SessionInfo session;
  // private final SubqueryPlanner subqueryPlanner;
  private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

  QueryPlanner(
      Analysis analysis,
      SymbolAllocator symbolAllocator,
      QueryId idAllocator,
      SessionInfo session,
      Map<NodeRef<Node>, RelationPlan> recursiveSubqueries) {
    requireNonNull(analysis, "analysis is null");
    requireNonNull(symbolAllocator, "symbolAllocator is null");
    requireNonNull(idAllocator, "idAllocator is null");
    requireNonNull(session, "session is null");
    requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
    this.idAllocator = idAllocator;
    this.session = session;
    // this.subqueryPlanner = null;
    this.recursiveSubqueries = recursiveSubqueries;
  }

  public RelationPlan plan(Query query) {
    PlanBuilder builder = planQueryBody(query);

    List<Expression> orderBy = analysis.getOrderByExpressions(query);
    // builder = subqueryPlanner.handleSubqueries(builder, orderBy, analysis.getSubqueries(query));

    List<Analysis.SelectExpression> selectExpressions = analysis.getSelectExpressions(query);
    List<Expression> outputs =
        selectExpressions.stream()
            .map(Analysis.SelectExpression::getExpression)
            .collect(toImmutableList());

    builder =
        builder.appendProjections(Iterables.concat(orderBy, outputs), symbolAllocator, idAllocator);

    Optional<OrderingScheme> orderingScheme =
        orderingScheme(builder, query.getOrderBy(), analysis.getOrderByExpressions(query));
    builder = sort(builder, orderingScheme);
    builder = offset(builder, query.getOffset());
    builder = limit(builder, query.getLimit(), orderingScheme);
    builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);

    return new RelationPlan(
        builder.getRoot(), analysis.getScope(query), computeOutputs(builder, outputs));
  }

  public RelationPlan plan(QuerySpecification node) {
    PlanBuilder builder = planFrom(node);

    builder = filter(builder, analysis.getWhere(node), node);
    // builder = aggregate(builder, node);
    builder = filter(builder, analysis.getHaving(node), node);
    // builder = planWindowFunctions(node, builder,
    // ImmutableList.copyOf(analysis.getWindowFunctions(node)));
    // builder = planWindowMeasures(node, builder,
    // ImmutableList.copyOf(analysis.getWindowMeasures(node)));

    List<Analysis.SelectExpression> selectExpressions = analysis.getSelectExpressions(node);
    List<Expression> expressions =
        selectExpressions.stream()
            .map(Analysis.SelectExpression::getExpression)
            .collect(toImmutableList());
    // builder = subqueryPlanner.handleSubqueries(builder, expressions,
    // analysis.getSubqueries(node));

    if (hasExpressionsToUnfold(selectExpressions)) {
      // pre-project the folded expressions to preserve any non-deterministic semantics of functions
      // that might be referenced
      builder = builder.appendProjections(expressions, symbolAllocator, idAllocator);
    }

    List<Expression> outputs = outputExpressions(selectExpressions);
    if (node.getOrderBy().isPresent()) {
      // ORDER BY requires outputs of SELECT to be visible.
      // For queries with aggregation, it also requires grouping keys and translated aggregations.
      if (analysis.isAggregation(node)) {
        // Add projections for aggregations required by ORDER BY. After this step, grouping keys and
        // translated
        // aggregations are visible.
        List<Expression> orderByAggregates = analysis.getOrderByAggregates(node.getOrderBy().get());
        builder = builder.appendProjections(orderByAggregates, symbolAllocator, idAllocator);
      }

      // Add projections for the outputs of SELECT, but stack them on top of the ones from the FROM
      // clause so both are visible
      // when resolving the ORDER BY clause.
      builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);

      // The new scope is the composite of the fields from the FROM and SELECT clause (local nested
      // scopes). Fields from the bottom of
      // the scope stack need to be placed first to match the expected layout for nested scopes.
      List<Symbol> newFields = new ArrayList<>();
      // newFields.addAll(builder.getTranslations().getFieldSymbols());

      //            outputs.stream()
      //                    .map(builder::translate)
      //                    .forEach(newFields::add);

      builder = builder.withScope(analysis.getScope(node.getOrderBy().get()), newFields);
    }

    List<Expression> orderBy = analysis.getOrderByExpressions(node);
    builder =
        builder.appendProjections(Iterables.concat(orderBy, outputs), symbolAllocator, idAllocator);

    // builder = distinct(builder, node, outputs);
    Optional<OrderingScheme> orderingScheme =
        orderingScheme(builder, node.getOrderBy(), analysis.getOrderByExpressions(node));
    builder = sort(builder, orderingScheme);
    builder = offset(builder, node.getOffset());
    builder = limit(builder, node.getLimit(), orderingScheme);
    builder = builder.appendProjections(outputs, symbolAllocator, idAllocator);

    return new RelationPlan(
        builder.getRoot(), analysis.getScope(node), computeOutputs(builder, outputs));
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
    //        for (Expression expression : outputExpressions) {
    //            // outputSymbols.add(builder.translate(expression));
    //        }
    return outputSymbols.build();
  }

  private PlanBuilder planQueryBody(Query query) {
    RelationPlan relationPlan =
        new RelationPlanner(analysis, symbolAllocator, idAllocator, session, recursiveSubqueries)
            .process(query.getQueryBody(), null);

    return newPlanBuilder(relationPlan, analysis, session);
  }

  private PlanBuilder planFrom(QuerySpecification node) {
    if (node.getFrom().isPresent()) {
      RelationPlan relationPlan =
          new RelationPlanner(analysis, symbolAllocator, idAllocator, session, recursiveSubqueries)
              .process(node.getFrom().get(), null);
      return newPlanBuilder(relationPlan, analysis, session);
    } else {
      throw new RuntimeException("From clause must not by empty");
    }
  }

  private PlanBuilder filter(PlanBuilder subPlan, Expression predicate, Node node) {
    if (predicate == null) {
      return subPlan;
    }

    // subPlan = subqueryPlanner.handleSubqueries(subPlan, predicate, analysis.getSubqueries(node));

    return subPlan.withNewRoot(
        new FilterNode(idAllocator.genPlanNodeId(), subPlan.getRoot(), predicate));
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
      Symbol symbol = null;
      // subPlan.translate(fieldOrExpression);

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
        new SortNode(idAllocator.genPlanNodeId(), subPlan.getRoot(), orderingScheme.get(), false));
  }

  private PlanBuilder offset(PlanBuilder subPlan, Optional<Offset> offset) {
    if (!offset.isPresent()) {
      return subPlan;
    }

    return subPlan.withNewRoot(
        new OffsetNode(
            idAllocator.genPlanNodeId(), subPlan.getRoot(), analysis.getOffset(offset.get())));
  }

  private PlanBuilder limit(
      PlanBuilder subPlan, Optional<Node> limit, Optional<OrderingScheme> orderingScheme) {
    if (limit.isPresent() && analysis.getLimit(limit.get()).isPresent()) {
      Optional<OrderingScheme> tiesResolvingScheme = Optional.empty();
      //            if (limit.get() instanceof FetchFirst && ((FetchFirst)
      // limit.get()).isWithTies()) {
      //                tiesResolvingScheme = orderingScheme;
      //            }
      return subPlan.withNewRoot(
          new LimitNode(
              idAllocator.genPlanNodeId(),
              subPlan.getRoot(),
              analysis.getLimit(limit.get()).getAsLong(),
              tiesResolvingScheme));
    }
    return subPlan;
  }
}
