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
import org.apache.iotdb.db.relational.sql.tree.Delete;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FieldReference;
import org.apache.iotdb.db.relational.sql.tree.Node;
import org.apache.iotdb.db.relational.sql.tree.Offset;
import org.apache.iotdb.db.relational.sql.tree.OrderBy;
import org.apache.iotdb.db.relational.sql.tree.Query;
import org.apache.iotdb.db.relational.sql.tree.QueryBody;
import org.apache.iotdb.db.relational.sql.tree.QuerySpecification;
import org.apache.iotdb.db.relational.sql.tree.SortItem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Pair;

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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PredicateUtils.extractGlobalTimePredicate;

public class QueryPlanner {
  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final MPPQueryContext queryContext;
  private final QueryId queryIdAllocator;
  private final SessionInfo session;
  private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

  // private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
  // private final SubqueryPlanner subqueryPlanner;

  public QueryPlanner(
      Analysis analysis,
      SymbolAllocator symbolAllocator,
      MPPQueryContext queryContext,
      SessionInfo session,
      Map<NodeRef<Node>, RelationPlan> recursiveSubqueries) {
    requireNonNull(analysis, "analysis is null");
    requireNonNull(symbolAllocator, "symbolAllocator is null");
    requireNonNull(queryContext, "idAllocator is null");
    requireNonNull(session, "session is null");
    requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
    this.queryContext = queryContext;
    this.queryIdAllocator = queryContext.getQueryId();
    this.session = session;
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
    if (orderBy.size() > 0) {
      builder =
          builder.appendProjections(
              Iterables.concat(orderBy, outputs), analysis, symbolAllocator, queryContext);
    }

    Optional<OrderingScheme> orderingScheme =
        orderingScheme(builder, query.getOrderBy(), analysis.getOrderByExpressions(query));
    builder = sort(builder, orderingScheme);
    builder = offset(builder, query.getOffset());
    builder = limit(builder, query.getLimit(), orderingScheme);
    builder = builder.appendProjections(outputs, analysis, symbolAllocator, queryContext);

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
      builder = builder.appendProjections(expressions, analysis, symbolAllocator, queryContext);
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
        builder =
            builder.appendProjections(orderByAggregates, analysis, symbolAllocator, queryContext);
      }

      // Add projections for the outputs of SELECT, but stack them on top of the ones from the FROM
      // clause so both are visible
      // when resolving the ORDER BY clause.
      builder = builder.appendProjections(outputs, analysis, symbolAllocator, queryContext);

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
    // TODO this appendProjections may be removed
    if (orderBy.size() > 0) {
      builder =
          builder.appendProjections(
              Iterables.concat(orderBy, outputs), analysis, symbolAllocator, queryContext);
    }

    Optional<OrderingScheme> orderingScheme =
        orderingScheme(builder, node.getOrderBy(), analysis.getOrderByExpressions(node));
    builder = sort(builder, orderingScheme);
    builder = offset(builder, node.getOffset());
    builder = limit(builder, node.getLimit(), orderingScheme);

    builder = builder.appendProjections(outputs, analysis, symbolAllocator, queryContext);

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
      Symbol symbol = null;
      if (expression instanceof FieldReference) {
        FieldReference reference = (FieldReference) expression;
        symbol = builder.getFieldSymbols()[reference.getFieldIndex()];
      }
      outputSymbols.add(symbol != null ? symbol : new Symbol(expression.toString()));
    }
    return outputSymbols.build();
  }

  private PlanBuilder planQueryBody(QueryBody queryBody) {
    RelationPlan relationPlan =
        new RelationPlanner(analysis, symbolAllocator, queryContext, session, recursiveSubqueries)
            .process(queryBody, null);

    return newPlanBuilder(relationPlan, analysis);
  }

  private PlanBuilder planFrom(QuerySpecification node) {
    if (node.getFrom().isPresent()) {
      RelationPlan relationPlan =
          new RelationPlanner(analysis, symbolAllocator, queryContext, session, recursiveSubqueries)
              .process(node.getFrom().get(), null);
      return newPlanBuilder(relationPlan, analysis);
    } else {
      throw new IllegalStateException("From clause must not by empty");
    }
  }

  private PlanBuilder filter(PlanBuilder planBuilder, Expression predicate) {
    if (predicate == null) {
      return planBuilder;
    }

    Pair<Expression, Boolean> resultPair = extractGlobalTimePredicate(predicate, true, true);
    Expression globalTimePredicate = resultPair.left;
    analysis.setGlobalTableModelTimePredicate(globalTimePredicate);
    boolean hasValueFilter = resultPair.right;
    if (!hasValueFilter) {
      return planBuilder;
    }
    // TODO if predicate equals TrueConstant, no need filter

    return planBuilder.withNewRoot(
        new FilterNode(
            queryIdAllocator.genPlanNodeId(),
            planBuilder.getRoot(),
            planBuilder.rewrite(predicate)));

    // subPlan = subqueryPlanner.handleSubqueries(subPlan, predicate, analysis.getSubqueries(node));
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
      Symbol symbol = new Symbol(fieldOrExpression.toString());
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
        new SortNode(
            queryIdAllocator.genPlanNodeId(), subPlan.getRoot(), orderingScheme.get(), false));
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
}
