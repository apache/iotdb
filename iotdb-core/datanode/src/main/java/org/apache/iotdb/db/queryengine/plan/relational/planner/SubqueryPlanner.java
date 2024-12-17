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

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner.PlanAndMappings;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ApplyNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExistsPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericDataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubqueryExpression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.SuccessorsFunction;
import com.google.common.graph.Traverser;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanBuilder.newPlanBuilder;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ScopeAware.scopeAwareKey;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression.Quantifier.ALL;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toSqlType;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;

class SubqueryPlanner {
  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final QueryId idAllocator;
  private final MPPQueryContext plannerContext;
  private final SessionInfo session;
  private final Map<NodeRef<Node>, RelationPlan> recursiveSubqueries;

  SubqueryPlanner(
      Analysis analysis,
      SymbolAllocator symbolAllocator,
      MPPQueryContext plannerContext,
      Optional<TranslationMap> outerContext,
      SessionInfo session,
      Map<NodeRef<Node>, RelationPlan> recursiveSubqueries) {
    requireNonNull(analysis, "analysis is null");
    requireNonNull(symbolAllocator, "symbolAllocator is null");
    requireNonNull(plannerContext, "plannerContext is null");
    requireNonNull(outerContext, "outerContext is null");
    requireNonNull(session, "session is null");
    requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
    this.idAllocator = plannerContext.getQueryId();
    this.plannerContext = plannerContext;
    this.session = session;
    this.recursiveSubqueries = recursiveSubqueries;
  }

  public PlanBuilder handleSubqueries(
      PlanBuilder builder,
      Collection<Expression> expressions,
      Analysis.SubqueryAnalysis subqueries) {
    for (Expression expression : expressions) {
      builder = handleSubqueries(builder, expression, subqueries);
    }
    return builder;
  }

  public PlanBuilder handleSubqueries(
      PlanBuilder builder, Expression expression, Analysis.SubqueryAnalysis subqueries) {
    for (Cluster<InPredicate> cluster :
        cluster(
            builder.getScope(),
            selectSubqueries(builder, expression, subqueries.getInPredicatesSubqueries()))) {
      builder = planInPredicate(builder, cluster, subqueries);
    }
    for (Cluster<SubqueryExpression> cluster :
        cluster(
            builder.getScope(),
            selectSubqueries(builder, expression, subqueries.getSubqueries()))) {
      builder = planScalarSubquery(builder, cluster);
    }
    for (Cluster<ExistsPredicate> cluster :
        cluster(
            builder.getScope(),
            selectSubqueries(builder, expression, subqueries.getExistsSubqueries()))) {
      builder = planExists(builder, cluster);
    }
    for (Cluster<QuantifiedComparisonExpression> cluster :
        cluster(
            builder.getScope(),
            selectSubqueries(
                builder, expression, subqueries.getQuantifiedComparisonSubqueries()))) {
      builder = planQuantifiedComparison(builder, cluster, subqueries);
    }

    return builder;
  }

  /**
   * Find subqueries from the candidate set that are children of the given parent and that have not
   * already been handled in the subplan
   */
  private <T extends Expression> List<T> selectSubqueries(
      PlanBuilder subPlan, Expression parent, List<T> candidates) {
    SuccessorsFunction<Node> recurse =
        expression -> {
          if (!(expression instanceof Expression)
              || (!analysis.isColumnReference((Expression) expression)
                  && // no point in following dereference chains
                  !subPlan.canTranslate(
                      (Expression)
                          expression))) { // don't consider subqueries under parts of the expression
            // that have already been handled
            return expression.getChildren();
          }

          return ImmutableList.of();
        };

    Iterable<Node> allSubExpressions = Traverser.forTree(recurse).depthFirstPreOrder(parent);

    return candidates.stream()
        .filter(candidate -> stream(allSubExpressions).anyMatch(child -> child == candidate))
        .filter(candidate -> !subPlan.canTranslate(candidate))
        .collect(toImmutableList());
  }

  /**
   * Group expressions into clusters such that all entries in a cluster are #equals to each other
   */
  private <T extends Expression> Collection<Cluster<T>> cluster(Scope scope, List<T> expressions) {
    Map<ScopeAware<T>, List<T>> sets = new LinkedHashMap<>();

    for (T expression : expressions) {
      sets.computeIfAbsent(scopeAwareKey(expression, analysis, scope), key -> new ArrayList<>())
          .add(expression);
    }

    return sets.values().stream()
        .map(cluster -> Cluster.newCluster(cluster, scope, analysis))
        .collect(toImmutableList());
  }

  private PlanBuilder planInPredicate(
      PlanBuilder subPlan, Cluster<InPredicate> cluster, Analysis.SubqueryAnalysis subqueries) {
    // Plan one of the predicates from the cluster
    InPredicate predicate = cluster.getRepresentative();

    Expression value = predicate.getValue();
    SubqueryExpression subquery = (SubqueryExpression) predicate.getValueList();
    Symbol output = symbolAllocator.newSymbol(predicate, BOOLEAN);

    subPlan = handleSubqueries(subPlan, value, subqueries);
    subPlan =
        planInPredicate(
            subPlan, value, subquery, output, predicate, analysis.getPredicateCoercions(predicate));

    return new PlanBuilder(
        subPlan
            .getTranslations()
            .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), output)),
        subPlan.getRoot());
  }

  /**
   * Plans a correlated subquery for value IN (subQuery)
   *
   * @param originalExpression the original expression from which the IN predicate was derived. Used
   *     for subsequent translations.
   */
  private PlanBuilder planInPredicate(
      PlanBuilder subPlan,
      Expression value,
      SubqueryExpression subquery,
      Symbol output,
      Expression originalExpression,
      Analysis.PredicateCoercions predicateCoercions) {
    PlanAndMappings subqueryPlan =
        planSubquery(subquery, predicateCoercions.getSubqueryCoercion(), subPlan.getTranslations());
    PlanAndMappings valuePlan =
        planValue(
            subPlan,
            value,
            predicateCoercions.getValueType(),
            predicateCoercions.getValueCoercion());

    return new PlanBuilder(
        valuePlan.getSubPlan().getTranslations(),
        new ApplyNode(
            idAllocator.genPlanNodeId(),
            valuePlan.getSubPlan().getRoot(),
            subqueryPlan.getSubPlan().getRoot(),
            ImmutableMap.of(
                output, new ApplyNode.In(valuePlan.get(value), subqueryPlan.get(subquery))),
            valuePlan.getSubPlan().getRoot().getOutputSymbols(),
            originalExpression));
  }

  private PlanBuilder planScalarSubquery(PlanBuilder subPlan, Cluster<SubqueryExpression> cluster) {
    // Plan one of the predicates from the cluster
    SubqueryExpression scalarSubquery = cluster.getRepresentative();

    RelationPlan relationPlan = planSubquery(scalarSubquery, subPlan.getTranslations());
    PlanBuilder subqueryPlan = newPlanBuilder(relationPlan, analysis);

    PlanNode root = new EnforceSingleRowNode(idAllocator.genPlanNodeId(), subqueryPlan.getRoot());

    Type type = analysis.getType(scalarSubquery);
    RelationType descriptor = relationPlan.getDescriptor();
    List<Symbol> fieldMappings = relationPlan.getFieldMappings();
    Symbol column;
    if (descriptor.getVisibleFieldCount() > 1) {
      column = symbolAllocator.newSymbol("row", type);

      ImmutableList.Builder<Expression> fields = ImmutableList.builder();
      for (int i = 0; i < descriptor.getAllFieldCount(); i++) {
        Field field = descriptor.getFieldByIndex(i);
        if (!field.isHidden()) {
          fields.add(fieldMappings.get(i).toSymbolReference());
        }
      }

      Expression expression = new Cast(new Row(fields.build()), toSqlType(type));

      root = new ProjectNode(idAllocator.genPlanNodeId(), root, Assignments.of(column, expression));
    } else {
      column = getOnlyElement(fieldMappings);
    }

    return appendCorrelatedJoin(
        subPlan,
        root,
        scalarSubquery.getQuery(),
        // Scalar subquery always contains EnforceSingleRowNode. Therefore, it's guaranteed
        // that subquery will return single row. Hence, correlated join can be of INNER type.
        JoinNode.JoinType.INNER,
        TRUE_LITERAL,
        mapAll(cluster, subPlan.getScope(), column));
  }

  public PlanBuilder appendCorrelatedJoin(
      PlanBuilder subPlan,
      PlanNode subquery,
      Query query,
      JoinNode.JoinType type,
      Expression filterCondition,
      Map<ScopeAware<Expression>, Symbol> mappings) {
    return new PlanBuilder(
        subPlan.getTranslations().withAdditionalMappings(mappings),
        new CorrelatedJoinNode(
            idAllocator.genPlanNodeId(),
            subPlan.getRoot(),
            subquery,
            subPlan.getRoot().getOutputSymbols(),
            type,
            filterCondition,
            query));
  }

  private PlanBuilder planExists(PlanBuilder subPlan, Cluster<ExistsPredicate> cluster) {
    // Plan one of the predicates from the cluster
    ExistsPredicate existsPredicate = cluster.getRepresentative();

    Expression subquery = existsPredicate.getSubquery();
    Symbol exists = symbolAllocator.newSymbol("exists", BOOLEAN);

    return new PlanBuilder(
        subPlan
            .getTranslations()
            .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), exists)),
        new ApplyNode(
            idAllocator.genPlanNodeId(),
            subPlan.getRoot(),
            planSubquery(subquery, subPlan.getTranslations()).getRoot(),
            ImmutableMap.of(exists, new ApplyNode.Exists()),
            subPlan.getRoot().getOutputSymbols(),
            subquery));
  }

  private RelationPlan planSubquery(Expression subquery, TranslationMap outerContext) {
    return new RelationPlanner(
            analysis,
            symbolAllocator,
            plannerContext,
            Optional.of(outerContext),
            session,
            recursiveSubqueries)
        .process(subquery, null);
  }

  private PlanBuilder planQuantifiedComparison(
      PlanBuilder subPlan,
      Cluster<QuantifiedComparisonExpression> cluster,
      Analysis.SubqueryAnalysis subqueries) {
    // Plan one of the predicates from the cluster
    QuantifiedComparisonExpression quantifiedComparison = cluster.getRepresentative();

    ComparisonExpression.Operator operator = quantifiedComparison.getOperator();
    QuantifiedComparisonExpression.Quantifier quantifier = quantifiedComparison.getQuantifier();
    Expression value = quantifiedComparison.getValue();
    SubqueryExpression subquery = (SubqueryExpression) quantifiedComparison.getSubquery();

    subPlan = handleSubqueries(subPlan, value, subqueries);

    Symbol output = symbolAllocator.newSymbol(quantifiedComparison, BOOLEAN);

    Analysis.PredicateCoercions predicateCoercions =
        analysis.getPredicateCoercions(quantifiedComparison);

    switch (operator) {
      case EQUAL:
        switch (quantifier) {
          case ALL:
            subPlan =
                planQuantifiedComparison(
                    subPlan, operator, quantifier, value, subquery, output, predicateCoercions);
            return new PlanBuilder(
                subPlan
                    .getTranslations()
                    .withAdditionalMappings(
                        ImmutableMap.of(
                            scopeAwareKey(quantifiedComparison, analysis, subPlan.getScope()),
                            output)),
                subPlan.getRoot());
          case ANY:
          case SOME:
            // A = ANY B <=> A IN B
            subPlan =
                planInPredicate(
                    subPlan, value, subquery, output, quantifiedComparison, predicateCoercions);
            return new PlanBuilder(
                subPlan
                    .getTranslations()
                    .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), output)),
                subPlan.getRoot());
          default:
            throw new IllegalArgumentException();
        }
      case NOT_EQUAL:
        switch (quantifier) {
          case ALL:
            // A <> ALL B <=> !(A IN B)
            return addNegation(
                planInPredicate(
                    subPlan, value, subquery, output, quantifiedComparison, predicateCoercions),
                cluster,
                output);
          case ANY:
          case SOME:
            // A <> ANY B <=> min B <> max B || A <> min B <=> !(min B = max B && A = min B) <=> !(A
            // = ALL B)
            // "A <> ANY B" is equivalent to "NOT (A = ALL B)" so add a rewrite for the initial
            // quantifiedComparison to notAll
            return addNegation(
                planQuantifiedComparison(
                    subPlan,
                    ComparisonExpression.Operator.EQUAL,
                    ALL,
                    value,
                    subquery,
                    output,
                    predicateCoercions),
                cluster,
                output);
          default:
            throw new IllegalArgumentException();
        }
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        subPlan =
            planQuantifiedComparison(
                subPlan, operator, quantifier, value, subquery, output, predicateCoercions);
        return new PlanBuilder(
            subPlan
                .getTranslations()
                .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), output)),
            subPlan.getRoot());
        // Cannot be used with quantified comparison
      case IS_DISTINCT_FROM:
      default:
        throw new IllegalArgumentException(
            format("Unexpected quantified comparison: '%s %s'", operator.getValue(), quantifier));
    }
  }

  /**
   * Adds a negation of the given input and remaps the provided expression to the negated expression
   */
  private PlanBuilder addNegation(
      PlanBuilder subPlan, Cluster<? extends Expression> cluster, Symbol input) {
    Symbol output = symbolAllocator.newSymbol("not", BOOLEAN);

    return new PlanBuilder(
        subPlan
            .getTranslations()
            .withAdditionalMappings(mapAll(cluster, subPlan.getScope(), output)),
        new ProjectNode(
            idAllocator.genPlanNodeId(),
            subPlan.getRoot(),
            Assignments.builder()
                .putIdentities(subPlan.getRoot().getOutputSymbols())
                .put(output, new NotExpression(input.toSymbolReference()))
                .build()));
  }

  private PlanBuilder planQuantifiedComparison(
      PlanBuilder subPlan,
      ComparisonExpression.Operator operator,
      QuantifiedComparisonExpression.Quantifier quantifier,
      Expression value,
      Expression subquery,
      Symbol assignment,
      Analysis.PredicateCoercions predicateCoercions) {
    PlanAndMappings subqueryPlan =
        planSubquery(subquery, predicateCoercions.getSubqueryCoercion(), subPlan.getTranslations());
    PlanAndMappings valuePlan =
        planValue(
            subPlan,
            value,
            predicateCoercions.getValueType(),
            predicateCoercions.getValueCoercion());

    return new PlanBuilder(
        valuePlan.getSubPlan().getTranslations(),
        new ApplyNode(
            idAllocator.genPlanNodeId(),
            valuePlan.getSubPlan().getRoot(),
            subqueryPlan.getSubPlan().getRoot(),
            ImmutableMap.of(
                assignment,
                new ApplyNode.QuantifiedComparison(
                    mapOperator(operator),
                    mapQuantifier(quantifier),
                    valuePlan.get(value),
                    subqueryPlan.get(subquery))),
            valuePlan.getSubPlan().getRoot().getOutputSymbols(),
            subquery));
  }

  private static ApplyNode.Quantifier mapQuantifier(
      QuantifiedComparisonExpression.Quantifier quantifier) {
    switch (quantifier) {
      case ALL:
        return ApplyNode.Quantifier.ALL;
      case ANY:
        return ApplyNode.Quantifier.ANY;
      case SOME:
        return ApplyNode.Quantifier.SOME;
      default:
        throw new IllegalArgumentException();
    }
  }

  private static ApplyNode.Operator mapOperator(ComparisonExpression.Operator operator) {
    switch (operator) {
      case EQUAL:
        return ApplyNode.Operator.EQUAL;
      case NOT_EQUAL:
        return ApplyNode.Operator.NOT_EQUAL;
      case LESS_THAN:
        return ApplyNode.Operator.LESS_THAN;
      case LESS_THAN_OR_EQUAL:
        return ApplyNode.Operator.LESS_THAN_OR_EQUAL;
      case GREATER_THAN:
        return ApplyNode.Operator.GREATER_THAN;
      case GREATER_THAN_OR_EQUAL:
        return ApplyNode.Operator.GREATER_THAN_OR_EQUAL;
      case IS_DISTINCT_FROM:
      default:
        throw new IllegalArgumentException();
    }
  }

  private PlanAndMappings planValue(
      PlanBuilder subPlan, Expression value, Type actualType, Optional<Type> coercion) {
    subPlan = subPlan.appendProjections(ImmutableList.of(value), symbolAllocator, plannerContext);

    // Adapt implicit row type (in the SQL spec, <row value special case>) by wrapping it with a row
    // constructor
    Symbol column = subPlan.translate(value);
    Type declaredType = analysis.getType(value);
    if (!actualType.equals(declaredType)) {
      Symbol wrapped = symbolAllocator.newSymbol("row", actualType);

      Assignments assignments =
          Assignments.builder()
              .putIdentities(subPlan.getRoot().getOutputSymbols())
              .put(wrapped, new Row(ImmutableList.of(column.toSymbolReference())))
              .build();

      subPlan =
          subPlan.withNewRoot(
              new ProjectNode(idAllocator.genPlanNodeId(), subPlan.getRoot(), assignments));

      column = wrapped;
    }

    return coerceIfNecessary(subPlan, column, value, coercion);
  }

  private PlanAndMappings planSubquery(
      Expression subquery, Optional<Type> coercion, TranslationMap outerContext) {
    Type type = analysis.getType(subquery);
    Symbol column = symbolAllocator.newSymbol("row", type);

    RelationPlan relationPlan = planSubquery(subquery, outerContext);

    PlanBuilder subqueryPlan =
        newPlanBuilder(
            relationPlan,
            analysis,
            ImmutableMap.of(scopeAwareKey(subquery, analysis, relationPlan.getScope()), column));

    RelationType descriptor = relationPlan.getDescriptor();
    ImmutableList.Builder<Expression> fields = ImmutableList.builder();
    for (int i = 0; i < descriptor.getAllFieldCount(); i++) {
      Field field = descriptor.getFieldByIndex(i);
      if (!field.isHidden()) {
        fields.add(relationPlan.getFieldMappings().get(i).toSymbolReference());
      }
    }

    subqueryPlan =
        subqueryPlan.withNewRoot(
            new ProjectNode(
                idAllocator.genPlanNodeId(),
                relationPlan.getRoot(),
                Assignments.of(
                    column,
                    new Cast(
                        new Row(fields.build()),
                        new GenericDataType(
                            new Identifier(type.toString()), ImmutableList.of())))));

    return coerceIfNecessary(subqueryPlan, column, subquery, coercion);
  }

  private PlanAndMappings coerceIfNecessary(
      PlanBuilder subPlan, Symbol symbol, Expression value, Optional<? extends Type> coercion) {
    Symbol coerced = symbol;

    if (coercion.isPresent()) {
      coerced = symbolAllocator.newSymbol(value, coercion.get());

      Assignments assignments =
          Assignments.builder()
              .putIdentities(subPlan.getRoot().getOutputSymbols())
              .put(
                  coerced,
                  new Cast(
                      symbol.toSymbolReference(),
                      new GenericDataType(
                          new Identifier(coercion.get().toString()), ImmutableList.of()),
                      false))
              .build();

      subPlan =
          subPlan.withNewRoot(
              new ProjectNode(idAllocator.genPlanNodeId(), subPlan.getRoot(), assignments));
    }

    return new PlanAndMappings(subPlan, ImmutableMap.of(NodeRef.of(value), coerced));
  }

  private <T extends Expression> Map<ScopeAware<Expression>, Symbol> mapAll(
      Cluster<T> cluster, Scope scope, Symbol output) {
    return cluster.getExpressions().stream()
        .collect(
            toImmutableMap(
                expression -> scopeAwareKey(expression, analysis, scope),
                expression -> output,
                (first, second) -> first));
  }

  /** A group of expressions that are equivalent to each other according to ScopeAware criteria */
  private static class Cluster<T extends Expression> {
    private final List<T> expressions;

    private Cluster(List<T> expressions) {
      checkArgument(!expressions.isEmpty(), "Cluster is empty");
      this.expressions = ImmutableList.copyOf(expressions);
    }

    public static <T extends Expression> Cluster<T> newCluster(
        List<T> expressions, Scope scope, Analysis analysis) {
      long count =
          expressions.stream()
              .map(expression -> scopeAwareKey(expression, analysis, scope))
              .distinct()
              .count();

      checkArgument(
          count == 1, "Cluster contains expressions that are not equivalent to each other");

      return new Cluster<>(expressions);
    }

    public List<T> getExpressions() {
      return expressions;
    }

    public T getRepresentative() {
      return expressions.get(0);
    }
  }
}
