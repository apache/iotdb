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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.GroupReference;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeCoercion;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.DeterminismEvaluator.isDeterministic;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.and;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.combineConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.extractConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleAggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.SymbolMapper.symbolMapper;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.EQUAL;

public class PlanNodeDecorrelator {
  private final PlannerContext plannerContext;
  private final SymbolAllocator symbolAllocator;
  private final Lookup lookup;
  private final TypeCoercion typeCoercion;

  public PlanNodeDecorrelator(
      PlannerContext plannerContext, SymbolAllocator symbolAllocator, Lookup lookup) {
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
    this.lookup = requireNonNull(lookup, "lookup is null");
    this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
  }

  public Optional<DecorrelatedNode> decorrelateFilters(PlanNode node, List<Symbol> correlation) {
    if (correlation.isEmpty()) {
      return Optional.of(new DecorrelatedNode(ImmutableList.of(), node));
    }

    Optional<DecorrelationResult> decorrelationResultOptional =
        node.accept(new DecorrelatingVisitor(plannerContext.getTypeManager(), correlation), null);
    return decorrelationResultOptional.flatMap(
        decorrelationResult ->
            decorrelatedNode(
                decorrelationResult.correlatedPredicates, decorrelationResult.node, correlation));
  }

  private class DecorrelatingVisitor extends PlanVisitor<Optional<DecorrelationResult>, Void> {
    private final TypeManager typeManager;
    private final List<Symbol> correlation;

    DecorrelatingVisitor(TypeManager typeManager, List<Symbol> correlation) {
      this.typeManager = requireNonNull(typeManager, "typeManager is null");
      this.correlation = requireNonNull(correlation, "correlation is null");
    }

    @Override
    public Optional<DecorrelationResult> visitPlan(PlanNode node, Void context) {
      if (containsCorrelation(node, correlation)) {
        return Optional.empty();
      }
      return Optional.of(
          new DecorrelationResult(
              node,
              ImmutableSet.of(),
              ImmutableList.of(),
              ImmutableMultimap.of(),
              ImmutableSet.of(),
              false));
    }

    @Override
    public Optional<DecorrelationResult> visitGroupReference(GroupReference node, Void context) {
      return lookup.resolve(node).accept(this, null);
    }

    @Override
    public Optional<DecorrelationResult> visitFilter(FilterNode node, Void context) {
      Optional<DecorrelationResult> childDecorrelationResultOptional =
          Optional.of(
              new DecorrelationResult(
                  node.getChild(),
                  ImmutableSet.of(),
                  ImmutableList.of(),
                  ImmutableMultimap.of(),
                  ImmutableSet.of(),
                  false));

      // try to decorrelate filters down the tree
      if (containsCorrelation(node.getChild(), correlation)) {
        childDecorrelationResultOptional = node.getChild().accept(this, null);
      }

      if (!childDecorrelationResultOptional.isPresent()) {
        return Optional.empty();
      }

      Expression predicate = node.getPredicate();
      Map<Boolean, List<Expression>> predicates =
          extractConjuncts(predicate).stream()
              .collect(Collectors.partitioningBy(DecorrelatingVisitor.this::isCorrelated));
      List<Expression> correlatedPredicates = ImmutableList.copyOf(predicates.get(true));
      List<Expression> uncorrelatedPredicates = ImmutableList.copyOf(predicates.get(false));

      DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
      FilterNode newFilterNode =
          new FilterNode(
              node.getPlanNodeId(),
              childDecorrelationResult.node,
              combineConjuncts(uncorrelatedPredicates));

      Set<Symbol> symbolsToPropagate =
          Sets.difference(
              SymbolsExtractor.extractUnique(correlatedPredicates),
              ImmutableSet.copyOf(correlation));
      return Optional.of(
          new DecorrelationResult(
              newFilterNode,
              Sets.union(childDecorrelationResult.symbolsToPropagate, symbolsToPropagate),
              ImmutableList.<Expression>builder()
                  .addAll(childDecorrelationResult.correlatedPredicates)
                  .addAll(correlatedPredicates)
                  .build(),
              ImmutableMultimap.<Symbol, Symbol>builder()
                  .putAll(childDecorrelationResult.correlatedSymbolsMapping)
                  .putAll(extractCorrelatedSymbolsMapping(correlatedPredicates))
                  .build(),
              ImmutableSet.<Symbol>builder()
                  .addAll(childDecorrelationResult.constantSymbols)
                  .addAll(extractConstantSymbols(correlatedPredicates))
                  .build(),
              childDecorrelationResult.atMostSingleRow));
    }

    @Override
    public Optional<DecorrelationResult> visitLimit(LimitNode node, Void context) {
      if (node.getCount() == 0 || node.isWithTies()) {
        return Optional.empty();
      }

      Optional<DecorrelationResult> childDecorrelationResultOptional =
          node.getChild().accept(this, null);
      if (!childDecorrelationResultOptional.isPresent()) {
        return Optional.empty();
      }

      DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
      if (childDecorrelationResult.atMostSingleRow) {
        return childDecorrelationResultOptional;
      }

      if (node.getCount() == 1) {
        return rewriteLimitWithRowCountOne(childDecorrelationResult, node.getPlanNodeId());
      }
      throw new SemanticException(
          "Decorrelation for LIMIT with row count greater than 1 is not supported yet");
      // return rewriteLimitWithRowCountGreaterThanOne(childDecorrelationResult, node);
    }

    // Limit (1) could be decorrelated by the method rewriteLimitWithRowCountGreaterThanOne()
    // as well.
    // The current decorrelation method for Limit (1) can not deal with subqueries outputting other
    // symbols
    // than constants.
    //
    // An example query that is currently not supported:
    // SELECT (
    //      SELECT a+b
    //      FROM (VALUES (1, 2), (1, 2)) inner_relation(a, b)
    //      WHERE a=x
    //      LIMIT 1)
    // FROM (VALUES (1)) outer_relation(x)
    //
    // Switching the decorrelation method would change the way that queries with EXISTS are
    // executed,
    // and thus it needs benchmarking.
    private Optional<DecorrelationResult> rewriteLimitWithRowCountOne(
        DecorrelationResult childDecorrelationResult, PlanNodeId nodeId) {
      PlanNode decorrelatedChildNode = childDecorrelationResult.node;
      Set<Symbol> constantSymbols = childDecorrelationResult.getConstantSymbols();

      if (constantSymbols.isEmpty()
          || !constantSymbols.containsAll(decorrelatedChildNode.getOutputSymbols())) {
        return Optional.empty();
      }

      // rewrite Limit to aggregation on constant symbols
      AggregationNode aggregationNode =
          singleAggregation(
              nodeId,
              decorrelatedChildNode,
              ImmutableMap.of(),
              singleGroupingSet(decorrelatedChildNode.getOutputSymbols()));

      return Optional.of(
          new DecorrelationResult(
              aggregationNode,
              childDecorrelationResult.symbolsToPropagate,
              childDecorrelationResult.correlatedPredicates,
              childDecorrelationResult.correlatedSymbolsMapping,
              childDecorrelationResult.constantSymbols,
              true));
    }

    /*
    private Optional<DecorrelationResult> rewriteLimitWithRowCountGreaterThanOne(
        DecorrelationResult childDecorrelationResult, LimitNode node) {
      PlanNode decorrelatedChildNode = childDecorrelationResult.node;

      // no rewrite needed (no symbols to partition by)
      if (childDecorrelationResult.symbolsToPropagate.isEmpty()) {
        return Optional.of(
            new DecorrelationResult(
                node.replaceChildren(ImmutableList.of(decorrelatedChildNode)),
                childDecorrelationResult.symbolsToPropagate,
                childDecorrelationResult.correlatedPredicates,
                childDecorrelationResult.correlatedSymbolsMapping,
                childDecorrelationResult.constantSymbols,
                false));
      }

      Set<Symbol> constantSymbols = childDecorrelationResult.getConstantSymbols();
      if (!constantSymbols.containsAll(childDecorrelationResult.symbolsToPropagate)) {
        return Optional.empty();
      }

      // rewrite Limit to RowNumberNode partitioned by constant symbols
      RowNumberNode rowNumberNode =
          new RowNumberNode(
              node.getPlanNodeId(),
              decorrelatedChildNode,
              ImmutableList.copyOf(childDecorrelationResult.symbolsToPropagate),
              false,
              symbolAllocator.newSymbol("row_number", BIGINT),
              Optional.of(toIntExact(node.getCount())),
              Optional.empty());

      return Optional.of(
          new DecorrelationResult(
              rowNumberNode,
              childDecorrelationResult.symbolsToPropagate,
              childDecorrelationResult.correlatedPredicates,
              childDecorrelationResult.correlatedSymbolsMapping,
              childDecorrelationResult.constantSymbols,
              false));
    }*/

    @Override
    public Optional<DecorrelationResult> visitTopK(TopKNode node, Void context) {
      throw new SemanticException("TopK is not supported in correlated subquery for now");
    }

    /*@Override
    public Optional<DecorrelationResult> visitTopK(TopKNode node, Void context)
    {
        if (node.getCount() == 0) {
            return Optional.empty();
        }

        checkState(node.getChildren().size() == 1, "Expected TopKNode to have a single child");
        Optional<DecorrelationResult> childDecorrelationResultOptional = node.getChildren().get(0).accept(this, null);
        if (!childDecorrelationResultOptional.isPresent()) {
            return Optional.empty();
        }

        DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
        if (childDecorrelationResult.atMostSingleRow) {
            return childDecorrelationResultOptional;
        }

        PlanNode decorrelatedChildNode = childDecorrelationResult.node;
        Set<Symbol> constantSymbols = childDecorrelationResult.getConstantSymbols();
        Optional<OrderingScheme> decorrelatedOrderingScheme = decorrelateOrderingScheme(node.getOrderingScheme(), constantSymbols);

        // no partitioning needed (no symbols to partition by)
        if (childDecorrelationResult.symbolsToPropagate.isEmpty()) {
            return decorrelatedOrderingScheme
                    .map(orderingScheme -> new DecorrelationResult(
                            // ordering symbols are present - return decorrelated TopNNode
                            new TopKNode(node.getPlanNodeId(), decorrelatedChildNode, node.getCount(), orderingScheme, node.getStep()),
                            childDecorrelationResult.symbolsToPropagate,
                            childDecorrelationResult.correlatedPredicates,
                            childDecorrelationResult.correlatedSymbolsMapping,
                            childDecorrelationResult.constantSymbols,
                            node.getCount() == 1))
                    .or(() -> Optional.of(new DecorrelationResult(
                            // no ordering symbols are left - convert to LimitNode
                            new LimitNode(node.getPlanNodeId(), decorrelatedChildNode, node.getCount(), Optional.empty()),
                            childDecorrelationResult.symbolsToPropagate,
                            childDecorrelationResult.correlatedPredicates,
                            childDecorrelationResult.correlatedSymbolsMapping,
                            childDecorrelationResult.constantSymbols,
                            node.getCount() == 1)));
        }

        if (!constantSymbols.containsAll(childDecorrelationResult.symbolsToPropagate)) {
            return Optional.empty();
        }

        return decorrelatedOrderingScheme
                .map(orderingScheme -> {
                    // ordering symbols are present - rewrite TopN to TopNRankingNode partitioned by constant symbols
                    TopNRankingNode topNRankingNode = new TopNRankingNode(
                            node.getId(),
                            decorrelatedChildNode,
                            new DataOrganizationSpecification(
                                    ImmutableList.copyOf(childDecorrelationResult.symbolsToPropagate),
                                    Optional.of(orderingScheme)),
                            ROW_NUMBER,
                            symbolAllocator.newSymbol("ranking", BIGINT),
                            toIntExact(node.getCount()),
                            false,
                            Optional.empty());

                    return Optional.of(new DecorrelationResult(
                            topNRankingNode,
                            childDecorrelationResult.symbolsToPropagate,
                            childDecorrelationResult.correlatedPredicates,
                            childDecorrelationResult.correlatedSymbolsMapping,
                            childDecorrelationResult.constantSymbols,
                            node.getCount() == 1));
                })
                .orElseGet(() -> {
                    // no ordering symbols are left - rewrite TopN to RowNumberNode partitioned by constant symbols
                    RowNumberNode rowNumberNode = new RowNumberNode(
                            node.getId(),
                            decorrelatedChildNode,
                            ImmutableList.copyOf(childDecorrelationResult.symbolsToPropagate),
                            false,
                            symbolAllocator.newSymbol("row_number", BIGINT),
                            Optional.of(toIntExact(node.getCount())),
                            Optional.empty());

                    return Optional.of(new DecorrelationResult(
                            rowNumberNode,
                            childDecorrelationResult.symbolsToPropagate,
                            childDecorrelationResult.correlatedPredicates,
                            childDecorrelationResult.correlatedSymbolsMapping,
                            childDecorrelationResult.constantSymbols,
                            node.getCount() == 1));
                });
    }

    private Optional<OrderingScheme> decorrelateOrderingScheme(
        OrderingScheme orderingScheme, Set<Symbol> constantSymbols) {
      // remove local and remote constant sort symbols from the OrderingScheme
      ImmutableList.Builder<Symbol> nonConstantOrderBy = ImmutableList.builder();
      ImmutableMap.Builder<Symbol, SortOrder> nonConstantOrderings = ImmutableMap.builder();
      for (Symbol symbol : orderingScheme.getOrderBy()) {
        if (!constantSymbols.contains(symbol) && !correlation.contains(symbol)) {
          nonConstantOrderBy.add(symbol);
          nonConstantOrderings.put(symbol, orderingScheme.getOrdering(symbol));
        }
      }
      if (nonConstantOrderBy.build().isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(
          new OrderingScheme(nonConstantOrderBy.build(), nonConstantOrderings.buildOrThrow()));
    }*/

    @Override
    public Optional<DecorrelationResult> visitAggregation(AggregationNode node, Void context) {
      // Aggregation with global grouping cannot be converted to aggregation grouped on constants.
      // Theoretically, if there are no constants to group on, the aggregation could be successfully
      // decorrelated.
      // However, it can only happen when Aggregation's source plan is not correlated.
      // Then:
      // - either we should not reach here because uncorrelated subplans of correlated filters are
      // not explored,
      // - or the aggregation contains correlation which is unresolvable. This is indicated by
      // returning Optional.empty().
      if (node.hasEmptyGroupingSet()) {
        return Optional.empty();
      }

      if (node.getGroupingSetCount() != 1) {
        return Optional.empty();
      }

      Optional<DecorrelationResult> childDecorrelationResultOptional =
          node.getChild().accept(this, null);
      if (!childDecorrelationResultOptional.isPresent()) {
        return Optional.empty();
      }

      DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
      Set<Symbol> constantSymbols = childDecorrelationResult.getConstantSymbols();

      AggregationNode decorrelatedAggregation =
          childDecorrelationResult
              .getCorrelatedSymbolMapper()
              .map(node, childDecorrelationResult.node);

      Set<Symbol> groupingKeys = ImmutableSet.copyOf(node.getGroupingKeys());
      checkState(
          ImmutableSet.copyOf(decorrelatedAggregation.getGroupingKeys()).equals(groupingKeys),
          "grouping keys were correlated");
      List<Symbol> symbolsToAdd =
          childDecorrelationResult.symbolsToPropagate.stream()
              .filter(symbol -> !groupingKeys.contains(symbol))
              .collect(toImmutableList());

      if (!constantSymbols.containsAll(symbolsToAdd)) {
        return Optional.empty();
      }

      AggregationNode newAggregation =
          AggregationNode.builderFrom(decorrelatedAggregation)
              .setGroupingSets(
                  singleGroupingSet(
                      ImmutableList.<Symbol>builder()
                          .addAll(node.getGroupingKeys())
                          .addAll(symbolsToAdd)
                          .build()))
              .setPreGroupedSymbols(ImmutableList.of())
              .build();

      return Optional.of(
          new DecorrelationResult(
              newAggregation,
              childDecorrelationResult.symbolsToPropagate,
              childDecorrelationResult.correlatedPredicates,
              childDecorrelationResult.correlatedSymbolsMapping,
              childDecorrelationResult.constantSymbols,
              constantSymbols.containsAll(newAggregation.getGroupingKeys())));
    }

    @Override
    public Optional<DecorrelationResult> visitProject(ProjectNode node, Void context) {
      Optional<DecorrelationResult> childDecorrelationResultOptional =
          node.getChild().accept(this, null);
      if (!childDecorrelationResultOptional.isPresent()) {
        return Optional.empty();
      }

      DecorrelationResult childDecorrelationResult = childDecorrelationResultOptional.get();
      Set<Symbol> nodeOutputSymbols = ImmutableSet.copyOf(node.getOutputSymbols());
      List<Symbol> symbolsToAdd =
          childDecorrelationResult.symbolsToPropagate.stream()
              .filter(symbol -> !nodeOutputSymbols.contains(symbol))
              .collect(toImmutableList());

      Assignments assignments =
          Assignments.builder().putAll(node.getAssignments()).putIdentities(symbolsToAdd).build();

      return Optional.of(
          new DecorrelationResult(
              new ProjectNode(node.getPlanNodeId(), childDecorrelationResult.node, assignments),
              childDecorrelationResult.symbolsToPropagate,
              childDecorrelationResult.correlatedPredicates,
              childDecorrelationResult.correlatedSymbolsMapping,
              childDecorrelationResult.constantSymbols,
              childDecorrelationResult.atMostSingleRow));
    }

    private Multimap<Symbol, Symbol> extractCorrelatedSymbolsMapping(
        List<Expression> correlatedConjuncts) {
      ImmutableMultimap.Builder<Symbol, Symbol> mapping = ImmutableMultimap.builder();
      for (Expression conjunct : correlatedConjuncts) {
        if (!(conjunct instanceof ComparisonExpression)) {
          continue;
        }

        ComparisonExpression comparison = (ComparisonExpression) conjunct;

        if (!(comparison.getLeft() instanceof SymbolReference
            && comparison.getRight() instanceof SymbolReference
            && comparison.getOperator() == EQUAL)) {
          continue;
        }

        Symbol left = Symbol.from(comparison.getLeft());
        Symbol right = Symbol.from(comparison.getRight());

        if (correlation.contains(left) && !correlation.contains(right)) {
          mapping.put(left, right);
        }

        if (correlation.contains(right) && !correlation.contains(left)) {
          mapping.put(right, left);
        }
      }

      return mapping.build();
    }

    private Set<Symbol> extractConstantSymbols(List<Expression> correlatedConjuncts) {
      ImmutableSet.Builder<Symbol> constants = ImmutableSet.builder();

      correlatedConjuncts.stream()
          .filter(ComparisonExpression.class::isInstance)
          .map(ComparisonExpression.class::cast)
          .filter(comparison -> comparison.getOperator() == EQUAL)
          .forEach(
              comparison -> {
                Expression left = comparison.getLeft();
                Expression right = comparison.getRight();

                if (!isCorrelated(left)
                    && (left instanceof SymbolReference || isSimpleInjectiveCast(left))
                    && isConstant(right)) {
                  constants.add(getSymbol(left));
                }

                if (!isCorrelated(right)
                    && (right instanceof SymbolReference || isSimpleInjectiveCast(right))
                    && isConstant(left)) {
                  constants.add(getSymbol(right));
                }
              });

      return constants.build();
    }

    // checks whether the expression is a deterministic combination of correlation symbols
    private boolean isConstant(Expression expression) {
      return isDeterministic(expression)
          && ImmutableSet.copyOf(correlation)
              .containsAll(SymbolsExtractor.extractUnique(expression));
    }

    // checks whether the expression is an injective cast over a symbol
    private boolean isSimpleInjectiveCast(Expression expression) {
      if (!(expression instanceof Cast)) {
        return false;
      }
      Cast cast = (Cast) expression;
      if (!(cast.getExpression() instanceof SymbolReference)) {
        return false;
      }
      // simply return true for now
      return true;
      /*Symbol sourceSymbol = Symbol.from(cast.getExpression());

      Type sourceType = symbolAllocator.getTypes().getTableModelType(sourceSymbol);
      Type targetType = typeManager.getType(toTypeSignature(((Cast) expression).getType()));

      return typeCoercion.isInjectiveCoercion(sourceType, targetType);*/
    }

    private Symbol getSymbol(Expression expression) {
      if (expression instanceof SymbolReference) {
        return Symbol.from(expression);
      }
      return Symbol.from(((Cast) expression).getExpression());
    }

    private boolean isCorrelated(Expression expression) {
      return correlation.stream().anyMatch(SymbolsExtractor.extractUnique(expression)::contains);
    }
  }

  private static class DecorrelationResult {
    final PlanNode node;
    final Set<Symbol> symbolsToPropagate;
    final List<Expression> correlatedPredicates;

    // mapping from correlated symbols to their uncorrelated equivalence
    final Multimap<Symbol, Symbol> correlatedSymbolsMapping;

    // local (uncorrelated) symbols known to be constant based on their dependency on correlation
    // symbols
    // they are derived from the filter predicate, e.g.
    // a = corr --> a is constant
    // b = f(corr_1, corr_2, ...) --> b is constant provided that f is deterministic
    // cast(c AS ...) = corr --> c is constant provided that cast is injective
    final Set<Symbol> constantSymbols;

    // If a subquery has at most single row for any correlation values?
    final boolean atMostSingleRow;

    DecorrelationResult(
        PlanNode node,
        Set<Symbol> symbolsToPropagate,
        List<Expression> correlatedPredicates,
        Multimap<Symbol, Symbol> correlatedSymbolsMapping,
        Set<Symbol> constantSymbols,
        boolean atMostSingleRow) {
      this.node = node;
      this.symbolsToPropagate = symbolsToPropagate;
      this.correlatedPredicates = correlatedPredicates;
      this.atMostSingleRow = atMostSingleRow;
      this.correlatedSymbolsMapping = correlatedSymbolsMapping;
      this.constantSymbols = constantSymbols;
      checkState(
          constantSymbols.containsAll(correlatedSymbolsMapping.values()),
          "Expected constant symbols to contain all correlated symbols local equivalents");
      checkState(
          symbolsToPropagate.containsAll(constantSymbols),
          "Expected symbols to propagate to contain all constant symbols");
    }

    SymbolMapper getCorrelatedSymbolMapper() {
      return symbolMapper(
          correlatedSymbolsMapping.asMap().entrySet().stream()
              .collect(
                  toImmutableMap(
                      Map.Entry::getKey, symbols -> Iterables.getLast(symbols.getValue()))));
    }

    /**
     * @return constant symbols from a perspective of a subquery
     */
    Set<Symbol> getConstantSymbols() {
      return constantSymbols;
    }
  }

  private Optional<DecorrelatedNode> decorrelatedNode(
      List<Expression> correlatedPredicates, PlanNode node, List<Symbol> correlation) {
    if (containsCorrelation(node, correlation)) {
      // node is still correlated ; /
      return Optional.empty();
    }
    return Optional.of(new DecorrelatedNode(correlatedPredicates, node));
  }

  private boolean containsCorrelation(PlanNode node, List<Symbol> correlation) {
    return Sets.union(
            SymbolsExtractor.extractUnique(node, lookup),
            SymbolsExtractor.extractOutputSymbols(node, lookup))
        .stream()
        .anyMatch(correlation::contains);
  }

  public static class DecorrelatedNode {
    private final List<Expression> correlatedPredicates;
    private final PlanNode node;

    public DecorrelatedNode(List<Expression> correlatedPredicates, PlanNode node) {
      requireNonNull(correlatedPredicates, "correlatedPredicates is null");
      this.correlatedPredicates = ImmutableList.copyOf(correlatedPredicates);
      this.node = requireNonNull(node, "node is null");
    }

    public Optional<Expression> getCorrelatedPredicates() {
      if (correlatedPredicates.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(and(correlatedPredicates));
    }

    public PlanNode getNode() {
      return node;
    }
  }
}
