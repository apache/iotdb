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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ExpressionSymbolInliner.inlineSymbols;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.and;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.extractConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleAggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.semiJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * Rewrite filtering semi-join to inner join.
 *
 * <p>Transforms:
 *
 * <pre>
 * - Filter (semiJoinSymbol AND predicate)
 *    - SemiJoin (semiJoinSymbol <- (a IN b))
 *        source: plan A producing symbol a
 *        filtering source: plan B producing symbol b
 * </pre>
 *
 * <p>Into:
 *
 * <pre>
 * - Project (semiJoinSymbol <- TRUE)
 *    - Join INNER on (a = b), joinFilter (predicate with semiJoinSymbol replaced with TRUE)
 *       - source
 *       - Aggregation distinct(b)
 *          - filtering source
 * </pre>
 */
public class TransformFilteringSemiJoinToInnerJoin implements Rule<FilterNode> {
  private static final Capture<SemiJoinNode> SEMI_JOIN = newCapture();

  private static final Pattern<FilterNode> PATTERN =
      filter().with(source().matching(semiJoin().capturedAs(SEMI_JOIN)));

  @Override
  public Pattern<FilterNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(FilterNode filterNode, Captures captures, Context context) {
    SemiJoinNode semiJoin = captures.get(SEMI_JOIN);

    Symbol semiJoinSymbol = semiJoin.getSemiJoinOutput();
    Predicate<Expression> isSemiJoinSymbol =
        expression -> expression.equals(semiJoinSymbol.toSymbolReference());

    List<Expression> conjuncts = extractConjuncts(filterNode.getPredicate());
    if (conjuncts.stream().noneMatch(isSemiJoinSymbol)) {
      return Result.empty();
    }
    Expression filteredPredicate =
        and(conjuncts.stream().filter(isSemiJoinSymbol.negate()).collect(toImmutableList()));

    Expression simplifiedPredicate =
        inlineSymbols(
            symbol -> {
              if (symbol.equals(semiJoinSymbol)) {
                return TRUE_LITERAL;
              }
              return symbol.toSymbolReference();
            },
            filteredPredicate);

    Optional<Expression> joinFilter =
        simplifiedPredicate.equals(TRUE_LITERAL)
            ? Optional.empty()
            : Optional.of(simplifiedPredicate);

    PlanNode filteringSourceDistinct =
        singleAggregation(
            context.getIdAllocator().genPlanNodeId(),
            semiJoin.getFilteringSource(),
            ImmutableMap.of(),
            singleGroupingSet(ImmutableList.of(semiJoin.getFilteringSourceJoinSymbol())));

    JoinNode innerJoin =
        new JoinNode(
            semiJoin.getPlanNodeId(),
            INNER,
            semiJoin.getSource(),
            filteringSourceDistinct,
            ImmutableList.of(
                new JoinNode.EquiJoinClause(
                    semiJoin.getSourceJoinSymbol(), semiJoin.getFilteringSourceJoinSymbol())),
            Optional.empty(),
            semiJoin.getSource().getOutputSymbols(),
            ImmutableList.of(),
            joinFilter,
            Optional.empty());

    ProjectNode project =
        new ProjectNode(
            context.getIdAllocator().genPlanNodeId(),
            innerJoin,
            Assignments.builder()
                .putIdentities(innerJoin.getOutputSymbols())
                .put(semiJoinSymbol, TRUE_LITERAL)
                .build());

    return Result.ofPlanNode(project);
  }
}
