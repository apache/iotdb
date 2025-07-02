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
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ResolvedFunction;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ApplyNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanNodeDecorrelator;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.read.common.type.LongType;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.globalAggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleAggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.LEFT;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.applyNode;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.Util.getResolvedBuiltInAggregateFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toSqlType;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;

/**
 * EXISTS is modeled as (if correlated predicates are equality comparisons):
 *
 * <pre>
 *     - Project(exists := COALESCE(subqueryTrue, false))
 *       - CorrelatedJoin(LEFT)
 *         - input
 *         - Project(subqueryTrue := true)
 *           - Limit(count=1)
 *             - subquery
 * </pre>
 *
 * or:
 *
 * <pre>
 *     - CorrelatedJoin(LEFT)
 *       - input
 *       - Project($0 > 0)
 *         - Aggregation(COUNT(*))
 *           - subquery
 * </pre>
 *
 * otherwise
 */
public class TransformExistsApplyToCorrelatedJoin implements Rule<ApplyNode> {
  private static final Pattern<ApplyNode> PATTERN = applyNode();

  private final PlannerContext plannerContext;

  public TransformExistsApplyToCorrelatedJoin(PlannerContext plannerContext) {
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
  }

  @Override
  public Pattern<ApplyNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(ApplyNode parent, Captures captures, Context context) {
    if (parent.getSubqueryAssignments().size() != 1) {
      return Result.empty();
    }

    ApplyNode.SetExpression expression = getOnlyElement(parent.getSubqueryAssignments().values());
    if (!(expression instanceof ApplyNode.Exists)) {
      return Result.empty();
    }

    /*
    Empty correlation list indicates that the subquery contains no correlation symbols from the
    immediate outer scope. The subquery might be either not correlated at all, or correlated with
    symbols from further outer scope.
    Currently, the two cases are indistinguishable.
    To support the latter case, the ApplyNode with empty correlation list is rewritten to default
    aggregation, which is inefficient in the rare case of uncorrelated EXISTS subquery,
    but currently allows to successfully decorrelate a correlated EXISTS subquery.

    Perhaps we can remove this condition when exploratory optimizer is implemented or support for decorrelating joins is implemented in PlanNodeDecorrelator
    */
    if (parent.getCorrelation().isEmpty()) {
      return Result.ofPlanNode(rewriteToDefaultAggregation(parent, context));
    }

    Optional<PlanNode> nonDefaultAggregation = rewriteToNonDefaultAggregation(parent, context);
    return nonDefaultAggregation
        .map(Result::ofPlanNode)
        .orElseGet(() -> Result.ofPlanNode(rewriteToDefaultAggregation(parent, context)));
  }

  private Optional<PlanNode> rewriteToNonDefaultAggregation(ApplyNode applyNode, Context context) {
    checkState(
        applyNode.getSubquery().getOutputSymbols().isEmpty(),
        "Expected subquery output symbols to be pruned");

    Symbol subqueryTrue = context.getSymbolAllocator().newSymbol("subqueryTrue", BOOLEAN);

    PlanNode subquery =
        new ProjectNode(
            context.getIdAllocator().genPlanNodeId(),
            new LimitNode(
                context.getIdAllocator().genPlanNodeId(),
                applyNode.getSubquery(),
                1L,
                Optional.empty()),
            Assignments.of(subqueryTrue, TRUE_LITERAL));

    PlanNodeDecorrelator decorrelator =
        new PlanNodeDecorrelator(plannerContext, context.getSymbolAllocator(), context.getLookup());
    if (!decorrelator.decorrelateFilters(subquery, applyNode.getCorrelation()).isPresent()) {
      return Optional.empty();
    }

    Symbol exists = getOnlyElement(applyNode.getSubqueryAssignments().keySet());
    Assignments.Builder assignments =
        Assignments.builder()
            .putIdentities(applyNode.getInput().getOutputSymbols())
            .put(
                exists,
                new CoalesceExpression(
                    ImmutableList.of(
                        subqueryTrue.toSymbolReference(), BooleanLiteral.FALSE_LITERAL)));

    return Optional.of(
        new ProjectNode(
            context.getIdAllocator().genPlanNodeId(),
            new CorrelatedJoinNode(
                applyNode.getPlanNodeId(),
                applyNode.getInput(),
                subquery,
                applyNode.getCorrelation(),
                LEFT,
                TRUE_LITERAL,
                applyNode.getOriginSubquery()),
            assignments.build()));
  }

  private PlanNode rewriteToDefaultAggregation(ApplyNode applyNode, Context context) {
    ResolvedFunction countFunction =
        getResolvedBuiltInAggregateFunction(
            plannerContext.getMetadata(), "count", ImmutableList.of());
    Symbol count = context.getSymbolAllocator().newSymbol("count", LongType.getInstance());
    Symbol exists = getOnlyElement(applyNode.getSubqueryAssignments().keySet());

    return new CorrelatedJoinNode(
        applyNode.getPlanNodeId(),
        applyNode.getInput(),
        new ProjectNode(
            context.getIdAllocator().genPlanNodeId(),
            singleAggregation(
                context.getIdAllocator().genPlanNodeId(),
                applyNode.getSubquery(),
                ImmutableMap.of(
                    count,
                    new AggregationNode.Aggregation(
                        countFunction,
                        ImmutableList.of(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())),
                globalAggregation()),
            Assignments.of(
                exists,
                new ComparisonExpression(
                    GREATER_THAN,
                    count.toSymbolReference(),
                    new Cast(new LongLiteral("0"), toSqlType(LongType.getInstance()))))),
        applyNode.getCorrelation(),
        INNER,
        TRUE_LITERAL,
        applyNode.getOriginSubquery());
  }
}
