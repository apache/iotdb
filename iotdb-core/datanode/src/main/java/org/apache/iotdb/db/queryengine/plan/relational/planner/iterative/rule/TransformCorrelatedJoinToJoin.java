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
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanNodeDecorrelator;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.combineConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.LEFT;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.CorrelatedJoin.correlation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.correlatedJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern.nonEmpty;

/**
 * Tries to decorrelate subquery and rewrite it using normal join. Decorrelated predicates are part
 * of join condition.
 */
public class TransformCorrelatedJoinToJoin implements Rule<CorrelatedJoinNode> {
  private static final Pattern<CorrelatedJoinNode> PATTERN =
      correlatedJoin().with(nonEmpty(correlation()));

  private final PlannerContext plannerContext;

  public TransformCorrelatedJoinToJoin(PlannerContext plannerContext) {
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
  }

  @Override
  public Pattern<CorrelatedJoinNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context) {
    checkArgument(
        correlatedJoinNode.getJoinType() == INNER || correlatedJoinNode.getJoinType() == LEFT,
        "correlation in %s JOIN",
        correlatedJoinNode.getJoinType().name());
    PlanNode subquery = correlatedJoinNode.getSubquery();

    PlanNodeDecorrelator planNodeDecorrelator =
        new PlanNodeDecorrelator(plannerContext, context.getSymbolAllocator(), context.getLookup());
    Optional<PlanNodeDecorrelator.DecorrelatedNode> decorrelatedNodeOptional =
        planNodeDecorrelator.decorrelateFilters(subquery, correlatedJoinNode.getCorrelation());
    if (!decorrelatedNodeOptional.isPresent()) {
      return Result.empty();
    }
    PlanNodeDecorrelator.DecorrelatedNode decorrelatedSubquery = decorrelatedNodeOptional.get();

    Expression filter =
        combineConjuncts(
            decorrelatedSubquery.getCorrelatedPredicates().orElse(TRUE_LITERAL),
            correlatedJoinNode.getFilter());

    return Result.ofPlanNode(
        new JoinNode(
            correlatedJoinNode.getPlanNodeId(),
            correlatedJoinNode.getJoinType(),
            correlatedJoinNode.getInput(),
            decorrelatedSubquery.getNode(),
            ImmutableList.of(),
            Optional.empty(),
            correlatedJoinNode.getInput().getOutputSymbols(),
            correlatedJoinNode.getSubquery().getOutputSymbols(),
            filter.equals(TRUE_LITERAL) ? Optional.empty() : Optional.of(filter),
            Optional.empty()));
  }
}
