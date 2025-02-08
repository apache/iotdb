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
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;

import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.intersection;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor.extractUnique;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.correlatedJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.isScalar;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;

public class PruneCorrelatedJoinColumns extends ProjectOffPushDownRule<CorrelatedJoinNode> {
  public PruneCorrelatedJoinColumns() {
    super(correlatedJoin());
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(
      Context context, CorrelatedJoinNode correlatedJoinNode, Set<Symbol> referencedOutputs) {
    PlanNode input = correlatedJoinNode.getInput();
    PlanNode subquery = correlatedJoinNode.getSubquery();

    // remove unused correlated join node, retain input
    if (intersection(ImmutableSet.copyOf(subquery.getOutputSymbols()), referencedOutputs)
        .isEmpty()) {
      // remove unused subquery of inner join
      if (correlatedJoinNode.getJoinType() == JoinNode.JoinType.INNER
          && isScalar(subquery, context.getLookup())
          && correlatedJoinNode.getFilter().equals(TRUE_LITERAL)) {
        return Optional.of(input);
      }
      // remove unused subquery of left join
      if (correlatedJoinNode.getJoinType() == JoinNode.JoinType.LEFT
          && isAtMostScalar(subquery, context.getLookup())) {
        return Optional.of(input);
      }
    }

    Set<Symbol> referencedAndCorrelationSymbols =
        ImmutableSet.<Symbol>builder()
            .addAll(referencedOutputs)
            .addAll(correlatedJoinNode.getCorrelation())
            .build();

    // remove unused input node, retain subquery
    if (intersection(ImmutableSet.copyOf(input.getOutputSymbols()), referencedAndCorrelationSymbols)
        .isEmpty()) {
      // remove unused input of inner join
      if (correlatedJoinNode.getJoinType() == JoinNode.JoinType.INNER
          && isScalar(input, context.getLookup())
          && correlatedJoinNode.getFilter().equals(TRUE_LITERAL)) {
        return Optional.of(subquery);
      }
      // remove unused input of right join
      if (correlatedJoinNode.getJoinType() == JoinNode.JoinType.RIGHT
          && isAtMostScalar(input, context.getLookup())) {
        return Optional.of(subquery);
      }
    }

    Set<Symbol> filterSymbols = extractUnique(correlatedJoinNode.getFilter());

    Set<Symbol> referencedAndFilterSymbols =
        ImmutableSet.<Symbol>builder().addAll(referencedOutputs).addAll(filterSymbols).build();

    Optional<PlanNode> newSubquery =
        restrictOutputs(context.getIdAllocator(), subquery, referencedAndFilterSymbols);

    Set<Symbol> referencedAndFilterAndCorrelationSymbols =
        ImmutableSet.<Symbol>builder()
            .addAll(referencedAndFilterSymbols)
            .addAll(correlatedJoinNode.getCorrelation())
            .build();

    Optional<PlanNode> newInput =
        restrictOutputs(context.getIdAllocator(), input, referencedAndFilterAndCorrelationSymbols);

    boolean pruned = newSubquery.isPresent() || newInput.isPresent();

    if (pruned) {
      return Optional.of(
          new CorrelatedJoinNode(
              correlatedJoinNode.getPlanNodeId(),
              newInput.orElse(input),
              newSubquery.orElse(subquery),
              correlatedJoinNode.getCorrelation(),
              correlatedJoinNode.getJoinType(),
              correlatedJoinNode.getFilter(),
              correlatedJoinNode.getOriginSubquery()));
    }

    return Optional.empty();
  }
}
