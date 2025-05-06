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
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AssignUniqueId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanNodeDecorrelator;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.type.LongType;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.LEFT;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.CorrelatedJoin.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.CorrelatedJoin.subquery;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.CorrelatedJoin.type;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.correlatedJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern.nonEmpty;

/**
 * This rule decorrelates a correlated subquery of LEFT correlated join with distinct operator
 * (grouped aggregation with no aggregation assignments)
 *
 * <p>Transforms:
 *
 * <pre>
 * - CorrelatedJoin LEFT (correlation: [c], filter: true, output: a, x)
 *      - Input (a, c)
 *      - Project (x <- b + 100)
 *           - Aggregation "distinct operator" group by [b]
 *                - Source (b) with correlated filter (b > c)
 * </pre>
 *
 * Into:
 *
 * <pre>
 * - Project (a <- a, x <- b + 100)
 *      - Aggregation "distinct operator" group by [a, c, unique, b]
 *           - LEFT join (filter: b > c)
 *                - UniqueId (unique)
 *                     - Input (a, c)
 *                - Source (b) decorrelated
 * </pre>
 */
public class TransformCorrelatedDistinctAggregationWithProjection
    implements Rule<CorrelatedJoinNode> {
  private static final Capture<ProjectNode> PROJECTION = newCapture();
  private static final Capture<AggregationNode> AGGREGATION = newCapture();

  private static final Pattern<CorrelatedJoinNode> PATTERN =
      correlatedJoin()
          .with(type().equalTo(LEFT))
          .with(nonEmpty(Patterns.CorrelatedJoin.correlation()))
          .with(filter().equalTo(TRUE_LITERAL))
          .with(
              subquery()
                  .matching(
                      project()
                          .capturedAs(PROJECTION)
                          .with(
                              source()
                                  .matching(
                                      aggregation()
                                          .matching(AggregationDecorrelation::isDistinctOperator)
                                          .capturedAs(AGGREGATION)))));

  private final PlannerContext plannerContext;

  public TransformCorrelatedDistinctAggregationWithProjection(PlannerContext plannerContext) {
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
  }

  @Override
  public Pattern<CorrelatedJoinNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context) {
    // decorrelate nested plan
    PlanNodeDecorrelator decorrelator =
        new PlanNodeDecorrelator(plannerContext, context.getSymbolAllocator(), context.getLookup());
    Optional<PlanNodeDecorrelator.DecorrelatedNode> decorrelatedSource =
        decorrelator.decorrelateFilters(
            captures.get(AGGREGATION).getChild(), correlatedJoinNode.getCorrelation());
    if (!decorrelatedSource.isPresent()) {
      return Result.empty();
    }

    PlanNode source = decorrelatedSource.get().getNode();

    // assign unique id on correlated join's input. It will be used to distinguish between original
    // input rows after join
    PlanNode inputWithUniqueId =
        new AssignUniqueId(
            context.getIdAllocator().genPlanNodeId(),
            correlatedJoinNode.getInput(),
            context.getSymbolAllocator().newSymbol("unique", LongType.getInstance()));

    JoinNode join =
        new JoinNode(
            context.getIdAllocator().genPlanNodeId(),
            JoinNode.JoinType.LEFT,
            inputWithUniqueId,
            source,
            ImmutableList.of(),
            Optional.empty(),
            inputWithUniqueId.getOutputSymbols(),
            source.getOutputSymbols(),
            decorrelatedSource.get().getCorrelatedPredicates(),
            Optional.empty());

    // restore aggregation
    AggregationNode aggregation = captures.get(AGGREGATION);
    aggregation =
        new AggregationNode(
            aggregation.getPlanNodeId(),
            join,
            aggregation.getAggregations(),
            singleGroupingSet(
                ImmutableList.<Symbol>builder()
                    .addAll(join.getLeftOutputSymbols())
                    .addAll(aggregation.getGroupingKeys())
                    .build()),
            ImmutableList.of(),
            aggregation.getStep(),
            Optional.empty(),
            Optional.empty());

    // restrict outputs and apply projection
    Set<Symbol> outputSymbols = new HashSet<>(correlatedJoinNode.getOutputSymbols());
    List<Symbol> expectedAggregationOutputs =
        aggregation.getOutputSymbols().stream()
            .filter(outputSymbols::contains)
            .collect(toImmutableList());

    Assignments assignments =
        Assignments.builder()
            .putIdentities(expectedAggregationOutputs)
            .putAll(captures.get(PROJECTION).getAssignments())
            .build();

    return Result.ofPlanNode(
        new ProjectNode(context.getIdAllocator().genPlanNodeId(), aggregation, assignments));
  }
}
