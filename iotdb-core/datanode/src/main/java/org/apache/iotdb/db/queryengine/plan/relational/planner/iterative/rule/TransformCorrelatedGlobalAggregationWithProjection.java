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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.LongType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.and;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.AggregationDecorrelation.isDistinctOperator;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.AggregationDecorrelation.restoreDistinctAggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.AggregationDecorrelation.rewriteWithMasks;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.LEFT;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.Aggregation.groupingColumns;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.CorrelatedJoin.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.CorrelatedJoin.subquery;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.correlatedJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern.empty;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern.nonEmpty;

/**
 * This rule decorrelates a correlated subquery of LEFT or INNER correlated join with: - single
 * global aggregation, or - global aggregation over distinct operator (grouped aggregation with no
 * aggregation assignments), in case when the distinct operator cannot be de-correlated by
 * PlanNodeDecorrelator
 *
 * <p>In the case of single aggregation, it transforms:
 *
 * <pre>
 * - CorrelatedJoin LEFT or INNER (correlation: [c], filter: true, output: a, x, y)
 *      - Input (a, c)
 *      - Project (x <- f(count), y <- f'(agg))
 *           - Aggregation global
 *             count <- count(*)
 *             agg <- agg(b)
 *                - Source (b) with correlated filter (b > c)
 * </pre>
 *
 * Into:
 *
 * <pre>
 * - Project (a <- a, x <- f(count), y <- f'(agg))
 *      - Aggregation (group by [a, c, unique])
 *        count <- count(*) mask(non_null)
 *        agg <- agg(b) mask(non_null)
 *           - LEFT join (filter: b > c)
 *                - UniqueId (unique)
 *                     - Input (a, c)
 *                - Project (non_null <- TRUE)
 *                     - Source (b) decorrelated
 * </pre>
 *
 * <p>In the case of global aggregation over distinct operator, it transforms:
 *
 * <pre>
 * - CorrelatedJoin LEFT or INNER (correlation: [c], filter: true, output: a, x, y)
 *      - Input (a, c)
 *      - Project (x <- f(count), y <- f'(agg))
 *           - Aggregation global
 *             count <- count(*)
 *             agg <- agg(b)
 *                - Aggregation "distinct operator" group by [b]
 *                     - Source (b) with correlated filter (b > c)
 * </pre>
 *
 * Into:
 *
 * <pre>
 * - Project (a <- a, x <- f(count), y <- f'(agg))
 *      - Aggregation (group by [a, c, unique])
 *        count <- count(*) mask(non_null)
 *        agg <- agg(b) mask(non_null)
 *           - Aggregation "distinct operator" group by [a, c, unique, non_null, b]
 *                - LEFT join (filter: b > c)
 *                     - UniqueId (unique)
 *                          - Input (a, c)
 *                     - Project (non_null <- TRUE)
 *                          - Source (b) decorrelated
 * </pre>
 */
public class TransformCorrelatedGlobalAggregationWithProjection
    implements Rule<CorrelatedJoinNode> {
  private static final Capture<ProjectNode> PROJECTION = newCapture();
  private static final Capture<AggregationNode> AGGREGATION = newCapture();
  private static final Capture<PlanNode> SOURCE = newCapture();

  private static final Pattern<CorrelatedJoinNode> PATTERN =
      correlatedJoin()
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
                                          .with(empty(groupingColumns()))
                                          .with(source().capturedAs(SOURCE))
                                          .capturedAs(AGGREGATION)))));

  private final PlannerContext plannerContext;

  public TransformCorrelatedGlobalAggregationWithProjection(PlannerContext plannerContext) {
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
        "unexpected correlated join type: %s",
        correlatedJoinNode.getJoinType());

    // if there is another aggregation below the AggregationNode, handle both
    PlanNode source = captures.get(SOURCE);

    // if we fail to decorrelate the nested plan, and it contains a distinct operator, we can
    // extract and special-handle the distinct operator
    AggregationNode distinct = null;

    // decorrelate nested plan
    PlanNodeDecorrelator decorrelator =
        new PlanNodeDecorrelator(plannerContext, context.getSymbolAllocator(), context.getLookup());
    Optional<PlanNodeDecorrelator.DecorrelatedNode> decorrelatedSource =
        decorrelator.decorrelateFilters(source, correlatedJoinNode.getCorrelation());
    if (!decorrelatedSource.isPresent()) {
      // we failed to decorrelate the nested plan, so check if we can extract a distinct operator
      // from the nested plan
      if (isDistinctOperator(source)) {
        distinct = (AggregationNode) source;
        source = distinct.getChild();
        decorrelatedSource =
            decorrelator.decorrelateFilters(source, correlatedJoinNode.getCorrelation());
      }
      if (!decorrelatedSource.isPresent()) {
        return Result.empty();
      }
    }

    source = decorrelatedSource.get().getNode();

    // append non-null symbol on nested plan. It will be used to restore semantics of null-sensitive
    // aggregations after LEFT join
    Symbol nonNull = context.getSymbolAllocator().newSymbol("non_null", BooleanType.getInstance());
    source =
        new ProjectNode(
            context.getIdAllocator().genPlanNodeId(),
            source,
            Assignments.builder()
                .putIdentities(source.getOutputSymbols())
                .put(nonNull, TRUE_LITERAL)
                .build());

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

    PlanNode root = join;

    // restore distinct aggregation
    if (distinct != null) {
      root =
          restoreDistinctAggregation(
              distinct,
              join,
              ImmutableList.<Symbol>builder()
                  .addAll(join.getLeftOutputSymbols())
                  .add(nonNull)
                  .addAll(distinct.getGroupingKeys())
                  .build());
    }

    // prepare mask symbols for aggregations
    // Every original aggregation agg() will be rewritten to agg() mask(non_null). If the
    // aggregation
    // already has a mask, it will be replaced with conjunction of the existing mask and non_null.
    // This is necessary to restore the original aggregation result in case when:
    // - the nested lateral subquery returned empty result for some input row,
    // - aggregation is null-sensitive, which means that its result over a single null row is
    // different
    //   than result for empty input (with global grouping)
    // It applies to the following aggregate functions: count(*), checksum(), array_agg().
    AggregationNode globalAggregation = captures.get(AGGREGATION);
    ImmutableMap.Builder<Symbol, Symbol> masks = ImmutableMap.builder();
    Assignments.Builder assignmentsBuilder = Assignments.builder();
    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry :
        globalAggregation.getAggregations().entrySet()) {
      AggregationNode.Aggregation aggregation = entry.getValue();
      if (aggregation.getMask().isPresent()) {
        Symbol newMask = context.getSymbolAllocator().newSymbol("mask", BooleanType.getInstance());
        Expression expression =
            and(aggregation.getMask().get().toSymbolReference(), nonNull.toSymbolReference());
        assignmentsBuilder.put(newMask, expression);
        masks.put(entry.getKey(), newMask);
      } else {
        masks.put(entry.getKey(), nonNull);
      }
    }
    Assignments maskAssignments = assignmentsBuilder.build();
    if (!maskAssignments.isEmpty()) {
      root =
          new ProjectNode(
              context.getIdAllocator().genPlanNodeId(),
              root,
              Assignments.builder()
                  .putIdentities(root.getOutputSymbols())
                  .putAll(maskAssignments)
                  .build());
    }

    // restore global aggregation
    globalAggregation =
        new AggregationNode(
            globalAggregation.getPlanNodeId(),
            root,
            rewriteWithMasks(globalAggregation.getAggregations(), masks.buildOrThrow()),
            singleGroupingSet(
                ImmutableList.<Symbol>builder()
                    .addAll(join.getLeftOutputSymbols())
                    .addAll(globalAggregation.getGroupingKeys())
                    .build()),
            ImmutableList.of(),
            globalAggregation.getStep(),
            Optional.empty(),
            Optional.empty());

    // restrict outputs and apply projection
    Set<Symbol> outputSymbols = new HashSet<>(correlatedJoinNode.getOutputSymbols());
    List<Symbol> expectedAggregationOutputs =
        globalAggregation.getOutputSymbols().stream()
            .filter(outputSymbols::contains)
            .collect(toImmutableList());

    Assignments assignments =
        Assignments.builder()
            .putIdentities(expectedAggregationOutputs)
            .putAll(captures.get(PROJECTION).getAssignments())
            .build();

    return Result.ofPlanNode(
        new ProjectNode(context.getIdAllocator().genPlanNodeId(), globalAggregation, assignments));
  }
}
