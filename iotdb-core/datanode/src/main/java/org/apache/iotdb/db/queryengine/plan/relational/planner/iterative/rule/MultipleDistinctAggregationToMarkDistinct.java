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
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MarkDistinctNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.aggregation;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;

/**
 * Implements distinct aggregations with different inputs by transforming plans of the following
 * shape:
 *
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(DISTINCT a0, a1, ...)
 *        F2(DISTINCT b0, b1, ...)
 *        F3(c0, c1, ...)
 *     - X
 * </pre>
 *
 * into
 *
 * <pre>
 * - Aggregation
 *        GROUP BY (k)
 *        F1(a0, a1, ...) mask ($0)
 *        F2(b0, b1, ...) mask ($1)
 *        F3(c0, c1, ...)
 *     - MarkDistinct (k, a0, a1, ...) -> $0
 *          - MarkDistinct (k, b0, b1, ...) -> $1
 *              - X
 * </pre>
 */
public class MultipleDistinctAggregationToMarkDistinct implements Rule<AggregationNode> {
  private static final Pattern<AggregationNode> PATTERN =
      aggregation()
          .matching(
              Predicates.and(
                  MultipleDistinctAggregationToMarkDistinct::hasNoDistinctWithFilterOrMask,
                  Predicates.or(
                      MultipleDistinctAggregationToMarkDistinct::hasMultipleDistincts,
                      MultipleDistinctAggregationToMarkDistinct::hasMixedDistinctAndNonDistincts)));

  private static boolean hasNoDistinctWithFilterOrMask(AggregationNode aggregationNode) {
    return aggregationNode.getAggregations().values().stream()
        .noneMatch(
            aggregation ->
                aggregation.isDistinct()
                    && (aggregation.getFilter().isPresent() || aggregation.getMask().isPresent()));
  }

  private static boolean hasMultipleDistincts(AggregationNode aggregationNode) {
    return aggregationNode.getAggregations().values().stream()
            .filter(AggregationNode.Aggregation::isDistinct)
            .map(AggregationNode.Aggregation::getArguments)
            .map(HashSet::new)
            .distinct()
            .count()
        > 1;
  }

  private static boolean hasMixedDistinctAndNonDistincts(AggregationNode aggregationNode) {
    long distincts =
        aggregationNode.getAggregations().values().stream()
            .filter(AggregationNode.Aggregation::isDistinct)
            .count();

    return distincts > 0 && distincts < aggregationNode.getAggregations().size();
  }

  @Override
  public Pattern<AggregationNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(AggregationNode parent, Captures captures, Context context) {
    if (!shouldAddMarkDistinct(parent, context)) {
      return Result.empty();
    }

    // the distinct marker for the given set of input columns
    Map<Set<Symbol>, Symbol> markers = new HashMap<>();

    Map<Symbol, AggregationNode.Aggregation> newAggregations = new HashMap<>();
    PlanNode subPlan = parent.getChild();

    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry :
        parent.getAggregations().entrySet()) {
      AggregationNode.Aggregation aggregation = entry.getValue();

      if (aggregation.isDistinct()
          && !aggregation.getFilter().isPresent()
          && !aggregation.getMask().isPresent()) {
        Set<Symbol> inputs = aggregation.getArguments().stream().map(Symbol::from).collect(toSet());

        Symbol marker = markers.get(inputs);
        if (marker == null) {
          marker =
              context
                  .getSymbolAllocator()
                  .newSymbol(Iterables.getLast(inputs).getName(), BOOLEAN, "distinct");
          markers.put(inputs, marker);

          ImmutableSet.Builder<Symbol> distinctSymbols =
              ImmutableSet.<Symbol>builder().addAll(parent.getGroupingKeys()).addAll(inputs);
          parent.getGroupIdSymbol().ifPresent(distinctSymbols::add);

          subPlan =
              new MarkDistinctNode(
                  context.getIdAllocator().genPlanNodeId(),
                  subPlan,
                  marker,
                  ImmutableList.copyOf(distinctSymbols.build()),
                  Optional.empty());
        }

        // remove the distinct flag and set the distinct marker
        newAggregations.put(
            entry.getKey(),
            new AggregationNode.Aggregation(
                aggregation.getResolvedFunction(),
                aggregation.getArguments(),
                false,
                aggregation.getFilter(),
                aggregation.getOrderingScheme(),
                Optional.of(marker)));
      } else {
        newAggregations.put(entry.getKey(), aggregation);
      }
    }

    return Result.ofPlanNode(
        AggregationNode.builderFrom(parent)
            .setSource(subPlan)
            .setAggregations(newAggregations)
            .setPreGroupedSymbols(ImmutableList.of())
            .build());
  }

  private boolean shouldAddMarkDistinct(AggregationNode aggregationNode, Context context) {
    if (aggregationNode.getGroupingKeys().isEmpty()) {
      // global distinct aggregation is computed using a single thread. MarkDistinct will help
      // parallelize the execution.
      return true;
    }
    if (aggregationNode.getGroupingKeys().size() > 1) {
      // NDV stats for multiple grouping keys are unreliable, let's keep MarkDistinct for this case
      // to avoid significant slowdown or OOM/too big hash table issues in case of
      // overestimation of very small NDV with big number of distinct values inside the groups.
      return true;
    }

    return false;
  }

  private static boolean hasSingleDistinctAndNonDistincts(AggregationNode aggregationNode) {
    long distincts =
        aggregationNode.getAggregations().values().stream()
            .filter(AggregationNode.Aggregation::isDistinct)
            .count();

    return distincts == 1 && distincts < aggregationNode.getAggregations().size();
  }
}
