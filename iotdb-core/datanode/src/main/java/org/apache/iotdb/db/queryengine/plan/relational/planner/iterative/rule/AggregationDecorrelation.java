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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

class AggregationDecorrelation {
  private AggregationDecorrelation() {}

  public static boolean isDistinctOperator(PlanNode node) {
    return node instanceof AggregationNode
        && ((AggregationNode) node).getAggregations().isEmpty()
        && ((AggregationNode) node).getGroupingSetCount() == 1
        && ((AggregationNode) node).hasNonEmptyGroupingSet();
  }

  public static Map<Symbol, AggregationNode.Aggregation> rewriteWithMasks(
      Map<Symbol, AggregationNode.Aggregation> aggregations, Map<Symbol, Symbol> masks) {
    ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> rewritten = ImmutableMap.builder();
    for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : aggregations.entrySet()) {
      Symbol symbol = entry.getKey();
      AggregationNode.Aggregation aggregation = entry.getValue();
      rewritten.put(
          symbol,
          new AggregationNode.Aggregation(
              aggregation.getResolvedFunction(),
              aggregation.getArguments(),
              aggregation.isDistinct(),
              aggregation.getFilter(),
              aggregation.getOrderingScheme(),
              Optional.of(masks.get(symbol))));
    }

    return rewritten.buildOrThrow();
  }

  /**
   * Creates distinct aggregation node based on existing distinct aggregation node.
   *
   * @see #isDistinctOperator(PlanNode)
   */
  public static AggregationNode restoreDistinctAggregation(
      AggregationNode distinct, PlanNode source, List<Symbol> groupingKeys) {
    checkArgument(isDistinctOperator(distinct));
    return new AggregationNode(
        distinct.getPlanNodeId(),
        source,
        ImmutableMap.of(),
        AggregationNode.singleGroupingSet(groupingKeys),
        ImmutableList.of(),
        distinct.getStep(),
        Optional.empty(),
        Optional.empty());
  }
}
