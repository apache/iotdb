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
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CopyToNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Select shared budgets only for FIs whose fan-in can consume any ready input. */
public final class SharedExchangeMemoryEligibility {
  private static final Set<String> UNORDERED_AGGREGATIONS =
      Set.of("count", "sum", "avg", "min", "max");

  private SharedExchangeMemoryEligibility() {}

  public static boolean isEligible(PlanNode root) {
    boolean hasCollect = false;
    Deque<PlanNode> nodes = new ArrayDeque<>();
    nodes.push(root);
    while (!nodes.isEmpty()) {
      PlanNode node = nodes.pop();
      if (node instanceof ExchangeNode) {
        // An upstream plan, if still attached, belongs to a different FI.
        continue;
      }
      // TopK's input ordering assumptions remain unchanged, including its single-child form.
      if (node instanceof TopKNode) {
        return false;
      }
      // Core relational nodes need not implement the serialization-only PlanNode.getType().
      boolean anyReady = node instanceof CollectNode || node instanceof UnionNode;
      hasCollect |= anyReady;
      if (!canConsumeUnordered(node)) {
        return false;
      }
      List<PlanNode> children = node.getChildren();
      // Unknown multi-input operators are not implicitly safe. In particular, sequential sinks
      // and joins can wait for an input while another input holds the entire shared budget.
      if (!anyReady && children.size() > 1) {
        return false;
      }
      nodes.addAll(children);
    }
    return hasCollect;
  }

  private static boolean canConsumeUnordered(PlanNode node) {
    if (node instanceof AggregationNode) {
      AggregationNode aggregation = (AggregationNode) node;
      return aggregation.getPreGroupedSymbols().isEmpty()
          && aggregation.getAggregations().values().stream()
              .allMatch(
                  function ->
                      function.getOrderingScheme().isEmpty()
                          && !function.isInputOrderedByTimeAscending()
                          && UNORDERED_AGGREGATIONS.contains(
                              function
                                  .getResolvedFunction()
                                  .getSignature()
                                  .getName()
                                  .toLowerCase(Locale.ROOT)));
    }
    // Do not infer safety from the number of children. Windows, streaming grouping and
    // TopK can depend on order even when they have only one input. Unknown
    // consumers conservatively retain sequential consumption within this FI.
    return node instanceof CollectNode
        || node instanceof UnionNode
        || node instanceof OutputNode
        || node instanceof ProjectNode
        || node instanceof FilterNode
        || node instanceof IdentitySinkNode
        || node instanceof ExchangeNode
        || node instanceof FillNode
        || node instanceof CopyToNode
        || node instanceof LimitNode
        || node instanceof OffsetNode
        || node.getClass() == SortNode.class
        || node.getChildren().isEmpty();
  }
}
