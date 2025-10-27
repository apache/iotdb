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
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.patternRecognition;

public class ImplementPatternRecognition implements Rule<PatternRecognitionNode> {
  private static final Pattern<PatternRecognitionNode> PATTERN = patternRecognition();

  public ImplementPatternRecognition() {}

  @Override
  public Pattern<PatternRecognitionNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(PatternRecognitionNode node, Captures captures, Context context) {
    PlanNode childRef = getOnlyElement(node.getChildren());
    PlanNode underlying = context.getLookup().resolve(childRef);

    List<Symbol> sortSymbols = new ArrayList<>();
    Map<Symbol, SortOrder> sortOrderings = new HashMap<>();
    for (Symbol symbol : node.getPartitionBy()) {
      sortSymbols.add(symbol);
      sortOrderings.put(symbol, SortOrder.ASC_NULLS_LAST);
    }
    int sortKeyOffset = sortSymbols.size();
    node.getOrderingScheme()
        .ifPresent(
            scheme ->
                scheme
                    .getOrderBy()
                    .forEach(
                        symbol -> {
                          if (!sortOrderings.containsKey(symbol)) {
                            sortSymbols.add(symbol);
                            sortOrderings.put(symbol, scheme.getOrdering(symbol));
                          }
                        }));

    if (sortSymbols.isEmpty() || underlying instanceof GroupNode) {
      return Result.empty();
    }

    OrderingScheme orderingScheme = new OrderingScheme(sortSymbols, sortOrderings);
    GroupNode wrapped =
        new GroupNode(
            context.getIdAllocator().genPlanNodeId(), childRef, orderingScheme, sortKeyOffset);

    PatternRecognitionNode rewritten =
        new PatternRecognitionNode(
            node.getPlanNodeId(),
            wrapped,
            node.getPartitionBy(),
            node.getOrderingScheme(),
            node.getHashSymbol(),
            node.getMeasures(),
            node.getRowsPerMatch(),
            node.getSkipToLabels(),
            node.getSkipToPosition(),
            node.getPattern(),
            node.getVariableDefinitions());

    return Result.ofPlanNode(rewritten);
  }
}
