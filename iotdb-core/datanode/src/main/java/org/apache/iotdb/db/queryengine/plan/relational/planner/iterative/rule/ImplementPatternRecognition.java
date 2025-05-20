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

  //  @Override
  //  public Result apply(PatternRecognitionNode node, Captures captures, Context context) {
  //    // generate GroupNode
  //    PlanNode child = getOnlyElement(node.getChildren());
  //
  //    List<Symbol> sortSymbols = new ArrayList<>();
  //    Map<Symbol, SortOrder> sortOrderings = new HashMap<>();
  //    for (Symbol symbol : node.getPartitionBy()) {
  //      sortSymbols.add(symbol);
  //      sortOrderings.put(symbol, SortOrder.ASC_NULLS_LAST);
  //    }
  //    int sortKeyOffset = sortSymbols.size();
  //    node.getOrderingScheme()
  //        .ifPresent(
  //            orderingScheme -> {
  //              for (Symbol symbol : orderingScheme.getOrderBy()) {
  //                if (!sortOrderings.containsKey(symbol)) {
  //                  sortSymbols.add(symbol);
  //                  sortOrderings.put(symbol, orderingScheme.getOrdering(symbol));
  //                }
  //              }
  //            });
  //
  //    if (sortSymbols.isEmpty() || child instanceof GroupNode) { // ensure the idempotence of the
  // call
  //      return Result.empty();
  //    }
  //
  //    OrderingScheme scheme = new OrderingScheme(sortSymbols, sortOrderings);
  //    GroupNode wrapper =
  //        new GroupNode(context.getIdAllocator().genPlanNodeId(), child, scheme, sortKeyOffset);
  //
  //    return Result.ofPlanNode(
  //        new PatternRecognitionNode(
  //            node.getPlanNodeId(),
  //            wrapper,
  //            node.getPartitionBy(),
  //            node.getOrderingScheme(),
  //            node.getHashSymbol(),
  //            node.getMeasures(),
  //            node.getRowsPerMatch(),
  //            node.getSkipToLabels(),
  //            node.getSkipToPosition(),
  //            node.getPattern(),
  //            node.getVariableDefinitions()));
  //  }

  //  @Override
  //  public Result apply(PatternRecognitionNode node, Captures captures, Context context) {
  //    // 只拿到唯一的 child
  //    PlanNode originalChild = getOnlyElement(node.getChildren());
  //
  //    // 构造 partitionBy + orderingScheme 的排序键和顺序
  //    List<Symbol> sortSymbols = new ArrayList<>();
  //    Map<Symbol, SortOrder> sortOrderings = new HashMap<>();
  //    for (Symbol symbol : node.getPartitionBy()) {
  //      sortSymbols.add(symbol);
  //      sortOrderings.put(symbol, SortOrder.ASC_NULLS_LAST);
  //    }
  //    int sortKeyOffset = sortSymbols.size();
  //    node.getOrderingScheme()
  //        .ifPresent(
  //            orderingScheme -> {
  //              for (Symbol symbol : orderingScheme.getOrderBy()) {
  //                if (!sortOrderings.containsKey(symbol)) {
  //                  sortSymbols.add(symbol);
  //                  sortOrderings.put(symbol, orderingScheme.getOrdering(symbol));
  //                }
  //              }
  //            });
  //
  //    // 幂等性检查：无排序键 / 已经 wrap 过，就跳过
  //    if (sortSymbols.isEmpty()
  //        || originalChild instanceof GroupNode
  //        || originalChild instanceof GroupReference) {
  //      return Result.empty();
  //    }
  //
  //    // 真正创建一个只包一层的 GroupNode
  //    OrderingScheme scheme = new OrderingScheme(sortSymbols, sortOrderings);
  //    GroupNode wrapped =
  //        new GroupNode(
  //            context.getIdAllocator().genPlanNodeId(), originalChild, scheme, sortKeyOffset);
  //
  //    // 用新的 wrapped child 构建一个新的 PatternRecognitionNode
  //    PatternRecognitionNode rewritten =
  //        new PatternRecognitionNode(
  //            node.getPlanNodeId(),
  //            wrapped,
  //            node.getPartitionBy(),
  //            node.getOrderingScheme(),
  //            node.getHashSymbol(),
  //            node.getMeasures(),
  //            node.getRowsPerMatch(),
  //            node.getSkipToLabels(),
  //            node.getSkipToPosition(),
  //            node.getPattern(),
  //            node.getVariableDefinitions());
  //
  //    return Result.ofPlanNode(rewritten);
  //  }

  @Override
  public Result apply(PatternRecognitionNode node, Captures captures, Context context) {
    // 1) 拿到唯一的 child（在 memo 里总是一个 GroupReference）
    PlanNode childRef = getOnlyElement(node.getChildren());

    // 2) 用 lookup 把它“打开”成真实的 PlanNode
    PlanNode underlying = context.getLookup().resolve(childRef);

    // 3) 准备 sortKeys
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

    // 4) 幂等性 guard：如果没有任何排序键，或者底层已经是 GroupNode，就啥也不做
    if (sortSymbols.isEmpty() || underlying instanceof GroupNode) {
      return Result.empty();
    }

    // 5) 构造新的 GroupNode，child 依然传入原始的 GroupReference
    OrderingScheme orderingScheme = new OrderingScheme(sortSymbols, sortOrderings);
    GroupNode wrapped =
        new GroupNode(
            context.getIdAllocator().genPlanNodeId(), childRef, orderingScheme, sortKeyOffset);

    // 6) 用 wrapped 构造出新的 PatternRecognitionNode 返回
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
