/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.SymbolMapper;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.Set;

import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Sets.intersection;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.topK;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.union;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

public class PushTopKThroughUnion implements Rule<TopKNode> {
  private static final Capture<UnionNode> CHILD = newCapture();

  private static final Pattern<TopKNode> PATTERN =
      topK().with(source().matching(union().capturedAs(CHILD)));

  @Override
  public Pattern<TopKNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(TopKNode topKNode, Captures captures, Context context) {
    UnionNode unionNode = captures.get(CHILD);

    ImmutableList.Builder<PlanNode> children = ImmutableList.builder();

    boolean shouldApply = false;
    for (PlanNode child : unionNode.getChildren()) {
      SymbolMapper.Builder symbolMapper = SymbolMapper.builder();
      Set<Symbol> sourceOutputSymbols = ImmutableSet.copyOf(child.getOutputSymbols());
      // This check is to ensure that we don't fire the optimizer if it was previously applied,
      // which is the same as PushLimitThroughUnion.
      if (isAtMost(child, context.getLookup(), topKNode.getCount())) {
        children.add(child);
      } else {
        shouldApply = true;
        for (Symbol unionOutput : unionNode.getOutputSymbols()) {
          Set<Symbol> inputSymbols =
              ImmutableSet.copyOf(unionNode.getSymbolMapping().get(unionOutput));
          Symbol unionInput = getLast(intersection(inputSymbols, sourceOutputSymbols));
          symbolMapper.put(unionOutput, unionInput);
        }
        children.add(
            symbolMapper
                .build()
                .map(
                    topKNode,
                    Collections.singletonList(child),
                    context.getIdAllocator().genPlanNodeId()));
      }
    }

    if (!shouldApply) {
      return Result.empty();
    }

    return Result.ofPlanNode(
        topKNode.replaceChildren(
            Collections.singletonList(
                new UnionNode(
                    unionNode.getPlanNodeId(),
                    children.build(),
                    unionNode.getSymbolMapping(),
                    unionNode.getOutputSymbols()))));
  }
}
