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

import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.join;

/**
 * Joins support output symbol selection, so make any project-off of child columns explicit in
 * project nodes.
 */
public class PruneJoinChildrenColumns implements Rule<JoinNode> {
  private static final Pattern<JoinNode> PATTERN = join();

  @Override
  public Pattern<JoinNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(JoinNode joinNode, Captures captures, Context context) {
    Set<Symbol> globallyUsableInputs =
        ImmutableSet.<Symbol>builder()
            .addAll(joinNode.getOutputSymbols())
            .addAll(
                joinNode.getFilter().map(SymbolsExtractor::extractUnique).orElse(ImmutableSet.of()))
            .build();

    Set<Symbol> leftUsableInputs =
        ImmutableSet.<Symbol>builder()
            .addAll(globallyUsableInputs)
            .addAll(
                joinNode.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).iterator())
            // .addAll(joinNode.getLeftHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()))
            .build();

    Set<Symbol> rightUsableInputs =
        ImmutableSet.<Symbol>builder()
            .addAll(globallyUsableInputs)
            .addAll(
                joinNode.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).iterator())
            // .addAll(joinNode.getRightHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()))
            .build();

    return restrictChildOutputs(
            context.getIdAllocator(), joinNode, leftUsableInputs, rightUsableInputs)
        .map(Result::ofPlanNode)
        .orElse(Result.empty());
  }
}
