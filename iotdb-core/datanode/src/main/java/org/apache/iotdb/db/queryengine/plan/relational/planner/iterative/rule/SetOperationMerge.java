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
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SetOperationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class SetOperationMerge {

  private final Rule.Context context;
  private final SetOperationNode node;
  private final List<PlanNode> newSources;

  public SetOperationMerge(SetOperationNode node, Rule.Context context) {
    this.node = node;
    this.context = context;
    this.newSources = new ArrayList<>();
  }

  // Merge multiple union into one union
  public Optional<SetOperationNode> merge() {

    checkState(
        node instanceof UnionNode, "unexpected node type: %s", node.getClass().getSimpleName());
    Lookup lookup = context.getLookup();
    // Pre-check
    boolean anyMerge =
        node.getChildren().stream()
            .map(lookup::resolve)
            .anyMatch(child -> node.getClass().equals(child.getClass()));
    if (!anyMerge) {
      return Optional.empty();
    }

    List<PlanNode> childrenOfUnion =
        node.getChildren().stream().map(lookup::resolve).collect(toImmutableList());

    ImmutableListMultimap.Builder<Symbol, Symbol> newMappingsBuilder =
        ImmutableListMultimap.builder();

    boolean rewritten = false;

    for (int i = 0; i < childrenOfUnion.size(); i++) {
      PlanNode child = childrenOfUnion.get(i);

      // Determine if set operations can be merged and whether the resulting set operation is
      // quantified DISTINCT or ALL
      Optional<Boolean> mergedQuantifier = mergedQuantifierIsDistinct(node, child);
      if (mergedQuantifier.isPresent()) {
        addMergedMappings((SetOperationNode) child, i, newMappingsBuilder);
        rewritten = true;
      } else {
        // Keep mapping as it is
        addOriginalMappings(child, i, newMappingsBuilder);
      }
    }

    if (!rewritten) {
      return Optional.empty();
    }

    // the union has merged
    return Optional.of(
        new UnionNode(
            node.getPlanNodeId(), newSources, newMappingsBuilder.build(), node.getOutputSymbols()));
  }

  private void addMergedMappings(
      SetOperationNode child,
      int childIndex,
      ImmutableListMultimap.Builder<Symbol, Symbol> newMappingsBuilder) {

    newSources.addAll(child.getChildren());
    Map<Symbol, Collection<Symbol>> symbolMappings = node.getSymbolMapping().asMap();
    for (Map.Entry<Symbol, Collection<Symbol>> mapping : symbolMappings.entrySet()) {
      Symbol input = Iterables.get(mapping.getValue(), childIndex);
      newMappingsBuilder.putAll(mapping.getKey(), child.getSymbolMapping().get(input));
    }
  }

  private void addOriginalMappings(
      PlanNode child,
      int childIndex,
      ImmutableListMultimap.Builder<Symbol, Symbol> newMappingsBuilder) {

    newSources.add(child);
    Map<Symbol, Collection<Symbol>> symbolMappings = node.getSymbolMapping().asMap();
    for (Map.Entry<Symbol, Collection<Symbol>> mapping : symbolMappings.entrySet()) {
      newMappingsBuilder.put(mapping.getKey(), Iterables.get(mapping.getValue(), childIndex));
    }
  }

  /**
   * Check if node and child are mergeable based on their set operation type and quantifier.
   *
   * <p>Optional.empty() indicates that merge is not possible.
   */
  private Optional<Boolean> mergedQuantifierIsDistinct(SetOperationNode node, PlanNode child) {

    if (!node.getClass().equals(child.getClass())) {
      return Optional.empty();
    }

    if (node instanceof UnionNode) {
      return Optional.of(false);
    }

    // the Judgment logic for intersect and except wait for supplying
    return Optional.empty();
  }
}
