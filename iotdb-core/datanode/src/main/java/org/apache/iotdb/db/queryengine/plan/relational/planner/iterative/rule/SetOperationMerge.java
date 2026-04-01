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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExceptNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntersectNode;
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

  /** Merge multiple union into one union or multiple intersect into one intersect */
  public Optional<SetOperationNode> merge() {

    checkState(
        node instanceof UnionNode || node instanceof IntersectNode,
        "unexpected node type: %s",
        node.getClass().getSimpleName());
    Lookup lookup = context.getLookup();
    // Pre-check
    boolean anyMerge =
        node.getChildren().stream()
            .map(lookup::resolve)
            .anyMatch(child -> node.getClass().equals(child.getClass()));
    if (!anyMerge) {
      return Optional.empty();
    }

    List<PlanNode> children =
        node.getChildren().stream().map(lookup::resolve).collect(toImmutableList());

    ImmutableListMultimap.Builder<Symbol, Symbol> newMappingsBuilder =
        ImmutableListMultimap.builder();

    boolean resultIsDistinct = false;
    boolean rewritten = false;

    for (int i = 0; i < children.size(); i++) {
      PlanNode child = children.get(i);

      // Determine if set operations can be merged and whether the resulting set operation is
      // quantified DISTINCT or ALL
      Optional<Boolean> mergedQuantifier = judgeQuantifierForUnionAndIntersect(node, child);
      if (mergedQuantifier.isPresent()) {
        addMergedMappings((SetOperationNode) child, i, newMappingsBuilder);
        // for intersect : as long as one of intersect node is (intersect distinct), the merged
        // intersect would be (intersect distinct).
        resultIsDistinct = resultIsDistinct || mergedQuantifier.get();
        rewritten = true;
      } else {
        // Keep mapping as it is
        addOriginalMappings(child, i, newMappingsBuilder);
      }
    }

    if (!rewritten) {
      return Optional.empty();
    }

    if (node instanceof UnionNode) {
      return Optional.of(
          new UnionNode(
              node.getPlanNodeId(),
              newSources,
              newMappingsBuilder.build(),
              node.getOutputSymbols()));
    }

    return Optional.of(
        new IntersectNode(
            node.getPlanNodeId(),
            newSources,
            newMappingsBuilder.build(),
            node.getOutputSymbols(),
            resultIsDistinct));
  }

  /** only for except node, only merge first source node */
  public Optional<SetOperationNode> mergeFirstSource() {

    checkState(
        node instanceof ExceptNode, "unexpected node type: %s", node.getClass().getSimpleName());

    Lookup lookup = context.getLookup();

    List<PlanNode> children =
        node.getChildren().stream().map(lookup::resolve).collect(toImmutableList());
    PlanNode firstChild = children.get(0);
    Optional<Boolean> mergedQuantifier = judgeQuantifierForExcept((ExceptNode) node, firstChild);
    if (!mergedQuantifier.isPresent()) {
      return Optional.empty();
    }

    ImmutableListMultimap.Builder<Symbol, Symbol> newMappingsBuilder =
        ImmutableListMultimap.builder();
    addMergedMappings((SetOperationNode) firstChild, 0, newMappingsBuilder);
    // Keep remaining as it is
    for (int i = 1; i < children.size(); i++) {
      PlanNode child = children.get(i);
      addOriginalMappings(child, i, newMappingsBuilder);
    }

    return Optional.of(
        new ExceptNode(
            node.getPlanNodeId(),
            newSources,
            newMappingsBuilder.build(),
            node.getOutputSymbols(),
            mergedQuantifier.get()));
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
   * <p>For parent and child of type UNION, merge is always possible and the assumed quantifier is
   * ALL, because UnionNode always represents UNION ALL.
   *
   * <p>For parent and child of type INTERSECT, merge is always possible: 1. If parent and child are
   * both INTERSECT ALL, the resulting set operation is INTERSECT ALL. 2. Otherwise, the resulting
   * set operation is INTERSECT DISTINCT: a. If the parent is DISTINCT, the result has unique
   * values, regardless of whether child branches were DISTINCT or ALL. b. If the child is DISTINCT,
   * that branch is guaranteed to have unique values, so at most one element of the other branches
   * will be retained -- this is equivalent to just doing DISTINCT on the parent.
   *
   * <p>Optional.empty() indicates that merge is not possible.
   *
   * <p>Optional.of(false) indicates that merged node with quantifier which is all.
   *
   * <p>Optional.of(true) indicates that merged node with quantifier which is distinct.
   */
  private Optional<Boolean> judgeQuantifierForUnionAndIntersect(
      SetOperationNode node, PlanNode child) {

    if (!node.getClass().equals(child.getClass())) {
      return Optional.empty();
    }

    if (node instanceof UnionNode) {
      return Optional.of(false);
    }

    if (node instanceof IntersectNode) {
      if (!((IntersectNode) node).isDistinct() && !((IntersectNode) child).isDistinct()) {
        return Optional.of(false);
      }
      return Optional.of(true);
    }

    // never should reach here
    throw new IllegalStateException(
        "unexpected setOperation node type: " + node.getClass().getSimpleName());
  }

  /**
   * Check if node and child are mergeable based on their set operation type and quantifier.
   *
   * <p>For parent and child of type EXCEPT: 1. if parent is EXCEPT DISTINCT and child is EXCEPT
   * ALL, merge is not possible 2. if parent and child are both EXCEPT DISTINCT, the resulting set
   * operation is EXCEPT DISTINCT 3. if parent and child are both EXCEPT ALL, the resulting set
   * operation is EXCEPT ALL 4. if parent is EXCEPT ALL and child is EXCEPT DISTINCT, the resulting
   * set operation is EXCEPT DISTINCT
   */
  private Optional<Boolean> judgeQuantifierForExcept(ExceptNode node, PlanNode child) {

    if (!node.getClass().equals(child.getClass())) {
      return Optional.empty();
    }

    if (node.isDistinct() && !((ExceptNode) child).isDistinct()) {
      return Optional.empty();
    }

    return Optional.of(((ExceptNode) child).isDistinct());
  }
}
