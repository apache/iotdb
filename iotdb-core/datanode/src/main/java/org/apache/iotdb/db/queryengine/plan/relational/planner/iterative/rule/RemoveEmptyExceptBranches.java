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
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExceptNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleAggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.isEmpty;

public class RemoveEmptyExceptBranches implements Rule<ExceptNode> {
  private final Pattern<ExceptNode> pattern = Patterns.except();

  @Override
  public Pattern<ExceptNode> getPattern() {
    return pattern;
  }

  @Override
  public Result apply(ExceptNode node, Captures captures, Context context) {

    //  pre-check
    Lookup lookup = context.getLookup();
    ImmutableList.Builder<Boolean> emptyFlagsBuilder = ImmutableList.builder();
    boolean anyEmpty = false;
    for (PlanNode child : node.getChildren()) {
      boolean isEmpty = isEmpty(child, lookup);
      anyEmpty |= isEmpty;
      emptyFlagsBuilder.add(isEmpty);
    }
    if (!anyEmpty) {
      return Result.empty();
    }

    ImmutableList<Boolean> emptyFlags = emptyFlagsBuilder.build();
    // if the first child of Except is empty set, replace the Except Node with Project node,
    // and let the first child append to the Project node
    if (emptyFlags.get(0)) {
      Assignments.Builder assignments = Assignments.builder();
      for (Symbol symbol : node.getOutputSymbols()) {
        assignments.put(symbol, node.getSymbolMapping().get(symbol).get(0).toSymbolReference());
      }
      return Result.ofPlanNode(
          new ProjectNode(node.getPlanNodeId(), node.getChildren().get(0), assignments.build()));
    }

    boolean hasEmptyBranches = false;
    ImmutableList.Builder<PlanNode> newSourcesBuilder = ImmutableList.builder();
    ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputsBuilder =
        ImmutableListMultimap.builder();
    for (int i = 0; i < node.getChildren().size(); i++) {
      PlanNode source = node.getChildren().get(i);
      // first source is the set we're excluding rows from, so treat it separately
      if (i == 0 || !emptyFlags.get(i)) {
        newSourcesBuilder.add(source);

        for (Symbol symbol : node.getOutputSymbols()) {
          outputsToInputsBuilder.put(symbol, node.getSymbolMapping().get(symbol).get(i));
        }
      } else {
        hasEmptyBranches = true;
      }
    }

    if (!hasEmptyBranches) {
      return Result.empty();
    }

    List<PlanNode> newSources = newSourcesBuilder.build();
    ListMultimap<Symbol, Symbol> outputsToInputs = outputsToInputsBuilder.build();

    // if the first child of Except node is non-empty set and other children of the Except node are
    // empty set,  replace the Except node with project node, add the aggregation node above the
    // project node if Except that with distinct
    if (newSources.size() == 1) {
      Assignments.Builder assignments = Assignments.builder();
      for (Map.Entry<Symbol, Symbol> entry : outputsToInputs.entries()) {
        assignments.put(entry.getKey(), entry.getValue().toSymbolReference());
      }

      ProjectNode projectNode =
          new ProjectNode(
              context.getIdAllocator().genPlanNodeId(), newSources.get(0), assignments.build());

      if (node.isDistinct()) {
        return Result.ofPlanNode(
            singleAggregation(
                context.getIdAllocator().genPlanNodeId(),
                projectNode,
                ImmutableMap.of(),
                singleGroupingSet(node.getOutputSymbols())));
      }

      return Result.ofPlanNode(projectNode);
    }

    return Result.ofPlanNode(
        new ExceptNode(
            node.getPlanNodeId(),
            newSources,
            outputsToInputs,
            node.getOutputSymbols(),
            node.isDistinct()));
  }
}
