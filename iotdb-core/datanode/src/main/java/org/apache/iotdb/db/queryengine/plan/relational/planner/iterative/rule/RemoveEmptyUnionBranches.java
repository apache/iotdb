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
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;

import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.union;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.isEmpty;

/**
 * Removes branches from a UnionNode that are guaranteed to produce 0 rows.
 *
 * <p>If there's only one branch left, it replaces the UnionNode with a projection to preserve the
 * outputs of the union.
 *
 * <p>If all branches are empty, now we do the same process with one branch left case.
 */
public class RemoveEmptyUnionBranches implements Rule<UnionNode> {
  private static final Pattern<UnionNode> PATTERN = union();

  @Override
  public Pattern<UnionNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(UnionNode node, Captures captures, Context context) {
    int emptyBranches = 0;
    ImmutableList.Builder<PlanNode> newChildrenBuilder = ImmutableList.builder();
    ImmutableListMultimap.Builder<Symbol, Symbol> outputsToInputsBuilder =
        ImmutableListMultimap.builder();
    for (int i = 0; i < node.getChildren().size(); i++) {
      PlanNode child = node.getChildren().get(i);
      if (!isEmpty(child, context.getLookup())) {
        newChildrenBuilder.add(child);

        for (Symbol column : node.getOutputSymbols()) {
          outputsToInputsBuilder.put(column, node.getSymbolMapping().get(column).get(i));
        }
      } else {
        emptyBranches++;
      }
    }

    if (emptyBranches == 0) {
      return Result.empty();
    }

    // restore after ValuesNode is introduced
    /*
        if (emptyBranches == node.getChildren().size()) {
        return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
    }*/

    // Now we do the same process with one branch left case, choose the first child as preserved.
    if (emptyBranches == node.getChildren().size()) {
      Assignments.Builder assignments = Assignments.builder();
      for (Symbol column : node.getOutputSymbols()) {
        assignments.put(column, node.getSymbolMapping().get(column).get(0).toSymbolReference());
      }

      return Result.ofPlanNode(
          new ProjectNode(node.getPlanNodeId(), node.getChildren().get(0), assignments.build()));
    }

    List<PlanNode> newChildren = newChildrenBuilder.build();
    ListMultimap<Symbol, Symbol> outputsToInputs = outputsToInputsBuilder.build();

    if (newChildren.size() == 1) {
      Assignments.Builder assignments = Assignments.builder();

      outputsToInputs
          .entries()
          .forEach(entry -> assignments.put(entry.getKey(), entry.getValue().toSymbolReference()));

      return Result.ofPlanNode(
          new ProjectNode(node.getPlanNodeId(), newChildren.get(0), assignments.build()));
    }

    return Result.ofPlanNode(
        new UnionNode(node.getPlanNodeId(), newChildren, outputsToInputs, node.getOutputSymbols()));
  }
}
