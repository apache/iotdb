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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import org.apache.tsfile.read.common.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.ExpressionSymbolInliner.inlineSymbols;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.union;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

public class PushProjectionThroughUnion implements Rule<ProjectNode> {
  private static final Capture<UnionNode> CHILD = newCapture();

  private static final Pattern<ProjectNode> PATTERN =
      project()
          .matching(PushProjectionThroughUnion::nonTrivialProjection)
          .with(source().matching(union().capturedAs(CHILD)));

  @Override
  public Pattern<ProjectNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(ProjectNode parent, Captures captures, Context context) {
    UnionNode child = captures.get(CHILD);

    // OutputLayout of the resultant Union, will be same as the layout of the Project
    List<Symbol> outputLayout = parent.getOutputSymbols();

    // Mapping from the output symbol to ordered list of symbols from each of the children
    ImmutableListMultimap.Builder<Symbol, Symbol> mappings = ImmutableListMultimap.builder();

    // sources for the resultant UnionNode
    ImmutableList.Builder<PlanNode> outputSources = ImmutableList.builder();

    for (int i = 0; i < child.getChildren().size(); i++) {
      Map<Symbol, SymbolReference> outputToInput =
          child.sourceSymbolMap(i); // Map: output of union -> input of this child to the union
      Assignments.Builder assignments =
          Assignments.builder(); // assignments for the new ProjectNode

      // mapping from current ProjectNode to new ProjectNode, used to identify the output layout
      Map<Symbol, Symbol> projectSymbolMapping = new HashMap<>();

      // Translate the assignments in the ProjectNode using symbols of the source of the UnionNode
      for (Map.Entry<Symbol, Expression> entry : parent.getAssignments().entrySet()) {
        Expression translatedExpression = inlineSymbols(outputToInput, entry.getValue());
        Type type = context.getSymbolAllocator().getTypes().getTableModelType(entry.getKey());
        Symbol symbol = context.getSymbolAllocator().newSymbol(translatedExpression, type);
        assignments.put(symbol, translatedExpression);
        projectSymbolMapping.put(entry.getKey(), symbol);
      }
      outputSources.add(
          new ProjectNode(
              context.getIdAllocator().genPlanNodeId(),
              child.getChildren().get(i),
              assignments.build()));
      outputLayout.forEach(symbol -> mappings.put(symbol, projectSymbolMapping.get(symbol)));
    }

    return Result.ofPlanNode(
        new UnionNode(
            parent.getPlanNodeId(),
            outputSources.build(),
            mappings.build(),
            ImmutableList.copyOf(mappings.build().keySet())));
  }

  private static boolean nonTrivialProjection(ProjectNode project) {
    return !project.getAssignments().getExpressions().stream()
        .allMatch(SymbolReference.class::isInstance);
  }
}
