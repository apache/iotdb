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

import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ApplyNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.Cardinality;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.applyNode;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.FALSE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;

/**
 * Given:
 *
 * <pre>
 * - Apply [X.*, e = EXISTS (true)]
 *   - X
 *   - S with cardinality >= 1
 * </pre>
 *
 * <p>Produces:
 *
 * <pre>
 * - Project [X.*, e = true]
 *   - X
 * </pre>
 *
 * <p>Given:
 *
 * <pre>
 * - Apply [X.*, e = EXISTS (true)]
 *   - X
 *   - S with cardinality = 0
 * </pre>
 *
 * <p>Produces:
 *
 * <pre>
 * - Project [X.*, e = false]
 *   - X
 * </pre>
 */
public class RemoveRedundantExists implements Rule<ApplyNode> {
  private static final Pattern<ApplyNode> PATTERN =
      applyNode()
          .matching(
              node ->
                  node.getSubqueryAssignments().values().stream()
                      .allMatch(ApplyNode.Exists.class::isInstance));

  @Override
  public Pattern<ApplyNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(ApplyNode node, Captures captures, Context context) {
    Assignments.Builder assignments = Assignments.builder();
    assignments.putIdentities(node.getInput().getOutputSymbols());

    Cardinality subqueryCardinality = extractCardinality(node.getSubquery(), context.getLookup());
    Expression result;
    if (subqueryCardinality.isEmpty()) {
      result = FALSE_LITERAL;
    } else if (subqueryCardinality.isAtLeastScalar()) {
      result = TRUE_LITERAL;
    } else {
      return Result.empty();
    }

    for (Symbol output : node.getSubqueryAssignments().keySet()) {
      assignments.put(output, result);
    }

    return Result.ofPlanNode(
        new ProjectNode(
            context.getIdAllocator().genPlanNodeId(), node.getInput(), assignments.build()));
  }
}
