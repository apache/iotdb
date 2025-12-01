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
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntersectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.intersect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.isEmpty;

public class EvaluateEmptyIntersect implements Rule<IntersectNode> {

  private static final Pattern<IntersectNode> PATTERN = intersect();

  @Override
  public Pattern<IntersectNode> getPattern() {
    return PATTERN;
  }

  /** if any child of the intersect node is empty set, then the result set is empty */
  @Override
  public Result apply(IntersectNode node, Captures captures, Context context) {

    Lookup lookup = context.getLookup();
    for (int i = 0; i < node.getChildren().size(); i++) {
      if (isEmpty(node.getChildren().get(i), lookup)) {

        // replace the intersect node with project node, append the empty node to the project node
        Assignments.Builder assignments = Assignments.builder();
        for (Symbol symbol : node.getOutputSymbols()) {
          assignments.put(symbol, node.getSymbolMapping().get(symbol).get(i).toSymbolReference());
        }
        return Result.ofPlanNode(
            new ProjectNode(node.getPlanNodeId(), node.getChildren().get(i), assignments.build()));
      }
    }

    return Result.empty();
  }
}
