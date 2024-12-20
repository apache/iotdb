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
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Lookup;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.INNER;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode.JoinType.RIGHT;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.CorrelatedJoin.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.correlatedJoin;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.isAtLeastScalar;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.isScalar;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;

public class RemoveUnreferencedScalarSubqueries implements Rule<CorrelatedJoinNode> {
  private static final Pattern<CorrelatedJoinNode> PATTERN =
      correlatedJoin().with(filter().equalTo(TRUE_LITERAL));

  @Override
  public Pattern<CorrelatedJoinNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context) {
    PlanNode input = correlatedJoinNode.getInput();
    PlanNode subquery = correlatedJoinNode.getSubquery();

    if (isUnreferencedScalar(input, context.getLookup())
        && correlatedJoinNode.getCorrelation().isEmpty()) {
      if (correlatedJoinNode.getJoinType() == INNER
          || correlatedJoinNode.getJoinType() == RIGHT
          || isAtLeastScalar(subquery, context.getLookup())) {
        return Result.ofPlanNode(subquery);
      }
    }

    if (isUnreferencedScalar(subquery, context.getLookup())) {
      return Result.ofPlanNode(input);
    }

    return Result.empty();
  }

  private boolean isUnreferencedScalar(PlanNode planNode, Lookup lookup) {
    return planNode.getOutputSymbols().isEmpty() && isScalar(planNode, lookup);
  }
}
