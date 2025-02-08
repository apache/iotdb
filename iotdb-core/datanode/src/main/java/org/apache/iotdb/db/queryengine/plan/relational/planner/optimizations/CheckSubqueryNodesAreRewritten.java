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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ApplyNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CorrelatedJoinNode;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.PlanNodeSearcher.searchFrom;
import static org.apache.iotdb.rpc.TSStatusCode.SEMANTIC_ERROR;

public class CheckSubqueryNodesAreRewritten implements PlanOptimizer {
  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    searchFrom(plan)
        .where(ApplyNode.class::isInstance)
        .findFirst()
        .ifPresent(
            node -> {
              ApplyNode applyNode = (ApplyNode) node;
              throw error(applyNode.getCorrelation());
            });

    searchFrom(plan)
        .where(CorrelatedJoinNode.class::isInstance)
        .findFirst()
        .ifPresent(
            node -> {
              CorrelatedJoinNode correlatedJoinNode = (CorrelatedJoinNode) node;
              throw error(correlatedJoinNode.getCorrelation());
            });

    return plan;
  }

  private SemanticException error(List<Symbol> correlation) {
    checkState(
        !correlation.isEmpty(),
        "All the non correlated subqueries should be rewritten at this point");
    throw new SemanticException(
        "Given correlated subquery is not supported", SEMANTIC_ERROR.getStatusCode());
  }
}
