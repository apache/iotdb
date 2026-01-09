/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.hint.Hint;
import org.apache.iotdb.db.queryengine.plan.relational.utils.hint.JoinOrderHint;
import org.apache.iotdb.db.queryengine.plan.relational.utils.hint.LeadingHint;

public class CollectJoinConstraint implements PlanOptimizer {
  @Override
  public PlanNode optimize(PlanNode plan, Context context) {
    if (!context.getAnalysis().isQuery()) {
      return plan;
    }
    return plan.accept(new Rewriter(context.getAnalysis()), null);
  }

  private static class Rewriter extends PlanVisitor<PlanNode, Context> {
    private final Analysis analysis;

    public Rewriter(Analysis analysis) {
      this.analysis = analysis;
    }

    @Override
    public PlanNode visitPlan(PlanNode node, Context context) {
      Hint hint = analysis.getHintMap().getOrDefault(JoinOrderHint.category, null);
      if (!(hint instanceof LeadingHint)) {
        return node;
      }

      for (PlanNode child : node.getChildren()) {
        child.accept(this, context);
      }
      return node;
    }

    @Override
    public PlanNode visitJoin(JoinNode node, Context context) {
      PlanNode leftNode = node.getLeftChild();
      PlanNode rightNode = node.getRightChild();
      return node;
    }

    //    @Override
    //    public PlanNode visitSemiJoin(SemiJoinNode node, Context context) {
    //      return visitTwoChildProcess(node, context);
    //    }
  }
}
