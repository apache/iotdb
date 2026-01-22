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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.utils.hint.Hint;
import org.apache.iotdb.db.queryengine.plan.relational.utils.hint.JoinOrderHint;
import org.apache.iotdb.db.queryengine.plan.relational.utils.hint.LeadingHint;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.tsfile.utils.Pair;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    public PlanNode visitDeviceTableScan(DeviceTableScanNode node, Context context) {
      String tableName = node.getQualifiedObjectName().getObjectName();
      LeadingHint leading = (LeadingHint) analysis.getHintMap().get(JoinOrderHint.category);
      leading.getRelationToScanMap().put(tableName, node);
      return node;
    }

    @Override
    public PlanNode visitJoin(JoinNode node, Context context) {
      for (PlanNode child : node.getChildren()) {
        child.accept(this, context);
      }

      LeadingHint leading = (LeadingHint) analysis.getHintMap().get(JoinOrderHint.category);
      Set<Identifier> leadingTables =
          leading.getTables().stream().map(Identifier::new).collect(Collectors.toSet());

      Set<Identifier> leftTables = node.getLeftTables();
      Set<Identifier> rightTables = node.getRightTables();
      Set<Identifier> totalJoinTables = ImmutableSet.of();

      // join conjunctions
      List<JoinNode.EquiJoinClause> criteria = node.getCriteria();
      for (JoinNode.EquiJoinClause equiJoin : criteria) {
        Set<Identifier> equiJoinTables = Sets.intersection(leadingTables, equiJoin.getTables());
        totalJoinTables = Sets.union(totalJoinTables, equiJoinTables);
        if (node.getJoinType() == JoinNode.JoinType.LEFT) {
          // do something
        }
        leading.getFilters().add(new Pair<>(equiJoinTables, equiJoin.toExpression()));
        // leading.putConditionJoinType(expression, join.getJoinType());
      }
      collectJoinConstraintList(leading, leftTables, rightTables, node, totalJoinTables);

      return node;
    }

    //    @Override
    //    public PlanNode visitSemiJoin(SemiJoinNode node, Context context) {
    //      return visitTwoChildProcess(node, context);
    //    }

    private void collectJoinConstraintList(
        LeadingHint leading,
        Set<Identifier> leftHand,
        Set<Identifier> rightHand,
        JoinNode join,
        Set<Identifier> joinTables) {
      Set<Identifier> totalTables = Sets.union(leftHand, rightHand);
      if (join.getJoinType() == JoinNode.JoinType.INNER) {
        leading.setInnerJoinTables(Sets.union(leading.getInnerJoinTables(), totalTables));
        return;
      }
      if (join.getJoinType() == JoinNode.JoinType.FULL) {
        // full join
        return;
      }
    }
  }
}
