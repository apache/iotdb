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
import org.apache.iotdb.db.queryengine.plan.relational.utils.hint.JoinConstraint;
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

      Set<Identifier> leftHand = node.getLeftTables();
      Set<Identifier> rightHand = node.getRightTables();
      Set<Identifier> totalJoinTables = ImmutableSet.of();

      // join conjunctions
      List<JoinNode.EquiJoinClause> criteria = node.getCriteria();
      for (JoinNode.EquiJoinClause equiJoin : criteria) {
        Set<Identifier> equiJoinTables = Sets.intersection(leadingTables, equiJoin.getTables());
        totalJoinTables = Sets.union(totalJoinTables, equiJoinTables);
        if (node.getJoinType() == JoinNode.JoinType.LEFT) {
          // leading.getFilters() is used to determine which join the join condition should apply
          // to.
          // For LEFT join, we need to add rightHand tables as well. In the getJoinConditions
          // function,
          // we ensure that the tables joined by the Join include both the tables in the join
          // condition
          // and the right tables of the outer join, so that the join condition can be applied.
          equiJoinTables = Sets.union(equiJoinTables, rightHand);
        }
        leading.getFilters().add(new Pair<>(equiJoinTables, equiJoin.toExpression()));
        leading.putConditionJoinType(equiJoin.toExpression(), node.getJoinType());
      }
      // join constraint
      collectJoinConstraintList(leading, leftHand, rightHand, node, totalJoinTables);

      return node;
    }

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
        JoinConstraint joinConstraint =
            new JoinConstraint(leftHand, rightHand, leftHand, rightHand, JoinNode.JoinType.FULL);
        leading.getJoinConstraintList().add(joinConstraint);
        return;
      }
      Set<Identifier> minLeftHand = Sets.intersection(joinTables, leftHand);
      Set<Identifier> innerJoinTables =
          Sets.intersection(totalTables, leading.getInnerJoinTables());
      Set<Identifier> filterAndInnerBelow = Sets.union(joinTables, innerJoinTables);
      Set<Identifier> minRightHand = Sets.intersection(filterAndInnerBelow, rightHand);

      for (JoinConstraint other : leading.getJoinConstraintList()) {
        if (other.getJoinType() == JoinNode.JoinType.FULL) {
          if (isOverlap(leftHand, other.getLeftHand())
              || isOverlap(leftHand, other.getRightHand())) {
            minLeftHand = Sets.union(minLeftHand, other.getLeftHand());
            minLeftHand = Sets.union(minLeftHand, other.getRightHand());
          }

          if (isOverlap(rightHand, other.getLeftHand())
              || isOverlap(rightHand, other.getRightHand())) {
            minRightHand = Sets.union(minRightHand, other.getLeftHand());
            minRightHand = Sets.union(minRightHand, other.getRightHand());
          }
          /* Needn't do anything else with the full join */
          continue;
        }

        // Current join's left table contains the right table of a previous join & join condition
        // contains the right table of a previous join
        if (isOverlap(leftHand, other.getRightHand())
            && isOverlap(joinTables, other.getRightHand())) {
          minLeftHand = Sets.union(minLeftHand, other.getLeftHand());
          minLeftHand = Sets.union(minLeftHand, other.getRightHand());
        }

        // Current join's right table contains the right table of a previous join
        if (isOverlap(rightHand, other.getRightHand())) {
          if (isOverlap(joinTables, other.getRightHand())
              || !isOverlap(joinTables, other.getMinLeftHand())) {
            minRightHand = Sets.union(minRightHand, other.getLeftHand());
            minRightHand = Sets.union(minRightHand, other.getRightHand());
          }
        }
      }
      if (minLeftHand.isEmpty()) {
        minLeftHand = leftHand;
      }
      if (minRightHand.isEmpty()) {
        minRightHand = rightHand;
      }

      JoinConstraint joinConstraint =
          new JoinConstraint(
              minLeftHand, minRightHand, minLeftHand, minRightHand, join.getJoinType());
      leading.getJoinConstraintList().add(joinConstraint);
    }

    private boolean isOverlap(Set<Identifier> set1, Set<Identifier> set2) {
      if (set1 == null || set2 == null || set1.isEmpty() || set2.isEmpty()) {
        return false;
      }
      return !Sets.intersection(set1, set2).isEmpty();
    }
  }
}
