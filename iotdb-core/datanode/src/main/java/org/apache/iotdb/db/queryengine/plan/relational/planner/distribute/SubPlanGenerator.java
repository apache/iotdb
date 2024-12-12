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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.MultiChildrenSinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;

import org.apache.commons.lang3.Validate;

import java.util.HashSet;
import java.util.Set;

/** Split SubPlan according to ExchangeNode. */
public class SubPlanGenerator {

  public SubPlan splitToSubPlan(QueryId queryId, PlanNode rootPlanNode) {
    SubPlan rootSubPlan = createSubPlan(rootPlanNode, queryId);
    Set<PlanNodeId> visitedSinkNode = new HashSet<>();
    splitToSubPlan(rootPlanNode, rootSubPlan, visitedSinkNode, queryId);
    return rootSubPlan;
  }

  private void splitToSubPlan(
      PlanNode root, SubPlan subPlan, Set<PlanNodeId> visitedSinkNode, QueryId queryId) {
    if (root instanceof WritePlanNode) {
      return;
    }
    if (root instanceof ExchangeNode) {
      // We add a FragmentSinkNode for newly created PlanFragment
      ExchangeNode exchangeNode = (ExchangeNode) root;
      Validate.isTrue(
          exchangeNode.getChild() instanceof MultiChildrenSinkNode,
          "child of ExchangeNode must be MultiChildrenSinkNode");
      MultiChildrenSinkNode sinkNode = (MultiChildrenSinkNode) (exchangeNode.getChild());

      // We cut off the subtree to make the ExchangeNode as the leaf node of current PlanFragment
      exchangeNode.cleanChildren();

      // If the SinkNode hasn't visited, build the child SubPlan Tree
      if (!visitedSinkNode.contains(sinkNode.getPlanNodeId())) {
        visitedSinkNode.add(sinkNode.getPlanNodeId());
        SubPlan childSubPlan = createSubPlan(sinkNode, queryId);
        splitToSubPlan(sinkNode, childSubPlan, visitedSinkNode, queryId);
        subPlan.addChild(childSubPlan);
      }
      return;
    }
    for (PlanNode child : root.getChildren()) {
      splitToSubPlan(child, subPlan, visitedSinkNode, queryId);
    }
  }

  private SubPlan createSubPlan(PlanNode root, QueryId queryId) {
    PlanFragment fragment = new PlanFragment(queryId.genPlanFragmentId(), root);
    return new SubPlan(fragment);
  }
}
