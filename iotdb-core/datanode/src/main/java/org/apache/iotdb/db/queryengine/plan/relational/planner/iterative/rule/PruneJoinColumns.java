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
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;

import java.util.Optional;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.join;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.MoreLists.filteredCopy;

/** Joins support output symbol selection, so absorb any project-off into the node. */
public class PruneJoinColumns extends ProjectOffPushDownRule<JoinNode> {
  public PruneJoinColumns() {
    super(join());
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(
      Context context, JoinNode joinNode, Set<Symbol> referencedOutputs) {
    return Optional.of(
        new JoinNode(
            joinNode.getPlanNodeId(),
            joinNode.getJoinType(),
            joinNode.getLeftChild(),
            joinNode.getRightChild(),
            joinNode.getCriteria(),
            filteredCopy(joinNode.getLeftOutputSymbols(), referencedOutputs::contains),
            filteredCopy(joinNode.getRightOutputSymbols(), referencedOutputs::contains),
            joinNode.getFilter(),
            joinNode.isSpillable()));
  }
}
