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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.NextFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PreviousFillNode;

import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.fill;

public class PruneFillColumns extends ProjectOffPushDownRule<FillNode> {

  public PruneFillColumns() {
    super(fill());
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(
      Context context, FillNode fillNode, Set<Symbol> referencedOutputs) {
    // Like PruneGapFillColumns: TIME_COLUMN / helper and FILL_GROUP symbols must remain in the
    // child's output even if no outer consumer references them (e.g. nested subquery pruning).
    ImmutableSet.Builder<Symbol> referencedInputs = ImmutableSet.builder();
    referencedInputs.addAll(referencedOutputs);
    if (fillNode instanceof PreviousFillNode) {
      PreviousFillNode previousFillNode = (PreviousFillNode) fillNode;
      previousFillNode.getHelperColumn().ifPresent(referencedInputs::add);
      previousFillNode.getGroupingKeys().ifPresent(keys -> referencedInputs.addAll(keys));
    } else if (fillNode instanceof NextFillNode) {
      NextFillNode nextFillNode = (NextFillNode) fillNode;
      nextFillNode.getHelperColumn().ifPresent(referencedInputs::add);
      nextFillNode.getGroupingKeys().ifPresent(keys -> referencedInputs.addAll(keys));
    } else if (fillNode instanceof LinearFillNode) {
      LinearFillNode linearFillNode = (LinearFillNode) fillNode;
      referencedInputs.add(linearFillNode.getHelperColumn());
      linearFillNode.getGroupingKeys().ifPresent(keys -> referencedInputs.addAll(keys));
    }
    return restrictChildOutputs(context.getIdAllocator(), fillNode, referencedInputs.build());
  }
}
