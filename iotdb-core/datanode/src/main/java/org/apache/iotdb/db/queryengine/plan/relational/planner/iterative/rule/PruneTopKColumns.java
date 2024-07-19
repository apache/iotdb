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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;

import com.google.common.collect.Streams;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.topK;

public class PruneTopKColumns extends ProjectOffPushDownRule<TopKNode> {
  public PruneTopKColumns() {
    super(topK());
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(
      Context context, TopKNode topKNode, Set<Symbol> referencedOutputs) {
    Set<Symbol> prunedTopNInputs =
        Streams.concat(
                referencedOutputs.stream(), topKNode.getOrderingScheme().getOrderBy().stream())
            .collect(toImmutableSet());

    return restrictChildOutputs(context.getIdAllocator(), topKNode, prunedTopNInputs);
  }
}
