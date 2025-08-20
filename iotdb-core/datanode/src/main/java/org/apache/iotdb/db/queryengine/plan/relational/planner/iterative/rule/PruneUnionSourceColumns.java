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

import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.union;

public class PruneUnionSourceColumns implements Rule<UnionNode> {
  @Override
  public Pattern<UnionNode> getPattern() {
    return union();
  }

  @Override
  public Result apply(UnionNode node, Captures captures, Context context) {
    @SuppressWarnings("unchecked")
    Set<Symbol>[] referencedInputs = new Set[node.getChildren().size()];
    for (int i = 0; i < node.getChildren().size(); i++) {
      referencedInputs[i] = ImmutableSet.copyOf(node.sourceOutputLayout(i));
    }
    return restrictChildOutputs(context.getIdAllocator(), node, referencedInputs)
        .map(Rule.Result::ofPlanNode)
        .orElse(Rule.Result.empty());
  }
}
