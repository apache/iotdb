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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.union;

/**
 * Transforms
 *
 * <pre>
 * - Project (a)
 *      - Union
 *        output mappings: {a->c, a->e, b->d, b->f}
 *          - Source (c, d)
 *          - Source (e, f)
 * </pre>
 *
 * into:
 *
 * <pre>
 * - Project (a)
 *      - Union
 *        output mappings: {a->c, a->e}
 *          - Source (c, d)
 *          - Source (e, f)
 * </pre>
 *
 * Note: as a result of this rule, the UnionNode's sources are eligible for pruning outputs. This is
 * accomplished by PruneUnionSourceColumns rule.
 */
public class PruneUnionColumns extends ProjectOffPushDownRule<UnionNode> {
  public PruneUnionColumns() {
    super(union());
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(
      Context context, UnionNode unionNode, Set<Symbol> referencedOutputs) {
    ImmutableListMultimap<Symbol, Symbol> prunedOutputMappings =
        unionNode.getSymbolMapping().entries().stream()
            .filter(entry -> referencedOutputs.contains(entry.getKey()))
            .collect(toImmutableListMultimap(Map.Entry::getKey, Map.Entry::getValue));

    return Optional.of(
        new UnionNode(
            unionNode.getPlanNodeId(),
            unionNode.getChildren(),
            prunedOutputMappings,
            ImmutableList.copyOf(prunedOutputMappings.keySet())));
  }
}
