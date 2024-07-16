/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;

import com.google.common.collect.Streams;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.Util.restrictChildOutputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.sort;

public class PruneSortColumns extends ProjectOffPushDownRule<SortNode> {
  public PruneSortColumns() {
    super(sort());
  }

  @Override
  protected Optional<PlanNode> pushDownProjectOff(
      Context context, SortNode sortNode, Set<Symbol> referencedOutputs) {
    Set<Symbol> referencedInputs =
        Streams.concat(
                referencedOutputs.stream(), sortNode.getOrderingScheme().getOrderBy().stream())
            .collect(toImmutableSet());

    return restrictChildOutputs(context.getIdAllocator(), sortNode, referencedInputs);
  }
}
