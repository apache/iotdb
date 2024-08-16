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

import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.join;

public class SortMergeJoin implements Rule<JoinNode> {
  private static final Pattern<JoinNode> PATTERN = join();

  @Override
  public Pattern<JoinNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(JoinNode node, Captures captures, Context context) {
    List<JoinNode.EquiJoinClause> criteria = node.getCriteria();
    List<Symbol> orderBy = new ArrayList<>();
    Map<Symbol, SortOrder> orderings = new HashMap<>();
    // OrderingScheme scheme = new OrderingScheme();

    JoinNode newJoinNode = (JoinNode) node.clone();
    SortNode leftSortNode =
        new SortNode(
            context.getIdAllocator().genPlanNodeId(), node.getLeftChild(), null, false, false);
    SortNode rightSortNode =
        new SortNode(
            context.getIdAllocator().genPlanNodeId(), node.getLeftChild(), null, false, false);
    newJoinNode.setLeftChild(leftSortNode);
    newJoinNode.setRightChild(rightSortNode);
    return Result.ofPlanNode(node);
  }
}
