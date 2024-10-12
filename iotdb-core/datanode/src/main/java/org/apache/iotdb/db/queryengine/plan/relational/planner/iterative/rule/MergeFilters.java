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

import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.combineConjuncts;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

public class MergeFilters implements Rule<FilterNode> {
  private static final Capture<FilterNode> CHILD = newCapture();

  private static final Pattern<FilterNode> PATTERN =
      filter().with(source().matching(filter().capturedAs(CHILD)));

  @Override
  public Pattern<FilterNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(FilterNode parent, Captures captures, Context context) {
    FilterNode child = captures.get(CHILD);

    return Result.ofPlanNode(
        new FilterNode(
            parent.getPlanNodeId(),
            child.getChild(),
            combineConjuncts(child.getPredicate(), parent.getPredicate())));
  }
}
