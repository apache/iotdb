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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import static java.lang.Math.addExact;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.offset;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * Transforms:
 *
 * <pre>
 * - Limit (row count x)
 *    - Offset (row count y)
 * </pre>
 *
 * Into:
 *
 * <pre>
 * - Offset (row count y)
 *    - Limit (row count x+y)
 * </pre>
 *
 * Applies to both limit with ties and limit without ties.
 */
public class PushLimitThroughOffset implements Rule<LimitNode> {
  private static final Capture<OffsetNode> CHILD = newCapture();

  private static final Pattern<LimitNode> PATTERN =
      limit().with(source().matching(offset().capturedAs(CHILD)));

  @Override
  public Pattern<LimitNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(LimitNode parent, Captures captures, Context context) {
    OffsetNode child = captures.get(CHILD);

    long count;
    try {
      count = addExact(parent.getCount(), child.getCount());
    } catch (ArithmeticException e) {
      return Result.empty();
    }

    return Result.ofPlanNode(
        child.replaceChildren(
            ImmutableList.of(
                new LimitNode(
                    parent.getPlanNodeId(),
                    child.getChild(),
                    count,
                    parent.getTiesResolvingScheme()))));
  }
}
