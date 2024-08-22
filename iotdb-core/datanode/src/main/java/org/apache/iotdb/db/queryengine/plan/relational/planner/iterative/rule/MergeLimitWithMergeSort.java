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
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * <b>Optimization phase:</b> Distributed plan planning.
 *
 * <p>Transforms:
 *
 * <pre>
 * - Limit (limit = x)
 *       - MergeSort (order by a, b)
 * </pre>
 *
 * Into:
 *
 * <pre>
 * - TopK (limit = x, order by a, b)
 * </pre>
 *
 * <pre>
 * - Limit (limit = x)
 *       - MergeSort (order by a, b)
 *          - StreamSort (order by a, b)
 * </pre>
 *
 * Into:
 *
 * <pre>
 * - TopK (limit = x, order by a, b)
 *    - Limit (limit = x)
 *      - StreamSort (order by a, b)
 * </pre>
 *
 * Applies to LimitNode without ties only.
 */
public class MergeLimitWithMergeSort implements Rule<LimitNode> {
  private static final Capture<MergeSortNode> CHILD = newCapture();

  private static final Pattern<LimitNode> PATTERN =
      limit()
          // .matching(limit -> !limit.isWithTies())
          .with(source().matching(mergeSort().capturedAs(CHILD)));

  @Override
  public Pattern<LimitNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(LimitNode parent, Captures captures, Context context) {
    MergeSortNode mergeSortNode = captures.get(CHILD);
    PlanNode childOfMergeSort = context.getLookup().resolve(mergeSortNode.getChildren().get(0));
    TopKNode topKNode = transformByMergeSortNode(parent, mergeSortNode, childOfMergeSort, context);
    return Result.ofPlanNode(topKNode);
  }

  static TopKNode transformByMergeSortNode(
      LimitNode parent, MergeSortNode mergeSortNode, PlanNode childOfMergeSort, Context context) {
    TopKNode topKNode;
    if (childOfMergeSort instanceof StreamSortNode) {
      topKNode =
          new TopKNode(
              parent.getPlanNodeId(),
              mergeSortNode.getOrderingScheme(),
              parent.getCount(),
              childOfMergeSort.getOutputSymbols(),
              true);
      for (PlanNode child : mergeSortNode.getChildren()) {
        LimitNode limitNode =
            new LimitNode(
                context.getIdAllocator().genPlanNodeId(),
                child,
                parent.getCount(),
                Optional.empty());
        topKNode.addChild(limitNode);
      }

    } else {
      topKNode =
          new TopKNode(
              parent.getPlanNodeId(),
              mergeSortNode.getChildren(),
              mergeSortNode.getOrderingScheme(),
              parent.getCount(),
              childOfMergeSort.getOutputSymbols(),
              true);
    }

    return topKNode;
  }
}
