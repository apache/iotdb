/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.Limit.requiresPreSortedInputs;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.union;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * Transforms:
 *
 * <pre>
 * - Limit
 *    - Union
 *       - relation1
 *       - relation2
 *       ..
 * </pre>
 *
 * Into:
 *
 * <pre>
 * - Limit
 *    - Union
 *       - Limit
 *          - relation1
 *       - Limit
 *          - relation2
 *       ..
 * </pre>
 *
 * Applies to LimitNode without ties only to avoid optimizer loop.
 */
public class PushLimitThroughUnion implements Rule<LimitNode> {
  private static final Capture<UnionNode> CHILD = newCapture();

  private static final Pattern<LimitNode> PATTERN =
      limit()
          .matching(limit -> !limit.isWithTies())
          .with(requiresPreSortedInputs().equalTo(false))
          .with(source().matching(union().capturedAs(CHILD)));

  @Override
  public Pattern<LimitNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(LimitNode parent, Captures captures, Context context) {
    UnionNode unionNode = captures.get(CHILD);
    ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
    boolean shouldApply = false;
    for (PlanNode child : unionNode.getChildren()) {
      // This check is to ensure that we don't fire the optimizer if it was previously applied.
      if (isAtMost(child, context.getLookup(), parent.getCount())) {
        builder.add(child);
      } else {
        shouldApply = true;
        builder.add(
            new LimitNode(
                context.getIdAllocator().genPlanNodeId(),
                child,
                parent.getCount(),
                Optional.empty()));
      }
    }

    if (!shouldApply) {
      return Result.empty();
    }

    return Result.ofPlanNode(
        parent.replaceChildren(ImmutableList.of(unionNode.replaceChildren(builder.build()))));
  }
}
