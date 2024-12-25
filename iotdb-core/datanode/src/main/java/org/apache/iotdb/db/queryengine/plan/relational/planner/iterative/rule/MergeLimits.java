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

import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * This rule handles both LimitNode with ties and LimitNode without ties. The parent LimitNode is
 * without ties.
 *
 * <p>If the child LimitNode is without ties, both nodes are merged into a single LimitNode with row
 * count being the minimum of their row counts:
 *
 * <pre>
 *    - Limit (3)
 *       - Limit (5)
 * </pre>
 *
 * is transformed into:
 *
 * <pre>
 *     - Limit (3)
 * </pre>
 *
 * <p>If the child LimitNode is with ties, the rule's behavior depends on both nodes' row count. If
 * parent row count is lower or equal to child row count, child node is removed from the plan:
 */
public class MergeLimits implements Rule<LimitNode> {
  private static final Capture<LimitNode> CHILD = newCapture();

  private static final Pattern<LimitNode> PATTERN =
      limit()
          // .matching(limit -> !limit.isWithTies())
          .with(source().matching(limit().capturedAs(CHILD)));

  @Override
  public Pattern<LimitNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(LimitNode parent, Captures captures, Context context) {
    LimitNode child = captures.get(CHILD);

    return Result.ofPlanNode(
        new LimitNode(
            parent.getPlanNodeId(),
            child.getChild(),
            Math.min(parent.getCount(), child.getCount()),
            parent.getTiesResolvingScheme()));
  }
}
