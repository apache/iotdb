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

import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * <b>Optimization phase:</b> Logical plan planning.
 *
 * <p>Transforms:
 *
 * <pre>
 * - Limit (limit = x)
 *    - Project (identity, narrowing)
 *       - Sort (order by a, b)
 * </pre>
 *
 * Into:
 *
 * <pre>
 * - Project (identity, narrowing)
 *    - TopN (limit = x, order by a, b)
 * </pre>
 *
 * Applies to LimitNode without ties only.
 */
public class MergeLimitOverProjectWithSort implements Rule<LimitNode> {
  private static final Capture<ProjectNode> PROJECT = newCapture();
  private static final Capture<SortNode> SORT = newCapture();

  private static final Pattern<LimitNode> PATTERN =
      limit()
          // .matching(limit -> !limit.isWithTies())
          .with(
              source()
                  .matching(
                      project()
                          .capturedAs(PROJECT)
                          .matching(ProjectNode::isIdentity)
                          .with(source().matching(sort().capturedAs(SORT)))));

  @Override
  public Pattern<LimitNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(LimitNode parent, Captures captures, Context context) {
    ProjectNode project = captures.get(PROJECT);
    SortNode sortNode = captures.get(SORT);

    if (sortNode instanceof StreamSortNode) {
      return Result.empty();
    }

    return Result.ofPlanNode(
        project.replaceChildren(
            ImmutableList.of(
                new TopKNode(
                    parent.getPlanNodeId(),
                    sortNode.getChildren(),
                    sortNode.getOrderingScheme(),
                    parent.getCount(),
                    sortNode.getOutputSymbols(),
                    sortNode.isOrderByAllIdsAndTime()))));
  }
}
