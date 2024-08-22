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
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule.MergeLimitWithMergeSort.transformByMergeSortNode;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.source;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Capture.newCapture;

/**
 * <b>Optimization phase:</b> Distributed plan planning.
 *
 * <p>Transforms:
 *
 * <pre>
 * - Limit (limit = x)
 *    - Project (identity, narrowing)
 *       - MergeSort (order by a, b)
 * </pre>
 *
 * Into:
 *
 * <pre>
 * - Project (identity, narrowing)
 *    - TopK (limit = x, order by a, b)
 * </pre>
 *
 * Applies to LimitNode without ties only.
 */
public class MergeLimitOverProjectWithMergeSort implements Rule<LimitNode> {
  private static final Capture<ProjectNode> PROJECT = newCapture();
  private static final Capture<MergeSortNode> MERGE_SORT = newCapture();

  private static final Pattern<LimitNode> PATTERN =
      limit()
          // .matching(limit -> !limit.isWithTies())
          .with(
              source()
                  .matching(
                      project()
                          .capturedAs(PROJECT)
                          .matching(ProjectNode::isIdentity)
                          .with(source().matching(mergeSort().capturedAs(MERGE_SORT)))));

  @Override
  public Pattern<LimitNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(LimitNode parent, Captures captures, Context context) {
    ProjectNode project = captures.get(PROJECT);
    MergeSortNode mergeSortNode = captures.get(MERGE_SORT);
    PlanNode childOfMergeSort = context.getLookup().resolve(mergeSortNode.getChildren().get(0));
    TopKNode topKNode = transformByMergeSortNode(parent, mergeSortNode, childOfMergeSort, context);
    return Result.ofPlanNode(project.replaceChildren(ImmutableList.of(topKNode)));
  }
}
