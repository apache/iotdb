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

import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ApplyNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.Apply.correlation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.applyNode;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern.empty;

/**
 * This optimizers looks for InPredicate expressions in ApplyNodes and replaces the nodes with
 * SemiJoin nodes.
 *
 * <p>Plan before optimizer:
 *
 * <pre>
 * Filter(a IN b):
 *   Apply
 *     - correlation: []  // empty
 *     - input: some plan A producing symbol a
 *     - subquery: some plan B producing symbol b
 * </pre>
 *
 * <p>Plan after optimizer:
 *
 * <pre>
 * Filter(semijoinresult):
 *   SemiJoin
 *     - source: plan A
 *     - filteringSource: B
 *     - sourceJoinSymbol: symbol a
 *     - filteringSourceJoinSymbol: symbol b
 *     - semiJoinOutput: semijoinresult
 * </pre>
 */
public class TransformUncorrelatedInPredicateSubqueryToSemiJoin implements Rule<ApplyNode> {
  private static final Pattern<ApplyNode> PATTERN = applyNode().with(empty(correlation()));

  @Override
  public Pattern<ApplyNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(ApplyNode applyNode, Captures captures, Context context) {
    if (applyNode.getSubqueryAssignments().size() != 1) {
      return Result.empty();
    }

    ApplyNode.SetExpression expression =
        getOnlyElement(applyNode.getSubqueryAssignments().values());
    if (!(expression instanceof ApplyNode.In)) {
      return Result.empty();
    }

    ApplyNode.In inPredicate = (ApplyNode.In) expression;

    Symbol semiJoinSymbol = getOnlyElement(applyNode.getSubqueryAssignments().keySet());

    SemiJoinNode replacement =
        new SemiJoinNode(
            context.getIdAllocator().genPlanNodeId(),
            applyNode.getInput(),
            applyNode.getSubquery(),
            inPredicate.getValue(),
            inPredicate.getReference(),
            semiJoinSymbol);

    return Result.ofPlanNode(replacement);
  }
}
