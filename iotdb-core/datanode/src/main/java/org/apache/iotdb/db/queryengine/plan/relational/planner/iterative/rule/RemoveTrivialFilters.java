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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;

public class RemoveTrivialFilters implements Rule<FilterNode> {
  private static final Pattern<FilterNode> PATTERN = filter();

  @Override
  public Pattern<FilterNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(FilterNode filterNode, Captures captures, Context context) {
    Expression predicate = filterNode.getPredicate();
    checkArgument(
        !(predicate instanceof NullLiteral), "Unexpected null literal without a cast to boolean");

    if (predicate.equals(TRUE_LITERAL)) {
      return Result.ofPlanNode(filterNode.getChild());
    }

    // TODO add it back after we support ValuesNode
    //    if (predicate.equals(FALSE_LITERAL) ||
    //        (predicate instanceof Cast && ((Cast) predicate).getExpression() instanceof
    // NullLiteral)) {
    //      return Result.ofPlanNode(
    //          new ValuesNode(context.getIdAllocator().genPlanNodeId(),
    // filterNode.getOutputSymbols(), emptyList()));
    //    }

    return Result.empty();
  }
}
