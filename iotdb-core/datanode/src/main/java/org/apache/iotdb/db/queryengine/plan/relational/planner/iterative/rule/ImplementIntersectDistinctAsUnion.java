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

import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntersectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.type.LongType;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.and;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;

public class ImplementIntersectDistinctAsUnion implements Rule<IntersectNode> {

  private static final Pattern<IntersectNode> PATTERN =
      Patterns.intersect().with(Patterns.Intersect.distinct().equalTo(true));

  private final Metadata metadata;

  @Override
  public Pattern<IntersectNode> getPattern() {
    return PATTERN;
  }

  public ImplementIntersectDistinctAsUnion(Metadata metadata) {
    this.metadata = requireNonNull(metadata, "metadata is null");
  }

  @Override
  public Result apply(IntersectNode node, Captures captures, Context context) {

    SetOperationNodeTranslator translator =
        new SetOperationNodeTranslator(
            metadata, context.getSymbolAllocator(), context.getIdAllocator());

    SetOperationNodeTranslator.TranslationResult result =
        translator.makeSetContainmentPlanForDistinct(node);

    // add the filterNode above the aggregation node
    Expression predicate =
        and(
            result.getCountSymbols().stream()
                .map(
                    symbol ->
                        new ComparisonExpression(
                            GREATER_THAN_OR_EQUAL,
                            symbol.toSymbolReference(),
                            new GenericLiteral(LongType.INT64.getDisplayName(), "1")))
                .collect(ImmutableList.toImmutableList()));

    FilterNode filterNode =
        new FilterNode(context.getIdAllocator().genPlanNodeId(), result.getPlanNode(), predicate);

    return Result.ofPlanNode(
        new ProjectNode(
            context.getIdAllocator().genPlanNodeId(),
            filterNode,
            Assignments.identity(node.getOutputSymbols())));
  }
}
