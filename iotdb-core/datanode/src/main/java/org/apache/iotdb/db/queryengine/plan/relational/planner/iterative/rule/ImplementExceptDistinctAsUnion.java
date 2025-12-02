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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExceptNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
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

public class ImplementExceptDistinctAsUnion implements Rule<ExceptNode> {

  private final Metadata metadata;
  private static final Pattern<ExceptNode> PATTERN =
      Patterns.except().with(Patterns.Except.distinct().equalTo(true));

  public ImplementExceptDistinctAsUnion(Metadata metadata) {
    this.metadata = requireNonNull(metadata, "metadata is null");
  }

  @Override
  public Pattern<ExceptNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(ExceptNode node, Captures captures, Context context) {

    SetOperationNodeTranslator translator =
        new SetOperationNodeTranslator(
            metadata, context.getSymbolAllocator(), context.getIdAllocator());

    SetOperationNodeTranslator.TranslationResult result =
        translator.makeSetContainmentPlanForDistinct(node);

    ImmutableList.Builder<Expression> predicatesBuilder = ImmutableList.builder();
    predicatesBuilder.add(
        new ComparisonExpression(
            GREATER_THAN_OR_EQUAL,
            result.getCountSymbols().get(0).toSymbolReference(),
            new GenericLiteral(LongType.INT64.getDisplayName(), "1")));

    for (int i = 1; i < node.getChildren().size(); i++) {
      predicatesBuilder.add(
          new ComparisonExpression(
              ComparisonExpression.Operator.EQUAL,
              result.getCountSymbols().get(i).toSymbolReference(),
              new GenericLiteral(LongType.INT64.getDisplayName(), "0")));
    }

    FilterNode filterNode =
        new FilterNode(
            context.getIdAllocator().genPlanNodeId(),
            result.getPlanNode(),
            and(predicatesBuilder.build()));

    return Result.ofPlanNode(
        new ProjectNode(
            context.getIdAllocator().genPlanNodeId(),
            filterNode,
            Assignments.identity(node.getOutputSymbols())));
  }
}
