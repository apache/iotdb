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

import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntersectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.Intersect.distinct;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.intersect;

public class ImplementIntersectAll implements Rule<IntersectNode> {

  private static final Pattern<IntersectNode> PATTERN = intersect().with(distinct().equalTo(false));

  private final Metadata metadata;

  public ImplementIntersectAll(Metadata metadata) {
    this.metadata = requireNonNull(metadata, "metadata is null");
  }

  @Override
  public Pattern<IntersectNode> getPattern() {
    return PATTERN;
  }

  @Override
  public Result apply(IntersectNode node, Captures captures, Context context) {

    SetOperationNodeTranslator translator =
        new SetOperationNodeTranslator(
            metadata, context.getSymbolAllocator(), context.getIdAllocator());

    // 1. translate the intersect(all) node to other planNodes
    SetOperationNodeTranslator.TranslationResult translationResult =
        translator.makeSetContainmentPlanForAll(node);

    // 2. add the filter node above the result node from translation process
    // filter condition : row_number <= least(countA, countB...)
    Expression minCount = translationResult.getCountSymbols().get(0).toSymbolReference();
    for (int i = 1; i < translationResult.getCountSymbols().size(); i++) {
      minCount =
          new FunctionCall(
              QualifiedName.of(TableBuiltinScalarFunction.LEAST.getFunctionName()),
              ImmutableList.of(
                  minCount, translationResult.getCountSymbols().get(i).toSymbolReference()));
    }

    FilterNode filterNode =
        new FilterNode(
            context.getIdAllocator().genPlanNodeId(),
            translationResult.getPlanNode(),
            new ComparisonExpression(
                ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
                translationResult.getRowNumberSymbol().toSymbolReference(),
                minCount));

    // 3. add the project node to remove the redundant columns
    return Result.ofPlanNode(
        new ProjectNode(
            context.getIdAllocator().genPlanNodeId(),
            filterNode,
            Assignments.identity(node.getOutputSymbols())));
  }
}
