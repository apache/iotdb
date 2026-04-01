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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExceptNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.read.common.type.LongType;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.Except.distinct;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression.Operator.SUBTRACT;

public class ImplementExceptAll implements Rule<ExceptNode> {

  private static final Pattern<ExceptNode> PATTERN =
      Patterns.except().with(distinct().equalTo(false));

  private final Metadata metadata;

  public ImplementExceptAll(Metadata metadata) {
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

    // 1. translate the except(all) node to other planNodes
    SetOperationNodeTranslator.TranslationResult translationResult =
        translator.makeSetContainmentPlanForAll(node);

    checkState(
        !translationResult.getCountSymbols().isEmpty(),
        "ExceptNode translation result has no count symbols");

    // 2. add the filter node above the result node from translation process
    // filter condition : row_number <= greatest(...greatest((greatest(count1 - count2, 0) - count3,
    // 0))....)
    Expression minusCount = translationResult.getCountSymbols().get(0).toSymbolReference();
    QualifiedName greatest =
        QualifiedName.of(TableBuiltinScalarFunction.GREATEST.getFunctionName());
    for (int i = 1; i < translationResult.getCountSymbols().size(); i++) {
      minusCount =
          new FunctionCall(
              greatest,
              ImmutableList.of(
                  new ArithmeticBinaryExpression(
                      SUBTRACT,
                      minusCount,
                      translationResult.getCountSymbols().get(i).toSymbolReference()),
                  new GenericLiteral(LongType.INT64.getDisplayName(), "0")));
    }

    FilterNode filterNode =
        new FilterNode(
            context.getIdAllocator().genPlanNodeId(),
            translationResult.getPlanNode(),
            new ComparisonExpression(
                ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
                translationResult.getRowNumberSymbol().toSymbolReference(),
                minusCount));

    // 3. add the project node to remove the redundant columns
    return Result.ofPlanNode(
        new ProjectNode(
            context.getIdAllocator().genPlanNodeId(),
            filterNode,
            Assignments.identity(node.getOutputSymbols())));
  }
}
