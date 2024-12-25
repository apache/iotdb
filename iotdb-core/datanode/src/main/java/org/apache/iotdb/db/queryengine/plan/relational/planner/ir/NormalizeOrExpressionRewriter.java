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

package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.and;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.ir.IrUtils.or;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.EQUAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression.Operator.AND;

public final class NormalizeOrExpressionRewriter {

  public static Expression normalizeOrExpression(Expression expression) {
    return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
  }

  private NormalizeOrExpressionRewriter() {}

  private static class Visitor extends ExpressionRewriter<Void> {

    @Override
    public Expression rewriteLogicalExpression(
        LogicalExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      List<Expression> terms =
          node.getTerms().stream()
              .map(expression -> treeRewriter.rewrite(expression, context))
              .collect(toImmutableList());

      if (node.getOperator() == AND) {
        return and(terms);
      }

      ImmutableList.Builder<InPredicate> inPredicateBuilder = ImmutableList.builder();
      ImmutableSet.Builder<Expression> expressionToSkipBuilder = ImmutableSet.builder();
      ImmutableList.Builder<Expression> othersExpressionBuilder = ImmutableList.builder();
      groupComparisonAndInPredicate(terms)
          .forEach(
              (expression, values) -> {
                if (values.size() > 1) {
                  inPredicateBuilder.add(
                      new InPredicate(expression, mergeToInListExpression(values)));
                  expressionToSkipBuilder.add(expression);
                }
              });

      Set<Expression> expressionToSkip = expressionToSkipBuilder.build();
      for (Expression expression : terms) {
        if (expression instanceof ComparisonExpression
            && ((ComparisonExpression) expression).getOperator() == EQUAL) {
          ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
          if (!expressionToSkip.contains(comparisonExpression.getLeft())) {
            othersExpressionBuilder.add(expression);
          }
        } else if (expression instanceof InPredicate
            && ((InPredicate) expression).getValueList() instanceof InListExpression) {
          InPredicate inPredicate = (InPredicate) expression;
          if (!expressionToSkip.contains(inPredicate.getValue())) {
            othersExpressionBuilder.add(expression);
          }
        } else {
          othersExpressionBuilder.add(expression);
        }
      }

      return or(
          ImmutableList.<Expression>builder()
              .addAll(othersExpressionBuilder.build())
              .addAll(inPredicateBuilder.build())
              .build());
    }

    private InListExpression mergeToInListExpression(Collection<Expression> expressions) {
      LinkedHashSet<Expression> expressionValues = new LinkedHashSet<>();
      for (Expression expression : expressions) {
        if (expression instanceof ComparisonExpression
            && ((ComparisonExpression) expression).getOperator() == EQUAL) {
          ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
          expressionValues.add(comparisonExpression.getRight());
        } else if (expression instanceof InPredicate
            && ((InPredicate) expression).getValueList() instanceof InListExpression) {
          InPredicate inPredicate = (InPredicate) expression;
          InListExpression valueList = (InListExpression) inPredicate.getValueList();
          expressionValues.addAll(valueList.getValues());
        } else {
          throw new IllegalStateException("Unexpected expression: " + expression);
        }
      }

      return new InListExpression(ImmutableList.copyOf(expressionValues));
    }

    private Map<Expression, Collection<Expression>> groupComparisonAndInPredicate(
        List<Expression> terms) {
      ImmutableMultimap.Builder<Expression, Expression> expressionBuilder =
          ImmutableMultimap.builder();
      for (Expression expression : terms) {
        if (expression instanceof ComparisonExpression
            && ((ComparisonExpression) expression).getOperator() == EQUAL) {
          ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
          expressionBuilder.put(comparisonExpression.getLeft(), comparisonExpression);
        } else if (expression instanceof InPredicate
            && ((InPredicate) expression).getValueList() instanceof InListExpression) {
          InPredicate inPredicate = (InPredicate) expression;
          expressionBuilder.put(inPredicate.getValue(), inPredicate);
        }
      }

      return expressionBuilder.build().asMap();
    }
  }
}
