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

import org.apache.iotdb.db.relational.sql.tree.ComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.relational.sql.tree.LogicalExpression;

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
import static org.apache.iotdb.db.relational.sql.tree.ComparisonExpression.Operator.EQUAL;
import static org.apache.iotdb.db.relational.sql.tree.LogicalExpression.Operator.AND;

public final class NormalizeOrExpressionRewriter {

  public static Expression normalizeOrExpression(Expression expression) {
    return new Visitor().process(expression, null);
  }

  private NormalizeOrExpressionRewriter() {}

  private static class Visitor extends RewritingVisitor<Void> {

    @Override
    public Expression visitLogicalExpression(LogicalExpression node, Void context) {
      List<Expression> terms =
          node.getTerms().stream()
              .map(expression -> process(expression, context))
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
                  // TODO mergeToInListExpression may have more than one value
                  inPredicateBuilder.add(
                      new InPredicate(expression, mergeToInListExpression(values).get(0)));
                  expressionToSkipBuilder.add(expression);
                }
              });

      Set<Expression> expressionToSkip = expressionToSkipBuilder.build();
      for (Expression expression : terms) {
        if (expression instanceof ComparisonExpression
            && ((ComparisonExpression) expression).getOperator() == EQUAL) {

          if (!expressionToSkip.contains(((ComparisonExpression) expression).getLeft())) {
            othersExpressionBuilder.add(expression);
          }
        } else if (expression instanceof InPredicate) {
          if (!expressionToSkip.contains(((InPredicate) expression).getValue())) {
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

    private List<Expression> mergeToInListExpression(Collection<Expression> expressions) {
      LinkedHashSet<Expression> expressionValues = new LinkedHashSet<>();
      for (Expression expression : expressions) {
        if (expression instanceof ComparisonExpression
            && ((ComparisonExpression) expression).getOperator() == EQUAL) {
          expressionValues.add(((ComparisonExpression) expression).getRight());
        } else if (expression instanceof InPredicate) {
          // TODO inPredicate has getValues method
          expressionValues.add(((InPredicate) expression).getValue());
        } else {
          throw new IllegalStateException("Unexpected expression: " + expression);
        }
      }

      return ImmutableList.copyOf(expressionValues);
    }

    private Map<Expression, Collection<Expression>> groupComparisonAndInPredicate(
        List<Expression> terms) {
      ImmutableMultimap.Builder<Expression, Expression> expressionBuilder =
          ImmutableMultimap.builder();
      for (Expression expression : terms) {
        if (expression instanceof ComparisonExpression
            && ((ComparisonExpression) expression).getOperator() == EQUAL) {
          expressionBuilder.put(((ComparisonExpression) expression).getLeft(), expression);
        } else if (expression instanceof InPredicate) {
          InPredicate inPredicate = (InPredicate) expression;
          expressionBuilder.put(inPredicate.getValue(), inPredicate);
        }
      }

      return expressionBuilder.build().asMap();
    }
  }
}
