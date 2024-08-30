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

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class ConstantsFoldingRewriter {

  public static Expression constantsFolding(Expression expression) {
    if (expression instanceof ComparisonExpression) {
      return evaluate((ComparisonExpression) expression);
    } else if (expression instanceof ArithmeticBinaryExpression) {
      return evaluate((ArithmeticBinaryExpression) expression);
    } else if (expression instanceof BetweenPredicate) {
      return evaluate((BetweenPredicate) expression);
    } else if (expression instanceof LogicalExpression) {
      return evaluate((LogicalExpression) expression);
    } else if (expression instanceof NotExpression) {
      return evaluate((NotExpression) expression);
    } else if (expression instanceof InPredicate) {
      return evaluate((InPredicate) expression);
    } else if (expression instanceof LongLiteral) {
      return expression;
    }
    return expression;
  }

  private ConstantsFoldingRewriter() {}

  private static Expression evaluate(ComparisonExpression expression) {
    Expression left = constantsFolding(expression.getLeft());
    Expression right = constantsFolding(expression.getRight());

    if (left instanceof LongLiteral && right instanceof LongLiteral) {
      long leftValue = ((LongLiteral) left).getParsedValue();
      long rightValue = ((LongLiteral) right).getParsedValue();

      switch (expression.getOperator()) {
        case EQUAL:
          return new BooleanLiteral(leftValue == rightValue);
        case NOT_EQUAL:
          return new BooleanLiteral(leftValue != rightValue);
        case LESS_THAN:
          return new BooleanLiteral(leftValue < rightValue);
        case LESS_THAN_OR_EQUAL:
          return new BooleanLiteral(leftValue <= rightValue);
        case GREATER_THAN:
          return new BooleanLiteral(leftValue > rightValue);
        case GREATER_THAN_OR_EQUAL:
          return new BooleanLiteral(leftValue >= rightValue);
        default:
          return expression;
      }
    }
    // return partial constants folding
    return new ComparisonExpression(expression.getOperator(), left, right);
  }

  private static Expression evaluate(ArithmeticBinaryExpression expression) {
    Expression left = constantsFolding(expression.getLeft());
    Expression right = constantsFolding(expression.getRight());

    if (left instanceof LongLiteral && right instanceof LongLiteral) {
      long leftValue = ((LongLiteral) left).getParsedValue();
      long rightValue = ((LongLiteral) right).getParsedValue();

      switch (expression.getOperator()) {
        case ADD:
          return new LongLiteral(leftValue + rightValue);
        case SUBTRACT:
          return new LongLiteral(leftValue - rightValue);
        case MULTIPLY:
          return new LongLiteral(leftValue * rightValue);
        case DIVIDE:
          if (rightValue == 0) {
            throw new ArithmeticException("Division by zero");
          }
          return new LongLiteral(leftValue / rightValue);
        case MODULUS:
          if (rightValue == 0) {
            throw new ArithmeticException("Modulus by zero");
          }
          return new LongLiteral(leftValue % rightValue);
        default:
          return expression;
      }
    }
    return expression;
  }

  private static Expression evaluate(BetweenPredicate expression) {
    Expression value = constantsFolding(expression.getValue());
    Expression min = constantsFolding(expression.getMin());
    Expression max = constantsFolding(expression.getMax());

    if (value instanceof LongLiteral && min instanceof LongLiteral && max instanceof LongLiteral) {
      long valueVal = ((LongLiteral) value).getParsedValue();
      long minVal = ((LongLiteral) min).getParsedValue();
      // short circuit
      if (!(minVal <= valueVal)) return new BooleanLiteral(Boolean.FALSE);
      long maxVal = ((LongLiteral) max).getParsedValue();
      return new BooleanLiteral(valueVal <= maxVal);
    } else if (min instanceof LongLiteral && max instanceof LongLiteral) {
      long minVal = ((LongLiteral) min).getParsedValue();
      long maxVal = ((LongLiteral) max).getParsedValue();
      if (!(minVal <= maxVal)) return new BooleanLiteral(Boolean.FALSE);
    } else if (min instanceof LongLiteral && value instanceof LongLiteral) {
      long minVal = ((LongLiteral) min).getParsedValue();
      long valueVal = ((LongLiteral) value).getParsedValue();
      if (!(minVal <= valueVal)) return new BooleanLiteral(Boolean.FALSE);
    } else if (value instanceof LongLiteral && max instanceof LongLiteral) {
      long valueVal = ((LongLiteral) value).getParsedValue();
      long maxVal = ((LongLiteral) max).getParsedValue();
      if (!(valueVal <= maxVal)) return new BooleanLiteral(Boolean.FALSE);
    }
    // return partial constants folding
    return new BetweenPredicate(value, min, max);
  }

  private static Expression evaluate(LogicalExpression expression) {
    // get shortCircuit for current Logical Operator
    Boolean shortCircuit;
    switch (expression.getOperator()) {
      case AND:
        shortCircuit = Boolean.FALSE;
        break;
      case OR:
        shortCircuit = Boolean.TRUE;
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported logical expression type: " + expression.getOperator());
    }

    List<Expression> foldedTerms = new ArrayList<>();

    for (Expression term : expression.getTerms()) {
      Expression foldedTerm = constantsFolding(term);

      if (foldedTerm instanceof BooleanLiteral) {
        BooleanLiteral booleanLiteral = (BooleanLiteral) foldedTerm;
        boolean value = booleanLiteral.getValue();

        if (shortCircuit.equals(value)) {
          return booleanLiteral;
        }
      } else {
        foldedTerms.add(foldedTerm);
      }
    }

    if (foldedTerms.isEmpty()) {
      return new BooleanLiteral(!shortCircuit);
    }

    if (foldedTerms.size() == 1) {
      return foldedTerms.get(0);
    }

    return new LogicalExpression(expression.getOperator(), foldedTerms);
  }

  private static Expression evaluate(NotExpression expression) {
    Expression value = constantsFolding(expression.getValue());

    if (value instanceof BooleanLiteral) {
      return new BooleanLiteral(!((BooleanLiteral) value).getValue());
    }

    return new NotExpression(value);
  }

  private static Expression evaluate(InPredicate expression) {
    // constants folding for InPredicate's value
    Expression foldedExpression = constantsFolding(expression.getValue());
    InListExpression valueList = (InListExpression) expression.getValueList();

    // constants folding for InPredicate's valueList
    List<Expression> foldedValues =
        valueList.getValues().stream()
            .map(ConstantsFoldingRewriter::constantsFolding)
            .collect(Collectors.toList());

    boolean allConstants = foldedValues.stream().allMatch(value -> value instanceof LongLiteral);

    if (foldedExpression instanceof LongLiteral && allConstants) {
      boolean result =
          foldedValues.stream()
              .anyMatch(
                  foldedValue ->
                      ((LongLiteral) foldedExpression).getParsedValue()
                          == (((LongLiteral) foldedValue).getParsedValue()));
      return new BooleanLiteral(result);
    }

    // return partial constants folding
    return new InPredicate(foldedExpression, new InListExpression(foldedValues));
  }
}
