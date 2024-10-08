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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.plan.expression.UnknownExpressionTypeException;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;

import org.apache.tsfile.utils.Pair;

import java.util.Arrays;

import static org.apache.iotdb.commons.conf.IoTDBConstant.TIME;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression.Operator.AND;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression.Operator.OR;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression.and;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression.or;

public class PredicateUtils {

  private PredicateUtils() {
    // util class
  }

  /**
   * TODO consider more expression types
   *
   * <p>Extract global time predicate from query predicate.
   *
   * @param predicate raw query predicate
   * @param canRewrite determined by the father of current expression
   * @param isFirstOr whether it is the first LogicOrExpression encountered
   * @return left is global time predicate, right is whether it has value filter
   * @throws UnknownExpressionTypeException unknown expression type
   */
  public static Pair<Expression, Boolean> extractGlobalTimePredicate(
      Expression predicate, boolean canRewrite, boolean isFirstOr) {
    if (predicate instanceof LogicalExpression
        && ((LogicalExpression) predicate).getOperator().equals(AND)) {
      Pair<Expression, Boolean> leftResultPair =
          extractGlobalTimePredicate(
              ((LogicalExpression) predicate).getTerms().get(0), canRewrite, isFirstOr);
      Pair<Expression, Boolean> rightResultPair =
          extractGlobalTimePredicate(
              ((LogicalExpression) predicate).getTerms().get(1), canRewrite, isFirstOr);

      // rewrite predicate to avoid duplicate calculation on time filter
      // If Left-child or Right-child does not contain value filter
      // We can set it to true in Predicate Tree
      if (canRewrite) {
        LogicalExpression logicalExpression = (LogicalExpression) predicate;
        Expression newLeftExpression = null, newRightExpression = null;
        if (leftResultPair.left != null && !leftResultPair.right) {
          newLeftExpression = TRUE_LITERAL;
        }
        if (rightResultPair.left != null && !rightResultPair.right) {
          newRightExpression = TRUE_LITERAL;
        }
        if (newLeftExpression != null || newRightExpression != null) {
          logicalExpression.setTerms(
              Arrays.asList(
                  newLeftExpression != null
                      ? newLeftExpression
                      : logicalExpression.getTerms().get(0),
                  newRightExpression != null
                      ? newRightExpression
                      : logicalExpression.getTerms().get(1)));
        }
      }

      if (leftResultPair.left != null && rightResultPair.left != null) {
        return new Pair<>(
            and(leftResultPair.left, rightResultPair.left),
            leftResultPair.right || rightResultPair.right);
      } else if (leftResultPair.left != null) {
        return new Pair<>(leftResultPair.left, true);
      } else if (rightResultPair.left != null) {
        return new Pair<>(rightResultPair.left, true);
      }
      return new Pair<>(null, true);
    } else if (predicate instanceof LogicalExpression
        && ((LogicalExpression) predicate).getOperator().equals(OR)) {
      Pair<Expression, Boolean> leftResultPair =
          extractGlobalTimePredicate(
              ((LogicalExpression) predicate).getTerms().get(0), false, false);
      Pair<Expression, Boolean> rightResultPair =
          extractGlobalTimePredicate(
              ((LogicalExpression) predicate).getTerms().get(1), false, false);

      if (leftResultPair.left != null && rightResultPair.left != null) {
        if (Boolean.TRUE.equals(isFirstOr && !leftResultPair.right && !rightResultPair.right)) {
          ((LogicalExpression) predicate).getTerms().set(0, TRUE_LITERAL);
          ((LogicalExpression) predicate).getTerms().set(0, TRUE_LITERAL);
        }
        return new Pair<>(
            or(leftResultPair.left, rightResultPair.left),
            leftResultPair.right || rightResultPair.right);
      }
      return new Pair<>(null, true);
    }
    //    else if (predicate.getExpressionType().equals(ExpressionType.LOGIC_NOT)) {
    //      Pair<Expression, Boolean> childResultPair =
    //          extractGlobalTimePredicate(
    //              ((UnaryExpression) predicate).getExpression(), canRewrite, isFirstOr);
    //      if (childResultPair.left != null) {
    //        return new Pair<>(not(childResultPair.left), childResultPair.right);
    //      }
    //      return new Pair<>(null, true);
    //    }
    else if (predicate instanceof ComparisonExpression) {
      Expression leftExpression = ((ComparisonExpression) predicate).getLeft();
      Expression rightExpression = ((ComparisonExpression) predicate).getRight();
      if (checkIsTimeFilter(leftExpression, rightExpression)
          || checkIsTimeFilter(rightExpression, leftExpression)) {
        return new Pair<>(predicate, false);
      }
      return new Pair<>(null, true);
    }
    //    else if (predicate instanceof LIKE)
    //        || predicate.getExpressionType().equals(ExpressionType.REGEXP)) {
    //      // time filter don't support LIKE and REGEXP
    //      return new Pair<>(null, true);
    //    }

    //    else if (predicate instanceof BetweenPredicate) {
    //      Expression firstExpression = ((TernaryExpression) predicate).getFirstExpression();
    //      Expression secondExpression = ((TernaryExpression) predicate).getSecondExpression();
    //      Expression thirdExpression = ((TernaryExpression) predicate).getThirdExpression();
    //
    //      boolean isTimeFilter = false;
    //      if (firstExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
    //        isTimeFilter = checkBetweenConstantSatisfy(secondExpression, thirdExpression);
    //      } else if (secondExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
    //        isTimeFilter = checkBetweenConstantSatisfy(firstExpression, thirdExpression);
    //      } else if (thirdExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
    //        isTimeFilter = checkBetweenConstantSatisfy(secondExpression, firstExpression);
    //      }
    //      if (isTimeFilter) {
    //        return new Pair<>(predicate, false);
    //      }
    //      return new Pair<>(null, true);
    //    }

    else if (predicate instanceof InPredicate) {
      // time filter don't support IS_NULL
      return new Pair<>(null, true);
    }
    //    else if (predicate.getExpressionType().equals(ExpressionType.IN)) {
    //      Expression timeExpression = ((InExpression) predicate).getExpression();
    //      if (timeExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
    //        return new Pair<>(predicate, false);
    //      }
    //      return new Pair<>(null, true);
    //    }
    //    else if (predicate.getExpressionType().equals(ExpressionType.TIMESERIES)
    //        || predicate.getExpressionType().equals(ExpressionType.CONSTANT)) {
    //      return new Pair<>(null, true);
    //    }
    //    else if (predicate.getExpressionType().equals(ExpressionType.CASE_WHEN_THEN)) {
    //      return new Pair<>(null, true);
    //    }
    //    else if (ExpressionType.FUNCTION.equals(predicate.getExpressionType())) {
    //      return new Pair<>(null, true);
    //    }
    else {
      throw new IllegalStateException(
          "Unsupported expression in extractGlobalTimePredicate: " + predicate);
    }
  }

  private static boolean checkIsTimeFilter(Expression timeExpression, Expression valueExpression) {
    return timeExpression instanceof Identifier
        && ((Identifier) timeExpression).getValue().equalsIgnoreCase(TIME)
        && valueExpression instanceof LongLiteral;
  }
}
