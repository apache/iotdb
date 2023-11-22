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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.UnknownExpressionTypeException;
import org.apache.iotdb.db.queryengine.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate.ReversePredicateVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

public class PredicateUtils {

  private PredicateUtils() {
    // util class
  }

  /**
   * Extract global time predicate from query predicate.
   *
   * @param predicate raw query predicate
   * @param canRewrite determined by the father of current expression
   * @param isFirstOr whether it is the first LogicOrExpression encountered
   * @return global time predicate
   * @throws UnknownExpressionTypeException unknown expression type
   */
  public static Pair<Expression, Boolean> extractGlobalTimePredicate(
      Expression predicate, boolean canRewrite, boolean isFirstOr) {
    if (predicate.getExpressionType().equals(ExpressionType.LOGIC_AND)) {
      Pair<Expression, Boolean> leftResultPair =
          extractGlobalTimePredicate(
              ((BinaryExpression) predicate).getLeftExpression(), canRewrite, isFirstOr);
      Pair<Expression, Boolean> rightResultPair =
          extractGlobalTimePredicate(
              ((BinaryExpression) predicate).getRightExpression(), canRewrite, isFirstOr);

      // rewrite predicate to avoid duplicate calculation on time filter
      // If Left-child or Right-child does not contain value filter
      // We can set it to true in Predicate Tree
      if (canRewrite) {
        if (leftResultPair.left != null && !leftResultPair.right) {
          ((BinaryExpression) predicate)
              .setLeftExpression(new ConstantOperand(TSDataType.BOOLEAN, "true"));
        }
        if (rightResultPair.left != null && !rightResultPair.right) {
          ((BinaryExpression) predicate)
              .setRightExpression(new ConstantOperand(TSDataType.BOOLEAN, "true"));
        }
      }

      if (leftResultPair.left != null && rightResultPair.left != null) {
        return new Pair<>(
            ExpressionFactory.and(leftResultPair.left, rightResultPair.left),
            leftResultPair.right || rightResultPair.right);
      } else if (leftResultPair.left != null) {
        return new Pair<>(leftResultPair.left, true);
      } else if (rightResultPair.left != null) {
        return new Pair<>(rightResultPair.left, true);
      }
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.LOGIC_OR)) {
      Pair<Expression, Boolean> leftResultPair =
          extractGlobalTimePredicate(
              ((BinaryExpression) predicate).getLeftExpression(), false, false);
      Pair<Expression, Boolean> rightResultPair =
          extractGlobalTimePredicate(
              ((BinaryExpression) predicate).getRightExpression(), false, false);

      if (leftResultPair.left != null && rightResultPair.left != null) {
        if (Boolean.TRUE.equals(isFirstOr && !leftResultPair.right && !rightResultPair.right)) {
          ((BinaryExpression) predicate)
              .setLeftExpression(new ConstantOperand(TSDataType.BOOLEAN, "true"));
          ((BinaryExpression) predicate)
              .setRightExpression(new ConstantOperand(TSDataType.BOOLEAN, "true"));
        }
        return new Pair<>(
            ExpressionFactory.or(leftResultPair.left, rightResultPair.left),
            leftResultPair.right || rightResultPair.right);
      }
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.LOGIC_NOT)) {
      Pair<Expression, Boolean> childResultPair =
          extractGlobalTimePredicate(
              ((UnaryExpression) predicate).getExpression(), canRewrite, isFirstOr);
      if (childResultPair.left != null) {
        return new Pair<>(ExpressionFactory.not(childResultPair.left), childResultPair.right);
      }
      return new Pair<>(null, true);
    } else if (predicate.isCompareBinaryExpression()) {
      Expression leftExpression = ((BinaryExpression) predicate).getLeftExpression();
      Expression rightExpression = ((BinaryExpression) predicate).getRightExpression();
      if (checkIsTimeFilter(leftExpression, rightExpression)
          || checkIsTimeFilter(rightExpression, leftExpression)) {
        return new Pair<>(predicate, false);
      }
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.LIKE)
        || predicate.getExpressionType().equals(ExpressionType.REGEXP)) {
      // time filter don't support LIKE and REGEXP
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.BETWEEN)) {
      Expression firstExpression = ((TernaryExpression) predicate).getFirstExpression();
      Expression secondExpression = ((TernaryExpression) predicate).getSecondExpression();
      Expression thirdExpression = ((TernaryExpression) predicate).getThirdExpression();

      boolean isTimeFilter = false;
      if (firstExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
        isTimeFilter = checkBetweenConstantSatisfy(secondExpression, thirdExpression);
      } else if (secondExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
        isTimeFilter = checkBetweenConstantSatisfy(firstExpression, thirdExpression);
      } else if (thirdExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
        isTimeFilter = checkBetweenConstantSatisfy(secondExpression, firstExpression);
      }
      if (isTimeFilter) {
        return new Pair<>(predicate, false);
      }
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.IS_NULL)) {
      // time filter don't support IS_NULL
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.IN)) {
      Expression timeExpression = ((InExpression) predicate).getExpression();
      if (timeExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
        return new Pair<>(predicate, false);
      }
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.TIMESERIES)
        || predicate.getExpressionType().equals(ExpressionType.CONSTANT)
        || predicate.getExpressionType().equals(ExpressionType.NULL)) {
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.CASE_WHEN_THEN)) {
      return new Pair<>(null, true);
    } else {
      throw new UnknownExpressionTypeException(predicate.getExpressionType());
    }
  }

  private static boolean checkIsTimeFilter(Expression timeExpression, Expression valueExpression) {
    return timeExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)
        && valueExpression instanceof ConstantOperand
        && ((ConstantOperand) valueExpression).getDataType() == TSDataType.INT64;
  }

  private static boolean checkBetweenConstantSatisfy(
      Expression firstExpression, Expression secondExpression) {
    return firstExpression.isConstantOperand()
        && secondExpression.isConstantOperand()
        && ((ConstantOperand) firstExpression).getDataType() == TSDataType.INT64
        && ((ConstantOperand) secondExpression).getDataType() == TSDataType.INT64
        && (Long.parseLong(((ConstantOperand) firstExpression).getValueString())
            <= Long.parseLong(((ConstantOperand) secondExpression).getValueString()));
  }

  /**
   * Check if the given expression contains time filter.
   *
   * @param predicate given expression
   * @return true if the given expression contains time filter
   */
  public static boolean checkIfTimeFilterExist(Expression predicate) {
    if (predicate instanceof TernaryExpression) {
      return checkIfTimeFilterExist(((TernaryExpression) predicate).getFirstExpression())
          || checkIfTimeFilterExist(((TernaryExpression) predicate).getSecondExpression())
          || checkIfTimeFilterExist(((TernaryExpression) predicate).getThirdExpression());
    } else if (predicate instanceof BinaryExpression) {
      return checkIfTimeFilterExist(((BinaryExpression) predicate).getLeftExpression())
          || checkIfTimeFilterExist(((BinaryExpression) predicate).getRightExpression());
    } else if (predicate instanceof UnaryExpression) {
      return checkIfTimeFilterExist(((UnaryExpression) predicate).getExpression());
    } else if (predicate instanceof FunctionExpression) {
      boolean timeFilterExist = false;
      for (Expression childExpression : predicate.getExpressions()) {
        timeFilterExist = timeFilterExist || checkIfTimeFilterExist(childExpression);
      }
      return timeFilterExist;
    } else if (predicate instanceof CaseWhenThenExpression) {
      for (Expression childExpression : predicate.getExpressions()) {
        if (checkIfTimeFilterExist(childExpression)) {
          return true;
        }
      }
      return false;
    } else if (predicate instanceof TimeSeriesOperand
        || predicate instanceof ConstantOperand
        || predicate instanceof NullOperand) {
      return false;
    } else if (predicate instanceof TimestampOperand) {
      return true;
    } else {
      throw new UnknownExpressionTypeException(predicate.getExpressionType());
    }
  }

  /**
   * Recursively removes all use of the not() operator in a predicate by replacing all instances of
   * not(x) with the inverse(x),
   *
   * <p>eg: not(and(eq(), not(eq(y))) -&gt; or(notEq(), eq(y))
   *
   * <p>The returned predicate should have the same meaning as the original, but without the use of
   * the not() operator.
   *
   * <p>See also {@link PredicateUtils#reversePredicate(Expression)}, which is used to do the
   * inversion.
   *
   * @param predicate the predicate to remove not() from
   * @return the predicate with all not() operators removed
   */
  public static Expression predicateRemoveNot(Expression predicate) {
    if (predicate.getExpressionType().equals(ExpressionType.LOGIC_AND)) {
      return ExpressionFactory.and(
          predicateRemoveNot(((BinaryExpression) predicate).getLeftExpression()),
          predicateRemoveNot(((BinaryExpression) predicate).getRightExpression()));
    } else if (predicate.getExpressionType().equals(ExpressionType.LOGIC_OR)) {
      return ExpressionFactory.or(
          predicateRemoveNot(((BinaryExpression) predicate).getLeftExpression()),
          predicateRemoveNot(((BinaryExpression) predicate).getRightExpression()));
    } else if (predicate.getExpressionType().equals(ExpressionType.LOGIC_NOT)) {
      return reversePredicate(((LogicNotExpression) predicate).getExpression());
    }
    return predicate;
  }

  /**
   * Converts a predicate to its logical inverse. The returned predicate should be equivalent to
   * not(p), but without the use of a not() operator.
   *
   * <p>See also {@link PredicateUtils#predicateRemoveNot(Expression)}, which can remove the use of
   * all not() operators without inverting the overall predicate.
   *
   * @param predicate given predicate
   * @return the predicate after reversing
   */
  public static Expression reversePredicate(Expression predicate) {
    return new ReversePredicateVisitor().process(predicate, null);
  }

  /**
   * Simplify the given predicate (Remove the TRUE expression).
   *
   * @param predicate given predicate
   * @return the predicate after simplifying
   */
  public static Expression simplifyPredicate(Expression predicate) {
    if (predicate.getExpressionType().equals(ExpressionType.LOGIC_AND)) {
      Expression left = simplifyPredicate(((BinaryExpression) predicate).getLeftExpression());
      Expression right = simplifyPredicate(((BinaryExpression) predicate).getRightExpression());
      boolean isLeftTrue =
          left.isConstantOperand() && Boolean.parseBoolean(left.getExpressionString());
      boolean isRightTrue =
          right.isConstantOperand() && Boolean.parseBoolean(right.getExpressionString());
      if (isLeftTrue && isRightTrue) {
        return new ConstantOperand(TSDataType.BOOLEAN, "true");
      } else if (isLeftTrue) {
        return right;
      } else if (isRightTrue) {
        return left;
      }
      return predicate;
    } else if (predicate.getExpressionType().equals(ExpressionType.LOGIC_OR)) {
      Expression left = simplifyPredicate(((BinaryExpression) predicate).getLeftExpression());
      Expression right = simplifyPredicate(((BinaryExpression) predicate).getRightExpression());
      boolean isLeftTrue =
          left.isConstantOperand() && Boolean.parseBoolean(left.getExpressionString());
      boolean isRightTrue =
          right.isConstantOperand() && Boolean.parseBoolean(right.getExpressionString());
      if (isRightTrue || isLeftTrue) {
        return new ConstantOperand(TSDataType.BOOLEAN, "true");
      }
    }
    return predicate;
  }

  /**
   * Convert the given predicate to time filter.
   *
   * <p>Note: the supplied predicate must not contain any instances of the not() operator as this is
   * not supported by this filter. The supplied predicate should first be run through {@link
   * PredicateUtils#predicateRemoveNot(Expression)} to rewrite it in a form that doesn't make use of
   * the not() operator.
   *
   * @param predicate given predicate
   * @return the time filter converted from the given predicate
   */
  public static Filter convertPredicateToTimeFilter(Expression predicate) {
    if (predicate == null) {
      return null;
    }
    return predicate.accept(new ConvertExpressionToTimeFilterVisitor(), null);
  }

  /**
   * Combine the given conjuncts into a single expression using "and".
   *
   * @param conjuncts given conjuncts
   * @return the expression combined by the given conjuncts
   */
  public static Expression combineConjuncts(List<Expression> conjuncts) {
    if (conjuncts.size() == 1) {
      return conjuncts.get(0);
    }
    return constructRightDeepTreeWithAnd(conjuncts);
  }

  private static Expression constructRightDeepTreeWithAnd(List<Expression> conjuncts) {
    // TODO: consider other structures of tree
    if (conjuncts.size() == 2) {
      return new LogicAndExpression(conjuncts.get(0), conjuncts.get(1));
    } else {
      return new LogicAndExpression(
          conjuncts.get(0), constructRightDeepTreeWithAnd(conjuncts.subList(1, conjuncts.size())));
    }
  }
}
