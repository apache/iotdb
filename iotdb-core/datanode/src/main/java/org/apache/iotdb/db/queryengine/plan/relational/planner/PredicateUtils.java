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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

  //  private static boolean checkBetweenConstantSatisfy(Expression e1, Expression e2) {
  //    return e1.isConstantOperand()
  //        && e2.isConstantOperand()
  //        && ((ConstantOperand) e1).getDataType() == TSDataType.INT64
  //        && ((ConstantOperand) e2).getDataType() == TSDataType.INT64
  //        && (Long.parseLong(((ConstantOperand) e1).getValueString())
  //            <= Long.parseLong(((ConstantOperand) e2).getValueString()));
  //  }

  /**
   * Check if the given expression contains time filter.
   *
   * @param predicate given expression
   * @return true if the given expression contains time filter
   */
  //  public static boolean checkIfTimeFilterExist(Expression predicate) {
  //    return new TimeFilterExistChecker().process(predicate, null);
  //  }

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
  //  public static Expression predicateRemoveNot(Expression predicate) {
  //    if (predicate.getExpressionType().equals(ExpressionType.LOGIC_AND)) {
  //      return ExpressionFactory.and(
  //          predicateRemoveNot(((BinaryExpression) predicate).getLeftExpression()),
  //          predicateRemoveNot(((BinaryExpression) predicate).getRightExpression()));
  //    } else if (predicate.getExpressionType().equals(ExpressionType.LOGIC_OR)) {
  //      return ExpressionFactory.or(
  //          predicateRemoveNot(((BinaryExpression) predicate).getLeftExpression()),
  //          predicateRemoveNot(((BinaryExpression) predicate).getRightExpression()));
  //    } else if (predicate.getExpressionType().equals(ExpressionType.LOGIC_NOT)) {
  //      return reversePredicate(((LogicNotExpression) predicate).getExpression());
  //    }
  //    return predicate;
  //  }

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
  //  public static Expression reversePredicate(Expression predicate) {
  //    return new ReversePredicateVisitor().process(predicate, null);
  //  }

  /**
   * Simplify the given predicate (Remove the NULL and TRUE/FALSE expression).
   *
   * @param predicate given predicate
   * @return the predicate after simplifying
   */
  //  public static Expression simplifyPredicate(Expression predicate) {
  //    return new PredicateSimplifier().process(predicate, null);
  //  }

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
  //  public static Filter convertPredicateToTimeFilter(Expression predicate) {
  //    if (predicate == null) {
  //      return null;
  //    }
  //    return predicate.accept(new ConvertPredicateToTimeFilterVisitor(), null);
  //  }

  //  public static Filter convertPredicateToFilter(
  //      Expression predicate,
  //      List<String> allMeasurements,
  //      boolean isBuildPlanUseTemplate,
  //      TypeProvider typeProvider) {
  //    if (predicate == null) {
  //      return null;
  //    }
  //    return predicate.accept(
  //        new ConvertPredicateToFilterVisitor(),
  //        new ConvertPredicateToFilterVisitor.Context(
  //            allMeasurements, isBuildPlanUseTemplate, typeProvider));
  //  }

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
      return and(conjuncts.get(0), conjuncts.get(1));
    } else {
      return and(
          conjuncts.get(0), constructRightDeepTreeWithAnd(conjuncts.subList(1, conjuncts.size())));
    }
  }

  public static Expression removeDuplicateConjunct(Expression predicate) {
    if (predicate == null) {
      return null;
    }
    Set<Expression> conjuncts = new HashSet<>();
    extractConjuncts(predicate, conjuncts);
    return combineConjuncts(new ArrayList<>(conjuncts));
  }

  public static List<Expression> extractConjuncts(Expression predicate) {
    Set<Expression> conjuncts = new HashSet<>();
    extractConjuncts(predicate, conjuncts);
    return new ArrayList<>(conjuncts);
  }

  private static void extractConjuncts(Expression predicate, Set<Expression> conjuncts) {
    if (predicate instanceof LogicalExpression
        && ((LogicalExpression) predicate).getOperator() == AND) {
      extractConjuncts(((LogicalExpression) predicate).getTerms().get(0), conjuncts);
      extractConjuncts(((LogicalExpression) predicate).getTerms().get(1), conjuncts);
    } else {
      conjuncts.add(predicate);
    }
  }

  /**
   * Extract the source symbol (full path for non-aligned path, device path for aligned path) from
   * the given predicate. If the predicate contains multiple source symbols, return null.
   *
   * @param predicate given predicate
   * @return the source symbol extracted from the given predicate
   */
  //  public static PartialPath extractPredicateSourceSymbol(Expression predicate) {
  //    List<Expression> sourceExpressions = ExpressionAnalyzer.searchSourceExpressions(predicate);
  //    Set<PartialPath> sourcePaths =
  //        sourceExpressions.stream()
  //            .map(expression -> ((TimeSeriesOperand) expression).getPath())
  //            .collect(Collectors.toSet());
  //    Iterator<PartialPath> pathIterator = sourcePaths.iterator();
  //    MeasurementPath firstPath = (MeasurementPath) pathIterator.next();
  //
  //    if (sourcePaths.size() == 1) {
  //      // only contain one source path, can be push down
  //      return firstPath.isUnderAlignedEntity() ? firstPath.getDevicePath() : firstPath;
  //    }
  //
  //    // sourcePaths contain more than one path, can be push down when
  //    // these paths under on aligned device
  //    if (!firstPath.isUnderAlignedEntity()) {
  //      return null;
  //    }
  //    PartialPath checkedDevice = firstPath.getDevicePath();
  //    while (pathIterator.hasNext()) {
  //      MeasurementPath path = (MeasurementPath) pathIterator.next();
  //      if (!path.isUnderAlignedEntity() || !path.getDevicePath().equals(checkedDevice)) {
  //        return null;
  //      }
  //    }
  //    return checkedDevice;
  //  }

  /**
   * Check if the given predicate can be pushed down from FilterNode to ScanNode.
   *
   * <p>The predicate <b>cannot</b> be pushed down if it satisfies the following conditions:
   * <li>predicate contains IS_NULL
   *
   * @param predicate given predicate
   * @return true if the given predicate can be pushed down to source
   */
  //  public static boolean predicateCanPushDownToSource(Expression predicate) {
  //    return new PredicateCanPushDownToSourceChecker().process(predicate, null);
  //  }

  /**
   * Check if the given predicate can be pushed into ScanOperator and execute using the {@link
   * Filter} interface.
   *
   * @param predicate given predicate
   * @return true if the given predicate can be pushed into ScanOperator
   */
  //  public static boolean predicateCanPushIntoScan(Expression predicate) {
  //    return new PredicatePushIntoScanChecker().process(predicate, null);
  //  }
}
