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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionFactory;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.UnknownExpressionTypeException;
import org.apache.iotdb.db.queryengine.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.logical.PredicateCanPushDownToSourceChecker;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.logical.TimeFilterExistChecker;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate.ConvertPredicateToFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate.ConvertPredicateToTimeFilterVisitor;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate.PredicatePushIntoScanChecker;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate.PredicateSimplifier;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate.ReversePredicateVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Pair;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    } else if (ExpressionType.FUNCTION.equals(predicate.getExpressionType())) {
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

  private static boolean checkBetweenConstantSatisfy(Expression e1, Expression e2) {
    return e1.isConstantOperand()
        && e2.isConstantOperand()
        && ((ConstantOperand) e1).getDataType() == TSDataType.INT64
        && ((ConstantOperand) e2).getDataType() == TSDataType.INT64
        && (Long.parseLong(((ConstantOperand) e1).getValueString())
            <= Long.parseLong(((ConstantOperand) e2).getValueString()));
  }

  /**
   * Check if the given expression contains time filter.
   *
   * @param predicate given expression
   * @return true if the given expression contains time filter
   */
  public static boolean checkIfTimeFilterExist(Expression predicate) {
    return new TimeFilterExistChecker().process(predicate, null);
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
   * Simplify the given predicate (Remove the NULL and TRUE/FALSE expression).
   *
   * @param predicate given predicate
   * @return the predicate after simplifying
   */
  public static Expression simplifyPredicate(Expression predicate) {
    return new PredicateSimplifier().process(predicate, null);
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
    return predicate.accept(new ConvertPredicateToTimeFilterVisitor(), null);
  }

  public static Filter convertPredicateToTimeFilter(
      org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression predicate) {
    if (predicate == null) {
      return null;
    }
    return predicate.accept(
        new org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate
            .ConvertPredicateToTimeFilterVisitor(),
        null);
  }

  public static Filter convertPredicateToFilter(
      Expression predicate,
      List<String> allMeasurements,
      boolean isBuildPlanUseTemplate,
      TypeProvider typeProvider,
      ZoneId zoneId) {
    if (predicate == null) {
      return null;
    }
    return predicate.accept(
        new ConvertPredicateToFilterVisitor(),
        new ConvertPredicateToFilterVisitor.Context(
            allMeasurements, isBuildPlanUseTemplate, typeProvider, zoneId));
  }

  public static Filter convertPredicateToFilter(
      org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression predicate,
      List<String> allMeasurements,
      Map<Symbol, ColumnSchema> schemaMap) {
    if (predicate == null) {
      return null;
    }
    return predicate.accept(
        new org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate
            .ConvertPredicateToFilterVisitor(),
        new org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate
            .ConvertPredicateToFilterVisitor.Context(allMeasurements, schemaMap));
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
    if (predicate.getExpressionType().equals(ExpressionType.LOGIC_AND)) {
      extractConjuncts(((BinaryExpression) predicate).getLeftExpression(), conjuncts);
      extractConjuncts(((BinaryExpression) predicate).getRightExpression(), conjuncts);
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
  public static PartialPath extractPredicateSourceSymbol(Expression predicate) {
    List<Expression> sourceExpressions = ExpressionAnalyzer.searchSourceExpressions(predicate);
    Set<PartialPath> sourcePaths =
        sourceExpressions.stream()
            .map(expression -> ((TimeSeriesOperand) expression).getPath())
            .collect(Collectors.toSet());
    Iterator<PartialPath> pathIterator = sourcePaths.iterator();
    MeasurementPath firstPath = (MeasurementPath) pathIterator.next();

    if (sourcePaths.size() == 1) {
      // only contain one source path, can be push down
      return firstPath.isUnderAlignedEntity() ? firstPath.getDevicePath() : firstPath;
    }

    // sourcePaths contain more than one path, can be push down when
    // these paths under on aligned device
    if (!firstPath.isUnderAlignedEntity()) {
      return null;
    }
    PartialPath checkedDevice = firstPath.getDevicePath();
    while (pathIterator.hasNext()) {
      MeasurementPath path = (MeasurementPath) pathIterator.next();
      if (!path.isUnderAlignedEntity() || !path.getDevicePath().equals(checkedDevice)) {
        return null;
      }
    }
    return checkedDevice;
  }

  /**
   * Check if the given predicate can be pushed down from FilterNode to ScanNode.
   *
   * <p>The predicate <b>cannot</b> be pushed down if it satisfies the following conditions:
   * <li>predicate contains IS_NULL
   *
   * @param predicate given predicate
   * @return true if the given predicate can be pushed down to source
   */
  public static boolean predicateCanPushDownToSource(Expression predicate) {
    return new PredicateCanPushDownToSourceChecker().process(predicate, null);
  }

  /**
   * Check if the given predicate can be pushed into ScanOperator and execute using the {@link
   * Filter} interface.
   *
   * @param predicate given predicate
   * @return true if the given predicate can be pushed into ScanOperator
   */
  public static boolean predicateCanPushIntoScan(Expression predicate) {
    return new PredicatePushIntoScanChecker().process(predicate, null);
  }

  public static boolean predicateCanPushIntoScan(
      org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression predicate) {
    return new org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate
            .PredicatePushIntoScanChecker()
        .process(predicate, null);
  }
}
