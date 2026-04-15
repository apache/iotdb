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

package org.apache.iotdb.db.queryengine.plan.expression.visitor.predicate;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.binary.CompareBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.other.GroupByTimeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.utils.TimeDuration;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.plan.expression.binary.CompareBinaryExpression.flipType;
import static org.apache.tsfile.read.filter.operator.Not.CONTAIN_NOT_ERR_MSG;

public class ConvertPredicateToTimeFilterVisitor extends PredicateVisitor<Filter, Void> {

  @Override
  public Filter visitInExpression(InExpression inExpression, Void context) {
    Expression expression = inExpression.getExpression();
    checkArgument(expression.getExpressionType().equals(ExpressionType.TIMESTAMP));

    Set<String> values = inExpression.getValues();
    if (values.size() == 1) {
      // rewrite 'time in (1)' to 'time = 1' or 'time not in 1' to 'time != 1'
      Expression rewrittenExpression = rewriteInExpressionToEqual(inExpression);
      return rewrittenExpression.accept(this, context);
    }

    Set<Long> longValues = new LinkedHashSet<>();
    for (String value : values) {
      longValues.add(Long.parseLong(value));
    }
    return inExpression.isNotIn() ? TimeFilterApi.notIn(longValues) : TimeFilterApi.in(longValues);
  }

  private Expression rewriteInExpressionToEqual(InExpression inExpression) {
    Set<String> values = inExpression.getValues();
    if (inExpression.isNotIn()) {
      return new NonEqualExpression(
          inExpression.getExpression(),
          new ConstantOperand(TSDataType.INT64, values.iterator().next()));
    } else {
      return new EqualToExpression(
          inExpression.getExpression(),
          new ConstantOperand(TSDataType.INT64, values.iterator().next()));
    }
  }

  @Override
  public Filter visitIsNullExpression(IsNullExpression isNullExpression, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not support IS NULL/IS NOT NULL");
  }

  @Override
  public Filter visitLikeExpression(LikeExpression likeExpression, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not support LIKE/NOT LIKE");
  }

  @Override
  public Filter visitRegularExpression(RegularExpression regularExpression, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not support REGEXP/NOT REGEXP");
  }

  @Override
  public Filter visitLogicNotExpression(LogicNotExpression logicNotExpression, Void context) {
    throw new IllegalArgumentException(CONTAIN_NOT_ERR_MSG);
  }

  @Override
  public Filter visitLogicAndExpression(LogicAndExpression logicAndExpression, Void context) {
    return FilterFactory.and(
        logicAndExpression.getLeftExpression().accept(this, context),
        logicAndExpression.getRightExpression().accept(this, context));
  }

  @Override
  public Filter visitLogicOrExpression(LogicOrExpression logicOrExpression, Void context) {
    return FilterFactory.or(
        logicOrExpression.getLeftExpression().accept(this, context),
        logicOrExpression.getRightExpression().accept(this, context));
  }

  @Override
  public Filter visitEqualToExpression(EqualToExpression equalToExpression, Void context) {
    return visitCompareBinaryExpression(equalToExpression, context);
  }

  @Override
  public Filter visitNonEqualExpression(NonEqualExpression nonEqualExpression, Void context) {
    return visitCompareBinaryExpression(nonEqualExpression, context);
  }

  @Override
  public Filter visitGreaterThanExpression(
      GreaterThanExpression greaterThanExpression, Void context) {
    return visitCompareBinaryExpression(greaterThanExpression, context);
  }

  @Override
  public Filter visitGreaterEqualExpression(
      GreaterEqualExpression greaterEqualExpression, Void context) {
    return visitCompareBinaryExpression(greaterEqualExpression, context);
  }

  @Override
  public Filter visitLessThanExpression(LessThanExpression lessThanExpression, Void context) {
    return visitCompareBinaryExpression(lessThanExpression, context);
  }

  @Override
  public Filter visitLessEqualExpression(LessEqualExpression lessEqualExpression, Void context) {
    return visitCompareBinaryExpression(lessEqualExpression, context);
  }

  @Override
  public Filter visitCompareBinaryExpression(
      CompareBinaryExpression comparisonExpression, Void context) {
    Expression leftExpression = comparisonExpression.getLeftExpression();
    Expression rightExpression = comparisonExpression.getRightExpression();
    ExpressionType expressionType = comparisonExpression.getExpressionType();

    if (leftExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      return constructCompareFilter(expressionType, rightExpression);
    } else {
      return constructCompareFilter(flipType(expressionType), leftExpression);
    }
  }

  private Filter constructCompareFilter(
      ExpressionType expressionType, Expression constantExpression) {
    long value = Long.parseLong(((ConstantOperand) constantExpression).getValueString());
    switch (expressionType) {
      case EQUAL_TO:
        return TimeFilterApi.eq(value);
      case NON_EQUAL:
        return TimeFilterApi.notEq(value);
      case GREATER_THAN:
        return TimeFilterApi.gt(value);
      case GREATER_EQUAL:
        return TimeFilterApi.gtEq(value);
      case LESS_THAN:
        return TimeFilterApi.lt(value);
      case LESS_EQUAL:
        return TimeFilterApi.ltEq(value);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported expression type %s", expressionType));
    }
  }

  @Override
  public Filter visitBetweenExpression(BetweenExpression betweenExpression, Void context) {
    Expression firstExpression = betweenExpression.getFirstExpression();
    Expression secondExpression = betweenExpression.getSecondExpression();
    Expression thirdExpression = betweenExpression.getThirdExpression();
    boolean isNot = betweenExpression.isNotBetween();

    if (firstExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      // firstExpression is TIMESTAMP
      long minValue = Long.parseLong(((ConstantOperand) secondExpression).getValueString());
      long maxValue = Long.parseLong(((ConstantOperand) thirdExpression).getValueString());

      if (minValue == maxValue) {
        return isNot ? TimeFilterApi.notEq(minValue) : TimeFilterApi.eq(minValue);
      }
      return isNot
          ? TimeFilterApi.notBetween(minValue, maxValue)
          : TimeFilterApi.between(minValue, maxValue);
    } else if (secondExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      // secondExpression is TIMESTAMP
      long value = Long.parseLong(((ConstantOperand) firstExpression).getValueString());
      long maxValue = Long.parseLong(((ConstantOperand) thirdExpression).getValueString());

      // cases:
      // 1 BETWEEN time AND 2 => time <= 1
      // 1 BETWEEN time AND 1 => time <= 1
      // 1 BETWEEN time AND 0 => FALSE
      // 1 NOT BETWEEN time AND 2 => time > 1
      // 1 NOT BETWEEN time AND 1 => time > 1
      // 1 NOT BETWEEN time AND 0 => TRUE
      checkArgument(
          value <= maxValue,
          String.format("Predicate [%s] should be simplified in previous step", betweenExpression));
      return isNot ? TimeFilterApi.gt(value) : TimeFilterApi.ltEq(value);
    }

    // thirdExpression is TIMESTAMP
    long value = Long.parseLong(((ConstantOperand) firstExpression).getValueString());
    long minValue = Long.parseLong(((ConstantOperand) secondExpression).getValueString());

    // cases:
    // 1 BETWEEN 2 AND time => FALSE
    // 1 BETWEEN 1 AND time => time >= 1
    // 1 BETWEEN 0 AND time => time >= 1
    // 1 NOT BETWEEN 2 AND time => TRUE
    // 1 NOT BETWEEN 1 AND time => time < 1
    // 1 NOT BETWEEN 0 AND time => time < 1
    checkArgument(
        value >= minValue,
        String.format("Predicate [%s] should be simplified in previous step", betweenExpression));
    return isNot ? TimeFilterApi.lt(value) : TimeFilterApi.gtEq(value);
  }

  @Override
  public Filter visitGroupByTimeExpression(
      GroupByTimeExpression groupByTimeExpression, Void context) {
    long startTime = groupByTimeExpression.getStartTime();
    long endTime = groupByTimeExpression.getEndTime();
    TimeDuration interval = groupByTimeExpression.getInterval();
    TimeDuration slidingStep = groupByTimeExpression.getSlidingStep();

    if (interval.containsMonth() || slidingStep.containsMonth()) {
      return TimeFilterApi.groupByMonth(
          startTime,
          endTime,
          interval,
          slidingStep,
          TimeZone.getTimeZone("+00:00"),
          TimestampPrecisionUtils.currPrecision);
    } else {
      return TimeFilterApi.groupBy(
          startTime, endTime, interval.nonMonthDuration, slidingStep.nonMonthDuration);
    }
  }
}
