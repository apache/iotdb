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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilter;
import org.apache.iotdb.tsfile.utils.TimeDuration;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.tsfile.read.filter.operator.Not.CONTAIN_NOT_ERR_MSG;

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
    return inExpression.isNotIn() ? TimeFilter.notIn(longValues) : TimeFilter.in(longValues);
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
    throw new UnsupportedOperationException("TIMESTAMP does not support IS NULL");
  }

  @Override
  public Filter visitLikeExpression(LikeExpression likeExpression, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not support LIKE");
  }

  @Override
  public Filter visitRegularExpression(RegularExpression regularExpression, Void context) {
    throw new UnsupportedOperationException("TIMESTAMP does not support REGEXP");
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
    return constructCompareFilter(
        equalToExpression.getExpressionType(),
        equalToExpression.getLeftExpression(),
        equalToExpression.getRightExpression());
  }

  @Override
  public Filter visitNonEqualExpression(NonEqualExpression nonEqualExpression, Void context) {
    return constructCompareFilter(
        nonEqualExpression.getExpressionType(),
        nonEqualExpression.getLeftExpression(),
        nonEqualExpression.getRightExpression());
  }

  @Override
  public Filter visitGreaterThanExpression(
      GreaterThanExpression greaterThanExpression, Void context) {
    return constructCompareFilter(
        greaterThanExpression.getExpressionType(),
        greaterThanExpression.getLeftExpression(),
        greaterThanExpression.getRightExpression());
  }

  @Override
  public Filter visitGreaterEqualExpression(
      GreaterEqualExpression greaterEqualExpression, Void context) {
    return constructCompareFilter(
        greaterEqualExpression.getExpressionType(),
        greaterEqualExpression.getLeftExpression(),
        greaterEqualExpression.getRightExpression());
  }

  @Override
  public Filter visitLessThanExpression(LessThanExpression lessThanExpression, Void context) {
    return constructCompareFilter(
        lessThanExpression.getExpressionType(),
        lessThanExpression.getLeftExpression(),
        lessThanExpression.getRightExpression());
  }

  @Override
  public Filter visitLessEqualExpression(LessEqualExpression lessEqualExpression, Void context) {
    return constructCompareFilter(
        lessEqualExpression.getExpressionType(),
        lessEqualExpression.getLeftExpression(),
        lessEqualExpression.getRightExpression());
  }

  private Filter constructCompareFilter(
      ExpressionType expressionType, Expression leftExpression, Expression rightExpression) {
    if (leftExpression.getExpressionType().equals(ExpressionType.CONSTANT)) {
      return constructCompareFilter(expressionType, rightExpression, leftExpression);
    }

    long value = Long.parseLong(((ConstantOperand) rightExpression).getValueString());

    switch (expressionType) {
      case EQUAL_TO:
        return TimeFilter.eq(value);
      case NON_EQUAL:
        return TimeFilter.notEq(value);
      case GREATER_THAN:
        return TimeFilter.gt(value);
      case GREATER_EQUAL:
        return TimeFilter.gtEq(value);
      case LESS_THAN:
        return TimeFilter.lt(value);
      case LESS_EQUAL:
        return TimeFilter.ltEq(value);
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
        return isNot ? TimeFilter.notEq(minValue) : TimeFilter.eq(minValue);
      }
      return isNot
          ? TimeFilter.notBetween(minValue, maxValue)
          : TimeFilter.between(minValue, maxValue);
    } else if (secondExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      // secondExpression is TIMESTAMP
      long value = Long.parseLong(((ConstantOperand) firstExpression).getValueString());
      return isNot ? TimeFilter.gt(value) : TimeFilter.ltEq(value);
    }

    // thirdExpression is TIMESTAMP
    long value = Long.parseLong(((ConstantOperand) firstExpression).getValueString());
    return isNot ? TimeFilter.lt(value) : TimeFilter.gtEq(value);
  }

  @Override
  public Filter visitGroupByTimeExpression(
      GroupByTimeExpression groupByTimeExpression, Void context) {
    long startTime = groupByTimeExpression.getStartTime();
    long endTime = groupByTimeExpression.getEndTime();
    TimeDuration interval = groupByTimeExpression.getInterval();
    TimeDuration slidingStep = groupByTimeExpression.getSlidingStep();

    if (slidingStep.compareTo(interval) <= 0) {
      // slidingStep <= interval, full time range
      return TimeFilter.between(startTime, endTime);
    } else {
      if (interval.containsMonth() || slidingStep.containsMonth()) {
        return TimeFilter.groupByMonth(
            startTime,
            endTime,
            interval,
            slidingStep,
            TimeZone.getTimeZone("+00:00"),
            TimestampPrecisionUtils.currPrecision);
      } else {
        return TimeFilter.groupBy(
            startTime, endTime, interval.nonMonthDuration, slidingStep.nonMonthDuration);
      }
    }
  }
}
