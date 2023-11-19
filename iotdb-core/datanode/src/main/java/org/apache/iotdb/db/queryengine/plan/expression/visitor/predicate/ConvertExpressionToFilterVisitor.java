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

import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
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
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.other.GroupByTimeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.factory.ValueFilter;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.apache.iotdb.tsfile.read.filter.operator.Not.CONTAIN_NOT_ERR_MSG;

/**
 * The supplied predicate expression should also have already been run through {@link
 * SchemaCompatibilityValidator} to make sure it is compatible with the schema.
 */
public class ConvertExpressionToFilterVisitor extends PredicateVisitor<Filter, TypeProvider> {

  @Override
  public Filter visitInExpression(InExpression inExpression, TypeProvider types) {
    Expression expression = inExpression.getExpression();
    TSDataType dataType = types.getType(expression.toString());

    Set<String> values = inExpression.getValues();
    if (values.size() == 1) {
      // rewrite 'a in (1)' to 'a = 1' or 'a not in 1' to 'a != 1'
      Expression rewrittenExpression = rewriteInExpressionToEqual(inExpression, dataType);
      return rewrittenExpression.accept(this, types);
    }

    switch (dataType) {
      case INT32:
        Set<Integer> intValues = new LinkedHashSet<>();
        for (String value : values) {
          intValues.add(Integer.parseInt(value));
        }
        return constructInFilter(inExpression, intValues);
      case INT64:
        Set<Long> longValues = new LinkedHashSet<>();
        for (String value : values) {
          longValues.add(Long.parseLong(value));
        }
        return constructInFilter(inExpression, longValues);
      case FLOAT:
        Set<Float> floatValues = new LinkedHashSet<>();
        for (String value : values) {
          floatValues.add(Float.parseFloat(value));
        }
        return constructInFilter(inExpression, floatValues);
      case DOUBLE:
        Set<Double> doubleValues = new LinkedHashSet<>();
        for (String value : values) {
          doubleValues.add(Double.parseDouble(value));
        }
        return constructInFilter(inExpression, doubleValues);
      case BOOLEAN:
        Set<Boolean> booleanValues = new LinkedHashSet<>();
        for (String value : values) {
          booleanValues.add(Boolean.parseBoolean(value));
        }
        return constructInFilter(inExpression, booleanValues);
      case TEXT:
        return constructInFilter(inExpression, values);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported data type %s", dataType));
    }
  }

  @Override
  public Filter visitGroupByTimeExpression(
      GroupByTimeExpression groupByTimeExpression, TypeProvider context) {
    throw new UnsupportedOperationException("FixedIntervalMultiRangeExpression is not supported");
  }

  private Expression rewriteInExpressionToEqual(InExpression inExpression, TSDataType dataType) {
    Set<String> values = inExpression.getValues();
    if (inExpression.isNotIn()) {
      return new NonEqualExpression(
          inExpression.getExpression(), new ConstantOperand(dataType, values.iterator().next()));
    } else {
      return new EqualToExpression(
          inExpression.getExpression(), new ConstantOperand(dataType, values.iterator().next()));
    }
  }

  private <T extends Comparable<T>> Filter constructInFilter(
      InExpression inExpression, Set<T> values) {
    boolean isNot = inExpression.isNotIn();
    Expression expression = inExpression.getExpression();

    if (expression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      return isNot ? TimeFilter.in((Set<Long>) values) : TimeFilter.notIn((Set<Long>) values);
    } else if (expression.getExpressionType().equals(ExpressionType.TIMESERIES)) {
      String measurement = ((TimeSeriesOperand) expression).getPath().getMeasurement();
      return isNot ? ValueFilter.in(measurement, values) : ValueFilter.notIn(measurement, values);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Filter visitIsNullExpression(IsNullExpression isNullExpression, TypeProvider types) {
    Expression expression = isNullExpression.getExpression();
    if (expression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      throw new UnsupportedOperationException("TIMESTAMP does not support IS NULL");
    }

    String measurement = ((TimeSeriesOperand) expression).getPath().getMeasurement();
    boolean isNot = isNullExpression.isNot();
    return isNot ? ValueFilter.notEq(measurement, null) : ValueFilter.eq(measurement, null);
  }

  @Override
  public Filter visitLikeExpression(LikeExpression likeExpression, TypeProvider types) {
    Expression expression = likeExpression.getExpression();
    if (expression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      throw new UnsupportedOperationException("TIMESTAMP does not support LIKE");
    }

    String measurement = ((TimeSeriesOperand) expression).getPath().getMeasurement();
    String likePattern = likeExpression.getPatternString();
    boolean isNot = likeExpression.isNot();
    return isNot
        ? ValueFilter.notLike(measurement, likePattern)
        : ValueFilter.like(measurement, likePattern);
  }

  @Override
  public Filter visitRegularExpression(RegularExpression regularExpression, TypeProvider types) {
    Expression expression = regularExpression.getExpression();
    if (expression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      throw new UnsupportedOperationException("TIMESTAMP does not support REGEXP");
    }

    String measurement = ((TimeSeriesOperand) expression).getPath().getMeasurement();
    String regex = regularExpression.getPatternString();
    boolean isNot = regularExpression.isNot();
    return isNot
        ? ValueFilter.notRegexp(measurement, regex)
        : ValueFilter.regexp(measurement, regex);
  }

  @Override
  public Filter visitLogicNotExpression(LogicNotExpression logicNotExpression, TypeProvider types) {
    throw new IllegalArgumentException(CONTAIN_NOT_ERR_MSG);
  }

  @Override
  public Filter visitLogicAndExpression(LogicAndExpression logicAndExpression, TypeProvider types) {
    return FilterFactory.and(
        logicAndExpression.getLeftExpression().accept(this, types),
        logicAndExpression.getRightExpression().accept(this, types));
  }

  @Override
  public Filter visitLogicOrExpression(LogicOrExpression logicOrExpression, TypeProvider types) {
    return FilterFactory.or(
        logicOrExpression.getLeftExpression().accept(this, types),
        logicOrExpression.getRightExpression().accept(this, types));
  }

  @Override
  public Filter visitEqualToExpression(EqualToExpression equalToExpression, TypeProvider types) {
    return constructCompareFilter(
        equalToExpression.getExpressionType(),
        equalToExpression.getLeftExpression(),
        equalToExpression.getRightExpression(),
        types);
  }

  @Override
  public Filter visitNonEqualExpression(NonEqualExpression nonEqualExpression, TypeProvider types) {
    return constructCompareFilter(
        nonEqualExpression.getExpressionType(),
        nonEqualExpression.getLeftExpression(),
        nonEqualExpression.getRightExpression(),
        types);
  }

  @Override
  public Filter visitGreaterThanExpression(
      GreaterThanExpression greaterThanExpression, TypeProvider types) {
    return constructCompareFilter(
        greaterThanExpression.getExpressionType(),
        greaterThanExpression.getLeftExpression(),
        greaterThanExpression.getRightExpression(),
        types);
  }

  @Override
  public Filter visitGreaterEqualExpression(
      GreaterEqualExpression greaterEqualExpression, TypeProvider types) {
    return constructCompareFilter(
        greaterEqualExpression.getExpressionType(),
        greaterEqualExpression.getLeftExpression(),
        greaterEqualExpression.getRightExpression(),
        types);
  }

  @Override
  public Filter visitLessThanExpression(LessThanExpression lessThanExpression, TypeProvider types) {
    return constructCompareFilter(
        lessThanExpression.getExpressionType(),
        lessThanExpression.getLeftExpression(),
        lessThanExpression.getRightExpression(),
        types);
  }

  @Override
  public Filter visitLessEqualExpression(
      LessEqualExpression lessEqualExpression, TypeProvider types) {
    return constructCompareFilter(
        lessEqualExpression.getExpressionType(),
        lessEqualExpression.getLeftExpression(),
        lessEqualExpression.getRightExpression(),
        types);
  }

  private Filter constructCompareFilter(
      ExpressionType expressionType,
      Expression leftExpression,
      Expression rightExpression,
      TypeProvider types) {
    if (leftExpression.getExpressionType().equals(ExpressionType.CONSTANT)) {
      return constructCompareFilter(expressionType, rightExpression, leftExpression, types);
    }

    TSDataType dataType = types.getType(leftExpression.toString());
    String valueString = ((ConstantOperand) rightExpression).getValueString();

    if (leftExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      return constructTimeCompareFilter(expressionType, valueString);
    }

    String measurement = ((TimeSeriesOperand) leftExpression).getPath().getMeasurement();
    switch (dataType) {
      case INT32:
        return constructValueCompareFilter(
            expressionType, measurement, Integer.parseInt(valueString));
      case INT64:
        return constructValueCompareFilter(
            expressionType, measurement, Long.parseLong(valueString));
      case FLOAT:
        return constructValueCompareFilter(
            expressionType, measurement, Float.parseFloat(valueString));
      case DOUBLE:
        return constructValueCompareFilter(
            expressionType, measurement, Double.parseDouble(valueString));
      case BOOLEAN:
        return constructValueCompareFilter(
            expressionType, measurement, Boolean.parseBoolean(valueString));
      case TEXT:
        return constructValueCompareFilter(expressionType, measurement, valueString);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported data type %s", dataType));
    }
  }

  private Filter constructTimeCompareFilter(ExpressionType expressionType, String valueString) {
    switch (expressionType) {
      case EQUAL_TO:
        return TimeFilter.eq(Long.parseLong(valueString));
      case NON_EQUAL:
        return TimeFilter.notEq(Long.parseLong(valueString));
      case GREATER_THAN:
        return TimeFilter.gt(Long.parseLong(valueString));
      case GREATER_EQUAL:
        return TimeFilter.gtEq(Long.parseLong(valueString));
      case LESS_THAN:
        return TimeFilter.lt(Long.parseLong(valueString));
      case LESS_EQUAL:
        return TimeFilter.ltEq(Long.parseLong(valueString));
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported expression type %s", expressionType));
    }
  }

  private <T extends Comparable<T>> Filter constructValueCompareFilter(
      ExpressionType expressionType, String measurement, T value) {
    switch (expressionType) {
      case EQUAL_TO:
        return ValueFilter.eq(measurement, value);
      case NON_EQUAL:
        return ValueFilter.notEq(measurement, value);
      case GREATER_THAN:
        return ValueFilter.gt(measurement, value);
      case GREATER_EQUAL:
        return ValueFilter.gtEq(measurement, value);
      case LESS_THAN:
        return ValueFilter.lt(measurement, value);
      case LESS_EQUAL:
        return ValueFilter.ltEq(measurement, value);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported expression type %s", expressionType));
    }
  }

  @Override
  public Filter visitBetweenExpression(BetweenExpression betweenExpression, TypeProvider types) {
    Expression firstExpression = betweenExpression.getFirstExpression();
    Expression secondExpression = betweenExpression.getSecondExpression();
    Expression thirdExpression = betweenExpression.getThirdExpression();
    boolean isNot = betweenExpression.isNotBetween();

    if (!(secondExpression instanceof ConstantOperand)
        || !(thirdExpression instanceof ConstantOperand)) {
      throw new IllegalArgumentException(
          "The second and third parameters of BETWEEN must be constant");
    }
    String minValue = ((ConstantOperand) secondExpression).getValueString();
    String maxValue = ((ConstantOperand) thirdExpression).getValueString();

    if (firstExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      return isNot
          ? TimeFilter.notBetween(Long.parseLong(minValue), Long.parseLong(maxValue))
          : TimeFilter.between(Long.parseLong(minValue), Long.parseLong(maxValue));
    }

    String measurement = ((TimeSeriesOperand) firstExpression).getPath().getMeasurement();
    TSDataType dataType = types.getType(firstExpression.toString());
    switch (dataType) {
      case INT32:
        return isNot
            ? ValueFilter.notBetween(
                measurement, Integer.parseInt(minValue), Integer.parseInt(maxValue))
            : ValueFilter.between(
                measurement, Integer.parseInt(minValue), Integer.parseInt(maxValue));
      case INT64:
        return isNot
            ? ValueFilter.notBetween(
                measurement, Long.parseLong(minValue), Long.parseLong(maxValue))
            : ValueFilter.between(measurement, Long.parseLong(minValue), Long.parseLong(maxValue));
      case FLOAT:
        return isNot
            ? ValueFilter.notBetween(
                measurement, Float.parseFloat(minValue), Float.parseFloat(maxValue))
            : ValueFilter.between(
                measurement, Float.parseFloat(minValue), Float.parseFloat(maxValue));
      case DOUBLE:
        return isNot
            ? ValueFilter.notBetween(
                measurement, Double.parseDouble(minValue), Double.parseDouble(maxValue))
            : ValueFilter.between(
                measurement, Double.parseDouble(minValue), Double.parseDouble(maxValue));
      case BOOLEAN:
        return isNot
            ? ValueFilter.notBetween(
                measurement, Boolean.parseBoolean(minValue), Boolean.parseBoolean(maxValue))
            : ValueFilter.between(
                measurement, Boolean.parseBoolean(minValue), Boolean.parseBoolean(maxValue));
      case TEXT:
        return isNot
            ? ValueFilter.notBetween(measurement, minValue, maxValue)
            : ValueFilter.between(measurement, minValue, maxValue);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported data type %s", dataType));
    }
  }
}
