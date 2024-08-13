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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
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
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.other.GroupByTimeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.utils.DateTimeUtils;

import com.google.common.io.BaseEncoding;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.tsfile.read.filter.operator.Not.CONTAIN_NOT_ERR_MSG;

public class ConvertPredicateToFilterVisitor
    extends PredicateVisitor<Filter, ConvertPredicateToFilterVisitor.Context> {

  private static final ConvertPredicateToTimeFilterVisitor timeFilterConvertor =
      new ConvertPredicateToTimeFilterVisitor();

  @Override
  public Filter visitInExpression(InExpression inExpression, Context context) {
    Expression operand = inExpression.getExpression();
    if (operand.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      return timeFilterConvertor.process(inExpression, null);
    }

    checkArgument(operand.getExpressionType().equals(ExpressionType.TIMESERIES));

    Set<String> stringValues = inExpression.getValues();
    if (stringValues.size() == 1) {
      // rewrite 'value in (1)' to 'value = 1' or 'value not in 1' to 'value != 1'
      Expression rewrittenExpression = rewriteInExpressionToEqual(inExpression, context);
      return process(rewrittenExpression, context);
    }

    PartialPath path = ((TimeSeriesOperand) operand).getPath();
    if (inExpression.isNotIn()) {
      return constructNotInFilter(path, stringValues, context);
    } else {
      return constructInFilter(path, stringValues, context);
    }
  }

  private Expression rewriteInExpressionToEqual(InExpression inExpression, Context context) {
    Set<String> stringValues = inExpression.getValues();
    PartialPath path = ((TimeSeriesOperand) inExpression.getExpression()).getPath();
    if (inExpression.isNotIn()) {
      return new NonEqualExpression(
          inExpression.getExpression(),
          new ConstantOperand(context.getType(path), stringValues.iterator().next()));
    } else {
      return new EqualToExpression(
          inExpression.getExpression(),
          new ConstantOperand(context.getType(path), stringValues.iterator().next()));
    }
  }

  private <T extends Comparable<T>> Filter constructNotInFilter(
      PartialPath path, Set<String> stringValues, Context context) {
    int measurementIndex = context.getMeasurementIndex(path.getMeasurement());
    TSDataType dataType = context.getType(path);
    Set<T> values = constructInSet(stringValues, dataType);
    return ValueFilterApi.notIn(measurementIndex, values, dataType);
  }

  private <T extends Comparable<T>> Filter constructInFilter(
      PartialPath path, Set<String> stringValues, Context context) {
    int measurementIndex = context.getMeasurementIndex(path.getMeasurement());
    TSDataType dataType = context.getType(path);
    Set<T> values = constructInSet(stringValues, dataType);
    return ValueFilterApi.in(measurementIndex, values, dataType);
  }

  private <T extends Comparable<T>> Set<T> constructInSet(
      Set<String> stringValues, TSDataType dataType) {
    Set<T> values = new HashSet<>();
    for (String valueString : stringValues) {
      values.add(getValue(valueString, dataType));
    }
    return values;
  }

  @Override
  public Filter visitIsNullExpression(IsNullExpression isNullExpression, Context context) {
    if (!isNullExpression.isNot()) {
      throw new IllegalArgumentException("IS NULL cannot be pushed down");
    }

    Expression operand = isNullExpression.getExpression();
    if (operand.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      throw new UnsupportedOperationException("TIMESTAMP does not support IS NULL/IS NOT NULL");
    }

    checkArgument(operand.getExpressionType().equals(ExpressionType.TIMESERIES));
    PartialPath path = ((TimeSeriesOperand) operand).getPath();
    int measurementIndex = context.getMeasurementIndex(path.getMeasurement());
    TSDataType dataType = context.getType(path);
    return ValueFilterApi.isNotNull(measurementIndex);
  }

  @Override
  public Filter visitLikeExpression(LikeExpression likeExpression, Context context) {
    Expression operand = likeExpression.getExpression();
    if (operand.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      throw new UnsupportedOperationException("TIMESTAMP does not support LIKE/NOT LIKE");
    }

    checkArgument(operand.getExpressionType().equals(ExpressionType.TIMESERIES));
    PartialPath path = ((TimeSeriesOperand) operand).getPath();
    int measurementIndex = context.getMeasurementIndex(path.getMeasurement());
    TSDataType dataType = context.getType(path);
    if (likeExpression.isNot()) {
      return ValueFilterApi.notLike(measurementIndex, likeExpression.getPattern(), dataType);
    } else {
      return ValueFilterApi.like(measurementIndex, likeExpression.getPattern(), dataType);
    }
  }

  @Override
  public Filter visitRegularExpression(RegularExpression regularExpression, Context context) {
    Expression operand = regularExpression.getExpression();
    if (operand.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      throw new UnsupportedOperationException("TIMESTAMP does not support REGEXP/NOT REGEXP");
    }

    checkArgument(operand.getExpressionType().equals(ExpressionType.TIMESERIES));
    int measurementIndex =
        context.getMeasurementIndex(((TimeSeriesOperand) operand).getPath().getMeasurement());
    PartialPath path = ((TimeSeriesOperand) operand).getPath();
    TSDataType dataType = context.getType(path);
    if (regularExpression.isNot()) {
      return ValueFilterApi.notRegexp(measurementIndex, regularExpression.getPattern(), dataType);
    } else {
      return ValueFilterApi.regexp(measurementIndex, regularExpression.getPattern(), dataType);
    }
  }

  @Override
  public Filter visitLogicNotExpression(LogicNotExpression logicNotExpression, Context context) {
    throw new IllegalArgumentException(CONTAIN_NOT_ERR_MSG);
  }

  @Override
  public Filter visitLogicAndExpression(LogicAndExpression logicAndExpression, Context context) {
    return FilterFactory.and(
        process(logicAndExpression.getLeftExpression(), context),
        process(logicAndExpression.getRightExpression(), context));
  }

  @Override
  public Filter visitLogicOrExpression(LogicOrExpression logicOrExpression, Context context) {
    return FilterFactory.or(
        process(logicOrExpression.getLeftExpression(), context),
        process(logicOrExpression.getRightExpression(), context));
  }

  @Override
  public Filter visitEqualToExpression(EqualToExpression equalToExpression, Context context) {
    return processCompareBinaryExpression(equalToExpression, context);
  }

  @Override
  public Filter visitNonEqualExpression(NonEqualExpression nonEqualExpression, Context context) {
    return processCompareBinaryExpression(nonEqualExpression, context);
  }

  @Override
  public Filter visitGreaterThanExpression(
      GreaterThanExpression greaterThanExpression, Context context) {
    return processCompareBinaryExpression(greaterThanExpression, context);
  }

  @Override
  public Filter visitGreaterEqualExpression(
      GreaterEqualExpression greaterEqualExpression, Context context) {
    return processCompareBinaryExpression(greaterEqualExpression, context);
  }

  @Override
  public Filter visitLessThanExpression(LessThanExpression lessThanExpression, Context context) {
    return processCompareBinaryExpression(lessThanExpression, context);
  }

  @Override
  public Filter visitLessEqualExpression(LessEqualExpression lessEqualExpression, Context context) {
    return processCompareBinaryExpression(lessEqualExpression, context);
  }

  private Filter processCompareBinaryExpression(
      CompareBinaryExpression comparisonExpression, Context context) {
    Expression leftExpression = comparisonExpression.getLeftExpression();
    Expression rightExpression = comparisonExpression.getRightExpression();
    if (leftExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)
        || rightExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      return timeFilterConvertor.process(comparisonExpression, null);
    }

    ExpressionType expressionType = comparisonExpression.getExpressionType();
    if (rightExpression.getExpressionType().equals(ExpressionType.CONSTANT)) {
      return constructCompareFilter(expressionType, leftExpression, rightExpression, context);
    } else {
      return constructCompareFilter(expressionType, rightExpression, leftExpression, context);
    }
  }

  private <T extends Comparable<T>> Filter constructCompareFilter(
      ExpressionType expressionType,
      Expression timeseriesOperand,
      Expression constantOperand,
      Context context) {
    PartialPath path = ((TimeSeriesOperand) timeseriesOperand).getPath();
    int measurementIndex = context.getMeasurementIndex(path.getMeasurement());
    TSDataType dataType = context.getType(path);
    T value = getValue(((ConstantOperand) constantOperand).getValueString(), dataType);
    switch (expressionType) {
      case EQUAL_TO:
        return ValueFilterApi.eq(measurementIndex, value, dataType);
      case NON_EQUAL:
        return ValueFilterApi.notEq(measurementIndex, value, dataType);
      case GREATER_THAN:
        return ValueFilterApi.gt(measurementIndex, value, dataType);
      case GREATER_EQUAL:
        return ValueFilterApi.gtEq(measurementIndex, value, dataType);
      case LESS_THAN:
        return ValueFilterApi.lt(measurementIndex, value, dataType);
      case LESS_EQUAL:
        return ValueFilterApi.ltEq(measurementIndex, value, dataType);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported expression type %s", expressionType));
    }
  }

  @Override
  public Filter visitBetweenExpression(BetweenExpression betweenExpression, Context context) {
    Expression firstExpression = betweenExpression.getFirstExpression();
    Expression secondExpression = betweenExpression.getSecondExpression();
    Expression thirdExpression = betweenExpression.getThirdExpression();

    if (firstExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)
        || secondExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)
        || thirdExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
      return timeFilterConvertor.process(betweenExpression, null);
    }

    boolean isNot = betweenExpression.isNotBetween();

    if (firstExpression.getExpressionType().equals(ExpressionType.TIMESERIES)) {
      // firstExpression is TIMESERIES
      return constructBetweenFilter(
          firstExpression, secondExpression, thirdExpression, isNot, context);
    } else if (secondExpression.getExpressionType().equals(ExpressionType.TIMESERIES)) {
      // secondExpression is TIMESERIES
      return isNot
          ? constructCompareFilter(
              ExpressionType.GREATER_THAN, secondExpression, firstExpression, context)
          : constructCompareFilter(
              ExpressionType.LESS_EQUAL, secondExpression, firstExpression, context);
    }

    // thirdExpression is TIMESERIES
    return isNot
        ? constructCompareFilter(
            ExpressionType.LESS_THAN, thirdExpression, firstExpression, context)
        : constructCompareFilter(
            ExpressionType.GREATER_EQUAL, thirdExpression, firstExpression, context);
  }

  private <T extends Comparable<T>> Filter constructBetweenFilter(
      Expression timeseriesOperand,
      Expression minValueConstantOperand,
      Expression maxValueConstantOperand,
      boolean isNot,
      Context context) {
    PartialPath path = ((TimeSeriesOperand) timeseriesOperand).getPath();
    int measurementIndex = context.getMeasurementIndex(path.getMeasurement());
    TSDataType dataType = context.getType(path);

    T minValue = getValue(((ConstantOperand) minValueConstantOperand).getValueString(), dataType);
    T maxValue = getValue(((ConstantOperand) maxValueConstantOperand).getValueString(), dataType);

    if (minValue == maxValue) {
      return isNot
          ? ValueFilterApi.notEq(measurementIndex, minValue, dataType)
          : ValueFilterApi.eq(measurementIndex, minValue, dataType);
    }
    return isNot
        ? ValueFilterApi.notBetween(measurementIndex, minValue, maxValue, dataType)
        : ValueFilterApi.between(measurementIndex, minValue, maxValue, dataType);
  }

  @SuppressWarnings("unchecked")
  public static <T extends Comparable<T>> T getValue(String valueString, TSDataType dataType) {
    try {
      switch (dataType) {
        case INT32:
          return (T) Integer.valueOf(valueString);
        case INT64:
        case TIMESTAMP:
          return (T) Long.valueOf(valueString);
        case FLOAT:
          return (T) Float.valueOf(valueString);
        case DOUBLE:
          return (T) Double.valueOf(valueString);
        case BOOLEAN:
          if (valueString.equalsIgnoreCase("true")) {
            return (T) Boolean.TRUE;
          } else if (valueString.equalsIgnoreCase("false")) {
            return (T) Boolean.FALSE;
          } else {
            throw new IllegalArgumentException(
                String.format("\"%s\" cannot be cast to [%s]", valueString, dataType));
          }
        case BLOB:
          return (T) new Binary(BaseEncoding.base16().decode(valueString));
        case TEXT:
        case STRING:
          return (T) new Binary(valueString, TSFileConfig.STRING_CHARSET);
        case DATE:
          return (T) DateTimeUtils.parseDateExpressionToInt(valueString);
        default:
          throw new UnsupportedOperationException(
              String.format("Unsupported data type %s", dataType));
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("\"%s\" cannot be cast to [%s]", valueString, dataType));
    }
  }

  @Override
  public Filter visitGroupByTimeExpression(
      GroupByTimeExpression groupByTimeExpression, Context context) {
    throw new IllegalArgumentException("GroupByTime filter cannot exist in value filter.");
  }

  public static class Context {

    private final List<String> allMeasurements;
    private final boolean isBuildPlanUseTemplate;
    private final TypeProvider typeProvider;
    private final ZoneId zoneId;
    private Map<String, IMeasurementSchema> schemaMap;

    public Context(
        List<String> allMeasurements,
        boolean isBuildPlanUseTemplate,
        TypeProvider typeProvider,
        ZoneId zoneId) {
      this.allMeasurements = allMeasurements;
      this.isBuildPlanUseTemplate = isBuildPlanUseTemplate;
      this.typeProvider = typeProvider;
      this.zoneId = zoneId;
      if (isBuildPlanUseTemplate) {
        this.schemaMap = typeProvider.getTemplatedInfo().getSchemaMap();
      }
    }

    public int getMeasurementIndex(String measurement) {
      int measurementIndex = allMeasurements.indexOf(measurement);
      if (measurementIndex == -1) {
        throw new IllegalArgumentException(
            String.format("Measurement %s does not exist", measurement));
      }
      return measurementIndex;
    }

    public TSDataType getType(PartialPath path) {
      if (isBuildPlanUseTemplate) {
        return schemaMap.get(path.getFullPath()).getType();
      } else {
        return typeProvider.getTreeModelType(path.getFullPath());
      }
    }

    public ZoneId getZoneId() {
      return zoneId;
    }
  }
}
