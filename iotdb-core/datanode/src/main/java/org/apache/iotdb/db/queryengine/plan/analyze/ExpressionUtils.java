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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.UnknownExpressionTypeException;
import org.apache.iotdb.db.queryengine.plan.expression.binary.AdditionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.DivisionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.ModuloExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.binary.WhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.queryengine.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.queryengine.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.InExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.queryengine.plan.expression.unary.UnaryExpression;

import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ExpressionUtils {

  private ExpressionUtils() {
    // util class
  }

  /* Use queryContext to record the memory usage of the constructed Expression. */
  public static List<Expression> reconstructTimeSeriesOperandsWithMemoryCheck(
      final TimeSeriesOperand rawExpression,
      final List<? extends PartialPath> actualPaths,
      final MPPQueryContext queryContext) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (PartialPath actualPath : actualPaths) {
      resultExpressions.add(
          reserveMemoryForExpression(
              queryContext, reconstructTimeSeriesOperand(rawExpression, actualPath)));
    }
    return resultExpressions;
  }

  public static Expression reconstructTimeSeriesOperand(
      TimeSeriesOperand rawExpression, PartialPath actualPath) {
    Expression resultExpression = new TimeSeriesOperand(actualPath, rawExpression.getType());
    return cloneCommonFields(rawExpression, resultExpression);
  }

  public static Expression reconstructTimeSeriesOperand(
      TimeSeriesOperand rawExpression, PartialPath actualPath, TSDataType dataType) {
    Expression resultExpression = new TimeSeriesOperand(actualPath, dataType);
    return cloneCommonFields(rawExpression, resultExpression);
  }

  public static List<Expression> reconstructFunctionExpressionsWithMemoryCheck(
      final FunctionExpression expression,
      final List<List<Expression>> childExpressionsList,
      final MPPQueryContext queryContext) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (List<Expression> functionExpressions : childExpressionsList) {
      resultExpressions.add(
          reserveMemoryForExpression(
              queryContext, reconstructFunctionExpression(expression, functionExpressions)));
    }
    return resultExpressions;
  }

  public static Expression reconstructFunctionExpression(
      FunctionExpression rawExpression, List<Expression> childExpressions) {
    Expression resultExpression =
        new FunctionExpression(
            rawExpression.getFunctionName(),
            rawExpression.getFunctionAttributes(),
            childExpressions,
            rawExpression.getCountTimeExpressions());
    return cloneCommonFields(rawExpression, resultExpression);
  }

  public static Expression reconstructFunctionExpressionWithLowerCaseFunctionName(
      FunctionExpression rawExpression, List<Expression> childExpressions) {
    Expression resultExpression =
        new FunctionExpression(
            rawExpression.getFunctionName().toLowerCase(),
            rawExpression.getFunctionAttributes(),
            childExpressions,
            rawExpression.getCountTimeExpressions());
    return cloneCommonFields(rawExpression, resultExpression);
  }

  public static List<Expression> reconstructUnaryExpressionsWithMemoryCheck(
      final UnaryExpression expression,
      final List<Expression> childExpressions,
      final MPPQueryContext queryContext) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (Expression childExpression : childExpressions) {
      resultExpressions.add(
          reserveMemoryForExpression(
              queryContext, reconstructUnaryExpression(expression, childExpression)));
    }
    return resultExpressions;
  }

  public static Expression reconstructCaseWhenThenExpression(
      CaseWhenThenExpression rawExpression, List<Expression> childExpressions) {
    Expression resultExpression =
        new CaseWhenThenExpression(
            childExpressions // transform to List<WhenThenExpression>
                .subList(0, childExpressions.size() - 1)
                .stream()
                .map(WhenThenExpression.class::cast)
                .collect(Collectors.toList()),
            childExpressions.get(childExpressions.size() - 1));
    return cloneCommonFields(rawExpression, resultExpression);
  }

  public static Expression reconstructUnaryExpression(
      UnaryExpression rawExpression, Expression childExpression) {
    Expression resultExpression;
    switch (rawExpression.getExpressionType()) {
      case IS_NULL:
        resultExpression =
            new IsNullExpression(childExpression, ((IsNullExpression) rawExpression).isNot());
        break;
      case IN:
        resultExpression =
            new InExpression(
                childExpression,
                ((InExpression) rawExpression).isNotIn(),
                ((InExpression) rawExpression).getValues());
        break;
      case LIKE:
        resultExpression =
            new LikeExpression(
                childExpression,
                ((LikeExpression) rawExpression).getPatternString(),
                ((LikeExpression) rawExpression).getPattern(),
                ((LikeExpression) rawExpression).isNot());
        break;
      case LOGIC_NOT:
        resultExpression = new LogicNotExpression(childExpression);
        break;
      case NEGATION:
        resultExpression = new NegationExpression(childExpression);
        break;
      case REGEXP:
        resultExpression =
            new RegularExpression(
                childExpression,
                ((RegularExpression) rawExpression).getPatternString(),
                ((RegularExpression) rawExpression).getPattern(),
                ((RegularExpression) rawExpression).isNot());
        break;
      default:
        throw new UnknownExpressionTypeException(rawExpression.getExpressionType());
    }
    return cloneCommonFields(rawExpression, resultExpression);
  }

  public static List<Expression> reconstructBinaryExpressionsWithMemoryCheck(
      final BinaryExpression expression,
      final List<Expression> leftExpressions,
      final List<Expression> rightExpressions,
      final MPPQueryContext queryContext) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (Expression le : leftExpressions) {
      for (Expression re : rightExpressions) {
        resultExpressions.add(
            reserveMemoryForExpression(
                queryContext, reconstructBinaryExpression(expression, le, re)));
      }
    }
    return resultExpressions;
  }

  public static Expression reconstructBinaryExpression(
      Expression rawExpression, Expression leftExpression, Expression rightExpression) {
    Expression resultExpression;
    switch (rawExpression.getExpressionType()) {
      case ADDITION:
        resultExpression = new AdditionExpression(leftExpression, rightExpression);
        break;
      case SUBTRACTION:
        resultExpression = new SubtractionExpression(leftExpression, rightExpression);
        break;
      case MULTIPLICATION:
        resultExpression = new MultiplicationExpression(leftExpression, rightExpression);
        break;
      case DIVISION:
        resultExpression = new DivisionExpression(leftExpression, rightExpression);
        break;
      case MODULO:
        resultExpression = new ModuloExpression(leftExpression, rightExpression);
        break;
      case LESS_THAN:
        resultExpression = new LessThanExpression(leftExpression, rightExpression);
        break;
      case LESS_EQUAL:
        resultExpression = new LessEqualExpression(leftExpression, rightExpression);
        break;
      case GREATER_THAN:
        resultExpression = new GreaterThanExpression(leftExpression, rightExpression);
        break;
      case GREATER_EQUAL:
        resultExpression = new GreaterEqualExpression(leftExpression, rightExpression);
        break;
      case EQUAL_TO:
        resultExpression = new EqualToExpression(leftExpression, rightExpression);
        break;
      case NON_EQUAL:
        resultExpression = new NonEqualExpression(leftExpression, rightExpression);
        break;
      case LOGIC_AND:
        resultExpression = new LogicAndExpression(leftExpression, rightExpression);
        break;
      case LOGIC_OR:
        resultExpression = new LogicOrExpression(leftExpression, rightExpression);
        break;
      case WHEN_THEN:
        resultExpression = new WhenThenExpression(leftExpression, rightExpression);
        break;
      default:
        throw new IllegalArgumentException(
            "unsupported rawExpression type: " + rawExpression.getExpressionType());
    }
    return cloneCommonFields(rawExpression, resultExpression);
  }

  public static List<Expression> reconstructTernaryExpressionsWithMemoryCheck(
      final TernaryExpression expression,
      final List<Expression> firstExpressions,
      final List<Expression> secondExpressions,
      final List<Expression> thirdExpressions,
      final MPPQueryContext queryContext) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (Expression fe : firstExpressions) {
      for (Expression se : secondExpressions)
        for (Expression te : thirdExpressions) {
          resultExpressions.add(
              reserveMemoryForExpression(
                  queryContext, reconstructTernaryExpression(expression, fe, se, te)));
        }
    }
    return resultExpressions;
  }

  public static Expression reconstructTernaryExpression(
      TernaryExpression rawExpression,
      Expression firstExpression,
      Expression secondExpression,
      Expression thirdExpression) {
    Expression resultExpression;
    if (rawExpression.getExpressionType() == ExpressionType.BETWEEN) {
      resultExpression =
          new BetweenExpression(
              firstExpression,
              secondExpression,
              thirdExpression,
              ((BetweenExpression) rawExpression).isNotBetween());
    } else {
      throw new UnknownExpressionTypeException(rawExpression.getExpressionType());
    }
    return cloneCommonFields(rawExpression, resultExpression);
  }

  private static Expression cloneCommonFields(
      Expression rawExpression, Expression resultExpression) {
    resultExpression.setViewPath(rawExpression.getViewPath());
    return resultExpression;
  }

  private static Expression reserveMemoryForExpression(
      MPPQueryContext queryContext, Expression expression) {
    queryContext.reserveMemoryForFrontEnd(expression == null ? 0 : expression.ramBytesUsed());
    return expression;
  }

  /**
   * Make cartesian product. Attention, in this implementation, the way to handle the empty set is
   * to ignore it instead of making the result an empty set.
   *
   * @param dimensionValue source data
   * @param resultList final results
   * @param layer the depth of recursive, dimensionValue[layer] will be processed this time, should
   *     always be 0 while call from outside
   * @param currentList intermediate result, should always be empty while call from outside
   * @param <T> any type
   */
  public static <T> void cartesianProduct(
      List<List<T>> dimensionValue, List<List<T>> resultList, int layer, List<T> currentList) {
    if (layer < dimensionValue.size() - 1) {
      if (dimensionValue.get(layer).isEmpty()) {
        cartesianProduct(dimensionValue, resultList, layer + 1, currentList);
      } else {
        for (int i = 0; i < dimensionValue.get(layer).size(); i++) {
          List<T> list = new ArrayList<>(currentList);
          list.add(dimensionValue.get(layer).get(i));
          cartesianProduct(dimensionValue, resultList, layer + 1, list);
        }
      }
    } else if (layer == dimensionValue.size() - 1) {
      if (dimensionValue.get(layer).isEmpty()) {
        resultList.add(currentList);
      } else {
        for (int i = 0; i < dimensionValue.get(layer).size(); i++) {
          List<T> list = new ArrayList<>(currentList);
          list.add(dimensionValue.get(layer).get(i));
          resultList.add(list);
        }
      }
    }
  }
}
