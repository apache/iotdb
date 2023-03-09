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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.binary.AdditionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.DivisionExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.EqualToExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LessThanExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.ModuloExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.mpp.plan.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LikeExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.RegularExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class ExpressionUtils {
  public static List<Expression> reconstructTimeSeriesOperands(
      List<? extends PartialPath> actualPaths) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (PartialPath actualPath : actualPaths) {
      resultExpressions.add(new TimeSeriesOperand(actualPath));
    }
    return resultExpressions;
  }

  public static Expression reconstructTimeSeriesOperand(PartialPath actualPath) {
    return new TimeSeriesOperand(actualPath);
  }

  public static List<Expression> reconstructFunctionExpressions(
      FunctionExpression expression, List<List<Expression>> childExpressionsList) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (List<Expression> functionExpressions : childExpressionsList) {
      resultExpressions.add(
          new FunctionExpression(
              expression.getFunctionName(),
              expression.getFunctionAttributes(),
              functionExpressions));
    }
    return resultExpressions;
  }

  public static Expression reconstructFunctionExpression(
      FunctionExpression expression, List<Expression> childExpressions) {
    return new FunctionExpression(
        expression.getFunctionName(), expression.getFunctionAttributes(), childExpressions);
  }

  public static List<Expression> reconstructUnaryExpressions(
      UnaryExpression expression, List<Expression> childExpressions) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (Expression childExpression : childExpressions) {
      resultExpressions.add(reconstructUnaryExpression(expression, childExpression));
    }
    return resultExpressions;
  }

  public static Expression reconstructUnaryExpression(
      UnaryExpression expression, Expression childExpression) {
    switch (expression.getExpressionType()) {
      case IS_NULL:
        return new IsNullExpression(childExpression, ((IsNullExpression) expression).isNot());
      case IN:
        return new InExpression(
            childExpression,
            ((InExpression) expression).isNotIn(),
            ((InExpression) expression).getValues());
      case LIKE:
        return new LikeExpression(
            childExpression,
            ((LikeExpression) expression).getPatternString(),
            ((LikeExpression) expression).getPattern());
      case LOGIC_NOT:
        return new LogicNotExpression(childExpression);

      case NEGATION:
        return new NegationExpression(childExpression);

      case REGEXP:
        return new RegularExpression(
            childExpression,
            ((RegularExpression) expression).getPatternString(),
            ((RegularExpression) expression).getPattern());

      default:
        throw new IllegalArgumentException(
            "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static List<Expression> reconstructBinaryExpressions(
      ExpressionType expressionType,
      List<Expression> leftExpressions,
      List<Expression> rightExpressions) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (Expression le : leftExpressions) {
      for (Expression re : rightExpressions) {
        resultExpressions.add(reconstructBinaryExpression(expressionType, le, re));
      }
    }
    return resultExpressions;
  }

  public static Expression reconstructBinaryExpression(
      ExpressionType expressionType, Expression leftExpression, Expression rightExpression) {
    switch (expressionType) {
      case ADDITION:
        return new AdditionExpression(leftExpression, rightExpression);

      case SUBTRACTION:
        return new SubtractionExpression(leftExpression, rightExpression);
      case MULTIPLICATION:
        return new MultiplicationExpression(leftExpression, rightExpression);
      case DIVISION:
        return new DivisionExpression(leftExpression, rightExpression);
      case MODULO:
        return new ModuloExpression(leftExpression, rightExpression);

      case LESS_THAN:
        return new LessThanExpression(leftExpression, rightExpression);
      case LESS_EQUAL:
        return new LessEqualExpression(leftExpression, rightExpression);

      case GREATER_THAN:
        return new GreaterThanExpression(leftExpression, rightExpression);

      case GREATER_EQUAL:
        return new GreaterEqualExpression(leftExpression, rightExpression);

      case EQUAL_TO:
        return new EqualToExpression(leftExpression, rightExpression);

      case NON_EQUAL:
        return new NonEqualExpression(leftExpression, rightExpression);

      case LOGIC_AND:
        return new LogicAndExpression(leftExpression, rightExpression);

      case LOGIC_OR:
        return new LogicOrExpression(leftExpression, rightExpression);

      default:
        throw new IllegalArgumentException("unsupported expression type: " + expressionType);
    }
  }

  public static List<Expression> reconstructTernaryExpressions(
      Expression expression,
      List<Expression> firstExpressions,
      List<Expression> secondExpressions,
      List<Expression> thirdExpressions) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (Expression fe : firstExpressions) {
      for (Expression se : secondExpressions)
        for (Expression te : thirdExpressions) {
          resultExpressions.add(reconstructTernaryExpression(expression, fe, se, te));
        }
    }
    return resultExpressions;
  }

  public static Expression reconstructTernaryExpression(
      Expression expression,
      Expression firstExpression,
      Expression secondExpression,
      Expression thirdExpression) {
    switch (expression.getExpressionType()) {
      case BETWEEN:
        return new BetweenExpression(
            firstExpression,
            secondExpression,
            thirdExpression,
            ((BetweenExpression) expression).isNotBetween());
      default:
        throw new IllegalArgumentException(
            "unsupported expression type: " + expression.getExpressionType());
    }
  }

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

  public static Filter constructTimeFilter(
      ExpressionType expressionType, Expression timeExpression, Expression valueExpression) {
    if (timeExpression instanceof TimestampOperand
        && valueExpression instanceof ConstantOperand
        && ((ConstantOperand) valueExpression).getDataType() == TSDataType.INT64) {
      long value = Long.parseLong(((ConstantOperand) valueExpression).getValueString());
      switch (expressionType) {
        case LESS_THAN:
          return TimeFilter.lt(value);
        case LESS_EQUAL:
          return TimeFilter.ltEq(value);
        case GREATER_THAN:
          return TimeFilter.gt(value);
        case GREATER_EQUAL:
          return TimeFilter.gtEq(value);
        case EQUAL_TO:
          return TimeFilter.eq(value);
        case NON_EQUAL:
          return TimeFilter.notEq(value);
        default:
          throw new IllegalArgumentException("unsupported expression type: " + expressionType);
      }
    }
    return null;
  }

  public static Pair<Filter, Boolean> getPairFromBetweenTimeFirst(
      Expression firstExpression, Expression secondExpression, boolean not) {
    if (firstExpression instanceof ConstantOperand
        && secondExpression instanceof ConstantOperand
        && ((ConstantOperand) firstExpression).getDataType() == TSDataType.INT64
        && ((ConstantOperand) secondExpression).getDataType() == TSDataType.INT64) {
      long value1 = Long.parseLong(((ConstantOperand) firstExpression).getValueString());
      long value2 = Long.parseLong(((ConstantOperand) secondExpression).getValueString());
      return new Pair<>(TimeFilter.between(value1, value2, not), false);
    } else {
      return new Pair<>(null, true);
    }
  }

  public static Pair<Filter, Boolean> getPairFromBetweenTimeSecond(
      BetweenExpression predicate, Expression expression) {
    if (predicate.isNotBetween()) {
      return new Pair<>(
          TimeFilter.gt(Long.parseLong(((ConstantOperand) expression).getValueString())), false);

    } else {
      return new Pair<>(
          TimeFilter.ltEq(Long.parseLong(((ConstantOperand) expression).getValueString())), false);
    }
  }

  public static Pair<Filter, Boolean> getPairFromBetweenTimeThird(
      BetweenExpression predicate, Expression expression) {
    if (predicate.isNotBetween()) {
      return new Pair<>(
          TimeFilter.lt(Long.parseLong(((ConstantOperand) expression).getValueString())), false);

    } else {
      return new Pair<>(
          TimeFilter.gtEq(Long.parseLong(((ConstantOperand) expression).getValueString())), false);
    }
  }

  public static boolean checkConstantSatisfy(
      Expression firstExpression, Expression secondExpression) {
    return firstExpression.isConstantOperand()
        && secondExpression.isConstantOperand()
        && ((ConstantOperand) firstExpression).getDataType() == TSDataType.INT64
        && ((ConstantOperand) secondExpression).getDataType() == TSDataType.INT64
        && (Long.parseLong(((ConstantOperand) firstExpression).getValueString())
            <= Long.parseLong(((ConstantOperand) secondExpression).getValueString()));
  }

  public static Expression constructQueryFilter(List<Expression> expressions) {
    if (expressions.size() == 1) {
      return expressions.get(0);
    }
    return ExpressionUtils.constructBinaryFilterTreeWithAnd(expressions);
  }

  private static Expression constructBinaryFilterTreeWithAnd(List<Expression> expressions) {
    // TODO: consider AVL tree
    if (expressions.size() == 2) {
      return new LogicAndExpression(expressions.get(0), expressions.get(1));
    } else {
      return new LogicAndExpression(
          expressions.get(0),
          constructBinaryFilterTreeWithAnd(expressions.subList(1, expressions.size())));
    }
  }
}
