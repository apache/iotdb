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

package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ExpressionType;
import org.apache.iotdb.db.query.expression.binary.AdditionExpression;
import org.apache.iotdb.db.query.expression.binary.BinaryExpression;
import org.apache.iotdb.db.query.expression.binary.DivisionExpression;
import org.apache.iotdb.db.query.expression.binary.EqualToExpression;
import org.apache.iotdb.db.query.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.query.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.query.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.query.expression.binary.LessThanExpression;
import org.apache.iotdb.db.query.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.query.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.query.expression.binary.ModuloExpression;
import org.apache.iotdb.db.query.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.query.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.query.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.query.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.query.expression.multi.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.InExpression;
import org.apache.iotdb.db.query.expression.unary.LikeExpression;
import org.apache.iotdb.db.query.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.query.expression.unary.NegationExpression;
import org.apache.iotdb.db.query.expression.unary.RegularExpression;
import org.apache.iotdb.db.query.expression.unary.UnaryExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExpressionAnalyzer {

  public static ExpressionAnalysis analyzeExpression(
      Expression expression, SchemaTree schemaTree, TypeProvider typeProvider) {
    List<Expression> outputExpressions =
        removeWildcardInExpression(expression, schemaTree, typeProvider);
    return new ExpressionAnalysis(outputExpressions);
  }

  private static List<Expression> removeWildcardInExpression(
      Expression expression, SchemaTree schemaTree, TypeProvider typeProvider) {
    if (expression instanceof BinaryExpression) {
      List<Expression> leftExpressions =
          removeWildcardInExpression(
              ((BinaryExpression) expression).getLeftExpression(), schemaTree, typeProvider);
      List<Expression> rightExpressions =
          removeWildcardInExpression(
              ((BinaryExpression) expression).getRightExpression(), schemaTree, typeProvider);
      return constructBinaryExpressions(
          expression.getExpressionType(), leftExpressions, rightExpressions);
    } else if (expression instanceof UnaryExpression) {
      List<Expression> childExpressions =
          removeWildcardInExpression(
              ((UnaryExpression) expression).getExpression(), schemaTree, typeProvider);
      return constructUnaryExpressions((UnaryExpression) expression, childExpressions);
    } else if (expression instanceof FunctionExpression) {
      List<List<Expression>> childExpressionsList =
          removeWildcardInFunctionExpression(expression.getExpressions(), schemaTree, typeProvider);
      return constructFunctionExpressions((FunctionExpression) expression, childExpressionsList);
    } else if (expression instanceof TimeSeriesOperand) {
      List<MeasurementPath> actualPaths =
          schemaTree.searchMeasurementPaths(((TimeSeriesOperand) expression).getPath()).left;
      return constructTimeSeriesOperands(actualPaths);
    } else if (expression instanceof ConstantOperand) {
      return Collections.singletonList(expression);
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  private static List<Expression> constructTimeSeriesOperands(List<MeasurementPath> actualPaths) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (MeasurementPath actualPath : actualPaths) {
      resultExpressions.add(new TimeSeriesOperand(actualPath));
    }
    return resultExpressions;
  }

  private static List<List<Expression>> removeWildcardInFunctionExpression(
      List<Expression> expressions, SchemaTree schemaTree, TypeProvider typeProvider) {
    // One by one, remove the wildcards from the input expressions. In most cases, an expression
    // will produce multiple expressions after removing the wildcards. We use extendedExpressions to
    // collect the produced expressions.
    List<List<Expression>> extendedExpressions = new ArrayList<>();
    for (Expression originExpression : expressions) {
      List<Expression> actualExpressions =
          removeWildcardInExpression(originExpression, schemaTree, typeProvider);
      if (actualExpressions.isEmpty()) {
        // Let's ignore the eval of the function which has at least one non-existence series as
        // input. See IOTDB-1212: https://github.com/apache/iotdb/pull/3101
        return Collections.emptyList();
      }
      extendedExpressions.add(actualExpressions);
    }

    // Calculate the Cartesian product of extendedExpressions to get the actual expressions after
    // removing all wildcards. We use actualExpressions to collect them.
    List<List<Expression>> actualExpressions = new ArrayList<>();
    cartesianProduct(extendedExpressions, actualExpressions, 0, new ArrayList<>());
    return actualExpressions;
  }

  private static List<Expression> constructFunctionExpressions(
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

  private static List<Expression> constructUnaryExpressions(
      UnaryExpression expression, List<Expression> childExpressions) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (Expression childExpression : childExpressions) {
      switch (expression.getExpressionType()) {
        case IN:
          resultExpressions.add(
              new InExpression(childExpression, ((InExpression) expression).getValues()));
          break;
        case LIKE:
          resultExpressions.add(
              new LikeExpression(
                  childExpression,
                  ((LikeExpression) expression).getPatternString(),
                  ((LikeExpression) expression).getPattern()));
          break;
        case LOGIC_NOT:
          resultExpressions.add(new LogicNotExpression(childExpression));
          break;
        case NEGATION:
          resultExpressions.add(new NegationExpression(childExpression));
          break;
        case REGEXP:
          resultExpressions.add(
              new RegularExpression(
                  childExpression,
                  ((RegularExpression) expression).getPatternString(),
                  ((RegularExpression) expression).getPattern()));
          break;
        default:
          throw new IllegalArgumentException(
              "unsupported expression type: " + expression.getExpressionType());
      }
    }
    return resultExpressions;
  }

  private static List<Expression> constructBinaryExpressions(
      ExpressionType expressionType,
      List<Expression> leftExpressions,
      List<Expression> rightExpressions) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (Expression le : leftExpressions) {
      for (Expression re : rightExpressions) {
        switch (expressionType) {
          case ADDITION:
            resultExpressions.add(new AdditionExpression(le, re));
            break;
          case SUBTRACTION:
            resultExpressions.add(new SubtractionExpression(le, re));
            break;
          case MULTIPLICATION:
            resultExpressions.add(new MultiplicationExpression(le, re));
            break;
          case DIVISION:
            resultExpressions.add(new DivisionExpression(le, re));
            break;
          case MODULO:
            resultExpressions.add(new ModuloExpression(le, re));
            break;
          case LESS_THAN:
            resultExpressions.add(new LessThanExpression(le, re));
            break;
          case LESS_EQUAL:
            resultExpressions.add(new LessEqualExpression(le, re));
            break;
          case GREATER_THAN:
            resultExpressions.add(new GreaterThanExpression(le, re));
            break;
          case GREATER_EQUAL:
            resultExpressions.add(new GreaterEqualExpression(le, re));
            break;
          case EQUAL_TO:
            resultExpressions.add(new EqualToExpression(le, re));
            break;
          case NON_EQUAL:
            resultExpressions.add(new NonEqualExpression(le, re));
            break;
          case LOGIC_AND:
            resultExpressions.add(new LogicAndExpression(le, re));
            break;
          case LOGIC_OR:
            resultExpressions.add(new LogicOrExpression(le, re));
            break;
          default:
            throw new IllegalArgumentException("unsupported expression type: " + expressionType);
        }
      }
    }
    return resultExpressions;
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
}
