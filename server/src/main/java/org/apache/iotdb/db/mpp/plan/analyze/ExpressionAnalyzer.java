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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.schematree.DeviceSchemaInfo;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.qp.constant.SQLConstant;
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
import org.apache.iotdb.db.query.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.query.expression.multi.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.InExpression;
import org.apache.iotdb.db.query.expression.unary.LikeExpression;
import org.apache.iotdb.db.query.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.query.expression.unary.NegationExpression;
import org.apache.iotdb.db.query.expression.unary.RegularExpression;
import org.apache.iotdb.db.query.expression.unary.UnaryExpression;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.Sets;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ExpressionAnalyzer {

  public static List<Expression> concatExpressionWithSuffixPaths(
      Expression expression, List<PartialPath> prefixPaths, PathPatternTree patternTree) {
    if (expression instanceof BinaryExpression) {
      List<Expression> leftExpressions =
          concatExpressionWithSuffixPaths(
              ((BinaryExpression) expression).getLeftExpression(), prefixPaths, patternTree);
      List<Expression> rightExpressions =
          concatExpressionWithSuffixPaths(
              ((BinaryExpression) expression).getRightExpression(), prefixPaths, patternTree);
      return reconstructBinaryExpressions(
          expression.getExpressionType(), leftExpressions, rightExpressions);
    } else if (expression instanceof UnaryExpression) {
      List<Expression> childExpressions =
          concatExpressionWithSuffixPaths(
              ((UnaryExpression) expression).getExpression(), prefixPaths, patternTree);
      return reconstructUnaryExpressions((UnaryExpression) expression, childExpressions);
    } else if (expression instanceof FunctionExpression) {
      List<List<Expression>> extendedExpressions = new ArrayList<>();
      for (Expression suffixExpression : expression.getExpressions()) {
        extendedExpressions.add(
            concatExpressionWithSuffixPaths(suffixExpression, prefixPaths, patternTree));
      }
      List<List<Expression>> childExpressionsList = new ArrayList<>();
      cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
      return reconstructFunctionExpressions((FunctionExpression) expression, childExpressionsList);
    } else if (expression instanceof TimeSeriesOperand) {
      PartialPath rawPath = ((TimeSeriesOperand) expression).getPath();
      List<PartialPath> actualPaths = new ArrayList<>();
      if (rawPath.getFullPath().startsWith(SQLConstant.ROOT + TsFileConstant.PATH_SEPARATOR)) {
        actualPaths.add(rawPath);
      } else {
        for (PartialPath prefixPath : prefixPaths) {
          PartialPath concatPath = prefixPath.concatPath(rawPath);
          patternTree.appendPath(concatPath);
          actualPaths.add(concatPath);
        }
      }
      return reconstructTimeSeriesOperands(actualPaths);
    } else if (expression instanceof TimestampOperand) {
      return Collections.singletonList(expression);
    } else if (expression instanceof ConstantOperand) {
      return Collections.singletonList(expression);
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static void constructPatternTreeFromQueryFilter(
      Expression predicate, List<PartialPath> prefixPaths, PathPatternTree patternTree) {
    if (predicate instanceof BinaryExpression) {
      constructPatternTreeFromQueryFilter(
          ((BinaryExpression) predicate).getLeftExpression(), prefixPaths, patternTree);
      constructPatternTreeFromQueryFilter(
          ((BinaryExpression) predicate).getRightExpression(), prefixPaths, patternTree);
    } else if (predicate instanceof UnaryExpression) {
      constructPatternTreeFromQueryFilter(
          ((UnaryExpression) predicate).getExpression(), prefixPaths, patternTree);
    } else if (predicate instanceof FunctionExpression) {
      for (Expression suffixExpression : predicate.getExpressions()) {
        constructPatternTreeFromQueryFilter(suffixExpression, prefixPaths, patternTree);
      }
    } else if (predicate instanceof TimeSeriesOperand) {
      PartialPath rawPath = ((TimeSeriesOperand) predicate).getPath();
      if (SQLConstant.isReservedPath(rawPath)) {
        return;
      }
      if (rawPath.getFullPath().startsWith(SQLConstant.ROOT + TsFileConstant.PATH_SEPARATOR)) {
        patternTree.appendPath(rawPath);
        return;
      }
      for (PartialPath prefixPath : prefixPaths) {
        PartialPath concatPath = prefixPath.concatPath(rawPath);
        patternTree.appendPath(concatPath);
      }
    } else if (predicate instanceof TimestampOperand || predicate instanceof ConstantOperand) {
      // do nothing
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }

  public static List<Expression> removeWildcardInExpression(
      Expression expression, SchemaTree schemaTree) {
    if (expression instanceof BinaryExpression) {
      List<Expression> leftExpressions =
          removeWildcardInExpression(
              ((BinaryExpression) expression).getLeftExpression(), schemaTree);
      List<Expression> rightExpressions =
          removeWildcardInExpression(
              ((BinaryExpression) expression).getRightExpression(), schemaTree);
      return reconstructBinaryExpressions(
          expression.getExpressionType(), leftExpressions, rightExpressions);
    } else if (expression instanceof UnaryExpression) {
      List<Expression> childExpressions =
          removeWildcardInExpression(((UnaryExpression) expression).getExpression(), schemaTree);
      return reconstructUnaryExpressions((UnaryExpression) expression, childExpressions);
    } else if (expression instanceof FunctionExpression) {
      // One by one, remove the wildcards from the input expressions. In most cases, an expression
      // will produce multiple expressions after removing the wildcards. We use extendedExpressions
      // to
      // collect the produced expressions.
      List<List<Expression>> extendedExpressions = new ArrayList<>();
      for (Expression originExpression : expression.getExpressions()) {
        List<Expression> actualExpressions =
            removeWildcardInExpression(originExpression, schemaTree);
        if (actualExpressions.isEmpty()) {
          // Let's ignore the eval of the function which has at least one non-existence series as
          // input. See IOTDB-1212: https://github.com/apache/iotdb/pull/3101
          return Collections.emptyList();
        }
        extendedExpressions.add(actualExpressions);
      }

      // Calculate the Cartesian product of extendedExpressions to get the actual expressions after
      // removing all wildcards. We use actualExpressions to collect them.
      List<List<Expression>> childExpressionsList = new ArrayList<>();
      cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
      return reconstructFunctionExpressions((FunctionExpression) expression, childExpressionsList);
    } else if (expression instanceof TimeSeriesOperand) {
      PartialPath path = ((TimeSeriesOperand) expression).getPath();
      List<MeasurementPath> actualPaths = schemaTree.searchMeasurementPaths(path).left;
      return reconstructTimeSeriesOperands(actualPaths);
    } else if (expression instanceof TimestampOperand) {
      return Collections.singletonList(expression);
    } else if (expression instanceof ConstantOperand) {
      return Collections.singletonList(expression);
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  private static List<Expression> reconstructTimeSeriesOperands(
      List<? extends PartialPath> actualPaths) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (PartialPath actualPath : actualPaths) {
      resultExpressions.add(new TimeSeriesOperand(actualPath));
    }
    return resultExpressions;
  }

  private static List<Expression> reconstructFunctionExpressions(
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

  private static List<Expression> reconstructUnaryExpressions(
      UnaryExpression expression, List<Expression> childExpressions) {
    List<Expression> resultExpressions = new ArrayList<>();
    for (Expression childExpression : childExpressions) {
      switch (expression.getExpressionType()) {
        case IN:
          resultExpressions.add(
              new InExpression(
                  childExpression,
                  ((InExpression) expression).isNotIn(),
                  ((InExpression) expression).getValues()));
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

  private static List<Expression> reconstructBinaryExpressions(
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

  private static <T> void cartesianProduct(
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

  public static List<Expression> searchSourceExpressions(Expression expression) {
    if (expression instanceof BinaryExpression) {
      List<Expression> resultExpressions = new ArrayList<>();
      resultExpressions.addAll(
          searchSourceExpressions(((BinaryExpression) expression).getLeftExpression()));
      resultExpressions.addAll(
          searchSourceExpressions(((BinaryExpression) expression).getRightExpression()));
      return resultExpressions;
    } else if (expression instanceof UnaryExpression) {
      return searchSourceExpressions(((UnaryExpression) expression).getExpression());
    } else if (expression instanceof FunctionExpression) {
      if (expression.isBuiltInAggregationFunctionExpression()) {
        return Collections.singletonList(expression);
      }
      List<Expression> resultExpressions = new ArrayList<>();
      for (Expression childExpression : expression.getExpressions()) {
        resultExpressions.addAll(searchSourceExpressions(childExpression));
      }
      return resultExpressions;
    } else if (expression instanceof TimeSeriesOperand) {
      return Collections.singletonList(expression);
    } else if (expression instanceof TimestampOperand || expression instanceof ConstantOperand) {
      return Collections.emptyList();
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static Pair<Expression, String> getMeasurementWithAliasInExpression(
      Expression expression, String alias) {
    if (expression instanceof TimeSeriesOperand) {
      String measurement = ((TimeSeriesOperand) expression).getPath().getMeasurement();
      if (measurement.equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        throw new SemanticException(
            "ALIGN BY DEVICE: prefix path in SELECT clause can only be one measurement or one-layer wildcard.");
      }
      if (alias != null && measurement.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
        throw new SemanticException(
            String.format(
                "ALIGN BY DEVICE: alias '%s' can only be matched with one measurement", alias));
      }
      Expression measurementExpression;
      try {
        measurementExpression = new TimeSeriesOperand(new PartialPath(measurement));
        return new Pair<>(measurementExpression, alias);
      } catch (IllegalPathException e) {
        throw new SemanticException("ALIGN BY DEVICE: illegal measurement name: " + measurement);
      }
    } else if (expression instanceof FunctionExpression) {
      if (expression.getExpressions().size() > 1) {
        throw new SemanticException(
            "ALIGN BY DEVICE: prefix path in SELECT clause can only be one measurement or one-layer wildcard.");
      }
      Expression measurementFunctionExpression =
          new FunctionExpression(
              ((FunctionExpression) expression).getFunctionName(),
              ((FunctionExpression) expression).getFunctionAttributes(),
              Collections.singletonList(
                  getMeasurementWithAliasInExpression(expression.getExpressions().get(0), alias)
                      .left));
      return new Pair<>(measurementFunctionExpression, alias);
    } else {
      throw new SemanticException(
          "ALIGN BY DEVICE: prefix path in SELECT clause can only be one measurement or one-layer wildcard.");
    }
  }

  public static PartialPath getPathInLeafExpression(Expression expression) {
    if (expression instanceof TimeSeriesOperand) {
      return ((TimeSeriesOperand) expression).getPath();
    } else if (expression instanceof FunctionExpression) {
      Validate.isTrue(expression.getExpressions().size() == 1);
      Validate.isTrue(expression.getExpressions().get(0) instanceof TimeSeriesOperand);
      return ((TimeSeriesOperand) expression.getExpressions().get(0)).getPath();
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static Expression replacePathInExpression(Expression expression, PartialPath path) {
    if (expression instanceof TimeSeriesOperand) {
      return new TimeSeriesOperand(path);
    } else if (expression instanceof FunctionExpression) {
      return new FunctionExpression(
          ((FunctionExpression) expression).getFunctionName(),
          ((FunctionExpression) expression).getFunctionAttributes(),
          Collections.singletonList(new TimeSeriesOperand(path)));
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static Expression replacePathInExpression(Expression expression, String path) {
    PartialPath newPath;
    try {
      newPath = new PartialPath(path);
    } catch (IllegalPathException e) {
      throw new SemanticException("illegal path: " + path);
    }
    return replacePathInExpression(expression, newPath);
  }

  public static Filter transformToGlobalTimeFilter(Expression predicate) {
    if (predicate instanceof LogicAndExpression) {
      Filter leftTimeFilter =
          transformToGlobalTimeFilter(((BinaryExpression) predicate).getLeftExpression());
      Filter rightTimeFilter =
          transformToGlobalTimeFilter(((BinaryExpression) predicate).getRightExpression());
      if (leftTimeFilter != null && rightTimeFilter != null) {
        return FilterFactory.and(leftTimeFilter, rightTimeFilter);
      } else if (leftTimeFilter != null) {
        return leftTimeFilter;
      } else {
        return rightTimeFilter;
      }
    } else if (predicate instanceof LogicOrExpression) {
      Filter leftTimeFilter =
          transformToGlobalTimeFilter(((BinaryExpression) predicate).getLeftExpression());
      Filter rightTimeFilter =
          transformToGlobalTimeFilter(((BinaryExpression) predicate).getRightExpression());
      if (leftTimeFilter != null && rightTimeFilter != null) {
        return FilterFactory.or(leftTimeFilter, rightTimeFilter);
      }
      return null;
    } else if (predicate instanceof LogicNotExpression) {
      Filter childTimeFilter =
          transformToGlobalTimeFilter(((UnaryExpression) predicate).getExpression());
      if (childTimeFilter != null) {
        return FilterFactory.not(childTimeFilter);
      }
      return null;
    } else if (predicate instanceof GreaterEqualExpression
        || predicate instanceof GreaterThanExpression
        || predicate instanceof LessEqualExpression
        || predicate instanceof LessThanExpression
        || predicate instanceof EqualToExpression
        || predicate instanceof NonEqualExpression) {
      Filter timeInLeftFilter =
          constructTimeFilter(
              predicate.getExpressionType(),
              ((BinaryExpression) predicate).getLeftExpression(),
              ((BinaryExpression) predicate).getRightExpression());
      if (timeInLeftFilter != null) {
        return timeInLeftFilter;
      }
      return constructTimeFilter(
          predicate.getExpressionType(),
          ((BinaryExpression) predicate).getRightExpression(),
          ((BinaryExpression) predicate).getLeftExpression());
    } else if (predicate instanceof InExpression) {
      Expression timeExpression = ((InExpression) predicate).getExpression();
      if (timeExpression instanceof TimestampOperand) {
        return TimeFilter.in(
            ((InExpression) predicate)
                .getValues().stream().map(Long::parseLong).collect(Collectors.toSet()),
            ((InExpression) predicate).isNotIn());
      }
      return null;
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }

  private static Filter constructTimeFilter(
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

  public static void setTypeProvider(Expression expression, TypeProvider typeProvider) {
    if (expression instanceof BinaryExpression) {
      setTypeProvider(((BinaryExpression) expression).getLeftExpression(), typeProvider);
      setTypeProvider(((BinaryExpression) expression).getRightExpression(), typeProvider);
    } else if (expression instanceof UnaryExpression) {
      setTypeProvider(((UnaryExpression) expression).getExpression(), typeProvider);
    } else if (expression instanceof FunctionExpression) {
      if (expression.isBuiltInAggregationFunctionExpression()) {
        Validate.isTrue(expression.getExpressions().size() == 1);
        Expression childExpression = expression.getExpressions().get(0);
        PartialPath path = ((TimeSeriesOperand) childExpression).getPath();
        typeProvider.setType(
            expression.getExpressionString(),
            SchemaUtils.getSeriesTypeByPath(
                path, ((FunctionExpression) expression).getFunctionName()));
        setTypeProvider(childExpression, typeProvider);
      } else {
        for (Expression childExpression : expression.getExpressions()) {
          setTypeProvider(childExpression, typeProvider);
        }
      }
    } else if (expression instanceof TimeSeriesOperand) {
      PartialPath rawPath = ((TimeSeriesOperand) expression).getPath();
      typeProvider.setType(
          rawPath.isMeasurementAliasExists()
              ? rawPath.getFullPathWithAlias()
              : rawPath.getFullPath(),
          rawPath.getSeriesType());
    } else if (expression instanceof ConstantOperand || expression instanceof TimestampOperand) {
      // do nothing
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static List<Expression> removeWildcardInQueryFilter(
      Expression predicate,
      List<PartialPath> prefixPaths,
      SchemaTree schemaTree,
      TypeProvider typeProvider) {
    if (predicate instanceof BinaryExpression) {
      List<Expression> leftExpressions =
          removeWildcardInQueryFilter(
              ((BinaryExpression) predicate).getLeftExpression(),
              prefixPaths,
              schemaTree,
              typeProvider);
      List<Expression> rightExpressions =
          removeWildcardInQueryFilter(
              ((BinaryExpression) predicate).getRightExpression(),
              prefixPaths,
              schemaTree,
              typeProvider);
      if (predicate.getExpressionType() == ExpressionType.LOGIC_AND) {
        List<Expression> resultExpressions = new ArrayList<>(leftExpressions);
        resultExpressions.addAll(rightExpressions);
        return resultExpressions;
      }
      return reconstructBinaryExpressions(
          predicate.getExpressionType(), leftExpressions, rightExpressions);
    } else if (predicate instanceof UnaryExpression) {
      List<Expression> childExpressions =
          removeWildcardInQueryFilter(
              ((UnaryExpression) predicate).getExpression(), prefixPaths, schemaTree, typeProvider);
      return reconstructUnaryExpressions((UnaryExpression) predicate, childExpressions);
    } else if (predicate instanceof FunctionExpression) {
      if (predicate.isBuiltInAggregationFunctionExpression()) {
        throw new SemanticException("aggregate functions are not supported in WHERE clause");
      }
      List<List<Expression>> extendedExpressions = new ArrayList<>();
      for (Expression suffixExpression : predicate.getExpressions()) {
        extendedExpressions.add(
            removeWildcardInQueryFilter(suffixExpression, prefixPaths, schemaTree, typeProvider));
      }
      List<List<Expression>> childExpressionsList = new ArrayList<>();
      cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
      return reconstructFunctionExpressions((FunctionExpression) predicate, childExpressionsList);
    } else if (predicate instanceof TimeSeriesOperand) {
      PartialPath filterPath = ((TimeSeriesOperand) predicate).getPath();
      List<PartialPath> concatPaths = new ArrayList<>();
      if (!filterPath.getFirstNode().equals(SQLConstant.ROOT)) {
        prefixPaths.forEach(prefix -> concatPaths.add(prefix.concatPath(filterPath)));
      } else {
        // do nothing in the case of "where root.d1.s1 > 5"
        concatPaths.add(filterPath);
      }

      List<PartialPath> noStarPaths =
          concatPaths.stream()
              .map(concatPath -> schemaTree.searchMeasurementPaths(concatPath).left)
              .flatMap(List::stream)
              .collect(Collectors.toList());
      noStarPaths.forEach(path -> typeProvider.setType(path.getFullPath(), path.getSeriesType()));
      return reconstructTimeSeriesOperands(noStarPaths);
    } else if (predicate instanceof TimestampOperand) {
      // do nothing in the case of "where time > 5"
      return Collections.singletonList(predicate);
    } else if (predicate instanceof ConstantOperand) {
      return Collections.singletonList(predicate);
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }

  public static List<Expression> removeWildcardInQueryFilterByDevice(
      Expression predicate, DeviceSchemaInfo deviceSchemaInfo, TypeProvider typeProvider) {
    if (predicate instanceof BinaryExpression) {
      List<Expression> leftExpressions =
          removeWildcardInQueryFilterByDevice(
              ((BinaryExpression) predicate).getLeftExpression(), deviceSchemaInfo, typeProvider);
      List<Expression> rightExpressions =
          removeWildcardInQueryFilterByDevice(
              ((BinaryExpression) predicate).getRightExpression(), deviceSchemaInfo, typeProvider);
      if (predicate.getExpressionType() == ExpressionType.LOGIC_AND) {
        List<Expression> resultExpressions = new ArrayList<>(leftExpressions);
        resultExpressions.addAll(rightExpressions);
        return resultExpressions;
      }
      return reconstructBinaryExpressions(
          predicate.getExpressionType(), leftExpressions, rightExpressions);
    } else if (predicate instanceof UnaryExpression) {
      List<Expression> childExpressions =
          removeWildcardInQueryFilterByDevice(
              ((UnaryExpression) predicate).getExpression(), deviceSchemaInfo, typeProvider);
      return reconstructUnaryExpressions((UnaryExpression) predicate, childExpressions);
    } else if (predicate instanceof FunctionExpression) {
      if (predicate.isBuiltInAggregationFunctionExpression()) {
        throw new SemanticException("aggregate functions are not supported in WHERE clause");
      }
      List<List<Expression>> extendedExpressions = new ArrayList<>();
      for (Expression suffixExpression : predicate.getExpressions()) {
        extendedExpressions.add(
            removeWildcardInQueryFilterByDevice(suffixExpression, deviceSchemaInfo, typeProvider));
      }
      List<List<Expression>> childExpressionsList = new ArrayList<>();
      cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
      return reconstructFunctionExpressions((FunctionExpression) predicate, childExpressionsList);
    } else if (predicate instanceof TimeSeriesOperand) {
      PartialPath filterPath = ((TimeSeriesOperand) predicate).getPath();
      String measurement = filterPath.getFullPath();
      List<PartialPath> concatPaths = new ArrayList<>();
      if (measurement.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
        concatPaths.addAll(deviceSchemaInfo.getMeasurements());
      } else {
        MeasurementPath concatPath = deviceSchemaInfo.getPathByMeasurement(measurement);
        if (concatPath == null) {
          throw new SemanticException(
              String.format(
                  "ALIGN BY DEVICE: measurement '%s' does not exist in device '%s'",
                  measurement, deviceSchemaInfo.getDevicePath()));
        }
        concatPaths.add(concatPath);
      }
      concatPaths.forEach(path -> typeProvider.setType(path.getFullPath(), path.getSeriesType()));
      return reconstructTimeSeriesOperands(concatPaths);
    } else if (predicate instanceof TimestampOperand) {
      // do nothing in the case of "where time > 5"
      return Collections.singletonList(predicate);
    } else if (predicate instanceof ConstantOperand) {
      return Collections.singletonList(predicate);
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }

  public static Expression constructBinaryFilterTreeWithAnd(List<Expression> expressions) {
    // TODO: consider AVL tree
    if (expressions.size() == 2) {
      return new LogicAndExpression(expressions.get(0), expressions.get(1));
    } else {
      return new LogicAndExpression(
          expressions.get(0),
          constructBinaryFilterTreeWithAnd(expressions.subList(1, expressions.size())));
    }
  }

  public static String getDeviceName(Expression expression) {
    if (expression instanceof TimeSeriesOperand) {
      return ((TimeSeriesOperand) expression).getPath().getDeviceIdString();
    } else if (expression instanceof FunctionExpression) {
      return getDeviceName(expression.getExpressions().get(0));
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static Set<PartialPath> collectPaths(Expression expression) {
    if (expression instanceof BinaryExpression) {
      Set<PartialPath> resultSet =
          collectPaths(((BinaryExpression) expression).getLeftExpression());
      resultSet.addAll(collectPaths(((BinaryExpression) expression).getRightExpression()));
      return resultSet;
    } else if (expression instanceof UnaryExpression) {
      return collectPaths(((UnaryExpression) expression).getExpression());
    } else if (expression instanceof FunctionExpression) {
      Set<PartialPath> resultSet = new HashSet<>();
      for (Expression childExpression : expression.getExpressions()) {
        resultSet.addAll(collectPaths(childExpression));
      }
      return resultSet;
    } else if (expression instanceof TimeSeriesOperand) {
      return Sets.newHashSet(((TimeSeriesOperand) expression).getPath());
    } else if (expression instanceof TimestampOperand || expression instanceof ConstantOperand) {
      return Collections.emptySet();
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static void checkIsAllMeasurement(Expression expression) {
    if (expression instanceof BinaryExpression) {
      checkIsAllMeasurement(((BinaryExpression) expression).getLeftExpression());
      checkIsAllMeasurement(((BinaryExpression) expression).getRightExpression());
    } else if (expression instanceof UnaryExpression) {
      checkIsAllMeasurement(((UnaryExpression) expression).getExpression());
    } else if (expression instanceof FunctionExpression) {
      for (Expression childExpression : expression.getExpressions()) {
        checkIsAllMeasurement(childExpression);
      }
    } else if (expression instanceof TimeSeriesOperand) {
      PartialPath path = ((TimeSeriesOperand) expression).getPath();
      if (path.getNodes().length > 1
          || path.getFullPath().equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        throw new SemanticException(
            "ALIGN BY DEVICE: the paths can only be measurement or one-level wildcard");
      }
    } else if (expression instanceof TimestampOperand || expression instanceof ConstantOperand) {
      // do nothing
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static void checkIsAllAggregation(Expression expression) {
    if (expression instanceof BinaryExpression) {
      checkIsAllAggregation(((BinaryExpression) expression).getLeftExpression());
      checkIsAllAggregation(((BinaryExpression) expression).getRightExpression());
    } else if (expression instanceof UnaryExpression) {
      checkIsAllAggregation(((UnaryExpression) expression).getExpression());
    } else if (expression instanceof FunctionExpression) {
      if (expression.getExpressions().size() != 1
          || !(expression.getExpressions().get(0) instanceof TimeSeriesOperand)) {
        throw new SemanticException(
            "The argument of the aggregation function must be a time series.");
      }
    } else if (expression instanceof TimeSeriesOperand) {
      throw new SemanticException(
          "Raw data queries and aggregated queries are not allowed to appear at the same time.");
    } else if (expression instanceof TimestampOperand || expression instanceof ConstantOperand) {
      // do nothing
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }
}
