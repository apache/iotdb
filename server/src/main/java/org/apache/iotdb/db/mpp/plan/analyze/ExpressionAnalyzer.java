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
import org.apache.iotdb.db.query.expression.binary.BinaryExpression;
import org.apache.iotdb.db.query.expression.binary.EqualToExpression;
import org.apache.iotdb.db.query.expression.binary.GreaterEqualExpression;
import org.apache.iotdb.db.query.expression.binary.GreaterThanExpression;
import org.apache.iotdb.db.query.expression.binary.LessEqualExpression;
import org.apache.iotdb.db.query.expression.binary.LessThanExpression;
import org.apache.iotdb.db.query.expression.binary.LogicAndExpression;
import org.apache.iotdb.db.query.expression.binary.LogicOrExpression;
import org.apache.iotdb.db.query.expression.binary.NonEqualExpression;
import org.apache.iotdb.db.query.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.query.expression.leaf.LeafOperand;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.query.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.query.expression.multi.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.InExpression;
import org.apache.iotdb.db.query.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.query.expression.unary.UnaryExpression;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
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

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.cartesianProduct;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.constructTimeFilter;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructBinaryExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructFunctionExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperands;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructUnaryExpressions;

public class ExpressionAnalyzer {

  /**
   * Check if all suffix paths in expression are measurements or one-level wildcards in ALIGN BY
   * DEVICE query. If not, throw a {@link SemanticException}.
   *
   * @param expression expression to be checked
   */
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
            "ALIGN BY DEVICE: the suffix paths can only be measurement or one-level wildcard");
      }
    } else if (expression instanceof TimestampOperand || expression instanceof ConstantOperand) {
      // do nothing
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  /**
   * Check if expression is a built-in aggregation function. If not, throw a {@link
   * SemanticException}.
   *
   * @param expression expression to be checked
   */
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

  /**
   * Concat suffix path in SELECT or WITHOUT NULL clause with the prefix path in the FROM clause.
   * and Construct a {@link PathPatternTree}.
   *
   * @param expression expression in SELECT or WITHOUT NULL clause which may include suffix paths
   * @param prefixPaths prefix paths in the FROM clause
   * @param patternTree a PathPatternTree contains all paths to query
   * @return the concatenated expression list
   */
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

  /**
   * Concat suffix path in WHERE clause with the prefix path in the FROM clause and Construct a
   * {@link PathPatternTree}. This method return void, i.e. will not rewrite the statement.
   *
   * @param predicate expression in WHERE clause
   * @param prefixPaths prefix paths in the FROM clause
   * @param patternTree a PathPatternTree contains all paths to query
   */
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

  /**
   * Bind schema ({@link PartialPath} -> {@link MeasurementPath}) and removes wildcards in
   * Expression.
   *
   * @param schemaTree interface for querying schema information
   * @return the expression list after binding schema
   */
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
      // to collect the produced expressions.
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

  /**
   * Concat suffix path in WHERE clause with the prefix path in the FROM clause. And then, bind
   * schema ({@link PartialPath} -> {@link MeasurementPath}) and removes wildcards in Expression.
   *
   * @param prefixPaths prefix paths in the FROM clause
   * @param schemaTree interface for querying schema information
   * @param typeProvider a map to record output symbols and their data types
   * @return the expression list with full path and after binding schema
   */
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

  /**
   * Concat measurement in WHERE clause with device path. And then, bind schema ({@link PartialPath}
   * -> {@link MeasurementPath}) and removes wildcards.
   *
   * @param deviceSchemaInfo device path and schema infos of measurements under this device
   * @param typeProvider a map to record output symbols and their data types
   * @return the expression list with full path and after binding schema
   */
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

  /**
   * Extract global time filter from query filter.
   *
   * @param predicate raw query filter
   * @return global time filter
   */
  public static Pair<Filter, Boolean> transformToGlobalTimeFilter(Expression predicate) {
    if (predicate instanceof LogicAndExpression) {
      Pair<Filter, Boolean> leftResultPair =
          transformToGlobalTimeFilter(((BinaryExpression) predicate).getLeftExpression());
      Pair<Filter, Boolean> rightResultPair =
          transformToGlobalTimeFilter(((BinaryExpression) predicate).getRightExpression());
      if (leftResultPair.left != null && rightResultPair.left != null) {
        return new Pair<>(
            FilterFactory.and(leftResultPair.left, rightResultPair.left),
            leftResultPair.right || rightResultPair.right);
      } else if (leftResultPair.left != null) {
        return new Pair<>(leftResultPair.left, true);
      } else if (rightResultPair.left != null) {
        return new Pair<>(rightResultPair.left, true);
      }
      return new Pair<>(null, true);
    } else if (predicate instanceof LogicOrExpression) {
      Pair<Filter, Boolean> leftResultPair =
          transformToGlobalTimeFilter(((BinaryExpression) predicate).getLeftExpression());
      Pair<Filter, Boolean> rightResultPair =
          transformToGlobalTimeFilter(((BinaryExpression) predicate).getRightExpression());
      if (leftResultPair.left != null && rightResultPair.left != null) {
        return new Pair<>(
            FilterFactory.or(leftResultPair.left, rightResultPair.left),
            leftResultPair.right || rightResultPair.right);
      }
      return new Pair<>(null, true);
    } else if (predicate instanceof LogicNotExpression) {
      Pair<Filter, Boolean> childResultPair =
          transformToGlobalTimeFilter(((UnaryExpression) predicate).getExpression());
      return new Pair<>(FilterFactory.not(childResultPair.left), childResultPair.right);
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
        return new Pair<>(timeInLeftFilter, false);
      }
      Filter timeInRightFilter =
          constructTimeFilter(
              predicate.getExpressionType(),
              ((BinaryExpression) predicate).getRightExpression(),
              ((BinaryExpression) predicate).getLeftExpression());
      if (timeInRightFilter != null) {
        return new Pair<>(timeInRightFilter, false);
      }
      return new Pair<>(null, true);
    } else if (predicate instanceof InExpression) {
      Expression timeExpression = ((InExpression) predicate).getExpression();
      if (timeExpression instanceof TimestampOperand) {
        return new Pair<>(
            TimeFilter.in(
                ((InExpression) predicate)
                    .getValues().stream().map(Long::parseLong).collect(Collectors.toSet()),
                ((InExpression) predicate).isNotIn()),
            false);
      }
      return new Pair<>(null, true);
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }

  /**
   * Search for subexpressions that can be queried natively, including time series raw data and
   * built-in aggregate functions.
   *
   * @param expression expression to be searched
   * @param isRawDataSource if true, built-in aggregate functions are not be returned
   * @return searched subexpression list
   */
  public static List<Expression> searchSourceExpressions(
      Expression expression, boolean isRawDataSource) {
    if (expression instanceof BinaryExpression) {
      List<Expression> resultExpressions = new ArrayList<>();
      resultExpressions.addAll(
          searchSourceExpressions(
              ((BinaryExpression) expression).getLeftExpression(), isRawDataSource));
      resultExpressions.addAll(
          searchSourceExpressions(
              ((BinaryExpression) expression).getRightExpression(), isRawDataSource));
      return resultExpressions;
    } else if (expression instanceof UnaryExpression) {
      return searchSourceExpressions(
          ((UnaryExpression) expression).getExpression(), isRawDataSource);
    } else if (expression instanceof FunctionExpression) {
      if (!isRawDataSource && expression.isBuiltInAggregationFunctionExpression()) {
        return Collections.singletonList(expression);
      }
      List<Expression> resultExpressions = new ArrayList<>();
      for (Expression childExpression : expression.getExpressions()) {
        resultExpressions.addAll(searchSourceExpressions(childExpression, isRawDataSource));
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

  /**
   * Search for built-in aggregate functions subexpressions.
   *
   * @param expression expression to be searched
   * @return searched aggregate functions list
   */
  public static List<Expression> searchAggregationExpressions(Expression expression) {
    if (expression instanceof BinaryExpression) {
      List<Expression> resultExpressions = new ArrayList<>();
      resultExpressions.addAll(
          searchAggregationExpressions(((BinaryExpression) expression).getLeftExpression()));
      resultExpressions.addAll(
          searchAggregationExpressions(((BinaryExpression) expression).getRightExpression()));
      return resultExpressions;
    } else if (expression instanceof UnaryExpression) {
      return searchAggregationExpressions(((UnaryExpression) expression).getExpression());
    } else if (expression instanceof FunctionExpression) {
      return Collections.singletonList(expression);
    } else if (expression instanceof LeafOperand) {
      return Collections.emptyList();
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  /** Returns all the timeseries path in the expression */
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

  /** Update typeProvider by expression. */
  public static void updateTypeProvider(Expression expression, TypeProvider typeProvider) {
    if (expression instanceof BinaryExpression) {
      updateTypeProvider(((BinaryExpression) expression).getLeftExpression(), typeProvider);
      updateTypeProvider(((BinaryExpression) expression).getRightExpression(), typeProvider);
    } else if (expression instanceof UnaryExpression) {
      updateTypeProvider(((UnaryExpression) expression).getExpression(), typeProvider);
    } else if (expression instanceof FunctionExpression) {
      if (expression.isBuiltInAggregationFunctionExpression()) {
        Validate.isTrue(expression.getExpressions().size() == 1);
        Expression childExpression = expression.getExpressions().get(0);
        PartialPath path = ((TimeSeriesOperand) childExpression).getPath();
        typeProvider.setType(
            expression.getExpressionString(),
            SchemaUtils.getSeriesTypeByPath(
                path, ((FunctionExpression) expression).getFunctionName()));
        updateTypeProvider(childExpression, typeProvider);
      } else {
        for (Expression childExpression : expression.getExpressions()) {
          updateTypeProvider(childExpression, typeProvider);
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

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Method can only be used in source expression
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public static Expression replacePathInSourceExpression(
      Expression expression, PartialPath replacedPath) {
    if (expression instanceof TimeSeriesOperand) {
      return new TimeSeriesOperand(replacedPath);
    } else if (expression instanceof FunctionExpression) {
      return new FunctionExpression(
          ((FunctionExpression) expression).getFunctionName(),
          ((FunctionExpression) expression).getFunctionAttributes(),
          Collections.singletonList(new TimeSeriesOperand(replacedPath)));
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static Expression replacePathInSourceExpression(
      Expression expression, String replacedPathString) {
    PartialPath replacedPath;
    try {
      replacedPath = new PartialPath(replacedPathString);
    } catch (IllegalPathException e) {
      throw new SemanticException("illegal path: " + replacedPathString);
    }
    return replacePathInSourceExpression(expression, replacedPath);
  }

  public static PartialPath getPathInSourceExpression(Expression expression) {
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

  public static String getDeviceNameInSourceExpression(Expression expression) {
    if (expression instanceof TimeSeriesOperand) {
      return ((TimeSeriesOperand) expression).getPath().getDeviceIdString();
    } else if (expression instanceof FunctionExpression) {
      return getDeviceNameInSourceExpression(expression.getExpressions().get(0));
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static Pair<Expression, String> getMeasurementWithAliasInSourceExpression(
      Expression expression, String alias) {
    if (expression instanceof TimeSeriesOperand) {
      String measurement = ((TimeSeriesOperand) expression).getPath().getMeasurement();
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
                  getMeasurementWithAliasInSourceExpression(
                          expression.getExpressions().get(0), alias)
                      .left));
      return new Pair<>(measurementFunctionExpression, alias);
    } else {
      throw new SemanticException(
          "ALIGN BY DEVICE: prefix path in SELECT clause can only be one measurement or one-layer wildcard.");
    }
  }
}
