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
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.udf.builtin.BuiltinScalarFunction;
import org.apache.iotdb.db.constant.SqlConstant;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.BindTypeForTimeSeriesOperandVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.CollectAggregationExpressionsVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.CollectSourceExpressionsVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ConcatDeviceAndRemoveWildcardVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ConcatExpressionWithSuffixPathsVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.GetMeasurementExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.RemoveAliasFromExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.RemoveWildcardInExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.RemoveWildcardInFilterByDeviceVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.RemoveWildcardInFilterVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ReplaceRawPathWithGroupedPathVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.checkConstantSatisfy;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.constructTimeFilter;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.getPairFromBetweenTimeFirst;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.getPairFromBetweenTimeSecond;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.getPairFromBetweenTimeThird;

public class ExpressionAnalyzer {
  /**
   * Check if all suffix paths in expression are measurements or one-level wildcards, used in ALIGN
   * BY DEVICE query or GroupByLevel query. If not, throw a {@link SemanticException}.
   *
   * @param expression expression to be checked
   */
  public static void checkIsAllMeasurement(Expression expression) {
    if (expression instanceof TernaryExpression) {
      checkIsAllMeasurement(((TernaryExpression) expression).getFirstExpression());
      checkIsAllMeasurement(((TernaryExpression) expression).getSecondExpression());
      checkIsAllMeasurement(((TernaryExpression) expression).getThirdExpression());
    } else if (expression instanceof BinaryExpression) {
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
            "the suffix paths can only be measurement or one-level wildcard");
      }
    } else if (expression instanceof TimestampOperand || expression instanceof ConstantOperand) {
      // do nothing
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  /**
   * Identify the expression is a valid built-in aggregation function.
   *
   * @param expression expression to be checked
   * @return true if this expression is valid
   */
  public static ResultColumn.ColumnType identifyOutputColumnType(
      Expression expression, boolean isRoot) {
    if (expression instanceof TernaryExpression) {
      ResultColumn.ColumnType firstType =
          identifyOutputColumnType(((TernaryExpression) expression).getFirstExpression(), false);
      ResultColumn.ColumnType secondType =
          identifyOutputColumnType(((TernaryExpression) expression).getSecondExpression(), false);
      ResultColumn.ColumnType thirdType =
          identifyOutputColumnType(((TernaryExpression) expression).getThirdExpression(), false);
      boolean rawFlag = false, aggregationFlag = false;
      if (firstType == ResultColumn.ColumnType.RAW
          || secondType == ResultColumn.ColumnType.RAW
          || thirdType == ResultColumn.ColumnType.RAW) {
        rawFlag = true;
      }
      if (firstType == ResultColumn.ColumnType.AGGREGATION
          || secondType == ResultColumn.ColumnType.AGGREGATION
          || thirdType == ResultColumn.ColumnType.AGGREGATION) {
        aggregationFlag = true;
      }
      if (rawFlag && aggregationFlag) {
        throw new SemanticException(
            "Raw data and aggregation result hybrid calculation is not supported.");
      }
      if (firstType == ResultColumn.ColumnType.CONSTANT
          && secondType == ResultColumn.ColumnType.CONSTANT
          && thirdType == ResultColumn.ColumnType.CONSTANT) {
        throw new SemanticException("Constant column is not supported.");
      }
      if (firstType != ResultColumn.ColumnType.CONSTANT) {
        return firstType;
      }
      if (secondType != ResultColumn.ColumnType.CONSTANT) {
        return secondType;
      }
      return thirdType;
    } else if (expression instanceof BinaryExpression) {
      ResultColumn.ColumnType leftType =
          identifyOutputColumnType(((BinaryExpression) expression).getLeftExpression(), false);
      ResultColumn.ColumnType rightType =
          identifyOutputColumnType(((BinaryExpression) expression).getRightExpression(), false);
      if ((leftType == ResultColumn.ColumnType.RAW
              && rightType == ResultColumn.ColumnType.AGGREGATION)
          || (leftType == ResultColumn.ColumnType.AGGREGATION
              && rightType == ResultColumn.ColumnType.RAW)) {
        throw new SemanticException(
            "Raw data and aggregation result hybrid calculation is not supported.");
      }
      if (isRoot
          && leftType == ResultColumn.ColumnType.CONSTANT
          && rightType == ResultColumn.ColumnType.CONSTANT) {
        throw new SemanticException("Constant column is not supported.");
      }
      if (leftType != ResultColumn.ColumnType.CONSTANT) {
        return leftType;
      }
      return rightType;
    } else if (expression instanceof UnaryExpression) {
      return identifyOutputColumnType(((UnaryExpression) expression).getExpression(), false);
    } else if (expression instanceof FunctionExpression) {
      List<Expression> inputExpressions = expression.getExpressions();
      if (expression.isBuiltInAggregationFunctionExpression()) {
        for (Expression inputExpression : inputExpressions) {
          if (identifyOutputColumnType(inputExpression, false)
              == ResultColumn.ColumnType.AGGREGATION) {
            throw new SemanticException(
                "Aggregation results cannot be as input of the aggregation function.");
          }
        }
        return ResultColumn.ColumnType.AGGREGATION;
      } else {
        ResultColumn.ColumnType checkedType = null;
        int lastCheckedIndex = 0;
        for (int i = 0; i < inputExpressions.size(); i++) {
          ResultColumn.ColumnType columnType =
              identifyOutputColumnType(inputExpressions.get(i), false);
          if (columnType != ResultColumn.ColumnType.CONSTANT) {
            checkedType = columnType;
            lastCheckedIndex = i;
            break;
          }
        }
        if (checkedType == null) {
          throw new SemanticException(
              String.format(
                  "Input of '%s' is illegal.",
                  ((FunctionExpression) expression).getFunctionName()));
        }
        for (int i = lastCheckedIndex; i < inputExpressions.size(); i++) {
          ResultColumn.ColumnType columnType =
              identifyOutputColumnType(inputExpressions.get(i), false);
          if (columnType != ResultColumn.ColumnType.CONSTANT && columnType != checkedType) {
            throw new SemanticException(
                String.format(
                    "Raw data and aggregation result hybrid input of '%s' is not supported.",
                    ((FunctionExpression) expression).getFunctionName()));
          }
        }
        return checkedType;
      }
    } else if (expression instanceof TimeSeriesOperand || expression instanceof TimestampOperand) {
      return ResultColumn.ColumnType.RAW;
    } else if (expression instanceof ConstantOperand) {
      return ResultColumn.ColumnType.CONSTANT;
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
    return new ConcatExpressionWithSuffixPathsVisitor()
        .process(
            expression,
            new ConcatExpressionWithSuffixPathsVisitor.Context(prefixPaths, patternTree));
  }

  /**
   * Concat suffix path in SELECT or WITHOUT NULL clause with the prefix path in the FROM clause.
   *
   * @param expression expression in SELECT or WITHOUT NULL clause which may include suffix paths
   * @param prefixPaths prefix paths in the FROM clause
   * @return the concatenated partialPath list
   */
  public static List<PartialPath> concatExpressionWithSuffixPaths(
      Expression expression, List<PartialPath> prefixPaths) {
    Set<PartialPath> resultPaths = new HashSet<>();
    if (expression instanceof TernaryExpression) {
      List<PartialPath> firstExpressions =
          concatExpressionWithSuffixPaths(
              ((TernaryExpression) expression).getFirstExpression(), prefixPaths);
      List<PartialPath> secondExpressions =
          concatExpressionWithSuffixPaths(
              ((TernaryExpression) expression).getSecondExpression(), prefixPaths);
      List<PartialPath> thirdExpressions =
          concatExpressionWithSuffixPaths(
              ((TernaryExpression) expression).getThirdExpression(), prefixPaths);
      resultPaths.addAll(firstExpressions);
      resultPaths.addAll(secondExpressions);
      resultPaths.addAll(thirdExpressions);
      return new ArrayList<>(resultPaths);
    } else if (expression instanceof BinaryExpression) {
      List<PartialPath> leftExpressions =
          concatExpressionWithSuffixPaths(
              ((BinaryExpression) expression).getLeftExpression(), prefixPaths);
      List<PartialPath> rightExpressions =
          concatExpressionWithSuffixPaths(
              ((BinaryExpression) expression).getRightExpression(), prefixPaths);
      resultPaths.addAll(leftExpressions);
      resultPaths.addAll(rightExpressions);
      return new ArrayList<>(resultPaths);
    } else if (expression instanceof UnaryExpression) {
      List<PartialPath> childExpressions =
          concatExpressionWithSuffixPaths(
              ((UnaryExpression) expression).getExpression(), prefixPaths);
      resultPaths.addAll(childExpressions);
      return new ArrayList<>(resultPaths);
    } else if (expression instanceof FunctionExpression) {
      for (Expression suffixExpression : expression.getExpressions()) {
        resultPaths.addAll(concatExpressionWithSuffixPaths(suffixExpression, prefixPaths));
      }
      return new ArrayList<>(resultPaths);
    } else if (expression instanceof TimeSeriesOperand) {
      PartialPath rawPath = ((TimeSeriesOperand) expression).getPath();
      List<PartialPath> actualPaths = new ArrayList<>();
      if (rawPath.getFullPath().startsWith(SqlConstant.ROOT + TsFileConstant.PATH_SEPARATOR)) {
        actualPaths.add(rawPath);
      } else {
        for (PartialPath prefixPath : prefixPaths) {
          PartialPath concatPath = prefixPath.concatPath(rawPath);
          actualPaths.add(concatPath);
        }
      }
      return actualPaths;
    } else if (expression instanceof TimestampOperand || expression instanceof ConstantOperand) {
      return new ArrayList<>();
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
  public static void constructPatternTreeFromExpression(
      Expression predicate, List<PartialPath> prefixPaths, PathPatternTree patternTree) {
    if (predicate instanceof TernaryExpression) {
      constructPatternTreeFromExpression(
          ((TernaryExpression) predicate).getFirstExpression(), prefixPaths, patternTree);
      constructPatternTreeFromExpression(
          ((TernaryExpression) predicate).getSecondExpression(), prefixPaths, patternTree);
      constructPatternTreeFromExpression(
          ((TernaryExpression) predicate).getThirdExpression(), prefixPaths, patternTree);
    } else if (predicate instanceof BinaryExpression) {
      constructPatternTreeFromExpression(
          ((BinaryExpression) predicate).getLeftExpression(), prefixPaths, patternTree);
      constructPatternTreeFromExpression(
          ((BinaryExpression) predicate).getRightExpression(), prefixPaths, patternTree);
    } else if (predicate instanceof UnaryExpression) {
      constructPatternTreeFromExpression(
          ((UnaryExpression) predicate).getExpression(), prefixPaths, patternTree);
    } else if (predicate instanceof FunctionExpression) {
      for (Expression suffixExpression : predicate.getExpressions()) {
        constructPatternTreeFromExpression(suffixExpression, prefixPaths, patternTree);
      }
    } else if (predicate instanceof TimeSeriesOperand) {
      PartialPath rawPath = ((TimeSeriesOperand) predicate).getPath();
      if (rawPath.getFullPath().startsWith(SqlConstant.ROOT + TsFileConstant.PATH_SEPARATOR)) {
        patternTree.appendPathPattern(rawPath);
        return;
      }
      for (PartialPath prefixPath : prefixPaths) {
        PartialPath concatPath = prefixPath.concatPath(rawPath);
        patternTree.appendPathPattern(concatPath);
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
      Expression expression, ISchemaTree schemaTree) {
    return new RemoveWildcardInExpressionVisitor().process(expression, schemaTree);
  }

  /**
   * Concat suffix path in WHERE and HAVING clause with the prefix path in the FROM clause. And
   * then, bind schema ({@link PartialPath} -> {@link MeasurementPath}) and removes wildcards in
   * Expression.
   *
   * @param prefixPaths prefix paths in the FROM clause
   * @param schemaTree interface for querying schema information
   * @return the expression list with full path and after binding schema
   */
  public static List<Expression> removeWildcardInFilter(
      Expression predicate, List<PartialPath> prefixPaths, ISchemaTree schemaTree, boolean isRoot) {
    return new RemoveWildcardInFilterVisitor()
        .process(
            predicate, new RemoveWildcardInFilterVisitor.Context(prefixPaths, schemaTree, isRoot));
  }

  public static Expression replaceRawPathWithGroupedPath(
      Expression expression,
      GroupByLevelController.RawPathToGroupedPathMap rawPathToGroupedPathMap) {
    return new ReplaceRawPathWithGroupedPathVisitor().process(expression, rawPathToGroupedPathMap);
  }

  /**
   * Concat expression with the device path in the FROM clause.And then, bind schema ({@link
   * PartialPath} -> {@link MeasurementPath}) and removes wildcards in Expression. This method used
   * in ALIGN BY DEVICE query.
   *
   * @param devicePath device path in the FROM clause
   * @return expression list with full path and after binding schema
   */
  public static List<Expression> concatDeviceAndRemoveWildcard(
      Expression expression, PartialPath devicePath, ISchemaTree schemaTree) {
    return new ConcatDeviceAndRemoveWildcardVisitor()
        .process(
            expression, new ConcatDeviceAndRemoveWildcardVisitor.Context(devicePath, schemaTree));
  }

  /**
   * Concat measurement in WHERE and HAVING clause with device path. And then, bind schema ({@link
   * PartialPath} -> {@link MeasurementPath}) and removes wildcards.
   *
   * @return the expression list with full path and after binding schema
   */
  public static List<Expression> removeWildcardInFilterByDevice(
      Expression predicate, PartialPath devicePath, ISchemaTree schemaTree, boolean isWhere) {
    return new RemoveWildcardInFilterByDeviceVisitor()
        .process(
            predicate,
            new RemoveWildcardInFilterByDeviceVisitor.Context(devicePath, schemaTree, isWhere));
  }

  /**
   * Extract global time filter from query filter.
   *
   * @param predicate raw query filter
   * @param canRewrite determined by the father of current expression
   * @param isFirstOr whether it is the first LogicOrExpression encountered
   * @return global time filter
   */
  public static Pair<Filter, Boolean> extractGlobalTimeFilter(
      Expression predicate, boolean canRewrite, boolean isFirstOr) {
    if (predicate.getExpressionType().equals(ExpressionType.LOGIC_AND)) {
      Pair<Filter, Boolean> leftResultPair =
          extractGlobalTimeFilter(
              ((BinaryExpression) predicate).getLeftExpression(), canRewrite, isFirstOr);
      Pair<Filter, Boolean> rightResultPair =
          extractGlobalTimeFilter(
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
            FilterFactory.and(leftResultPair.left, rightResultPair.left),
            leftResultPair.right || rightResultPair.right);
      } else if (leftResultPair.left != null) {
        return new Pair<>(leftResultPair.left, true);
      } else if (rightResultPair.left != null) {
        return new Pair<>(rightResultPair.left, true);
      }
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.LOGIC_OR)) {
      Pair<Filter, Boolean> leftResultPair =
          extractGlobalTimeFilter(((BinaryExpression) predicate).getLeftExpression(), false, false);
      Pair<Filter, Boolean> rightResultPair =
          extractGlobalTimeFilter(
              ((BinaryExpression) predicate).getRightExpression(), false, false);

      if (leftResultPair.left != null && rightResultPair.left != null) {
        if (isFirstOr && !leftResultPair.right && !rightResultPair.right) {
          ((BinaryExpression) predicate)
              .setLeftExpression(new ConstantOperand(TSDataType.BOOLEAN, "true"));
          ((BinaryExpression) predicate)
              .setRightExpression(new ConstantOperand(TSDataType.BOOLEAN, "true"));
        }
        return new Pair<>(
            FilterFactory.or(leftResultPair.left, rightResultPair.left),
            leftResultPair.right || rightResultPair.right);
      }
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.LOGIC_NOT)) {
      Pair<Filter, Boolean> childResultPair =
          extractGlobalTimeFilter(
              ((UnaryExpression) predicate).getExpression(), canRewrite, isFirstOr);
      if (childResultPair.left != null) {
        return new Pair<>(FilterFactory.not(childResultPair.left), childResultPair.right);
      }
      return new Pair<>(null, true);
    } else if (predicate.isCompareBinaryExpression()) {
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
    } else if (predicate.getExpressionType().equals(ExpressionType.LIKE)
        || predicate.getExpressionType().equals(ExpressionType.REGEXP)) {
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.BETWEEN)) {
      Expression firstExpression = ((TernaryExpression) predicate).getFirstExpression();
      Expression secondExpression = ((TernaryExpression) predicate).getSecondExpression();
      Expression thirdExpression = ((TernaryExpression) predicate).getThirdExpression();
      if (firstExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
        return getPairFromBetweenTimeFirst(
            secondExpression, thirdExpression, ((BetweenExpression) predicate).isNotBetween());
      } else if (secondExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
        if (checkConstantSatisfy(firstExpression, thirdExpression)) {
          return getPairFromBetweenTimeSecond((BetweenExpression) predicate, firstExpression);
        } else {
          return new Pair<>(null, true); // TODO return Filter.True/False
        }
      } else if (thirdExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
        if (checkConstantSatisfy(secondExpression, firstExpression)) {
          return getPairFromBetweenTimeThird((BetweenExpression) predicate, firstExpression);
        } else {
          return new Pair<>(null, true); // TODO return Filter.True/False
        }
      }
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.IS_NULL)) {
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.IN)) {
      Expression timeExpression = ((InExpression) predicate).getExpression();
      if (timeExpression.getExpressionType().equals(ExpressionType.TIMESTAMP)) {
        return new Pair<>(
            TimeFilter.in(
                ((InExpression) predicate)
                    .getValues().stream().map(Long::parseLong).collect(Collectors.toSet()),
                ((InExpression) predicate).isNotIn()),
            false);
      }
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.TIMESERIES)) {
      return new Pair<>(null, true);
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }

  public static boolean checkIfTimeFilterExist(Expression predicate) {
    if (predicate instanceof TernaryExpression) {
      return checkIfTimeFilterExist(((TernaryExpression) predicate).getFirstExpression())
          || checkIfTimeFilterExist(((TernaryExpression) predicate).getSecondExpression())
          || checkIfTimeFilterExist(((TernaryExpression) predicate).getThirdExpression());
    } else if (predicate instanceof BinaryExpression) {
      return checkIfTimeFilterExist(((BinaryExpression) predicate).getLeftExpression())
          || checkIfTimeFilterExist(((BinaryExpression) predicate).getRightExpression());
    } else if (predicate instanceof UnaryExpression) {
      return checkIfTimeFilterExist(((UnaryExpression) predicate).getExpression());
    } else if (predicate instanceof FunctionExpression) {
      boolean timeFilterExist = false;
      for (Expression childExpression : predicate.getExpressions()) {
        timeFilterExist = timeFilterExist || checkIfTimeFilterExist(childExpression);
      }
      return timeFilterExist;
    } else if (predicate instanceof TimeSeriesOperand || predicate instanceof ConstantOperand) {
      return false;
    } else if (predicate instanceof TimestampOperand) {
      return true;
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }

  /**
   * Search for subexpressions that can be queried natively, including all time series.
   *
   * @param expression expression to be searched
   * @return searched subexpression list
   */
  public static List<Expression> searchSourceExpressions(Expression expression) {
    return new CollectSourceExpressionsVisitor().process(expression, null);
  }

  /**
   * Search for built-in aggregate functions subexpressions.
   *
   * @param expression expression to be searched
   * @return searched aggregate functions list
   */
  public static List<Expression> searchAggregationExpressions(Expression expression) {
    return new CollectAggregationExpressionsVisitor().process(expression, null);
  }

  /**
   * Remove alias from expression. eg: root.sg.d1.status + 1 -> root.sg.d1.s2 + 1, and reconstruct
   * the name of FunctionExpression to lowercase.
   *
   * @return expression after removing alias
   */
  public static Expression removeAliasFromExpression(Expression expression) {
    return new RemoveAliasFromExpressionVisitor().process(expression, null);
  }

  /** Check for arithmetic expression, logical expression, UDF. Returns true if it exists. */
  public static boolean checkIsNeedTransform(Expression expression) {
    if (expression instanceof TernaryExpression) {
      return true;
    } else if (expression instanceof BinaryExpression) {
      return true;
    } else if (expression instanceof UnaryExpression) {
      return true;
    } else if (expression instanceof FunctionExpression) {
      return !expression.isBuiltInAggregationFunctionExpression();
    } else if (expression instanceof TimeSeriesOperand) {
      return false;
    } else if (expression instanceof ConstantOperand) {
      return false;
    } else if (expression instanceof NullOperand) {
      return true;
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Method can only be used in source expression
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public static String getDeviceNameInSourceExpression(Expression expression) {
    if (!(expression instanceof TimeSeriesOperand)) {
      throw new IllegalArgumentException(
          "unsupported expression type for source expression: " + expression.getExpressionType());
    }
    return ((TimeSeriesOperand) expression).getPath().getDevice();
  }

  public static Expression getMeasurementExpression(Expression expression) {
    return new GetMeasurementExpressionVisitor().process(expression, null);
  }

  public static Expression evaluatePredicate(Expression predicate) {
    if (predicate.getExpressionType().equals(ExpressionType.LOGIC_AND)) {
      Expression left = evaluatePredicate(((BinaryExpression) predicate).getLeftExpression());
      Expression right = evaluatePredicate(((BinaryExpression) predicate).getRightExpression());
      boolean isLeftTrue =
          left.isConstantOperand() && Boolean.parseBoolean(left.getExpressionString());
      boolean isRightTrue =
          right.isConstantOperand() && Boolean.parseBoolean(right.getExpressionString());
      if (isLeftTrue && isRightTrue) {
        return new ConstantOperand(TSDataType.BOOLEAN, "true");
      } else if (isLeftTrue) {
        return right;
      } else if (isRightTrue) {
        return left;
      }
      return predicate;
    } else if (predicate.getExpressionType().equals(ExpressionType.LOGIC_OR)) {
      Expression left = evaluatePredicate(((BinaryExpression) predicate).getLeftExpression());
      Expression right = evaluatePredicate(((BinaryExpression) predicate).getRightExpression());
      boolean isLeftTrue =
          left.isConstantOperand() && Boolean.parseBoolean(left.getExpressionString());
      boolean isRightTrue =
          right.isConstantOperand() && Boolean.parseBoolean(right.getExpressionString());
      if (isRightTrue || isLeftTrue) {
        return new ConstantOperand(TSDataType.BOOLEAN, "true");
      }
    }
    return predicate;
  }

  /**
   * Bind DataType for TimeSeriesOperand in Expression with according ColumnHeaderName. eg:
   *
   * <p>columnHeaders: [[QueryId, TEXT], [DataNodeId, INT32]...]
   *
   * <p>dataNodeID > 1 -> DataNodeId > 1, `DataNodeID` will be a MeasurementPath with INT32
   *
   * <p>errorInput > 1, no according ColumnHeaderName of `errorInput`, throw exception
   */
  public static Expression bindTypeForTimeSeriesOperand(
      Expression predicate, List<ColumnHeader> columnHeaders) {
    return new BindTypeForTimeSeriesOperandVisitor().process(predicate, columnHeaders);
  }

  public static boolean isDeviceViewNeedSpecialProcess(Expression expression) {
    if (expression instanceof TernaryExpression) {
      TernaryExpression ternaryExpression = (TernaryExpression) expression;
      return isDeviceViewNeedSpecialProcess(ternaryExpression.getFirstExpression())
          || isDeviceViewNeedSpecialProcess(ternaryExpression.getSecondExpression())
          || isDeviceViewNeedSpecialProcess(ternaryExpression.getThirdExpression());
    } else if (expression instanceof BinaryExpression) {
      BinaryExpression binaryExpression = (BinaryExpression) expression;
      return isDeviceViewNeedSpecialProcess(binaryExpression.getLeftExpression())
          || isDeviceViewNeedSpecialProcess(binaryExpression.getRightExpression());
    } else if (expression instanceof UnaryExpression) {
      return isDeviceViewNeedSpecialProcess(((UnaryExpression) expression).getExpression());
    } else if (expression instanceof FunctionExpression) {
      if (((FunctionExpression) expression).isBuiltInScalarFunction()
          && BuiltinScalarFunction.DEVICE_VIEW_SPECIAL_PROCESS_FUNCTIONS.contains(
              ((FunctionExpression) expression).getFunctionName().toLowerCase())) {
        return true;
      }
      for (Expression child : expression.getExpressions()) {
        if (isDeviceViewNeedSpecialProcess(child)) {
          return true;
        }
      }
      return false;
    } else if (expression instanceof LeafOperand) {
      return false;
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static boolean checkIsScalarExpression(Expression expression, Analysis analysis) {
    if (expression instanceof TernaryExpression) {
      TernaryExpression ternaryExpression = (TernaryExpression) expression;
      return checkIsScalarExpression(ternaryExpression.getFirstExpression(), analysis)
          && checkIsScalarExpression(ternaryExpression.getSecondExpression(), analysis)
          && checkIsScalarExpression(ternaryExpression.getThirdExpression(), analysis);
    } else if (expression instanceof BinaryExpression) {
      BinaryExpression binaryExpression = (BinaryExpression) expression;
      return checkIsScalarExpression(binaryExpression.getLeftExpression(), analysis)
          && checkIsScalarExpression(binaryExpression.getRightExpression(), analysis);
    } else if (expression instanceof UnaryExpression) {
      return checkIsScalarExpression(((UnaryExpression) expression).getExpression(), analysis);
    } else if (expression instanceof FunctionExpression) {
      FunctionExpression functionExpression = (FunctionExpression) expression;
      if (!functionExpression.isMappable(analysis.getExpressionTypes())
          || BuiltinScalarFunction.DEVICE_VIEW_SPECIAL_PROCESS_FUNCTIONS.contains(
              functionExpression.getFunctionName())) {
        return false;
      }
      List<Expression> inputExpressions = functionExpression.getExpressions();
      for (Expression inputExpression : inputExpressions) {
        if (!checkIsScalarExpression(inputExpression, analysis)) {
          return false;
        }
      }
      return true;
    } else if (expression instanceof LeafOperand) {
      return true;
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }
}
