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
import org.apache.iotdb.db.exception.sql.SemanticException;
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
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.cartesianProduct;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.checkConstantSatisfy;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.constructTimeFilter;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.getPairFromBetweenTimeFirst;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.getPairFromBetweenTimeSecond;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.getPairFromBetweenTimeThird;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructBinaryExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructBinaryExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructFunctionExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructFunctionExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTernaryExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTernaryExpressions;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperand;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperands;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructUnaryExpression;
import static org.apache.iotdb.db.mpp.plan.analyze.ExpressionUtils.reconstructUnaryExpressions;

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
    if (expression instanceof TernaryExpression) {
      List<Expression> firstExpressions =
          concatExpressionWithSuffixPaths(
              ((TernaryExpression) expression).getFirstExpression(), prefixPaths, patternTree);
      List<Expression> secondExpressions =
          concatExpressionWithSuffixPaths(
              ((TernaryExpression) expression).getSecondExpression(), prefixPaths, patternTree);
      List<Expression> thirdExpressions =
          concatExpressionWithSuffixPaths(
              ((TernaryExpression) expression).getThirdExpression(), prefixPaths, patternTree);
      return reconstructTernaryExpressions(
          expression, firstExpressions, secondExpressions, thirdExpressions);
    } else if (expression instanceof BinaryExpression) {
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
        patternTree.appendPathPattern(rawPath);
      } else {
        for (PartialPath prefixPath : prefixPaths) {
          PartialPath concatPath = prefixPath.concatPath(rawPath);
          patternTree.appendPathPattern(concatPath);
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
      if (rawPath.getFullPath().startsWith(SQLConstant.ROOT + TsFileConstant.PATH_SEPARATOR)) {
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
      if (rawPath.getFullPath().startsWith(SQLConstant.ROOT + TsFileConstant.PATH_SEPARATOR)) {
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
    if (expression instanceof TernaryExpression) {
      List<Expression> firstExpressions =
          removeWildcardInExpression(
              ((TernaryExpression) expression).getFirstExpression(), schemaTree);
      List<Expression> secondExpressions =
          removeWildcardInExpression(
              ((TernaryExpression) expression).getSecondExpression(), schemaTree);
      List<Expression> thirdExpressions =
          removeWildcardInExpression(
              ((TernaryExpression) expression).getThirdExpression(), schemaTree);
      return reconstructTernaryExpressions(
          expression, firstExpressions, secondExpressions, thirdExpressions);
    } else if (expression instanceof BinaryExpression) {
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
   * Concat suffix path in WHERE and HAVING clause with the prefix path in the FROM clause. And
   * then, bind schema ({@link PartialPath} -> {@link MeasurementPath}) and removes wildcards in
   * Expression.
   *
   * @param prefixPaths prefix paths in the FROM clause
   * @param schemaTree interface for querying schema information
   * @return the expression list with full path and after binding schema
   */
  public static List<Expression> removeWildcardInFilter(
      Expression predicate,
      List<PartialPath> prefixPaths,
      ISchemaTree schemaTree,
      boolean isWhere) {
    if (predicate instanceof TernaryExpression) {
      List<Expression> firstExpressions =
          removeWildcardInFilter(
              ((TernaryExpression) predicate).getFirstExpression(),
              prefixPaths,
              schemaTree,
              isWhere);
      List<Expression> secondExpressions =
          removeWildcardInFilter(
              ((TernaryExpression) predicate).getSecondExpression(),
              prefixPaths,
              schemaTree,
              isWhere);
      List<Expression> thirdExpressions =
          removeWildcardInFilter(
              ((TernaryExpression) predicate).getThirdExpression(),
              prefixPaths,
              schemaTree,
              isWhere);
      return reconstructTernaryExpressions(
          predicate, firstExpressions, secondExpressions, thirdExpressions);
    } else if (predicate instanceof BinaryExpression) {
      List<Expression> leftExpressions =
          removeWildcardInFilter(
              ((BinaryExpression) predicate).getLeftExpression(), prefixPaths, schemaTree, isWhere);
      List<Expression> rightExpressions =
          removeWildcardInFilter(
              ((BinaryExpression) predicate).getRightExpression(),
              prefixPaths,
              schemaTree,
              isWhere);
      if (predicate.getExpressionType() == ExpressionType.LOGIC_AND) {
        List<Expression> resultExpressions = new ArrayList<>(leftExpressions);
        resultExpressions.addAll(rightExpressions);
        return resultExpressions;
      }
      return reconstructBinaryExpressions(
          predicate.getExpressionType(), leftExpressions, rightExpressions);
    } else if (predicate instanceof UnaryExpression) {
      List<Expression> childExpressions =
          removeWildcardInFilter(
              ((UnaryExpression) predicate).getExpression(), prefixPaths, schemaTree, isWhere);
      return reconstructUnaryExpressions((UnaryExpression) predicate, childExpressions);
    } else if (predicate instanceof FunctionExpression) {
      if (predicate.isBuiltInAggregationFunctionExpression() && isWhere) {
        throw new SemanticException("aggregate functions are not supported in WHERE clause");
      }
      List<List<Expression>> extendedExpressions = new ArrayList<>();
      for (Expression suffixExpression : predicate.getExpressions()) {
        extendedExpressions.add(
            removeWildcardInFilter(suffixExpression, prefixPaths, schemaTree, isWhere));
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

      List<PartialPath> noStarPaths = new ArrayList<>();
      for (PartialPath concatPath : concatPaths) {
        List<MeasurementPath> actualPaths = schemaTree.searchMeasurementPaths(concatPath).left;
        if (actualPaths.size() == 0) {
          return Collections.singletonList(new NullOperand());
        }
        noStarPaths.addAll(actualPaths);
      }
      return reconstructTimeSeriesOperands(noStarPaths);
    } else if (predicate instanceof LeafOperand) {
      // do nothing in the case of "where time > 5"
      return Collections.singletonList(predicate);
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }

  public static Expression replaceRawPathWithGroupedPath(
      Expression expression,
      GroupByLevelController.RawPathToGroupedPathMap rawPathToGroupedPathMap) {
    if (expression instanceof TernaryExpression) {
      Expression firstExpression =
          replaceRawPathWithGroupedPath(
              ((TernaryExpression) expression).getFirstExpression(), rawPathToGroupedPathMap);
      Expression secondExpression =
          replaceRawPathWithGroupedPath(
              ((TernaryExpression) expression).getSecondExpression(), rawPathToGroupedPathMap);
      Expression thirdExpression =
          replaceRawPathWithGroupedPath(
              ((TernaryExpression) expression).getThirdExpression(), rawPathToGroupedPathMap);
      return reconstructTernaryExpression(
          expression, firstExpression, secondExpression, thirdExpression);
    } else if (expression instanceof BinaryExpression) {
      Expression leftExpression =
          replaceRawPathWithGroupedPath(
              ((BinaryExpression) expression).getLeftExpression(), rawPathToGroupedPathMap);
      Expression rightExpression =
          replaceRawPathWithGroupedPath(
              ((BinaryExpression) expression).getRightExpression(), rawPathToGroupedPathMap);
      return reconstructBinaryExpression(
          expression.getExpressionType(), leftExpression, rightExpression);
    } else if (expression instanceof UnaryExpression) {
      Expression childExpression =
          replaceRawPathWithGroupedPath(
              ((UnaryExpression) expression).getExpression(), rawPathToGroupedPathMap);
      return reconstructUnaryExpression((UnaryExpression) expression, childExpression);
    } else if (expression instanceof FunctionExpression) {
      List<Expression> childrenExpressions = new ArrayList<>();
      for (Expression childExpression : expression.getExpressions()) {
        childrenExpressions.add(
            replaceRawPathWithGroupedPath(childExpression, rawPathToGroupedPathMap));
      }
      return reconstructFunctionExpression((FunctionExpression) expression, childrenExpressions);
    } else if (expression instanceof TimeSeriesOperand) {
      PartialPath rawPath = ((TimeSeriesOperand) expression).getPath();
      PartialPath groupedPath = rawPathToGroupedPathMap.get(rawPath);
      return reconstructTimeSeriesOperand(groupedPath);
    } else if (expression instanceof TimestampOperand || expression instanceof ConstantOperand) {
      return expression;
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
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
    if (expression instanceof TernaryExpression) {
      List<Expression> firstExpressions =
          concatDeviceAndRemoveWildcard(
              ((TernaryExpression) expression).getFirstExpression(), devicePath, schemaTree);
      List<Expression> secondExpressions =
          concatDeviceAndRemoveWildcard(
              ((TernaryExpression) expression).getSecondExpression(), devicePath, schemaTree);
      List<Expression> thirdExpressions =
          concatDeviceAndRemoveWildcard(
              ((TernaryExpression) expression).getThirdExpression(), devicePath, schemaTree);
      return reconstructTernaryExpressions(
          expression, firstExpressions, secondExpressions, thirdExpressions);
    } else if (expression instanceof BinaryExpression) {
      List<Expression> leftExpressions =
          concatDeviceAndRemoveWildcard(
              ((BinaryExpression) expression).getLeftExpression(), devicePath, schemaTree);
      List<Expression> rightExpressions =
          concatDeviceAndRemoveWildcard(
              ((BinaryExpression) expression).getRightExpression(), devicePath, schemaTree);
      return reconstructBinaryExpressions(
          expression.getExpressionType(), leftExpressions, rightExpressions);
    } else if (expression instanceof UnaryExpression) {
      List<Expression> childExpressions =
          concatDeviceAndRemoveWildcard(
              ((UnaryExpression) expression).getExpression(), devicePath, schemaTree);
      return reconstructUnaryExpressions((UnaryExpression) expression, childExpressions);
    } else if (expression instanceof FunctionExpression) {
      List<List<Expression>> extendedExpressions = new ArrayList<>();
      for (Expression suffixExpression : expression.getExpressions()) {
        List<Expression> concatedExpression =
            concatDeviceAndRemoveWildcard(suffixExpression, devicePath, schemaTree);
        if (concatedExpression != null && concatedExpression.size() != 0) {
          extendedExpressions.add(concatedExpression);
        }
      }
      List<List<Expression>> childExpressionsList = new ArrayList<>();
      cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
      return reconstructFunctionExpressions((FunctionExpression) expression, childExpressionsList);
    } else if (expression instanceof TimeSeriesOperand) {
      PartialPath measurement = ((TimeSeriesOperand) expression).getPath();
      PartialPath concatPath = devicePath.concatPath(measurement);

      List<MeasurementPath> actualPaths = schemaTree.searchMeasurementPaths(concatPath).left;
      if (actualPaths.isEmpty()) {
        return Collections.emptyList();
      }
      List<PartialPath> noStarPaths = new ArrayList<>(actualPaths);
      return reconstructTimeSeriesOperands(noStarPaths);
    } else if (expression instanceof TimestampOperand) {
      // do nothing in the case of "where time > 5"
      return Collections.singletonList(expression);
    } else if (expression instanceof ConstantOperand) {
      return Collections.singletonList(expression);
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  /**
   * Concat measurement in WHERE and HAVING clause with device path. And then, bind schema ({@link
   * PartialPath} -> {@link MeasurementPath}) and removes wildcards.
   *
   * @return the expression list with full path and after binding schema
   */
  public static List<Expression> removeWildcardInFilterByDevice(
      Expression predicate, PartialPath devicePath, ISchemaTree schemaTree, boolean isWhere) {
    if (predicate instanceof TernaryExpression) {
      List<Expression> firstExpressions =
          removeWildcardInFilterByDevice(
              ((TernaryExpression) predicate).getFirstExpression(),
              devicePath,
              schemaTree,
              isWhere);
      List<Expression> secondExpressions =
          removeWildcardInFilterByDevice(
              ((TernaryExpression) predicate).getSecondExpression(),
              devicePath,
              schemaTree,
              isWhere);
      List<Expression> thirdExpressions =
          removeWildcardInFilterByDevice(
              ((TernaryExpression) predicate).getThirdExpression(),
              devicePath,
              schemaTree,
              isWhere);
      return reconstructTernaryExpressions(
          predicate, firstExpressions, secondExpressions, thirdExpressions);
    } else if (predicate instanceof BinaryExpression) {
      List<Expression> leftExpressions =
          removeWildcardInFilterByDevice(
              ((BinaryExpression) predicate).getLeftExpression(), devicePath, schemaTree, isWhere);
      List<Expression> rightExpressions =
          removeWildcardInFilterByDevice(
              ((BinaryExpression) predicate).getRightExpression(), devicePath, schemaTree, isWhere);
      if (predicate.getExpressionType() == ExpressionType.LOGIC_AND) {
        List<Expression> resultExpressions = new ArrayList<>(leftExpressions);
        resultExpressions.addAll(rightExpressions);
        return resultExpressions;
      }
      return reconstructBinaryExpressions(
          predicate.getExpressionType(), leftExpressions, rightExpressions);
    } else if (predicate instanceof UnaryExpression) {
      List<Expression> childExpressions =
          removeWildcardInFilterByDevice(
              ((UnaryExpression) predicate).getExpression(), devicePath, schemaTree, isWhere);
      return reconstructUnaryExpressions((UnaryExpression) predicate, childExpressions);
    } else if (predicate instanceof FunctionExpression) {
      if (predicate.isBuiltInAggregationFunctionExpression() && isWhere) {
        throw new SemanticException("aggregate functions are not supported in WHERE clause");
      }
      List<List<Expression>> extendedExpressions = new ArrayList<>();
      for (Expression suffixExpression : predicate.getExpressions()) {
        extendedExpressions.add(
            removeWildcardInFilterByDevice(suffixExpression, devicePath, schemaTree, isWhere));
      }
      List<List<Expression>> childExpressionsList = new ArrayList<>();
      cartesianProduct(extendedExpressions, childExpressionsList, 0, new ArrayList<>());
      return reconstructFunctionExpressions((FunctionExpression) predicate, childExpressionsList);
    } else if (predicate instanceof TimeSeriesOperand) {
      PartialPath measurement = ((TimeSeriesOperand) predicate).getPath();
      PartialPath concatPath = devicePath.concatPath(measurement);

      List<MeasurementPath> noStarPaths = schemaTree.searchMeasurementPaths(concatPath).left;
      if (noStarPaths.size() == 0) {
        return Collections.singletonList(new NullOperand());
      }
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
    if (expression instanceof TernaryExpression) {
      List<Expression> resultExpressions = new ArrayList<>();
      resultExpressions.addAll(
          searchSourceExpressions(((TernaryExpression) expression).getFirstExpression()));
      resultExpressions.addAll(
          searchSourceExpressions(((TernaryExpression) expression).getSecondExpression()));
      resultExpressions.addAll(
          searchSourceExpressions(((TernaryExpression) expression).getThirdExpression()));
      return resultExpressions;
    } else if (expression instanceof BinaryExpression) {
      List<Expression> resultExpressions = new ArrayList<>();
      resultExpressions.addAll(
          searchSourceExpressions(((BinaryExpression) expression).getLeftExpression()));
      resultExpressions.addAll(
          searchSourceExpressions(((BinaryExpression) expression).getRightExpression()));
      return resultExpressions;
    } else if (expression instanceof UnaryExpression) {
      return searchSourceExpressions(((UnaryExpression) expression).getExpression());
    } else if (expression instanceof FunctionExpression) {
      List<Expression> resultExpressions = new ArrayList<>();
      for (Expression childExpression : expression.getExpressions()) {
        resultExpressions.addAll(searchSourceExpressions(childExpression));
      }
      return resultExpressions;
    } else if (expression instanceof TimeSeriesOperand) {
      return Collections.singletonList(expression);
    } else if (expression instanceof LeafOperand) {
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
    if (expression instanceof TernaryExpression) {
      List<Expression> resultExpressions = new ArrayList<>();
      resultExpressions.addAll(
          searchAggregationExpressions(((TernaryExpression) expression).getFirstExpression()));
      resultExpressions.addAll(
          searchAggregationExpressions(((TernaryExpression) expression).getSecondExpression()));
      resultExpressions.addAll(
          searchAggregationExpressions(((TernaryExpression) expression).getThirdExpression()));
      return resultExpressions;
    } else if (expression instanceof BinaryExpression) {
      List<Expression> resultExpressions = new ArrayList<>();
      resultExpressions.addAll(
          searchAggregationExpressions(((BinaryExpression) expression).getLeftExpression()));
      resultExpressions.addAll(
          searchAggregationExpressions(((BinaryExpression) expression).getRightExpression()));
      return resultExpressions;
    } else if (expression instanceof UnaryExpression) {
      return searchAggregationExpressions(((UnaryExpression) expression).getExpression());
    } else if (expression instanceof FunctionExpression) {
      if (expression.isBuiltInAggregationFunctionExpression()) {
        return Collections.singletonList(expression);
      }

      List<Expression> resultExpressions = new ArrayList<>();
      for (Expression inputExpression : expression.getExpressions()) {
        resultExpressions.addAll(searchAggregationExpressions(inputExpression));
      }
      return resultExpressions;
    } else if (expression instanceof LeafOperand) {
      return Collections.emptyList();
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  /**
   * Remove alias from expression. eg: root.sg.d1.status + 1 -> root.sg.d1.s2 + 1, and reconstruct
   * the name of FunctionExpression to lowercase.
   *
   * @return expression after removing alias
   */
  public static Expression removeAliasFromExpression(Expression expression) {
    if (expression instanceof TernaryExpression) {
      Expression firstExpression =
          removeAliasFromExpression(((TernaryExpression) expression).getFirstExpression());
      Expression secondExpression =
          removeAliasFromExpression(((TernaryExpression) expression).getSecondExpression());
      Expression thirdExpression =
          removeAliasFromExpression(((TernaryExpression) expression).getThirdExpression());
      return reconstructTernaryExpressions(
              expression,
              Collections.singletonList(firstExpression),
              Collections.singletonList(secondExpression),
              Collections.singletonList(thirdExpression))
          .get(0);
    } else if (expression instanceof BinaryExpression) {
      Expression leftExpression =
          removeAliasFromExpression(((BinaryExpression) expression).getLeftExpression());
      Expression rightExpression =
          removeAliasFromExpression(((BinaryExpression) expression).getRightExpression());
      return reconstructBinaryExpressions(
              expression.getExpressionType(),
              Collections.singletonList(leftExpression),
              Collections.singletonList(rightExpression))
          .get(0);
    } else if (expression instanceof UnaryExpression) {
      Expression childExpression =
          removeAliasFromExpression(((UnaryExpression) expression).getExpression());
      return reconstructUnaryExpressions(
              (UnaryExpression) expression, Collections.singletonList(childExpression))
          .get(0);
    } else if (expression instanceof FunctionExpression) {
      FunctionExpression functionExpression = (FunctionExpression) expression;
      List<Expression> childExpressions = new ArrayList<>();
      for (Expression suffixExpression : expression.getExpressions()) {
        childExpressions.add(removeAliasFromExpression(suffixExpression));
      }
      // Reconstruct the function name to lower case to finish the calculation afterwards while the
      // origin name will be only as output name
      return new FunctionExpression(
          functionExpression.getFunctionName().toLowerCase(),
          functionExpression.getFunctionAttributes(),
          childExpressions);
    } else if (expression instanceof TimeSeriesOperand) {
      PartialPath rawPath = ((TimeSeriesOperand) expression).getPath();
      if (rawPath.isMeasurementAliasExists()) {
        MeasurementPath measurementPath = (MeasurementPath) rawPath;
        MeasurementPath newPath =
            new MeasurementPath(measurementPath, measurementPath.getMeasurementSchema());
        newPath.setUnderAlignedEntity(measurementPath.isUnderAlignedEntity());
        return new TimeSeriesOperand(newPath);
      }
      return expression;
    } else if (expression instanceof LeafOperand) {
      // do nothing
      return expression;
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
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
    if (expression instanceof TernaryExpression) {
      String DeviceName =
          getDeviceNameInSourceExpression(((TernaryExpression) expression).getFirstExpression());
      if (DeviceName == null) {
        DeviceName =
            getDeviceNameInSourceExpression(((TernaryExpression) expression).getSecondExpression());
      }
      if (DeviceName == null) {
        DeviceName =
            getDeviceNameInSourceExpression(((TernaryExpression) expression).getThirdExpression());
      }
      return DeviceName;
    } else if (expression instanceof BinaryExpression) {
      String leftDeviceName =
          getDeviceNameInSourceExpression(((BinaryExpression) expression).getLeftExpression());
      if (leftDeviceName != null) {
        return leftDeviceName;
      }
      return getDeviceNameInSourceExpression(((BinaryExpression) expression).getRightExpression());
    } else if (expression instanceof UnaryExpression) {
      return getDeviceNameInSourceExpression(((UnaryExpression) expression).getExpression());
    } else if (expression instanceof TimeSeriesOperand) {
      return ((TimeSeriesOperand) expression).getPath().getDevice();
    } else if (expression instanceof FunctionExpression) {
      return getDeviceNameInSourceExpression(expression.getExpressions().get(0));
    } else if (expression instanceof ConstantOperand || expression instanceof TimestampOperand) {
      return null;
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
  }

  public static Expression getMeasurementExpression(Expression expression) {
    if (expression instanceof TernaryExpression) {
      Expression firstExpression =
          getMeasurementExpression(((TernaryExpression) expression).getFirstExpression());
      Expression secondExpression =
          getMeasurementExpression(((TernaryExpression) expression).getSecondExpression());
      Expression thirdExpression =
          getMeasurementExpression(((TernaryExpression) expression).getThirdExpression());
      return reconstructTernaryExpressions(
              expression,
              Collections.singletonList(firstExpression),
              Collections.singletonList(secondExpression),
              Collections.singletonList(thirdExpression))
          .get(0);
    } else if (expression instanceof BinaryExpression) {
      Expression leftExpression =
          getMeasurementExpression(((BinaryExpression) expression).getLeftExpression());
      Expression rightExpression =
          getMeasurementExpression(((BinaryExpression) expression).getRightExpression());
      return reconstructBinaryExpressions(
              expression.getExpressionType(),
              Collections.singletonList(leftExpression),
              Collections.singletonList(rightExpression))
          .get(0);
    } else if (expression instanceof UnaryExpression) {
      Expression childExpression =
          getMeasurementExpression(((UnaryExpression) expression).getExpression());
      return reconstructUnaryExpressions(
              (UnaryExpression) expression, Collections.singletonList(childExpression))
          .get(0);
    } else if (expression instanceof FunctionExpression) {
      List<Expression> childExpressions = new ArrayList<>();
      for (Expression suffixExpression : expression.getExpressions()) {
        childExpressions.add(getMeasurementExpression(suffixExpression));
      }
      return new FunctionExpression(
          ((FunctionExpression) expression).getFunctionName(),
          ((FunctionExpression) expression).getFunctionAttributes(),
          childExpressions);
    } else if (expression instanceof TimeSeriesOperand) {
      MeasurementPath rawPath = (MeasurementPath) ((TimeSeriesOperand) expression).getPath();
      PartialPath measurement = new PartialPath(rawPath.getMeasurement(), false);
      MeasurementPath measurementWithSchema =
          new MeasurementPath(measurement, rawPath.getMeasurementSchema());
      if (rawPath.isMeasurementAliasExists()) {
        measurementWithSchema.setMeasurementAlias(rawPath.getMeasurementAlias());
      }
      return new TimeSeriesOperand(measurementWithSchema);
    } else if (expression instanceof LeafOperand) {
      return expression;
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + expression.getExpressionType());
    }
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
}
