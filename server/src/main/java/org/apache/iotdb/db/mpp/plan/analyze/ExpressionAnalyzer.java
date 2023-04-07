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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.schematree.ISchemaTree;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.other.CaseWhenThenExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.TernaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.UnaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.CartesianProductVisitor.ConcatDeviceAndRemoveWildcardVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.CartesianProductVisitor.ConcatExpressionWithSuffixPathsVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.CartesianProductVisitor.RemoveWildcardInExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.CartesianProductVisitor.RemoveWildcardInFilterByDeviceVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.CartesianProductVisitor.RemoveWildcardInFilterVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.CollectVisitor.CollectAggregationExpressionsVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.CollectVisitor.CollectSourceExpressionsVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.IdentifyOutputColumnTypeVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.MergeVisitor.CheckIfTimeFilterExistVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.MergeVisitor.CheckIsScalarExpression;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.MergeVisitor.ExtractFullPathFromExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.MergeVisitor.IsDeviceViewNeedSpecialProcessVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.NoReturnValueVisitor.CheckIsAllMeasurementVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.NoReturnValueVisitor.ConstructPatternTreeFromExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.ReconstructVisitor.BindTypeForTimeSeriesOperandVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.ReconstructVisitor.GetMeasurementExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.ReconstructVisitor.RemoveAliasFromExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.ReconstructVisitor.ReplaceRawPathWithGroupedPathVisitor;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;
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
    new CheckIsAllMeasurementVisitor().process(expression, null);
  }

  public static ResultColumn.ColumnType identifyOutputColumnType(
      Expression expression, boolean isRoot) {
    return new IdentifyOutputColumnTypeVisitor().process(expression, isRoot);
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
  public static List<PartialPath> extractFullPathFromExpression(
      Expression expression, List<PartialPath> prefixPaths) {
    return new ExtractFullPathFromExpressionVisitor().process(expression, prefixPaths);
  }

  /**
   * Concat suffix path in WHERE clause with the prefix path in the FROM clause, to construct a
   * {@link PathPatternTree}. This method return all partialPaths, i.e. will not rewrite the
   * statement.
   *
   * @param predicate expression in WHERE clause
   * @param prefixPaths prefix paths in the FROM clause
   */
  public static void constructPatternTreeFromExpression(
      Expression predicate, List<PartialPath> prefixPaths, PathPatternTree patternTree) {
    new ConstructPatternTreeFromExpressionVisitor()
        .process(
            predicate,
            new ConstructPatternTreeFromExpressionVisitor.Context(prefixPaths, patternTree));
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
    } else if (predicate.getExpressionType().equals(ExpressionType.TIMESERIES)
        || predicate.getExpressionType().equals(ExpressionType.CONSTANT)
        || predicate.getExpressionType().equals(ExpressionType.NULL)) {
      return new Pair<>(null, true);
    } else if (predicate.getExpressionType().equals(ExpressionType.CASE_WHEN_THEN)) {
      return new Pair<>(null, true);
    } else {
      throw new IllegalArgumentException(
          "unsupported expression type: " + predicate.getExpressionType());
    }
  }

  public static boolean checkIfTimeFilterExist(Expression predicate) {
    return new CheckIfTimeFilterExistVisitor().process(predicate, null);
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
    } else if (expression instanceof CaseWhenThenExpression) {
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
    return new IsDeviceViewNeedSpecialProcessVisitor().process(expression, null);
  }

  public static boolean checkIsScalarExpression(Expression expression, Analysis analysis) {
    return new CheckIsScalarExpression().process(expression, analysis);
  }
}
