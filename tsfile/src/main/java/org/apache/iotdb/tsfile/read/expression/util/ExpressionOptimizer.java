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
package org.apache.iotdb.tsfile.read.expression.util;

import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IBinaryExpression;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import java.util.List;

public class ExpressionOptimizer {

  private ExpressionOptimizer() {}

  public static ExpressionOptimizer getInstance() {
    return QueryFilterOptimizerHelper.INSTANCE;
  }

  /**
   * try to remove GlobalTimeExpression.
   *
   * @param expression IExpression to be transferred
   * @param selectedSeries selected series
   * @return an executable query filter, whether a GlobalTimeExpression or All leaf nodes are
   *     SingleSeriesExpression
   */
  public IExpression optimize(IExpression expression, List<Path> selectedSeries)
      throws QueryFilterOptimizationException {
    if (expression instanceof IUnaryExpression) {
      return expression;
    } else if (expression instanceof IBinaryExpression) {
      ExpressionType relation = expression.getType();
      IExpression left = ((IBinaryExpression) expression).getLeft();
      IExpression right = ((IBinaryExpression) expression).getRight();
      if (left.getType() == ExpressionType.GLOBAL_TIME
          && right.getType() == ExpressionType.GLOBAL_TIME) {
        return combineTwoGlobalTimeFilter(
            (GlobalTimeExpression) left, (GlobalTimeExpression) right, expression.getType());
      } else if (left.getType() == ExpressionType.GLOBAL_TIME
          && right.getType() != ExpressionType.GLOBAL_TIME) {
        return handleOneGlobalTimeFilter(
            (GlobalTimeExpression) left, right, selectedSeries, relation);
      } else if (left.getType() != ExpressionType.GLOBAL_TIME
          && right.getType() == ExpressionType.GLOBAL_TIME) {
        return handleOneGlobalTimeFilter(
            (GlobalTimeExpression) right, left, selectedSeries, relation);
      } else if (left.getType() != ExpressionType.GLOBAL_TIME
          && right.getType() != ExpressionType.GLOBAL_TIME) {
        try {
          IExpression regularLeft = optimize(left, selectedSeries);
          IExpression regularRight = optimize(right, selectedSeries);
          IBinaryExpression midRet = null;
          if (relation == ExpressionType.AND) {
            midRet = BinaryExpression.and(regularLeft, regularRight);
          } else if (relation == ExpressionType.OR) {
            midRet = BinaryExpression.or(regularLeft, regularRight);
          } else {
            throw new UnsupportedOperationException("unsupported IExpression type: " + relation);
          }
          if (midRet.getLeft().getType() == ExpressionType.GLOBAL_TIME
              || midRet.getRight().getType() == ExpressionType.GLOBAL_TIME) {
            return optimize(midRet, selectedSeries);
          } else {
            return midRet;
          }
        } catch (StackOverflowError stackOverflowError) {
          throw new QueryFilterOptimizationException("StackOverflowError is encountered.");
        }
      }
    }
    throw new UnsupportedOperationException(
        "unknown IExpression type: " + expression.getClass().getName());
  }

  private IExpression handleOneGlobalTimeFilter(
      GlobalTimeExpression globalTimeExpression,
      IExpression expression,
      List<Path> selectedSeries,
      ExpressionType relation)
      throws QueryFilterOptimizationException {
    IExpression regularRightIExpression = optimize(expression, selectedSeries);
    if (regularRightIExpression instanceof GlobalTimeExpression) {
      return combineTwoGlobalTimeFilter(
          globalTimeExpression, (GlobalTimeExpression) regularRightIExpression, relation);
    }
    if (relation == ExpressionType.AND) {
      addTimeFilterToQueryFilter((globalTimeExpression).getFilter(), regularRightIExpression);
      return regularRightIExpression;
    } else if (relation == ExpressionType.OR) {
      IExpression afterTransform =
          pushGlobalTimeFilterToAllSeries(globalTimeExpression, selectedSeries);
      return mergeSecondTreeToFirstTree(afterTransform, regularRightIExpression);
    }
    throw new QueryFilterOptimizationException("unknown relation in IExpression:" + relation);
  }

  /**
   * This method merge the second input, which is of tree structure, to the first parameter. It
   * visits all leaf nodes, which are SingleSeriesExpressions, or AndExpression in right Expression,
   * merge them to the right position in leftExpression.
   *
   * @param leftExpression The IExpression transformed from GlobalTimeExpression, which might have
   *     already be updated and merged.
   * @param rightExpression The IExpression to be merged into the first IExpression
   * @return a merged IExpression, which is initially based on the input leftExpression
   */
  private IExpression mergeSecondTreeToFirstTree(
      IExpression leftExpression, IExpression rightExpression) {
    if (rightExpression.getType() == ExpressionType.SERIES) {
      SingleSeriesExpression leaf = (SingleSeriesExpression) rightExpression;
      updateFilterWithOr(leftExpression, leaf.getFilter(), leaf.getSeriesPath());
      return leftExpression;
    } else if (rightExpression.getType() == ExpressionType.OR) {
      IExpression leftChild = ((BinaryExpression) rightExpression).getLeft();
      IExpression rightChild = ((BinaryExpression) rightExpression).getRight();
      leftExpression = mergeSecondTreeToFirstTree(leftExpression, leftChild);
      leftExpression = mergeSecondTreeToFirstTree(leftExpression, rightChild);
      return leftExpression;
    } else {
      return BinaryExpression.or(leftExpression, rightExpression);
    }
  }

  /**
   * This method search the node in the input expression, whose path is identical to the input path,
   * then merges its filter and the input filter with relation OR.
   *
   * @return true if the input filter is merged.
   */
  private boolean updateFilterWithOr(IExpression expression, Filter filter, Path path) {
    if (expression.getType() == ExpressionType.SERIES
        && ((SingleSeriesExpression) expression).getSeriesPath().equals(path)) {
      Filter nodeFilter = ((SingleSeriesExpression) expression).getFilter();
      nodeFilter = FilterFactory.or(nodeFilter, filter);
      ((SingleSeriesExpression) expression).setFilter(nodeFilter);
      return true;
    } else if (expression.getType() == ExpressionType.OR) {
      assert expression instanceof BinaryExpression;
      IExpression left = ((BinaryExpression) expression).getLeft();
      IExpression right = ((BinaryExpression) expression).getRight();
      return updateFilterWithOr(left, filter, path) || updateFilterWithOr(right, filter, path);
    } else {
      return false;
    }
  }

  /**
   * Combine GlobalTimeExpression with all selected series. example: input:
   * GlobalTimeExpression(timeFilter) Selected Series: path1, path2, path3 output: QueryFilterOR(
   * QueryFilterOR( SingleSeriesExpression(path1, timeFilter), SingleSeriesExpression(path2,
   * timeFilter) ), SingleSeriesExpression(path3, timeFilter) )
   *
   * @return a DNF query filter without GlobalTimeExpression
   */
  private IExpression pushGlobalTimeFilterToAllSeries(
      GlobalTimeExpression timeFilter, List<Path> selectedSeries)
      throws QueryFilterOptimizationException {
    if (selectedSeries.size() == 0) {
      throw new QueryFilterOptimizationException("size of selectSeries could not be 0");
    }
    IExpression expression =
        new SingleSeriesExpression(selectedSeries.get(0), timeFilter.getFilter());
    for (int i = 1; i < selectedSeries.size(); i++) {
      expression =
          BinaryExpression.or(
              expression,
              new SingleSeriesExpression(selectedSeries.get(i), timeFilter.getFilter()));
    }
    return expression;
  }

  /** Combine TimeFilter with all SeriesFilters in the expression. */
  private void addTimeFilterToQueryFilter(Filter timeFilter, IExpression expression) {
    if (expression instanceof SingleSeriesExpression) {
      addTimeFilterToSeriesFilter(timeFilter, (SingleSeriesExpression) expression);
    } else if (expression instanceof BinaryExpression) {
      addTimeFilterToQueryFilter(timeFilter, ((BinaryExpression) expression).getLeft());
      addTimeFilterToQueryFilter(timeFilter, ((BinaryExpression) expression).getRight());
    } else {
      throw new UnsupportedOperationException(
          "IExpression should contains only SingleSeriesExpression but other type is found:"
              + expression.getClass().getName());
    }
  }

  /**
   * Merge the timeFilter with the filter in SingleSeriesExpression with AndExpression. example:
   * input: timeFilter SingleSeriesExpression(path, filter) output: SingleSeriesExpression( path,
   * AndExpression(filter, timeFilter) )
   */
  private void addTimeFilterToSeriesFilter(
      Filter timeFilter, SingleSeriesExpression singleSeriesExp) {
    singleSeriesExp.setFilter(FilterFactory.and(singleSeriesExp.getFilter(), timeFilter));
  }

  /**
   * combine two GlobalTimeExpression by merge the TimeFilter in each GlobalTimeExpression. example:
   * input: QueryFilterAnd/OR( GlobalTimeExpression(timeFilter1), GlobalTimeExpression(timeFilter2)
   * ) output: GlobalTimeExpression( AndExpression/OR(timeFilter1, timeFilter2) )
   */
  private GlobalTimeExpression combineTwoGlobalTimeFilter(
      GlobalTimeExpression left, GlobalTimeExpression right, ExpressionType type) {
    if (type == ExpressionType.AND) {
      return new GlobalTimeExpression(FilterFactory.and(left.getFilter(), right.getFilter()));
    } else if (type == ExpressionType.OR) {
      return new GlobalTimeExpression(FilterFactory.or(left.getFilter(), right.getFilter()));
    }
    throw new UnsupportedOperationException("unrecognized QueryFilterOperatorType :" + type);
  }

  private static class QueryFilterOptimizerHelper {

    private static final ExpressionOptimizer INSTANCE = new ExpressionOptimizer();
  }
}
