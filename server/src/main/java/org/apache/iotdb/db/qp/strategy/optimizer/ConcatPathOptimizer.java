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
package org.apache.iotdb.db.qp.strategy.optimizer;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.sql.SQLParserException;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ResultColumn;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.qp.constant.FilterConstant.FilterType;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromComponent;
import org.apache.iotdb.db.qp.logical.crud.FunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.InOperator;
import org.apache.iotdb.db.qp.logical.crud.LikeOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.RegexpOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectComponent;
import org.apache.iotdb.db.qp.logical.crud.WhereComponent;
import org.apache.iotdb.db.qp.utils.GroupByLevelController;
import org.apache.iotdb.db.qp.utils.WildcardsRemover;
import org.apache.iotdb.db.service.IoTDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/** concat paths in select and from clause. */
public class ConcatPathOptimizer implements ILogicalOptimizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConcatPathOptimizer.class);

  private static final String WARNING_NO_SUFFIX_PATHS =
      "failed to concat series paths because the given query operator didn't have suffix paths";
  private static final String WARNING_NO_PREFIX_PATHS =
      "failed to concat series paths because the given query operator didn't have prefix paths";

  @Override
  public Operator transform(Operator operator)
      throws LogicalOptimizeException, PathNumOverLimitException {
    QueryOperator queryOperator = (QueryOperator) operator;
    if (!optimizable(queryOperator)) {
      return queryOperator;
    }

    concatSelect(queryOperator);
    concatWithoutNullColumns(queryOperator);
    removeWildcardsInSelectPaths(queryOperator);
    removeWildcardsWithoutNullColumns(queryOperator);
    concatFilterAndRemoveWildcards(queryOperator);
    return queryOperator;
  }

  private boolean optimizable(QueryOperator queryOperator) {
    if (queryOperator.isAlignByDevice()) {
      return false;
    }

    SelectComponent select = queryOperator.getSelectComponent();
    if (select == null || select.getResultColumns().isEmpty()) {
      LOGGER.warn(WARNING_NO_SUFFIX_PATHS);
      return false;
    }

    FromComponent from = queryOperator.getFromComponent();
    if (from == null || from.getPrefixPaths().isEmpty()) {
      LOGGER.warn(WARNING_NO_PREFIX_PATHS);
      return false;
    }

    return true;
  }

  private void concatSelect(QueryOperator queryOperator) throws LogicalOptimizeException {
    List<PartialPath> prefixPaths = queryOperator.getFromComponent().getPrefixPaths();
    List<ResultColumn> resultColumns = new ArrayList<>();
    for (ResultColumn suffixColumn : queryOperator.getSelectComponent().getResultColumns()) {
      boolean needAliasCheck = suffixColumn.hasAlias() && !queryOperator.isGroupByLevel();
      suffixColumn.concat(prefixPaths, resultColumns, needAliasCheck);
    }
    queryOperator.getSelectComponent().setResultColumns(resultColumns);
  }

  private void concatWithoutNullColumns(QueryOperator queryOperator)
      throws LogicalOptimizeException {
    List<PartialPath> prefixPaths = queryOperator.getFromComponent().getPrefixPaths();
    // has without null columns
    if (queryOperator.getSpecialClauseComponent() != null
        && !queryOperator.getSpecialClauseComponent().getWithoutNullColumns().isEmpty()) {
      List<Expression> withoutNullColumns = new ArrayList<>();
      for (Expression expression :
          queryOperator.getSpecialClauseComponent().getWithoutNullColumns()) {
        concatWithoutNullColumns(
            prefixPaths, expression, withoutNullColumns, queryOperator.getAliasSet());
      }
      queryOperator.getSpecialClauseComponent().setWithoutNullColumns(withoutNullColumns);
    }
  }

  private void concatWithoutNullColumns(
      List<PartialPath> prefixPaths,
      Expression expression,
      List<Expression> withoutNullColumns,
      Set<String> aliasSet)
      throws LogicalOptimizeException {
    if (expression instanceof TimeSeriesOperand) {
      TimeSeriesOperand timeSeriesOperand = (TimeSeriesOperand) expression;
      if (timeSeriesOperand
          .getPath()
          .getFullPath()
          .startsWith(SQLConstant.ROOT + ".")) { // start with "root." don't concat
        // because the full path that starts with 'root.' won't be split
        // so we need to split it.
        if (((TimeSeriesOperand) expression).getPath().getNodeLength() == 1) { // no split
          try {
            ((TimeSeriesOperand) expression)
                .setPath(
                    new PartialPath(
                        PathUtils.splitPathToDetachedNodes(
                            ((TimeSeriesOperand) expression)
                                .getPath()
                                .getFirstNode()))); // split path To nodes
          } catch (IllegalPathException e) {
            throw new LogicalOptimizeException(e.getMessage());
          }
        }
        withoutNullColumns.add(expression);
      } else {
        if (!aliasSet.contains(expression.getExpressionString())) { // not alias, concat
          List<Expression> resultExpressions = new ArrayList<>();
          expression.concat(prefixPaths, resultExpressions);
          withoutNullColumns.addAll(resultExpressions);
        } else { // alias, don't concat
          withoutNullColumns.add(expression);
        }
      }
    } else {
      List<Expression> resultExpressions = new ArrayList<>();
      expression.concat(prefixPaths, resultExpressions);
      withoutNullColumns.addAll(resultExpressions);
    }
  }

  private void removeWildcardsInSelectPaths(QueryOperator queryOperator)
      throws LogicalOptimizeException, PathNumOverLimitException {
    if (queryOperator.getIndexType() != null) {
      return;
    }

    List<ResultColumn> resultColumns = new ArrayList<>();
    // Only used for group by level
    GroupByLevelController groupByLevelController = null;
    if (queryOperator.isGroupByLevel()) {
      groupByLevelController = new GroupByLevelController(queryOperator);
      queryOperator.resetSLimitOffset();
      resultColumns = new LinkedList<>();
    }

    WildcardsRemover wildcardsRemover = new WildcardsRemover(queryOperator);
    for (ResultColumn resultColumn : queryOperator.getSelectComponent().getResultColumns()) {
      boolean needAliasCheck = resultColumn.hasAlias() && !queryOperator.isGroupByLevel();
      resultColumn.removeWildcards(wildcardsRemover, resultColumns, needAliasCheck);
      if (groupByLevelController != null) {
        groupByLevelController.control(resultColumn, resultColumns);
      }
      if (wildcardsRemover.checkIfPathNumberIsOverLimit(resultColumns)) {
        break;
      }
    }
    wildcardsRemover.checkIfSoffsetIsExceeded(resultColumns);
    queryOperator.getSelectComponent().setResultColumns(resultColumns);
    if (groupByLevelController != null) {
      queryOperator.getSpecialClauseComponent().setGroupByLevelController(groupByLevelController);
    }
  }

  private void removeWildcardsWithoutNullColumns(QueryOperator queryOperator)
      throws LogicalOptimizeException {
    if (queryOperator.getIndexType() != null) {
      return;
    }

    if (queryOperator.getSpecialClauseComponent() == null) {
      return;
    }

    List<Expression> expressions =
        queryOperator.getSpecialClauseComponent().getWithoutNullColumns();
    WildcardsRemover withoutNullWildcardsRemover = new WildcardsRemover(queryOperator);

    // because timeSeries path may be with "*", so need to remove it for getting some actual
    // timeSeries paths
    // actualExpressions store the actual timeSeries paths
    List<Expression> actualExpressions = new ArrayList<>();
    List<Expression> resultExpressions = new ArrayList<>();

    // because expression.removeWildcards will ignore the TimeSeries path that exists in the meta
    // so we need to recognise the alias, just simply add to the resultExpressions
    for (Expression expression : expressions) {
      if (queryOperator.getAliasSet().contains(expression.getExpressionString())) {
        resultExpressions.add(expression);
        continue;
      }
      expression.removeWildcards(withoutNullWildcardsRemover, actualExpressions);
    }

    // group by level, use groupedPathMap
    GroupByLevelController groupByLevelController =
        queryOperator.getSpecialClauseComponent().getGroupByLevelController();
    if (groupByLevelController != null) {
      for (Expression expression : actualExpressions) {
        String groupedPath =
            groupByLevelController.getGroupedPath(expression.getExpressionString());
        if (groupedPath != null) {
          resultExpressions.add(new TimeSeriesOperand(new PartialPath(groupedPath, false)));
        } else {
          resultExpressions.add(expression);
        }
      }
    } else {
      resultExpressions.addAll(actualExpressions);
    }
    queryOperator.getSpecialClauseComponent().setWithoutNullColumns(resultExpressions);
  }

  private void concatFilterAndRemoveWildcards(QueryOperator queryOperator)
      throws LogicalOptimizeException {
    WhereComponent whereComponent = queryOperator.getWhereComponent();
    if (whereComponent == null) {
      return;
    }

    Set<PartialPath> filterPaths = new HashSet<>();
    whereComponent.setFilterOperator(
        concatFilterAndRemoveWildcards(
            queryOperator.getFromComponent().getPrefixPaths(),
            whereComponent.getFilterOperator(),
            filterPaths,
            queryOperator.isPrefixMatchPath()));
    whereComponent.getFilterOperator().setPathSet(filterPaths);
  }

  private FilterOperator concatFilterAndRemoveWildcards(
      List<PartialPath> fromPaths,
      FilterOperator operator,
      Set<PartialPath> filterPaths,
      boolean isPrefixMatch)
      throws LogicalOptimizeException {
    if (!operator.isLeaf()) {
      List<FilterOperator> newFilterList = new ArrayList<>();
      for (FilterOperator child : operator.getChildren()) {
        newFilterList.add(
            concatFilterAndRemoveWildcards(fromPaths, child, filterPaths, isPrefixMatch));
      }
      operator.setChildren(newFilterList);
      return operator;
    }
    FunctionOperator functionOperator = (FunctionOperator) operator;
    PartialPath filterPath = functionOperator.getSinglePath();
    List<PartialPath> concatPaths = new ArrayList<>();
    if (SQLConstant.isReservedPath(filterPath)) {
      // do nothing in the case of "where time > 5"
      filterPaths.add(filterPath);
      return operator;
    } else if (filterPath.getFirstNode().startsWith(SQLConstant.ROOT)) {
      // do nothing in the case of "where root.d1.s1 > 5"
      concatPaths.add(filterPath);
    } else {
      fromPaths.forEach(fromPath -> concatPaths.add(fromPath.concatPath(filterPath)));
    }

    List<PartialPath> noStarPaths = removeWildcardsInConcatPaths(concatPaths, isPrefixMatch);
    filterPaths.addAll(noStarPaths);
    if (noStarPaths.size() == 1) {
      // Transform "select s1 from root.car.* where s1 > 10" to
      // "select s1 from root.car.* where root.car.*.s1 > 10"
      functionOperator.setSinglePath(noStarPaths.get(0));
      return operator;
    } else {
      // Transform "select s1 from root.car.d1, root.car.d2 where s1 > 10" to
      // "select s1 from root.car.d1, root.car.d2 where root.car.d1.s1 > 10 and root.car.d2.s1 > 10"
      // Note that,
      // two fork tree has to be maintained while removing stars in paths for DnfFilterOptimizer
      // requirement.
      return constructBinaryFilterTreeWithAnd(noStarPaths, operator);
    }
  }

  private FilterOperator constructBinaryFilterTreeWithAnd(
      List<PartialPath> noStarPaths, FilterOperator operator) throws LogicalOptimizeException {
    FilterOperator filterBinaryTree = new FilterOperator(FilterType.KW_AND);
    FilterOperator currentNode = filterBinaryTree;
    for (int i = 0; i < noStarPaths.size(); i++) {
      if (i > 0 && i < noStarPaths.size() - 1) {
        FilterOperator newInnerNode = new FilterOperator(FilterType.KW_AND);
        currentNode.addChildOperator(newInnerNode);
        currentNode = newInnerNode;
      }
      try {
        if (operator instanceof InOperator) {
          currentNode.addChildOperator(
              new InOperator(
                  operator.getFilterType(),
                  noStarPaths.get(i),
                  ((InOperator) operator).getNot(),
                  ((InOperator) operator).getValues()));
        } else if (operator instanceof LikeOperator) {
          currentNode.addChildOperator(
              new LikeOperator(
                  operator.getFilterType(),
                  noStarPaths.get(i),
                  ((LikeOperator) operator).getValue()));
        } else if (operator instanceof RegexpOperator) {
          currentNode.addChildOperator(
              new RegexpOperator(
                  operator.getFilterType(),
                  noStarPaths.get(i),
                  ((RegexpOperator) operator).getValue()));
        } else {
          currentNode.addChildOperator(
              new BasicFunctionOperator(
                  operator.getFilterType(),
                  noStarPaths.get(i),
                  ((BasicFunctionOperator) operator).getValue()));
        }
      } catch (SQLParserException e) {
        throw new LogicalOptimizeException(e.getMessage());
      }
    }
    return filterBinaryTree;
  }

  private List<PartialPath> removeWildcardsInConcatPaths(
      List<PartialPath> originalPaths, boolean isPrefixMatch) throws LogicalOptimizeException {
    HashSet<PartialPath> actualPaths = new HashSet<>();
    try {
      for (PartialPath originalPath : originalPaths) {
        List<MeasurementPath> all =
            IoTDB.schemaProcessor.getMeasurementPathsWithAlias(
                    originalPath, 0, 0, isPrefixMatch, false)
                .left;
        if (all.isEmpty()) {
          throw new LogicalOptimizeException(
              String.format("Unknown time series %s in `where clause`", originalPath));
        }
        actualPaths.addAll(all);
      }
    } catch (MetadataException e) {
      throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
    }
    return new ArrayList<>(actualPaths);
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
