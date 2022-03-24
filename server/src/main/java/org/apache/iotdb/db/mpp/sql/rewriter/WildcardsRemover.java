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

package org.apache.iotdb.db.mpp.sql.rewriter;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.sql.SQLParserException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.filter.*;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.sql.constant.FilterConstant;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.component.GroupByLevelController;
import org.apache.iotdb.db.mpp.sql.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.sql.statement.component.WhereCondition;
import org.apache.iotdb.db.mpp.sql.statement.crud.AggregationQueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.QueryStatement;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.*;

/**
 * This rewriter:
 *
 * <p>1. Bind metadata to paths in SELECT, WHERE, and WITHOUT NULL clauses.
 *
 * <p>2. Remove wildcards and apply SLIMIT & SOFFSET.
 */
public class WildcardsRemover {

  private SchemaTree schemaTree;

  private ColumnPaginationController paginationController;

  /**
   * Since IoTDB v0.13, all DDL and DML use patternMatch as default. Before IoTDB v0.13, all DDL and
   * DML use prefixMatch.
   */
  private boolean isPrefixMatchPath;

  public Statement rewrite(
      Statement statement, SchemaTree schemaTree, Map<String, Set<PartialPath>> deviceIdToPathsMap)
      throws StatementAnalyzeException, PathNumOverLimitException {
    QueryStatement queryStatement = (QueryStatement) statement;
    this.paginationController =
        new ColumnPaginationController(
            queryStatement.getSeriesLimit(), queryStatement.getSeriesOffset());
    this.schemaTree = schemaTree;
    this.isPrefixMatchPath = queryStatement.isPrefixMatchPath();

    if (queryStatement.getIndexType() == null) {
      // remove wildcards in SELECT caluse
      removeWildcardsInSelectPaths(queryStatement);
      deviceIdToPathsMap.putAll(queryStatement.getSelectComponent().getDeviceIdToPathsMap());

      // remove wildcards in WITHOUT NULL caluse
      if (queryStatement.getFilterNullComponent() != null
          && !queryStatement.getFilterNullComponent().getWithoutNullColumns().isEmpty()) {
        removeWildcardsWithoutNullColumns(queryStatement);
      }
    }

    // remove wildcards in WHERE caluse
    if (queryStatement.getWhereCondition() != null) {
      removeWildcardsInQueryFilter(queryStatement, deviceIdToPathsMap);
    }

    return queryStatement;
  }

  private void removeWildcardsInSelectPaths(QueryStatement queryStatement)
      throws StatementAnalyzeException, PathNumOverLimitException {
    List<ResultColumn> resultColumns = new ArrayList<>();

    // Only used for group by level
    GroupByLevelController groupByLevelController = null;
    if (queryStatement.isGroupByLevel()) {
      groupByLevelController = new GroupByLevelController(queryStatement);
      queryStatement.resetSLimitOffset();
      resultColumns = new LinkedList<>();
    }

    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      boolean needAliasCheck = resultColumn.hasAlias() && !queryStatement.isGroupByLevel();
      resultColumn.removeWildcards(this, resultColumns, needAliasCheck);
      if (groupByLevelController != null) {
        groupByLevelController.control(resultColumn, resultColumns);
      }
      if (paginationController.checkIfPathNumberIsOverLimit(resultColumns)) {
        break;
      }
    }
    paginationController.checkIfSoffsetIsExceeded(resultColumns);
    queryStatement.getSelectComponent().setResultColumns(resultColumns);
    if (groupByLevelController != null) {
      ((AggregationQueryStatement) queryStatement)
          .getGroupByLevelComponent()
          .setGroupByLevelController(groupByLevelController);
    }
  }

  private void removeWildcardsWithoutNullColumns(QueryStatement queryStatement)
      throws StatementAnalyzeException {

    List<Expression> expressions = queryStatement.getFilterNullComponent().getWithoutNullColumns();

    // because timeSeries path may be with "*", so need to remove it for getting some actual
    // timeSeries paths
    // actualExpressions store the actual timeSeries paths
    List<Expression> actualExpressions = new ArrayList<>();
    List<Expression> resultExpressions = new ArrayList<>();

    // because expression.removeWildcards will ignore the TimeSeries path that exists in the meta
    // so we need to recognise the alias, just simply add to the resultExpressions
    for (Expression expression : expressions) {
      if (queryStatement
          .getSelectComponent()
          .getAliasSet()
          .contains(expression.getExpressionString())) {
        resultExpressions.add(expression);
        continue;
      }
      expression.removeWildcards(this, actualExpressions);
    }

    // group by level, use groupedPathMap
    if (queryStatement.isGroupByLevel()) {
      GroupByLevelController groupByLevelController =
          ((AggregationQueryStatement) queryStatement)
              .getGroupByLevelComponent()
              .getGroupByLevelController();
      for (Expression expression : actualExpressions) {
        String groupedPath =
            groupByLevelController.getGroupedPath(expression.getExpressionString());
        if (groupedPath != null) {
          try {
            resultExpressions.add(new TimeSeriesOperand(new PartialPath(groupedPath)));
          } catch (IllegalPathException e) {
            throw new StatementAnalyzeException(e.getMessage());
          }
        } else {
          resultExpressions.add(expression);
        }
      }
    } else {
      resultExpressions.addAll(actualExpressions);
    }
    queryStatement.getFilterNullComponent().setWithoutNullColumns(resultExpressions);
  }

  private void removeWildcardsInQueryFilter(
      QueryStatement queryStatement, Map<String, Set<PartialPath>> deviceIdToPathsMap)
      throws StatementAnalyzeException {
    WhereCondition whereCondition = queryStatement.getWhereCondition();

    Set<PartialPath> resultPaths = new HashSet<>();
    whereCondition.setQueryFilter(
        removeWildcardsInQueryFilter(whereCondition.getQueryFilter(), resultPaths));
    whereCondition.getQueryFilter().setPathSet(resultPaths);

    for (PartialPath path : resultPaths) {
      deviceIdToPathsMap.computeIfAbsent(path.getDevice(), k -> new HashSet<>()).add(path);
    }
  }

  private QueryFilter removeWildcardsInQueryFilter(QueryFilter filter, Set<PartialPath> resultPaths)
      throws StatementAnalyzeException {
    if (!filter.isLeaf()) {
      List<QueryFilter> newFilterList = new ArrayList<>();
      for (QueryFilter child : filter.getChildren()) {
        newFilterList.add(removeWildcardsInQueryFilter(child, resultPaths));
      }
      filter.setChildren(newFilterList);
      return filter;
    }
    FunctionFilter functionFilter = (FunctionFilter) filter;
    PartialPath filterPath = functionFilter.getSinglePath();

    if (SQLConstant.isReservedPath(filterPath)) {
      // do nothing in the case of "where time > 5"
      resultPaths.add(filterPath);
      return filter;
    }

    List<PartialPath> noStarPaths = removeWildcardsInFilterPath(filterPath);
    resultPaths.addAll(noStarPaths);
    if (noStarPaths.size() == 1) {
      // Transform "select s1 from root.car.* where s1 > 10" to
      // "select s1 from root.car.* where root.car.*.s1 > 10"
      functionFilter.setSinglePath(noStarPaths.get(0));
      return filter;
    } else {
      // Transform "select s1 from root.car.d1, root.car.d2 where s1 > 10" to
      // "select s1 from root.car.d1, root.car.d2 where root.car.d1.s1 > 10 and root.car.d2.s1 > 10"
      // Note that,
      // two fork tree has to be maintained while removing stars in paths for DnfFilterOptimizer
      // requirement.
      return constructBinaryFilterTreeWithAnd(noStarPaths, filter);
    }
  }

  private QueryFilter constructBinaryFilterTreeWithAnd(
      List<PartialPath> noStarPaths, QueryFilter filter) throws StatementAnalyzeException {
    QueryFilter filterBinaryTree = new QueryFilter(FilterConstant.FilterType.KW_AND);
    QueryFilter currentNode = filterBinaryTree;
    for (int i = 0; i < noStarPaths.size(); i++) {
      if (i > 0 && i < noStarPaths.size() - 1) {
        QueryFilter newInnerNode = new QueryFilter(FilterConstant.FilterType.KW_AND);
        currentNode.addChildOperator(newInnerNode);
        currentNode = newInnerNode;
      }
      try {
        if (filter instanceof InFilter) {
          currentNode.addChildOperator(
              new InFilter(
                  filter.getFilterType(),
                  noStarPaths.get(i),
                  ((InFilter) filter).getNot(),
                  ((InFilter) filter).getValues()));
        } else if (filter instanceof LikeFilter) {
          currentNode.addChildOperator(
              new LikeFilter(
                  filter.getFilterType(), noStarPaths.get(i), ((LikeFilter) filter).getValue()));
        } else if (filter instanceof RegexpFilter) {
          currentNode.addChildOperator(
              new RegexpFilter(
                  filter.getFilterType(), noStarPaths.get(i), ((RegexpFilter) filter).getValue()));
        } else {
          currentNode.addChildOperator(
              new BasicFunctionFilter(
                  filter.getFilterType(),
                  noStarPaths.get(i),
                  ((BasicFunctionFilter) filter).getValue()));
        }
      } catch (SQLParserException e) {
        throw new StatementAnalyzeException(e.getMessage());
      }
    }
    return filterBinaryTree;
  }

  public List<MeasurementPath> removeWildcardInPath(PartialPath path)
      throws StatementAnalyzeException {
    try {
      Pair<List<MeasurementPath>, Integer> pair =
          schemaTree.searchMeasurementPaths(
              path,
              paginationController.getCurLimit(),
              paginationController.getCurOffset(),
              isPrefixMatchPath);
      paginationController.consume(pair.left.size(), pair.right);
      return pair.left;
    } catch (Exception e) {
      throw new StatementAnalyzeException(
          "error occurred when removing wildcard: " + e.getMessage());
    }
  }

  public List<List<Expression>> removeWildcardsInExpressions(List<Expression> expressions)
      throws StatementAnalyzeException {
    // One by one, remove the wildcards from the input expressions. In most cases, an expression
    // will produce multiple expressions after removing the wildcards. We use extendedExpressions to
    // collect the produced expressions.
    List<List<Expression>> extendedExpressions = new ArrayList<>();
    for (Expression originExpression : expressions) {
      List<Expression> actualExpressions = new ArrayList<>();
      originExpression.removeWildcards(new WildcardsRemover(), actualExpressions);
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

    // Apply the soffset & slimit control to the actualExpressions and return the remaining
    // expressions.
    List<List<Expression>> remainingExpressions = new ArrayList<>();
    for (List<Expression> actualExpression : actualExpressions) {
      if (paginationController.hasCurOffset()) {
        paginationController.decCurOffset();
        continue;
      } else if (paginationController.hasCurLimit()) {
        paginationController.decCurLimit();
      } else {
        break;
      }
      remainingExpressions.add(actualExpression);
    }
    paginationController.incConsumed(actualExpressions.size());
    return remainingExpressions;
  }

  private List<PartialPath> removeWildcardsInFilterPath(PartialPath originalPath)
      throws StatementAnalyzeException {
    List<PartialPath> actualPaths = new ArrayList<>();
    try {
      List<MeasurementPath> all =
          schemaTree.searchMeasurementPaths(originalPath, 0, 0, isPrefixMatchPath).left;
      if (all.isEmpty()) {
        throw new StatementAnalyzeException(
            String.format("Unknown time series %s in `where clause`", originalPath));
      }
      actualPaths.addAll(all);
    } catch (Exception e) {
      throw new StatementAnalyzeException("error when remove star: " + e.getMessage());
    }
    return actualPaths;
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
