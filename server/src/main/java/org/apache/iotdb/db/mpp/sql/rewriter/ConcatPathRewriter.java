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
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.sql.SQLParserException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.mpp.common.filter.*;
import org.apache.iotdb.db.mpp.sql.analyze.AnalysisContext;
import org.apache.iotdb.db.mpp.sql.constant.FilterConstant.FilterType;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.component.*;
import org.apache.iotdb.db.mpp.sql.statement.crud.AggregationQueryStatement;
import org.apache.iotdb.db.mpp.sql.statement.crud.QueryStatement;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.db.service.IoTDB;

import java.util.*;

/**
 * This rewriter:
 *
 * <p>1. concat paths in SELECT, FROM, WHERE and WITHOUT NULL clause.
 *
 * <p>2. remove wildcards.
 */
public class ConcatPathRewriter implements IStatementRewriter {

  @Override
  public Statement rewrite(Statement statement, AnalysisContext context)
      throws StatementAnalyzeException, PathNumOverLimitException {
    QueryStatement queryStatement = (QueryStatement) statement;
    concatSelectWithFrom(queryStatement);
    concatWithoutNullColumnsWithFrom(queryStatement);
    removeWildcardsInSelect(queryStatement);
    removeWildcardsInWithoutNullColumns(queryStatement);
    concatWhereWithFromAndRemoveWildcards(queryStatement);
    return queryStatement;
  }

  /**
   * Concat the prefix path in the SELECT clause and the suffix path in the FROM clause into a full
   * path pattern.
   */
  private void concatSelectWithFrom(QueryStatement queryStatement)
      throws StatementAnalyzeException {
    // prefix paths in the FROM clause
    List<PartialPath> prefixPaths = queryStatement.getFromComponent().getPrefixPaths();

    List<ResultColumn> resultColumns = new ArrayList<>();
    for (ResultColumn suffixPath : queryStatement.getSelectComponent().getResultColumns()) {
      boolean needAliasCheck = suffixPath.hasAlias() && !queryStatement.isGroupByLevel();
      suffixPath.concat(prefixPaths, resultColumns, needAliasCheck);
    }
    queryStatement.getSelectComponent().setResultColumns(resultColumns);
  }

  /**
   * Concat the prefix path in the WITHOUT NULL clause and the suffix path in the FROM clause into a
   * full path pattern.
   */
  private void concatWithoutNullColumnsWithFrom(QueryStatement queryStatement)
      throws StatementAnalyzeException {
    // prefix paths in the FROM clause
    List<PartialPath> prefixPaths = queryStatement.getFromComponent().getPrefixPaths();

    // has without null columns
    if (queryStatement.getFilterNullComponent() != null
        && !queryStatement.getFilterNullComponent().getWithoutNullColumns().isEmpty()) {
      List<Expression> withoutNullColumns = new ArrayList<>();
      for (Expression expression :
          queryStatement.getFilterNullComponent().getWithoutNullColumns()) {
        concatWithoutNullColumnsWithFrom(
            prefixPaths,
            expression,
            withoutNullColumns,
            queryStatement.getSelectComponent().getAliasSet());
      }
      queryStatement.getFilterNullComponent().setWithoutNullColumns(withoutNullColumns);
    }
  }

  private void concatWithoutNullColumnsWithFrom(
      List<PartialPath> prefixPaths,
      Expression expression,
      List<Expression> withoutNullColumns,
      Set<String> aliasSet)
      throws StatementAnalyzeException {
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
                        MetaUtils.splitPathToDetachedPath(
                            ((TimeSeriesOperand) expression)
                                .getPath()
                                .getFirstNode()))); // split path To nodes
          } catch (IllegalPathException e) {
            throw new StatementAnalyzeException(e.getMessage());
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

  /** Remove wildcards (* or **) in SELECT clause. */
  private void removeWildcardsInSelect(QueryStatement queryStatement)
      throws PathNumOverLimitException, StatementAnalyzeException {
    if (queryStatement.getIndexType() != null) {
      return;
    }

    List<ResultColumn> resultColumns = new ArrayList<>();
    // Only used for group by level
    GroupByLevelController groupByLevelController = null;
    if (queryStatement.isGroupByLevel()) {
      groupByLevelController = new GroupByLevelController(queryStatement);
      queryStatement.resetSLimitOffset();
      resultColumns = new LinkedList<>();
    }

    WildcardsRemover wildcardsRemover = new WildcardsRemover(queryStatement);
    for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
      boolean needAliasCheck = resultColumn.hasAlias() && !queryStatement.isGroupByLevel();
      resultColumn.removeWildcards(wildcardsRemover, resultColumns, needAliasCheck);
      if (groupByLevelController != null) {
        groupByLevelController.control(resultColumn, resultColumns);
      }
      if (wildcardsRemover.checkIfPathNumberIsOverLimit(resultColumns)) {
        break;
      }
    }
    wildcardsRemover.checkIfSoffsetIsExceeded(resultColumns);
    queryStatement.getSelectComponent().setResultColumns(resultColumns);
    if (groupByLevelController != null) {
      ((AggregationQueryStatement) queryStatement)
          .getGroupByLevelComponent()
          .setGroupByLevelController(groupByLevelController);
    }
  }

  /** Remove wildcards (* or **) in WITHOUT NULL clause. */
  private void removeWildcardsInWithoutNullColumns(QueryStatement queryStatement)
      throws StatementAnalyzeException {
    if (queryStatement.getIndexType() != null) {
      return;
    }

    if (queryStatement.getFilterNullComponent() == null) {
      return;
    }

    List<Expression> expressions = queryStatement.getFilterNullComponent().getWithoutNullColumns();
    WildcardsRemover withoutNullWildcardsRemover = new WildcardsRemover(queryStatement);

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
      expression.removeWildcards(withoutNullWildcardsRemover, actualExpressions);
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

  /**
   * Concat the prefix path in the WHERE clause and the suffix path in the FROM clause into a full
   * path pattern. And remove wildcards.
   */
  private void concatWhereWithFromAndRemoveWildcards(QueryStatement queryStatement)
      throws StatementAnalyzeException {
    WhereCondition whereCondition = queryStatement.getWhereCondition();
    if (whereCondition == null) {
      return;
    }

    Set<PartialPath> filterPaths = new HashSet<>();
    whereCondition.setQueryFilter(
        concatWhereWithFromAndRemoveWildcards(
            queryStatement.getFromComponent().getPrefixPaths(),
            whereCondition.getQueryFilter(),
            filterPaths,
            queryStatement.isPrefixMatchPath()));
    whereCondition.getQueryFilter().setPathSet(filterPaths);
  }

  private QueryFilter concatWhereWithFromAndRemoveWildcards(
      List<PartialPath> fromPaths,
      QueryFilter filter,
      Set<PartialPath> filterPaths,
      boolean isPrefixMatch)
      throws StatementAnalyzeException {
    if (!filter.isLeaf()) {
      List<QueryFilter> newFilterList = new ArrayList<>();
      for (QueryFilter child : filter.getChildren()) {
        newFilterList.add(
            concatWhereWithFromAndRemoveWildcards(fromPaths, child, filterPaths, isPrefixMatch));
      }
      filter.setChildren(newFilterList);
      return filter;
    }
    FunctionFilter functionOperator = (FunctionFilter) filter;
    PartialPath filterPath = functionOperator.getSinglePath();
    List<PartialPath> concatPaths = new ArrayList<>();
    if (SQLConstant.isReservedPath(filterPath)) {
      // do nothing in the case of "where time > 5"
      filterPaths.add(filterPath);
      return filter;
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
    QueryFilter filterBinaryTree = new QueryFilter(FilterType.KW_AND);
    QueryFilter currentNode = filterBinaryTree;
    for (int i = 0; i < noStarPaths.size(); i++) {
      if (i > 0 && i < noStarPaths.size() - 1) {
        QueryFilter newInnerNode = new QueryFilter(FilterType.KW_AND);
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

  private List<PartialPath> removeWildcardsInConcatPaths(
      List<PartialPath> originalPaths, boolean isPrefixMatch) throws StatementAnalyzeException {
    HashSet<PartialPath> actualPaths = new HashSet<>();
    try {
      for (PartialPath originalPath : originalPaths) {
        List<MeasurementPath> all =
            IoTDB.schemaEngine.getMeasurementPathsWithAlias(originalPath, 0, 0, isPrefixMatch).left;
        if (all.isEmpty()) {
          throw new StatementAnalyzeException(
              String.format("Unknown time series %s in `where clause`", originalPath));
        }
        actualPaths.addAll(all);
      }
    } catch (MetadataException e) {
      throw new StatementAnalyzeException("error when remove star: " + e.getMessage());
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
