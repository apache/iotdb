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
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.mpp.common.filter.*;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.sql.constant.FilterConstant.FilterType;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.component.*;
import org.apache.iotdb.db.mpp.sql.statement.crud.QueryStatement;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

import java.util.*;

/**
 * This rewriter:
 *
 * <p>1. Concat prefix path in SELECT, WHERE, and WITHOUT NULL clause with the suffix path in the
 * FROM clause.
 *
 * <p>2. Construct a {@link PathPatternTree}.
 */
public class ConcatPathRewriter {

  private PathPatternTree patternTree;

  public Statement rewrite(Statement statement, PathPatternTree patternTree)
      throws StatementAnalyzeException, PathNumOverLimitException {
    QueryStatement queryStatement = (QueryStatement) statement;
    this.patternTree = patternTree;

    // concat SELECT with FROM
    concatSelectWithFrom(queryStatement);

    // concat WITHOUT NULL with FROM
    if (queryStatement.getFilterNullComponent() != null
        && !queryStatement.getFilterNullComponent().getWithoutNullColumns().isEmpty()) {
      concatWithoutNullColumnsWithFrom(queryStatement);
    }

    // concat WHERE with FROM
    if (queryStatement.getWhereCondition() != null) {
      concatWhereWithFrom(queryStatement);
    }
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

    // resultColumns after concat
    List<ResultColumn> resultColumns = new ArrayList<>();
    for (ResultColumn suffixPath : queryStatement.getSelectComponent().getResultColumns()) {
      boolean needAliasCheck = suffixPath.hasAlias() && !queryStatement.isGroupByLevel();
      suffixPath.concat(prefixPaths, resultColumns, needAliasCheck, patternTree);
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

    // result after concat
    List<Expression> withoutNullColumns = new ArrayList<>();
    for (Expression expression : queryStatement.getFilterNullComponent().getWithoutNullColumns()) {
      concatWithoutNullColumnsWithFrom(
          prefixPaths,
          expression,
          withoutNullColumns,
          queryStatement.getSelectComponent().getAliasSet());
    }
    queryStatement.getFilterNullComponent().setWithoutNullColumns(withoutNullColumns);
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
        patternTree.append(((TimeSeriesOperand) expression).getPath());
        withoutNullColumns.add(expression);
      } else {
        if (!aliasSet.contains(expression.getExpressionString())) { // not alias, concat
          List<Expression> resultExpressions = new ArrayList<>();
          expression.concat(prefixPaths, resultExpressions, patternTree);
          withoutNullColumns.addAll(resultExpressions);
        } else { // alias, don't concat
          withoutNullColumns.add(expression);
        }
      }
    } else {
      List<Expression> resultExpressions = new ArrayList<>();
      expression.concat(prefixPaths, resultExpressions, patternTree);
      withoutNullColumns.addAll(resultExpressions);
    }
  }

  /**
   * Concat the prefix path in the WHERE clause and the suffix path in the FROM clause into a full
   * path pattern. And remove wildcards.
   */
  private void concatWhereWithFrom(QueryStatement queryStatement) throws StatementAnalyzeException {
    WhereCondition whereCondition = queryStatement.getWhereCondition();

    // result after concat
    Set<PartialPath> filterPaths = new HashSet<>();
    whereCondition.setQueryFilter(
        concatWhereWithFrom(
            queryStatement.getFromComponent().getPrefixPaths(),
            whereCondition.getQueryFilter(),
            filterPaths));
    whereCondition.getQueryFilter().setPathSet(filterPaths);
  }

  private QueryFilter concatWhereWithFrom(
      List<PartialPath> fromPaths, QueryFilter filter, Set<PartialPath> filterPaths)
      throws StatementAnalyzeException {
    if (!filter.isLeaf()) {
      List<QueryFilter> newFilterList = new ArrayList<>();
      for (QueryFilter child : filter.getChildren()) {
        newFilterList.add(concatWhereWithFrom(fromPaths, child, filterPaths));
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

    filterPaths.addAll(concatPaths);
    if (concatPaths.size() == 1) {
      // Transform "select s1 from root.car.* where s1 > 10" to
      // "select s1 from root.car.* where root.car.*.s1 > 10"
      functionOperator.setSinglePath(concatPaths.get(0));
      patternTree.append(filter.getSinglePath());
      return filter;
    } else {
      // Transform "select s1 from root.car.d1, root.car.d2 where s1 > 10" to
      // "select s1 from root.car.d1, root.car.d2 where root.car.d1.s1 > 10 and root.car.d2.s1 > 10"
      // Note that,
      // two fork tree has to be maintained while removing stars in paths for DnfFilterOptimizer
      // requirement.
      return constructBinaryFilterTreeWithAnd(concatPaths, filter);
    }
  }

  private QueryFilter constructBinaryFilterTreeWithAnd(
      List<PartialPath> concatPaths, QueryFilter filter) throws StatementAnalyzeException {
    QueryFilter filterBinaryTree = new QueryFilter(FilterType.KW_AND);
    QueryFilter currentNode = filterBinaryTree;
    for (int i = 0; i < concatPaths.size(); i++) {
      if (i > 0 && i < concatPaths.size() - 1) {
        QueryFilter newInnerNode = new QueryFilter(FilterType.KW_AND);
        currentNode.addChildOperator(newInnerNode);
        currentNode = newInnerNode;
      }
      try {
        QueryFilter childFilter;
        if (filter instanceof InFilter) {
          childFilter =
              new InFilter(
                  filter.getFilterType(),
                  concatPaths.get(i),
                  ((InFilter) filter).getNot(),
                  ((InFilter) filter).getValues());
        } else if (filter instanceof LikeFilter) {
          childFilter =
              new LikeFilter(
                  filter.getFilterType(), concatPaths.get(i), ((LikeFilter) filter).getValue());
        } else if (filter instanceof RegexpFilter) {
          childFilter =
              new RegexpFilter(
                  filter.getFilterType(), concatPaths.get(i), ((RegexpFilter) filter).getValue());
        } else {
          childFilter =
              new BasicFunctionFilter(
                  filter.getFilterType(),
                  concatPaths.get(i),
                  ((BasicFunctionFilter) filter).getValue());
        }
        patternTree.append(childFilter.getSinglePath());
        currentNode.addChildOperator(childFilter);
      } catch (SQLParserException e) {
        throw new StatementAnalyzeException(e.getMessage());
      }
    }
    return filterBinaryTree;
  }
}
