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
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.mpp.common.filter.*;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
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
      constructPatternTreeFromWhereWithFrom(queryStatement);
    }
    return queryStatement;
  }

  /**
   * Concat the prefix path in the SELECT clause and the suffix path in the FROM clause into a full
   * path pattern. And construct pattern tree.
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
   * full path pattern. And construct pattern tree.
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
        patternTree.appendPath(((TimeSeriesOperand) expression).getPath());
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
   * path pattern. And construct pattern tree.
   */
  private void constructPatternTreeFromWhereWithFrom(QueryStatement queryStatement) {
    constructPatternTreeFromWhereWithFrom(
        queryStatement.getFromComponent().getPrefixPaths(),
        queryStatement.getWhereCondition().getQueryFilter());
  }

  private void constructPatternTreeFromWhereWithFrom(
      List<PartialPath> fromPaths, QueryFilter filter) {
    if (!filter.isLeaf()) {
      for (QueryFilter child : filter.getChildren()) {
        constructPatternTreeFromWhereWithFrom(fromPaths, child);
      }
      return;
    }

    FunctionFilter functionOperator = (FunctionFilter) filter;
    PartialPath filterPath = functionOperator.getSinglePath();
    List<PartialPath> concatPaths = new ArrayList<>();
    if (SQLConstant.isReservedPath(filterPath)) {
      // do nothing in the case of "where time > 5"
      return;
    } else if (filterPath.getFirstNode().startsWith(SQLConstant.ROOT)) {
      // do nothing in the case of "where root.d1.s1 > 5"
      concatPaths.add(filterPath);
    } else {
      fromPaths.forEach(fromPath -> concatPaths.add(fromPath.concatPath(filterPath)));
    }
    concatPaths.forEach(concatPath -> patternTree.appendPath(concatPath));
  }
}
