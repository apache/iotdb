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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;

import java.util.ArrayList;
import java.util.List;

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
      throws StatementAnalyzeException {
    QueryStatement queryStatement = (QueryStatement) statement;
    this.patternTree = patternTree;

    // prefix paths in the FROM clause
    List<PartialPath> prefixPaths = queryStatement.getFromComponent().getPrefixPaths();

    if (queryStatement.isAlignByDevice()) {
      for (ResultColumn resultColumn : queryStatement.getSelectComponent().getResultColumns()) {
        ExpressionAnalyzer.constructPatternTreeFromExpression(
            resultColumn.getExpression(), prefixPaths, patternTree);
      }
      if (queryStatement.hasGroupByExpression()) {
        ExpressionAnalyzer.constructPatternTreeFromExpression(
            queryStatement.getGroupByComponent().getControlColumnExpression(),
            prefixPaths,
            patternTree);
      }
    } else {
      // concat SELECT with FROM
      List<ResultColumn> resultColumns =
          concatSelectWithFrom(
              queryStatement.getSelectComponent(), prefixPaths, queryStatement.isGroupByLevel());
      queryStatement.getSelectComponent().setResultColumns(resultColumns);

      // concat GROUP BY VARIATION with FROM
      if (queryStatement.hasGroupByExpression()) {
        queryStatement
            .getGroupByComponent()
            .setControlColumnExpression(
                contactGroupByWithFrom(
                    queryStatement.getGroupByComponent().getControlColumnExpression(),
                    prefixPaths));
      }
    }

    // concat WHERE with FROM
    if (queryStatement.getWhereCondition() != null) {
      ExpressionAnalyzer.constructPatternTreeFromExpression(
          queryStatement.getWhereCondition().getPredicate(), prefixPaths, patternTree);
    }

    // concat HAVING with FROM
    if (queryStatement.getHavingCondition() != null) {
      ExpressionAnalyzer.constructPatternTreeFromExpression(
          queryStatement.getHavingCondition().getPredicate(), prefixPaths, patternTree);
    }

    return queryStatement;
  }

  /**
   * Concat the prefix path in the SELECT clause and the suffix path in the FROM clause into a full
   * path pattern. And construct pattern tree.
   */
  private List<ResultColumn> concatSelectWithFrom(
      SelectComponent selectComponent, List<PartialPath> prefixPaths, boolean isGroupByLevel)
      throws StatementAnalyzeException {
    // resultColumns after concat
    List<ResultColumn> resultColumns = new ArrayList<>();
    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      boolean needAliasCheck = resultColumn.hasAlias() && !isGroupByLevel;
      List<Expression> resultExpressions =
          ExpressionAnalyzer.concatExpressionWithSuffixPaths(
              resultColumn.getExpression(), prefixPaths, patternTree);
      if (needAliasCheck && resultExpressions.size() > 1) {
        throw new SemanticException(
            String.format(
                "alias '%s' can only be matched with one time series", resultColumn.getAlias()));
      }

      for (Expression resultExpression : resultExpressions) {
        resultColumns.add(
            new ResultColumn(
                resultExpression, resultColumn.getAlias(), resultColumn.getColumnType()));
      }
    }
    return resultColumns;
  }

  private Expression contactGroupByWithFrom(Expression expression, List<PartialPath> prefixPaths) {
    List<Expression> resultExpressions =
        ExpressionAnalyzer.concatExpressionWithSuffixPaths(expression, prefixPaths, patternTree);
    if (resultExpressions.size() != 1) {
      throw new IllegalStateException("Expression in group by should indicate one value");
    }
    return resultExpressions.get(0);
  }
}
