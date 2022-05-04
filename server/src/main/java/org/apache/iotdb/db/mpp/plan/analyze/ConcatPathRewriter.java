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
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.component.FilterNullComponent;
import org.apache.iotdb.db.mpp.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.mpp.plan.statement.component.SelectComponent;
import org.apache.iotdb.db.mpp.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.query.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    // concat SELECT with FROM
    List<ResultColumn> resultColumns =
        concatSelectWithFrom(
            queryStatement.getSelectComponent(), prefixPaths, queryStatement.isGroupByLevel());
    queryStatement.getSelectComponent().setResultColumns(resultColumns);

    // concat WITHOUT NULL with FROM
    if (queryStatement.getFilterNullComponent() != null
        && !queryStatement.getFilterNullComponent().getWithoutNullColumns().isEmpty()) {
      List<Expression> withoutNullColumns =
          concatWithoutNullColumnsWithFrom(
              queryStatement.getFilterNullComponent(),
              prefixPaths,
              queryStatement.getSelectComponent().getAliasToColumnMap());
      queryStatement.getFilterNullComponent().setWithoutNullColumns(withoutNullColumns);
    }

    // concat WHERE with FROM
    if (queryStatement.getWhereCondition() != null) {
      ExpressionAnalyzer.constructPatternTreeFromQueryFilter(
          queryStatement.getWhereCondition().getPredicate(), prefixPaths, patternTree);
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
      resultColumns.addAll(
          resultExpressions.stream()
              .map(expression -> new ResultColumn(expression, resultColumn.getAlias()))
              .collect(Collectors.toList()));
    }
    return resultColumns;
  }

  /**
   * Concat the prefix path in the WITHOUT NULL clause and the suffix path in the FROM clause into a
   * full path pattern. And construct pattern tree.
   */
  private List<Expression> concatWithoutNullColumnsWithFrom(
      FilterNullComponent filterNullComponent,
      List<PartialPath> prefixPaths,
      Map<String, Expression> aliasToColumnMap)
      throws StatementAnalyzeException {
    // raw expression after replace alias
    List<Expression> rawWithoutNullColumns =
        filterNullComponent.getWithoutNullColumns().stream()
            .map(
                expression ->
                    aliasToColumnMap.getOrDefault(expression.getExpressionString(), expression))
            .collect(Collectors.toList());

    // result after concat
    return rawWithoutNullColumns.stream()
        .map(
            expression ->
                ExpressionAnalyzer.concatExpressionWithSuffixPaths(
                    expression, prefixPaths, patternTree))
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }
}
