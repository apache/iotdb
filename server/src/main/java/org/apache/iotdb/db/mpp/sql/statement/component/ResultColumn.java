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

package org.apache.iotdb.db.mpp.sql.statement.component;

import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.sql.rewriter.WildcardsRemover;
import org.apache.iotdb.db.mpp.sql.statement.StatementNode;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class is used to represent a result column of a query.
 *
 * <p>Assume that we have time series in db as follows: <br>
 * [ root.sg.d.a, root.sg.d.b, root.sg.e.a, root.sg.e.b ]
 *
 * <ul>
 *   Example 1: select a, a + b, udf(udf(b)) from root.sg.d, root.sg.e;
 *   <li>Step 1: constructed by sql visitor in logical operator: <br>
 *       result columns: <br>
 *       [a, a + b, udf(udf(b))]
 *   <li>Step 2: concatenated with prefix paths in logical optimizer:<br>
 *       result columns: <br>
 *       [root.sg.d.a, root.sg.e.a, root.sg.d.a + root.sg.d.b, root.sg.d.a + root.sg.e.b,
 *       root.sg.e.a + root.sg.d.b, root.sg.e.a + root.sg.e.b, udf(udf(root.sg.d.b)),
 *       udf(udf(root.sg.e.b))]
 *   <li>Step 3: remove wildcards in logical optimizer:<br>
 *       result columns: <br>
 *       [root.sg.d.a, root.sg.e.a, root.sg.d.a + root.sg.d.b, root.sg.d.a + root.sg.e.b,
 *       root.sg.e.a + root.sg.d.b, root.sg.e.a + root.sg.e.b, udf(udf(root.sg.d.b)),
 *       udf(udf(root.sg.e.b))]
 * </ul>
 *
 * <ul>
 *   Example 2: select *, a + *, udf(udf(*)) from root.sg.d;
 *   <li>Step 1: constructed by sql visitor in logical operator: <br>
 *       result columns: <br>
 *       [*, a + * , udf(udf(*))]
 *   <li>Step 2: concatenated with prefix paths in logical optimizer:<br>
 *       result columns: <br>
 *       [root.sg.d.*, root.sg.d.a + root.sg.d.*, udf(udf(root.sg.d.*))]
 *   <li>Step 3: remove wildcards in logical optimizer:<br>
 *       result columns: <br>
 *       [root.sg.d.a, root.sg.d.b, root.sg.d.a + root.sg.d.a, root.sg.d.a + root.sg.d.b,
 *       udf(udf(root.sg.d.a)), udf(udf(root.sg.d.b))]
 * </ul>
 */
public class ResultColumn extends StatementNode {

  private final Expression expression;
  private final String alias;

  private TSDataType dataType;

  private List<PartialPath> allPathsInExpression;

  public ResultColumn(Expression expression, String alias) {
    this.expression = expression;
    this.alias = alias;
  }

  public ResultColumn(Expression expression) {
    this.expression = expression;
    alias = null;
  }

  public List<PartialPath> collectPaths() {
    if (allPathsInExpression == null) {
      Set<PartialPath> pathSet = new HashSet<>();
      expression.collectPaths(pathSet);
      allPathsInExpression = new ArrayList<>(pathSet);
    }
    return allPathsInExpression;
  }

  public Expression getExpression() {
    return expression;
  }

  public boolean hasAlias() {
    return alias != null;
  }

  public String getAlias() {
    return alias;
  }

  public String getResultColumnName() {
    return alias != null ? alias : expression.getExpressionString();
  }

  public String getExpressionString() {
    return expression.getExpressionString();
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  /**
   * @param prefixPaths prefix paths in the from clause
   * @param resultColumns used to collect the result columns
   * @param needAliasCheck used to skip illegal alias judgement here. Including !isGroupByLevel
   *     because count(*) may be * unfolded to more than one expression, but it still can be
   *     aggregated together later.
   */
  public void concat(
      List<PartialPath> prefixPaths,
      List<ResultColumn> resultColumns,
      boolean needAliasCheck,
      PathPatternTree patternTree)
      throws StatementAnalyzeException {
    List<Expression> resultExpressions = new ArrayList<>();
    expression.concat(prefixPaths, resultExpressions, patternTree);
    if (needAliasCheck && 1 < resultExpressions.size()) {
      throw new StatementAnalyzeException(
          String.format("alias '%s' can only be matched with one time series", alias));
    }
    for (Expression resultExpression : resultExpressions) {
      resultColumns.add(new ResultColumn(resultExpression, alias));
    }
  }

  /**
   * @param wildcardsRemover used to remove wildcards from {@code expression} and apply slimit &
   *     soffset control
   * @param resultColumns used to collect the result columns
   * @param needAliasCheck used to skip illegal alias judgement here. Including !isGroupByLevel
   *     because count(*) may be * unfolded to more than one expression, but it still can be
   *     aggregated together later.
   */
  public void removeWildcards(
      WildcardsRemover wildcardsRemover, List<ResultColumn> resultColumns, boolean needAliasCheck)
      throws StatementAnalyzeException {
    List<Expression> resultExpressions = new ArrayList<>();
    expression.removeWildcards(wildcardsRemover, resultExpressions);
    if (needAliasCheck && 1 < resultExpressions.size()) {
      throw new StatementAnalyzeException(
          String.format("alias '%s' can only be matched with one time series", alias));
    }
    for (Expression resultExpression : resultExpressions) {
      resultColumns.add(new ResultColumn(resultExpression, alias));
    }
  }

  public ColumnHeader constructColumnHeader() {
    return new ColumnHeader(
        this.getExpressionString(),
        ((TimeSeriesOperand) this.getExpression()).getPath().getSeriesType(),
        this.getAlias());
  }

  @Override
  public String toString() {
    return "ResultColumn{" + "expression=" + expression + ", alias='" + alias + '\'' + '}';
  }

  @Override
  public final int hashCode() {
    return alias == null ? getResultColumnName().hashCode() : alias.hashCode();
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof org.apache.iotdb.db.query.expression.ResultColumn)) {
      return false;
    }
    return getResultColumnName()
        .equals(((org.apache.iotdb.db.query.expression.ResultColumn) o).getResultColumnName());
  }
}
