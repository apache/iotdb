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

package org.apache.iotdb.db.query.expression;

import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.utils.WildcardsRemover;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ResultColumn {

  private final Expression expression;
  private final String alias;

  public ResultColumn(Expression expression, String alias) {
    this.expression = expression;
    this.alias = alias;
  }

  public ResultColumn(Expression expression) {
    this.expression = expression;
    alias = null;
  }

  public void concat(List<PartialPath> prefixPaths, List<ResultColumn> resultColumns)
      throws LogicalOptimizeException {
    List<Expression> resultExpressions = new ArrayList<>();
    expression.concat(prefixPaths, resultExpressions);
    if (hasAlias() && 1 < resultExpressions.size()) {
      throw new LogicalOptimizeException(
          String.format("alias '%s' can only be matched with one time series", alias));
    }
    for (Expression resultExpression : resultExpressions) {
      resultColumns.add(new ResultColumn(resultExpression, alias));
    }
  }

  public void removeWildcards(WildcardsRemover wildcardsRemover, List<ResultColumn> resultColumns)
      throws LogicalOptimizeException {
    List<Expression> resultExpressions = new ArrayList<>();
    expression.removeWildcards(wildcardsRemover, resultExpressions);
    if (hasAlias() && 1 < resultExpressions.size()) {
      throw new LogicalOptimizeException(
          String.format("alias '%s' can only be matched with one time series", alias));
    }
    for (Expression resultExpression : resultExpressions) {
      resultColumns.add(new ResultColumn(resultExpression, alias));
    }
  }

  public Set<PartialPath> collectPaths() {
    Set<PartialPath> pathSet = new HashSet<>();
    expression.collectPaths(pathSet);
    return pathSet;
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
    return alias != null ? alias : expression.toString();
  }
}
