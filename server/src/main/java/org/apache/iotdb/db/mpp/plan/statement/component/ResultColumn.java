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

package org.apache.iotdb.db.mpp.plan.statement.component;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.statement.StatementNode;

import java.util.Objects;

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

  private final ColumnType columnType;

  public ResultColumn(Expression expression, String alias, ColumnType columnType) {
    this.expression = expression;
    this.alias = alias;
    this.columnType = columnType;
  }

  public ResultColumn(Expression expression, ColumnType columnType) {
    this.expression = expression;
    this.columnType = columnType;
    alias = null;
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

  public ColumnType getColumnType() {
    return columnType;
  }

  @Override
  public String toString() {
    return "ResultColumn{" + "expression=" + expression + ", alias='" + alias + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResultColumn that = (ResultColumn) o;
    return Objects.equals(expression, that.expression) && Objects.equals(alias, that.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, alias);
  }

  public enum ColumnType {
    RAW,
    AGGREGATION,
    CONSTANT
  }
}
