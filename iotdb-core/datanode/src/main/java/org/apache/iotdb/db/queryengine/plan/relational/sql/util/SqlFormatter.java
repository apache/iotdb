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

package org.apache.iotdb.db.queryengine.plan.relational.sql.util;

import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.Node;
import org.apache.iotdb.db.node_commons.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.db.node_commons.plan.relational.sql.util.ExpressionFormatter;

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class SqlFormatter {

  protected static final String INDENT = "   ";

  private SqlFormatter() {}

  public static String formatSql(Node root) {
    StringBuilder builder = new StringBuilder();
    new DataNodeSqlFormatter(builder).process(root, 0);
    return builder.toString();
  }

  static String formatName(Identifier identifier) {
    return ExpressionFormatter.formatExpression(identifier);
  }

  public static String formatName(QualifiedName name) {
    return name.getOriginalParts().stream().map(SqlFormatter::formatName).collect(joining("."));
  }

  static String formatExpression(Expression expression) {
    return ExpressionFormatter.formatExpression(expression);
  }

  protected static class SqlBuilder {
    private final StringBuilder builder;

    public SqlBuilder(StringBuilder builder) {
      this.builder = requireNonNull(builder, "builder is null");
    }

    @CanIgnoreReturnValue
    public SqlBuilder append(CharSequence value) {
      builder.append(value);
      return this;
    }

    @CanIgnoreReturnValue
    public SqlBuilder append(char c) {
      builder.append(c);
      return this;
    }
  }
}
