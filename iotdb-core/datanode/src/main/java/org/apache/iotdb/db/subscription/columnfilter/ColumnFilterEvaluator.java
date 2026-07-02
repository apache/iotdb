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

package org.apache.iotdb.db.subscription.columnfilter;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CommonQueryAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.subscription.columnfilter.ColumnMetadata;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

public class ColumnFilterEvaluator implements CommonQueryAstVisitor<Boolean, ColumnMetadata> {

  public static boolean evaluate(final Expression expression, final ColumnMetadata metadata) {
    return Boolean.TRUE.equals(new ColumnFilterEvaluator().process(expression, metadata));
  }

  @Override
  public Boolean visitNode(final Node node, final ColumnMetadata context) {
    throw new IllegalArgumentException(
        String.format(
            DataNodeMiscMessages.UNSUPPORTED_EXPRESSION_FMT, node.getClass().getSimpleName()));
  }

  @Override
  public Boolean visitBooleanLiteral(final BooleanLiteral node, final ColumnMetadata context) {
    return node.getValue();
  }

  @Override
  public Boolean visitLogicalExpression(
      final LogicalExpression node, final ColumnMetadata context) {
    if (node.getOperator() == LogicalExpression.Operator.AND) {
      for (final Expression term : node.getTerms()) {
        if (!process(term, context)) {
          return false;
        }
      }
      return true;
    }

    for (final Expression term : node.getTerms()) {
      if (process(term, context)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Boolean visitNotExpression(final NotExpression node, final ColumnMetadata context) {
    return !process(node.getValue(), context);
  }

  @Override
  public Boolean visitComparisonExpression(
      final ComparisonExpression node, final ColumnMetadata context) {
    final String left = fieldValue((Identifier) node.getLeft(), context);
    final String right = ((StringLiteral) node.getRight()).getValue();
    final boolean equals = equalsIgnoreCase(left, right);
    return node.getOperator() == ComparisonExpression.Operator.EQUAL ? equals : !equals;
  }

  @Override
  public Boolean visitInPredicate(final InPredicate node, final ColumnMetadata context) {
    final String left = fieldValue((Identifier) node.getValue(), context);
    for (final Expression expression : ((InListExpression) node.getValueList()).getValues()) {
      if (equalsIgnoreCase(left, ((StringLiteral) expression).getValue())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Boolean visitLikePredicate(final LikePredicate node, final ColumnMetadata context) {
    final String left = fieldValue((Identifier) node.getValue(), context);
    final String pattern = ((StringLiteral) node.getPattern()).getValue();
    final String escape =
        node.getEscape().map(expression -> ((StringLiteral) expression).getValue()).orElse(null);
    return compileLikePattern(pattern, escape).matcher(left).matches();
  }

  @Override
  public Boolean visitFunctionCall(final FunctionCall node, final ColumnMetadata context) {
    final String left = fieldValue((Identifier) node.getArguments().get(0), context);
    final String pattern = ((StringLiteral) node.getArguments().get(1)).getValue();
    return Pattern.compile(pattern, Pattern.CASE_INSENSITIVE).matcher(left).matches();
  }

  @Override
  public Boolean visitIsNullPredicate(final IsNullPredicate node, final ColumnMetadata context) {
    return Objects.isNull(fieldValue((Identifier) node.getValue(), context));
  }

  static Pattern compileLikePattern(final String pattern, final String escape) {
    final Character escapeChar;
    if (Objects.isNull(escape)) {
      escapeChar = null;
    } else if (escape.length() == 1) {
      escapeChar = escape.charAt(0);
    } else {
      throw new IllegalArgumentException(DataNodeMiscMessages.LIKE_ESCAPE_MUST_BE_SINGLE_CHARACTER);
    }

    final StringBuilder regex = new StringBuilder();
    boolean escaping = false;
    for (int i = 0; i < pattern.length(); i++) {
      final char ch = pattern.charAt(i);
      if (Objects.nonNull(escapeChar) && ch == escapeChar && !escaping) {
        escaping = true;
        continue;
      }
      if (!escaping && ch == '%') {
        regex.append(".*");
      } else if (!escaping && ch == '_') {
        regex.append('.');
      } else {
        regex.append(Pattern.quote(String.valueOf(ch)));
      }
      escaping = false;
    }
    if (escaping) {
      throw new IllegalArgumentException(
          DataNodeMiscMessages.LIKE_PATTERN_ENDS_WITH_ESCAPE_CHARACTER);
    }
    return Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE);
  }

  private static String fieldValue(final Identifier field, final ColumnMetadata metadata) {
    switch (ColumnFilterValidator.normalizeField(field.getValue())) {
      case "database":
        return metadata.getDatabase();
      case "table_name":
        return metadata.getTableName();
      case "column_name":
        return metadata.getColumnName();
      case "datatype":
        return metadata.getDatatype();
      case "category":
        return metadata.getCategory();
      default:
        throw new IllegalArgumentException(
            String.format(
                DataNodeMiscMessages.UNSUPPORTED_COLUMN_METADATA_FIELD_FMT, field.getValue()));
    }
  }

  private static boolean equalsIgnoreCase(final String left, final String right) {
    return left.toLowerCase(Locale.ROOT).equals(right.toLowerCase(Locale.ROOT));
  }
}
