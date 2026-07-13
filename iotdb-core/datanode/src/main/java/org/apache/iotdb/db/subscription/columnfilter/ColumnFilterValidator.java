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
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class ColumnFilterValidator implements CommonQueryAstVisitor<Void, Void> {

  private static final Set<String> LEGAL_FIELDS =
      Set.of("database", "table_name", "column_name", "datatype", "category");
  private static final String REGEXP_LIKE = "regexp_like";

  public static void validate(final Expression expression) {
    new ColumnFilterValidator().process(expression);
  }

  @Override
  public Void visitNode(final Node node, final Void context) {
    throw invalid(
        String.format(
            DataNodeMiscMessages.UNSUPPORTED_EXPRESSION_FMT, node.getClass().getSimpleName()));
  }

  @Override
  public Void visitBooleanLiteral(final BooleanLiteral node, final Void context) {
    return null;
  }

  @Override
  public Void visitLogicalExpression(final LogicalExpression node, final Void context) {
    node.getTerms().forEach(this::process);
    return null;
  }

  @Override
  public Void visitNotExpression(final NotExpression node, final Void context) {
    process(node.getValue());
    return null;
  }

  @Override
  public Void visitComparisonExpression(final ComparisonExpression node, final Void context) {
    if (node.getOperator() != ComparisonExpression.Operator.EQUAL
        && node.getOperator() != ComparisonExpression.Operator.NOT_EQUAL) {
      throw invalid(DataNodeMiscMessages.ONLY_COLUMN_FILTER_COMPARISONS_SUPPORTED);
    }
    requireField(node.getLeft());
    requireStringLiteral(
        node.getRight(), DataNodeMiscMessages.COLUMN_FILTER_COMPARISON_RIGHT_OPERAND);
    return null;
  }

  @Override
  public Void visitInPredicate(final InPredicate node, final Void context) {
    requireField(node.getValue());
    if (!(node.getValueList() instanceof InListExpression)) {
      throw invalid(DataNodeMiscMessages.IN_PREDICATE_MUST_USE_STRING_LITERAL_LIST);
    }
    for (final Expression expression : ((InListExpression) node.getValueList()).getValues()) {
      requireStringLiteral(expression, DataNodeMiscMessages.COLUMN_FILTER_IN_ELEMENT);
    }
    return null;
  }

  @Override
  public Void visitLikePredicate(final LikePredicate node, final Void context) {
    requireField(node.getValue());
    final StringLiteral pattern =
        requireStringLiteral(node.getPattern(), DataNodeMiscMessages.COLUMN_FILTER_LIKE_PATTERN);
    final String escape =
        node.getEscape()
            .map(
                expression ->
                    requireStringLiteral(expression, DataNodeMiscMessages.COLUMN_FILTER_LIKE_ESCAPE)
                        .getValue())
            .orElse(null);
    ColumnFilterEvaluator.compileLikePattern(pattern.getValue(), escape);
    return null;
  }

  @Override
  public Void visitFunctionCall(final FunctionCall node, final Void context) {
    if (!REGEXP_LIKE.equalsIgnoreCase(node.getName().toString())
        || node.isDistinct()
        || node.getProcessingMode().isPresent()
        || node.getArguments().size() != 2) {
      throw invalid(DataNodeMiscMessages.ONLY_REGEXP_SUPPORTED_AS_REGEXP_LIKE);
    }

    requireField(node.getArguments().get(0));
    final String pattern =
        requireStringLiteral(
                node.getArguments().get(1), DataNodeMiscMessages.COLUMN_FILTER_REGEXP_PATTERN)
            .getValue();
    try {
      Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
    } catch (final PatternSyntaxException e) {
      throw invalid(String.format(DataNodeMiscMessages.ILLEGAL_REGEXP_PATTERN_FMT, e.getMessage()));
    }
    return null;
  }

  @Override
  public Void visitIsNullPredicate(final IsNullPredicate node, final Void context) {
    requireField(node.getValue());
    return null;
  }

  private static Identifier requireField(final Expression expression) {
    if (!(expression instanceof Identifier)) {
      throw invalid(DataNodeMiscMessages.LEFT_OPERAND_MUST_BE_COLUMN_METADATA_FIELD);
    }
    final Identifier identifier = (Identifier) expression;
    final String normalizedField = normalizeField(identifier.getValue());
    if (!LEGAL_FIELDS.contains(normalizedField)) {
      throw invalid(
          String.format(
              DataNodeMiscMessages.UNSUPPORTED_COLUMN_METADATA_FIELD_FMT, identifier.getValue()));
    }
    return identifier;
  }

  private static StringLiteral requireStringLiteral(
      final Expression expression, final String description) {
    if (!(expression instanceof StringLiteral)) {
      throw invalid(String.format(DataNodeMiscMessages.MUST_BE_STRING_LITERAL_FMT, description));
    }
    return (StringLiteral) expression;
  }

  static String normalizeField(final String fieldName) {
    return fieldName.trim().toLowerCase(Locale.ROOT);
  }

  private static IllegalArgumentException invalid(final String message) {
    return new IllegalArgumentException(message);
  }
}
