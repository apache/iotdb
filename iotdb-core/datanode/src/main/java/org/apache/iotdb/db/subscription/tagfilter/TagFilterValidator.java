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

package org.apache.iotdb.db.subscription.tagfilter;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Validates the expression shape and collects the TAG columns referenced by a tag-filter. */
public class TagFilterValidator implements CommonQueryAstVisitor<Void, Void> {

  static final int MAX_AST_DEPTH = 32;
  static final int MAX_AST_NODES = 256;
  static final int MAX_IN_VALUES = 256;
  static final int MAX_REGEXP_LENGTH = 1024;

  private static final String REGEXP_LIKE = "regexp_like";

  private final List<Identifier> referencedFields = new ArrayList<>();
  private final Map<Expression, Pattern> compiledPatterns = new IdentityHashMap<>();
  private int depth;
  private int nodeCount;

  public static List<Identifier> validate(final Expression expression) {
    return validateAndCompile(expression).getReferencedFields();
  }

  public static ValidationResult validateAndCompile(final Expression expression) {
    final TagFilterValidator validator = new TagFilterValidator();
    validator.process(expression);
    return new ValidationResult(
        Collections.unmodifiableList(new ArrayList<>(validator.referencedFields)),
        Collections.unmodifiableMap(new IdentityHashMap<>(validator.compiledPatterns)));
  }

  @Override
  public Void process(final Node node, final Void context) {
    if (node == null) {
      throw invalid(DataNodeMiscMessages.UNSUPPORTED_EXPRESSION_FMT.replace("%s", "null"));
    }
    accountNode(node);
    if (++depth > MAX_AST_DEPTH) {
      throw invalid(
          String.format(
              DataNodeMiscMessages.EXCEPTION_TAG_FILTER_AST_DEPTH_EXCEEDS_MAXIMUM_OF_ARG_64C92731,
              MAX_AST_DEPTH));
    }
    try {
      return CommonQueryAstVisitor.super.process(node, context);
    } finally {
      depth--;
    }
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
      throw invalid(
          DataNodeMiscMessages.EXCEPTION_ONLY_AND_COMPARISONS_ARE_SUPPORTED_IN_TAG_FILTER_19946958);
    }
    requireTagField(node.getLeft());
    requireStringLiteral(
        node.getRight(), DataNodeMiscMessages.COLUMN_FILTER_COMPARISON_RIGHT_OPERAND);
    return null;
  }

  @Override
  public Void visitInPredicate(final InPredicate node, final Void context) {
    requireTagField(node.getValue());
    if (!(node.getValueList() instanceof InListExpression)) {
      throw invalid(DataNodeMiscMessages.IN_PREDICATE_MUST_USE_STRING_LITERAL_LIST);
    }
    final List<Expression> values = ((InListExpression) node.getValueList()).getValues();
    if (values.isEmpty() || values.size() > MAX_IN_VALUES) {
      throw invalid(
          String.format(
              DataNodeMiscMessages
                  .EXCEPTION_TAG_FILTER_IN_LIST_EXCEEDS_MAXIMUM_OF_ARG_VALUES_07E7FC6C,
              MAX_IN_VALUES));
    }
    accountNode(node.getValueList());
    for (final Expression expression : values) {
      accountNode(expression);
      requireStringLiteral(expression, DataNodeMiscMessages.COLUMN_FILTER_IN_ELEMENT);
    }
    return null;
  }

  @Override
  public Void visitLikePredicate(final LikePredicate node, final Void context) {
    requireTagField(node.getValue());
    final StringLiteral pattern =
        requireStringLiteral(node.getPattern(), DataNodeMiscMessages.COLUMN_FILTER_LIKE_PATTERN);
    final String escape =
        node.getEscape()
            .map(
                expression ->
                    requireStringLiteral(expression, DataNodeMiscMessages.COLUMN_FILTER_LIKE_ESCAPE)
                        .getValue())
            .orElse(null);
    compiledPatterns.put(node, TagFilterEvaluator.compileLikePattern(pattern.getValue(), escape));
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

    requireTagField(node.getArguments().get(0));
    final String pattern =
        requireStringLiteral(
                node.getArguments().get(1), DataNodeMiscMessages.COLUMN_FILTER_REGEXP_PATTERN)
            .getValue();
    if (pattern.length() > MAX_REGEXP_LENGTH) {
      throw invalid(
          String.format(
              DataNodeMiscMessages
                  .EXCEPTION_TAG_FILTER_REGEXP_PATTERN_EXCEEDS_MAXIMUM_LENGTH_OF_ARG_CHARACTERS_67590971,
              MAX_REGEXP_LENGTH));
    }
    try {
      compiledPatterns.put(node, Pattern.compile(pattern));
    } catch (final PatternSyntaxException e) {
      throw invalid(String.format(DataNodeMiscMessages.ILLEGAL_REGEXP_PATTERN_FMT, e.getMessage()));
    }
    return null;
  }

  @Override
  public Void visitIsNullPredicate(final IsNullPredicate node, final Void context) {
    requireTagField(node.getValue());
    return null;
  }

  private Identifier requireTagField(final Expression expression) {
    if (!(expression instanceof Identifier)) {
      throw invalid(DataNodeMiscMessages.EXCEPTION_LEFT_OPERAND_MUST_BE_A_TAG_COLUMN_F9D4548B);
    }
    final Identifier identifier = (Identifier) expression;
    referencedFields.add(identifier);
    return identifier;
  }

  private static StringLiteral requireStringLiteral(
      final Expression expression, final String description) {
    if (!(expression instanceof StringLiteral)) {
      throw invalid(String.format(DataNodeMiscMessages.MUST_BE_STRING_LITERAL_FMT, description));
    }
    return (StringLiteral) expression;
  }

  private static IllegalArgumentException invalid(final String message) {
    return new IllegalArgumentException(message);
  }

  private void accountNode(final Node node) {
    if (++nodeCount > MAX_AST_NODES) {
      throw invalid(
          String.format(
              DataNodeMiscMessages
                  .EXCEPTION_TAG_FILTER_AST_NODE_COUNT_EXCEEDS_MAXIMUM_OF_ARG_40CCE694,
              MAX_AST_NODES));
    }
  }

  public static final class ValidationResult {

    private final List<Identifier> referencedFields;
    private final Map<Expression, Pattern> compiledPatterns;

    private ValidationResult(
        final List<Identifier> referencedFields, final Map<Expression, Pattern> compiledPatterns) {
      this.referencedFields = referencedFields;
      this.compiledPatterns = compiledPatterns;
    }

    public List<Identifier> getReferencedFields() {
      return referencedFields;
    }

    public Map<Expression, Pattern> getCompiledPatterns() {
      return compiledPatterns;
    }
  }
}
