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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/** Evaluates a validated tag-filter with SQL three-valued NULL semantics. */
public class TagFilterEvaluator
    implements CommonQueryAstVisitor<
        TagFilterEvaluator.TruthValue, TagFilterEvaluator.ValueProvider> {

  enum TruthValue {
    TRUE,
    FALSE,
    UNKNOWN
  }

  private final Map<Expression, Pattern> compiledPatterns;

  private TagFilterEvaluator() {
    this(Collections.emptyMap());
  }

  private TagFilterEvaluator(final Map<Expression, Pattern> compiledPatterns) {
    this.compiledPatterns = compiledPatterns;
  }

  @FunctionalInterface
  public interface ValueProvider {

    String getValue(Identifier identifier);
  }

  public static boolean evaluate(final Expression expression, final ValueProvider valueProvider) {
    final TagFilterValidator.ValidationResult validation =
        TagFilterValidator.validateAndCompile(expression);
    return evaluate(expression, valueProvider, validation.getCompiledPatterns());
  }

  public static boolean evaluate(
      final Expression expression,
      final ValueProvider valueProvider,
      final Map<Expression, Pattern> compiledPatterns) {
    return new TagFilterEvaluator(compiledPatterns).process(expression, valueProvider)
        == TruthValue.TRUE;
  }

  @Override
  public TruthValue visitNode(final Node node, final ValueProvider context) {
    throw new IllegalArgumentException(
        String.format(
            DataNodeMiscMessages.UNSUPPORTED_EXPRESSION_FMT, node.getClass().getSimpleName()));
  }

  @Override
  public TruthValue visitBooleanLiteral(final BooleanLiteral node, final ValueProvider context) {
    return node.getValue() ? TruthValue.TRUE : TruthValue.FALSE;
  }

  @Override
  public TruthValue visitLogicalExpression(
      final LogicalExpression node, final ValueProvider context) {
    boolean hasUnknown = false;
    if (node.getOperator() == LogicalExpression.Operator.AND) {
      for (final Expression term : node.getTerms()) {
        final TruthValue result = process(term, context);
        if (result == TruthValue.FALSE) {
          return TruthValue.FALSE;
        }
        hasUnknown |= result == TruthValue.UNKNOWN;
      }
      return hasUnknown ? TruthValue.UNKNOWN : TruthValue.TRUE;
    }

    for (final Expression term : node.getTerms()) {
      final TruthValue result = process(term, context);
      if (result == TruthValue.TRUE) {
        return TruthValue.TRUE;
      }
      hasUnknown |= result == TruthValue.UNKNOWN;
    }
    return hasUnknown ? TruthValue.UNKNOWN : TruthValue.FALSE;
  }

  @Override
  public TruthValue visitNotExpression(final NotExpression node, final ValueProvider context) {
    final TruthValue result = process(node.getValue(), context);
    if (result == TruthValue.UNKNOWN) {
      return TruthValue.UNKNOWN;
    }
    return result == TruthValue.TRUE ? TruthValue.FALSE : TruthValue.TRUE;
  }

  @Override
  public TruthValue visitComparisonExpression(
      final ComparisonExpression node, final ValueProvider context) {
    final String left = context.getValue((Identifier) node.getLeft());
    if (Objects.isNull(left)) {
      return TruthValue.UNKNOWN;
    }
    final boolean equals = left.equals(((StringLiteral) node.getRight()).getValue());
    return booleanValue(
        node.getOperator() == ComparisonExpression.Operator.EQUAL ? equals : !equals);
  }

  @Override
  public TruthValue visitInPredicate(final InPredicate node, final ValueProvider context) {
    final String left = context.getValue((Identifier) node.getValue());
    if (Objects.isNull(left)) {
      return TruthValue.UNKNOWN;
    }
    for (final Expression expression : ((InListExpression) node.getValueList()).getValues()) {
      if (left.equals(((StringLiteral) expression).getValue())) {
        return TruthValue.TRUE;
      }
    }
    return TruthValue.FALSE;
  }

  @Override
  public TruthValue visitLikePredicate(final LikePredicate node, final ValueProvider context) {
    final String left = context.getValue((Identifier) node.getValue());
    if (Objects.isNull(left)) {
      return TruthValue.UNKNOWN;
    }
    final Pattern pattern = compiledPatterns.get(node);
    if (pattern == null) {
      throw new IllegalArgumentException(
          DataNodeMiscMessages.EXCEPTION_UNCOMPILED_LIKE_PREDICATE_01AEE439);
    }
    return booleanValue(pattern.matcher(left).matches());
  }

  @Override
  public TruthValue visitFunctionCall(final FunctionCall node, final ValueProvider context) {
    final String left = context.getValue((Identifier) node.getArguments().get(0));
    if (Objects.isNull(left)) {
      return TruthValue.UNKNOWN;
    }
    final Pattern pattern = compiledPatterns.get(node);
    if (pattern == null) {
      throw new IllegalArgumentException(
          DataNodeMiscMessages.EXCEPTION_UNCOMPILED_REGEXP_PREDICATE_2B0DD646);
    }
    return booleanValue(pattern.matcher(left).matches());
  }

  @Override
  public TruthValue visitIsNullPredicate(final IsNullPredicate node, final ValueProvider context) {
    return booleanValue(Objects.isNull(context.getValue((Identifier) node.getValue())));
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
    return Pattern.compile(regex.toString(), Pattern.DOTALL);
  }

  private static TruthValue booleanValue(final boolean value) {
    return value ? TruthValue.TRUE : TruthValue.FALSE;
  }
}
