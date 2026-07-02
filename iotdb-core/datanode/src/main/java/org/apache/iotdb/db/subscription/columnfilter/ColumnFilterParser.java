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
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.parser.ParsingException;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.relational.grammar.sql.SubscriptionColumnFilterBaseVisitor;
import org.apache.iotdb.db.relational.grammar.sql.SubscriptionColumnFilterLexer;
import org.apache.iotdb.db.relational.grammar.sql.SubscriptionColumnFilterParser;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class ColumnFilterParser {

  private static final Pattern SINGLE_FIELD_PATTERN =
      Pattern.compile("\\s*(?:[A-Za-z_][A-Za-z_0-9]*|\"(?:\"\"|[^\"])*\")\\s*");
  private static final Pattern FUNCTION_CALL_START_PATTERN =
      Pattern.compile("\\s*(?:[A-Za-z_][A-Za-z_0-9]*|\"(?:\"\"|[^\"])*\")\\s*\\(.*");
  private static final Pattern UNQUOTED_COMPARISON_RIGHT_PATTERN =
      Pattern.compile("(?is).*(?:!=|<>|=)\\s*[A-Za-z_][A-Za-z_0-9]*\\s*");

  private static final BaseErrorListener ERROR_LISTENER =
      new BaseErrorListener() {
        @Override
        public void syntaxError(
            final Recognizer<?, ?> recognizer,
            final Object offendingSymbol,
            final int line,
            final int charPositionInLine,
            final String message,
            final RecognitionException e) {
          throw new ParsingException(message, e, line, charPositionInLine + 1);
        }
      };

  public Expression parseAndValidate(final String rawColumnFilter) throws SubscriptionException {
    try {
      final Expression expression = parse(rawColumnFilter);
      ColumnFilterValidator.validate(expression);
      return expression;
    } catch (final ParsingException | IllegalArgumentException e) {
      throw new SubscriptionException(
          String.format(DataNodeMiscMessages.INVALID_COLUMN_FILTER_FMT, e.getMessage()), e);
    }
  }

  Expression parse(final String rawColumnFilter) {
    if (rawColumnFilter == null || rawColumnFilter.trim().isEmpty()) {
      throw new ParsingException(
          DataNodeMiscMessages.COLUMN_FILTER_SHOULD_NOT_BE_EMPTY, null, 1, 1);
    }
    validateUnsupportedSyntax(rawColumnFilter);

    final SubscriptionColumnFilterLexer lexer =
        new SubscriptionColumnFilterLexer(CharStreams.fromString(rawColumnFilter));
    final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    final SubscriptionColumnFilterParser parser = new SubscriptionColumnFilterParser(tokenStream);

    lexer.removeErrorListeners();
    lexer.addErrorListener(ERROR_LISTENER);
    parser.removeErrorListeners();
    parser.addErrorListener(ERROR_LISTENER);
    parser.setErrorHandler(
        new DefaultErrorStrategy() {
          @Override
          public Token recoverInline(final Parser recognizer) throws RecognitionException {
            if (nextTokensContext == null) {
              throw new InputMismatchException(recognizer);
            }
            throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext);
          }
        });

    try {
      parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
      return new AstBuilder().visit(parser.subscriptionColumnFilter());
    } catch (final ParsingException e) {
      tokenStream.seek(0);
      parser.reset();
      parser.getInterpreter().setPredictionMode(PredictionMode.LL);
      return new AstBuilder().visit(parser.subscriptionColumnFilter());
    }
  }

  private static void validateUnsupportedSyntax(final String rawColumnFilter) {
    final String trimmedColumnFilter = rawColumnFilter.trim();
    if ((SINGLE_FIELD_PATTERN.matcher(rawColumnFilter).matches()
            && !"true".equalsIgnoreCase(trimmedColumnFilter)
            && !"false".equalsIgnoreCase(trimmedColumnFilter))
        || FUNCTION_CALL_START_PATTERN.matcher(rawColumnFilter).matches()) {
      throw new ParsingException(
          DataNodeMiscMessages.EXPECTED_COLUMN_PREDICATE_OPERATOR, null, 1, 1);
    }
    if (UNQUOTED_COMPARISON_RIGHT_PATTERN.matcher(rawColumnFilter).matches()) {
      throw new ParsingException(DataNodeMiscMessages.EXPECTED_STRING_LITERAL, null, 1, 1);
    }
    for (int i = 0; i < rawColumnFilter.length(); i++) {
      final char ch = rawColumnFilter.charAt(i);
      if (ch == '"') {
        i = skipQuotedIdentifier(rawColumnFilter, i);
        continue;
      }
      if (ch == '<') {
        if (i + 1 < rawColumnFilter.length() && rawColumnFilter.charAt(i + 1) == '>') {
          i++;
          continue;
        }
        throw new ParsingException(
            String.format(DataNodeMiscMessages.UNSUPPORTED_COMPARISON_OPERATOR_FMT, "<"),
            null,
            1,
            i + 1);
      }
      if (ch == '>') {
        throw new ParsingException(
            String.format(DataNodeMiscMessages.UNSUPPORTED_COMPARISON_OPERATOR_FMT, ">"),
            null,
            1,
            i + 1);
      }
      if (ch == '+') {
        throw new ParsingException(
            String.format(DataNodeMiscMessages.UNEXPECTED_CHARACTER_FMT, "+"), null, 1, i + 1);
      }
    }
  }

  private static int skipQuotedIdentifier(final String text, final int quoteIndex) {
    for (int i = quoteIndex + 1; i < text.length(); i++) {
      if (text.charAt(i) == '"') {
        if (i + 1 < text.length() && text.charAt(i + 1) == '"') {
          i++;
          continue;
        }
        return i;
      }
    }
    return text.length();
  }

  private static class AstBuilder extends SubscriptionColumnFilterBaseVisitor<Expression> {

    @Override
    public Expression visitSubscriptionColumnFilter(
        final SubscriptionColumnFilterParser.SubscriptionColumnFilterContext context) {
      return visit(context.booleanExpression());
    }

    @Override
    public Expression visitPredicateExpression(
        final SubscriptionColumnFilterParser.PredicateExpressionContext context) {
      return visit(context.predicate());
    }

    @Override
    public Expression visitLogicalNot(
        final SubscriptionColumnFilterParser.LogicalNotContext context) {
      return new NotExpression(visit(context.booleanExpression()));
    }

    @Override
    public Expression visitLogicalBinary(
        final SubscriptionColumnFilterParser.LogicalBinaryContext context) {
      final Expression left = visit(context.booleanExpression(0));
      final Expression right = visit(context.booleanExpression(1));
      return Objects.nonNull(context.AND())
          ? LogicalExpression.and(left, right)
          : LogicalExpression.or(left, right);
    }

    @Override
    public Expression visitPredicate(
        final SubscriptionColumnFilterParser.PredicateContext context) {
      if (Objects.nonNull(context.booleanValue())) {
        return visit(context.booleanValue());
      }
      if (Objects.nonNull(context.booleanExpression())) {
        return visit(context.booleanExpression());
      }

      final Identifier field = toIdentifier(context.field());
      if (Objects.nonNull(context.comparisonOperator())) {
        return new ComparisonExpression(
            Objects.nonNull(context.comparisonOperator().EQ())
                ? ComparisonExpression.Operator.EQUAL
                : ComparisonExpression.Operator.NOT_EQUAL,
            field,
            toStringLiteral(context.string(0)));
      }
      if (Objects.nonNull(context.IN())) {
        final List<Expression> values = new ArrayList<>();
        for (final SubscriptionColumnFilterParser.StringContext string : context.string()) {
          values.add(toStringLiteral(string));
        }
        return maybeNegate(
            new InPredicate(field, new InListExpression(values)), Objects.nonNull(context.NOT()));
      }
      if (Objects.nonNull(context.LIKE())) {
        final Expression like =
            context.string().size() > 1
                ? new LikePredicate(
                    field, toStringLiteral(context.string(0)), toStringLiteral(context.string(1)))
                : new LikePredicate(field, toStringLiteral(context.string(0)));
        return maybeNegate(like, Objects.nonNull(context.NOT()));
      }
      if (Objects.nonNull(context.REGEXP())) {
        final Expression regexp =
            new FunctionCall(
                QualifiedName.of("regexp_like"),
                List.of(field, toStringLiteral(context.string(0))));
        return maybeNegate(regexp, Objects.nonNull(context.NOT()));
      }
      if (Objects.nonNull(context.IS())) {
        return maybeNegate(new IsNullPredicate(field), Objects.nonNull(context.NOT()));
      }

      throw new IllegalArgumentException(DataNodeMiscMessages.UNSUPPORTED_COLUMN_FILTER_PREDICATE);
    }

    @Override
    public Expression visitBooleanValue(
        final SubscriptionColumnFilterParser.BooleanValueContext context) {
      return Objects.nonNull(context.TRUE())
          ? BooleanLiteral.TRUE_LITERAL
          : BooleanLiteral.FALSE_LITERAL;
    }

    private static Expression maybeNegate(final Expression expression, final boolean negated) {
      return negated ? new NotExpression(expression) : expression;
    }

    private static Identifier toIdentifier(
        final SubscriptionColumnFilterParser.FieldContext context) {
      final TerminalNode quoted = context.QUOTED_IDENTIFIER();
      if (Objects.nonNull(quoted)) {
        return new Identifier(unquote(quoted.getText()), true);
      }
      return new Identifier(context.IDENTIFIER().getText());
    }

    private static StringLiteral toStringLiteral(
        final SubscriptionColumnFilterParser.StringContext context) {
      return new StringLiteral(unquote(context.QUOTED_IDENTIFIER().getText()));
    }

    private static String unquote(final String text) {
      return text.substring(1, text.length() - 1).replace("\"\"", "\"");
    }
  }
}
