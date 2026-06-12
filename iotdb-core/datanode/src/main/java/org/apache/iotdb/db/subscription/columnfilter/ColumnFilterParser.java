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
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class ColumnFilterParser {

  public Expression parseAndValidate(final String rawColumnFilter) throws SubscriptionException {
    try {
      final Expression expression = parse(rawColumnFilter);
      ColumnFilterValidator.validate(expression);
      return expression;
    } catch (final ParsingException | IllegalArgumentException e) {
      throw new SubscriptionException(
          String.format("Invalid column-filter: %s", e.getMessage()), e);
    }
  }

  Expression parse(final String rawColumnFilter) {
    if (rawColumnFilter == null || rawColumnFilter.trim().isEmpty()) {
      throw parsingException("column-filter should not be empty", 0);
    }
    return new InternalParser(tokenize(rawColumnFilter)).parse();
  }

  private static List<Token> tokenize(final String expression) {
    final List<Token> tokens = new ArrayList<>();
    int offset = 0;
    while (offset < expression.length()) {
      final char ch = expression.charAt(offset);
      if (Character.isWhitespace(ch)) {
        offset++;
        continue;
      }

      switch (ch) {
        case '(':
          tokens.add(new Token(TokenType.LEFT_PAREN, "(", offset));
          offset++;
          continue;
        case ')':
          tokens.add(new Token(TokenType.RIGHT_PAREN, ")", offset));
          offset++;
          continue;
        case ',':
          tokens.add(new Token(TokenType.COMMA, ",", offset));
          offset++;
          continue;
        case '=':
          tokens.add(new Token(TokenType.EQ, "=", offset));
          offset++;
          continue;
        case '!':
          if (offset + 1 < expression.length() && expression.charAt(offset + 1) == '=') {
            tokens.add(new Token(TokenType.NEQ, "!=", offset));
            offset += 2;
            continue;
          }
          throw parsingException("unexpected character '!'", offset);
        case '<':
          if (offset + 1 < expression.length() && expression.charAt(offset + 1) == '>') {
            tokens.add(new Token(TokenType.NEQ, "<>", offset));
            offset += 2;
            continue;
          }
          throw parsingException("unsupported comparison operator '<'", offset);
        case '>':
          throw parsingException("unsupported comparison operator '>'", offset);
        case '"':
          final int start = offset;
          final StringBuilder builder = new StringBuilder();
          offset++;
          while (offset < expression.length()) {
            final char current = expression.charAt(offset);
            if (current == '"') {
              if (offset + 1 < expression.length() && expression.charAt(offset + 1) == '"') {
                builder.append('"');
                offset += 2;
                continue;
              }
              offset++;
              tokens.add(new Token(TokenType.QUOTED, builder.toString(), start));
              break;
            }
            builder.append(current);
            offset++;
          }
          if (tokens.isEmpty() || tokens.get(tokens.size() - 1).position != start) {
            throw parsingException("unterminated quoted literal", start);
          }
          continue;
        default:
          if (isIdentifierStart(ch)) {
            final int startPosition = offset;
            offset++;
            while (offset < expression.length() && isIdentifierPart(expression.charAt(offset))) {
              offset++;
            }
            final String text = expression.substring(startPosition, offset);
            tokens.add(new Token(keywordType(text), text, startPosition));
            continue;
          }
          throw parsingException(String.format("unexpected character '%s'", ch), offset);
      }
    }
    tokens.add(new Token(TokenType.EOF, "", expression.length()));
    return tokens;
  }

  private static boolean isIdentifierStart(final char ch) {
    return Character.isLetter(ch) || ch == '_';
  }

  private static boolean isIdentifierPart(final char ch) {
    return Character.isLetterOrDigit(ch) || ch == '_';
  }

  private static TokenType keywordType(final String text) {
    switch (text.toUpperCase(Locale.ROOT)) {
      case "TRUE":
        return TokenType.TRUE;
      case "FALSE":
        return TokenType.FALSE;
      case "AND":
        return TokenType.AND;
      case "OR":
        return TokenType.OR;
      case "NOT":
        return TokenType.NOT;
      case "IN":
        return TokenType.IN;
      case "LIKE":
        return TokenType.LIKE;
      case "REGEXP":
        return TokenType.REGEXP;
      case "IS":
        return TokenType.IS;
      case "NULL":
        return TokenType.NULL;
      case "ESCAPE":
        return TokenType.ESCAPE;
      default:
        return TokenType.IDENTIFIER;
    }
  }

  private static ParsingException parsingException(final String message, final int position) {
    return new ParsingException(message, null, 1, position + 1);
  }

  private enum TokenType {
    IDENTIFIER,
    QUOTED,
    TRUE,
    FALSE,
    AND,
    OR,
    NOT,
    IN,
    LIKE,
    REGEXP,
    IS,
    NULL,
    ESCAPE,
    EQ,
    NEQ,
    LEFT_PAREN,
    RIGHT_PAREN,
    COMMA,
    EOF
  }

  private static class Token {
    private final TokenType type;
    private final String text;
    private final int position;

    private Token(final TokenType type, final String text, final int position) {
      this.type = type;
      this.text = text;
      this.position = position;
    }
  }

  private static class InternalParser {

    private final List<Token> tokens;
    private int cursor;

    private InternalParser(final List<Token> tokens) {
      this.tokens = tokens;
    }

    private Expression parse() {
      final Expression expression = parseOr();
      expect(TokenType.EOF);
      return expression;
    }

    private Expression parseOr() {
      Expression result = parseAnd();
      while (match(TokenType.OR)) {
        result = LogicalExpression.or(result, parseAnd());
      }
      return result;
    }

    private Expression parseAnd() {
      Expression result = parseNot();
      while (match(TokenType.AND)) {
        result = LogicalExpression.and(result, parseNot());
      }
      return result;
    }

    private Expression parseNot() {
      if (match(TokenType.NOT)) {
        return new NotExpression(parseNot());
      }
      return parsePredicate();
    }

    private Expression parsePredicate() {
      if (match(TokenType.LEFT_PAREN)) {
        final Expression expression = parseOr();
        expect(TokenType.RIGHT_PAREN);
        return expression;
      }
      if (match(TokenType.TRUE)) {
        return new BooleanLiteral("true");
      }
      if (match(TokenType.FALSE)) {
        return new BooleanLiteral("false");
      }

      final Identifier field = parseField();
      if (match(TokenType.EQ)) {
        return new ComparisonExpression(
            ComparisonExpression.Operator.EQUAL, field, parseStringLiteral());
      }
      if (match(TokenType.NEQ)) {
        return new ComparisonExpression(
            ComparisonExpression.Operator.NOT_EQUAL, field, parseStringLiteral());
      }
      if (match(TokenType.NOT)) {
        if (match(TokenType.IN)) {
          return new NotExpression(parseInPredicate(field));
        }
        if (match(TokenType.LIKE)) {
          return new NotExpression(parseLikePredicate(field));
        }
        if (match(TokenType.REGEXP)) {
          return new NotExpression(parseRegexpFunction(field));
        }
        throw parsingException("expected IN, LIKE, or REGEXP after NOT", previous().position);
      }
      if (match(TokenType.IN)) {
        return parseInPredicate(field);
      }
      if (match(TokenType.LIKE)) {
        return parseLikePredicate(field);
      }
      if (match(TokenType.REGEXP)) {
        return parseRegexpFunction(field);
      }
      if (match(TokenType.IS)) {
        final boolean isNot = match(TokenType.NOT);
        expect(TokenType.NULL);
        final Expression isNull = new IsNullPredicate(field);
        return isNot ? new NotExpression(isNull) : isNull;
      }

      throw parsingException("expected column predicate operator", peek().position);
    }

    private InPredicate parseInPredicate(final Identifier field) {
      expect(TokenType.LEFT_PAREN);
      final List<Expression> values = new ArrayList<>();
      values.add(parseStringLiteral());
      while (match(TokenType.COMMA)) {
        values.add(parseStringLiteral());
      }
      expect(TokenType.RIGHT_PAREN);
      return new InPredicate(field, new InListExpression(values));
    }

    private LikePredicate parseLikePredicate(final Identifier field) {
      final StringLiteral pattern = parseStringLiteral();
      if (match(TokenType.ESCAPE)) {
        return new LikePredicate(field, pattern, parseStringLiteral());
      }
      return new LikePredicate(field, pattern);
    }

    private FunctionCall parseRegexpFunction(final Identifier field) {
      return new FunctionCall(
          QualifiedName.of("regexp_like"), Arrays.asList(field, parseStringLiteral()));
    }

    private Identifier parseField() {
      if (match(TokenType.IDENTIFIER)) {
        return new Identifier(previous().text, false);
      }
      if (match(TokenType.QUOTED)) {
        return new Identifier(previous().text, true);
      }
      throw parsingException("expected column metadata field", peek().position);
    }

    private StringLiteral parseStringLiteral() {
      if (match(TokenType.QUOTED)) {
        return new StringLiteral(previous().text);
      }
      throw parsingException("expected string literal", peek().position);
    }

    private boolean match(final TokenType type) {
      if (peek().type != type) {
        return false;
      }
      cursor++;
      return true;
    }

    private Token expect(final TokenType type) {
      if (peek().type == type) {
        cursor++;
        return previous();
      }
      throw parsingException(
          String.format("expected %s but found '%s'", type, peek().text), peek().position);
    }

    private Token peek() {
      return tokens.get(cursor);
    }

    private Token previous() {
      return tokens.get(cursor - 1);
    }
  }
}
