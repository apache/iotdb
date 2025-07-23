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

package org.apache.iotdb.db.auth;

import org.apache.iotdb.commons.schema.SecurityLabel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Label policy expression evaluator for LBAC (Label-Based Access Control).
 *
 * <p>Supports policy expressions like: - env="prod" and region="cn" - env!="test" or (region="eu"
 * and role!="guest") - level=3 - level > 4 - level<=2
 */
public class LabelPolicyEvaluator {

  private static final Logger LOGGER = LoggerFactory.getLogger(LabelPolicyEvaluator.class);

  // Regex patterns for parsing policy expressions - support single quotes only
  private static final Pattern TOKEN_PATTERN =
      Pattern.compile(
          "\\s*(\\(|\\)|AND|OR|[a-zA-Z][a-zA-Z0-9_]*\\s*(?:=|!=|>|<|>=|<=)\\s*(?:'[^']*'|\\d+))\\s*",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern CONDITION_PATTERN =
      Pattern.compile(
          "([a-zA-Z][a-zA-Z0-9_]*)\\s*(=|!=|>|<|>=|<=)\\s*('[^']*'|\\d+)",
          Pattern.CASE_INSENSITIVE);

  /** Enum for expression tokens */
  private enum TokenType {
    CONDITION,
    AND,
    OR,
    LEFT_PAREN,
    RIGHT_PAREN
  }

  /** Token class for parsing */
  private static class Token {
    private final TokenType type;
    private final String value;

    public Token(TokenType type, String value) {
      this.type = type;
      this.value = value;
    }

    public TokenType getType() {
      return type;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.format("Token{type=%s, value='%s'}", type, value);
    }
  }

  /** Private constructor - utility class should not be instantiated */
  private LabelPolicyEvaluator() {
    // empty constructor
  }

  /**
   * Evaluate a label policy expression against security labels
   *
   * @param policyExpression the policy expression to evaluate (e.g., "env='prod' and region='cn'")
   * @param securityLabel the security labels to match against
   * @return true if the policy matches the labels, false otherwise
   * @throws IllegalArgumentException if the policy expression is invalid
   */
  public static boolean evaluate(String policyExpression, SecurityLabel securityLabel) {
    if (policyExpression == null || policyExpression.trim().isEmpty()) {
      throw new IllegalArgumentException("Policy expression cannot be null or empty");
    }

    if (securityLabel == null) {
      LOGGER.debug("Security label is null, policy  evaluation fails");
      return false;
    }

    try {
      LOGGER.debug(
          "Evaluating policy: {} against labels: {}", policyExpression, securityLabel.getLabels());
      return evaluateExpression(policyExpression.trim(), securityLabel.getLabels());
    } catch (Exception e) {
      LOGGER.error("Error evaluating policy expression: {}", policyExpression, e);
      throw new IllegalArgumentException("Invalid policy expression: " + policyExpression, e);
    }
  }

  /**
   * Parse and evaluate the expression using recursive descent parsing
   *
   * @param expression the expression to evaluate
   * @param labels the security labels map
   * @return evaluation result
   */
  private static boolean evaluateExpression(String expression, Map<String, String> labels) {
    java.util.List<Token> tokens = tokenize(expression);
    if (tokens.isEmpty()) {
      return false;
    }

    return parseOrExpression(tokens, 0, labels).result;
  }

  /** Parse result container */
  private static class ParseResult {
    private final boolean result;
    private final int nextIndex;

    public ParseResult(boolean result, int nextIndex) {
      this.result = result;
      this.nextIndex = nextIndex;
    }
  }

  /** Parse OR expression (lowest precedence) */
  private static ParseResult parseOrExpression(
      java.util.List<Token> tokens, int startIndex, Map<String, String> labels) {
    ParseResult left = parseAndExpression(tokens, startIndex, labels);
    int currentIndex = left.nextIndex;

    while (currentIndex < tokens.size() && tokens.get(currentIndex).getType() == TokenType.OR) {
      currentIndex++; // skip OR token
      ParseResult right = parseAndExpression(tokens, currentIndex, labels);
      left = new ParseResult(left.result || right.result, right.nextIndex);
      currentIndex = right.nextIndex;
    }

    return left;
  }

  /** Parse AND expression (higher precedence) */
  private static ParseResult parseAndExpression(
      java.util.List<Token> tokens, int startIndex, Map<String, String> labels) {
    ParseResult left = parsePrimaryExpression(tokens, startIndex, labels);
    int currentIndex = left.nextIndex;

    while (currentIndex < tokens.size() && tokens.get(currentIndex).getType() == TokenType.AND) {
      currentIndex++; // skip AND token
      ParseResult right = parsePrimaryExpression(tokens, currentIndex, labels);
      left = new ParseResult(left.result && right.result, right.nextIndex);
      currentIndex = right.nextIndex;
    }

    return left;
  }

  /** Parse primary expression (condition or parenthesized expression) */
  private static ParseResult parsePrimaryExpression(
      java.util.List<Token> tokens, int startIndex, Map<String, String> labels) {
    if (startIndex >= tokens.size()) {
      throw new IllegalArgumentException("Unexpected end of expression");
    }

    Token token = tokens.get(startIndex);

    if (token.getType() == TokenType.LEFT_PAREN) {
      // Parse parenthesized expression
      ParseResult result = parseOrExpression(tokens, startIndex + 1, labels);
      int nextIndex = result.nextIndex;

      if (nextIndex >= tokens.size() || tokens.get(nextIndex).getType() != TokenType.RIGHT_PAREN) {
        throw new IllegalArgumentException("Missing closing parenthesis");
      }

      return new ParseResult(result.result, nextIndex + 1);
    } else if (token.getType() == TokenType.CONDITION) {
      // Parse condition
      boolean result = evaluateCondition(token.getValue(), labels);
      return new ParseResult(result, startIndex + 1);
    } else {
      throw new IllegalArgumentException("Unexpected token: " + token);
    }
  }

  /** Tokenize the expression string into tokens */
  private static java.util.List<Token> tokenize(String expression) {
    java.util.List<Token> tokens = new java.util.ArrayList<>();
    Matcher matcher = TOKEN_PATTERN.matcher(expression);

    while (matcher.find()) {
      String tokenValue = matcher.group(1).trim();

      if (tokenValue.equals("(")) {
        tokens.add(new Token(TokenType.LEFT_PAREN, tokenValue));
      } else if (tokenValue.equals(")")) {
        tokens.add(new Token(TokenType.RIGHT_PAREN, tokenValue));
      } else if (tokenValue.equalsIgnoreCase("AND")) {
        tokens.add(new Token(TokenType.AND, tokenValue));
      } else if (tokenValue.equalsIgnoreCase("OR")) {
        tokens.add(new Token(TokenType.OR, tokenValue));
      } else {
        // Must be a condition
        tokens.add(new Token(TokenType.CONDITION, tokenValue));
      }
    }

    return tokens;
  }

  /** Evaluate a single condition (e.g., "env='prod'", "level>3") */
  private static boolean evaluateCondition(String condition, Map<String, String> labels) {
    Matcher matcher = CONDITION_PATTERN.matcher(condition);

    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid condition format: " + condition);
    }

    String labelKey = matcher.group(1);
    String operator = matcher.group(2);
    String valueStr = matcher.group(3);

    // Get the actual value from labels
    String labelValue = labels.get(labelKey);

    // If the label doesn't exist in the security labels, the condition fails
    if (labelValue == null) {
      LOGGER.debug("Label key '{}' not found in security labels, condition fails", labelKey);
      return false;
    }

    // Parse the expected value - support single quotes only
    String expectedValue;
    boolean isNumeric = false;

    if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
      // String value (single quotes only)
      expectedValue = valueStr.substring(1, valueStr.length() - 1);
    } else {
      // Numeric value
      expectedValue = valueStr;
      isNumeric = true;
    }

    return evaluateComparison(labelValue, operator, expectedValue, isNumeric);
  }

  /** Evaluate comparison between label value and expected value */
  private static boolean evaluateComparison(
      String labelValue, String operator, String expectedValue, boolean isNumeric) {
    try {
      if (isNumeric) {
        // Numeric comparison - ensure both values are numeric
        try {
          double labelNum = Double.parseDouble(labelValue);
          double expectedNum = Double.parseDouble(expectedValue);

          switch (operator) {
            case "=":
              return Double.compare(labelNum, expectedNum) == 0;
            case "!=":
              return Double.compare(labelNum, expectedNum) != 0;
            case ">":
              return labelNum > expectedNum;
            case "<":
              return labelNum < expectedNum;
            case ">=":
              return labelNum >= expectedNum;
            case "<=":
              return labelNum <= expectedNum;
            default:
              throw new IllegalArgumentException("Unsupported numeric operator: " + operator);
          }
        } catch (NumberFormatException e) {
          // If label value cannot be parsed as numeric, but policy expects numeric
          LOGGER.error(
              "Type mismatch: Policy expects numeric value '{}' but database label '{}' is not numeric",
              expectedValue,
              labelValue);
          throw new IllegalArgumentException(
              String.format(
                  "Type mismatch: Cannot compare numeric policy value '%s' with non-numeric database label '%s'",
                  expectedValue, labelValue));
        }
      } else {
        // String comparison - ensure both values are strings
        // Check if label value looks like a number but policy expects string
        try {
          Double.parseDouble(labelValue);
          // If label value is numeric but policy expects string, this might be
          // intentional
          // (e.g., comparing "123" as string vs 123 as number)
          LOGGER.debug(
              "String comparison: Policy expects string value '{}' but database label '{}' is numeric",
              expectedValue,
              labelValue);
        } catch (NumberFormatException e) {
          // Label value is not numeric, which is fine for string comparison
        }

        switch (operator) {
          case "=":
            return labelValue.equals(expectedValue);
          case "!=":
            return !labelValue.equals(expectedValue);
          case ">":
          case "<":
          case ">=":
          case "<=":
            throw new IllegalArgumentException(
                "Relational operators (>, <, >=, <=) are not supported for string values");
          default:
            throw new IllegalArgumentException("Unsupported string operator: " + operator);
        }
      }
    } catch (NumberFormatException e) {
      LOGGER.error(
          "Failed to parse numeric value: labelValue='{}', expectedValue='{}'",
          labelValue,
          expectedValue);
      throw new IllegalArgumentException("Invalid numeric value in comparison", e);
    }
  }

  /**
   * Validate that a policy expression is syntactically correct
   *
   * @param policyExpression the policy expression to validate
   * @return true if valid, false otherwise
   */
  public static boolean isValidExpression(String policyExpression) {
    if (policyExpression == null || policyExpression.trim().isEmpty()) {
      return false;
    }

    try {
      // Try to tokenize and parse the expression
      java.util.List<Token> tokens = tokenize(policyExpression.trim());
      if (tokens.isEmpty()) {
        return false;
      }

      // Use empty labels for validation - we just want to check syntax
      Map<String, String> emptyLabels = new java.util.HashMap<>();
      parseOrExpression(tokens, 0, emptyLabels);
      return true;
    } catch (Exception e) {
      LOGGER.debug("Policy expression validation failed: {}", policyExpression, e);
      return false;
    }
  }
}
