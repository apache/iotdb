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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.subscription.columnfilter.ColumnMetadata;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.junit.Assert;
import org.junit.Test;

public class ColumnFilterParserTest {

  private static final ColumnFilterParser PARSER = new ColumnFilterParser();
  private static final ColumnMetadata TEMPERATURE =
      new ColumnMetadata("db1", "weather", "temperature", "DOUBLE", "FIELD");
  private static final ColumnMetadata TAG =
      new ColumnMetadata("db1", "weather", "city", "STRING", "TAG");

  @Test
  public void testBooleanLiteral() throws Exception {
    Assert.assertTrue(evaluate("true", TEMPERATURE));
    Assert.assertFalse(evaluate("false", TEMPERATURE));
  }

  @Test
  public void testComparisonAndInAreCaseInsensitive() throws Exception {
    Assert.assertTrue(
        evaluate("CATEGORY = \"field\" AND datatype IN (\"int32\", \"double\")", TEMPERATURE));
    Assert.assertTrue(evaluate("\"column_name\" != \"humidity\"", TEMPERATURE));
    Assert.assertTrue(evaluate("column_name <> \"humidity\"", TEMPERATURE));
    Assert.assertFalse(evaluate("table_name NOT IN (\"weather\", \"status\")", TEMPERATURE));
  }

  @Test
  public void testLikeRegexpAndNot() throws Exception {
    Assert.assertTrue(evaluate("column_name LIKE \"temp%\"", TEMPERATURE));
    Assert.assertTrue(evaluate("column_name REGEXP \"temp.*\"", TEMPERATURE));
    Assert.assertTrue(evaluate("NOT category = \"TAG\"", TEMPERATURE));
    Assert.assertTrue(evaluate("column_name NOT LIKE \"hum%\"", TEMPERATURE));
    Assert.assertTrue(evaluate("column_name NOT REGEXP \"hum.*\"", TEMPERATURE));
  }

  @Test
  public void testEscapedStringLiteralAndLikeEscape() throws Exception {
    final ColumnMetadata quotedColumn =
        new ColumnMetadata("db1", "weather", "temperature\"sensor", "DOUBLE", "FIELD");
    final ColumnMetadata escapedLikeColumn =
        new ColumnMetadata("db1", "weather", "temp_sensor", "DOUBLE", "FIELD");

    Assert.assertTrue(evaluate("column_name = \"temperature\"\"sensor\"", quotedColumn));
    Assert.assertTrue(evaluate("column_name LIKE \"temp!_%\" ESCAPE \"!\"", escapedLikeColumn));
  }

  @Test
  public void testLogicalPrecedence() throws Exception {
    Assert.assertTrue(
        evaluate(
            "category = \"TAG\" OR category = \"FIELD\" AND datatype = \"DOUBLE\"", TEMPERATURE));
    Assert.assertFalse(
        evaluate(
            "(category = \"TAG\" OR category = \"FIELD\") AND datatype = \"STRING\"", TEMPERATURE));
  }

  @Test
  public void testIsNullAndIsNotNull() throws Exception {
    Assert.assertFalse(evaluate("column_name IS NULL", TAG));
    Assert.assertTrue(evaluate("column_name IS NOT NULL", TAG));
  }

  @Test
  public void testSpecialCharactersInsideStringLiterals() throws Exception {
    final ColumnMetadata plusColumn =
        new ColumnMetadata("db1", "weather", "a+b", "DOUBLE", "FIELD");
    final ColumnMetadata greaterColumn =
        new ColumnMetadata("db1", "weather", "a>b", "DOUBLE", "FIELD");
    final ColumnMetadata regexpColumn =
        new ColumnMetadata("db1", "weather", "s9", "DOUBLE", "FIELD");

    Assert.assertTrue(evaluate("column_name = \"a+b\"", plusColumn));
    Assert.assertTrue(evaluate("column_name LIKE \"a>%\"", greaterColumn));
    Assert.assertTrue(evaluate("column_name REGEXP \"s[0-9]+\"", regexpColumn));
  }

  @Test
  public void testRejectInvalidExpressions() {
    assertRejected("temperature > 10", "unsupported comparison operator");
    assertRejected("column_name = \"s1\" + \"s2\"", "unexpected character '+'");
    assertRejected("lower(column_name) = \"s1\"", "expected column predicate operator");
    assertRejected("column_name = table_name", "expected string literal");
    assertRejected("owner = \"alice\"", "unsupported column metadata field");
    assertRejected("column_name", "expected column predicate operator");
    assertRejected("\"temperature\"", "expected column predicate operator");
    assertRejected("column_name REGEXP \"[\"", "illegal REGEXP pattern");
    assertRejected(
        "column_name LIKE \"temp!\" ESCAPE \"!\"", "LIKE pattern ends with escape character");
  }

  private static boolean evaluate(final String expression, final ColumnMetadata metadata)
      throws SubscriptionException {
    final Expression parsed = PARSER.parseAndValidate(expression);
    return ColumnFilterEvaluator.evaluate(parsed, metadata);
  }

  private static void assertRejected(final String expression, final String expectedMessagePart) {
    try {
      PARSER.parseAndValidate(expression);
      Assert.fail("Expected column-filter to be rejected: " + expression);
    } catch (final SubscriptionException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessagePart));
    }
  }
}
