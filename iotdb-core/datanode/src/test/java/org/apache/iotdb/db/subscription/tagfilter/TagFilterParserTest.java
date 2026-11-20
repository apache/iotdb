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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TagFilterParserTest {

  private static final TagFilterParser PARSER = new TagFilterParser();

  @Test
  public void testComparisonIsCaseSensitive() throws Exception {
    Assert.assertTrue(evaluate("region = \"north\"", Map.of("region", "north")));
    Assert.assertFalse(evaluate("region = \"north\"", Map.of("region", "North")));
    Assert.assertTrue(evaluate("region != \"north\"", Map.of("region", "North")));
  }

  @Test
  public void testQuotesInTagValues() throws Exception {
    Assert.assertTrue(evaluate("region = \"O'Reilly\"", Map.of("region", "O'Reilly")));
    Assert.assertFalse(evaluate("region = \"O'Reilly\"", Map.of("region", "O''Reilly")));
    Assert.assertTrue(evaluate("region = \"a''b\"", Map.of("region", "a''b")));
    Assert.assertTrue(evaluate("region = \"a\"\"\"\"b\"", Map.of("region", "a\"\"b")));
    Assert.assertFalse(evaluate("region = \"a\"\"\"\"b\"", Map.of("region", "a\"b")));
    Assert.assertTrue(evaluate("region = \"\"\"quoted\"\"\"", Map.of("region", "\"quoted\"")));
    Assert.assertTrue(
        evaluate(
            "region = \"O'Reilly \"\"A\"\"\"\"B\"\"\"", Map.of("region", "O'Reilly \"A\"\"B\"")));
  }

  @Test
  public void testQuotesInInLikeAndRegexp() throws Exception {
    final String inFilter = "region IN (\"O'Reilly\", \"a\"\"\"\"b\")";
    Assert.assertTrue(evaluate(inFilter, Map.of("region", "O'Reilly")));
    Assert.assertTrue(evaluate(inFilter, Map.of("region", "a\"\"b")));
    Assert.assertFalse(evaluate(inFilter, Map.of("region", "a\"b")));
    Assert.assertTrue(
        evaluate("region LIKE \"O'Reilly \"\"%\"\"\"", Map.of("region", "O'Reilly \"north\"")));
    Assert.assertTrue(
        evaluate(
            "region REGEXP \"^O'Reilly \"\"[A-Z]+\"\"$\"", Map.of("region", "O'Reilly \"NORTH\"")));
  }

  @Test
  public void testInLikeRegexpAndBooleanOperators() throws Exception {
    Assert.assertTrue(
        evaluate(
            "region IN (\"north\", \"west\") AND device LIKE \"d_%\"",
            Map.of("region", "north", "device", "d_1")));
    Assert.assertTrue(
        evaluate(
            "region NOT REGEXP \"south|east\" OR NOT device = \"d2\"",
            Map.of("region", "north", "device", "d2")));
    Assert.assertTrue(evaluate("device LIKE \"d!_%\" ESCAPE \"!\"", Map.of("device", "d_sensor")));
    Assert.assertTrue(evaluate("device LIKE \"d_%\"", Map.of("device", "d\n1")));
  }

  @Test
  public void testNullUsesThreeValuedLogic() throws Exception {
    final Map<String, String> values = new HashMap<>();
    values.put("region", null);

    Assert.assertTrue(evaluate("region IS NULL", values));
    Assert.assertFalse(evaluate("region IS NOT NULL", values));
    Assert.assertFalse(evaluate("region = \"north\"", values));
    Assert.assertFalse(evaluate("NOT region = \"north\"", values));
    Assert.assertTrue(evaluate("region = \"north\" OR true", values));
    Assert.assertFalse(evaluate("region = \"north\" AND true", values));
    Assert.assertTrue(evaluate("NOT (region = \"north\" AND false)", values));
    Assert.assertFalse(evaluate("NOT (region = \"north\" OR true)", values));
    Assert.assertTrue(evaluate("NOT (false AND region = \"north\")", values));
    Assert.assertFalse(evaluate("NOT (true OR region = \"north\")", values));
  }

  @Test
  public void testFlatLogicalChainsDoNotExceedMaxAstDepth() throws Exception {
    final String orChain =
        IntStream.range(0, 40)
            .mapToObj(i -> "region = \"r" + i + "\"")
            .collect(Collectors.joining(" OR "));
    final Expression orExpression = PARSER.parseAndValidate(orChain);
    Assert.assertTrue(orExpression instanceof LogicalExpression);
    Assert.assertEquals(
        LogicalExpression.Operator.OR, ((LogicalExpression) orExpression).getOperator());
    Assert.assertEquals(40, ((LogicalExpression) orExpression).getTerms().size());
    Assert.assertTrue(TagFilterEvaluator.evaluate(orExpression, identifier -> "r39"));
    Assert.assertFalse(TagFilterEvaluator.evaluate(orExpression, identifier -> "r40"));

    final String andChain =
        IntStream.range(0, 40)
            .mapToObj(i -> "region <> \"bad" + i + "\"")
            .collect(Collectors.joining(" AND "));
    final Expression andExpression = PARSER.parseAndValidate(andChain);
    Assert.assertTrue(andExpression instanceof LogicalExpression);
    Assert.assertEquals(
        LogicalExpression.Operator.AND, ((LogicalExpression) andExpression).getOperator());
    Assert.assertEquals(40, ((LogicalExpression) andExpression).getTerms().size());
    Assert.assertTrue(TagFilterEvaluator.evaluate(andExpression, identifier -> "good"));
    Assert.assertFalse(TagFilterEvaluator.evaluate(andExpression, identifier -> "bad39"));

    // Precedence between OR and AND must still be preserved when flattening.
    Assert.assertFalse(
        evaluate(
            "(region = \"north\" OR region = \"south\") AND device = \"d2\"",
            Map.of("region", "north", "device", "d1")));

    // Genuine nesting deeper than MAX_AST_DEPTH (32) must still be rejected.
    final StringBuilder deeplyNested = new StringBuilder();
    for (int i = 0; i < 33; i++) {
      deeplyNested.append("NOT (");
    }
    deeplyNested.append("region = \"north\"");
    for (int i = 0; i < 33; i++) {
      deeplyNested.append(")");
    }
    assertRejected(deeplyNested.toString(), "AST depth exceeds maximum");
  }

  @Test
  public void testRejectInvalidExpressions() {
    assertRejected("", "tag-filter should not be empty");
    assertRejected("region > \"north\"", "unsupported comparison operator");
    assertRejected("lower(region) = \"north\"", "expected column predicate operator");
    assertRejected("region = other_tag", "expected string literal");
    assertRejected("region REGEXP \"[\"", "illegal REGEXP pattern");
  }

  private static boolean evaluate(final String filter, final Map<String, String> values)
      throws SubscriptionException {
    final Expression expression = PARSER.parseAndValidate(filter);
    return TagFilterEvaluator.evaluate(expression, identifier -> values.get(identifier.getValue()));
  }

  private static void assertRejected(final String filter, final String expectedMessagePart) {
    try {
      PARSER.parseAndValidate(filter);
      Assert.fail("Expected tag-filter to be rejected: " + filter);
    } catch (final SubscriptionException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessagePart));
    }
  }
}
