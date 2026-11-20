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
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  public void testRejectInvalidExpressions() {
    assertRejected("", "tag-filter should not be empty");
    assertRejected("region > \"north\"", "unsupported comparison operator");
    assertRejected("lower(region) = \"north\"", "expected column predicate operator");
    assertRejected("region = other_tag", "expected string literal");
    assertRejected("region REGEXP \"[\"", "illegal REGEXP pattern");
  }

  @Test
  public void testValidateAndCompileBuildsInPredicateHashSets() throws Exception {
    final int maxInLiterals = TagFilterValidator.MAX_IN_VALUES;
    final List<String> literals = new ArrayList<>(maxInLiterals);
    for (int i = 0; i < maxInLiterals; i++) {
      literals.add(String.format("\"d_%03d\"", i));
    }
    final Expression maxInExpr =
        PARSER.parseAndValidate("device IN (" + String.join(", ", literals) + ")");
    final TagFilterValidator.ValidationResult maxValidation =
        TagFilterValidator.validateAndCompile(maxInExpr);

    Assert.assertEquals(1, maxValidation.getCompiledInSets().size());
    final Set<String> compiledMaxSet = maxValidation.getCompiledInSets().values().iterator().next();
    Assert.assertEquals(maxInLiterals, compiledMaxSet.size());
    Assert.assertTrue(compiledMaxSet.contains("d_000"));
    Assert.assertTrue(compiledMaxSet.contains("d_127"));
    Assert.assertTrue(compiledMaxSet.contains("d_255"));

    Assert.assertTrue(
        TagFilterEvaluator.evaluate(
            maxInExpr,
            id -> "d_127",
            maxValidation.getCompiledPatterns(),
            maxValidation.getCompiledInSets()));
    Assert.assertFalse(
        TagFilterEvaluator.evaluate(
            maxInExpr,
            id -> "d_256",
            maxValidation.getCompiledPatterns(),
            maxValidation.getCompiledInSets()));
    Assert.assertFalse(
        TagFilterEvaluator.evaluate(
            maxInExpr,
            id -> null,
            maxValidation.getCompiledPatterns(),
            maxValidation.getCompiledInSets()));

    literals.add("\"d_256\"");
    assertRejected(
        "device IN (" + String.join(", ", literals) + ")",
        "IN list exceeds maximum of " + TagFilterValidator.MAX_IN_VALUES);

    final Expression compositeExpr =
        PARSER.parseAndValidate(
            "device IN (\"d_0\", \"d_0\", \"O'Reilly\") OR NOT (region IN (\"north\", \"south\"))");
    final TagFilterValidator.ValidationResult compositeValidation =
        TagFilterValidator.validateAndCompile(compositeExpr);
    Assert.assertEquals(2, compositeValidation.getCompiledInSets().size());

    final Map<String, String> matchSecond = new HashMap<>();
    matchSecond.put("device", "other");
    matchSecond.put("region", "west");
    Assert.assertTrue(
        TagFilterEvaluator.evaluate(
            compositeExpr,
            id -> matchSecond.get(id.getValue()),
            compositeValidation.getCompiledPatterns(),
            compositeValidation.getCompiledInSets()));

    final Map<String, String> nullRegion = new HashMap<>();
    nullRegion.put("device", "other");
    nullRegion.put("region", null);
    Assert.assertFalse(
        TagFilterEvaluator.evaluate(
            compositeExpr,
            id -> nullRegion.get(id.getValue()),
            compositeValidation.getCompiledPatterns(),
            compositeValidation.getCompiledInSets()));
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
