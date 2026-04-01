/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphJsonPrinter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PlanGraphJsonPrinterTest {

  @Test
  public void testSimplePlanToJson() {
    // Build a simple plan: Output -> Limit -> Filter
    LimitNode placeholder = new LimitNode(new PlanNodeId("placeholder"), null, 0, Optional.empty());
    FilterNode filterNode =
        new FilterNode(
            new PlanNodeId("3"),
            placeholder,
            new ComparisonExpression(
                ComparisonExpression.Operator.EQUAL,
                new SymbolReference("s1"),
                new GenericLiteral("INT32", "1")));

    LimitNode limitNode = new LimitNode(new PlanNodeId("2"), filterNode, 10, Optional.empty());

    List<Symbol> outputSymbols = Arrays.asList(new Symbol("s1"), new Symbol("s2"));
    OutputNode outputNode =
        new OutputNode(new PlanNodeId("1"), limitNode, Arrays.asList("s1", "s2"), outputSymbols);

    String json = PlanGraphJsonPrinter.toPrettyJson(outputNode);

    assertNotNull(json);
    assertTrue(json.length() > 0);

    // Parse the JSON and verify structure
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();
    assertEquals("OutputNode-1", root.get("name").getAsString());
    assertEquals("1", root.get("id").getAsString());
    assertTrue(root.has("properties"));
    assertTrue(root.has("children"));

    // Verify child (LimitNode)
    JsonObject limitJson = root.getAsJsonArray("children").get(0).getAsJsonObject();
    assertEquals("LimitNode-2", limitJson.get("name").getAsString());

    // Verify grandchild (FilterNode)
    JsonObject filterJson = limitJson.getAsJsonArray("children").get(0).getAsJsonObject();
    assertEquals("FilterNode-3", filterJson.get("name").getAsString());
  }

  @Test
  public void testGetJsonLinesReturnsSingleElement() {
    LimitNode limitNode =
        new LimitNode(
            new PlanNodeId("1"),
            new LimitNode(new PlanNodeId("placeholder"), null, 0, Optional.empty()),
            5,
            Optional.empty());

    List<String> lines = PlanGraphJsonPrinter.getJsonLines(limitNode);
    assertEquals(1, lines.size());

    // The single element should be valid JSON
    JsonObject root = JsonParser.parseString(lines.get(0)).getAsJsonObject();
    assertNotNull(root.get("name"));
  }

  @Test
  public void testJsonIsValidFormat() {
    LimitNode limitNode =
        new LimitNode(
            new PlanNodeId("1"),
            new LimitNode(new PlanNodeId("placeholder"), null, 0, Optional.empty()),
            42,
            Optional.empty());

    String json = PlanGraphJsonPrinter.toPrettyJson(limitNode);

    // Should parse without errors
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();
    assertEquals("LimitNode-1", root.get("name").getAsString());
    assertTrue(root.has("properties"));
    assertEquals("42", root.getAsJsonObject("properties").get("Count").getAsString());
  }
}
