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
import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainOutputFormat;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

  @Test
  public void testOffsetNode() {
    LimitNode child = new LimitNode(new PlanNodeId("c"), null, 0, Optional.empty());
    OffsetNode offsetNode = new OffsetNode(new PlanNodeId("10"), child, 5);

    String json = PlanGraphJsonPrinter.toPrettyJson(offsetNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertEquals("OffsetNode-10", root.get("name").getAsString());
    assertEquals("10", root.get("id").getAsString());
    assertTrue(root.has("properties"));
    assertEquals("5", root.getAsJsonObject("properties").get("Count").getAsString());
    assertTrue(root.has("children"));
  }

  @Test
  public void testProjectNode() {
    LimitNode child = new LimitNode(new PlanNodeId("c"), null, 0, Optional.empty());
    Symbol s1 = new Symbol("s1");
    Symbol s2 = new Symbol("s2");
    Assignments assignments =
        Assignments.of(s1, new SymbolReference("s1"), s2, new SymbolReference("s2"));

    ProjectNode projectNode = new ProjectNode(new PlanNodeId("20"), child, assignments);

    String json = PlanGraphJsonPrinter.toPrettyJson(projectNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertEquals("ProjectNode-20", root.get("name").getAsString());
    assertTrue(root.has("properties"));
    JsonObject props = root.getAsJsonObject("properties");
    assertTrue(props.has("OutputSymbols"));
    assertTrue(props.has("Expressions"));
  }

  @Test
  public void testFilterNodeWithPredicate() {
    LimitNode child = new LimitNode(new PlanNodeId("c"), null, 0, Optional.empty());
    FilterNode filterNode =
        new FilterNode(
            new PlanNodeId("30"),
            child,
            new ComparisonExpression(
                ComparisonExpression.Operator.GREATER_THAN,
                new SymbolReference("temperature"),
                new GenericLiteral("FLOAT", "25.0")));

    String json = PlanGraphJsonPrinter.toPrettyJson(filterNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertEquals("FilterNode-30", root.get("name").getAsString());
    assertTrue(root.has("properties"));
    assertTrue(root.getAsJsonObject("properties").has("Predicate"));
  }

  @Test
  public void testSortNode() {
    LimitNode child = new LimitNode(new PlanNodeId("c"), null, 0, Optional.empty());
    Symbol s1 = new Symbol("s1");
    OrderingScheme orderingScheme =
        new OrderingScheme(ImmutableList.of(s1), ImmutableMap.of(s1, SortOrder.ASC_NULLS_FIRST));

    SortNode sortNode = new SortNode(new PlanNodeId("40"), child, orderingScheme, false, false);

    String json = PlanGraphJsonPrinter.toPrettyJson(sortNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertEquals("SortNode-40", root.get("name").getAsString());
    assertTrue(root.has("properties"));
    assertTrue(root.getAsJsonObject("properties").has("OrderBy"));
  }

  @Test
  public void testMergeSortNode() {
    Symbol s1 = new Symbol("s1");
    OrderingScheme orderingScheme =
        new OrderingScheme(ImmutableList.of(s1), ImmutableMap.of(s1, SortOrder.DESC_NULLS_LAST));

    LimitNode child1 = new LimitNode(new PlanNodeId("c1"), null, 0, Optional.empty());
    LimitNode child2 = new LimitNode(new PlanNodeId("c2"), null, 0, Optional.empty());

    MergeSortNode mergeSortNode =
        new MergeSortNode(
            new PlanNodeId("50"),
            Arrays.asList(child1, child2),
            orderingScheme,
            ImmutableList.of(s1));

    String json = PlanGraphJsonPrinter.toPrettyJson(mergeSortNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertEquals("MergeSortNode-50", root.get("name").getAsString());
    assertTrue(root.has("properties"));
    assertTrue(root.getAsJsonObject("properties").has("OrderBy"));
    assertTrue(root.has("children"));
    assertEquals(2, root.getAsJsonArray("children").size());
  }

  @Test
  public void testExchangeNode() {
    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("60"));

    String json = PlanGraphJsonPrinter.toPrettyJson(exchangeNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertEquals("ExchangeNode-60", root.get("name").getAsString());
    assertEquals("60", root.get("id").getAsString());
    // ExchangeNode has no extra properties
    assertFalse(root.has("properties"));
  }

  @Test
  public void testExplainAnalyzeNode() {
    LimitNode child = new LimitNode(new PlanNodeId("c"), null, 0, Optional.empty());
    Symbol outputSymbol = new Symbol("explain_result");
    List<Symbol> childPermittedOutputs = Arrays.asList(new Symbol("s1"), new Symbol("s2"));

    ExplainAnalyzeNode explainNode =
        new ExplainAnalyzeNode(
            new PlanNodeId("70"),
            child,
            true,
            12345L,
            60000L,
            outputSymbol,
            childPermittedOutputs,
            ExplainOutputFormat.JSON);

    String json = PlanGraphJsonPrinter.toPrettyJson(explainNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertEquals("ExplainAnalyzeNode-70", root.get("name").getAsString());
    assertTrue(root.has("properties"));
    JsonArray permittedOutputs =
        root.getAsJsonObject("properties").getAsJsonArray("ChildPermittedOutputs");
    assertNotNull(permittedOutputs);
    assertEquals(2, permittedOutputs.size());
  }

  @Test
  public void testJoinNode() {
    ExchangeNode leftChild = new ExchangeNode(new PlanNodeId("l"));
    leftChild.setOutputSymbols(ImmutableList.of(new Symbol("left_s1")));
    ExchangeNode rightChild = new ExchangeNode(new PlanNodeId("r"));
    rightChild.setOutputSymbols(ImmutableList.of(new Symbol("right_s1")));
    Symbol leftSymbol = new Symbol("left_s1");
    Symbol rightSymbol = new Symbol("right_s1");

    JoinNode joinNode =
        new JoinNode(
            new PlanNodeId("80"),
            JoinNode.JoinType.INNER,
            leftChild,
            rightChild,
            ImmutableList.of(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol)),
            Optional.empty(),
            ImmutableList.of(leftSymbol),
            ImmutableList.of(rightSymbol),
            Optional.empty(),
            Optional.empty());

    String json = PlanGraphJsonPrinter.toPrettyJson(joinNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertEquals("JoinNode-80", root.get("name").getAsString());
    assertTrue(root.has("properties"));
    JsonObject props = root.getAsJsonObject("properties");
    assertEquals("INNER", props.get("JoinType").getAsString());
    assertTrue(props.has("Criteria"));
    assertTrue(props.has("OutputSymbols"));
    assertTrue(root.has("children"));
    assertEquals(2, root.getAsJsonArray("children").size());
  }

  @Test
  public void testUnionNode() {
    ExchangeNode child1 = new ExchangeNode(new PlanNodeId("c1"));
    child1.setOutputSymbols(ImmutableList.of(new Symbol("input_s1_a")));
    ExchangeNode child2 = new ExchangeNode(new PlanNodeId("c2"));
    child2.setOutputSymbols(ImmutableList.of(new Symbol("input_s1_b")));
    Symbol out = new Symbol("output_s1");

    Symbol in1 = new Symbol("input_s1_a");
    Symbol in2 = new Symbol("input_s1_b");

    UnionNode unionNode =
        new UnionNode(
            new PlanNodeId("90"),
            Arrays.asList(child1, child2),
            ImmutableListMultimap.of(out, in1, out, in2),
            ImmutableList.of(out));

    String json = PlanGraphJsonPrinter.toPrettyJson(unionNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertEquals("UnionNode-90", root.get("name").getAsString());
    assertTrue(root.has("properties"));
    assertTrue(root.getAsJsonObject("properties").has("OutputSymbols"));
    assertTrue(root.has("children"));
    assertEquals(2, root.getAsJsonArray("children").size());
  }

  @Test
  public void testNodeWithNoChildren() {
    ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId("100"));

    String json = PlanGraphJsonPrinter.toPrettyJson(exchangeNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    // ExchangeNode has no children added
    assertFalse(root.has("children"));
  }

  @Test
  public void testOutputNodeProperties() {
    LimitNode child = new LimitNode(new PlanNodeId("c"), null, 0, Optional.empty());
    List<Symbol> outputSymbols =
        Arrays.asList(new Symbol("time"), new Symbol("device"), new Symbol("temperature"));
    OutputNode outputNode =
        new OutputNode(
            new PlanNodeId("110"),
            child,
            Arrays.asList("time", "device", "temperature"),
            outputSymbols);

    String json = PlanGraphJsonPrinter.toPrettyJson(outputNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    JsonObject props = root.getAsJsonObject("properties");
    assertNotNull(props);
    JsonArray outputColumns = props.getAsJsonArray("OutputColumns");
    assertEquals(3, outputColumns.size());
    assertEquals("time", outputColumns.get(0).getAsString());
    assertEquals("device", outputColumns.get(1).getAsString());
    assertEquals("temperature", outputColumns.get(2).getAsString());

    JsonArray outputSymbolsArray = props.getAsJsonArray("OutputSymbols");
    assertEquals(3, outputSymbolsArray.size());
  }

  @Test
  public void testDeepPlanHierarchy() {
    // Build a 4-level deep plan: Output -> Sort -> Filter -> Limit
    LimitNode leaf = new LimitNode(new PlanNodeId("leaf"), null, 100, Optional.empty());

    FilterNode filterNode =
        new FilterNode(
            new PlanNodeId("filter"),
            leaf,
            new ComparisonExpression(
                ComparisonExpression.Operator.LESS_THAN,
                new SymbolReference("v"),
                new GenericLiteral("INT32", "50")));

    Symbol s1 = new Symbol("v");
    OrderingScheme orderingScheme =
        new OrderingScheme(ImmutableList.of(s1), ImmutableMap.of(s1, SortOrder.ASC_NULLS_LAST));
    SortNode sortNode =
        new SortNode(new PlanNodeId("sort"), filterNode, orderingScheme, false, false);

    OutputNode outputNode =
        new OutputNode(
            new PlanNodeId("output"),
            sortNode,
            Collections.singletonList("v"),
            ImmutableList.of(s1));

    String json = PlanGraphJsonPrinter.toPrettyJson(outputNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    // Verify 4 levels
    assertEquals("OutputNode-output", root.get("name").getAsString());
    JsonObject sort = root.getAsJsonArray("children").get(0).getAsJsonObject();
    assertEquals("SortNode-sort", sort.get("name").getAsString());
    JsonObject filter = sort.getAsJsonArray("children").get(0).getAsJsonObject();
    assertEquals("FilterNode-filter", filter.get("name").getAsString());
    JsonObject limit = filter.getAsJsonArray("children").get(0).getAsJsonObject();
    assertEquals("LimitNode-leaf", limit.get("name").getAsString());
    assertEquals("100", limit.getAsJsonObject("properties").get("Count").getAsString());
  }

  @Test
  public void testExplainAnalyzeNodeDefaultFormat() {
    LimitNode child = new LimitNode(new PlanNodeId("c"), null, 0, Optional.empty());
    Symbol outputSymbol = new Symbol("result");
    List<Symbol> childPermittedOutputs = ImmutableList.of(new Symbol("s1"));

    // Use constructor with default TEXT format
    ExplainAnalyzeNode explainNode =
        new ExplainAnalyzeNode(
            new PlanNodeId("ea"), child, false, 1L, 30000L, outputSymbol, childPermittedOutputs);

    String json = PlanGraphJsonPrinter.toPrettyJson(explainNode);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();

    assertEquals("ExplainAnalyzeNode-ea", root.get("name").getAsString());
    assertTrue(root.has("properties"));
  }
}
