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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.GapFillNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertAnalyzeSemanticException;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the GROUP BY ALL syntax. Verifies that the analyzer correctly infers grouping columns
 * from the SELECT clause and that integration with date_bin_gapfill is preserved.
 */
public class GroupByAllTest {

  // ---- basic column inference ----

  @Test
  public void groupByAllSingleColumnTest() {
    // GROUP BY ALL should infer s1 as the grouping key (the only non-aggregate expression)
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan plan =
        planTester.createPlan("SELECT s1, count(s2) FROM table1 GROUP BY ALL");
    assertPlan(
        plan,
        output(
            aggregation(
                singleGroupingSet("s1"),
                ImmutableMap.of(
                    Optional.empty(),
                    aggregationFunction("count", ImmutableList.of("s2"))),
                ImmutableList.of(),
                Optional.empty(),
                SINGLE,
                tableScan(
                    "testdb.table1",
                    ImmutableList.of("s1", "s2"),
                    ImmutableSet.of("s1", "s2")))));
  }

  @Test
  public void groupByAllMultipleColumnsTest() {
    // GROUP BY ALL should infer tag1, tag2, tag3 as grouping keys.
    // The optimizer pushes aggregation into the table scan, producing AggregationTableScanNode.
    // Verify by walking the plan tree.
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan plan =
        planTester.createPlan(
            "SELECT tag1, tag2, tag3, count(s2) FROM table1 GROUP BY ALL");

    PlanNode root = plan.getRootNode();
    AggregationTableScanNode aggScan = findNode(root, AggregationTableScanNode.class);
    assertTrue(
        "Expected AggregationTableScanNode with grouping keys [tag1, tag2, tag3]",
        aggScan != null
            && aggScan.getGroupingKeys().stream()
                .map(s -> s.getName())
                .collect(java.util.stream.Collectors.toSet())
                .containsAll(ImmutableSet.of("tag1", "tag2", "tag3")));
  }

  @Test
  public void groupByAllEquivalenceTest() {
    // GROUP BY ALL and explicit GROUP BY should produce the same plan structure
    PlanTester tester1 = new PlanTester();
    PlanTester tester2 = new PlanTester();
    LogicalQueryPlan allPlan =
        tester1.createPlan("SELECT s1, count(s2) FROM table1 GROUP BY ALL");
    LogicalQueryPlan explicitPlan =
        tester2.createPlan("SELECT s1, count(s2) FROM table1 GROUP BY s1");

    // Both plans should match the same pattern
    assertPlan(
        allPlan,
        output(
            aggregation(
                singleGroupingSet("s1"),
                ImmutableMap.of(
                    Optional.empty(),
                    aggregationFunction("count", ImmutableList.of("s2"))),
                ImmutableList.of(),
                Optional.empty(),
                SINGLE,
                tableScan(
                    "testdb.table1",
                    ImmutableList.of("s1", "s2"),
                    ImmutableSet.of("s1", "s2")))));
    assertPlan(
        explicitPlan,
        output(
            aggregation(
                singleGroupingSet("s1"),
                ImmutableMap.of(
                    Optional.empty(),
                    aggregationFunction("count", ImmutableList.of("s2"))),
                ImmutableList.of(),
                Optional.empty(),
                SINGLE,
                tableScan(
                    "testdb.table1",
                    ImmutableList.of("s1", "s2"),
                    ImmutableSet.of("s1", "s2")))));
  }

  // ---- complex expression in SELECT ----

  @Test
  public void groupByAllWithExpressionTest() {
    // GROUP BY ALL with a non-aggregate expression (s1 + 1) should infer it as a grouping key
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan plan =
        planTester.createPlan("SELECT s1 + 1, count(s2) FROM table1 GROUP BY ALL");

    // Verify there is an AggregationNode with a non-empty grouping set
    PlanNode root = plan.getRootNode();
    AggregationNode aggNode = findNode(root, AggregationNode.class);
    assertTrue(
        "Expected AggregationNode with non-empty grouping keys for expression grouping",
        aggNode != null && !aggNode.getGroupingKeys().isEmpty());
  }

  // ---- global aggregation (all SELECT items are aggregates) ----

  @Test
  public void groupByAllGlobalAggregationTest() {
    // When all SELECT expressions are aggregates, GROUP BY ALL is equivalent to no GROUP BY
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan plan =
        planTester.createPlan("SELECT count(s1), sum(s2) FROM table1 GROUP BY ALL");

    // Verify global aggregation: grouping keys should be empty
    PlanNode root = plan.getRootNode();
    // May be AggregationNode or AggregationTableScanNode
    AggregationNode aggNode = findNode(root, AggregationNode.class);
    if (aggNode != null) {
      assertTrue(
          "Expected empty grouping keys for global aggregation",
          aggNode.getGroupingKeys().isEmpty());
    } else {
      AggregationTableScanNode aggScan = findNode(root, AggregationTableScanNode.class);
      assertTrue(
          "Expected AggregationTableScanNode with empty grouping keys",
          aggScan != null && aggScan.getGroupingKeys().isEmpty());
    }
  }

  // ---- date_bin_gapfill integration ----

  @Test
  public void groupByAllWithGapFillTest() {
    // GROUP BY ALL with date_bin_gapfill should produce a GapFillNode in the plan
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan plan =
        planTester.createPlan(
            "SELECT date_bin_gapfill(1h, time), tag1, avg(s1) "
                + "FROM table1 "
                + "WHERE time >= 1 AND time <= 10 "
                + "GROUP BY ALL");

    // The plan should contain a GapFillNode
    PlanNode root = plan.getRootNode();
    GapFillNode gapFillNode = findNode(root, GapFillNode.class);
    assertTrue("Expected GapFillNode in plan for date_bin_gapfill + GROUP BY ALL", gapFillNode != null);
  }

  @Test
  public void groupByAllMultipleGapFillTest() {
    // Two date_bin_gapfill calls should be rejected even with GROUP BY ALL
    assertAnalyzeSemanticException(
        "SELECT date_bin_gapfill(1h, time), date_bin_gapfill(2h, time), avg(s1) "
            + "FROM table1 WHERE time >= 1 AND time <= 10 GROUP BY ALL",
        "multiple date_bin_gapfill calls not allowed");
  }

  // ---- backward compatibility ----

  @Test
  public void groupByAllQuantifierBackwardCompatibilityTest() {
    // GROUP BY ALL a, b (ALL as set-quantifier, not GROUP BY ALL feature)
    // must still parse and plan correctly — the ALL keyword followed by explicit columns
    // should be treated as the set-quantifier form, not the new GROUP BY ALL syntax.
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan plan =
        planTester.createPlan("SELECT count(s2) FROM table1 GROUP BY ALL s1");
    PlanNode root = plan.getRootNode();
    // The plan should have an aggregation with s1 as a grouping key
    AggregationNode aggNode = findNode(root, AggregationNode.class);
    AggregationTableScanNode aggScan = findNode(root, AggregationTableScanNode.class);
    boolean hasS1Grouping;
    if (aggNode != null) {
      hasS1Grouping =
          aggNode.getGroupingKeys().stream().anyMatch(s -> s.getName().equals("s1"));
    } else {
      hasS1Grouping =
          aggScan != null
              && aggScan.getGroupingKeys().stream().anyMatch(s -> s.getName().equals("s1"));
    }
    assertTrue(
        "GROUP BY ALL s1 should parse as ALL-quantifier with explicit grouping key s1",
        hasS1Grouping);
  }

  // ---- helper ----

  @SuppressWarnings("unchecked")
  private static <T extends PlanNode> T findNode(PlanNode root, Class<T> nodeClass) {
    if (nodeClass.isInstance(root)) {
      return (T) root;
    }
    for (PlanNode child : root.getChildren()) {
      T result = findNode(child, nodeClass);
      if (result != null) {
        return result;
      }
    }
    return null;
  }
}
