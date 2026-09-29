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

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder.DESC_NULLS_LAST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrderingPropertyPropagationTest {

  private static final String TAGS = "tag1, tag2, tag3";

  @Test
  public void testNonPrefixPartitionAggregationUsesHashGrouping() {
    String[] queries = {
      "SELECT tag2, max(tag1) FROM CAPACITY(DATA => table1 PARTITION BY (tag1, tag2) ORDER BY time, SIZE => 3, SLIDE => 1) GROUP BY tag2",
      "SELECT tag2, count(tag1) FROM CAPACITY(DATA => table1 PARTITION BY (tag1, tag2) ORDER BY time, SIZE => 3, SLIDE => 1) GROUP BY tag2",
      "SELECT tag1, max(tag2) FROM CAPACITY(DATA => table1 PARTITION BY (tag2, tag1) ORDER BY time, SIZE => 3, SLIDE => 1) GROUP BY tag1",
      "SELECT tag2, tag3, max(tag1) FROM CAPACITY(DATA => table1 PARTITION BY (tag1, tag2, tag3) ORDER BY time, SIZE => 3, SLIDE => 1) GROUP BY tag2, tag3",
      "SELECT tag2, max(tag1) FROM SESSION(DATA => table1 PARTITION BY (tag1, tag2) ORDER BY time, GAP => 10m) GROUP BY tag2"
    };
    for (String sql : queries) {
      PlanTester tester = new PlanTester();
      PlanNode logical = tester.createPlan(sql).getRootNode();
      List<AggregationNode> aggregations = findNodes(logical, AggregationNode.class);
      assertFalse(sql, aggregations.isEmpty());
      assertTrue(
          sql, aggregations.stream().allMatch(node -> node.getPreGroupedSymbols().isEmpty()));
      assertFalse(sql, findNodes(tester, AggregationNode.class).isEmpty());
    }
  }

  @Test
  public void testPartitionPrefixKeepsStreamingAggregation() {
    PlanTester tester = new PlanTester();
    String sql =
        "SELECT tag1, max(tag2) FROM CAPACITY(DATA => table1 PARTITION BY (tag1, tag2) ORDER BY time, SIZE => 3, SLIDE => 1) GROUP BY tag1";
    PlanNode logical = tester.createPlan(sql).getRootNode();
    assertTrue(
        findNodes(logical, AggregationNode.class).stream()
            .anyMatch(node -> node.getPreGroupedSymbols().equals(List.of(new Symbol("tag1")))));
    assertTrue(
        findNodes(tester, AggregationNode.class).stream()
            .anyMatch(node -> node.getPreGroupedSymbols().equals(List.of(new Symbol("tag1")))));
  }

  @Test
  public void testCommonWindowTimeOrderNeedsNoAdditionalSort() {
    for (String function : List.of("lag(s1)", "lead(s1)", "count(s1)")) {
      for (String tagOrder : List.of(TAGS, "tag1 DESC, tag2 DESC, tag3 DESC")) {
        for (String limit : List.of("", " LIMIT 5")) {
          PlanTester tester =
              plan(
                  "SELECT *, "
                      + function
                      + " OVER (PARTITION BY "
                      + TAGS
                      + " ORDER BY time) AS w FROM table1 ORDER BY "
                      + tagOrder
                      + ", time"
                      + limit);
          assertFalse(findNodes(tester, WindowNode.class).isEmpty());
          assertTrue(findNodes(tester, SortNode.class).isEmpty());
        }
      }
    }
  }

  @Test
  public void testWindowFieldOrderKeepsStreamingPrefix() {
    for (String partition : List.of(TAGS, "tag1")) {
      PlanTester tester =
          plan(
              "SELECT *, count(s1) OVER (PARTITION BY "
                  + partition
                  + " ORDER BY time) AS c FROM table1 ORDER BY "
                  + partition
                  + ", s1");
      assertStreamingSortAbove(tester, WindowNode.class, partition.split(", ").length - 1);
    }
  }

  @Test
  public void testWindowOppositeTimeDirectionRetainsOnlyStreamingSort() {
    PlanTester tester =
        plan(
            "SELECT *, lag(s1) OVER (PARTITION BY "
                + TAGS
                + " ORDER BY time) AS lg FROM table1 ORDER BY "
                + TAGS
                + ", time DESC");
    assertStreamingSortAbove(tester, WindowNode.class, 2);
    for (SortNode sort : sortsAbove(tester, WindowNode.class)) {
      assertEquals(DESC_NULLS_LAST, sort.getOrderingScheme().getOrdering(new Symbol("time")));
    }
  }

  @Test
  public void testRankPartitionOrderDoesNotNeedOuterSort() {
    PlanTester tester =
        plan(
            "SELECT *, rank() OVER (PARTITION BY "
                + TAGS
                + " ORDER BY s1) AS rk FROM table1 ORDER BY "
                + TAGS);
    assertFalse(findNodes(tester, WindowNode.class).isEmpty());
    assertTrue(sortsAbove(tester, WindowNode.class).isEmpty());
  }

  @Test
  public void testPatternRecognitionPreservesOnlyPartitionPrefix() {
    PlanTester tester =
        plan(
            "SELECT * FROM table1 MATCH_RECOGNIZE (PARTITION BY "
                + TAGS
                + " ORDER BY time MEASURES A.s1 AS col1 ONE ROW PER MATCH PATTERN (A B+)"
                + " DEFINE B AS B.s1 > A.s1) AS m ORDER BY "
                + TAGS
                + ", col1");
    assertStreamingSortAbove(tester, PatternRecognitionNode.class, 2);
  }

  @Test
  public void testRowNumberUsesNativeTimeOrderWithoutSort() {
    for (String direction : List.of("ASC", "DESC")) {
      PlanTester tester =
          plan(
              "SELECT *, row_number() OVER (PARTITION BY "
                  + TAGS
                  + ") AS rn FROM table1 ORDER BY "
                  + TAGS
                  + ", time "
                  + direction);
      assertFalse(findNodes(tester, RowNumberNode.class).isEmpty());
      assertTrue(findNodes(tester, SortNode.class).isEmpty());
    }
  }

  @Test
  public void testTopKRetainsEveryRegionAfterGroupElimination() {
    for (String direction : List.of("ASC", "DESC")) {
      PlanTester tester =
          plan(
              "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY "
                  + TAGS
                  + " ORDER BY time "
                  + direction
                  + ") AS rn FROM table1) WHERE rn <= 3 ORDER BY "
                  + TAGS
                  + ", time "
                  + direction);
      assertFalse(findNodes(tester, TopKRankingNode.class).isEmpty());
      assertTrue(findNodes(tester, SortNode.class).isEmpty());
      long copies =
          findNodes(tester, DeviceTableScanNode.class).stream()
              .flatMap(scan -> scan.getDeviceEntries().stream())
              .filter(entry -> entry.getDeviceID().toString().equals("table1.shenzhen.B1.XX"))
              .count();
      assertEquals(2, copies);
    }
  }

  @Test
  public void testTopKDoesNotSortAllInputRowsByRankingField() {
    PlanTester tester =
        plan(
            "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY "
                + TAGS
                + " ORDER BY s1) AS rn FROM table1) WHERE rn <= 2 ORDER BY "
                + TAGS
                + ", time");
    assertStreamingSortAbove(tester, TopKRankingNode.class, 2);
    // The heap ranks rows itself, including a device whose data spans multiple regions.
    for (SortNode sort : findNodes(tester, SortNode.class)) {
      assertFalse(findNodes(sort, TopKRankingNode.class).isEmpty());
    }
  }

  @Test
  public void testSingleRegionPartitionsDoNotImplyGlobalRankingOrder() {
    PlanTester tester =
        plan(
            "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY "
                + TAGS
                + " ORDER BY s1) AS rn FROM table1 WHERE tag1='shanghai') WHERE rn <= 2 ORDER BY s1");
    List<SortNode> sorts = sortsAbove(tester, TopKRankingNode.class);
    assertEquals(1, sorts.size());
    assertFalse(sorts.get(0) instanceof StreamSortNode);
    assertEquals(List.of(new Symbol("s1")), sorts.get(0).getOrderingScheme().getOrderBy());
  }

  @Test
  public void testSingleDeviceRankingKeepsSortElimination() {
    for (String predicate : List.of("tag1='beijing' AND tag2='A1'", "tag2='B2'")) {
      PlanTester tester =
          plan(
              "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY "
                  + TAGS
                  + " ORDER BY s1) AS rn FROM table1 WHERE "
                  + predicate
                  + ") WHERE rn <= 2 ORDER BY s1");
      assertEquals(
          1,
          findNodes(tester, DeviceTableScanNode.class).stream()
              .flatMap(scan -> scan.getDeviceEntries().stream())
              .map(entry -> entry.getDeviceID())
              .distinct()
              .count());
      assertTrue(findNodes(tester, SortNode.class).isEmpty());
    }
  }

  @Test
  public void testValueFillDoesNotClearWindowAndTvfSortRestrictions() {
    String window = "(SELECT *, count(s1) OVER (ORDER BY " + TAGS + ") AS c FROM table1)";
    for (String source : List.of(window, "HOP(DATA => " + window + ", SLIDE => 5m, SIZE => 10m)")) {
      PlanTester tester =
          plan(
              "SELECT * FROM "
                  + source
                  + " FILL METHOD CONSTANT 0 ORDER BY "
                  + TAGS
                  + ", time DESC");
      List<SortNode> sorts = sortsAbove(tester, ValueFillNode.class);
      assertFalse(sorts.isEmpty());
      for (SortNode sort : sorts) {
        assertEquals(DESC_NULLS_LAST, sort.getOrderingScheme().getOrdering(new Symbol("time")));
      }
    }
  }

  @Test
  public void testFillKeepsNonNullDeviceAndTimeSortElimination() {
    String window =
        "(SELECT *, lag(s1) OVER (PARTITION BY " + TAGS + " ORDER BY time) AS lg FROM table1)";
    for (String source :
        List.of("table1", window, "HOP(DATA => " + window + ", SLIDE => 5m, SIZE => 10m)")) {
      for (String value : List.of("0", "'missing'")) {
        for (String projection : List.of("*", TAGS + ", time")) {
          PlanTester tester =
              plan(
                  "SELECT "
                      + projection
                      + " FROM "
                      + source
                      + " FILL METHOD CONSTANT "
                      + value
                      + " ORDER BY "
                      + TAGS
                      + ", time");
          assertFalse(findNodes(tester, ValueFillNode.class).isEmpty());
          assertTrue(findNodes(tester, SortNode.class).isEmpty());
        }
      }
    }
  }

  @Test
  public void testUnorderedQueriesKeepUnorderedCollection() {
    for (String sql :
        List.of(
            "SELECT *, lag(s1) OVER (PARTITION BY " + TAGS + " ORDER BY time) AS lg FROM table1",
            "SELECT * FROM CAPACITY(DATA => table1 PARTITION BY ("
                + TAGS
                + ") ORDER BY time, SIZE => 3, SLIDE => 1)")) {
      PlanTester tester = plan(sql);
      PlanNode coordinator = tester.getFragmentPlan(0);
      assertFalse(findNodes(coordinator, CollectNode.class).isEmpty());
      assertTrue(findNodes(tester, SortNode.class).isEmpty());
    }
  }

  @Test
  public void testOrderPreservingSetFunctionsKeepNativeTimeOrder() {
    for (String function :
        List.of(
            "SESSION(DATA => table1 PARTITION BY (" + TAGS + ") ORDER BY time, GAP => 10m)",
            "CAPACITY(DATA => table1 PARTITION BY ("
                + TAGS
                + ") ORDER BY time, SIZE => 3, SLIDE => 1)",
            "VARIATION(DATA => table1 PARTITION BY ("
                + TAGS
                + ") ORDER BY time, COL => 's1', DELTA => 1.0)")) {
      PlanTester tester = plan("SELECT * FROM " + function + " ORDER BY " + TAGS + ", time");
      assertFalse(findNodes(tester, TableFunctionProcessorNode.class).isEmpty());
      assertTrue(findNodes(tester, SortNode.class).isEmpty());
    }
  }

  @Test
  public void testMismatchedWindowPartitionAndDiffKeepFullSort() {
    PlanTester window =
        plan(
            "SELECT *, count(s1) OVER (PARTITION BY s2 ORDER BY time) AS c"
                + " FROM table1 ORDER BY "
                + TAGS
                + ", s1");
    List<SortNode> sorts = sortsAbove(window, WindowNode.class);
    assertFalse(sorts.isEmpty());
    assertTrue(sorts.stream().noneMatch(StreamSortNode.class::isInstance));
    PlanTester diff =
        plan("SELECT * FROM (SELECT *, diff(s1) AS d FROM table1) ORDER BY " + TAGS + ", s1");
    assertFalse(findNodes(diff, SortNode.class).isEmpty());
    assertTrue(findNodes(diff, StreamSortNode.class).isEmpty());
  }

  private static PlanTester plan(String sql) {
    PlanTester tester = new PlanTester();
    tester.createPlan(sql);
    return tester;
  }

  private static void assertStreamingSortAbove(
      PlanTester tester, Class<? extends PlanNode> type, int prefixEnd) {
    List<SortNode> sorts = sortsAbove(tester, type);
    assertFalse(sorts.isEmpty());
    for (SortNode sort : sorts) {
      assertTrue(sort instanceof StreamSortNode);
      assertEquals(prefixEnd, ((StreamSortNode) sort).getStreamCompareKeyEndIndex());
      assertEquals(
          Arrays.asList(TAGS.split(", ")).subList(0, prefixEnd + 1),
          sort.getOrderingScheme().getOrderBy().subList(0, prefixEnd + 1).stream()
              .map(Symbol::getName)
              .collect(Collectors.toList()));
    }
  }

  private static List<SortNode> sortsAbove(PlanTester tester, Class<? extends PlanNode> type) {
    return findNodes(tester, SortNode.class).stream()
        .filter(sort -> !findNodes(sort, type).isEmpty())
        .collect(Collectors.toList());
  }

  private static <T extends PlanNode> List<T> findNodes(PlanTester tester, Class<T> type) {
    return tester.getDistributedPlan().getFragments().stream()
        .flatMap(fragment -> findNodes(fragment.getPlanNodeTree(), type).stream())
        .collect(Collectors.toList());
  }

  private static <T extends PlanNode> List<T> findNodes(PlanNode root, Class<T> type) {
    List<T> result = new ArrayList<>();
    if (type.isInstance(root)) {
      result.add(type.cast(root));
    }
    root.getChildren().forEach(child -> result.addAll(findNodes(child, type)));
    return result;
  }
}
