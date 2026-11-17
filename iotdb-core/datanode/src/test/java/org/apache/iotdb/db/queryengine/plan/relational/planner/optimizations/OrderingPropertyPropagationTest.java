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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.tsfile.CopyToTsFileOptions;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CopyToNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder.ASC_NULLS_FIRST;
import static org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder.DESC_NULLS_LAST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
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
  public void testFillGroupKeepsGroupAndTimeOrdering() {
    String window = ", lag(s1) OVER (PARTITION BY " + TAGS + " ORDER BY time) AS lg";
    for (String extraColumn : List.of("", window)) {
      for (String source : List.of("table1", "HOP(DATA => table1, SLIDE => 5m, SIZE => 10m)")) {
        for (String method : statefulFillMethods()) {
          for (String[] group : new String[][] {{"2,3,4", TAGS}, {"2", "tag1"}}) {
            String input =
                "SELECT time, "
                    + TAGS
                    + ", s1"
                    + extraColumn
                    + " FROM "
                    + source
                    + " FILL METHOD "
                    + method
                    + " TIME_COLUMN 1 FILL_GROUP "
                    + group[0];
            for (String order : List.of(group[1], group[1] + ", time")) {
              String sql = input + " ORDER BY " + order;
              PlanTester tester = plan(sql);
              assertFalse(sql, findNodes(tester, FillNode.class).isEmpty());
              assertTrue(sql, sortsAbove(tester, FillNode.class).isEmpty());
            }
            assertFalse(
                input,
                sortsAbove(plan(input + " ORDER BY " + group[1] + ", time DESC"), FillNode.class)
                    .isEmpty());
          }
        }
      }
    }
  }

  @Test
  public void testFillGroupKeepsAggregateBucketOrdering() {
    for (String bucket : List.of("date_bin", "date_bin_gapfill")) {
      for (String method : statefulFillMethods()) {
        String sql =
            "SELECT "
                + bucket
                + "(1h, time) AS t, "
                + TAGS
                + ", avg(s1) AS v FROM table1 WHERE time >= 0 AND time < 7200000"
                + " GROUP BY 1,2,3,4 FILL METHOD "
                + method
                + " TIME_COLUMN 1 FILL_GROUP 2,3,4 ORDER BY "
                + TAGS
                + ", t";
        PlanTester tester = plan(sql);
        assertFalse(sql, findNodes(tester, FillNode.class).isEmpty());
        assertTrue(sql, sortsAbove(tester, FillNode.class).isEmpty());
        // Keep the input organization required by fill/gapfill, without sorting the result again.
        assertEquals(sql, 1, findNodes(tester, SortNode.class).size());
      }
    }
  }

  @Test
  public void testStatefulFillKeepsSortOnNullableValueKeys() {
    for (String method : statefulFillMethods()) {
      for (String nullOrder : List.of("NULLS FIRST", "NULLS LAST")) {
        String sql =
            "SELECT * FROM (SELECT * FROM table1 ORDER BY s1 "
                + nullOrder
                + ", s2 LIMIT 10) FILL METHOD "
                + method
                + (method.equals("PREVIOUS") || method.equals("NEXT") ? "" : " TIME_COLUMN 1")
                + " ORDER BY s1 "
                + nullOrder
                + ", s2";
        assertFalse(sql, sortsAbove(plan(sql), FillNode.class).isEmpty());
      }
    }
  }

  @Test
  public void testFilledHelperDoesNotPreserveSecondaryPeerOrdering() {
    String input =
        "SELECT * FROM (SELECT CASE WHEN s1 > 0 THEN time END AS t, tag1, tag2 FROM table1"
            + " ORDER BY tag1, t, tag2 LIMIT 10)"
            + " FILL METHOD PREVIOUS TIME_COLUMN 1 FILL_GROUP 2";
    // Filling a trailing NULL helper keeps the primary order, but can merge timestamp peers.
    // Even a non-null secondary key must then be sorted again.
    assertTrue(sortsAbove(plan(input + " ORDER BY tag1, t"), FillNode.class).isEmpty());
    assertFalse(sortsAbove(plan(input + " ORDER BY tag1, t, tag2"), FillNode.class).isEmpty());
  }

  private static List<String> statefulFillMethods() {
    return List.of("PREVIOUS", "PREVIOUS TIME_BOUND 1h", "NEXT", "NEXT TIME_BOUND 1h", "LINEAR");
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

  @Test
  public void testPlainQueryPreservesOrderBy() {
    PlanTester tester = new PlanTester();
    PlanNode logical =
        tester
            .createPlan("SELECT time, tag1, tag2, tag3, s1 FROM table1 ORDER BY time DESC")
            .getRootNode();
    List<SortNode> sorts = findNodes(logical, SortNode.class);
    assertEquals(1, sorts.size());
    Symbol time = sorts.get(0).getOrderingScheme().getOrderBy().get(0);
    assertEquals("time", time.getName());
    assertEquals(DESC_NULLS_LAST, sorts.get(0).getOrderingScheme().getOrdering(time));

    SortNode queryRoot = Mockito.spy(sorts.get(0));
    PlanOptimizer.Context context = Mockito.mock(PlanOptimizer.Context.class);
    Mockito.when(context.getAnalysis()).thenReturn(tester.getAnalysis());
    assertSame(queryRoot, new RemoveRedundantSortForCopyTo().optimize(queryRoot, context));
    Mockito.verify(queryRoot, Mockito.never()).getChild();
    Mockito.verify(queryRoot, Mockito.never()).getChildren();
    assertSame(logical, new RemoveRedundantSortForCopyTo().optimize(logical, context));

    List<MergeSortNode> merges = findNodes(tester, MergeSortNode.class);
    assertFalse(merges.isEmpty());
    for (MergeSortNode merge : merges) {
      assertEquals(List.of(time), merge.getOrderingScheme().getOrderBy());
      assertEquals(DESC_NULLS_LAST, merge.getOrderingScheme().getOrdering(time));
    }
  }

  @Test
  public void testCopyToWithoutFromKeepsRegularSort() {
    for (String prefix : List.of("", "EXPLAIN ", "EXPLAIN ANALYZE ")) {
      PlanTester tester = new PlanTester();
      PlanNode logical =
          tester
              .createPlan(
                  prefix
                      + "COPY (SELECT CAST(1 AS TIMESTAMP) AS time, 1 AS v)"
                      + " TO 'constant.tsfile' (MEMORY_THRESHOLD 1000000)")
              .getRootNode();
      CopyToNode copyTo = onlyCopyTo(findNodes(logical, CopyToNode.class));
      assertFalse(((CopyToTsFileOptions) copyTo.getCopyToOptions()).isGenerateNewTimeColumn());
      List<SortNode> sorts = findNodes(copyTo.getChild(), SortNode.class);
      assertEquals(1, sorts.size());
      assertCopyToOrdering(copyTo, sorts.get(0).getOrderingScheme(), "time");
      assertTrue(findNodes(logical, StreamSortNode.class).isEmpty());
      assertTrue(findNodes(logical, DeviceTableScanNode.class).isEmpty());

      CopyToNode distributedCopyTo = onlyCopyTo(findNodes(tester, CopyToNode.class));
      sorts = findNodes(tester, SortNode.class);
      assertEquals(1, sorts.size());
      assertCopyToOrdering(distributedCopyTo, sorts.get(0).getOrderingScheme(), "time");
      assertTrue(findNodes(tester, StreamSortNode.class).isEmpty());
      assertTrue(findNodes(tester, DeviceTableScanNode.class).isEmpty());
    }
  }

  @Test
  public void testCopyToSortUsesStreamingScanAndDistributedMerge() {
    for (String source :
        List.of(
            "table1",
            "table1(time, tag1, tag2, tag3, s1)",
            "(SELECT time, tag1, tag2, tag3, s1 FROM table1)",
            "(SELECT time, tag1, tag2, tag3, s1 FROM table1 ORDER BY time DESC)",
            "(SELECT time, tag1, tag2, tag3, s1 FROM table1 ORDER BY s1 DESC)")) {
      PlanTester tester = new PlanTester();
      PlanNode logical = tester.createPlan("COPY " + source + " TO 'copy.tsfile'").getRootNode();
      assertTrue(tester.getAnalysis().isQuery());
      assertTrue(tester.getAnalysis().hasSortNode());
      CopyToNode copyTo = onlyCopyTo(findNodes(logical, CopyToNode.class));
      List<SortNode> sorts = findNodes(copyTo.getChild(), SortNode.class);
      assertEquals(1, sorts.size());
      assertTrue(sorts.get(0) instanceof StreamSortNode);
      assertCopyToOrdering(
          copyTo, sorts.get(0).getOrderingScheme(), "tag1", "tag2", "tag3", "time");

      CopyToNode distributedCopyTo = onlyCopyTo(findNodes(tester, CopyToNode.class));
      List<MergeSortNode> merges = findNodes(tester, MergeSortNode.class);
      assertFalse(merges.isEmpty());
      for (MergeSortNode merge : merges) {
        assertCopyToOrdering(
            distributedCopyTo, merge.getOrderingScheme(), "tag1", "tag2", "tag3", "time");
      }
      assertTrue(findNodes(tester, SortNode.class).isEmpty());
      List<DeviceTableScanNode> scans = findNodes(tester, DeviceTableScanNode.class);
      assertFalse(scans.isEmpty());
      scans.forEach(scan -> assertEquals(Ordering.ASC, scan.getScanOrder()));
    }
  }

  @Test
  public void testExplainCopyToRemovesRedundantSort() {
    for (String prefix : List.of("EXPLAIN ", "EXPLAIN ANALYZE ")) {
      PlanTester tester = new PlanTester();
      PlanNode logical =
          tester
              .createPlan(
                  prefix
                      + "COPY (SELECT time, tag1, tag2, tag3, s1 FROM table1 ORDER BY time DESC)"
                      + " TO 'copy_explain.tsfile'")
              .getRootNode();
      CopyToNode copyTo = onlyCopyTo(findNodes(logical, CopyToNode.class));
      List<SortNode> sorts = findNodes(copyTo.getChild(), SortNode.class);
      assertEquals(1, sorts.size());
      assertTrue(sorts.get(0) instanceof StreamSortNode);
      assertCopyToOrdering(
          copyTo, sorts.get(0).getOrderingScheme(), "tag1", "tag2", "tag3", "time");
      onlyCopyTo(findNodes(tester, CopyToNode.class));
      assertTrue(findNodes(tester, SortNode.class).isEmpty());
    }
  }

  @Test
  public void testCopyToSortPreservesInnerTopKAndOffset() {
    for (String orderColumn : List.of("time", "s1")) {
      PlanTester tester = new PlanTester();
      PlanNode logical =
          tester
              .createPlan(
                  "COPY (SELECT time, s1 FROM table1 ORDER BY "
                      + orderColumn
                      + " DESC OFFSET 2 LIMIT 3) TO 'copy_limit.tsfile'")
              .getRootNode();
      CopyToNode copyTo = onlyCopyTo(findNodes(logical, CopyToNode.class));
      List<SortNode> sorts = findNodes(copyTo.getChild(), SortNode.class);
      assertEquals(1, sorts.size());
      assertCopyToOrdering(copyTo, sorts.get(0).getOrderingScheme(), "time");
      List<OffsetNode> offsets = findNodes(sorts.get(0), OffsetNode.class);
      assertEquals(1, offsets.size());
      assertEquals(2, offsets.get(0).getCount());
      List<TopKNode> topKs = findNodes(offsets.get(0), TopKNode.class);
      assertEquals(1, topKs.size());
      Symbol orderSymbol =
          copyTo.getInnerQueryOutputSymbols().get(orderColumn.equals("time") ? 0 : 1);
      assertEquals(5, topKs.get(0).getCount());
      assertEquals(List.of(orderSymbol), topKs.get(0).getOrderingScheme().getOrderBy());
      assertEquals(DESC_NULLS_LAST, topKs.get(0).getOrderingScheme().getOrdering(orderSymbol));

      CopyToNode distributedCopyTo = onlyCopyTo(findNodes(tester, CopyToNode.class));
      List<SortNode> outerSorts = sortsAbove(tester, OffsetNode.class);
      assertEquals(1, outerSorts.size());
      assertCopyToOrdering(distributedCopyTo, outerSorts.get(0).getOrderingScheme(), "time");
      offsets = findNodes(tester, OffsetNode.class);
      assertEquals(1, offsets.size());
      assertEquals(2, offsets.get(0).getCount());
      topKs = findNodes(tester, TopKNode.class);
      assertFalse(topKs.isEmpty());
      for (TopKNode topK : topKs) {
        assertEquals(5, topK.getCount());
        assertEquals(List.of(orderSymbol), topK.getOrderingScheme().getOrderBy());
        assertEquals(DESC_NULLS_LAST, topK.getOrderingScheme().getOrdering(orderSymbol));
      }
    }
  }

  @Test
  public void testCopyToSortPreservesOffsetWithoutLimit() {
    PlanTester tester = new PlanTester();
    PlanNode logical =
        tester
            .createPlan(
                "COPY (SELECT time, s1 FROM table1 ORDER BY time DESC OFFSET 2)"
                    + " TO 'copy_offset.tsfile'")
            .getRootNode();
    CopyToNode copyTo = onlyCopyTo(findNodes(logical, CopyToNode.class));
    List<SortNode> sorts = findNodes(copyTo.getChild(), SortNode.class);
    assertEquals(2, sorts.size());
    assertCopyToOrdering(copyTo, sorts.get(0).getOrderingScheme(), "time");
    List<OffsetNode> offsets = findNodes(sorts.get(0), OffsetNode.class);
    assertEquals(1, offsets.size());
    assertEquals(2, offsets.get(0).getCount());
    List<SortNode> innerSorts = findNodes(offsets.get(0), SortNode.class);
    assertEquals(1, innerSorts.size());
    Symbol time = copyTo.getInnerQueryOutputSymbols().get(0);
    assertEquals(DESC_NULLS_LAST, innerSorts.get(0).getOrderingScheme().getOrdering(time));

    onlyCopyTo(findNodes(tester, CopyToNode.class));
    assertEquals(1, sortsAbove(tester, OffsetNode.class).size());
    offsets = findNodes(tester, OffsetNode.class);
    assertEquals(1, offsets.size());
    assertEquals(2, offsets.get(0).getCount());
  }

  @Test
  public void testCopyToSortResolvesAliasesAndTargetTagOrder() {
    PlanTester tester = new PlanTester();
    PlanNode logical =
        tester
            .createPlan(
                "COPY (SELECT time AS t, tag1 AS k1, tag2 AS k2, tag3 AS k3, s1 AS v"
                    + " FROM table1) TO 'copy_alias.tsfile' WITH (TIME t, TAGS(k2, k1, k3))")
            .getRootNode();
    CopyToNode copyTo = onlyCopyTo(findNodes(logical, CopyToNode.class));
    List<SortNode> sorts = findNodes(copyTo.getChild(), SortNode.class);
    assertEquals(1, sorts.size());
    assertTrue(sorts.get(0) instanceof StreamSortNode);
    assertCopyToOrdering(copyTo, sorts.get(0).getOrderingScheme(), "k2", "k1", "k3", "t");

    CopyToNode distributedCopyTo = onlyCopyTo(findNodes(tester, CopyToNode.class));
    List<MergeSortNode> merges = findNodes(tester, MergeSortNode.class);
    assertFalse(merges.isEmpty());
    for (MergeSortNode merge : merges) {
      assertCopyToOrdering(distributedCopyTo, merge.getOrderingScheme(), "k2", "k1", "k3", "t");
    }
    assertTrue(findNodes(tester, SortNode.class).isEmpty());
  }

  @Test
  public void testCopyToGeneratedTimePreservesInnerOrderBy() {
    PlanTester tester = new PlanTester();
    PlanNode logical =
        tester
            .createPlan("COPY (SELECT s1 FROM table1 ORDER BY s1 DESC) TO 'copy_generated.tsfile'")
            .getRootNode();
    CopyToNode copyTo = onlyCopyTo(findNodes(logical, CopyToNode.class));
    assertTrue(((CopyToTsFileOptions) copyTo.getCopyToOptions()).isGenerateNewTimeColumn());
    List<SortNode> sorts = findNodes(copyTo.getChild(), SortNode.class);
    assertEquals(1, sorts.size());
    Symbol value = copyTo.getInnerQueryOutputSymbols().get(0);
    assertEquals(List.of(value), sorts.get(0).getOrderingScheme().getOrderBy());
    assertEquals(DESC_NULLS_LAST, sorts.get(0).getOrderingScheme().getOrdering(value));

    onlyCopyTo(findNodes(tester, CopyToNode.class));
    sorts = findNodes(tester, SortNode.class);
    assertFalse(sorts.isEmpty());
    for (SortNode sort : sorts) {
      assertEquals(List.of(value), sort.getOrderingScheme().getOrderBy());
      assertEquals(DESC_NULLS_LAST, sort.getOrderingScheme().getOrdering(value));
    }
  }

  @Test
  public void testCopyToGeneratedTimeNeedsNoSort() {
    PlanTester tester = new PlanTester();
    PlanNode logical =
        tester
            .createPlan("COPY (SELECT tag1, tag2, tag3, s1 FROM table1) TO 'copy_generated.tsfile'")
            .getRootNode();
    CopyToNode copyTo = onlyCopyTo(findNodes(logical, CopyToNode.class));
    assertTrue(((CopyToTsFileOptions) copyTo.getCopyToOptions()).isGenerateNewTimeColumn());
    assertFalse(tester.getAnalysis().hasSortNode());
    assertTrue(findNodes(logical, SortNode.class).isEmpty());
    onlyCopyTo(findNodes(tester, CopyToNode.class));
    assertTrue(findNodes(tester, SortNode.class).isEmpty());
    assertTrue(findNodes(tester, MergeSortNode.class).isEmpty());
  }

  private static CopyToNode onlyCopyTo(List<CopyToNode> nodes) {
    assertEquals(1, nodes.size());
    return nodes.get(0);
  }

  private static void assertCopyToOrdering(
      CopyToNode copyTo, OrderingScheme ordering, String... columnNames) {
    List<String> outputNames =
        copyTo.getInnerQueryDatasetHeader().getColumnHeaders().stream()
            .map(ColumnHeader::getColumnName)
            .collect(Collectors.toList());
    List<Symbol> expected = new ArrayList<>();
    for (String columnName : columnNames) {
      int index = outputNames.indexOf(columnName);
      assertTrue(columnName, index >= 0);
      expected.add(copyTo.getInnerQueryOutputSymbols().get(index));
    }
    assertEquals(expected, ordering.getOrderBy());
    expected.forEach(symbol -> assertEquals(ASC_NULLS_FIRST, ordering.getOrdering(symbol)));
    assertTrue(copyTo.getChild().getOutputSymbols().containsAll(expected));
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
