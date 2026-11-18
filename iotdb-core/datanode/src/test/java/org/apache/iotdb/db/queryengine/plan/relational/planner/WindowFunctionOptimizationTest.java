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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.group;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.rowNumber;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.topKRanking;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.window;
import static org.junit.Assert.assertTrue;

public class WindowFunctionOptimizationTest {
  @Test
  public void testMergeWindowFunctions() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT a + min(s1) OVER (PARTITION BY tag1 ORDER BY s1) as b FROM (SELECT *, max(s1) OVER (PARTITION BY tag1 ORDER BY s1) as a FROM table1)";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan("testdb.table1", ImmutableList.of("tag1", "s1"), ImmutableSet.of("tag1", "s1"));

    // Two window function node separate by subquery and merge into one window function node
    /*
     *   └──OutputNode
     *           └──ProjectNode
     *             └──WindowNode
     *               └──SortNode
     *                 └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(project(window(sort(tableScan)))));

    // Skip distribution plan since we already test same pattern in another file
  }

  @Test
  public void testSwapWindowFunctions() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT min(s1) OVER (PARTITION BY tag1), sum(s1) OVER (PARTITION BY tag1, s1) FROM table1";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan =
        tableScan("testdb.table1", ImmutableList.of("tag1", "s1"), ImmutableSet.of("tag1", "s1"));

    /*
     *   └──OutputNode
     *        └──ProjectNode
     *             └──WindowNode(PARTITION BY tag1, s1)
     *                 └──SortNode
     *                     └──WindowNode(PARTITION BY tag1)
     *                         └──SortNode
     *                              └──TableScanNode
     */
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                window(
                    ImmutableList.of("tag1", "s1"),
                    ImmutableList.of(),
                    sort(window(sort(tableScan)))))));

    String sql2 =
        "SELECT sum(s1) OVER (PARTITION BY tag1, s1), min(s1) OVER (PARTITION BY tag1) FROM table1";
    LogicalQueryPlan logicalQueryPlan2 = planTester.createPlan(sql2);

    // The initial sort by (tag1, s1) satisfies both windows. The second window can therefore
    // reuse the grouping over tag1 without an extra sort.
    /*
     *   └──OutputNode
     *        └──ProjectNode
     *             └──WindowNode(PARTITION BY tag1)
     *                 └──GroupNode(PARTITION BY tag1)
     *                     └──WindowNode(PARTITION BY tag1, s1)
     *                         └──SortNode
     *                              └──TableScanNode
     */
    assertPlan(
        logicalQueryPlan2,
        output(
            project(
                window(
                    ImmutableList.of("tag1"),
                    ImmutableList.of(),
                    group(
                        window(
                            ImmutableList.of("tag1", "s1"),
                            ImmutableList.of(),
                            sort(tableScan)))))));
  }

  @Test
  public void testReorderedSubsetWindowPartitionKeepsSingleInstance() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT count(s1) OVER (PARTITION BY tag2, tag1) FROM "
            + "(SELECT *, max(s1) OVER (PARTITION BY tag1, tag2, tag3) AS m FROM table1)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // The outer PARTITION BY is a reordered strict subset of the inner one. It must not reuse the
    // inner grouping, whose partitions are finer than the outer window's, so the outer window
    // stays single-instance above a sort.
    assertPlan(logicalQueryPlan, output(project(window(sort(project(group(tableScan)))))));
    assertPlan(
        planTester.getFragmentPlan(0),
        output(project(window(mergeSort(exchange(), exchange(), exchange())))));
  }

  @Test
  public void testSameOrderPrefixWindowPartitionKeepsSingleInstance() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT count(s1) OVER (PARTITION BY tag1, tag2) FROM "
            + "(SELECT *, max(s1) OVER (PARTITION BY tag1, tag2, tag3) AS m FROM table1)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // The outer PARTITION BY is a strict prefix of the inner one. A grouping is only clustered
    // per instance, so the outer window must not reuse it: it stays single-instance above a sort.
    assertPlan(logicalQueryPlan, output(project(window(sort(project(group(tableScan)))))));
    assertPlan(
        planTester.getFragmentPlan(0),
        output(project(window(mergeSort(exchange(), exchange(), exchange())))));
  }

  @Test
  public void testPermutedPrefixWindowPartitionKeepsGroup() {
    PlanTester planTester = new PlanTester();

    // The outer PARTITION BY permutes the first two keys of the inner window's sort, which keeps
    // the grouped property: no extra full sort.
    String sql =
        "SELECT count(s1) OVER (PARTITION BY tag2, tag1) FROM "
            + "(SELECT *, sum(s1) OVER (PARTITION BY tag1, tag2 ORDER BY time) AS m FROM table1)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    assertPlan(
        logicalQueryPlan,
        output(
            project(
                window(
                    ImmutableList.of("tag2", "tag1"),
                    ImmutableList.of(),
                    group(project(sort(tableScan("testdb.table1"))))))));
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            project(
                window(
                    ImmutableList.of("tag2", "tag1"),
                    ImmutableList.of(),
                    group(project(mergeSort(exchange(), exchange(), exchange())))))));
  }

  @Test
  public void testWindowOverRowNumberFallsBackToSort() {
    PlanTester planTester = new PlanTester();

    // Distribution drops the sort below a row number, so the outer window must keep its own.
    String sql =
        "SELECT s1, s3, rn, count(*) OVER (PARTITION BY s1) AS cnt FROM "
            + "(SELECT s1, s3, row_number() OVER (PARTITION BY s1, s3) AS rn FROM table1)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    assertPlan(
        logicalQueryPlan,
        output(
            window(
                ImmutableList.of("s1"),
                ImmutableList.of(),
                sort(rowNumber(sort(tableScan("testdb.table1")))))));
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            window(
                ImmutableList.of("s1"),
                ImmutableList.of(),
                sort(rowNumber(collect(exchange(), exchange(), exchange()))))));
  }

  @Test
  public void testWindowOverRowNumberWithDegradedInputFallsBackToSort() {
    PlanTester planTester = new PlanTester();

    // The keys match the row number's, but its input grouping cannot survive, so no claim passes
    // through. The filter keeps the two windows from being merged.
    String sql =
        "SELECT s1, s3, rn, count(*) OVER (PARTITION BY s1, s3) AS cnt FROM "
            + "(SELECT * FROM (SELECT s1, s3, row_number() OVER (PARTITION BY s1, s3) AS rn "
            + "FROM table1) WHERE rn >= 1)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    assertPlan(
        logicalQueryPlan,
        output(
            window(
                ImmutableList.of("s1", "s3"),
                ImmutableList.of(),
                sort(filter(rowNumber(sort(tableScan("testdb.table1"))))))));
  }

  @Test
  public void testWindowOverTopKRankingFallsBackToSort() {
    PlanTester planTester = new PlanTester();

    // Same as the row number: a sort below a ranking is dropped in distribution.
    String sql =
        "SELECT *, count(*) OVER (PARTITION BY tag1) AS cnt FROM "
            + "(SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tag1, tag2 ORDER BY s1) AS rn "
            + "FROM table1) WHERE rn <= 2)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    assertPlan(
        logicalQueryPlan,
        output(
            window(
                ImmutableList.of("tag1"),
                ImmutableList.of(),
                sort(topKRanking(sort(tableScan("testdb.table1")))))));
  }

  @Test
  public void testTimeBucketWindowOverTaggedAggregationKeepsGroup() {
    PlanTester planTester = new PlanTester();

    // The scan is ordered by the pre-grouped tags, which equal the window partition keys, so
    // per-device windows stay parallel instead of moving to a single instance above a sort.
    String sql =
        "SELECT tag1, tag2, tag3, h, m, "
            + "lag(m) OVER (PARTITION BY tag1, tag2, tag3 ORDER BY h) AS prev FROM "
            + "(SELECT tag1, tag2, tag3, date_bin(1h, time) AS h, avg(s1) AS m FROM table1 "
            + "GROUP BY tag1, tag2, tag3, date_bin(1h, time))";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    WindowNode window =
        PlanNodeSearcher.searchFrom(logicalQueryPlan.getRootNode())
            .where(WindowNode.class::isInstance)
            .findFirst()
            .map(WindowNode.class::cast)
            .orElseThrow(() -> new AssertionError("no window in plan"));
    assertTrue(
        "the window must reuse the aggregation's clustering instead of sorting",
        window.getChild() instanceof GroupNode);
    assertTrue(
        "the aggregation must stay pushed into the table scan",
        ((GroupNode) window.getChild()).getChild() instanceof AggregationTableScanNode);
    // The regions keep the aggregation.
    assertPlan(planTester.getFragmentPlan(1), aggregationTableScan());
    assertPlan(planTester.getFragmentPlan(2), aggregationTableScan());
    assertPlan(planTester.getFragmentPlan(3), aggregationTableScan());
  }

  @Test
  public void testTagCompleteSubsetWindowPartitionKeepsGroup() {
    PlanTester planTester = new PlanTester();

    // The outer PARTITION BY covers every TAG column, so each of its partitions is a single
    // device and cannot span regions even though the inner grouping is finer.
    String sql =
        "SELECT *, count(s1) OVER (PARTITION BY tag1, tag2, tag3) FROM "
            + "(SELECT *, max(s1) OVER (PARTITION BY tag1, tag2, tag3, attr1) AS m FROM table1)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    assertPlan(
        logicalQueryPlan,
        output(
            window(
                ImmutableList.of("tag1", "tag2", "tag3"),
                ImmutableList.of(),
                group(
                    window(
                        ImmutableList.of("tag1", "tag2", "tag3", "attr1"),
                        ImmutableList.of(),
                        group(tableScan("testdb.table1")))))));
    // Both windows run per region; only a collect joins them.
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));
  }

  @Test
  public void testTopKRankingPushDown() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tag1, tag2, tag3 ORDER BY s1) as rn FROM table1) WHERE rn <= 2";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Transform window node into TopKRankingNode
    /*
     *   └──OutputNode
     *        └──TopKRankingNode
     *              └──GroupNode
     *                   └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(topKRanking(group(tableScan))));

    // TopK push down
    /*  OutputNode
     *         └──CollectNode
     *               ├──ExchangeNode
     *               │    └──TopKRankingNode
     *               │        └──TableScan
     *               ├──ExchangeNode
     *               │    └──TopKRankingNode
     *               │        └──TableScan
     *               └──ExchangeNode
     *                    └──TopKRankingNode
     *                        └──MergeSortNode
     *                            ├──ExchangeNode ← TableScan
     *                            └──ExchangeNode ← TableScan
     */
    assertPlan(
        planTester.getFragmentPlan(0), output((collect(exchange(), exchange(), exchange()))));
    assertPlan(planTester.getFragmentPlan(1), topKRanking(tableScan));
    assertPlan(planTester.getFragmentPlan(2), topKRanking(tableScan));
    // A device spanning regions must retain both inputs. TopK ranks them without an input field
    // sort.
    assertPlan(planTester.getFragmentPlan(3), topKRanking(mergeSort(exchange(), exchange())));
    assertPlan(planTester.getFragmentPlan(4), tableScan);
    assertPlan(planTester.getFragmentPlan(5), tableScan);
  }

  @Test
  public void testPushDownFilterIntoWindow() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tag1 ORDER BY s1) as rn FROM table1) WHERE rn <= 2";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    /*
     *   └──OutputNode
     *         └──TopKRankingNode
     *              └──SortNode
     *                   └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(topKRanking(sort(tableScan))));

    /*  OutputNode
     *     └──TopKRankingNode
     *         └──CollectNode
     *               ├──ExchangeNode
     *               │        └──TableScan
     *               ├──ExchangeNode
     *               │        └──TableScan
     *               └──ExchangeNode
     *                        └──TableScan
     */
    assertPlan(
        planTester.getFragmentPlan(0),
        output(topKRanking(collect(exchange(), exchange(), exchange()))));
    assertPlan(planTester.getFragmentPlan(1), tableScan);
    assertPlan(planTester.getFragmentPlan(2), tableScan);
    assertPlan(planTester.getFragmentPlan(3), tableScan);
  }

  @Test
  public void testPushDownLimitIntoWindow() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tag1 ORDER BY s1) as rn FROM table1) LIMIT 2";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Limit is for all data
    /*
     *   └──OutputNode
     *        └──LimitNode
     *            └──TopKRankingNode
     *                └──SortNode
     *                    └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(limit(2, topKRanking(sort(tableScan)))));

    /*  OutputNode
     *    └──LimitNode
     *      └──TopKRankingNode
     *          └──MergeSortNode
     *               ├──ExchangeNode
     *               │        └──TableScan
     *               ├──ExchangeNode
     *               │        └──TableScan
     *               └──ExchangeNode
     *                        └──TableScan
     */
    assertPlan(
        planTester.getFragmentPlan(0),
        output(limit(2, topKRanking(collect(exchange(), exchange(), exchange())))));
    assertPlan(planTester.getFragmentPlan(1), tableScan);
    assertPlan(planTester.getFragmentPlan(2), tableScan);
    assertPlan(planTester.getFragmentPlan(3), tableScan);
  }

  @Test
  public void testReplaceWindowWithRowNumber() {
    PlanTester planTester = new PlanTester();

    String sql = "SELECT *, row_number() OVER (PARTITION BY tag1) FROM table1";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Limit is for all data
    /*
     *   └──OutputNode
     *        └──RowNumberNode
     *             └──SortNode
     *                  └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(rowNumber(sort(tableScan))));

    /*  OutputNode
     *     └──RowNumberNode
     *         └──CollectNode
     *               ├──ExchangeNode
     *               │        └──TableScan
     *               ├──ExchangeNode
     *               │        └──TableScan
     *               └──ExchangeNode
     *                        └──TableScan
     */
    assertPlan(
        planTester.getFragmentPlan(0),
        output(rowNumber(collect(exchange(), exchange(), exchange()))));
    assertPlan(planTester.getFragmentPlan(1), tableScan);
    assertPlan(planTester.getFragmentPlan(2), tableScan);
    assertPlan(planTester.getFragmentPlan(3), tableScan);
  }

  @Test
  public void testRowNumberPushDown() {
    PlanTester planTester = new PlanTester();

    String sql = "SELECT *, row_number() OVER (PARTITION BY tag1, tag2, tag3) FROM table1";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Limit is for all data
    /*
     *   └──OutputNode
     *        └──RowNumberNode
     *             └──GroupNode
     *                  └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(rowNumber(group(tableScan))));

    /*  OutputNode
     *         └──CollectNode
     *               ├──ExchangeNode
     *               │    └──RowNumberNode
     *               │        └──TableScan
     *               ├──ExchangeNode
     *               │    └──RowNumberNode
     *               │        └──TableScan
     *               └──ExchangeNode
     *                    └──RowNumberNode
     *                        └──TableScan
     */
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));
    assertPlan(planTester.getFragmentPlan(1), rowNumber(tableScan));
    assertPlan(planTester.getFragmentPlan(2), rowNumber(tableScan));
  }
}
