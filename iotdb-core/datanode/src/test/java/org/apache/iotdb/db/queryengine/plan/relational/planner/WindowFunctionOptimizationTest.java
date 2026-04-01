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

import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.group;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.rowNumber;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.topKRanking;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.window;

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

    // Two window function has swapped, but the query plan remains the same
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
        logicalQueryPlan2,
        output(
            project(
                window(
                    ImmutableList.of("tag1", "s1"),
                    ImmutableList.of(),
                    sort(window(sort(tableScan)))))));
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
     *                        └──SortNode
     *                            └──TableScan
     */
    assertPlan(
        planTester.getFragmentPlan(0), output((collect(exchange(), exchange(), exchange()))));
    assertPlan(planTester.getFragmentPlan(1), topKRanking(tableScan));
    assertPlan(planTester.getFragmentPlan(2), topKRanking(tableScan));
    assertPlan(planTester.getFragmentPlan(3), topKRanking(sort(tableScan)));
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
