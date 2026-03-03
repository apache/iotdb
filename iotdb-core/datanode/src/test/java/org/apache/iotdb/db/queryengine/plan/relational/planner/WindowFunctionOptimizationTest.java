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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKRankingNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
        "SELECT * FROM (SELECT *, rank() OVER (PARTITION BY tag1, tag2, tag3 ORDER BY s1) as rn FROM table1) WHERE rn <= 2";
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
    assertPlan(planTester.getFragmentPlan(3), topKRanking(mergeSort(exchange(), exchange())));
    assertPlan(planTester.getFragmentPlan(4), sort(tableScan));
    assertPlan(planTester.getFragmentPlan(5), sort(tableScan));
  }

  @Test
  public void testPushDownFilterIntoWindow() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * FROM (SELECT *, rank() OVER (PARTITION BY tag1 ORDER BY s1) as rn FROM table1) WHERE rn <= 2";
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

  @Test
  public void testTopKRankingOrderByTimeLimitPushDown() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tag1, tag2, tag3 ORDER BY time) as rn FROM table1) WHERE rn <= 2";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Logical plan: OutputNode -> TopKRankingNode -> GroupNode -> TableScanNode
    assertPlan(logicalQueryPlan, output(topKRanking(group(tableScan))));

    // Distributed plan: TopKRankingNode pushed down to each partition with limit push-down.
    // Fragment 0: OutputNode -> CollectNode -> ExchangeNodes
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));

    // Worker fragments: TopKRankingNode -> DeviceTableScanNode
    // Verify limit is pushed to DeviceTableScanNode and TopKRankingNode is marked for streaming.
    for (int i = 1; i <= 2; i++) {
      PlanNode fragmentRoot = planTester.getFragmentPlan(i);
      assertTrue(
          "Fragment " + i + " root should be TopKRankingNode",
          fragmentRoot instanceof TopKRankingNode);
      TopKRankingNode topKNode = (TopKRankingNode) fragmentRoot;
      assertTrue(
          "TopKRankingNode should be marked as dataPreSortedAndLimited",
          topKNode.isDataPreSortedAndLimited());

      PlanNode scanChild = topKNode.getChild();
      assertNotNull("TopKRankingNode should have a child", scanChild);
      assertTrue("Child should be DeviceTableScanNode", scanChild instanceof DeviceTableScanNode);
      DeviceTableScanNode dts = (DeviceTableScanNode) scanChild;
      assertTrue("pushLimitToEachDevice should be true", dts.isPushLimitToEachDevice());
      assertEquals("pushDownLimit should be 2", 2, dts.getPushDownLimit());
    }
  }

  @Test
  public void testTopKRankingEliminatedWhenRankSymbolNotOutput() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT tag1, s1 FROM (SELECT *, row_number() OVER (PARTITION BY tag1, tag2, tag3 ORDER BY time) as rn FROM table1) WHERE rn <= 2";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Logical plan: OutputNode -> ProjectNode -> TopKRankingNode -> GroupNode -> TableScanNode
    assertPlan(logicalQueryPlan, output(project(topKRanking(group(tableScan)))));

    // Distributed plan: TopKRankingNode eliminated since rn is not in the output.
    // Limit is pushed to DeviceTableScanNode.
    // Fragment 0: OutputNode -> CollectNode -> ExchangeNodes
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));

    // Worker fragments: ProjectNode -> DeviceTableScanNode (no TopKRankingNode)
    for (int i = 1; i <= 2; i++) {
      PlanNode fragmentRoot = planTester.getFragmentPlan(i);
      assertFalse(
          "Fragment " + i + " root should NOT be TopKRankingNode",
          fragmentRoot instanceof TopKRankingNode);
      assertPlan(planTester.getFragmentPlan(i), project(tableScan));

      assertTrue(
          "Fragment " + i + " root should be ProjectNode", fragmentRoot instanceof ProjectNode);
      PlanNode scanChild = fragmentRoot.getChildren().get(0);
      assertTrue("Child should be DeviceTableScanNode", scanChild instanceof DeviceTableScanNode);
      DeviceTableScanNode dts = (DeviceTableScanNode) scanChild;
      assertTrue("pushLimitToEachDevice should be true", dts.isPushLimitToEachDevice());
      assertEquals("pushDownLimit should be 2", 2, dts.getPushDownLimit());
    }
  }

  @Test
  public void testTopKRankingKeptWhenRankSymbolIsOutput() {
    PlanTester planTester = new PlanTester();

    // Same query but SELECT * includes rn - TopKRankingNode should NOT be eliminated
    String sql =
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tag1, tag2, tag3 ORDER BY time) as rn FROM table1) WHERE rn <= 2";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    assertPlan(logicalQueryPlan, output(topKRanking(group(tableScan))));

    // Worker fragments should still have TopKRankingNode
    for (int i = 1; i <= 2; i++) {
      PlanNode fragmentRoot = planTester.getFragmentPlan(i);
      assertTrue(
          "Fragment " + i + " root should be TopKRankingNode",
          fragmentRoot instanceof TopKRankingNode);
    }
  }

  @Test
  public void testRowNumberEliminatedWhenRowNumberNotOutput() {
    PlanTester planTester = new PlanTester();

    // RowNumber with all IDs as partition - row number not in output
    String sql =
        "SELECT tag1, s1 FROM (SELECT *, row_number() OVER (PARTITION BY tag1, tag2, tag3) as rn FROM table1)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Logical plan: row_number is pruned at the window level by PruneWindowColumns
    // since rn is not referenced anywhere. The plan should not contain RowNumberNode.
    assertPlan(logicalQueryPlan, output(project(group(tableScan))));
  }

  @Test
  public void testRowNumberPushDownWhenRowNumberIsOutput() {
    PlanTester planTester = new PlanTester();

    // RowNumber with all IDs as partition and rn IS referenced in output
    String sql =
        "SELECT tag1, s1, rn FROM (SELECT *, row_number() OVER (PARTITION BY tag1, tag2, tag3) as rn FROM table1)";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Logical plan: OutputNode -> ProjectNode -> RowNumberNode -> GroupNode -> TableScanNode
    // (project is inlined since it selects a subset including rn)
    assertPlan(logicalQueryPlan, output(project(rowNumber(group(tableScan)))));

    // RowNumberNode is pushed down to each partition (not eliminated, since rn IS in the output)
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));
    assertPlan(planTester.getFragmentPlan(1), project(rowNumber(tableScan)));
    assertPlan(planTester.getFragmentPlan(2), project(rowNumber(tableScan)));
  }

  @Test
  public void testRowNumberWithMaxCountEliminatedWhenRowNumberNotOutput() {
    PlanTester planTester = new PlanTester();

    // rn <= 2 pushes the limit into RowNumberNode (maxRowCountPerPartition=2), and since rn is
    // not in the outer SELECT, the RowNumberNode is eliminated in the distributed plan.
    String sql =
        "SELECT tag1, s1 FROM (SELECT *, row_number() OVER (PARTITION BY tag1, tag2, tag3) as rn FROM table1) WHERE rn <= 2";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Logical plan: PushFilterIntoRowNumber absorbs rn<=2, leaving RowNumberNode with maxRowCount=2
    // No filter remains above RowNumberNode.
    assertPlan(logicalQueryPlan, output(project(rowNumber(group(tableScan)))));

    // Distributed plan: RowNumberNode eliminated since rn is not in the output.
    // Limit (maxRowCountPerPartition=2) is pushed to each DeviceTableScanNode.
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));

    // Worker fragments: ProjectNode -> DeviceTableScanNode (no RowNumberNode)
    for (int i = 1; i <= 2; i++) {
      PlanNode fragmentRoot = planTester.getFragmentPlan(i);
      assertFalse(
          "Fragment " + i + " root should NOT be RowNumberNode",
          fragmentRoot instanceof RowNumberNode);
      assertPlan(planTester.getFragmentPlan(i), project(tableScan));

      assertTrue(
          "Fragment " + i + " root should be ProjectNode", fragmentRoot instanceof ProjectNode);
      PlanNode scanChild = fragmentRoot.getChildren().get(0);
      assertTrue("Child should be DeviceTableScanNode", scanChild instanceof DeviceTableScanNode);
      DeviceTableScanNode dts = (DeviceTableScanNode) scanChild;
      assertTrue("pushLimitToEachDevice should be true", dts.isPushLimitToEachDevice());
      assertEquals("pushDownLimit should be 2", 2, dts.getPushDownLimit());
    }
  }

  @Test
  public void testRowNumberWithMaxCountKeptWhenRowNumberIsOutput() {
    PlanTester planTester = new PlanTester();

    // Same query but SELECT * includes rn - RowNumberNode should NOT be eliminated
    String sql =
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tag1, tag2, tag3) as rn FROM table1) WHERE rn <= 2";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Logical plan: RowNumberNode with maxRowCount=2 (filter absorbed), no outer project removing
    // rn
    assertPlan(logicalQueryPlan, output(rowNumber(group(tableScan))));

    // Worker fragments should still have RowNumberNode since rn IS in the output
    for (int i = 1; i <= 2; i++) {
      PlanNode fragmentRoot = planTester.getFragmentPlan(i);
      assertTrue(
          "Fragment " + i + " root should be RowNumberNode", fragmentRoot instanceof RowNumberNode);
    }
  }

  @Test
  public void testTopKRankingWithEqualPredicate() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tag1 ORDER BY s1) as rn FROM table1) WHERE rn = 2";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // TopKRanking created with maxRanking=2, but filter(rn = 2) is kept because
    // ranking values 1..2 do not all satisfy rn = 2
    /*
     *   └──OutputNode
     *         └──FilterNode(rn = 2)
     *              └──TopKRankingNode
     *                   └──SortNode
     *                        └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(filter(topKRanking(sort(tableScan)))));
  }

  @Test
  public void testTopKRankingWithEqualPredicateAllPartitions() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tag1, tag2, tag3 ORDER BY s1) as rn FROM table1) WHERE rn = 2";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // TopKRanking created with maxRanking=2, filter(rn = 2) is kept
    /*
     *   └──OutputNode
     *         └──FilterNode(rn = 2)
     *              └──TopKRankingNode
     *                   └──GroupNode
     *                        └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(filter(topKRanking(group(tableScan)))));

    // Distributed plan: TopKRanking and filter pushed down
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));
    assertPlan(planTester.getFragmentPlan(1), filter(topKRanking(tableScan)));
    assertPlan(planTester.getFragmentPlan(2), filter(topKRanking(tableScan)));
  }

  @Test
  public void testTopKRankingWithLessThanPredicate() {
    PlanTester planTester = new PlanTester();

    // rn < 3 is equivalent to rn <= 2, so the filter should be fully absorbed
    String sql =
        "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tag1 ORDER BY s1) as rn FROM table1) WHERE rn < 3";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Filter absorbed into TopKRankingNode (maxRanking=2)
    /*
     *   └──OutputNode
     *         └──TopKRankingNode
     *              └──SortNode
     *                   └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(topKRanking(sort(tableScan))));
  }

  @Test
  public void testTopKRankingWithEqualPredicateColumnPruned() {
    PlanTester planTester = new PlanTester();

    // rn = 2 with rn not in output: filter kept, rn pruned by project
    String sql =
        "SELECT tag1, s1 FROM (SELECT *, row_number() OVER (PARTITION BY tag1, tag2, tag3 ORDER BY s1) as rn FROM table1) WHERE rn = 2";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Filter(rn = 2) kept, project prunes rn from output
    /*
     *   └──OutputNode
     *         └──ProjectNode
     *              └──FilterNode(rn = 2)
     *                └──ProjectNode
     *                   └──TopKRankingNode
     *                        └──GroupNode
     *                             └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(project(filter(project(topKRanking(group(tableScan)))))));
  }
}
