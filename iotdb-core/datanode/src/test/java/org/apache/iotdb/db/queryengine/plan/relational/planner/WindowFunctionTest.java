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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.window;

public class WindowFunctionTest {
  @Test
  public void testSimpleWindowFunction() {
    PlanTester planTester = new PlanTester();

    String sql = "SELECT sum(s1) OVER(PARTITION BY tag1) FROM table1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan("testdb.table1", ImmutableList.of("tag1", "s1"), ImmutableSet.of("tag1", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *           └──ProjectNode
     *             └──WindowNode
     *               └──SortNode
     *                 └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(project(window(sort(tableScan)))));

    /*  ProjectNode
     *     └──WindowNode
     *         └──MergeSort
     *               ├──ExchangeNode
     *               │    └──TableScan
     *               ├──ExchangeNode
     *               │    └──TableScan
     *               └──ExchangeNode
     *                    └──TableScan
     */
    assertPlan(
        planTester.getFragmentPlan(0),
        output(project(window(mergeSort(exchange(), exchange(), exchange())))));
    assertPlan(planTester.getFragmentPlan(1), tableScan);
    assertPlan(planTester.getFragmentPlan(2), tableScan);
    assertPlan(planTester.getFragmentPlan(3), tableScan);
  }

  @Test
  public void testWindowFunctionWithOrderBy() {
    PlanTester planTester = new PlanTester();

    String sql = "SELECT sum(s1) OVER(PARTITION BY tag1 ORDER BY s1) FROM table1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan("testdb.table1", ImmutableList.of("tag1", "s1"), ImmutableSet.of("tag1", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *           └──ProjectNode
     *             └──WindowNode
     *               └──SortNode
     *                 └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(project(window(sort(tableScan)))));

    /*  ProjectNode
     *     └──WindowNode
     *         └──MergeSort
     *               ├──ExchangeNode
     *               │    └──SortNode
     *               │      └──TableScan
     *               ├──ExchangeNode
     *               │    └──SortNode
     *               │       └──TableScan
     *               └──ExchangeNode
     *                    └──SortNode
     *                       └──TableScan
     */
    assertPlan(
        planTester.getFragmentPlan(0),
        output(project(window(mergeSort(exchange(), exchange(), exchange())))));
    assertPlan(planTester.getFragmentPlan(1), sort(tableScan));
    assertPlan(planTester.getFragmentPlan(2), sort(tableScan));
    assertPlan(planTester.getFragmentPlan(3), sort(tableScan));
  }

  @Test
  public void testWindowFunctionOrderByPartitionByDup() {
    PlanTester planTester = new PlanTester();

    String sql = "SELECT sum(s1) OVER(PARTITION BY tag1 ORDER BY tag1) FROM table1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan("testdb.table1", ImmutableList.of("tag1", "s1"), ImmutableSet.of("tag1", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *           └──ProjectNode
     *             └──WindowNode
     *               └──SortNode
     *                 └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(project(window(sort(tableScan)))));

    /*  ProjectNode
     *     └──WindowNode
     *         └──MergeSort
     *               ├──ExchangeNode
     *               │    └──TableScan
     *               ├──ExchangeNode
     *               │    └──TableScan
     *               └──ExchangeNode
     *                    └──TableScan
     */
    assertPlan(
        planTester.getFragmentPlan(0),
        output(project(window(mergeSort(exchange(), exchange(), exchange())))));
    assertPlan(planTester.getFragmentPlan(1), tableScan);
    assertPlan(planTester.getFragmentPlan(2), tableScan);
    assertPlan(planTester.getFragmentPlan(3), tableScan);
  }

  @Test
  public void testWindowFunctionWithFrame() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT sum(s1) OVER(PARTITION BY tag1 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM table1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan("testdb.table1", ImmutableList.of("tag1", "s1"), ImmutableSet.of("tag1", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *           └──ProjectNode
     *             └──WindowNode
     *               └──SortNode
     *                 └──ProjectNode
     *                   └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(project(window(sort(project(tableScan))))));

    /*  ProjectNode
     *     └──WindowNode
     *         └──MergeSort
     *               ├──ExchangeNode
     *               │    └──ProjectNode
     *               │        └──TableScan
     *               ├──ExchangeNode
     *               │    └──ProjectNode
     *               │        └──TableScan
     *               └──ExchangeNode
     *                    └──ProjectNode
     *                        └──TableScan
     */
    assertPlan(
        planTester.getFragmentPlan(0),
        output(project(window(mergeSort(exchange(), exchange(), exchange())))));
    assertPlan(planTester.getFragmentPlan(1), project(tableScan));
    assertPlan(planTester.getFragmentPlan(2), project(tableScan));
    assertPlan(planTester.getFragmentPlan(3), project(tableScan));
  }

  @Test
  public void testWindowFunctionWithPushDown() {
    PlanTester planTester = new PlanTester();

    String sql = "SELECT sum(s1) OVER(PARTITION BY tag1,tag2,tag3) FROM table1";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableList.of("tag1", "tag2", "tag3", "s1"),
            ImmutableSet.of("tag1", "tag2", "tag3", "s1"));

    // Verify full LogicalPlan
    /*
     *   └──OutputNode
     *           └──ProjectNode
     *             └──WindowNode
     *               └──SortNode
     *                 └──ProjectNode
     *                   └──TableScanNode
     */
    assertPlan(logicalQueryPlan, output(project(window(group(tableScan)))));

    // Verify DistributionPlan
    /*
     *   └──OutputNode
     *         └──CollectNode
     *               ├──ExchangeNode
     *               │    └──ProjectNode
     *               │        └──WindowNode
     *               │            └──TableScanNode
     *               ├──ExchangeNode
     *               │    └──ProjectNode
     *               │        └──WindowNode
     *               │            └──TableScanNode
     *               └──ExchangeNode
     *                    └──ProjectNode
     *                         └──WindowNode
     *                              └──MergeSortNode
     *                                     ├──ExchangeNode
     *                                     │     └──TableScan
     *                                     └──ExchangeNode
     *                                           └──TableScan
     */
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));
    assertPlan(planTester.getFragmentPlan(1), project(window(tableScan)));
    assertPlan(planTester.getFragmentPlan(2), project(window(tableScan)));
    assertPlan(planTester.getFragmentPlan(3), project(window(mergeSort(exchange(), exchange()))));
    assertPlan(planTester.getFragmentPlan(4), tableScan);
    assertPlan(planTester.getFragmentPlan(5), tableScan);
  }
}
