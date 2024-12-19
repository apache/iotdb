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
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlanTester;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationTreeDeviceViewTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.streamSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.treeAlignedDeviceViewTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.treeDeviceViewTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.treeNonAlignedDeviceViewTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.INTERMEDIATE;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;

public class TreeViewTest {
  // ==================================================================
  // ===================== Device View Test =======================
  // ==================================================================

  @Test
  public void rawDataQueryTest() {
    PlanTester planTester = new PlanTester();

    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan("select * from tree_view_db.\"root.test.device_view\"");
    PlanMatchPattern expectedPlanPattern =
        output(
            treeDeviceViewTableScan(
                "tree_view_db.\"root.test.device_view\"",
                ImmutableList.of("time", "tag1", "tag2", "s1", "s2"),
                ImmutableSet.of("time", "tag1", "tag2", "s1", "s2")));
    assertPlan(logicalQueryPlan, expectedPlanPattern);

    // column prune test
    logicalQueryPlan =
        planTester.createPlan(
            "select s1 from tree_view_db.\"root.test.device_view\" order by tag1");
    expectedPlanPattern =
        output(
            project(
                streamSort(
                    treeDeviceViewTableScan(
                        "tree_view_db.\"root.test.device_view\"",
                        ImmutableList.of("tag1", "s1"),
                        ImmutableSet.of("tag1", "s1")))));
    assertPlan(logicalQueryPlan, expectedPlanPattern);

    // distributionPlan test
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            project(
                mergeSort(
                    exchange(),
                    treeAlignedDeviceViewTableScan(
                        "tree_view_db.\"root.test.device_view\"",
                        ImmutableList.of("tag1", "s1"),
                        ImmutableSet.of("tag1", "s1")),
                    treeNonAlignedDeviceViewTableScan(
                        "tree_view_db.\"root.test.device_view\"",
                        ImmutableList.of("tag1", "s1"),
                        ImmutableSet.of("tag1", "s1"))))));

    assertPlan(
        planTester.getFragmentPlan(1),
        treeAlignedDeviceViewTableScan(
            "tree_view_db.\"root.test.device_view\"",
            ImmutableList.of("tag1", "s1"),
            ImmutableSet.of("tag1", "s1")));
  }

  @Test
  public void aggregationQueryTest() {
    PlanTester planTester = new PlanTester();

    // has non-aligned DeviceEntry, no push-down
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(
            "select tag1, count(s1) from tree_view_db.\"root.test.device_view\" group by tag1");
    PlanMatchPattern expectedPlanPattern =
        output(
            aggregation(
                ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of("s1"))),
                treeDeviceViewTableScan(
                    "tree_view_db.\"root.test.device_view\"",
                    ImmutableList.of("tag1", "s1"),
                    ImmutableSet.of("tag1", "s1"))));
    assertPlan(logicalQueryPlan, expectedPlanPattern);

    // only aligned DeviceEntry, do push-down
    logicalQueryPlan =
        planTester.createPlan(
            "select tag1, count(s1) from tree_view_db.\"root.test.device_view\" where tag1='shanghai' group by tag1");
    expectedPlanPattern =
        output(
            aggregation(
                ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of("count_0"))),
                FINAL,
                aggregationTreeDeviceViewTableScan(
                    singleGroupingSet("tag1"),
                    ImmutableList.of("tag1"),
                    Optional.empty(),
                    PARTIAL,
                    "tree_view_db.\"root.test.device_view\"",
                    ImmutableList.of("tag1", "count_0"),
                    ImmutableSet.of("tag1", "s1"))));
    assertPlan(logicalQueryPlan, expectedPlanPattern);

    // distributionPlan test
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            aggregation(
                ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of("count_1"))),
                FINAL,
                mergeSort(
                    exchange(),
                    aggregation(
                        ImmutableMap.of(
                            "count_1", aggregationFunction("count", ImmutableList.of("count_0"))),
                        INTERMEDIATE,
                        aggregationTreeDeviceViewTableScan(
                            singleGroupingSet("tag1"),
                            ImmutableList.of("tag1"),
                            Optional.empty(),
                            PARTIAL,
                            "tree_view_db.\"root.test.device_view\"",
                            ImmutableList.of("tag1", "count_0"),
                            ImmutableSet.of("tag1", "s1")))))));

    assertPlan(
        planTester.getFragmentPlan(1),
        aggregation(
            ImmutableMap.of("count_1", aggregationFunction("count", ImmutableList.of("count_0"))),
            INTERMEDIATE,
            aggregationTreeDeviceViewTableScan(
                singleGroupingSet("tag1"),
                ImmutableList.of("tag1"),
                Optional.empty(),
                PARTIAL,
                "tree_view_db.\"root.test.device_view\"",
                ImmutableList.of("tag1", "count_0"),
                ImmutableSet.of("tag1", "s1"))));
  }
}
