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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.FINAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.PARTIAL;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;

public class AggregationTest {
  @Test
  public void noPushDownTest() {
    PlanTester planTester = new PlanTester();
    String sql = "SELECT count(s2) FROM table1 group by s1";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    // Output - Project - Aggregation - TableScan
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("s1"),
                    ImmutableMap.of(
                        Optional.empty(), aggregationFunction("count", ImmutableList.of("s2"))),
                    Optional.empty(),
                    SINGLE,
                    tableScan(
                        "testdb.table1",
                        ImmutableList.of("s1", "s2"),
                        ImmutableSet.of("s1", "s2"))))));
  }

  @Test
  public void partialPushDownTest() {
    PlanTester planTester = new PlanTester();
    String sql = "SELECT count(s2) FROM table1 group by tag1";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    // Output - Project - Aggregation - AggTableScan
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("tag1"),
                    ImmutableMap.of(
                        Optional.empty(),
                        aggregationFunction("count", ImmutableList.of("count_0"))),
                    Optional.empty(),
                    FINAL,
                    aggregationTableScan(
                        singleGroupingSet("tag1"),
                        Optional.empty(),
                        PARTIAL,
                        "testdb.table1",
                        ImmutableList.of("tag1", "count_0"),
                        ImmutableSet.of("tag1", "count_0"))))));
  }

  @Test
  public void completePushDownTest() {
    PlanTester planTester = new PlanTester();

    String sql = "SELECT count(s2) FROM table1 group by s1";
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableList.of("time", "tag1", "tag2", "tag3", "attr1", "s1", "s2", "s3"),
            ImmutableSet.of("time", "tag1", "tag2", "tag3", "attr1", "s1", "s2", "s3"));

    // Verify full LogicalPlan
    // Output - Project - Aggregation - TableScan
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("s1"),
                    ImmutableMap.of(
                        Optional.empty(), aggregationFunction("count", ImmutableList.of("s2"))),
                    Optional.empty(),
                    SINGLE,
                    tableScan(
                        "testdb.table1",
                        ImmutableList.of("s1", "s2"),
                        ImmutableSet.of("s1", "s2"))))));
  }
}
