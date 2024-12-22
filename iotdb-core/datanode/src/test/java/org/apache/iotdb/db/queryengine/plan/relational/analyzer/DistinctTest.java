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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode.Step.SINGLE;

public class DistinctTest {
  // ==================================================================
  // ===================== Select Distinct Test =======================
  // ==================================================================

  @Test
  public void simpleTest() {
    PlanTester planTester = new PlanTester();
    // Measurements in SELECT
    LogicalQueryPlan distinctQueryPlan = planTester.createPlan("select distinct s1 from table1");
    PlanMatchPattern expectedPlanPattern =
        output(
            aggregation(
                ImmutableMap.of(),
                SINGLE,
                tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"))));
    assertPlan(distinctQueryPlan, expectedPlanPattern);

    // Test if this agg-query is equal to select-distinct
    LogicalQueryPlan aggQueryPlan = planTester.createPlan("select s1 from table1 group by s1");
    assertPlan(aggQueryPlan, expectedPlanPattern);

    // Only device columns in SELECT
    // Test if the optimizer of agg-query is also effective to select-distinct
    distinctQueryPlan = planTester.createPlan("select distinct tag1, tag2, tag3 from table1");
    expectedPlanPattern =
        output(
            aggregationTableScan(
                singleGroupingSet("tag1", "tag2", "tag3"),
                ImmutableList.of("tag1", "tag2", "tag3"),
                Optional.empty(),
                SINGLE,
                "testdb.table1",
                ImmutableList.of("tag1", "tag2", "tag3"),
                ImmutableSet.of("tag1", "tag2", "tag3")));
    assertPlan(distinctQueryPlan, expectedPlanPattern);

    // Test if this agg-query is equal to select-distinct
    aggQueryPlan =
        planTester.createPlan("select tag1, tag2, tag3 from table1 group by tag1, tag2, tag3");
    assertPlan(aggQueryPlan, expectedPlanPattern);
  }

  @Test
  public void withGroupByTest() {
    PlanTester planTester = new PlanTester();
    // Hit optimizer PruneDistinctAggregation
    LogicalQueryPlan distinctQueryPlan =
        planTester.createPlan("select distinct s1 from table1 group by s1");
    PlanMatchPattern expectedPlanPattern =
        output(
            aggregation(
                ImmutableMap.of(),
                SINGLE,
                tableScan("testdb.table1", ImmutableList.of("s1"), ImmutableSet.of("s1"))));
    assertPlan(distinctQueryPlan, expectedPlanPattern);

    // Doesn't hit optimizer PruneDistinctAggregation
    distinctQueryPlan = planTester.createPlan("select distinct s1 from table1 group by s1,s2");
    expectedPlanPattern =
        output(
            aggregation(
                ImmutableMap.of(),
                SINGLE,
                project(
                    aggregation(
                        ImmutableMap.of(),
                        SINGLE,
                        tableScan(
                            "testdb.table1",
                            ImmutableList.of("s1", "s2"),
                            ImmutableSet.of("s1", "s2"))))));
    assertPlan(distinctQueryPlan, expectedPlanPattern);

    distinctQueryPlan = planTester.createPlan("select distinct avg(s1) from table1 group by s1,s2");
    assertPlan(distinctQueryPlan, expectedPlanPattern);
  }
}
