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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.distinctAggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.markDistinct;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
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

  // ==================================================================
  // ================== Agg-Function Distinct Test ====================
  // ==================================================================
  @Test
  public void simpleAggFunctionDistinctTest() {
    PlanTester planTester = new PlanTester();
    // Doesn't hit optimizer PushAggregationIntoTableScan
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(
            "select count(distinct s1), count(distinct s2) from table1 group by tag1");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    // The distinct flag in Aggregation is true
                    ImmutableMap.of(
                        "count",
                        distinctAggregationFunction("count", ImmutableList.of("s1")),
                        "count_0",
                        distinctAggregationFunction("count", ImmutableList.of("s2"))),
                    SINGLE,
                    tableScan(
                        "testdb.table1",
                        ImmutableList.of("tag1", "s1", "s2"),
                        ImmutableSet.of("s1", "s2", "tag1"))))));

    // Test distribution plan, doesn't split AggNode into multi-stages
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            project(
                aggregation(
                    ImmutableMap.of(
                        "count",
                        distinctAggregationFunction("count", ImmutableList.of("s1")),
                        "count_0",
                        distinctAggregationFunction("count", ImmutableList.of("s2"))),
                    SINGLE,
                    mergeSort(exchange(), exchange(), exchange())))));
    for (int i = 1; i <= 3; i++) {
      assertPlan(
          planTester.getFragmentPlan(i),
          tableScan(
              "testdb.table1",
              ImmutableList.of("tag1", "s1", "s2"),
              ImmutableSet.of("s1", "s2", "tag1")));
    }

    logicalQueryPlan =
        planTester.createPlan(
            "select count(distinct s1), count(distinct s2) from table1 group by s3");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    ImmutableMap.of(
                        "count",
                        distinctAggregationFunction("count", ImmutableList.of("s1")),
                        "count_0",
                        distinctAggregationFunction("count", ImmutableList.of("s2"))),
                    SINGLE,
                    tableScan(
                        "testdb.table1",
                        ImmutableList.of("s1", "s2", "s3"),
                        ImmutableSet.of("s1", "s2", "s3"))))));

    // Test distribution plan, doesn't split AggNode into multi-stages too
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            project(
                aggregation(
                    ImmutableMap.of(
                        "count",
                        distinctAggregationFunction("count", ImmutableList.of("s1")),
                        "count_0",
                        distinctAggregationFunction("count", ImmutableList.of("s2"))),
                    SINGLE,
                    collect(exchange(), exchange(), exchange())))));
    for (int i = 1; i <= 3; i++) {
      assertPlan(
          planTester.getFragmentPlan(i),
          tableScan(
              "testdb.table1",
              ImmutableList.of("s1", "s2", "s3"),
              ImmutableSet.of("s1", "s2", "s3")));
    }
  }

  @Test
  public void optimizer1Test() {
    // Hit optimizer SingleDistinctAggregationToGroupBy
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan(
            "select count(distinct s1), avg(distinct s1) from table1 group by tag1");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("tag1"),
                    ImmutableMap.of(
                        Optional.of("count"),
                        aggregationFunction("count", ImmutableList.of("s1")),
                        Optional.of("avg"),
                        aggregationFunction("avg", ImmutableList.of("s1"))),
                    Optional.empty(),
                    SINGLE,
                    aggregation(
                        singleGroupingSet("tag1", "s1"),
                        ImmutableMap.of(),
                        ImmutableList.of("tag1"),
                        Optional.empty(),
                        SINGLE,
                        tableScan(
                            "testdb.table1",
                            ImmutableList.of("tag1", "s1"),
                            ImmutableSet.of("s1", "tag1")))))));
  }

  @Test
  public void optimizer2Test() {
    // Hit optimizer MultipleDistinctAggregationToMarkDistinct
    // case1: global Agg
    PlanTester planTester = new PlanTester();
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan("select count(distinct s1), count(s2) from table1");
    assertPlan(
        logicalQueryPlan,
        output(
            aggregation(
                singleGroupingSet(),
                ImmutableMap.of(
                    Optional.of("count"),
                    aggregationFunction("count", ImmutableList.of("s1")),
                    Optional.of("count_0"),
                    aggregationFunction("count", ImmutableList.of("s2"))),
                ImmutableList.of(),
                ImmutableList.of("s1$distinct"),
                Optional.empty(),
                SINGLE,
                markDistinct(
                    "s1$distinct",
                    ImmutableList.of("s1"),
                    tableScan(
                        "testdb.table1",
                        ImmutableList.of("s1", "s2"),
                        ImmutableSet.of("s1", "s2"))))));

    // case2: groupingKeys are more than one
    logicalQueryPlan =
        planTester.createPlan(
            "select count(distinct s1), count(s2) from table1 group by tag1, tag2");
    assertPlan(
        logicalQueryPlan,
        output(
            project(
                aggregation(
                    singleGroupingSet("tag1", "tag2"),
                    ImmutableMap.of(
                        Optional.of("count"),
                        aggregationFunction("count", ImmutableList.of("s1")),
                        Optional.of("count_0"),
                        aggregationFunction("count", ImmutableList.of("s2"))),
                    ImmutableList.of("tag1", "tag2"),
                    ImmutableList.of("s1$distinct"),
                    Optional.empty(),
                    SINGLE,
                    markDistinct(
                        "s1$distinct",
                        ImmutableList.of("tag1", "tag2", "s1"),
                        tableScan(
                            "testdb.table1",
                            ImmutableList.of("tag1", "tag2", "s1", "s2"),
                            ImmutableSet.of("tag1", "tag2", "s1", "s2")))))));

    // DistributionPlanTest
    assertPlan(
        planTester.getFragmentPlan(0),
        output(
            project(
                aggregation(
                    singleGroupingSet("tag1", "tag2"),
                    ImmutableMap.of(
                        Optional.of("count"),
                        aggregationFunction("count", ImmutableList.of("s1")),
                        Optional.of("count_0"),
                        aggregationFunction("count", ImmutableList.of("s2"))),
                    ImmutableList.of("tag1", "tag2"),
                    ImmutableList.of("s1$distinct"),
                    Optional.empty(),
                    SINGLE,
                    markDistinct(
                        "s1$distinct",
                        ImmutableList.of("tag1", "tag2", "s1"),
                        mergeSort(exchange(), exchange(), exchange()))))));
    for (int i = 1; i <= 3; i++) {
      assertPlan(
          planTester.getFragmentPlan(i),
          tableScan(
              "testdb.table1",
              ImmutableList.of("tag1", "tag2", "s1", "s2"),
              ImmutableSet.of("tag1", "tag2", "s1", "s2")));
    }
  }
}
