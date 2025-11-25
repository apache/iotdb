/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregationFunction;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.union;
import static org.junit.Assert.assertEquals;

public class PushAggregationThroughUnionTest {
  @Before
  public void setUp() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  @Test
  public void UnionAggregationTest() {

    String sql = "select * from t1 union select * from t2";
    Analysis analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    LogicalQueryPlan actualLogicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);

    // verify the Logical plan    `Output - Aggregation - union - 2*tableScan`
    assertPlan(
        actualLogicalQueryPlan,
        output(aggregation(union(tableScan("testdb.t1"), tableScan("testdb.t2")))));

    // verify the Distributed plan    `Output - Aggregation - collect - 6*exchange` +
    // 6*(aggregation-project-tableScan)`
    TableDistributedPlanner TableDistributedPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, actualLogicalQueryPlan, TEST_MATADATA, null);
    DistributedQueryPlan actualDistributedQueryPlan = TableDistributedPlanner.plan();
    assertEquals(7, actualDistributedQueryPlan.getFragments().size());

    PlanMatchPattern expectedPattern =
        output(
            aggregation(
                collect(exchange(), exchange(), exchange(), exchange(), exchange(), exchange())));

    OutputNode outputNode =
        (OutputNode)
            actualDistributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertPlan(outputNode, expectedPattern);

    for (int i = 1; i < actualDistributedQueryPlan.getFragments().size(); i++) {
      PlanNode planNode =
          actualDistributedQueryPlan.getFragments().get(i).getPlanNodeTree().getChildren().get(0);
      assertPlan(planNode, aggregation(project(tableScan(i <= 3 ? "testdb.t1" : "testdb.t2"))));
    }
  }

  @Test
  public void unionAllWithGroupByAggregationTest() {

    String sql =
        "SELECT tag1, COUNT(s1), sum(s1) FROM (SELECT tag1, s1 FROM t1 UNION ALL SELECT tag1, s1 FROM t2) GROUP BY tag1";
    Analysis analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();

    LogicalQueryPlan actualLogicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);

    // verify the Logical plan
    assertPlan(
        actualLogicalQueryPlan,
        output(aggregation(union(tableScan("testdb.t1"), tableScan("testdb.t2")))));

    // verify the Distributed plan
    TableDistributedPlanner tableDistributedPlanner =
        new TableDistributedPlanner(
            analysis, symbolAllocator, actualLogicalQueryPlan, TEST_MATADATA, null);
    DistributedQueryPlan actualDistributedQueryPlan = tableDistributedPlanner.plan();

    assertEquals(7, actualDistributedQueryPlan.getFragments().size());
    PlanMatchPattern expectedRootPattern =
        output(
            aggregation(
                singleGroupingSet("tag1"),
                ImmutableMap.of(
                    Optional.of("count"),
                    aggregationFunction("count", ImmutableList.of("count_12")),
                    Optional.of("sum"),
                    aggregationFunction("sum", ImmutableList.of("sum_13"))),
                Optional.empty(),
                AggregationNode.Step.FINAL,
                collect(exchange(), exchange(), exchange(), exchange(), exchange(), exchange())));
    OutputNode outputNode =
        (OutputNode)
            actualDistributedQueryPlan.getFragments().get(0).getPlanNodeTree().getChildren().get(0);
    assertPlan(outputNode, expectedRootPattern);

    for (int i = 1; i < actualDistributedQueryPlan.getFragments().size(); i++) {
      PlanNode planNode =
          actualDistributedQueryPlan.getFragments().get(i).getPlanNodeTree().getChildren().get(0);
      PlanMatchPattern expectedLeafPattern =
          aggregation(project(tableScan(i <= 3 ? "testdb.t1" : "testdb.t2")));
      assertPlan(planNode, expectedLeafPattern);
    }
  }
}
