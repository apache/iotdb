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

import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.changePoint;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.group;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.window;

public class ChangePointOptimizationTest {

  @Test
  public void testChangePointOptimization() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * FROM (SELECT *, LEAD(s1) OVER (PARTITION BY tag1, tag2, tag3 ORDER BY time) AS next FROM table1) WHERE s1 <> next OR next IS NULL";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Logical plan still has Filter -> Window (optimization happens at distributed level)
    assertPlan(logicalQueryPlan, output((filter(window(group(tableScan))))));

    // Distributed plan: ChangePointNode replaces Filter -> Window per partition
    // Fragment 0: Output -> Collect -> Exchange*
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange(), exchange())));
    // Fragment 1+: ChangePointNode -> TableScan
    assertPlan(planTester.getFragmentPlan(1), changePoint(tableScan));
    assertPlan(planTester.getFragmentPlan(2), changePoint(tableScan));
  }

  @Test
  public void testChangePointNotMatchedWithLag() {
    PlanTester planTester = new PlanTester();

    // LAG instead of LEAD should NOT be optimized
    String sql =
        "SELECT * FROM (SELECT *, LAG(s1) OVER (PARTITION BY tag1, tag2, tag3 ORDER BY time) AS prev FROM table1) WHERE s1 <> prev OR prev IS NULL";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Should remain Filter -> Window in the logical plan
    assertPlan(logicalQueryPlan, output((filter(window(group(tableScan))))));

    // Distributed plan: should still have Filter -> Window per partition (no ChangePoint)
    assertPlan(planTester.getFragmentPlan(1), filter(window(tableScan)));
  }

  @Test
  public void testChangePointNotMatchedWithDifferentPredicate() {
    PlanTester planTester = new PlanTester();

    // Different predicate (s1 = next instead of s1 != next) should NOT be optimized
    String sql =
        "SELECT * FROM (SELECT *, LEAD(s1) OVER (PARTITION BY tag1, tag2, tag3 ORDER BY time) AS next FROM table1) WHERE s1 = next";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    assertPlan(logicalQueryPlan, output(filter(window(group(tableScan)))));

    // Distributed plan: should still have Filter -> Window (no ChangePoint)
    assertPlan(planTester.getFragmentPlan(1), filter(window(tableScan)));
  }

  @Test
  public void testChangePointNotMatchedWithMultipleWindowFunctions() {
    PlanTester planTester = new PlanTester();

    // Multiple window functions should NOT be optimized
    String sql =
        "SELECT * FROM (SELECT *, LEAD(s1) OVER (PARTITION BY tag1, tag2, tag3 ORDER BY time) AS next, row_number() OVER (PARTITION BY tag1, tag2, tag3 ORDER BY time) AS rn FROM table1) WHERE s1 <> next OR next IS NULL";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);
    PlanMatchPattern tableScan = tableScan("testdb.table1");

    // Should not be transformed to ChangePoint because there are multiple window functions
    assertPlan(logicalQueryPlan, output(filter(window(group(tableScan)))));
  }
}
