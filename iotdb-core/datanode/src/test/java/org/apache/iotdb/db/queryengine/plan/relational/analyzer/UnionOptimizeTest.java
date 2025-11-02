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
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;

import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.topK;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.union;

public class UnionOptimizeTest {

  @Test
  public void simpleLeftDeepMerge() {

    String sql =
        "(select tag1 from t1 union all select tag1 from t2) union all select tag1 from t3 ";
    Analysis analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    LogicalQueryPlan actualLogicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);

    // just verify the Logical plan    `Output  - union - 3*tableScan`
    assertPlan(
        actualLogicalQueryPlan,
        output((union(tableScan("testdb.t1"), tableScan("testdb.t2"), tableScan("testdb.t3")))));
  }

  @Test
  public void simpleRightDeepMerge() {

    String sql =
        "select tag1 from t1 union all (select tag1 from t2 union all select tag1 from t3) ";
    Analysis analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    LogicalQueryPlan actualLogicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);

    // just verify the Logical plan    `Output  - union - 3*tableScan`
    assertPlan(
        actualLogicalQueryPlan,
        output((union(tableScan("testdb.t1"), tableScan("testdb.t2"), tableScan("testdb.t3")))));
  }

  @Test
  public void bushyTreeMerge() {

    String sql =
        "(select tag1 from t1 union all select tag1 from t2) union all (select tag1 from t3 union all select tag1 from t4) ";
    Analysis analysis = analyzeSQL(sql, TEST_MATADATA, QUERY_CONTEXT);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    LogicalQueryPlan actualLogicalQueryPlan =
        new TableLogicalPlanner(
                QUERY_CONTEXT, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);

    // just verify the Logical plan    `Output  - union - 4*tableScan`
    assertPlan(
        actualLogicalQueryPlan,
        output(
            (union(
                tableScan("testdb.t1"),
                tableScan("testdb.t2"),
                tableScan("testdb.t3"),
                tableScan("testdb.t4")))));
  }

  @Test
  public void pushLimitThroughUnionTest() {
    assertPlan(
        new PlanTester()
            .createPlan("(select tag1 from t1) union all (select tag1 from t2) limit 1"),
        output(
            limit(1, (union(limit(1, tableScan("testdb.t1")), limit(1, tableScan("testdb.t2")))))));
  }

  @Test
  public void pushProjectionThroughUnionTest() {
    assertPlan(
        new PlanTester()
            .createPlan("select s1 + 1 from ((select s1 from t1) union all (select s1 from t2)) "),
        output((union(project(tableScan("testdb.t1")), project(tableScan("testdb.t2"))))));
  }

  @Test
  public void pushTopKThroughUnionTest() {
    assertPlan(
        new PlanTester()
            .createPlan(
                "(select tag1 from t1) union all (select tag1 from t2) order by tag1 limit 10"),
        output(topK((union(topK(tableScan("testdb.t1")), topK(tableScan("testdb.t2")))))));
  }

  @Test
  public void removeEmptyUnionBranchesTest1() {
    // Normal case
    assertPlan(
        new PlanTester()
            .createPlan(
                "(select tag1 from t1 limit 0) union all (select tag1 from t2)  union all (select tag1 from t3)"),
        output((union(tableScan("testdb.t2"), tableScan("testdb.t3")))));
  }

  @Test
  public void removeEmptyUnionBranchesTest2() {
    // One non-empty branch
    assertPlan(
        new PlanTester()
            .createPlan(
                "(select tag1 from t1 limit 0) union all (select tag1 from t2 limit 0)  union all (select tag1 from t3 limit 1)"),
        output(limit(1, tableScan("testdb.t3"))));
  }

  @Test
  public void removeEmptyUnionBranchesTest3() {
    // All branches are empty
    assertPlan(
        new PlanTester()
            .createPlan(
                "(select tag1 from t1 limit 0) union all (select tag1 from t2 limit 0)  union all (select tag1 from t3 limit 0)"),
        output(limit(0, tableScan("testdb.t1"))));
  }
}
