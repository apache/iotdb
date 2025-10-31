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
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;

import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.QUERY_CONTEXT;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.union;

public class MergeUnionTest {

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
}
