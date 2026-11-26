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

import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.aggregation;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.union;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.window;

/** tests for intersect (distinct) and intersect all */
public class IntersectTest {

  @Test
  public void intersectTest() {

    PlanTester planTester = new PlanTester();
    LogicalQueryPlan actualLogicalQueryPlan =
        planTester.createPlan("select tag1 from t1 intersect select tag1 from t2");
    // just verify the Logical plan:  `Output  - project - filter - aggregation - union - 2*(project
    // - tableScan)`
    assertPlan(
        actualLogicalQueryPlan,
        output(
            project(
                filter(
                    aggregation(
                        union(
                            project(tableScan("testdb.t1")), project(tableScan("testdb.t2"))))))));
  }

  @Test
  public void intersectAllTest() {

    PlanTester planTester = new PlanTester();
    LogicalQueryPlan actualLogicalQueryPlan =
        planTester.createPlan("select tag1 from t1 intersect all select tag1 from t2");
    assertPlan(
        actualLogicalQueryPlan,
        output(
            project(
                filter(
                    project(
                        window(
                            sort(
                                union(
                                    project(tableScan("testdb.t1")),
                                    project(tableScan("testdb.t2"))))))))));
  }

  @Test
  public void typeCompatibleTest() {
    // use CAST if types of according columns is not compatible
    // s1 is INT64, s3 is DOUBLE

    PlanTester planTester = new PlanTester();
    LogicalQueryPlan actualLogicalQueryPlan =
        planTester.createPlan("select s1, s3 from table2 intersect all select s1, s1 from table3 ");

    assertPlan(
        actualLogicalQueryPlan,
        output(
            project(
                filter(
                    project(
                        window(
                            sort(
                                union(
                                    project(tableScan("testdb.table2")),
                                    project(tableScan("testdb.table3"))))))))));
  }

  /** the priority of intersect is higher than that of the union */
  @Test
  public void setOperationPriority() {

    PlanTester planTester = new PlanTester();
    LogicalQueryPlan actualLogicalQueryPlan =
        planTester.createPlan(
            "select tag1 from t1 union select tag1 from t2 intersect select tag1 from t3");

    assertPlan(
        actualLogicalQueryPlan,
        output(
            aggregation(
                union(
                    tableScan("testdb.t1"),
                    project(
                        filter(
                            aggregation(
                                union(
                                    project(tableScan("testdb.t2")),
                                    project(tableScan("testdb.t3"))))))))));
  }
}
