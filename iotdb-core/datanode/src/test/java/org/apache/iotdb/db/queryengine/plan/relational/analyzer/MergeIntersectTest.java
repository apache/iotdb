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

/**
 * For parent and child of type INTERSECT, merge is always possible: 1. if parent and child are both
 * INTERSECT ALL, the resulting set operation is INTERSECT ALL 2. otherwise, the resulting set
 * operation is INTERSECT DISTINCT: 3. if the parent is DISTINCT, the result has unique values,
 * regardless of whether child branches were DISTINCT or ALL, 4. if the child is DISTINCT, that
 * branch is guaranteed to have unique values, so at most one element of the other branches will be
 * retained -- this is equivalent to just doing DISTINCT on the parent.
 */
public class MergeIntersectTest {

  @Test
  public void mergeTwoIntersectAll() {
    PlanTester planTester = new PlanTester();

    // the parent
    assertPlan(
        planTester.createPlan(
            "select tag1 from t1 intersect all select tag1 from t2  intersect all select tag1 from t3 "),
        output(
            project(
                filter(
                    project(
                        window(
                            sort(
                                union(
                                    project(tableScan("testdb.t1")),
                                    project(tableScan("testdb.t2")),
                                    project(tableScan("testdb.t3"))))))))));
  }

  @Test
  public void mergeTwoIntersectDistinct() {
    PlanTester planTester = new PlanTester();

    // the parent
    assertPlan(
        planTester.createPlan(
            "select tag1 from t1 intersect distinct select tag1 from t2  intersect distinct select tag1 from t3 "),
        output(
            project(
                filter(
                    aggregation(
                        union(
                            project(tableScan("testdb.t1")),
                            project(tableScan("testdb.t2")),
                            project(tableScan("testdb.t3"))))))));
  }

  @Test
  public void mergeIntersectAllAndIntersectDistinct() {
    PlanTester planTester = new PlanTester();

    assertPlan(
        planTester.createPlan(
            "select tag1 from t1 intersect all select tag1 from t2  intersect distinct select tag1 from t3 "),
        output(
            project(
                filter(
                    aggregation(
                        union(
                            project(tableScan("testdb.t1")),
                            project(tableScan("testdb.t2")),
                            project(tableScan("testdb.t3"))))))));

    assertPlan(
        planTester.createPlan(
            "select tag1 from t1 intersect select tag1 from t2  intersect all select tag1 from t3 "),
        output(
            project(
                filter(
                    aggregation(
                        union(
                            project(tableScan("testdb.t1")),
                            project(tableScan("testdb.t2")),
                            project(tableScan("testdb.t3"))))))));
  }
}
