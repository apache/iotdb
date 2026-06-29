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
 * for except node, only merge first source node
 *
 * <p>For parent and child of type EXCEPT: 1. if parent is EXCEPT DISTINCT and child is EXCEPT ALL,
 * merge is not possible 2. if parent and child are both EXCEPT DISTINCT, the resulting set
 * operation is EXCEPT DISTINCT 3. if parent and child are both EXCEPT ALL, the resulting set
 * operation is EXCEPT ALL 4. if parent is EXCEPT ALL and child is EXCEPT DISTINCT, the resulting
 * set operation is EXCEPT DISTINCT
 */
public class MergeExceptTest {

  @Test
  public void mergeTwoExceptAll() {
    PlanTester planTester = new PlanTester();

    // the parent
    assertPlan(
        planTester.createPlan(
            "select tag1 from t1 except all select tag1 from t2 except all select tag1 from t3 "),
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
  public void mergeTwoExceptDistinct() {
    PlanTester planTester = new PlanTester();

    // the parent
    assertPlan(
        planTester.createPlan(
            "select tag1 from t1 except distinct select tag1 from t2 except distinct select tag1 from t3 "),
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
  public void mergeExceptAllAndExceptDistinct() {
    PlanTester planTester = new PlanTester();

    assertPlan(
        planTester.createPlan(
            "select tag1 from t1 except distinct select tag1 from t2  except all select tag1 from t3 "),
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
  public void testMergeNotPossibleForDistinctOverAll() {
    PlanTester planTester = new PlanTester();

    assertPlan(
        planTester.createPlan(
            "select tag1 from t1 except all select tag1 from t2  except  select tag1 from t3 "),
        output(
            project(
                filter(
                    aggregation(
                        union(
                            project(
                                project(
                                    filter(
                                        project(
                                            window(
                                                sort(
                                                    union(
                                                        project(tableScan("testdb.t1")),
                                                        project(tableScan("testdb.t2"))))))))),
                            project(tableScan("testdb.t3"))))))));
  }

  @Test
  public void testNoMergeForRightChild() {
    PlanTester planTester = new PlanTester();

    assertPlan(
        planTester.createPlan(
            "select tag1 from t1 except (select tag1 from t2 except select tag1 from t3) "),
        output(
            project(
                filter(
                    aggregation(
                        union(
                            project(tableScan("testdb.t1")),
                            project(
                                project(
                                    filter(
                                        aggregation(
                                            union(
                                                project(tableScan("testdb.t2")),
                                                project(tableScan("testdb.t3")))))))))))));
  }
}
