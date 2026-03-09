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
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.project;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.union;

public class UnionTest {
  @Test
  public void simpleTest() {
    PlanTester planTester = new PlanTester();

    assertPlan(
        planTester.createPlan("(select * from table2) union all (select * from table3)"),
        output(union(tableScan("testdb.table2"), tableScan("testdb.table3"))));

    // use Aggregation to process distinct
    assertPlan(
        planTester.createPlan("(select * from table2) union (select * from table3)"),
        output(aggregation(union(tableScan("testdb.table2"), tableScan("testdb.table3")))));

    // use CAST if types of according columns is not compatible
    // s1 is INT64, s3 is DOUBLE
    assertPlan(
        planTester.createPlan("(select s1, s3 from table2) union (select s1, s1 from table3)"),
        output(
            aggregation(union(tableScan("testdb.table2"), project(tableScan("testdb.table3"))))));
  }

  @Test
  public void optimizerTest() {
    PlanTester planTester = new PlanTester();

    // The predicate will be push down into TableScanNode
    assertPlan(
        planTester.createPlan(
            "select * from ((select * from table2) union all (select * from table3)) where s1 > 1"),
        output(union(tableScan("testdb.table2"), tableScan("testdb.table3"))));
  }
}
