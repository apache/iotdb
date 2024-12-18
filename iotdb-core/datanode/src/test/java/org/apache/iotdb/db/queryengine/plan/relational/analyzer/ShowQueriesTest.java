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

import java.util.Optional;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.collect;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.exchange;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.infoSchemaTableScan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.limit;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.mergeSort;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.offset;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.sort;

public class ShowQueriesTest {
  private final PlanTester planTester = new PlanTester();

  @Test
  public void testNormal() {
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan("show queries");
    assertPlan(
        logicalQueryPlan,
        output(infoSchemaTableScan("information_schema.queries", Optional.empty())));

    //                  - Exchange
    // Output - Collect - Exchange
    assertPlan(planTester.getFragmentPlan(0), output(collect(exchange(), exchange())));
    // TableScan
    assertPlan(
        planTester.getFragmentPlan(1),
        infoSchemaTableScan("information_schema.queries", Optional.of(1)));
    // TableScan
    assertPlan(
        planTester.getFragmentPlan(2),
        infoSchemaTableScan("information_schema.queries", Optional.of(2)));
  }

  @Test
  public void testLimitOffset() {
    LogicalQueryPlan logicalQueryPlan = planTester.createPlan("show queries limit 1 offset 2");
    assertPlan(
        logicalQueryPlan,
        output(
            offset(
                2, limit(3, infoSchemaTableScan("information_schema.queries", Optional.empty())))));

    //                                   - Exchange
    // Output - offset - limit - Collect - Exchange
    assertPlan(
        planTester.getFragmentPlan(0),
        output(offset(2, limit(3, collect(exchange(), exchange())))));
    // TableScan
    assertPlan(
        planTester.getFragmentPlan(1),
        infoSchemaTableScan("information_schema.queries", Optional.of(1)));
    // TableScan
    assertPlan(
        planTester.getFragmentPlan(2),
        infoSchemaTableScan("information_schema.queries", Optional.of(2)));
  }

  @Test
  public void testSort() {
    LogicalQueryPlan logicalQueryPlan =
        planTester.createPlan("show queries order by time, query_id");
    assertPlan(
        logicalQueryPlan,
        output(sort(infoSchemaTableScan("information_schema.queries", Optional.empty()))));

    //                    - Exchange
    // Output - MergeSort - Exchange
    assertPlan(planTester.getFragmentPlan(0), output(mergeSort(exchange(), exchange())));
    // Sort - TableScan
    assertPlan(
        planTester.getFragmentPlan(1),
        sort(infoSchemaTableScan("information_schema.queries", Optional.of(1))));
    // Sort - TableScan
    assertPlan(
        planTester.getFragmentPlan(2),
        sort(infoSchemaTableScan("information_schema.queries", Optional.of(2))));
  }
}
