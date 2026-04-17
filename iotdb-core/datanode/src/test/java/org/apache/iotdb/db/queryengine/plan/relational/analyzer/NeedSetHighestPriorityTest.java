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

import static org.junit.Assert.assertTrue;

public class NeedSetHighestPriorityTest {

  @Test
  public void testShowQueriesNeedSetHighestPriority() {
    final Analysis analysis = planAndGetAnalysis("show queries");
    assertTrue(analysis.needSetHighestPriority());
  }

  @Test
  public void testInformationSchemaQueriesNeedSetHighestPriority() {
    final Analysis analysis = planAndGetAnalysis("select query_id from information_schema.queries");
    assertTrue(analysis.needSetHighestPriority());
  }

  @Test
  public void testNestedInformationSchemaQueriesNeedSetHighestPriority() {
    final Analysis analysis =
        planAndGetAnalysis(
            "select * from (select query_id from information_schema.queries) t "
                + "where t.query_id is not null");
    assertTrue(analysis.needSetHighestPriority());
  }

  private Analysis planAndGetAnalysis(final String sql) {
    PlanTester planTester = new PlanTester();
    planTester.createPlan(sql);
    return planTester.getAnalysis();
  }
}
