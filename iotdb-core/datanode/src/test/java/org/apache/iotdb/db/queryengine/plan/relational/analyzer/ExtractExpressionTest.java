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

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.assertAnalyzeSemanticException;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanAssert.assertPlan;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.output;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;

public class ExtractExpressionTest {
  @Test
  public void pushDownTest() {
    PlanTester planTester = new PlanTester();

    assertPlan(
        planTester.createPlan("select * from table1 where extract(ms from time) > 5"),
        output(tableScan("testdb.table1")));
  }

  @Test
  public void exceptionTest() {
    String errMsg = "Invalid EXTRACT field: s";
    // Wrong extract field
    assertAnalyzeSemanticException("select * from table1 where extract(s from time) > 5", errMsg);

    errMsg = "Cannot extract from INT64";
    // Wrong extract input type
    assertAnalyzeSemanticException("select * from table1 where extract(ms from s1) > 5", errMsg);
  }
}
