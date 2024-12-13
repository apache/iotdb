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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.PlanMatchPattern.tableScan;

public class RowPatternRecognitionTest {
  @Test
  public void Test1() {
    PlanTester planTester = new PlanTester();

    String sql =
        "SELECT * "
            + "FROM table1 "
            + "MATCH_RECOGNIZE ( "
            + "  ORDER BY time "
            + "  MEASURES "
            + "    A.s1 AS col1 "
            + "  PATTERN (A B+) "
            + "  DEFINE "
            + "    B AS B.s2 > 5 "
            + ") AS m";

    LogicalQueryPlan logicalQueryPlan = planTester.createPlan(sql);

    PlanMatchPattern tableScan =
        tableScan(
            "testdb.table1",
            ImmutableList.of("time", "tag1", "tag2", "tag3", "attr1", "s1", "s2", "s3"),
            ImmutableSet.of("time", "tag1", "tag2", "tag3", "attr1", "s1", "s2", "s3"));

    // Verify full LogicalPlan
    // Output - PatternRecognition - TableScan
    // TODO:
    //  need to add PatternRecognitionMatcher
    //  assertPlan(logicalQueryPlan, output(anyTree(tableScan)));
  }
}
