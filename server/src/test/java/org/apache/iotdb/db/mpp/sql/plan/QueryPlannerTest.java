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

package org.apache.iotdb.db.mpp.sql.plan;

import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.execution.QueryExecution;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;
import org.apache.iotdb.db.mpp.sql.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.sql.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.sql.statement.Statement;

import org.junit.Ignore;
import org.junit.Test;

import java.time.ZoneId;

public class QueryPlannerTest {

  @Ignore
  @Test
  public void TestSqlToDistributedPlan() {

    String querySql = "SELECT d1.*, d333.s1 FROM root.sg LIMIT 10";

    Statement stmt = StatementGenerator.createStatement(querySql, ZoneId.systemDefault());

    QueryExecution queryExecution =
        new QueryExecution(
            stmt,
            new MPPQueryContext(
                querySql, new QueryId("query1"), new SessionInfo(), QueryType.READ));
    queryExecution.doLogicalPlan();
    System.out.printf("SQL: %s%n%n", querySql);
    System.out.println("===== Step 1: Logical Plan =====");
    System.out.println(PlanNodeUtil.nodeToString(queryExecution.getLogicalPlan().getRootNode()));

    queryExecution.doDistributedPlan();
    DistributedQueryPlan distributedQueryPlan = queryExecution.getDistributedPlan();

    System.out.println("===== Step 4: Split Fragment Instance =====");
    distributedQueryPlan.getInstances().forEach(System.out::println);
  }
}
