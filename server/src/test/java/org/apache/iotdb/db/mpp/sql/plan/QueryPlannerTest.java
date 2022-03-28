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
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.junit.Test;

import java.time.ZoneId;

public class QueryPlannerTest {

  @Test
  public void TestSqlToDistributedPlan() {

    String querySql = "SELECT d1.*, d22.s1 FROM root.sg";

    Statement stmt = StatementGenerator.createStatement(querySql, ZoneId.systemDefault());
    System.out.println(stmt);

    QueryExecution queryExecution = new QueryExecution(stmt, new MPPQueryContext(querySql, new QueryId("query1"), new SessionInfo(), QueryType.READ));
    System.out.println(queryExecution);
  }
}
