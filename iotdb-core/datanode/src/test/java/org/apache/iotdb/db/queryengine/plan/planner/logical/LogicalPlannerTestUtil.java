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

package org.apache.iotdb.db.queryengine.plan.planner.logical;

import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.Analyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.FakePartitionFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import java.time.ZoneId;
import java.time.ZonedDateTime;

/** This class generates logical plans for test cases of the read statements. */
public class LogicalPlannerTestUtil {

  public static PlanNode parseSQLToPlanNode(String sql) {
    Statement statement = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
    return analyzeStatementToPlanNode(statement);
  }

  public static PlanNode analyzeStatementToPlanNode(Statement statement) {
    MPPQueryContext context =
        new MPPQueryContext(
            "",
            new QueryId("test_query"),
            new SessionInfo(0, AuthorityChecker.SUPER_USER, ZoneId.systemDefault()),
            DataNodeEndPoints.LOCAL_HOST_DATA_BLOCK_ENDPOINT,
            DataNodeEndPoints.LOCAL_HOST_INTERNAL_ENDPOINT);
    Analyzer analyzer =
        new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
    Analysis analysis = analyzer.analyze(statement);
    return new LogicalPlanVisitor(analysis).process(analysis.getTreeStatement(), context);
  }
}
