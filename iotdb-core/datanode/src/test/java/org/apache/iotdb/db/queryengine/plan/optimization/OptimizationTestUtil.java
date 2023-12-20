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

package org.apache.iotdb.db.queryengine.plan.optimization;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.Analyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.FakePartitionFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;

import org.junit.Assert;

import java.time.ZonedDateTime;
import java.util.ArrayList;

public class OptimizationTestUtil {

  private OptimizationTestUtil() {
    // util class
  }

  public static void checkPushDown(
      PlanOptimizer optimizer, String sql, PlanNode rawPlan, PlanNode optPlan) {
    Statement statement = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());

    MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
    Analyzer analyzer =
        new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
    Analysis analysis = analyzer.analyze(statement);

    LogicalPlanner planner = new LogicalPlanner(context, new ArrayList<>());
    PlanNode actualPlan = planner.plan(analysis).getRootNode();
    Assert.assertEquals(rawPlan, actualPlan);

    PlanNode actualOptPlan = optimizer.optimize(actualPlan, analysis, context);
    Assert.assertEquals(optPlan, actualOptPlan);
  }

  public static void checkCannotPushDown(PlanOptimizer optimizer, String sql, PlanNode rawPlan) {
    Statement statement = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());

    MPPQueryContext context = new MPPQueryContext(new QueryId("test_query"));
    Analyzer analyzer =
        new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
    Analysis analysis = analyzer.analyze(statement);

    LogicalPlanner planner = new LogicalPlanner(context, new ArrayList<>());
    PlanNode actualPlan = planner.plan(analysis).getRootNode();

    Assert.assertEquals(rawPlan, actualPlan);
    Assert.assertEquals(actualPlan, optimizer.optimize(actualPlan, analysis, context));
  }
}
