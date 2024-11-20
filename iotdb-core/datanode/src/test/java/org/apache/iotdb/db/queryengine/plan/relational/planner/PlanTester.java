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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analyzer;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzerFactory;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TSBSMetadata;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMatadata;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector.NOOP;
import static org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static org.junit.Assert.fail;

public class PlanTester {
  private final QueryId queryId = new QueryId("test_query");
  private final SessionInfo sessionInfo =
      new SessionInfo(
          1L,
          "iotdb-user",
          ZoneId.systemDefault(),
          IoTDBConstant.ClientVersion.V_1_0,
          "db",
          IClientSession.SqlDialect.TABLE);
  private final Metadata metadata;

  private DistributedQueryPlan distributedQueryPlan;

  private Analysis analysis;

  private SymbolAllocator symbolAllocator;

  private LogicalQueryPlan plan;

  public PlanTester() {
    this(new TestMatadata());
  }

  public PlanTester(Metadata metadata) {
    this.metadata = metadata;
  }

  public LogicalQueryPlan createPlan(String sql) {
    return createPlan(sessionInfo, sql, NOOP, createPlanOptimizersStatsCollector());
  }

  public LogicalQueryPlan createPlan(SessionInfo sessionInfo, String sql) {
    return createPlan(sessionInfo, sql, NOOP, createPlanOptimizersStatsCollector());
  }

  public LogicalQueryPlan createPlan(
      SessionInfo sessionInfo,
      String sql,
      WarningCollector warningCollector,
      PlanOptimizersStatsCollector planOptimizersStatsCollector) {
    distributedQueryPlan = null;
    MPPQueryContext context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);

    Analysis analysis = analyze(sql, metadata);
    this.analysis = analysis;
    this.symbolAllocator = new SymbolAllocator();

    TableLogicalPlanner logicalPlanner =
        new TableLogicalPlanner(
            context, metadata, sessionInfo, symbolAllocator, WarningCollector.NOOP);

    plan = logicalPlanner.plan(analysis);

    return plan;
  }

  public LogicalQueryPlan createPlan(
      SessionInfo sessionInfo,
      String sql,
      List<PlanOptimizer> optimizers,
      WarningCollector warningCollector,
      PlanOptimizersStatsCollector planOptimizersStatsCollector) {
    distributedQueryPlan = null;
    MPPQueryContext context = new MPPQueryContext(sql, queryId, sessionInfo, null, null);

    Analysis analysis = analyze(sql, metadata);

    TableLogicalPlanner logicalPlanner =
        new TableLogicalPlanner(
            context, metadata, sessionInfo, symbolAllocator, WarningCollector.NOOP, optimizers);

    return logicalPlanner.plan(analysis);
  }

  public static Analysis analyze(String sql, Metadata metadata) {
    SqlParser sqlParser = new SqlParser();
    Statement statement = sqlParser.createStatement(sql, ZoneId.systemDefault());
    String databaseName;
    if (metadata instanceof TSBSMetadata) {
      databaseName = "tsbs";
    } else {
      databaseName = "testdb";
    }
    SessionInfo session =
        new SessionInfo(
            0, "test", ZoneId.systemDefault(), databaseName, IClientSession.SqlDialect.TABLE);
    final MPPQueryContext context =
        new MPPQueryContext(sql, new QueryId("test_query"), session, null, null);
    return analyzeStatement(statement, metadata, context, sqlParser, session);
  }

  public static Analysis analyzeStatement(
      Statement statement,
      Metadata metadata,
      MPPQueryContext context,
      SqlParser sqlParser,
      SessionInfo session) {
    try {
      StatementAnalyzerFactory statementAnalyzerFactory =
          new StatementAnalyzerFactory(metadata, sqlParser, new NopAccessControl());

      Analyzer analyzer =
          new Analyzer(
              context,
              session,
              statementAnalyzerFactory,
              Collections.emptyList(),
              Collections.emptyMap(),
              NOOP);
      return analyzer.analyze(statement);
    } catch (Exception e) {
      e.printStackTrace();
      fail(statement + ", " + e.getMessage());
    }
    fail();
    return null;
  }

  public PlanNode getFragmentPlan(int index) {
    if (distributedQueryPlan == null) {
      distributedQueryPlan =
          new TableDistributedPlanner(analysis, symbolAllocator, plan, metadata).plan();
    }
    return distributedQueryPlan.getFragments().get(index).getPlanNodeTree().getChildren().get(0);
  }

  private static class NopAccessControl implements AccessControl {}
}
