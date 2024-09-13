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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analyzer;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzerFactory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.scheduler.ClusterScheduler;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class TableModelPlanner implements IPlanner {

  private final Statement statement;

  private final SqlParser sqlParser;
  private final Metadata metadata;
  private final SymbolAllocator symbolAllocator = new SymbolAllocator();

  // TODO access control
  private final AccessControl accessControl = new NopAccessControl();

  private final WarningCollector warningCollector = WarningCollector.NOOP;

  private final ExecutorService executor;
  private final ExecutorService writeOperationExecutor;
  private final ScheduledExecutorService scheduledExecutor;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      syncInternalServiceClientManager;

  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      asyncInternalServiceClientManager;

  public TableModelPlanner(
      Statement statement,
      SqlParser sqlParser,
      Metadata metadata,
      ExecutorService executor,
      ExecutorService writeOperationExecutor,
      ScheduledExecutorService scheduledExecutor,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncInternalServiceClientManager,
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager) {
    this.statement = statement;
    this.sqlParser = sqlParser;
    this.metadata = metadata;
    this.executor = executor;
    this.writeOperationExecutor = writeOperationExecutor;
    this.scheduledExecutor = scheduledExecutor;
    this.syncInternalServiceClientManager = syncInternalServiceClientManager;
    this.asyncInternalServiceClientManager = asyncInternalServiceClientManager;
  }

  @Override
  public IAnalysis analyze(MPPQueryContext context) {
    StatementAnalyzerFactory statementAnalyzerFactory =
        new StatementAnalyzerFactory(metadata, sqlParser, accessControl);

    Analyzer analyzer =
        new Analyzer(
            context,
            context.getSession(),
            statementAnalyzerFactory,
            Collections.emptyList(),
            Collections.emptyMap(),
            warningCollector);
    return analyzer.analyze(statement);
  }

  @Override
  public LogicalQueryPlan doLogicalPlan(IAnalysis analysis, MPPQueryContext context) {
    return new TableLogicalPlanner(
            context, metadata, context.getSession(), symbolAllocator, warningCollector)
        .plan((Analysis) analysis);
  }

  @Override
  public DistributedQueryPlan doDistributionPlan(IAnalysis analysis, LogicalQueryPlan logicalPlan) {
    return new TableDistributedPlanner((Analysis) analysis, symbolAllocator, logicalPlan).plan();
  }

  @Override
  public IScheduler doSchedule(
      IAnalysis analysis,
      DistributedQueryPlan distributedPlan,
      MPPQueryContext context,
      QueryStateMachine stateMachine) {
    IScheduler scheduler =
        new ClusterScheduler(
            context,
            stateMachine,
            distributedPlan.getInstances(),
            context.getQueryType(),
            executor,
            writeOperationExecutor,
            scheduledExecutor,
            syncInternalServiceClientManager,
            asyncInternalServiceClientManager);
    scheduler.start();
    return scheduler;
  }

  @Override
  public void invalidatePartitionCache() {}

  @Override
  public ScheduledExecutorService getScheduledExecutorService() {
    return null;
  }

  @Override
  public void setRedirectInfo(
      IAnalysis analysis, TEndPoint localEndPoint, TSStatus tsstatus, TSStatusCode statusCode) {}

  private static class NopAccessControl implements AccessControl {}
}
