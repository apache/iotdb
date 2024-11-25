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
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analyzer;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzerFactory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.scheduler.ClusterScheduler;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class TableModelPlanner implements IPlanner {

  private final Statement statement;

  private final SqlParser sqlParser;
  private final Metadata metadata;
  private final List<PlanOptimizer> logicalPlanOptimizers;
  private final List<PlanOptimizer> distributionPlanOptimizers;
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
      final Statement statement,
      final SqlParser sqlParser,
      final Metadata metadata,
      final ExecutorService executor,
      final ExecutorService writeOperationExecutor,
      final ScheduledExecutorService scheduledExecutor,
      final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
          syncInternalServiceClientManager,
      final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager,
      final List<PlanOptimizer> logicalPlanOptimizers,
      final List<PlanOptimizer> distributionPlanOptimizers) {
    this.statement = statement;
    this.sqlParser = sqlParser;
    this.metadata = metadata;
    this.executor = executor;
    this.writeOperationExecutor = writeOperationExecutor;
    this.scheduledExecutor = scheduledExecutor;
    this.syncInternalServiceClientManager = syncInternalServiceClientManager;
    this.asyncInternalServiceClientManager = asyncInternalServiceClientManager;
    this.logicalPlanOptimizers = logicalPlanOptimizers;
    this.distributionPlanOptimizers = distributionPlanOptimizers;
  }

  @Override
  public IAnalysis analyze(final MPPQueryContext context) {
    return new Analyzer(
            context,
            context.getSession(),
            new StatementAnalyzerFactory(metadata, sqlParser, accessControl),
            Collections.emptyList(),
            Collections.emptyMap(),
            warningCollector)
        .analyze(statement);
  }

  @Override
  public LogicalQueryPlan doLogicalPlan(final IAnalysis analysis, final MPPQueryContext context) {
    return new TableLogicalPlanner(
            context,
            metadata,
            context.getSession(),
            symbolAllocator,
            warningCollector,
            logicalPlanOptimizers)
        .plan((Analysis) analysis);
  }

  @Override
  public DistributedQueryPlan doDistributionPlan(
      final IAnalysis analysis, final LogicalQueryPlan logicalPlan) {
    return new TableDistributedPlanner(
            (Analysis) analysis, symbolAllocator, logicalPlan, metadata, distributionPlanOptimizers)
        .plan();
  }

  @Override
  public IScheduler doSchedule(
      final IAnalysis analysis,
      final DistributedQueryPlan distributedPlan,
      final MPPQueryContext context,
      final QueryStateMachine stateMachine) {
    final IScheduler scheduler;

    final boolean isPipeEnrichedTsFileLoad =
        statement instanceof PipeEnriched
            && ((PipeEnriched) statement).getInnerStatement() instanceof LoadTsFile;
    if (statement instanceof LoadTsFile || isPipeEnrichedTsFileLoad) {
      scheduler =
          new LoadTsFileScheduler(
              distributedPlan,
              context,
              stateMachine,
              syncInternalServiceClientManager,
              ClusterPartitionFetcher.getInstance(),
              isPipeEnrichedTsFileLoad);
    } else {
      scheduler =
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
    }
    scheduler.start();
    return scheduler;
  }

  @Override
  public void invalidatePartitionCache() {}

  @Override
  public ScheduledExecutorService getScheduledExecutorService() {
    return scheduledExecutor;
  }

  @Override
  public void setRedirectInfo(
      IAnalysis iAnalysis, TEndPoint localEndPoint, TSStatus tsstatus, TSStatusCode statusCode) {
    Analysis analysis = (Analysis) iAnalysis;

    // Get the inner statement of PipeEnriched
    Statement statementToRedirect =
        analysis.getStatement() instanceof PipeEnriched
            ? ((PipeEnriched) analysis.getStatement()).getInnerStatement()
            : analysis.getStatement();

    if (!(statementToRedirect instanceof WrappedInsertStatement)) {
      return;
    }
    InsertBaseStatement insertStatement =
        ((WrappedInsertStatement) statementToRedirect).getInnerTreeStatement();

    if (!analysis.isFinishQueryAfterAnalyze()) {
      // Table Model Session only supports insertTablet
      if (insertStatement instanceof InsertTabletStatement) {
        if (statusCode == TSStatusCode.SUCCESS_STATUS) {
          boolean needRedirect = false;
          List<TEndPoint> redirectNodeList = analysis.getRedirectNodeList();
          List<TSStatus> subStatus = new ArrayList<>(redirectNodeList.size());
          for (TEndPoint endPoint : redirectNodeList) {
            // redirect writing only if the redirectEndPoint is not the current node
            if (!localEndPoint.equals(endPoint)) {
              subStatus.add(
                  RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS).setRedirectNode(endPoint));
              needRedirect = true;
            } else {
              subStatus.add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            }
          }
          if (needRedirect) {
            tsstatus.setCode(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
            tsstatus.setSubStatus(subStatus);
          }
        }
      }
    }
  }

  public static class NopAccessControl implements AccessControl {}
}
