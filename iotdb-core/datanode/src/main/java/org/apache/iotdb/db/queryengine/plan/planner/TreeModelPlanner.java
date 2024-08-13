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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.Analyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.scheduler.ClusterScheduler;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.pipe.PipeEnrichedStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class TreeModelPlanner implements IPlanner {

  private final Statement statement;

  private final ExecutorService executor;
  private final ExecutorService writeOperationExecutor;
  private final ScheduledExecutorService scheduledExecutor;

  private final IPartitionFetcher partitionFetcher;

  private final ISchemaFetcher schemaFetcher;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      syncInternalServiceClientManager;

  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      asyncInternalServiceClientManager;

  public TreeModelPlanner(
      Statement statement,
      ExecutorService executor,
      ExecutorService writeOperationExecutor,
      ScheduledExecutorService scheduledExecutor,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncInternalServiceClientManager,
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager) {
    this.statement = statement;
    this.executor = executor;
    this.writeOperationExecutor = writeOperationExecutor;
    this.scheduledExecutor = scheduledExecutor;
    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;
    this.syncInternalServiceClientManager = syncInternalServiceClientManager;
    this.asyncInternalServiceClientManager = asyncInternalServiceClientManager;
  }

  @Override
  public IAnalysis analyze(MPPQueryContext context) {
    return new Analyzer(context, partitionFetcher, schemaFetcher).analyze(statement);
  }

  @Override
  public LogicalQueryPlan doLogicalPlan(IAnalysis analysis, MPPQueryContext context) {
    LogicalPlanner logicalPlanner = new LogicalPlanner(context);
    return logicalPlanner.plan((Analysis) analysis);
  }

  @Override
  public DistributedQueryPlan doDistributionPlan(IAnalysis analysis, LogicalQueryPlan logicalPlan) {
    DistributionPlanner planner = new DistributionPlanner((Analysis) analysis, logicalPlan);
    return planner.planFragments();
  }

  @Override
  public IScheduler doSchedule(
      IAnalysis analysis,
      DistributedQueryPlan distributedPlan,
      MPPQueryContext context,
      QueryStateMachine stateMachine) {
    IScheduler scheduler;

    boolean isPipeEnrichedTsFileLoad =
        statement instanceof PipeEnrichedStatement
            && ((PipeEnrichedStatement) statement).getInnerStatement()
                instanceof LoadTsFileStatement;
    if (statement instanceof LoadTsFileStatement || isPipeEnrichedTsFileLoad) {
      scheduler =
          new LoadTsFileScheduler(
              distributedPlan,
              context,
              stateMachine,
              syncInternalServiceClientManager,
              partitionFetcher,
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
  public void invalidatePartitionCache() {
    partitionFetcher.invalidAllCache();
  }

  @Override
  public ScheduledExecutorService getScheduledExecutorService() {
    return scheduledExecutor;
  }

  @Override
  public void setRedirectInfo(
      IAnalysis iAnalysis, TEndPoint localEndPoint, TSStatus tsstatus, TSStatusCode statusCode) {
    Analysis analysis = (Analysis) iAnalysis;

    // Get the inner statement of PipeEnrichedStatement
    Statement statementToRedirect =
        analysis.getTreeStatement() instanceof PipeEnrichedStatement
            ? ((PipeEnrichedStatement) analysis.getTreeStatement()).getInnerStatement()
            : analysis.getTreeStatement();

    if (statementToRedirect instanceof InsertBaseStatement
        && !analysis.isFinishQueryAfterAnalyze()) {
      InsertBaseStatement insertStatement = (InsertBaseStatement) statementToRedirect;
      List<TEndPoint> redirectNodeList = analysis.getRedirectNodeList();
      if (insertStatement instanceof InsertRowsStatement
          || insertStatement instanceof InsertMultiTabletsStatement) {
        // multiple devices
        if (statusCode == TSStatusCode.SUCCESS_STATUS) {
          boolean needRedirect = false;
          List<TSStatus> subStatus = new ArrayList<>();
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
      } else {
        // single device
        TEndPoint redirectEndPoint = redirectNodeList.get(0);
        // redirect writing only if the redirectEndPoint is not the current node
        if (!localEndPoint.equals(redirectEndPoint)) {
          tsstatus.setRedirectNode(redirectEndPoint);
        }
      }
    }
  }
}
