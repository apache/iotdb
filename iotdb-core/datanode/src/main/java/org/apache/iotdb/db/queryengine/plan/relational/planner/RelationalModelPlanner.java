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
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analyzer;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzerFactory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector.NOOP;

public class RelationalModelPlanner implements IPlanner {

  private Metadata metadata;

  @Override
  public IAnalysis analyze(MPPQueryContext context) {
    SessionInfo sessionInfo = null;
    StatementAnalyzerFactory statementAnalyzerFactory = null;
    Analyzer analyzer =
        new Analyzer(
            sessionInfo,
            statementAnalyzerFactory,
            Collections.emptyList(),
            Collections.emptyMap(),
            NOOP);
    return analyzer.analyze(null);
    // return null;
  }

  @Override
  public LogicalQueryPlan doLogicalPlan(IAnalysis analysis, MPPQueryContext context) {
    // TODO need implemented by Beyyes
      try {
          new LogicalPlanner(context, metadata, null, null).plan((Analysis)analysis);
      } catch (IoTDBException e) {
          throw new RuntimeException(e);
      }
      return null;
  }

  @Override
  public DistributedQueryPlan doDistributionPlan(IAnalysis analysis, LogicalQueryPlan logicalPlan) {
    // TODO
    return null;
  }

  @Override
  public IScheduler doSchedule(
      IAnalysis analysis,
      DistributedQueryPlan distributedPlan,
      MPPQueryContext context,
      QueryStateMachine stateMachine) {
    return null;
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
}
