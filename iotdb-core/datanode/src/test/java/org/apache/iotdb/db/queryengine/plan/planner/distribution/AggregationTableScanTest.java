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

package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutorService;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.Analyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.FakePartitionFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;

import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
public class AggregationTableScanTest {

  @Test
  public void lastAggTest() {
    final String sql = null;
    DataNodeQueryContext dataNodeQueryContext = new DataNodeQueryContext(1);

    SessionInfo sessionInfo =
        new SessionInfo(
            0, "root", ZoneId.systemDefault(), "last_agg_db", IClientSession.SqlDialect.TABLE);
    QueryId queryId = new QueryId("test");
    MPPQueryContext context =
        new MPPQueryContext(
            sql,
            queryId,
            sessionInfo,
            new TEndPoint("127.0.0.1", 6667),
            new TEndPoint("127.0.0.1", 6667));
    Analyzer analyzer =
        new Analyzer(context, new FakePartitionFetcherImpl(), new FakeSchemaFetcherImpl());
    Statement statement = StatementGenerator.createStatement(sql, ZonedDateTime.now().getOffset());
    Analysis analysis = analyzer.analyze(statement);
    LogicalPlanner logicalPlanner = new LogicalPlanner(context);
    LogicalQueryPlan logicalPlan = logicalPlanner.plan(analysis);
    DistributionPlanner distributionPlanner = new DistributionPlanner(analysis, logicalPlan);
    FragmentInstance instance = distributionPlanner.planFragments().getInstances().get(0);

    LocalExecutionPlanner localExecutionPlanner = LocalExecutionPlanner.getInstance();
    localExecutionPlanner.plan(
        instance.getFragment().getPlanNodeTree(),
        instance.getFragment().getTypeProvider(),
        mockFIContext(queryId),
        dataNodeQueryContext);
  }

  private FragmentInstanceContext mockFIContext(QueryId queryId) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "last_agg-instance-notification");
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext instanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    IDataRegionForQuery dataRegionForQuery = Mockito.mock(DataRegion.class);
    instanceContext.setDataRegion(dataRegionForQuery);
    return instanceContext;
  }
}
