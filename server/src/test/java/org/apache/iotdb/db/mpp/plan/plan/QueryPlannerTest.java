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

package org.apache.iotdb.db.mpp.plan.plan;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.plan.analyze.FakePartitionFetcherImpl;
import org.apache.iotdb.db.mpp.plan.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.mpp.plan.execution.QueryExecution;
import org.apache.iotdb.db.mpp.plan.parser.StatementGenerator;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.plan.statement.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.time.ZoneId;

public class QueryPlannerTest {

  private static IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      syncInternalServiceClientManager;

  private static IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      asyncInternalServiceClientManager;

  @BeforeClass
  public static void setUp() {
    syncInternalServiceClientManager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    asyncInternalServiceClientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory());
  }

  @AfterClass
  public static void destroy() {
    syncInternalServiceClientManager.close();
  }

  @Ignore
  @Test
  public void testSqlToDistributedPlan() {

    String querySql = "SELECT d1.*, d333.s1 FROM root.sg LIMIT 10";

    Statement stmt = StatementGenerator.createStatement(querySql, ZoneId.systemDefault());

    QueryExecution queryExecution =
        new QueryExecution(
            stmt,
            new MPPQueryContext(
                querySql,
                new QueryId("query1"),
                new SessionInfo(1L, "fakeUsername", "fakeZoneId"),
                new TEndPoint(),
                new TEndPoint()),
            IoTDBThreadPoolFactory.newSingleThreadExecutor("test_query"),
            IoTDBThreadPoolFactory.newSingleThreadExecutor("test_write_operation"),
            IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("test_query_scheduled"),
            new FakePartitionFetcherImpl(),
            new FakeSchemaFetcherImpl(),
            syncInternalServiceClientManager,
            asyncInternalServiceClientManager);
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
