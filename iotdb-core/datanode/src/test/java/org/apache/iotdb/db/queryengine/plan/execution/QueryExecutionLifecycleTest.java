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

package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;

import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryExecutionLifecycleTest {

  @Test
  public void isActiveTellsWhetherAnRpcOfTheQueryIsInProgress() throws Exception {
    ExecutorService notifications = Executors.newSingleThreadExecutor();
    try {
      QueryExecution execution = new QueryExecution(planner(), context(), notifications);
      // the execution is created by the RPC that executes the statement
      assertTrue(execution.isActive());
      // that RPC returns, and the client has not asked for the result yet
      execution.recordExecutionTime(1);
      assertFalse(execution.isActive());
      // an RPC that fetches the result arrives
      execution.updateCurrentRpcStartTime(System.nanoTime());
      assertTrue(execution.isActive());
      // and returns as well
      execution.recordExecutionTime(1);
      assertFalse(execution.isActive());
    } finally {
      notifications.shutdownNow();
      assertTrue(notifications.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static IPlanner planner() {
    IPlanner planner = mock(IPlanner.class);
    when(planner.analyze(any())).thenReturn(mock(IAnalysis.class));
    return planner;
  }

  private static MPPQueryContext context() {
    MPPQueryContext context = new MPPQueryContext(new QueryId("is_active"));
    context.setQueryType(QueryType.READ);
    return context;
  }
}
