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

package org.apache.iotdb.db.queryengine.plan;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** What the RPC that executes a statement is told when its query is removed while it starts. */
public class CoordinatorRemovedQueryTest {

  // A query id is never reused, as a terminated query releases itself asynchronously by its id
  private static final AtomicLong NEXT_QUERY_ID = new AtomicLong(20_001L);

  private static final long QUERY_TIMEOUT_IN_MS = 2_000L;
  // Coordinator.cleanUpStaleQueries spares a query until it exceeds its timeout by this long
  private static final long STALE_QUERY_GRACE_PERIOD_IN_MS = 60_000L;

  private static int previousDataNodeId;
  private static ExecutorService stateMachineExecutor;

  private final List<Long> executedQueryIds = new ArrayList<>();

  @BeforeClass
  public static void setUp() {
    // the coordinator builds its query id generator from the data node id of this node
    previousDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    stateMachineExecutor = Executors.newSingleThreadExecutor();
  }

  @AfterClass
  public static void tearDown() {
    stateMachineExecutor.shutdownNow();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
  }

  @After
  public void releaseExecutedQueries() {
    // a query that had to be left alone is still registered
    executedQueryIds.forEach(Coordinator.getInstance()::cleanupQueryExecution);
  }

  @Test
  public void testQueryReclaimedWhileItStartsIsReportedAsNotFound() throws Throwable {
    long queryId = nextQueryId();

    // The start of the query takes so long, for example in the dispatch of its fragment
    // instances, that the stale query cleanup reclaims the query. The start then succeeds.
    ExecutionResult result =
        startStatement(
            queryId,
            QueryType.READ,
            context -> {
              context.setStartTime(
                  System.currentTimeMillis()
                      - (QUERY_TIMEOUT_IN_MS + STALE_QUERY_GRACE_PERIOD_IN_MS + 1_000L));
              Coordinator.getInstance().cleanUpStaleQueries();
            });

    assertNull(Coordinator.getInstance().getQueryExecution(queryId));
    // nobody can fetch the result of this query, so the statement must not be reported as
    // successful
    assertEquals(TSStatusCode.QUERY_WAS_KILLED.getStatusCode(), result.status.getCode());
    assertEquals(
        DataNodeQueryMessages
            .EXCEPTION_QUERY_IS_NOT_FOUND_IT_MAY_BE_KILLED_BY_OTHERS_TIMEOUT_OR_SOME_OTHER_RUNTIME_ERRORS_YOU_CAN_SEE_MORE_DETAILS_IN_SERVER_LOG_90BD085E,
        result.status.getMessage());
  }

  @Test
  public void testQueryThatStaysRegisteredIsReportedAsSuccessful() throws Throwable {
    long queryId = nextQueryId();

    ExecutionResult result = startStatement(queryId, QueryType.READ, context -> {});

    assertNotNull(Coordinator.getInstance().getQueryExecution(queryId));
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.status.getCode());
  }

  @Test
  public void testStatementThatIsNotAQueryIsReportedAsSuccessful() throws Throwable {
    long queryId = nextQueryId();

    // only queries are registered, so a missing execution means nothing for any other statement
    ExecutionResult result = startStatement(queryId, QueryType.WRITE, context -> {});

    assertNull(Coordinator.getInstance().getQueryExecution(queryId));
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.status.getCode());
  }

  @Test
  public void testFailureOfQueryRemovedWhileItStartsIsKept() throws Throwable {
    long queryId = nextQueryId();
    TSStatus failure = RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "dispatch failed");
    IQueryExecution execution = mock(IQueryExecution.class);
    when(execution.isQuery()).thenReturn(true);
    when(execution.getQueryId()).thenReturn("removed_and_failed");
    doAnswer(
            invocation -> {
              Coordinator.getInstance().cleanupQueryExecution(queryId);
              return null;
            })
        .when(execution)
        .start();
    when(execution.getStatus())
        .thenReturn(new ExecutionResult(new QueryId("removed_and_failed"), failure));

    ExecutionResult result = execute(queryId, (queryContext, startTime) -> execution);

    assertNull(Coordinator.getInstance().getQueryExecution(queryId));
    // the reason of the failure tells more than "not found"
    assertEquals(failure, result.status);
  }

  private long nextQueryId() {
    long queryId = NEXT_QUERY_ID.getAndIncrement();
    executedQueryIds.add(queryId);
    return queryId;
  }

  /**
   * Executes a statement the way a client RPC does and returns what that RPC is told. The result of
   * the statement is already in memory, so no fragment instance is dispatched. {@code
   * whileItStarts} runs after the statement has been registered and before its start returns.
   */
  private static ExecutionResult startStatement(
      long queryId, QueryType queryType, Consumer<MPPQueryContext> whileItStarts) throws Throwable {
    IAnalysis analysis = mock(IAnalysis.class);
    when(analysis.canSkipExecute(any(MPPQueryContext.class)))
        .thenAnswer(
            invocation -> {
              whileItStarts.accept(invocation.getArgument(0));
              return true;
            });
    when(analysis.constructResultForMemorySource(any(MPPQueryContext.class)))
        .thenReturn(new TsBlock(0));
    IPlanner planner = mock(IPlanner.class);
    when(planner.analyze(any(MPPQueryContext.class))).thenReturn(analysis);
    return execute(
        queryId,
        (queryContext, startTime) -> {
          queryContext.setQueryType(queryType);
          queryContext.setTimeOut(QUERY_TIMEOUT_IN_MS);
          queryContext.setStartTime(startTime);
          return new QueryExecution(planner, queryContext, stateMachineExecutor);
        });
  }

  private static ExecutionResult execute(
      long queryId, BiFunction<MPPQueryContext, Long, IQueryExecution> queryExecutionFactory)
      throws Throwable {
    Method execution =
        Coordinator.class.getDeclaredMethod(
            "execution",
            long.class,
            SessionInfo.class,
            String.class,
            boolean.class,
            boolean.class,
            BiFunction.class);
    execution.setAccessible(true);
    try {
      return (ExecutionResult)
          execution.invoke(
              Coordinator.getInstance(),
              queryId,
              new SessionInfo(0, "test", ZoneId.systemDefault(), "test"),
              "SELECT s FROM t1",
              false,
              false,
              queryExecutionFactory);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
