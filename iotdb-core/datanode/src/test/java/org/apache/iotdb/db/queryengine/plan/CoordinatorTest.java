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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorTest {

  // A query id is never reused, as a terminated query releases itself asynchronously by its id
  private static final AtomicLong NEXT_QUERY_ID = new AtomicLong(10_001L);

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
  public void testStaleQueryWithoutRpcInProgressIsCleanedUp() throws Throwable {
    QueryExecution execution = executeQuery(QUERY_TIMEOUT_IN_MS);
    // the RPC that executed the statement returns, and the client never fetches the result
    finishRpc(execution);
    setElapsedTime(execution, QUERY_TIMEOUT_IN_MS + STALE_QUERY_GRACE_PERIOD_IN_MS + 1_000L);

    Coordinator.getInstance().cleanUpStaleQueries();

    assertNull(Coordinator.getInstance().getQueryExecution(queryIdOf(execution)));
  }

  @Test
  public void testStaleQueryWithRpcInProgressIsCleanedUp() throws Throwable {
    // the RPC that executed the statement has not returned yet, or its caller never reports that
    // it returned
    QueryExecution executing = executeQuery(QUERY_TIMEOUT_IN_MS);
    // that RPC returned, and a later RPC is fetching the result
    QueryExecution fetching = executeQuery(QUERY_TIMEOUT_IN_MS);
    finishRpc(fetching);
    fetching.updateCurrentRpcStartTime(System.nanoTime());
    long elapsedTime = QUERY_TIMEOUT_IN_MS + STALE_QUERY_GRACE_PERIOD_IN_MS + 1_000L;
    setElapsedTime(executing, elapsedTime);
    setElapsedTime(fetching, elapsedTime);

    Coordinator.getInstance().cleanUpStaleQueries();

    // an RPC still in progress that long after the timeout is stuck, and stopping the query is
    // what unblocks it
    for (QueryExecution execution : new QueryExecution[] {executing, fetching}) {
      assertNull(Coordinator.getInstance().getQueryExecution(queryIdOf(execution)));
    }
  }

  @Test
  public void testQueryWithRpcInProgressWithinGracePeriodIsLeftAlone() throws Throwable {
    QueryExecution execution = executeQuery(QUERY_TIMEOUT_IN_MS);
    // timed out, but not yet by more than the grace period
    setElapsedTime(execution, QUERY_TIMEOUT_IN_MS + STALE_QUERY_GRACE_PERIOD_IN_MS / 2);

    Coordinator.getInstance().cleanUpStaleQueries();

    assertLeftAlone(execution);
  }

  @Test
  public void testIdleQueryWithinGracePeriodIsLeftAlone() throws Throwable {
    QueryExecution execution = executeQuery(QUERY_TIMEOUT_IN_MS);
    finishRpc(execution);
    // timed out, but not yet by more than the grace period
    setElapsedTime(execution, QUERY_TIMEOUT_IN_MS + STALE_QUERY_GRACE_PERIOD_IN_MS / 2);

    Coordinator.getInstance().cleanUpStaleQueries();

    assertLeftAlone(execution);
  }

  @Test
  public void testIdleQueryWithoutPositiveTimeoutIsLeftAlone() throws Throwable {
    QueryExecution zeroTimeout = executeQuery(0L);
    QueryExecution negativeTimeout = executeQuery(-1L);
    finishRpc(zeroTimeout);
    finishRpc(negativeTimeout);
    setElapsedTime(zeroTimeout, 10 * STALE_QUERY_GRACE_PERIOD_IN_MS);
    setElapsedTime(negativeTimeout, 10 * STALE_QUERY_GRACE_PERIOD_IN_MS);

    Coordinator.getInstance().cleanUpStaleQueries();

    assertLeftAlone(zeroTimeout);
    assertLeftAlone(negativeTimeout);
  }

  @Test
  public void testKilledQueryIsReleasedUnlessAnRpcIsInProgress() throws Throwable {
    // the RPC that executed the statement has not returned yet
    QueryExecution executing = executeQuery(QUERY_TIMEOUT_IN_MS);
    // that RPC returned, and the client has not fetched the result yet
    QueryExecution idle = executeQuery(QUERY_TIMEOUT_IN_MS);
    finishRpc(idle);

    executing.cancel();
    idle.cancel();

    // the state machine thread answers getStatus after it has run the cleanup of the killed query
    assertEquals(TSStatusCode.QUERY_WAS_KILLED.getStatusCode(), statusCodeOf(executing));
    assertEquals(TSStatusCode.QUERY_WAS_KILLED.getStatusCode(), statusCodeOf(idle));
    // the RPC in progress releases its query when it returns, nothing would release the idle one
    assertSame(executing, Coordinator.getInstance().getQueryExecution(queryIdOf(executing)));
    assertNull(Coordinator.getInstance().getQueryExecution(queryIdOf(idle)));
  }

  /**
   * Executes a query the way a client RPC does. When this method returns the query is RUNNING and
   * registered in the coordinator, and the RPC that created it is still in progress. The result of
   * the query is already in memory, so no fragment instance is dispatched.
   */
  private QueryExecution executeQuery(long timeout) throws Throwable {
    long queryId = NEXT_QUERY_ID.getAndIncrement();
    executedQueryIds.add(queryId);
    IAnalysis analysis = mock(IAnalysis.class);
    when(analysis.canSkipExecute(any(MPPQueryContext.class))).thenReturn(true);
    when(analysis.constructResultForMemorySource(any(MPPQueryContext.class)))
        .thenReturn(new TsBlock(0));
    IPlanner planner = mock(IPlanner.class);
    when(planner.analyze(any(MPPQueryContext.class))).thenReturn(analysis);
    QueryExecution[] createdExecution = new QueryExecution[1];
    execute(
        queryId,
        "SELECT s FROM t1",
        (queryContext, startTime) -> {
          queryContext.setTimeOut(timeout);
          queryContext.setStartTime(startTime);
          createdExecution[0] = new QueryExecution(planner, queryContext, stateMachineExecutor);
          return createdExecution[0];
        });
    return createdExecution[0];
  }

  /** What the RPC in progress does when it returns to the client. */
  private static void finishRpc(QueryExecution execution) {
    Coordinator.getInstance().recordExecutionTime(queryIdOf(execution), 1L);
  }

  /** Makes the query look as if it had been started {@code elapsedTimeInMs} ago. */
  private static void setElapsedTime(QueryExecution execution, long elapsedTimeInMs) {
    execution.getContext().setStartTime(System.currentTimeMillis() - elapsedTimeInMs);
  }

  private static long queryIdOf(QueryExecution execution) {
    return execution.getContext().getLocalQueryId();
  }

  private static int statusCodeOf(QueryExecution execution) {
    return execution.getStatus().status.getCode();
  }

  private static void assertLeftAlone(QueryExecution execution) {
    assertSame(execution, Coordinator.getInstance().getQueryExecution(queryIdOf(execution)));
    // still running, so its result can still be fetched
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), statusCodeOf(execution));
  }

  private static void execute(
      long queryId,
      String sql,
      BiFunction<MPPQueryContext, Long, IQueryExecution> queryExecutionFactory)
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
      execution.invoke(
          Coordinator.getInstance(),
          queryId,
          new SessionInfo(0, "test", ZoneId.systemDefault(), "test"),
          sql,
          false,
          false,
          queryExecutionFactory);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
