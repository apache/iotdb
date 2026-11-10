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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.scheduler.ClusterScheduler;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class IoTDBInternalLocalReporterTest {

  private static int originalDataNodeId;

  private final Logger logger = (Logger) LoggerFactory.getLogger(IoTDBInternalLocalReporter.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private final Coordinator coordinator = mock(Coordinator.class);
  private final SessionInfo sessionInfo = mock(SessionInfo.class);
  private final IPartitionFetcher partitionFetcher = mock(IPartitionFetcher.class);
  private final ISchemaFetcher schemaFetcher = mock(ISchemaFetcher.class);
  private ScheduledExecutorService service;
  private IoTDBInternalLocalReporter reporter;
  private Level originalLevel;
  private boolean originalStopping;

  @BeforeClass
  public static void initializeDataNodeId() {
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @AfterClass
  public static void restoreDataNodeId() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
  }

  @Before
  public void setUp() {
    originalLevel = logger.getLevel();
    logger.setLevel(Level.DEBUG);
    appender.start();
    logger.addAppender(appender);
    originalStopping = CommonDescriptor.getInstance().getConfig().isStopping();
    CommonDescriptor.getInstance().getConfig().setStopping(false);
    service = mock(ScheduledExecutorService.class);
    doAnswer(
            invocation -> {
              invocation.<Runnable>getArgument(0).run();
              return null;
            })
        .when(service)
        .execute(any(Runnable.class));
    reporter = newReporter(service);
  }

  @After
  public void tearDown() throws Exception {
    // Some tests intentionally restore the interrupted flag while returning a failed status.
    Thread.interrupted();
    reporter.stop();
    service.awaitTermination(5, TimeUnit.SECONDS);
    CommonDescriptor.getInstance().getConfig().setStopping(originalStopping);
    logger.detachAppender(appender);
    appender.stop();
    logger.setLevel(originalLevel);
  }

  @Test
  public void testStoppedReporterRejectsNewWritesAndRestart() {
    reporter.stop();

    writeMetric();

    assertFalse(reporter.start());
    verify(service, never()).execute(any(Runnable.class));
    verifyZeroInteractions(coordinator);
    assertFalse(hasWarning());
  }

  @Test
  public void testNodeShutdownRejectsNewWrites() {
    CommonDescriptor.getInstance().getConfig().setStopping(true);

    writeMetric();

    verify(service, never()).execute(any(Runnable.class));
    verifyZeroInteractions(coordinator);
    assertFalse(hasWarning());
  }

  @Test
  public void testQueuedWriteDoesNotRunAfterStop() {
    doNothing().when(service).execute(any(Runnable.class));
    writeMetric();
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    verify(service).execute(task.capture());

    reporter.stop();
    task.getValue().run();

    verifyZeroInteractions(coordinator);
    assertFalse(hasWarning());
  }

  @Test
  public void testStopBetweenCheckAndSubmission() {
    doAnswer(
            invocation -> {
              reporter.stop();
              throw new RejectedExecutionException("executor stopped");
            })
        .when(service)
        .execute(any(Runnable.class));

    writeMetric();

    verifyZeroInteractions(coordinator);
    assertFalse(hasWarning());
  }

  @Test(expected = RejectedExecutionException.class)
  public void testUnexpectedSubmissionFailureIsPropagated() {
    doThrow(new RejectedExecutionException("unexpected rejection"))
        .when(service)
        .execute(any(Runnable.class));

    writeMetric();
  }

  @Test
  public void testMissingTimeseriesIsCreatedAndWriteRetried() {
    execution()
        .thenReturn(
            result(TSStatusCode.METADATA_ERROR),
            result(TSStatusCode.SUCCESS_STATUS),
            result(TSStatusCode.SUCCESS_STATUS));

    writeMetric();

    List<Statement> statements = executedStatements(3);
    assertTrue(statements.get(0) instanceof InsertRowStatement);
    assertTrue(statements.get(1) instanceof CreateMultiTimeSeriesStatement);
    assertTrue(statements.get(2) instanceof InsertRowStatement);
    assertFalse(hasWarning());
  }

  @Test
  public void testStopAfterInsertDoesNotCreateTimeseries() {
    execution()
        .thenAnswer(
            invocation -> {
              reporter.stop();
              Thread.currentThread().interrupt();
              return result(TSStatusCode.METADATA_ERROR);
            });

    writeMetric();

    executedStatements(1);
    assertTrue(Thread.currentThread().isInterrupted());
    assertTrue(hasWarning());
  }

  @Test
  public void testStopInterruptsAutoCreateAndPreventsRetry() throws Exception {
    verifyReloadInterrupt(null, null, true);
  }

  @Test
  public void testNodeShutdownReadOnlyFailureDoesNotWarn() {
    execution()
        .thenAnswer(
            invocation -> {
              CommonDescriptor.getInstance().getConfig().setStopping(true);
              return result(TSStatusCode.SYSTEM_READ_ONLY);
            });

    writeMetric();

    executedStatements(1);
    assertFalse(hasWarning());
  }

  @Test
  public void testReadOnlyFailureWithoutNodeShutdownStillWarns() {
    execution().thenReturn(result(TSStatusCode.SYSTEM_READ_ONLY));

    writeMetric();

    assertTrue(hasWarning());
  }

  @Test
  public void testInterruptedWaitWithoutShutdownStillWarns() throws Exception {
    QueryExecution queryExecution =
        queryExecutionWithDelayedStatus(null, null, new CountDownLatch(0));
    execution()
        .thenAnswer(
            invocation -> {
              Thread.currentThread().interrupt();
              return queryExecution.getStatus();
            });

    writeMetric();

    assertTrue(hasWarning());
  }

  @Test
  public void testInternalErrorDuringStopWithoutInterruptionStillWarns() {
    execution()
        .thenAnswer(
            invocation -> {
              reporter.stop();
              return result(TSStatusCode.INTERNAL_SERVER_ERROR);
            });

    writeMetric();

    assertTrue(hasWarning());
  }

  @Test
  public void testAutoCreateFailureWithoutInterruptionStillWarnsDuringStop() {
    execution()
        .thenReturn(result(TSStatusCode.METADATA_ERROR))
        .thenAnswer(
            invocation -> {
              reporter.stop();
              return result(TSStatusCode.INTERNAL_SERVER_ERROR);
            });

    writeMetric();

    executedStatements(2);
    assertTrue(hasWarning());
  }

  @Test
  public void testReporterReloadDoesNotHideReadOnlyFailure() {
    execution()
        .thenAnswer(
            invocation -> {
              reporter.stop();
              return result(TSStatusCode.SYSTEM_READ_ONLY);
            });

    writeMetric();

    assertTrue(hasWarning());
  }

  @Test
  public void testReloadInterruptPreservesRecordedReadOnlyFailure() throws Exception {
    verifyReloadInterrupt(new TSStatus(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()), null, false);
  }

  @Test
  public void testReloadInterruptPreservesRecordedWriteFailure() throws Exception {
    verifyReloadInterrupt(
        new TSStatus(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode()), null, false);
  }

  @Test
  public void testReloadInterruptPreservesRecordedInternalFailure() throws Exception {
    verifyReloadInterrupt(
        new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()), null, false);
  }

  @Test
  public void testReloadInterruptPreservesRecordedException() throws Exception {
    verifyReloadInterrupt(null, new IllegalStateException("existing write failure"), false);
  }

  @Test
  public void testReloadInterruptPreservesRecordedAutoCreateFailure() throws Exception {
    verifyReloadInterrupt(
        new TSStatus(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode()), null, true);
  }

  @Test
  public void testReloadWithOnlyInterruptedWaitDoesNotWarn() throws Exception {
    verifyReloadInterrupt(null, null, false);
  }

  @Test
  public void testRemoteWriteInterruptedDuringReloadDoesNotWarn() throws Exception {
    verifyRemoteDispatchInterruption(false, false, null);
  }

  @Test
  public void testRemoteAutoCreateInterruptedDuringReloadDoesNotWarn() throws Exception {
    verifyRemoteDispatchInterruption(true, false, null);
  }

  @Test
  public void testRemoteInterruptionSurvivesClearedThreadFlag() throws Exception {
    verifyRemoteDispatchInterruption(false, true, null);
  }

  @Test
  public void testRemoteInterruptionDoesNotReplaceExistingFailure() throws Exception {
    verifyRemoteDispatchInterruption(
        false, false, new TSStatus(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode()));
  }

  @Test
  public void testRemoteInterruptionDoesNotHideExistingException() throws Exception {
    verifyRemoteDispatchInterruption(
        false, false, null, new IllegalStateException("existing write failure"));
  }

  private void verifyRemoteDispatchInterruption(
      boolean autoCreate, boolean clearInterrupt, TSStatus recordedFailure) throws Exception {
    verifyRemoteDispatchInterruption(autoCreate, clearInterrupt, recordedFailure, null);
  }

  private void verifyRemoteDispatchInterruption(
      boolean autoCreate,
      boolean clearInterrupt,
      TSStatus recordedFailure,
      Throwable recordedException)
      throws Exception {
    service = Executors.newSingleThreadScheduledExecutor();
    reporter = newReporter(service);
    MPPQueryContext context = mock(MPPQueryContext.class);
    when(context.getQueryId()).thenReturn(new QueryId("remote_metric"));
    IPlanner planner = mock(IPlanner.class);
    when(planner.analyze(context)).thenReturn(mock(IAnalysis.class));
    ExecutorService notifications = mock(ExecutorService.class);
    QueryExecution queryExecution = new QueryExecution(planner, context, notifications);
    QueryStateMachine stateMachine = stateMachine(queryExecution);
    if (recordedFailure != null) {
      stateMachine.transitionToFailed(recordedFailure);
    } else if (recordedException != null) {
      stateMachine.transitionToFailed(recordedException);
    } else {
      stateMachine.transitionToPlanned();
    }

    TEndPoint endpoint = new TEndPoint("192.0.2.1", 10730);
    TDataNodeLocation remote = new TDataNodeLocation().setInternalEndPoint(endpoint);
    TRegionReplicaSet region =
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
            Collections.singletonList(remote));
    PlanNode node = mock(PlanNode.class);
    when(node.serializeToByteBuffer()).thenReturn(ByteBuffer.allocate(0));
    when(node.getChildren()).thenReturn(Collections.emptyList());
    PlanFragment fragment = mock(PlanFragment.class);
    when(fragment.getPlanNodeTree()).thenReturn(node);
    FragmentInstance instance = mock(FragmentInstance.class);
    when(instance.getHostDataNode()).thenReturn(remote);
    when(instance.getRegionReplicaSet()).thenReturn(region);
    when(instance.getFragment()).thenReturn(fragment);
    DistributedQueryPlan plan = mock(DistributedQueryPlan.class);
    when(plan.getInstances()).thenReturn(Collections.singletonList(instance));

    @SuppressWarnings("unchecked")
    IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clients =
        mock(IClientManager.class);
    AsyncDataNodeInternalServiceClient client = mock(AsyncDataNodeInternalServiceClient.class);
    when(clients.borrowClient(endpoint)).thenReturn(client);
    CountDownLatch dispatched = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              // Leave the RPC response pending so the real AsyncPlanNodeSender waits.
              dispatched.countDown();
              return null;
            })
        .when(client)
        .sendBatchPlanNode(any(TSendBatchPlanNodeReq.class), any());
    ClusterScheduler scheduler =
        new ClusterScheduler(context, stateMachine, plan, QueryType.WRITE, null, null, clients);
    AtomicReference<ExecutionResult> returnedResult = new AtomicReference<>();
    OngoingStubbing<ExecutionResult> execution = execution();
    if (autoCreate) {
      execution = execution.thenReturn(result(TSStatusCode.METADATA_ERROR));
    }
    execution.thenAnswer(
        invocation -> {
          scheduler.start();
          if (clearInterrupt) {
            Thread.interrupted();
          }
          // Deliver the getStatus() listener immediately, also exercising its normal return
          // path when the interrupt flag has been consumed after dispatch.
          doAnswer(
                  notification -> {
                    notification.<Runnable>getArgument(0).run();
                    return null;
                  })
              .when(notifications)
              .execute(any(Runnable.class));
          ExecutionResult result = queryExecution.getStatus();
          returnedResult.set(result);
          return result;
        });

    writeMetric();
    assertTrue(dispatched.await(5, TimeUnit.SECONDS));
    reporter.stop();
    assertTrue(service.awaitTermination(5, TimeUnit.SECONDS));

    ExecutionResult result = returnedResult.get();
    if (recordedFailure != null) {
      assertSame(recordedFailure, result.status);
    } else {
      assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), result.status.getCode());
    }
    boolean hasRecordedFailure = recordedFailure != null || recordedException != null;
    assertEquals(hasRecordedFailure, hasWarning());
    assertEquals(!hasRecordedFailure, result.failureCausedByInterruption);
    executedStatements(autoCreate ? 2 : 1);
    verify(client).sendBatchPlanNode(any(TSendBatchPlanNodeReq.class), any());
  }

  private void verifyReloadInterrupt(
      TSStatus recordedFailure, Throwable recordedException, boolean autoCreate) throws Exception {
    service = Executors.newSingleThreadScheduledExecutor();
    reporter = newReporter(service);
    CountDownLatch waitingForStatus = new CountDownLatch(1);
    QueryExecution queryExecution =
        queryExecutionWithDelayedStatus(recordedFailure, recordedException, waitingForStatus);
    AtomicReference<ExecutionResult> returnedResult = new AtomicReference<>();
    AtomicBoolean interrupted = new AtomicBoolean();
    OngoingStubbing<ExecutionResult> execution = execution();
    if (autoCreate) {
      execution = execution.thenReturn(result(TSStatusCode.METADATA_ERROR));
    }
    execution.thenAnswer(
        invocation -> {
          ExecutionResult result = queryExecution.getStatus();
          returnedResult.set(result);
          interrupted.set(Thread.currentThread().isInterrupted());
          return result;
        });

    writeMetric();
    assertTrue(waitingForStatus.await(5, TimeUnit.SECONDS));
    // The node remains running: stop() represents replacing just the reporter on reload.
    reporter.stop();
    assertTrue(service.awaitTermination(5, TimeUnit.SECONDS));

    assertTrue(interrupted.get());
    ExecutionResult result = returnedResult.get();
    if (recordedFailure != null) {
      assertSame(recordedFailure, result.status);
    } else {
      assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), result.status.getCode());
      if (recordedException != null) {
        assertEquals(recordedException.getMessage(), result.status.getMessage());
      }
    }
    executedStatements(autoCreate ? 2 : 1);
    assertEquals(
        recordedFailure == null && recordedException == null, result.failureCausedByInterruption);
    assertEquals(recordedFailure != null || recordedException != null, hasWarning());
  }

  private QueryExecution queryExecutionWithDelayedStatus(
      TSStatus recordedFailure, Throwable recordedException, CountDownLatch waitingForStatus)
      throws Exception {
    MPPQueryContext context = mock(MPPQueryContext.class);
    when(context.getQueryId()).thenReturn(new QueryId("metric_reload"));
    IPlanner planner = mock(IPlanner.class);
    when(planner.analyze(context)).thenReturn(mock(IAnalysis.class));
    ExecutorService notifications = mock(ExecutorService.class);
    QueryExecution execution = new QueryExecution(planner, context, notifications);
    QueryStateMachine stateMachine = stateMachine(execution);
    if (recordedFailure != null) {
      stateMachine.transitionToFailed(recordedFailure);
    } else if (recordedException != null) {
      stateMachine.transitionToFailed(recordedException);
    }
    // Delay listener delivery as with a busy notification executor. The real getStatus() waits
    // until shutdownNow() interrupts it, even when the state machine already holds a failure.
    doAnswer(
            invocation -> {
              waitingForStatus.countDown();
              return null;
            })
        .when(notifications)
        .execute(any(Runnable.class));
    return execution;
  }

  private QueryStateMachine stateMachine(QueryExecution execution) throws Exception {
    Field stateMachineField = QueryExecution.class.getDeclaredField("stateMachine");
    stateMachineField.setAccessible(true);
    return (QueryStateMachine) stateMachineField.get(execution);
  }

  private IoTDBInternalLocalReporter newReporter(ScheduledExecutorService executor) {
    return new IoTDBInternalLocalReporter(
        executor, coordinator, sessionInfo, partitionFetcher, schemaFetcher);
  }

  private OngoingStubbing<ExecutionResult> execution() {
    return when(
        coordinator.executeForTreeModel(
            any(Statement.class),
            anyLong(),
            same(sessionInfo),
            eq(""),
            same(partitionFetcher),
            same(schemaFetcher)));
  }

  private ExecutionResult result(TSStatusCode code) {
    return new ExecutionResult(new QueryId("metric_test"), new TSStatus(code.getStatusCode()));
  }

  private void writeMetric() {
    reporter.writeMetricToIoTDB(
        Collections.singletonMap("value", 1L), "root.__system.metric.test", 1L);
  }

  private List<Statement> executedStatements(int count) {
    ArgumentCaptor<Statement> statement = ArgumentCaptor.forClass(Statement.class);
    verify(coordinator, times(count))
        .executeForTreeModel(
            statement.capture(),
            anyLong(),
            same(sessionInfo),
            eq(""),
            same(partitionFetcher),
            same(schemaFetcher));
    return statement.getAllValues();
  }

  private boolean hasWarning() {
    return appender.list.stream().anyMatch(event -> event.getLevel().isGreaterOrEqual(Level.WARN));
  }
}
