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

package org.apache.iotdb.db.queryengine.execution.driver;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public class DriverContextTest {

  @Test
  public void testConcurrentFailedWaitsUntilFailureCauseRecordedBeforeLocalSinkAbort()
      throws Exception {
    QueryId queryId = new QueryId("driver_context_concurrent_failed_test");
    FragmentInstanceId instanceId = new FragmentInstanceId(new PlanFragmentId(queryId, 1), "0");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, directExecutor());
    FragmentInstanceContext spyContext =
        Mockito.spy(
            FragmentInstanceContext.createFragmentInstanceContext(instanceId, stateMachine));

    CountDownLatch enteredContextFailed = new CountDownLatch(1);
    CountDownLatch allowRecordFailure = new CountDownLatch(1);
    Mockito.doAnswer(
            invocation -> {
              enteredContextFailed.countDown();
              Assert.assertTrue(
                  "Timed out waiting to release failure recording",
                  allowRecordFailure.await(5, TimeUnit.SECONDS));
              return invocation.callRealMethod();
            })
        .when(spyContext)
        .failed(Mockito.any());

    DriverContext driverContext = new DriverContext(spyContext, 0);
    LocalExchangePair exchange = createLocalExchange(instanceId.toThrift(), spyContext);
    ListenableFuture<?> blocked = exchange.sourceHandle.isBlocked();
    Assert.assertFalse(blocked.isDone());

    SemanticException causeA =
        new SemanticException("The second and third argument must be greater than 0");
    SemanticException causeB = new SemanticException("Cascading abort cause");
    AtomicReference<Throwable> threadError = new AtomicReference<>();

    Thread driverThread =
        new Thread(
            () -> {
              try {
                driverContext.failed(causeA);
              } catch (Throwable t) {
                threadError.compareAndSet(null, t);
              }
            },
            "test-driver-failed-thread");

    CountDownLatch aborterStarted = new CountDownLatch(1);
    Thread aborterThread =
        new Thread(
            () -> {
              try {
                aborterStarted.countDown();
                // Mirrors DriverScheduler.clearDriverTask: driver.failed(...) then abort sink.
                driverContext.failed(causeB);
                exchange.sinkChannel.abort();
              } catch (Throwable t) {
                threadError.compareAndSet(null, t);
              }
            },
            "test-aborter-thread");

    driverThread.start();
    Assert.assertTrue(
        "Driver thread did not enter FragmentInstanceContext.failed",
        enteredContextFailed.await(5, TimeUnit.SECONDS));

    aborterThread.start();
    Assert.assertTrue(aborterStarted.await(5, TimeUnit.SECONDS));

    long waitDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (aborterThread.getState() != Thread.State.BLOCKED
        && aborterThread.isAlive()
        && System.nanoTime() < waitDeadline) {
      Thread.yield();
    }
    // Aborter must wait for the in-flight failed() call instead of aborting the sink with no cause.
    Assert.assertEquals(Thread.State.BLOCKED, aborterThread.getState());
    Assert.assertFalse(exchange.sinkChannel.isAborted());
    Assert.assertFalse(blocked.isDone());

    allowRecordFailure.countDown();
    driverThread.join(5000);
    aborterThread.join(5000);
    Assert.assertFalse(driverThread.isAlive());
    Assert.assertFalse(aborterThread.isAlive());
    Assert.assertNull(threadError.get());

    Assert.assertTrue(driverContext.isDone());
    Assert.assertEquals(Optional.of(causeA), spyContext.getFailureCause());
    Assert.assertEquals(1, stateMachine.getFailureCauses().size());
    Assert.assertTrue(exchange.sinkChannel.isAborted());

    try {
      blocked.get(5, TimeUnit.SECONDS);
      Assert.fail("Expected ExecutionException carrying SemanticException");
    } catch (ExecutionException e) {
      Assert.assertSame(causeA, e.getCause());
    }
  }

  @Test
  public void testFailedAfterFinishedStillRecordsCauseForLocalSinkAbort() throws Exception {
    QueryId queryId = new QueryId("driver_context_failed_after_finished_test");
    FragmentInstanceId instanceId = new FragmentInstanceId(new PlanFragmentId(queryId, 1), "0");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, directExecutor());
    FragmentInstanceContext context =
        FragmentInstanceContext.createFragmentInstanceContext(instanceId, stateMachine);

    DriverContext driverContext = new DriverContext(context, 0);
    LocalExchangePair exchange = createLocalExchange(instanceId.toThrift(), context);
    ListenableFuture<?> blocked = exchange.sourceHandle.isBlocked();

    driverContext.finished();
    Assert.assertTrue(driverContext.isDone());

    SemanticException cause =
        new SemanticException("The second and third argument must be greater than 0");
    driverContext.failed(cause);
    exchange.sinkChannel.abort();

    Assert.assertEquals(Optional.of(cause), context.getFailureCause());
    Assert.assertEquals(1, stateMachine.getFailureCauses().size());
    try {
      blocked.get(5, TimeUnit.SECONDS);
      Assert.fail("Expected ExecutionException carrying SemanticException");
    } catch (ExecutionException e) {
      Assert.assertSame(cause, e.getCause());
    }
  }

  @Test
  public void testFailedMarksDoneEvenIfContextFailedThrows() {
    FragmentInstanceContext mockContext = Mockito.mock(FragmentInstanceContext.class);
    RuntimeException notificationError = new RuntimeException("executor rejected");
    Mockito.doThrow(notificationError).when(mockContext).failed(Mockito.any());

    DriverContext driverContext = new DriverContext(mockContext);
    RuntimeException cause1 = new RuntimeException("cause-1");
    RuntimeException cause2 = new RuntimeException("cause-2");

    try {
      driverContext.failed(cause1);
      Assert.fail("Expected RuntimeException from FragmentInstanceContext.failed");
    } catch (RuntimeException e) {
      Assert.assertSame(notificationError, e);
    }

    Assert.assertTrue(driverContext.isDone());
    driverContext.failed(cause2);
    Mockito.verify(mockContext, Mockito.times(1)).failed(cause1);
    Mockito.verify(mockContext, Mockito.never()).failed(cause2);
  }

  private static LocalExchangePair createLocalExchange(
      TFragmentInstanceId localFragmentInstanceId, FragmentInstanceContext context) {
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = new MemoryManager(1024L * 1024L);
    MemoryPool memoryPool = new MemoryPool("test", memoryManager, 1024L * 1024L);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(memoryPool);

    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            localFragmentInstanceId,
            "result_node",
            mockLocalMemoryManager,
            newDirectExecutorService());
    LocalSourceHandle sourceHandle =
        new LocalSourceHandle(
            new TFragmentInstanceId(localFragmentInstanceId.getQueryId(), 0, "0"),
            "result_node",
            queue,
            Mockito.mock(SourceHandleListener.class));
    SinkListener sinkListener =
        new SinkListener() {
          @Override
          public void onFinish(ISink sink) {}

          @Override
          public void onEndOfBlocks(ISink sink) {}

          @Override
          public Optional<Throwable> onAborted(ISink sink) {
            return context.getFailureCause();
          }

          @Override
          public void onFailure(ISink sink, Throwable t) {}
        };
    LocalSinkChannel sinkChannel =
        new LocalSinkChannel(localFragmentInstanceId, queue, sinkListener);
    return new LocalExchangePair(sinkChannel, sourceHandle);
  }

  private static final class LocalExchangePair {
    private final LocalSinkChannel sinkChannel;
    private final LocalSourceHandle sourceHandle;

    private LocalExchangePair(LocalSinkChannel sinkChannel, LocalSourceHandle sourceHandle) {
      this.sinkChannel = sinkChannel;
      this.sourceHandle = sourceHandle;
    }
  }
}
