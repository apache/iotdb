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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async;

import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkCriticalException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.agent.task.subtask.sink.PipeRealtimePriorityBlockingQueue;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeAsyncClientManager;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler.PipeTransferTsFileHandler;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

public class IoTDBDataRegionAsyncSinkTest {

  @Test
  public void testTsFileStartupWakesBackpressuredQueueBeforeTransferCompletes() throws Exception {
    assertTsFileStartupWakesBackpressuredQueue(false);
  }

  @Test
  public void testRejectedTsFileStartupWakesBackpressuredQueue() throws Exception {
    assertTsFileStartupWakesBackpressuredQueue(true);
  }

  private void assertTsFileStartupWakesBackpressuredQueue(final boolean rejectStartup)
      throws Exception {
    final long previousBlockingTimeMs =
        PipeConfig.getInstance().getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs();
    final int initialCounter =
        PipeConfig.getInstance().getPipeRealTimeQueueMaxWaitingTsFileSize() - 1;
    final AtomicInteger counter = new AtomicInteger(initialCounter);
    final IoTDBDataRegionAsyncSink sink = new IoTDBDataRegionAsyncSink();
    sink.setTransferTsFileCounter(counter);
    // Also cover the default realtime-first queue with compaction disabled.
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue();
    queue.setOfferTsFileCounter(counter);
    final Event event = Mockito.mock(PipeTsFileInsertionEvent.class);
    Assert.assertTrue(queue.offer(event));

    final CountDownLatch startupEntered = new CountDownLatch(1);
    final CountDownLatch allowStartup = new CountDownLatch(1);
    final ExecutorService startupExecutor =
        rejectStartup ? Mockito.mock(ExecutorService.class) : Executors.newSingleThreadExecutor();
    final ExecutorService submittingExecutor = Executors.newSingleThreadExecutor();
    final IoTDBDataNodeAsyncClientManager clientManager =
        Mockito.mock(IoTDBDataNodeAsyncClientManager.class);
    final AsyncPipeDataTransferServiceClient client =
        Mockito.mock(AsyncPipeDataTransferServiceClient.class);
    final PipeTransferTsFileHandler handler = Mockito.mock(PipeTransferTsFileHandler.class);
    Mockito.when(clientManager.getExecutor()).thenReturn(startupExecutor);
    if (rejectStartup) {
      Mockito.doAnswer(
              invocation -> {
                startupEntered.countDown();
                Assert.assertTrue(allowStartup.await(5, TimeUnit.SECONDS));
                throw new RejectedExecutionException("startup rejected");
              })
          .when(startupExecutor)
          .execute(Mockito.any(Runnable.class));
    } else {
      Mockito.when(clientManager.borrowClient())
          .thenAnswer(
              invocation -> {
                startupEntered.countDown();
                Assert.assertTrue(allowStartup.await(5, TimeUnit.SECONDS));
                return client;
              });
    }
    final Field clientManagerField =
        IoTDBDataRegionAsyncSink.class.getDeclaredField("transferTsFileClientManager");
    clientManagerField.setAccessible(true);
    clientManagerField.set(sink, clientManager);
    final Method transfer =
        IoTDBDataRegionAsyncSink.class.getDeclaredMethod(
            "transfer", PipeTransferTsFileHandler.class);
    transfer.setAccessible(true);
    final FutureTask<Event> poll = new FutureTask<>(queue::waitedPoll);
    final Thread pollingThread = new Thread(poll);
    try {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(10_000);
      final Future<?> submission =
          submittingExecutor.submit(
              () -> {
                transfer.invoke(sink, handler);
                return null;
              });
      Assert.assertTrue(startupEntered.await(5, TimeUnit.SECONDS));
      Assert.assertEquals(initialCounter + 1, counter.get());
      pollingThread.start();
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> pollingThread.getState() == Thread.State.TIMED_WAITING);

      allowStartup.countDown();
      Assert.assertSame(event, poll.get(1, TimeUnit.SECONDS));
      submission.get(1, TimeUnit.SECONDS);
      Assert.assertEquals(initialCounter, counter.get());
      if (rejectStartup) {
        Mockito.verify(handler).onError(Mockito.any(RejectedExecutionException.class));
      } else {
        Mockito.verify(handler).transfer(clientManager, client);
        // No completion callback has run: capacity tracks startup, not the file's lifecycle.
        Mockito.verify(handler, Mockito.never()).onComplete(Mockito.any());
      }
    } finally {
      allowStartup.countDown();
      pollingThread.interrupt();
      pollingThread.join(5_000);
      startupExecutor.shutdownNow();
      submittingExecutor.shutdownNow();
      if (!rejectStartup) {
        startupExecutor.awaitTermination(5, TimeUnit.SECONDS);
      }
      submittingExecutor.awaitTermination(5, TimeUnit.SECONDS);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(previousBlockingTimeMs);
    }
  }

  @Test
  public void testRetryQueueFailureMessageIncludesRootCauseAndIsCleared() {
    final IoTDBDataRegionAsyncSink sink = new IoTDBDataRegionAsyncSink();
    final Event event = Mockito.mock(Event.class);

    sink.addFailureEventToRetryQueue(
        event,
        new PipeException(
            "sink transfer wrapper", new IllegalStateException("receiver rejected request")));

    Assert.assertEquals("receiver rejected request", sink.getLastRetryFailureMessage());
    Assert.assertTrue(
        IoTDBDataRegionAsyncSink.formatRetryQueueFailureMessage(
                1, 1, 0, sink.getLastRetryFailureMessage())
            .contains("receiver rejected request"));

    sink.clearRetryEventsReferenceCount();

    Assert.assertNull(sink.getLastRetryFailureMessage());
    Assert.assertFalse(
        IoTDBDataRegionAsyncSink.formatRetryQueueFailureMessage(
                0, 0, 0, sink.getLastRetryFailureMessage())
            .contains("receiver rejected request"));
  }

  @Test
  public void testRetryQueueFailureMessageKeepsRootCauseTypeWhenMessageIsMissing() {
    final IoTDBDataRegionAsyncSink sink = new IoTDBDataRegionAsyncSink();
    final Event event = Mockito.mock(Event.class);

    sink.addFailureEventToRetryQueue(
        event, new PipeException("sink transfer wrapper", new NullPointerException()));

    Assert.assertEquals("java.lang.NullPointerException", sink.getLastRetryFailureMessage());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMissingTsFileRetryLimitKeepsEventQueuedAndStopsSink() throws Exception {
    final IoTDBDataRegionAsyncSink sink = new IoTDBDataRegionAsyncSink();
    final PipeTsFileInsertionEvent event = Mockito.mock(PipeTsFileInsertionEvent.class);
    final File missingTsFile = new File("missing-retry-limit.tsfile").getAbsoluteFile();
    Mockito.when(event.getTsFile()).thenReturn(missingTsFile);

    final Field retryLimitField =
        IoTDBDataRegionAsyncSink.class.getDeclaredField("MAX_FILE_NOT_FOUND_RETRY_TIMES");
    retryLimitField.setAccessible(true);
    final int retryLimit = retryLimitField.getInt(null);

    final Field retryTimesField =
        IoTDBDataRegionAsyncSink.class.getDeclaredField("missingFileRetryTimes");
    retryTimesField.setAccessible(true);
    ((Map<Event, Integer>) retryTimesField.get(sink)).put(event, retryLimit);

    try {
      sink.addFailureEventToRetryQueue(
          event, new FileNotFoundException(missingTsFile.getAbsolutePath()));

      Assert.assertEquals(1, sink.getRetryEventQueueSize());
      Assert.assertTrue(
          sink.getLastRetryFailureMessage().contains(missingTsFile.getAbsolutePath()));
      Assert.assertTrue(sink.getLastRetryFailureMessage().contains(String.valueOf(retryLimit)));
      Mockito.verify(event, Mockito.never())
          .clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());

      try {
        sink.transfer(Mockito.mock(Event.class));
        Assert.fail("The sink should stop transferring after the missing-file retry limit.");
      } catch (final PipeRuntimeSinkCriticalException e) {
        Assert.assertTrue(e.getMessage().contains(missingTsFile.getAbsolutePath()));
      }
    } finally {
      sink.clearRetryEventsReferenceCount();
    }

    Mockito.verify(event).clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());
  }
}
