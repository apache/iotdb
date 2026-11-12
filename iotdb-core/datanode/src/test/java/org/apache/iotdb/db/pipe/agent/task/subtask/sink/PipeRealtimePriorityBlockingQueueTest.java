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

package org.apache.iotdb.db.pipe.agent.task.subtask.sink;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeCompactedTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class PipeRealtimePriorityBlockingQueueTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testCompletionBarrierIsNotDroppedAfterHeartbeat() {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue();
    final PipeHeartbeatEvent heartbeatEvent = new PipeHeartbeatEvent(1, false);
    final PipeHeartbeatEvent completionBarrier = new PipeHeartbeatEvent(1, false, true);

    Assert.assertTrue(queue.offer(heartbeatEvent));
    Assert.assertTrue(queue.offer(completionBarrier));
    Assert.assertSame(heartbeatEvent, queue.directPoll());
    Assert.assertSame(completionBarrier, queue.directPoll());
  }

  @Test
  public void testWaitedPollRetainsTsFilesUntilAsyncSendCapacityIsAvailable() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    final int maxWaitingTsFiles =
        PipeConfig.getInstance().getPipeRealTimeQueueMaxWaitingTsFileSize();
    final AtomicInteger waitingTsFiles = new AtomicInteger(maxWaitingTsFiles);
    queue.setOfferTsFileCounter(waitingTsFiles);
    final PipeTsFileInsertionEvent event =
        createEvent(newTsFile("waiting.tsfile"), new CommitterKey("pipe", 1L, 1, 0), 1L);
    Assert.assertTrue(queue.offer(event));

    for (final int waiting : new int[] {maxWaitingTsFiles, maxWaitingTsFiles + 1}) {
      waitingTsFiles.set(waiting);
      for (int i = 0; i < 3; i++) {
        Assert.assertNull(queue.waitedPoll());
        Assert.assertNull(queue.directPoll());
        Assert.assertEquals(1, queue.getTsFileInsertionEventCount());
        Assert.assertSame(event, queue.peek());
      }
    }

    waitingTsFiles.set(maxWaitingTsFiles - 1);
    Assert.assertSame(event, queue.waitedPoll());
    Assert.assertEquals(0, queue.getTsFileInsertionEventCount());
    verify(event, never()).decreaseReferenceCount(anyString(), anyBoolean());
  }

  @Test
  public void testConcurrentPollReservesOnlyAvailableTsFileStartupSlot() throws Exception {
    final int previousMaxWaitingTsFiles =
        PipeConfig.getInstance().getPipeRealTimeQueueMaxWaitingTsFileSize();
    final AtomicInteger startupCount = new AtomicInteger(0);
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    final ExecutorService pollers = Executors.newFixedThreadPool(16);
    final CountDownLatch startPollers = new CountDownLatch(1);
    try {
      CommonDescriptor.getInstance().getConfig().setPipeRealTimeQueueMaxWaitingTsFileSize(1);
      queue.setOfferTsFileCounter(startupCount);
      queue.enableTsFileStartupReservation();
      for (int i = 0; i < 16; i++) {
        Assert.assertTrue(
            queue.offer(
                createEvent(
                    newTsFile("parallel-" + i + ".tsfile"),
                    new CommitterKey("pipe", 1L, 1, 0),
                    i)));
      }

      final List<Future<Event>> polls = new ArrayList<>();
      for (int i = 0; i < 16; i++) {
        polls.add(
            pollers.submit(
                () -> {
                  startPollers.await();
                  return queue.directPoll();
                }));
      }
      startPollers.countDown();

      TsFileInsertionEvent polledEvent = null;
      int polledEventCount = 0;
      for (final Future<Event> poll : polls) {
        final Event event = poll.get(5, TimeUnit.SECONDS);
        if (event != null) {
          polledEvent = (TsFileInsertionEvent) event;
          polledEventCount++;
        }
      }

      Assert.assertEquals(1, polledEventCount);
      Assert.assertEquals(1, startupCount.get());
      Assert.assertEquals(15, queue.getTsFileInsertionEventCount());
      Assert.assertTrue(queue.cancelTsFileStartupReservation(polledEvent));
      Assert.assertEquals(0, startupCount.get());
    } finally {
      pollers.shutdownNow();
      pollers.awaitTermination(5, TimeUnit.SECONDS);
      queue.discardAllEvents();
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeRealTimeQueueMaxWaitingTsFileSize(previousMaxWaitingTsFiles);
    }
  }

  @Test
  public void testDiscardedPipeKeepsPolledStartupReservationUntilSinkCancelsIt() throws Exception {
    final int previousMaxWaitingTsFiles =
        PipeConfig.getInstance().getPipeRealTimeQueueMaxWaitingTsFileSize();
    final AtomicInteger startupCount = new AtomicInteger(0);
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final PipeTsFileInsertionEvent event =
        createEvent(newTsFile("discarded.tsfile"), committerKey, 1L);
    try {
      CommonDescriptor.getInstance().getConfig().setPipeRealTimeQueueMaxWaitingTsFileSize(1);
      queue.setOfferTsFileCounter(startupCount);
      queue.enableTsFileStartupReservation();
      Assert.assertTrue(queue.offer(event));
      Assert.assertSame(event, queue.directPoll());
      Assert.assertEquals(1, startupCount.get());

      queue.discardEventsOfPipe(committerKey);

      Assert.assertEquals(1, startupCount.get());
      Assert.assertNull(queue.directPoll());
      Assert.assertTrue(queue.cancelTsFileStartupReservation(event));
      Assert.assertEquals(0, startupCount.get());
    } finally {
      queue.discardAllEvents();
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeRealTimeQueueMaxWaitingTsFileSize(previousMaxWaitingTsFiles);
    }
  }

  @Test
  public void testWaitedPollDeliversHeartbeatsWhileTsFileSendQueueIsFull() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    queue.setOfferTsFileCounter(
        new AtomicInteger(PipeConfig.getInstance().getPipeRealTimeQueueMaxWaitingTsFileSize()));
    final PipeTsFileInsertionEvent event =
        createEvent(newTsFile("waiting.tsfile"), new CommitterKey("pipe", 1L, 1, 0), 1L);
    final PipeHeartbeatEvent heartbeat = new PipeHeartbeatEvent(1, false);
    final PipeHeartbeatEvent completionBarrier = new PipeHeartbeatEvent(1, false, true);
    Assert.assertTrue(queue.offer(event));
    Assert.assertTrue(queue.offer(heartbeat));
    Assert.assertTrue(queue.offer(completionBarrier));

    Assert.assertSame(heartbeat, queue.waitedPoll());
    Assert.assertSame(completionBarrier, queue.waitedPoll());
    Assert.assertNull(queue.waitedPoll());
    Assert.assertSame(event, queue.peek());
    Assert.assertEquals(1, queue.getTsFileInsertionEventCount());
  }

  @Test
  public void testHeartbeatWakesBackpressuredPoll() throws Exception {
    final long previousBlockingTimeMs =
        PipeConfig.getInstance().getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs();
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    queue.setOfferTsFileCounter(
        new AtomicInteger(PipeConfig.getInstance().getPipeRealTimeQueueMaxWaitingTsFileSize()));
    final PipeTsFileInsertionEvent event =
        createEvent(newTsFile("waiting.tsfile"), new CommitterKey("pipe", 1L, 1, 0), 1L);
    Assert.assertTrue(queue.offer(event));
    final FutureTask<Event> poll = new FutureTask<>(queue::waitedPoll);
    final Thread pollingThread = new Thread(poll);
    try {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(10_000);
      pollingThread.start();
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> pollingThread.getState() == Thread.State.TIMED_WAITING);

      final PipeHeartbeatEvent heartbeat = new PipeHeartbeatEvent(1, false);
      Assert.assertTrue(queue.offer(heartbeat));
      Assert.assertSame(heartbeat, poll.get(1, TimeUnit.SECONDS));
      Assert.assertSame(event, queue.peek());
    } finally {
      pollingThread.interrupt();
      pollingThread.join(5_000);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(previousBlockingTimeMs);
    }
  }

  @Test
  public void testWaitedPollRechecksCapacityAfterWaitingForAnEvent() throws Exception {
    final long previousBlockingTimeMs =
        PipeConfig.getInstance().getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs();
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    final AtomicInteger waitingTsFiles = new AtomicInteger(0);
    queue.setOfferTsFileCounter(waitingTsFiles);
    final PipeTsFileInsertionEvent event =
        createEvent(newTsFile("waiting.tsfile"), new CommitterKey("pipe", 1L, 1, 0), 1L);
    final FutureTask<Event> poll = new FutureTask<>(queue::waitedPoll);
    final Thread pollingThread = new Thread(poll);
    try {
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(1_000);
      pollingThread.start();
      await()
          .pollInterval(5, TimeUnit.MILLISECONDS)
          .atMost(5, TimeUnit.SECONDS)
          .until(() -> pollingThread.getState() == Thread.State.TIMED_WAITING);

      waitingTsFiles.set(PipeConfig.getInstance().getPipeRealTimeQueueMaxWaitingTsFileSize());
      Assert.assertTrue(queue.offer(event));
      Assert.assertNull(poll.get(2, TimeUnit.SECONDS));
      Assert.assertSame(event, queue.peek());
      Assert.assertEquals(1, queue.getTsFileInsertionEventCount());
    } finally {
      pollingThread.interrupt();
      pollingThread.join(5_000);
      CommonDescriptor.getInstance()
          .getConfig()
          .setPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(previousBlockingTimeMs);
    }
  }

  @Test
  public void testWaitedPollPreservesPendingFilesForCompaction() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    final int maxWaitingTsFiles =
        PipeConfig.getInstance().getPipeRealTimeQueueMaxWaitingTsFileSize();
    final AtomicInteger waitingTsFiles = new AtomicInteger(maxWaitingTsFiles);
    queue.setOfferTsFileCounter(waitingTsFiles);
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final File sourceFile0 = newTsFile("source0.tsfile");
    final File sourceFile1 = newTsFile("source1.tsfile");
    final File targetFile = newTsFile("target.tsfile");
    final PipeTsFileInsertionEvent sourceEvent0 = createEvent(sourceFile0, committerKey, 1L);
    final PipeTsFileInsertionEvent sourceEvent1 = createEvent(sourceFile1, committerKey, 2L);
    Assert.assertTrue(queue.offer(sourceEvent0));
    Assert.assertTrue(queue.offer(sourceEvent1));
    Assert.assertNull(queue.waitedPoll());

    queue.replace(
        "1",
        new HashSet<>(
            Arrays.asList(new TsFileResource(sourceFile0), new TsFileResource(sourceFile1))),
        Collections.singletonList(new TsFileResource(targetFile)));

    Assert.assertEquals(1, queue.getTsFileInsertionEventCount());
    Assert.assertNull(queue.waitedPoll());
    waitingTsFiles.decrementAndGet();
    final Event event = queue.waitedPoll();
    Assert.assertTrue(event instanceof PipeCompactedTsFileInsertionEvent);
    final PipeCompactedTsFileInsertionEvent compactedEvent =
        (PipeCompactedTsFileInsertionEvent) event;
    try {
      Assert.assertEquals(
          targetFile.getAbsoluteFile(), compactedEvent.getSourceTsFile().getAbsoluteFile());
      Assert.assertEquals(
          new HashSet<>(Arrays.asList(1L, 2L)), new HashSet<>(compactedEvent.getCommitIds()));
      Assert.assertEquals(0, queue.getTsFileInsertionEventCount());
      verify(sourceEvent0)
          .decreaseReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName(), false);
      verify(sourceEvent1)
          .decreaseReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName(), false);
    } finally {
      compactedEvent.decreaseReferenceCount(
          PipeRealtimePriorityBlockingQueue.class.getName(), false);
    }
  }

  @Test
  public void testReplaceOnlyRemovesEventsForCompactionSourceFiles() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final File sourceFile = newTsFile("source.tsfile");
    final File unrelatedFile = newTsFile("unrelated.tsfile");
    final PipeTsFileInsertionEvent sourceEvent = createEvent(sourceFile, committerKey, 1L);
    final PipeTsFileInsertionEvent unrelatedEvent = createEvent(unrelatedFile, committerKey, 2L);

    queue.offer(sourceEvent);
    queue.offer(unrelatedEvent);
    queue.replace(
        "1", Collections.singleton(new TsFileResource(sourceFile)), Collections.emptyList());

    Assert.assertSame(unrelatedEvent, queue.directPoll());
    Assert.assertNull(queue.directPoll());
    verify(sourceEvent)
        .decreaseReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName(), false);
    verify(unrelatedEvent, never()).decreaseReferenceCount(anyString(), anyBoolean());
  }

  @Test
  public void testReplaceDoesNotMatchUnrelatedEventsByCount() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final File sourceFile = newTsFile("source.tsfile");
    final PipeTsFileInsertionEvent unrelatedEvent =
        createEvent(newTsFile("unrelated.tsfile"), committerKey, 1L);

    queue.offer(unrelatedEvent);
    queue.replace(
        "1", Collections.singleton(new TsFileResource(sourceFile)), Collections.emptyList());

    Assert.assertSame(unrelatedEvent, queue.directPoll());
    verify(unrelatedEvent, never()).decreaseReferenceCount(anyString(), anyBoolean());
  }

  @Test
  public void testReplaceKeepsCompactedEventsAtTheOldestQueuePosition() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final File sourceFile0 = newTsFile("source0.tsfile");
    final File sourceFile1 = newTsFile("source1.tsfile");
    final File laterFile = newTsFile("later.tsfile");
    final File targetFile0 = newTsFile("target0.tsfile");
    final File targetFile1 = newTsFile("target1.tsfile");
    final PipeTsFileInsertionEvent sourceEvent0 = createEvent(sourceFile0, committerKey, 1L);
    final PipeTsFileInsertionEvent sourceEvent1 = createEvent(sourceFile1, committerKey, 2L);
    final PipeTsFileInsertionEvent laterEvent = createEvent(laterFile, committerKey, 3L);

    queue.offer(sourceEvent0);
    queue.offer(sourceEvent1);
    queue.offer(laterEvent);
    queue.replace(
        "1",
        new HashSet<>(
            Arrays.asList(new TsFileResource(sourceFile0), new TsFileResource(sourceFile1))),
        Arrays.asList(new TsFileResource(targetFile0), new TsFileResource(targetFile1)));

    final List<Event> queuedEvents = new ArrayList<>();
    queue.forEach(queuedEvents::add);
    Assert.assertEquals(3, queuedEvents.size());
    Assert.assertSame(laterEvent, queuedEvents.get(2));
    final PipeTsFileInsertionEvent compactedEvent0 = (PipeTsFileInsertionEvent) queuedEvents.get(0);
    final PipeTsFileInsertionEvent compactedEvent1 = (PipeTsFileInsertionEvent) queuedEvents.get(1);
    Assert.assertTrue(compactedEvent1 instanceof PipeCompactedTsFileInsertionEvent);
    Assert.assertTrue(compactedEvent0 instanceof PipeCompactedTsFileInsertionEvent);
    Assert.assertEquals(
        targetFile1.getAbsoluteFile(), compactedEvent1.getSourceTsFile().getAbsoluteFile());
    Assert.assertEquals(
        targetFile0.getAbsoluteFile(), compactedEvent0.getSourceTsFile().getAbsoluteFile());
    compactedEvent1.decreaseReferenceCount(
        PipeRealtimePriorityBlockingQueue.class.getName(), false);
    compactedEvent0.decreaseReferenceCount(
        PipeRealtimePriorityBlockingQueue.class.getName(), false);
  }

  @Test
  public void testReplaceLeavesNonHeadSourceEventsQueuedToPreserveOrder() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final PipeTsFileInsertionEvent olderEvent =
        createEvent(newTsFile("older.tsfile"), committerKey, 1L);
    final File sourceFile = newTsFile("source.tsfile");
    final PipeTsFileInsertionEvent sourceEvent = createEvent(sourceFile, committerKey, 2L);
    final PipeTsFileInsertionEvent laterEvent =
        createEvent(newTsFile("later.tsfile"), committerKey, 3L);

    queue.offer(olderEvent);
    queue.offer(sourceEvent);
    queue.offer(laterEvent);
    queue.replace(
        "1",
        Collections.singleton(new TsFileResource(sourceFile)),
        Collections.singletonList(new TsFileResource(newTsFile("target.tsfile"))));

    final List<Event> queuedEvents = new ArrayList<>();
    queue.forEach(queuedEvents::add);
    Assert.assertEquals(3, queuedEvents.size());
    Assert.assertSame(olderEvent, queuedEvents.get(0));
    Assert.assertSame(sourceEvent, queuedEvents.get(1));
    Assert.assertSame(laterEvent, queuedEvents.get(2));
    verify(sourceEvent, never())
        .decreaseReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName(), false);
  }

  @Test
  public void testEarlierEventFromAnotherPipeDoesNotBlockReplacement() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    final PipeTsFileInsertionEvent otherPipeEvent =
        createEvent(newTsFile("other-pipe.tsfile"), new CommitterKey("otherPipe", 1L, 1, 0), 1L);
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final File sourceFile = newTsFile("source.tsfile");
    final PipeTsFileInsertionEvent sourceEvent = createEvent(sourceFile, committerKey, 2L);

    queue.offer(otherPipeEvent);
    queue.offer(sourceEvent);
    queue.replace(
        "1",
        Collections.singleton(new TsFileResource(sourceFile)),
        Collections.singletonList(new TsFileResource(newTsFile("target.tsfile"))));

    final List<Event> queuedEvents = new ArrayList<>();
    queue.forEach(queuedEvents::add);
    Assert.assertEquals(2, queuedEvents.size());
    Assert.assertTrue(queuedEvents.get(0) instanceof PipeCompactedTsFileInsertionEvent);
    Assert.assertSame(otherPipeEvent, queuedEvents.get(1));
    verify(sourceEvent)
        .decreaseReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName(), false);
  }

  @Test
  public void testPollCanRaceWithReplacementWithoutCorruptingQueue() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue(true);
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final File sourceFile = newTsFile("source.tsfile");
    final PipeTsFileInsertionEvent event = createEvent(sourceFile, committerKey, 1L);
    final CountDownLatch sourcePathRead = new CountDownLatch(1);
    final CountDownLatch allowReplacementToContinue = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      doAnswer(
              invocation -> {
                sourcePathRead.countDown();
                Assert.assertTrue(allowReplacementToContinue.await(5, TimeUnit.SECONDS));
                return sourceFile;
              })
          .when(event)
          .getSourceTsFile();
      queue.offer(event);

      final Future<?> replacementFuture =
          executor.submit(
              () ->
                  queue.replace(
                      "1",
                      Collections.singleton(new TsFileResource(sourceFile)),
                      Collections.emptyList()));
      Assert.assertTrue(sourcePathRead.await(5, TimeUnit.SECONDS));
      final Future<Event> pollFuture = executor.submit(queue::directPoll);
      Assert.assertSame(event, pollFuture.get(5, TimeUnit.SECONDS));

      allowReplacementToContinue.countDown();
      replacementFuture.get(5, TimeUnit.SECONDS);
      verify(event, never())
          .decreaseReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName(), false);
    } finally {
      allowReplacementToContinue.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testReplaceIsDisabledByDefault() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue();
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final File sourceFile = newTsFile("source.tsfile");
    final PipeTsFileInsertionEvent sourceEvent = createEvent(sourceFile, committerKey, 1L);

    queue.offer(sourceEvent);
    queue.replace(
        "1", Collections.singleton(new TsFileResource(sourceFile)), Collections.emptyList());

    Assert.assertSame(sourceEvent, queue.directPoll());
    Assert.assertNull(queue.directPoll());
    verify(sourceEvent, never())
        .decreaseReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName(), false);
  }

  private PipeTsFileInsertionEvent createEvent(
      final File file, final CommitterKey committerKey, final long commitId) {
    final PipeTsFileInsertionEvent event =
        Mockito.spy(
            new PipeTsFileInsertionEvent(false, "root.db", new TsFileResource(file), false));
    event.setCommitterKeyAndCommitId(committerKey, commitId);
    doReturn(true).when(event).decreaseReferenceCount(anyString(), anyBoolean());
    return event;
  }

  private File newTsFile(final String name) throws Exception {
    final File sequenceDir = new File(temporaryFolder.getRoot(), "sequence/1/0");
    Assert.assertTrue(sequenceDir.mkdirs() || sequenceDir.isDirectory());
    final File tsFile = new File(sequenceDir, name);
    Assert.assertTrue(tsFile.createNewFile());
    return tsFile;
  }
}
