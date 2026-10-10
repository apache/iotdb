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

import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class PipeRealtimePriorityBlockingQueueTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testReplaceOnlyRemovesEventsForCompactionSourceFiles() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue();
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final File sourceFile = temporaryFolder.newFile("source.tsfile");
    final File unrelatedFile = temporaryFolder.newFile("unrelated.tsfile");
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
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue();
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final File sourceFile = temporaryFolder.newFile("source.tsfile");
    final PipeTsFileInsertionEvent unrelatedEvent =
        createEvent(temporaryFolder.newFile("unrelated.tsfile"), committerKey, 1L);

    queue.offer(unrelatedEvent);
    queue.replace(
        "1", Collections.singleton(new TsFileResource(sourceFile)), Collections.emptyList());

    Assert.assertSame(unrelatedEvent, queue.directPoll());
    verify(unrelatedEvent, never()).decreaseReferenceCount(anyString(), anyBoolean());
  }

  @Test
  public void testPollCanRaceWithReplacementWithoutCorruptingQueue() throws Exception {
    final PipeRealtimePriorityBlockingQueue queue = new PipeRealtimePriorityBlockingQueue();
    final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, 0);
    final File sourceFile = temporaryFolder.newFile("source.tsfile");
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

      allowReplacementToContinue.countDown();
      replacementFuture.get(5, TimeUnit.SECONDS);
      Assert.assertSame(event, pollFuture.get(5, TimeUnit.SECONDS));
      verify(event, never())
          .decreaseReferenceCount(PipeRealtimePriorityBlockingQueue.class.getName(), false);
    } finally {
      allowReplacementToContinue.countDown();
      executor.shutdownNow();
    }
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
}
