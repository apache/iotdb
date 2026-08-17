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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.donothing.DoNothingProcessor;
import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipeProcessorSubtaskExecutorTest extends PipeSubtaskExecutorTest {

  @Before
  public void setUp() throws Exception {
    executor = new PipeProcessorSubtaskExecutor();

    subtask =
        Mockito.spy(
            new PipeProcessorSubtask(
                "PipeProcessorSubtaskExecutorTest",
                "TestPipe",
                System.currentTimeMillis(),
                0,
                mock(EventSupplier.class),
                mock(PipeProcessor.class),
                mock(PipeEventCollector.class)));
  }

  @Test
  public void testTsFileInsertionEventPreservesOutOfMemoryCause() {
    final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
    PipeMemoryBlock memoryBlock = null;

    try {
      memoryBlock =
          memoryManager.forceAllocateForTabletWithRetry(
              PipeMemoryManager.getTotalNonFloatingMemorySizeInBytes());
      Assert.assertFalse(memoryManager.isEnough4TabletParsing());

      final File tsFile =
          new File("target/testTsFileInsertionEventPreservesOutOfMemoryCause.tsfile");
      final TsFileResource resource = mock(TsFileResource.class);
      when(resource.isClosed()).thenReturn(true);
      when(resource.isEmpty()).thenReturn(false);
      when(resource.isGeneratedByPipe()).thenReturn(false);
      when(resource.isGeneratedByPipeConsensus()).thenReturn(false);
      when(resource.getTsFilePath()).thenReturn(tsFile.getPath());

      final PipeTsFileInsertionEvent event =
          new PipeTsFileInsertionEvent(
              resource, tsFile, false, false, false, "testPipe", 0, null, null, 0, 1);

      final PipeException exception =
          Assert.assertThrows(PipeException.class, () -> event.toTabletInsertionEvents(1));
      Assert.assertTrue(exception.getCause() instanceof PipeRuntimeOutOfMemoryCriticalException);
    } finally {
      memoryManager.release(memoryBlock);
    }
  }

  @Test
  public void testProcessorSubtaskTreatsOutOfMemoryCauseAsTemporaryFailure() throws Exception {
    final EventSupplier eventSupplier = mock(EventSupplier.class);
    final PipeProcessor pipeProcessor = mock(PipeProcessor.class);
    final PipeEventCollector pipeEventCollector = mock(PipeEventCollector.class);
    final TsFileInsertionEvent tsFileInsertionEvent = mock(TsFileInsertionEvent.class);
    when(eventSupplier.supply()).thenReturn(tsFileInsertionEvent);
    doThrow(
            new PipeException(
                "Parse TsFile error",
                new PipeRuntimeOutOfMemoryCriticalException(
                    "TimeoutException: Waited 22.016 seconds for memory to parse TsFile")))
        .when(pipeProcessor)
        .process(tsFileInsertionEvent, pipeEventCollector);

    final TestablePipeProcessorSubtask pipeProcessorSubtask =
        new TestablePipeProcessorSubtask(
            "PipeProcessorSubtaskExecutorTest",
            "TestPipe",
            System.currentTimeMillis(),
            0,
            eventSupplier,
            pipeProcessor,
            pipeEventCollector);

    Assert.assertFalse(pipeProcessorSubtask.executeOnceForTest());
  }

  @Test
  public void testTsFilesCanBeParsedInParallelInOneProcessorSubtask() throws Exception {
    final EventSupplier eventSupplier = mock(EventSupplier.class);
    final PipeEventCollector pipeEventCollector = mock(PipeEventCollector.class);
    final PipeEventCollector firstParserCollector = mock(PipeEventCollector.class);
    final PipeEventCollector secondParserCollector = mock(PipeEventCollector.class);
    final PipeTsFileInsertionEvent firstEvent = mock(PipeTsFileInsertionEvent.class);
    final PipeTsFileInsertionEvent secondEvent = mock(PipeTsFileInsertionEvent.class);

    when(eventSupplier.supply()).thenReturn(firstEvent, secondEvent, null);
    when(pipeEventCollector.shouldParseTsFileEvent(any(PipeTsFileInsertionEvent.class)))
        .thenReturn(true);
    when(pipeEventCollector.forkForTsFileParser())
        .thenReturn(firstParserCollector, secondParserCollector);
    when(firstEvent.tryReserveTsFileParserMemory()).thenReturn(true);
    when(secondEvent.tryReserveTsFileParserMemory()).thenReturn(true);

    final CountDownLatch parsersStarted = new CountDownLatch(2);
    final CountDownLatch releaseParsers = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              parsersStarted.countDown();
              releaseParsers.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(firstParserCollector)
        .collect(firstEvent);
    doAnswer(
            invocation -> {
              parsersStarted.countDown();
              releaseParsers.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(secondParserCollector)
        .collect(secondEvent);

    final TestablePipeProcessorSubtask pipeProcessorSubtask =
        new TestablePipeProcessorSubtask(
            "parallel-test",
            "pipe",
            System.currentTimeMillis(),
            0,
            eventSupplier,
            new DoNothingProcessor(),
            pipeEventCollector,
            2);
    try {
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Assert.assertTrue(parsersStarted.await(5, TimeUnit.SECONDS));
    } finally {
      releaseParsers.countDown();
      pipeProcessorSubtask.close();
    }
  }

  @Test
  public void testNonTsFileEventWaitsForInFlightTsFileParser() throws Exception {
    final EventSupplier eventSupplier = mock(EventSupplier.class);
    final PipeEventCollector pipeEventCollector = mock(PipeEventCollector.class);
    final PipeEventCollector parserCollector = mock(PipeEventCollector.class);
    final PipeTsFileInsertionEvent tsFileEvent = mock(PipeTsFileInsertionEvent.class);
    final TabletInsertionEvent barrierEvent = mock(TabletInsertionEvent.class);

    when(eventSupplier.supply()).thenReturn(tsFileEvent, barrierEvent, null);
    when(pipeEventCollector.shouldParseTsFileEvent(tsFileEvent)).thenReturn(true);
    when(pipeEventCollector.forkForTsFileParser()).thenReturn(parserCollector);
    when(tsFileEvent.tryReserveTsFileParserMemory()).thenReturn(true);

    final CountDownLatch parserStarted = new CountDownLatch(1);
    final CountDownLatch releaseParser = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              parserStarted.countDown();
              releaseParser.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(parserCollector)
        .collect(tsFileEvent);

    final TestablePipeProcessorSubtask pipeProcessorSubtask =
        new TestablePipeProcessorSubtask(
            "barrier-test",
            "pipe",
            System.currentTimeMillis(),
            0,
            eventSupplier,
            new DoNothingProcessor(),
            pipeEventCollector,
            2);
    try {
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Assert.assertTrue(parserStarted.await(5, TimeUnit.SECONDS));

      Assert.assertFalse(pipeProcessorSubtask.executeOnceForTest());
      Mockito.verify(pipeEventCollector, Mockito.never()).collect(barrierEvent);

      releaseParser.countDown();
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      boolean barrierProcessed = false;
      while (!barrierProcessed && System.nanoTime() < deadline) {
        barrierProcessed = pipeProcessorSubtask.executeOnceForTest();
        if (!barrierProcessed) {
          Thread.sleep(10);
        }
      }
      Assert.assertTrue(barrierProcessed);
      Mockito.verify(pipeEventCollector).collect(barrierEvent);
    } finally {
      releaseParser.countDown();
      pipeProcessorSubtask.close();
    }
  }

  @Test
  public void testProgressReportEventDoesNotWaitForInFlightTsFileParser() throws Exception {
    final EventSupplier eventSupplier = mock(EventSupplier.class);
    final PipeEventCollector pipeEventCollector = mock(PipeEventCollector.class);
    final PipeEventCollector firstParserCollector = mock(PipeEventCollector.class);
    final PipeEventCollector secondParserCollector = mock(PipeEventCollector.class);
    final PipeTsFileInsertionEvent firstTsFileEvent = mock(PipeTsFileInsertionEvent.class);
    final ProgressReportEvent progressReportEvent = mock(ProgressReportEvent.class);
    final PipeTsFileInsertionEvent secondTsFileEvent = mock(PipeTsFileInsertionEvent.class);

    when(eventSupplier.supply())
        .thenReturn(firstTsFileEvent, progressReportEvent, secondTsFileEvent, null);
    when(pipeEventCollector.shouldParseTsFileEvent(any(PipeTsFileInsertionEvent.class)))
        .thenReturn(true);
    when(pipeEventCollector.forkForTsFileParser())
        .thenReturn(firstParserCollector, secondParserCollector);
    when(firstTsFileEvent.tryReserveTsFileParserMemory()).thenReturn(true);
    when(secondTsFileEvent.tryReserveTsFileParserMemory()).thenReturn(true);

    final CountDownLatch parsersStarted = new CountDownLatch(2);
    final CountDownLatch releaseParsers = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              parsersStarted.countDown();
              releaseParsers.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(firstParserCollector)
        .collect(firstTsFileEvent);
    doAnswer(
            invocation -> {
              parsersStarted.countDown();
              releaseParsers.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(secondParserCollector)
        .collect(secondTsFileEvent);

    final TestablePipeProcessorSubtask pipeProcessorSubtask =
        new TestablePipeProcessorSubtask(
            "progress-report-barrier-test",
            "pipe",
            System.currentTimeMillis(),
            0,
            eventSupplier,
            new DoNothingProcessor(),
            pipeEventCollector,
            2);
    try {
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Mockito.verify(pipeEventCollector).collect(progressReportEvent);
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Assert.assertTrue(parsersStarted.await(5, TimeUnit.SECONDS));
    } finally {
      releaseParsers.countDown();
      pipeProcessorSubtask.close();
    }
  }

  @Test
  public void testHeartbeatEventDoesNotWaitForInFlightTsFileParser() throws Exception {
    final EventSupplier eventSupplier = mock(EventSupplier.class);
    final PipeEventCollector pipeEventCollector = mock(PipeEventCollector.class);
    final PipeEventCollector firstParserCollector = mock(PipeEventCollector.class);
    final PipeEventCollector secondParserCollector = mock(PipeEventCollector.class);
    final PipeTsFileInsertionEvent firstTsFileEvent = mock(PipeTsFileInsertionEvent.class);
    final PipeHeartbeatEvent heartbeatEvent = mock(PipeHeartbeatEvent.class);
    final PipeTsFileInsertionEvent secondTsFileEvent = mock(PipeTsFileInsertionEvent.class);

    when(eventSupplier.supply())
        .thenReturn(firstTsFileEvent, heartbeatEvent, secondTsFileEvent, null);
    when(pipeEventCollector.shouldParseTsFileEvent(any(PipeTsFileInsertionEvent.class)))
        .thenReturn(true);
    when(pipeEventCollector.forkForTsFileParser())
        .thenReturn(firstParserCollector, secondParserCollector);
    when(firstTsFileEvent.tryReserveTsFileParserMemory()).thenReturn(true);
    when(secondTsFileEvent.tryReserveTsFileParserMemory()).thenReturn(true);

    final CountDownLatch parsersStarted = new CountDownLatch(2);
    final CountDownLatch releaseParsers = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              parsersStarted.countDown();
              releaseParsers.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(firstParserCollector)
        .collect(firstTsFileEvent);
    doAnswer(
            invocation -> {
              parsersStarted.countDown();
              releaseParsers.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(secondParserCollector)
        .collect(secondTsFileEvent);

    final TestablePipeProcessorSubtask pipeProcessorSubtask =
        new TestablePipeProcessorSubtask(
            "heartbeat-barrier-test",
            "pipe",
            System.currentTimeMillis(),
            0,
            eventSupplier,
            new DoNothingProcessor(),
            pipeEventCollector,
            2);
    try {
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Mockito.verify(pipeEventCollector).collect(heartbeatEvent);
      Mockito.verify(heartbeatEvent).onProcessed();
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      Assert.assertTrue(parsersStarted.await(5, TimeUnit.SECONDS));
    } finally {
      releaseParsers.countDown();
      pipeProcessorSubtask.close();
    }
  }

  @Test
  public void testPermanentParallelParserFailureReachesRetryLimit() throws Exception {
    final EventSupplier eventSupplier = mock(EventSupplier.class);
    final PipeEventCollector pipeEventCollector = mock(PipeEventCollector.class);
    final PipeEventCollector parserCollector = mock(PipeEventCollector.class);
    final PipeTsFileInsertionEvent tsFileEvent = mock(PipeTsFileInsertionEvent.class);
    final PipeException parserFailure = new PipeException("broken TsFile");

    when(eventSupplier.supply()).thenReturn(tsFileEvent, null);
    when(pipeEventCollector.shouldParseTsFileEvent(tsFileEvent)).thenReturn(true);
    when(pipeEventCollector.forkForTsFileParser()).thenReturn(parserCollector);
    when(tsFileEvent.tryReserveTsFileParserMemory()).thenReturn(true);
    doThrow(parserFailure).when(parserCollector).collect(tsFileEvent);
    doThrow(parserFailure).when(pipeEventCollector).collect(tsFileEvent);

    final TestablePipeProcessorSubtask pipeProcessorSubtask =
        new TestablePipeProcessorSubtask(
            "parser-retry-limit-test",
            "pipe",
            System.currentTimeMillis(),
            0,
            eventSupplier,
            new DoNothingProcessor(),
            pipeEventCollector,
            2);
    try {
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      awaitParserFailure(pipeProcessorSubtask);
      pipeProcessorSubtask.recordFailureForTest();

      for (int retry = 0; retry < PipeProcessorSubtask.MAX_RETRY_TIMES; retry++) {
        Assert.assertThrows(PipeException.class, pipeProcessorSubtask::executeOnceForTest);
        pipeProcessorSubtask.recordFailureForTest();
      }

      Assert.assertEquals(
          PipeProcessorSubtask.MAX_RETRY_TIMES + 1, pipeProcessorSubtask.getRetryCountForTest());
      Assert.assertTrue(pipeProcessorSubtask.isStoppedByException());
      Mockito.verify(pipeEventCollector, Mockito.times(1)).forkForTsFileParser();
    } finally {
      pipeProcessorSubtask.close();
    }
  }

  @Test
  public void testParallelParserRetryCountClearsOnlyAfterSuccessfulRetry() throws Exception {
    final EventSupplier eventSupplier = mock(EventSupplier.class);
    final PipeEventCollector pipeEventCollector = mock(PipeEventCollector.class);
    final PipeEventCollector parserCollector = mock(PipeEventCollector.class);
    final long creationTime = System.currentTimeMillis();
    final File tsFile = new File("target/testParallelParserRetry.tsfile");
    final TsFileResource resource = mock(TsFileResource.class);
    when(resource.getTsFilePath()).thenReturn(tsFile.getPath());
    final PipeTsFileInsertionEvent tsFileEvent =
        mock(
            PipeTsFileInsertionEvent.class,
            Mockito.withSettings()
                .useConstructor(
                    resource, tsFile, false, false, false, "pipe", creationTime, null, null, 0L, 1L)
                .defaultAnswer(Mockito.RETURNS_DEFAULTS));

    when(eventSupplier.supply()).thenReturn(tsFileEvent, null);
    when(pipeEventCollector.shouldParseTsFileEvent(tsFileEvent)).thenReturn(true);
    when(pipeEventCollector.forkForTsFileParser()).thenReturn(parserCollector);
    doReturn(true).when(tsFileEvent).tryReserveTsFileParserMemory();
    doThrow(new PipeException("transient parser failure"))
        .when(parserCollector)
        .collect(tsFileEvent);
    doThrow(
            new PipeException(
                "temporary memory pressure",
                new PipeRuntimeOutOfMemoryCriticalException("parser memory unavailable")))
        .doNothing()
        .when(pipeEventCollector)
        .collect(tsFileEvent);

    final TestablePipeProcessorSubtask pipeProcessorSubtask =
        new TestablePipeProcessorSubtask(
            "parser-retry-success-test",
            "pipe",
            creationTime,
            0,
            eventSupplier,
            new DoNothingProcessor(),
            pipeEventCollector,
            2);
    try {
      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      awaitParserFailure(pipeProcessorSubtask);
      pipeProcessorSubtask.recordFailureForTest();

      Assert.assertFalse(pipeProcessorSubtask.executeOnceForTest());
      pipeProcessorSubtask.onSuccess(false);
      Assert.assertEquals(1, pipeProcessorSubtask.getRetryCountForTest());

      Assert.assertTrue(pipeProcessorSubtask.executeOnceForTest());
      pipeProcessorSubtask.onSuccess(true);
      Assert.assertEquals(0, pipeProcessorSubtask.getRetryCountForTest());
      Mockito.verify(pipeEventCollector, Mockito.times(1)).forkForTsFileParser();
    } finally {
      pipeProcessorSubtask.close();
    }
  }

  private static PipeException awaitParserFailure(
      final TestablePipeProcessorSubtask pipeProcessorSubtask) throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      try {
        pipeProcessorSubtask.executeOnceForTest();
      } catch (final PipeException e) {
        return e;
      }
      Thread.sleep(10);
    }
    throw new AssertionError("Timed out waiting for parallel parser failure");
  }

  private static class TestablePipeProcessorSubtask extends PipeProcessorSubtask {

    private TestablePipeProcessorSubtask(
        final String taskID,
        final String pipeName,
        final long creationTime,
        final int regionId,
        final EventSupplier inputEventSupplier,
        final PipeProcessor pipeProcessor,
        final PipeEventCollector outputEventCollector) {
      super(
          taskID,
          pipeName,
          creationTime,
          regionId,
          inputEventSupplier,
          pipeProcessor,
          outputEventCollector);
    }

    private TestablePipeProcessorSubtask(
        final String taskID,
        final String pipeName,
        final long creationTime,
        final int regionId,
        final EventSupplier inputEventSupplier,
        final PipeProcessor pipeProcessor,
        final PipeEventCollector outputEventCollector,
        final int tsFileParserParallelism) {
      super(
          taskID,
          pipeName,
          creationTime,
          regionId,
          inputEventSupplier,
          pipeProcessor,
          outputEventCollector,
          tsFileParserParallelism);
    }

    private boolean executeOnceForTest() throws Exception {
      return executeOnce();
    }

    private void recordFailureForTest() {
      retryCount.incrementAndGet();
    }

    private int getRetryCountForTest() {
      return retryCount.get();
    }
  }
}
