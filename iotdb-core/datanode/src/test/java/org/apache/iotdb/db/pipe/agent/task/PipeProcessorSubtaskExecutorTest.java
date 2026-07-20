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
import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

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

    private boolean executeOnceForTest() throws Exception {
      return executeOnce();
    }
  }
}
