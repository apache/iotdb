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

package org.apache.iotdb.confignode.manager.pipe.agent.task;

import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.subtask.PipeAbstractSinkSubtask;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipeExtractor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class PipeConfigNodeSubtaskTest {

  private PipeConfigNodeSubtask subtask;
  private PipeExtractor source;
  private PipeConnector sink;
  private boolean subtaskClosed;

  @Before
  public void setUp() throws Exception {
    source = mock(PipeExtractor.class);
    sink = mock(PipeConnector.class);
    subtask =
        new PipeConfigNodeSubtask(
            "PipeConfigNodeSubtaskTest",
            System.currentTimeMillis(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.singletonMap(
                PipeSinkConstant.CONNECTOR_KEY,
                BuiltinPipePlugin.DO_NOTHING_CONNECTOR.getPipePluginName()),
            new PipeTaskMeta(MinimumProgressIndex.INSTANCE, Integer.MIN_VALUE));
    final Field sourceField = PipeConfigNodeSubtask.class.getDeclaredField("source");
    sourceField.setAccessible(true);
    sourceField.set(subtask, source);
    final Field sinkField = PipeAbstractSinkSubtask.class.getDeclaredField("outputPipeSink");
    sinkField.setAccessible(true);
    sinkField.set(subtask, sink);
  }

  @After
  public void tearDown() {
    if (!subtaskClosed) {
      subtask.close();
    }
  }

  @Test
  public void testStoppedSubtaskDoesNotPollSource() throws Exception {
    assertFalse(subtask.executeOnce());
    verifyZeroInteractions(source, sink);
  }

  @Test
  public void testStopDuringSupplyRetainsEventUntilRestart() throws Exception {
    assertStopDuringSupply(false);
  }

  @Test
  public void testDropDuringSupplyReleasesEvent() throws Exception {
    assertStopDuringSupply(true);
  }

  private void assertStopDuringSupply(final boolean drop) throws Exception {
    final EnrichedEvent event = mock(EnrichedEvent.class);
    final CountDownLatch supplyStarted = new CountDownLatch(1);
    final CountDownLatch supplyReleased = new CountDownLatch(1);
    final ExecutorService worker = Executors.newSingleThreadExecutor();
    when(source.supply())
        .thenAnswer(
            invocation -> {
              supplyStarted.countDown();
              assertTrue(supplyReleased.await(10, TimeUnit.SECONDS));
              return event;
            });
    subtask.allowSubmittingSelf();
    try {
      final Future<Boolean> execution = worker.submit(() -> subtask.executeOnce());
      assertTrue(supplyStarted.await(10, TimeUnit.SECONDS));
      assertTrue(subtask.disallowSubmittingSelf());
      if (drop) {
        subtask.close();
        subtaskClosed = true;
      }
      supplyReleased.countDown();

      assertFalse(execution.get(10, TimeUnit.SECONDS));
      assertFalse(subtask.executeOnce());
      verify(sink, never()).transfer(event);
      verify(event, never()).decreaseReferenceCount(anyString(), anyBoolean());

      if (drop) {
        verify(event).clearReferenceCount(anyString());
      } else {
        verifyZeroInteractions(sink);
        subtask.allowSubmittingSelf();
        assertTrue(subtask.executeOnce());
        verify(source).supply();
        verify(sink).transfer(event);
        verify(event).decreaseReferenceCount(anyString(), eq(true));
      }
    } finally {
      supplyReleased.countDown();
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
