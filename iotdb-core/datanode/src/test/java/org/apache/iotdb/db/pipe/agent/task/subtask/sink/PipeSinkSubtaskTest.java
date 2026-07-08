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

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.sink.protocol.PipeConnectorWithEventDiscard;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class PipeSinkSubtaskTest {

  @Test
  public void testDiscardEventsOfPipeDelegatesToConnector() {
    final PipeConnector connector =
        mock(
            PipeConnector.class,
            withSettings().extraInterfaces(PipeConnectorWithEventDiscard.class));
    final UnboundedBlockingPendingQueue<?> pendingQueue = mock(UnboundedBlockingPendingQueue.class);

    final PipeSinkSubtask subtask =
        Mockito.spy(
            new PipeSinkSubtask(
                "PipeSinkSubtaskTest",
                System.currentTimeMillis(),
                "data_test",
                "data_test",
                0,
                (UnboundedBlockingPendingQueue) pendingQueue,
                connector));

    try {
      final CommitterKey committerKey = new CommitterKey("pipe", 1L, 1, -1);
      subtask.discardEventsOfPipe(committerKey);

      verify((PipeConnectorWithEventDiscard) connector).discardEventsOfPipe(committerKey);
    } finally {
      subtask.close();
    }
  }

  @Test
  public void testTransferExceptionUsesDisplayTaskID() throws Exception {
    final PipeConnector connector = mock(PipeConnector.class);
    final UnboundedBlockingPendingQueue<Event> pendingQueue =
        mock(UnboundedBlockingPendingQueue.class);
    final Event event = mock(Event.class);

    when(pendingQueue.waitedPoll()).thenReturn(event);
    doThrow(new RuntimeException("No more authentication methods available"))
        .when(connector)
        .transfer(any(Event.class));

    final PipeSinkSubtask subtask =
        new PipeSinkSubtask(
            "data_{sink=TSFILE_REMOTE_SINK, sink.scp.password=Iotdb@2026}_1701687309493_0",
            1701687309493L,
            "data_{sink=TSFILE_REMOTE_SINK, sink.scp.password=Iotdb@2026}",
            "data_{sink=TSFILE_REMOTE_SINK, sink.scp.host=172.20.70.119}",
            0,
            pendingQueue,
            connector);

    try {
      subtask.executeOnce();
      Assert.fail();
    } catch (final PipeException e) {
      Assert.assertTrue(e.getMessage().contains("Exception in pipe transfer, subtask: data_{"));
      Assert.assertTrue(e.getMessage().contains("sink=TSFILE_REMOTE_SINK"));
      Assert.assertTrue(e.getMessage().contains("sink.scp.host=172.20.70.119"));
      Assert.assertTrue(e.getMessage().contains("No more authentication methods available"));
      Assert.assertFalse(e.getMessage().contains("sink.scp.password"));
      Assert.assertFalse(e.getMessage().contains("Iotdb@2026"));
    } finally {
      subtask.close();
    }
  }
}
