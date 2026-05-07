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
import org.apache.iotdb.commons.pipe.sink.protocol.PipeConnectorWithEventDiscard;
import org.apache.iotdb.pipe.api.PipeConnector;

import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
                0,
                (UnboundedBlockingPendingQueue) pendingQueue,
                connector));

    try {
      subtask.discardEventsOfPipe("pipe", 1L, 1);

      verify((PipeConnectorWithEventDiscard) connector).discardEventsOfPipe("pipe", 1L, 1);
    } finally {
      subtask.close();
    }
  }
}
