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

package org.apache.iotdb.db.pipe.task.subtask;

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.deletion.DeletionEvent;
import org.apache.iotdb.pipe.api.event.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

public class PipeConnectorSubtask extends PipeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConnectorSubtask.class);

  // input
  private final ArrayBlockingQueue<Event> pendingQueue;
  // output
  private final PipeConnector pipeConnector;

  /** @param taskID connectorAttributeSortedString */
  public PipeConnectorSubtask(String taskID, PipeConnector pipeConnector) {
    super(taskID);
    // TODO: make the size of the queue size reasonable and configurable
    this.pendingQueue = new ArrayBlockingQueue<>(Integer.MAX_VALUE);
    this.pipeConnector = pipeConnector;
  }

  public ArrayBlockingQueue<Event> getInputPendingQueue() {
    return pendingQueue;
  }

  // TODO: for a while
  @Override
  protected void executeForAWhile() {
    if (pendingQueue.isEmpty()) {
      return;
    }

    final Event event = pendingQueue.poll();

    try {
      if (event instanceof TabletInsertionEvent) {
        pipeConnector.transfer((TabletInsertionEvent) event);
      } else if (event instanceof TsFileInsertionEvent) {
        pipeConnector.transfer((TsFileInsertionEvent) event);
      } else if (event instanceof DeletionEvent) {
        pipeConnector.transfer((DeletionEvent) event);
      } else {
        throw new RuntimeException("Unsupported event type: " + event.getClass().getName());
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new PipeException(
          "Error occurred during executing PipeConnector#transfer, perhaps need to check whether the implementation of PipeConnector is correct according to the pipe-api description.",
          e);
    }
  }

  @Override
  public void close() {
    try {
      pipeConnector.close();
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.info(
          "Error occurred during closing PipeConnector, perhaps need to check whether the implementation of PipeConnector is correct according to the pipe-api description.",
          e);
    }
  }
}
