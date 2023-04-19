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

package org.apache.iotdb.db.pipe.task.callable;

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.deletion.DeletionEvent;
import org.apache.iotdb.pipe.api.event.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.concurrent.ArrayBlockingQueue;

public class PipeConnectorSubtask extends PipeSubtask {

  private final ArrayBlockingQueue<Event> pendingQueue;
  private final PipeConnector pipeConnector;

  public PipeConnectorSubtask(
      String taskID, PipeConnector pipeConnector, ArrayBlockingQueue<Event> pendingQueue) {
    super(taskID);
    this.pipeConnector = pipeConnector;
    this.pendingQueue = pendingQueue;
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
}
