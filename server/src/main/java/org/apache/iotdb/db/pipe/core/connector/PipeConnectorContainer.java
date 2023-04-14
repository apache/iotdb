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

package org.apache.iotdb.db.pipe.core.connector;

import org.apache.iotdb.db.pipe.task.callable.PipeConnectorSubtask;
import org.apache.iotdb.db.pipe.task.callable.PipeSubtask;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

public class PipeConnectorContainer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConnectorContainer.class);

  private final ArrayBlockingQueue<Event> pendingQueue;
  private final PipeConnectorSubtask subtask;
  private final int pendingQueueSize = 1000;

  public PipeConnectorContainer(PipeSubtask subtask) {
    this.subtask = (PipeConnectorSubtask) subtask;
    this.pendingQueue = new ArrayBlockingQueue<>(pendingQueueSize);

    this.subtask.setPendingQueue(pendingQueue);
  }

  public boolean addEvent(Event event) {
    if (pendingQueue.size() >= pendingQueueSize) {
      LOGGER.warn("Pending queue is full now and do not submit events for a while.");
      return false;
    }

    pendingQueue.add(event);
    return true;
  }
}
