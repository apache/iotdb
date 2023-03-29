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

package org.apache.iotdb.db.pipe.core.processor;

import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.deletion.DeletionEvent;
import org.apache.iotdb.pipe.api.event.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

public class PipeProcessorPluginRuntimeWrapper {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeProcessorPluginRuntimeWrapper.class);
  private final PipeProcessor pipeProcessor;
  private final EventCollector eventCollector;

  private final Queue<Event> queue;

  public PipeProcessorPluginRuntimeWrapper(
      PipeProcessor pipeProcessor, EventCollector eventCollector, Queue<Event> queue) {
    this.pipeProcessor = pipeProcessor;
    this.eventCollector = eventCollector;
    this.queue = queue;
  }

  public void runOnce() {
    if (queue.isEmpty()) {
      return;
    }

    Event event = queue.poll();
    try {
      if (event instanceof TabletInsertionEvent) {
        pipeProcessor.process((TabletInsertionEvent) event, eventCollector);
      } else if (event instanceof TsFileInsertionEvent) {
        pipeProcessor.process((TsFileInsertionEvent) event, eventCollector);
      } else if (event instanceof DeletionEvent) {
        pipeProcessor.process((DeletionEvent) event, eventCollector);
      } else {
        throw new RuntimeException("Unsupported event type: " + event.getClass().getName());
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Error occurred during executing PipeProcessor#process, perhaps need to check whether the implementation of PipeProcessor is correct according to the pipe-api description."
              + e);
    }
  }
}
