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
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

public class PipeProcessorPluginRuntimeWrapper {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeProcessorPluginRuntimeWrapper.class);

  private final Queue<Event> inputEventQueue;
  private final PipeProcessor pipeProcessor;
  private final EventCollector outputEventCollector;

  public PipeProcessorPluginRuntimeWrapper(
      Queue<Event> inputEventQueue,
      PipeProcessor pipeProcessor,
      EventCollector outputEventCollector) {
    this.inputEventQueue = inputEventQueue;
    this.pipeProcessor = pipeProcessor;
    this.outputEventCollector = outputEventCollector;
  }

  public void executeForAWhile() {
    if (inputEventQueue.isEmpty()) {
      return;
    }

    final Event event = inputEventQueue.poll();

    try {
      if (event instanceof TabletInsertionEvent) {
        pipeProcessor.process((TabletInsertionEvent) event, outputEventCollector);
      } else if (event instanceof TsFileInsertionEvent) {
        pipeProcessor.process((TsFileInsertionEvent) event, outputEventCollector);
      } else if (event instanceof DeletionEvent) {
        pipeProcessor.process((DeletionEvent) event, outputEventCollector);
      } else {
        throw new RuntimeException("Unsupported event type: " + event.getClass().getName());
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new PipeException(
          "Error occurred during executing PipeProcessor#process, perhaps need to check whether the implementation of PipeProcessor is correct according to the pipe-api description.",
          e);
    }
  }
}
