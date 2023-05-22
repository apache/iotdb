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

import org.apache.iotdb.db.pipe.task.queue.EventSupplier;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.deletion.DeletionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeProcessorSubtask extends PipeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcessorSubtask.class);

  private final EventSupplier inputEventSupplier;
  private final PipeProcessor pipeProcessor;
  private final EventCollector outputEventCollector;

  public PipeProcessorSubtask(
      String taskID,
      EventSupplier inputEventSupplier,
      PipeProcessor pipeProcessor,
      EventCollector outputEventCollector) {
    super(taskID);
    this.inputEventSupplier = inputEventSupplier;
    this.pipeProcessor = pipeProcessor;
    this.outputEventCollector = outputEventCollector;
  }

  @Override
  protected synchronized void executeForAWhile() throws Exception {
    final Event event = lastEvent != null ? lastEvent : inputEventSupplier.supply();
    // record the last event for retry when exception occurs
    lastEvent = event;
    if (event == null) {
      return;
    }

    try {
      switch (event.getType()) {
        case TABLET_INSERTION:
          pipeProcessor.process((TabletInsertionEvent) event, outputEventCollector);
          break;
        case TSFILE_INSERTION:
          pipeProcessor.process((TsFileInsertionEvent) event, outputEventCollector);
          break;
        case DELETION:
          pipeProcessor.process((DeletionEvent) event, outputEventCollector);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported event type: " + event.getClass().getName());
      }

      releaseLastEvent();
    } catch (Exception e) {
      e.printStackTrace();
      throw new PipeException(
          "Error occurred during executing PipeProcessor#process, perhaps need to check whether the implementation of PipeProcessor is correct according to the pipe-api description.",
          e);
    }
  }

  @Override
  // synchronized for pipeProcessor.close() and releaseLastEvent() in super.close().
  // make sure that the lastEvent will not be updated after pipeProcessor.close() to avoid
  // resource leak because of the lastEvent is not released.
  public synchronized void close() {
    try {
      pipeProcessor.close();

      // should be called after pipeProcessor.close()
      super.close();
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.info(
          "Error occurred during closing PipeProcessor, perhaps need to check whether the implementation of PipeProcessor is correct according to the pipe-api description.",
          e);
    }
  }
}
