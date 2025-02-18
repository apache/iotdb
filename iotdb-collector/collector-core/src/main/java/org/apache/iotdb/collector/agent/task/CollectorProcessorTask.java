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

package org.apache.iotdb.collector.agent.task;

import org.apache.iotdb.collector.agent.collect.CollectorEventCollector;
import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class CollectorProcessorTask extends CollectorTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectorProcessorTask.class);

  private final Map<String, String> processorAttribute;
  private final PipeProcessor pipeProcessor;
  private final EventSupplier eventSupplier;
  private final BlockingQueue<Event> pendingQueue;
  private final CollectorEventCollector collectorEventCollector;
  private boolean isStarted = true;

  public CollectorProcessorTask(
      final String taskId,
      final Map<String, String> processorAttribute,
      final PipeProcessor pipeProcessor,
      final EventSupplier eventSupplier,
      final BlockingQueue<Event> pendingQueue) {
    super(taskId);
    this.processorAttribute = processorAttribute;
    this.pipeProcessor = pipeProcessor;
    this.eventSupplier = eventSupplier;
    this.pendingQueue = pendingQueue;
    this.collectorEventCollector = new CollectorEventCollector(pendingQueue);
  }

  @Override
  public void runMayThrow() {
    while (isStarted) {
      try {
        pipeProcessor.process(eventSupplier.supply(), collectorEventCollector);
      } catch (final Exception e) {
        LOGGER.warn("error occur while processing event because {}", e.getMessage());
      }
    }
  }

  public Map<String, String> getProcessorAttribute() {
    return processorAttribute;
  }

  public PipeProcessor getPipeProcessor() {
    return pipeProcessor;
  }

  public EventSupplier getEventSupplier() {
    return eventSupplier;
  }

  public BlockingQueue<Event> getPendingQueue() {
    return pendingQueue;
  }

  public void stop() {
    isStarted = false;
  }
}
