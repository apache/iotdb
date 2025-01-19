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

import org.apache.iotdb.pipe.api.PipeSink;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class CollectorSinkTask extends CollectorTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectorSinkTask.class);

  private final Map<String, String> sinkAttribute;
  private final PipeSink pipeSink;
  private final BlockingQueue<Event> pendingQueue;
  private boolean isStarted = true;

  public CollectorSinkTask(
      final String taskId,
      final Map<String, String> sinkAttribute,
      final PipeSink pipeSink,
      final BlockingQueue<Event> pendingQueue) {
    super(taskId);
    this.sinkAttribute = sinkAttribute;
    this.pipeSink = pipeSink;
    this.pendingQueue = pendingQueue;
  }

  @Override
  public void runMayThrow() {
    try {
      pipeSink.handshake();
    } catch (final Exception e) {
      LOGGER.warn("handshake fail because {}", e.getMessage());
    }
    isStarted = true;
    while (isStarted) {
      try {
        final Event event = pendingQueue.take();
        pipeSink.transfer(event);
        LOGGER.info("transfer event {} success, remain number is {}", event, pendingQueue.size());
      } catch (final InterruptedException e) {
        LOGGER.warn("interrupted while waiting for take a event");
      } catch (final Exception e) {
        LOGGER.warn("error occur while transfer event to endpoint");
      }
    }
  }

  public Map<String, String> getSinkAttribute() {
    return sinkAttribute;
  }

  public PipeSink getPipeSink() {
    return pipeSink;
  }

  public void stop() {
    isStarted = false;
  }

  public BlockingQueue<Event> getPendingQueue() {
    return pendingQueue;
  }
}
