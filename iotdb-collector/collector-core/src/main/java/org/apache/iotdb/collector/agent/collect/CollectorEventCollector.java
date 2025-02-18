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

package org.apache.iotdb.collector.agent.collect;

import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class CollectorEventCollector implements EventCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectorEventCollector.class);

  private final BlockingQueue<Event> pendingQueue;

  public CollectorEventCollector(final BlockingQueue<Event> pendingQueue) {
    this.pendingQueue = pendingQueue;
  }

  @Override
  public void collect(final Event event) {
    try {
      pendingQueue.put(event);
    } catch (final InterruptedException e) {
      LOGGER.warn("collect event failed because {}", e.getMessage(), e);
    }
  }
}
