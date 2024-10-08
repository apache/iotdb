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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.db.pipe.extractor.schemaregion.SchemaRegionListeningQueue;
import org.apache.iotdb.db.pipe.metric.PipeSchemaRegionListenerMetrics;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PipeSchemaRegionListenerManager {

  private final Map<SchemaRegionId, PipeSchemaRegionListener> id2ListenerMap =
      new ConcurrentHashMap<>();

  public synchronized Set<SchemaRegionId> regionIds() {
    return id2ListenerMap.keySet();
  }

  public synchronized SchemaRegionListeningQueue listener(final SchemaRegionId schemaRegionId) {
    return id2ListenerMap.computeIfAbsent(schemaRegionId, PipeSchemaRegionListener::new)
        .listeningQueue;
  }

  public synchronized int increaseAndGetReferenceCount(final SchemaRegionId schemaRegionId) {
    return id2ListenerMap
        .computeIfAbsent(schemaRegionId, PipeSchemaRegionListener::new)
        .listeningQueueReferenceCount
        .incrementAndGet();
  }

  public synchronized int decreaseAndGetReferenceCount(final SchemaRegionId schemaRegionId) {
    return id2ListenerMap
        .computeIfAbsent(schemaRegionId, PipeSchemaRegionListener::new)
        .listeningQueueReferenceCount
        .updateAndGet(v -> v > 0 ? v - 1 : 0);
  }

  public synchronized void notifyLeaderReady(final SchemaRegionId schemaRegionId) {
    id2ListenerMap
        .computeIfAbsent(schemaRegionId, PipeSchemaRegionListener::new)
        .notifyLeaderReady();
  }

  public synchronized void notifyLeaderUnavailable(final SchemaRegionId schemaRegionId) {
    id2ListenerMap
        .computeIfAbsent(schemaRegionId, PipeSchemaRegionListener::new)
        .notifyLeaderUnavailable();
  }

  public synchronized boolean isLeaderReady(final SchemaRegionId schemaRegionId) {
    return id2ListenerMap
        .computeIfAbsent(schemaRegionId, PipeSchemaRegionListener::new)
        .isLeaderReady();
  }

  private static class PipeSchemaRegionListener {

    private final SchemaRegionListeningQueue listeningQueue = new SchemaRegionListeningQueue();
    private final AtomicInteger listeningQueueReferenceCount = new AtomicInteger(0);

    private final AtomicBoolean isLeaderReady = new AtomicBoolean(false);

    protected PipeSchemaRegionListener(final SchemaRegionId schemaRegionId) {
      PipeSchemaRegionListenerMetrics.getInstance()
          .register(listeningQueue, schemaRegionId.getId());
    }

    /**
     * Get leader ready state, DO NOT use consensus layer's leader ready flag because
     * SimpleConsensus' ready flag is always {@code true}. Note that this flag has nothing to do
     * with listening and a {@link PipeTask} starts only iff the current node is a leader and ready.
     *
     * @return {@code true} iff the current node is a leader and ready
     */
    private boolean isLeaderReady() {
      return isLeaderReady.get();
    }

    // Leader ready flag has the following effect
    // 1. The linked list starts serving only after leader gets ready
    // 2. Config pipe task is only created after leader gets ready
    private void notifyLeaderReady() {
      isLeaderReady.set(true);
    }

    private void notifyLeaderUnavailable() {
      isLeaderReady.set(false);
    }
  }
}
