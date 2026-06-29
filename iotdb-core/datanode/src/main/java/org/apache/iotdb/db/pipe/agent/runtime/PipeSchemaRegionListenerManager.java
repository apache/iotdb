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
import org.apache.iotdb.db.pipe.metric.schema.PipeSchemaRegionListenerMetrics;
import org.apache.iotdb.db.pipe.source.schemaregion.SchemaRegionListeningQueue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PipeSchemaRegionListenerManager {

  private final Map<SchemaRegionId, PipeSchemaRegionListener> id2ListenerMap =
      new ConcurrentHashMap<>();
  private final Map<SchemaRegionId, AtomicBoolean> id2LeaderReadyMap = new ConcurrentHashMap<>();

  public synchronized Set<SchemaRegionId> regionIds() {
    return id2ListenerMap.keySet();
  }

  public synchronized SchemaRegionListeningQueue listener(final SchemaRegionId schemaRegionId) {
    return id2ListenerMap.computeIfAbsent(schemaRegionId, PipeSchemaRegionListener::new)
        .listeningQueue;
  }

  public synchronized SchemaRegionListeningQueue listenerIfPresent(
      final SchemaRegionId schemaRegionId) {
    final PipeSchemaRegionListener listener = id2ListenerMap.get(schemaRegionId);
    return listener == null ? null : listener.listeningQueue;
  }

  public synchronized int increaseAndGetReferenceCount(final SchemaRegionId schemaRegionId) {
    return id2ListenerMap
        .computeIfAbsent(schemaRegionId, PipeSchemaRegionListener::new)
        .listeningQueueReferenceCount
        .incrementAndGet();
  }

  public synchronized int decreaseAndGetReferenceCount(final SchemaRegionId schemaRegionId) {
    final PipeSchemaRegionListener listener = id2ListenerMap.get(schemaRegionId);
    if (listener == null) {
      return 0;
    }

    final int referenceCount =
        listener.listeningQueueReferenceCount.updateAndGet(v -> v > 0 ? v - 1 : 0);
    if (referenceCount == 0 && !listener.listeningQueue.isOpened()) {
      cleanupListenerIfUnused(schemaRegionId, listener);
    }
    return referenceCount;
  }

  public synchronized void notifyLeaderReady(final SchemaRegionId schemaRegionId) {
    id2LeaderReadyMap.computeIfAbsent(schemaRegionId, id -> new AtomicBoolean()).set(true);
  }

  public synchronized void notifyLeaderUnavailable(final SchemaRegionId schemaRegionId) {
    id2LeaderReadyMap.computeIfAbsent(schemaRegionId, id -> new AtomicBoolean()).set(false);
  }

  public synchronized boolean isLeaderReady(final SchemaRegionId schemaRegionId) {
    final AtomicBoolean isLeaderReady = id2LeaderReadyMap.get(schemaRegionId);
    return isLeaderReady != null && isLeaderReady.get();
  }

  public synchronized void cleanupListenerIfUnused(final SchemaRegionId schemaRegionId) {
    final PipeSchemaRegionListener listener = id2ListenerMap.get(schemaRegionId);
    if (listener != null) {
      cleanupListenerIfUnused(schemaRegionId, listener);
    }
  }

  public synchronized void clearSchemaRegionState(final SchemaRegionId schemaRegionId) {
    final PipeSchemaRegionListener listener = id2ListenerMap.remove(schemaRegionId);
    if (listener != null) {
      PipeSchemaRegionListenerMetrics.getInstance().deregister(schemaRegionId.getId());
    }
    id2LeaderReadyMap.remove(schemaRegionId);
  }

  private void cleanupListenerIfUnused(
      final SchemaRegionId schemaRegionId, final PipeSchemaRegionListener listener) {
    if (listener.listeningQueueReferenceCount.get() > 0 || listener.listeningQueue.isOpened()) {
      return;
    }
    if (id2ListenerMap.remove(schemaRegionId, listener)) {
      PipeSchemaRegionListenerMetrics.getInstance().deregister(schemaRegionId.getId());
    }
  }

  private static class PipeSchemaRegionListener {

    private final SchemaRegionListeningQueue listeningQueue = new SchemaRegionListeningQueue();
    private final AtomicInteger listeningQueueReferenceCount = new AtomicInteger(0);

    protected PipeSchemaRegionListener(final SchemaRegionId schemaRegionId) {
      PipeSchemaRegionListenerMetrics.getInstance()
          .register(listeningQueue, schemaRegionId.getId());
    }
  }
}
