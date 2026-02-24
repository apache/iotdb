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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.listener;

import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;

import org.apache.tsfile.utils.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeTimePartitionListener {

  private final Map<Integer, Map<String, PipeRealtimeDataRegionSource>> dataRegionId2Sources =
      new ConcurrentHashMap<>();

  // This variable is used to record the upper and lower bounds that each data region's time
  // partition ID has ever reached.
  private final Map<Integer, Pair<Long, Long>> dataRegionId2TimePartitionIdBound =
      new ConcurrentHashMap<>();

  //////////////////////////// start & stop ////////////////////////////

  public synchronized void startListen(
      final int dataRegionId, final PipeRealtimeDataRegionSource source) {
    dataRegionId2Sources
        .computeIfAbsent(dataRegionId, o -> new HashMap<>())
        .put(source.getTaskID(), source);
    // Assign the previously recorded upper and lower bounds of time partition to the source that
    // has just started listening to the growth of time partition.
    final Pair<Long, Long> timePartitionIdBound =
        dataRegionId2TimePartitionIdBound.get(dataRegionId);
    if (Objects.nonNull(timePartitionIdBound)) {
      source.setDataRegionTimePartitionIdBound(timePartitionIdBound);
    }
  }

  public synchronized void stopListen(
      final int dataRegionId, final PipeRealtimeDataRegionSource source) {
    final Map<String, PipeRealtimeDataRegionSource> sources =
        dataRegionId2Sources.get(dataRegionId);
    if (Objects.isNull(sources)) {
      return;
    }
    sources.remove(source.getTaskID());
    if (sources.isEmpty()) {
      dataRegionId2Sources.remove(dataRegionId);
    }
  }

  //////////////////////////// listen to changes ////////////////////////////

  public synchronized void listenToTimePartitionGrow(
      final int dataRegionId, final Pair<Long, Long> newTimePartitionIdBound) {
    boolean shouldBroadcastTimePartitionChange = false;
    final Pair<Long, Long> oldTimePartitionIdBound =
        dataRegionId2TimePartitionIdBound.get(dataRegionId);

    if (Objects.isNull(oldTimePartitionIdBound)) {
      dataRegionId2TimePartitionIdBound.put(dataRegionId, newTimePartitionIdBound);
      shouldBroadcastTimePartitionChange = true;
    } else if (newTimePartitionIdBound.left < oldTimePartitionIdBound.left
        || oldTimePartitionIdBound.right < newTimePartitionIdBound.right) {
      dataRegionId2TimePartitionIdBound.put(
          dataRegionId,
          new Pair<>(
              Math.min(oldTimePartitionIdBound.left, newTimePartitionIdBound.left),
              Math.max(oldTimePartitionIdBound.right, newTimePartitionIdBound.right)));
      shouldBroadcastTimePartitionChange = true;
    }

    if (shouldBroadcastTimePartitionChange) {
      final Map<String, PipeRealtimeDataRegionSource> sources =
          dataRegionId2Sources.get(dataRegionId);
      if (Objects.isNull(sources)) {
        return;
      }
      final Pair<Long, Long> timePartitionIdBound =
          dataRegionId2TimePartitionIdBound.get(dataRegionId);
      sources.forEach(
          (id, source) -> source.setDataRegionTimePartitionIdBound(timePartitionIdBound));
    }
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeTimePartitionListenerHolder {

    private static final PipeTimePartitionListener INSTANCE = new PipeTimePartitionListener();

    private PipeTimePartitionListenerHolder() {
      // empty constructor
    }
  }

  public static PipeTimePartitionListener getInstance() {
    return PipeTimePartitionListener.PipeTimePartitionListenerHolder.INSTANCE;
  }

  private PipeTimePartitionListener() {
    // empty constructor
  }
}
