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

package org.apache.iotdb.db.pipe.extractor.realtime.listener;

import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeTimePartitionListener {

  private final Map<String, Map<String, PipeRealtimeDataRegionExtractor>> dataRegionId2Extractors =
      new ConcurrentHashMap<>();

  private final Map<String, Pair<Long, Long>> dataRegionId2TimePartitionIdBound =
      new ConcurrentHashMap<>();

  //////////////////////////// start & stop ////////////////////////////

  public synchronized void startListen(
      String dataRegionId, PipeRealtimeDataRegionExtractor extractor) {
    dataRegionId2Extractors
        .computeIfAbsent(dataRegionId, o -> new HashMap<>())
        .put(extractor.getTaskID(), extractor);
  }

  public synchronized void stopListen(
      String dataRegionId, PipeRealtimeDataRegionExtractor extractor) {
    Map<String, PipeRealtimeDataRegionExtractor> extractors =
        dataRegionId2Extractors.get(dataRegionId);
    if (Objects.isNull(extractors)) {
      return;
    }
    extractors.remove(extractor.getTaskID());
    if (extractors.isEmpty()) {
      dataRegionId2Extractors.remove(dataRegionId);
    }
  }

  //////////////////////////// listen to changes ////////////////////////////

  public void listenToTimePartitionGrow(
      String dataRegionId, Pair<Long, Long> newTimePartitionIdBound) {
    boolean shouldBroadcastTimePartitionChange = false;
    Pair<Long, Long> oldTimePartitionBound = dataRegionId2TimePartitionIdBound.get(dataRegionId);

    if (Objects.isNull(oldTimePartitionBound)) {
      dataRegionId2TimePartitionIdBound.put(dataRegionId, newTimePartitionIdBound);
      shouldBroadcastTimePartitionChange = true;
    } else if (newTimePartitionIdBound.left < oldTimePartitionBound.left
        || oldTimePartitionBound.right < newTimePartitionIdBound.right) {
      dataRegionId2TimePartitionIdBound.put(
          dataRegionId,
          new Pair<>(
              Math.min(oldTimePartitionBound.left, newTimePartitionIdBound.left),
              Math.max(oldTimePartitionBound.right, newTimePartitionIdBound.right)));
      shouldBroadcastTimePartitionChange = true;
    }

    if (shouldBroadcastTimePartitionChange) {
      dataRegionId2Extractors
          .get(dataRegionId)
          .forEach(
              (id, extractor) ->
                  extractor.setDataRegionTimePartitionIdBound(
                      dataRegionId2TimePartitionIdBound.get(dataRegionId)));
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
