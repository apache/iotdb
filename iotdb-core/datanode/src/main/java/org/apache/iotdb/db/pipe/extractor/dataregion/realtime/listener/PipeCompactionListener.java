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

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime.listener;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeCompactionListener {
  private final Map<String, Map<String, PipeRealtimeDataRegionExtractor>> dataRegionId2Extractors =
      new ConcurrentHashMap<>();

  private Map<String, PipeDataRegionCompactionRecorder> dataRegionId2CompactionRecorder =
      new ConcurrentHashMap<>();

  public synchronized void startListenTo(
      String dataRegionId, PipeRealtimeDataRegionExtractor extractor) {
    if (!PipeConfig.getInstance().isPipeRefreshFileListByCompactionEnabled()) {
      return;
    }

    dataRegionId2Extractors
        .computeIfAbsent(dataRegionId, o -> new ConcurrentHashMap<>())
        .put(extractor.getTaskID(), extractor);
  }

  public synchronized void stopListenTo(
      String dataRegionId, PipeRealtimeDataRegionExtractor extractor) {
    if (!PipeConfig.getInstance().isPipeRefreshFileListByCompactionEnabled()) {
      return;
    }

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

  public synchronized void listenToCompaction(
      String dataRegionId,
      @NonNull List<TsFileResource> sourceResources,
      @NonNull List<TsFileResource> targetResources) {
    if (Objects.isNull(dataRegionId2CompactionRecorder.get(dataRegionId))) {
      dataRegionId2CompactionRecorder.put(
          dataRegionId, new PipeDataRegionCompactionRecorder(sourceResources, targetResources));
    } else {
      dataRegionId2CompactionRecorder
          .get(dataRegionId)
          .addCompaction(sourceResources, targetResources);
    }
  }

  private void refreshFileList() {
    final Map<String, PipeDataRegionCompactionRecorder> dataRegionId2CompactionRecorderSnapshot;

    synchronized (this) {
      // Get a snapshot of this.dataRegionId2CompactionRecorder
      dataRegionId2CompactionRecorderSnapshot = this.dataRegionId2CompactionRecorder;
      this.dataRegionId2CompactionRecorder = new ConcurrentHashMap<>();
    }

    for (Map.Entry<String, Map<String, PipeRealtimeDataRegionExtractor>> entry :
        dataRegionId2Extractors.entrySet()) {
      if (dataRegionId2CompactionRecorderSnapshot.containsKey(entry.getKey())
          && !dataRegionId2CompactionRecorderSnapshot.get(entry.getKey()).isEmpty()) {
        for (PipeRealtimeDataRegionExtractor extractor : entry.getValue().values()) {
          extractor.refreshFileList(dataRegionId2CompactionRecorderSnapshot.get(entry.getKey()));
          // Should reset the "visited" bits.
          dataRegionId2CompactionRecorderSnapshot.get(entry.getKey()).resetVisited();
        }
      }
    }

    // Close to unpin TsFiles
    for (PipeDataRegionCompactionRecorder recorder :
        dataRegionId2CompactionRecorderSnapshot.values()) {
      recorder.close();
    }
  }

  /////////////////////////////// singleton ///////////////////////////////

  private PipeCompactionListener() {
    if (!PipeConfig.getInstance().isPipeRefreshFileListByCompactionEnabled()) {
      return;
    }

    PipeAgent.runtime()
        .registerPeriodicalJob(
            "PipeCompactionListener#refreshFileList()",
            this::refreshFileList,
            PipeConfig.getInstance().getPipeRefreshFileListByCompactionIntervalSeconds());
  }

  public static PipeCompactionListener getInstance() {
    return PipeCompactionListenerHolder.INSTANCE;
  }

  private static class PipeCompactionListenerHolder {
    private static final PipeCompactionListener INSTANCE = new PipeCompactionListener();
  }
}
