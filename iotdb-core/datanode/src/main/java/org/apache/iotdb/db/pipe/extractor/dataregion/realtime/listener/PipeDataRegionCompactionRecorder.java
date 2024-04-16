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

import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEventFactory;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class is used to record whenever a compaction is performed. It records the source files and
 * target files of a compaction, and build {@link PipeRealtimeEvent} for each target file. When an
 * extractor refreshes its pending file list, it will check if each file is a source file of a
 * compaction, and if so, it will remove the file and add the target files of this compaction to its
 * pending queue.
 */
public class PipeDataRegionCompactionRecorder implements AutoCloseable {
  // Map from each source file path to a pair of target files and a boolean indicating
  // whether the target files have been visited (to avoid adding the same file twice).
  // All source files of one compaction will be mapped to the same value.
  private final Map<String, Pair<List<PipeRealtimeEvent>, Boolean>> sourcePath2targetEvents;

  public PipeDataRegionCompactionRecorder() {
    this.sourcePath2targetEvents = new HashMap<>();
  }

  public PipeDataRegionCompactionRecorder(
      List<TsFileResource> sourceList, List<TsFileResource> targetList) {
    this.sourcePath2targetEvents = new HashMap<>();
    for (TsFileResource source : sourceList) {
      this.sourcePath2targetEvents.put(
          source.getTsFilePath(),
          new Pair<>(
              targetList.stream()
                  .map(
                      resource -> {
                        // Build a PipeRealtimeEvent for each target file and increase its reference
                        // count,
                        // TsFile will be pinned here.
                        PipeRealtimeEvent event =
                            PipeRealtimeEventFactory.createRealtimeEvent(resource, false, false);
                        event.increaseReferenceCount(
                            PipeDataRegionCompactionRecorder.class.getName());
                        return event;
                      })
                  .collect(Collectors.toList()),
              false));
    }
  }

  public void addCompaction(List<TsFileResource> sourceList, List<TsFileResource> targetList) {
    for (TsFileResource source : sourceList) {
      this.sourcePath2targetEvents.put(
          source.getTsFilePath(),
          new Pair<>(
              targetList.stream()
                  .map(
                      resource -> {
                        // Build a PipeRealtimeEvent for each target file and increase its reference
                        // count,
                        // TsFile will be pinned here.
                        PipeRealtimeEvent event =
                            PipeRealtimeEventFactory.createRealtimeEvent(resource, false, false);
                        event.increaseReferenceCount(
                            PipeDataRegionCompactionRecorder.class.getName());
                        return event;
                      })
                  .collect(Collectors.toList()),
              false));
    }
  }

  public Pair<List<PipeRealtimeEvent>, Boolean> getTargetRealtimeEventList(String sourcePath) {
    return this.sourcePath2targetEvents.get(sourcePath);
  }

  /** Reset the visit state. */
  public void resetVisited() {
    for (Pair<List<PipeRealtimeEvent>, Boolean> pair : this.sourcePath2targetEvents.values()) {
      pair.right = false;
    }
  }

  public boolean isEmpty() {
    return this.sourcePath2targetEvents.isEmpty();
  }

  @Override
  public void close() {
    for (Pair<List<PipeRealtimeEvent>, Boolean> pair : this.sourcePath2targetEvents.values()) {
      for (PipeRealtimeEvent event : pair.left) {
        // Decrease the reference count.
        event.decreaseReferenceCount(PipeDataRegionCompactionRecorder.class.getName(), false);
      }
    }
  }
}
