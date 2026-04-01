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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch;

import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionSourceMetrics;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TsFileEpoch {

  private final TsFileResource resource;
  private final ConcurrentMap<PipeRealtimeDataRegionSource, AtomicReference<State>>
      dataRegionExtractor2State;
  private final AtomicLong insertNodeMinTime;

  public TsFileEpoch(final TsFileResource resource) {
    this.resource = resource;
    this.dataRegionExtractor2State = new ConcurrentHashMap<>();
    this.insertNodeMinTime = new AtomicLong(Long.MAX_VALUE);
  }

  public TsFileEpoch.State getState(final PipeRealtimeDataRegionSource extractor) {
    AtomicReference<State> stateRef = dataRegionExtractor2State.get(extractor);

    if (stateRef == null) {
      dataRegionExtractor2State.putIfAbsent(
          extractor, stateRef = new AtomicReference<>(State.EMPTY));
      extractor.increaseExtractEpochSize();
      setExtractorsRecentProcessedTsFileEpochState();
    }

    return stateRef.get();
  }

  public void migrateState(
      final PipeRealtimeDataRegionSource extractor, final TsFileEpochStateMigrator visitor) {
    AtomicReference<State> stateRef = dataRegionExtractor2State.get(extractor);

    if (stateRef == null) {
      dataRegionExtractor2State.putIfAbsent(
          extractor, stateRef = new AtomicReference<>(State.EMPTY));
      extractor.increaseExtractEpochSize();
      setExtractorsRecentProcessedTsFileEpochState();
    }

    State migratedState = visitor.migrate(stateRef.get());
    if (!Objects.equals(stateRef.get(), migratedState)) {
      stateRef.set(migratedState);
      setExtractorsRecentProcessedTsFileEpochState();
    }
  }

  public void clearState(final PipeRealtimeDataRegionSource extractor) {
    if (dataRegionExtractor2State.containsKey(extractor)) {
      extractor.decreaseExtractEpochSize();
    }
    if (extractor.extractEpochSizeIsEmpty()) {
      PipeDataRegionSourceMetrics.getInstance()
          .setRecentProcessedTsFileEpochState(extractor.getTaskID(), State.EMPTY);
    }
  }

  public void setExtractorsRecentProcessedTsFileEpochState() {
    dataRegionExtractor2State.forEach(
        (extractor, state) ->
            PipeDataRegionSourceMetrics.getInstance()
                .setRecentProcessedTsFileEpochState(extractor.getTaskID(), state.get()));
  }

  public void updateInsertNodeMinTime(final long newComingMinTime) {
    insertNodeMinTime.updateAndGet(recordedMinTime -> Math.min(recordedMinTime, newComingMinTime));
  }

  public TsFileResource getResource() {
    return resource;
  }

  public String getFilePath() {
    return resource.getTsFilePath();
  }

  @Override
  public String toString() {
    return "TsFileEpoch{"
        + "resource='"
        + resource
        + '\''
        + ", dataRegionExtractor2State="
        + dataRegionExtractor2State
        + '\''
        + ", insertNodeMinTime="
        + insertNodeMinTime.get()
        + '}';
  }

  public enum State {
    EMPTY(0),
    USING_TABLET(1),
    USING_BOTH(2),
    USING_TSFILE(3);

    private final int id;

    State(final int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }
  }
}
