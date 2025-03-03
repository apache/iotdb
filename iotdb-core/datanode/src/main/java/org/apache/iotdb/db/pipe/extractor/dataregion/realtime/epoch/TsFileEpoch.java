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

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime.epoch;

import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionExtractorMetrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TsFileEpoch {

  private final String filePath;
  private final ConcurrentMap<PipeRealtimeDataRegionExtractor, AtomicReference<State>>
      dataRegionExtractor2State;
  private final AtomicLong insertNodeMinTime;

  public TsFileEpoch(String filePath) {
    this.filePath = filePath;
    this.dataRegionExtractor2State = new ConcurrentHashMap<>();
    this.insertNodeMinTime = new AtomicLong(Long.MAX_VALUE);
  }

  public TsFileEpoch.State getState(PipeRealtimeDataRegionExtractor extractor) {
    return dataRegionExtractor2State
        .computeIfAbsent(extractor, o -> new AtomicReference<>(State.EMPTY))
        .get();
  }

  public void migrateState(
      PipeRealtimeDataRegionExtractor extractor, TsFileEpochStateMigrator visitor) {
    dataRegionExtractor2State
        .computeIfAbsent(extractor, o -> new AtomicReference<>(State.EMPTY))
        .getAndUpdate(visitor::migrate);
  }

  public void setExtractorsRecentProcessedTsFileEpochState() {
    dataRegionExtractor2State.forEach(
        (extractor, state) ->
            PipeDataRegionExtractorMetrics.getInstance()
                .setRecentProcessedTsFileEpochState(extractor.getTaskID(), state.get()));
  }

  public void updateInsertNodeMinTime(long newComingMinTime) {
    insertNodeMinTime.updateAndGet(recordedMinTime -> Math.min(recordedMinTime, newComingMinTime));
  }

  public long getInsertNodeMinTime() {
    return insertNodeMinTime.get();
  }

  @Override
  public String toString() {
    return "TsFileEpoch{"
        + "filePath='"
        + filePath
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

    State(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }
  }
}
