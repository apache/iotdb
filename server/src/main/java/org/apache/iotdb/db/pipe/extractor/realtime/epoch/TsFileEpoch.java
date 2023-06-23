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

package org.apache.iotdb.db.pipe.extractor.realtime.epoch;

import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class TsFileEpoch {

  private final String filePath;
  private final ConcurrentMap<PipeRealtimeDataRegionExtractor, AtomicReference<State>>
      dataRegionExtractor2State;

  public TsFileEpoch(String filePath) {
    this.filePath = filePath;
    this.dataRegionExtractor2State = new ConcurrentHashMap<>();
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

  @Override
  public String toString() {
    return "TsFileEpoch{"
        + "filePath='"
        + filePath
        + '\''
        + ", dataRegionExtractor2State="
        + dataRegionExtractor2State
        + '}';
  }

  public enum State {
    EMPTY,
    USING_TABLET,
    USING_TSFILE
  }
}
