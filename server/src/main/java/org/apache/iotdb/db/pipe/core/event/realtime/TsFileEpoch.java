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

package org.apache.iotdb.db.pipe.core.event.realtime;

import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeCollector;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class TsFileEpoch {
  private final String filePath;
  private final ConcurrentMap<PipeRealtimeCollector, AtomicReference<State>> collector2State;

  public TsFileEpoch(String filePath) {
    this.filePath = filePath;
    this.collector2State = new ConcurrentHashMap<>();
  }

  public TsFileEpoch.State getState(PipeRealtimeCollector collector) {
    return collector2State.get(collector).get();
  }

  public void visit(PipeRealtimeCollector collector, TsFileEpochVisitor visitor) {
    collector2State
        .computeIfAbsent(collector, o -> new AtomicReference<>(State.EMPTY))
        .getAndUpdate(visitor::executeFromState);
  }

  @Override
  public String toString() {
    return "TsFileEpoch{"
        + "filePath='"
        + filePath
        + '\''
        + ", collector2State="
        + collector2State
        + '}';
  }

  public enum State {
    EMPTY,
    USING_WAL,
    USING_TSFILE
  }
}
