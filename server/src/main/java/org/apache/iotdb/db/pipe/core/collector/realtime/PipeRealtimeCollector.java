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

package org.apache.iotdb.db.pipe.core.collector.realtime;

import org.apache.iotdb.db.pipe.core.collector.PipeCollector;
import org.apache.iotdb.db.pipe.core.event.realtime.PipeRealtimeCollectEvent;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class PipeRealtimeCollector implements PipeCollector {
  protected final String pattern;
  protected final String dataRegionId;
  protected final PipeRealtimeCollectorManager manager;
  protected final AtomicBoolean hasBeenStarted;

  public PipeRealtimeCollector(
      String pattern, String dataRegionId, PipeRealtimeCollectorManager manager) {
    this.pattern = pattern;
    this.dataRegionId = dataRegionId;
    this.manager = manager;
    this.hasBeenStarted = new AtomicBoolean(false);
  }

  public final String getPattern() {
    return pattern;
  }

  public abstract void collectEvent(PipeRealtimeCollectEvent event);

  @Override
  public void start() {
    manager.register(this, dataRegionId);
    hasBeenStarted.set(true);
  }

  @Override
  public boolean hasBeenStarted() {
    return hasBeenStarted.get();
  }

  @Override
  public void close() {
    manager.deregister(this, dataRegionId);
  }

  @Override
  public String toString() {
    return "PipeRealtimeCollector{"
        + "pattern='"
        + pattern
        + '\''
        + ", dataRegionId='"
        + dataRegionId
        + '\''
        + ", hasBeenStarted="
        + hasBeenStarted
        + '}';
  }
}
