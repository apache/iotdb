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

import org.apache.iotdb.db.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.core.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.db.pipe.core.event.realtime.TsFileEpoch;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.EventType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

public class PipeRealtimeHybridCollector extends PipeRealtimeCollector {
  private static final Logger logger = LoggerFactory.getLogger(PipeRealtimeHybridCollector.class);
  private final ArrayBlockingQueue<PipeRealtimeCollectEvent> pendingQueue;

  public PipeRealtimeHybridCollector(
      String pattern, String dataRegionId, PipeRealtimeCollectorManager manager) {
    super(pattern, dataRegionId, manager);
    this.pendingQueue =
        new ArrayBlockingQueue<>(
            PipeConfig.getInstance().getRealtimeCollectorPendingQueueCapacity());
  }

  @Override
  public void collectEvent(PipeRealtimeCollectEvent event) {
    if (event.getEvent().getType().equals(EventType.TABLET_INSERTION)) { // offer tablet event
      if (approachingCapacity()) {
        event.getTsFileEpoch().visit(this, state -> TsFileEpoch.State.USING_TSFILE);
      }
      if (!event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TSFILE)) {
        pendingQueue.offer(event);
      }
    } else { // offer tsfile event
      event
          .getTsFileEpoch()
          .visit(
              this,
              state ->
                  (state.equals(TsFileEpoch.State.EMPTY)) ? TsFileEpoch.State.USING_TSFILE : state);
      if (!pendingQueue.offer(event)) {
        logger.warn(
            String.format(
                "Pending Queue of Hybrid Realtime Collector %s has reached capacity, discard TsFile Event %s, current state %s",
                this, event, event.getTsFileEpoch().getState(this)));
        // TODO: Exception collect
      }
    }
  }

  private boolean approachingCapacity() {
    return pendingQueue.size()
        >= PipeConfig.getInstance().getRealtimeCollectorPendingQueueTabletLimit();
  }

  @Override
  public Event supply() {
    PipeRealtimeCollectEvent collectEvent = pendingQueue.poll();

    while (collectEvent != null) {
      Event event = collectEvent.getEvent();
      if (event.getType().equals(EventType.TABLET_INSERTION)) {
        collectEvent
            .getTsFileEpoch()
            .visit(
                this,
                state ->
                    (state.equals(TsFileEpoch.State.EMPTY)) ? TsFileEpoch.State.USING_WAL : state);
        if (collectEvent.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_WAL)) {
          return event;
        }
      } else {
        collectEvent
            .getTsFileEpoch()
            .visit(
                this,
                state -> {
                  if (state.equals(TsFileEpoch.State.EMPTY)) {
                    logger.warn(
                        String.format("EMPTY TsFileEpoch when supplying TsFile Event %s", event));
                    return TsFileEpoch.State.USING_TSFILE;
                  }
                  return state;
                });
        if (collectEvent.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TSFILE)) {
          return event;
        }
      }

      collectEvent = pendingQueue.poll();
    }
    return null;
  }
}
