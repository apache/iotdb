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
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.collector.PipeCollectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

// TODO: make this collector as a builtin pipe plugin. register it in BuiltinPipePlugin.
public class PipeRealtimeHybridDataRegionCollector extends PipeRealtimeDataRegionCollector {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeHybridDataRegionCollector.class);

  // TODO: memory control
  // This queue is used to store pending events collected by the method collect(). The method
  // supply() will poll events from this queue and send them to the next pipe plugin.
  private final ArrayBlockingQueue<PipeRealtimeCollectEvent> pendingQueue;

  public PipeRealtimeHybridDataRegionCollector(String pattern, String dataRegionId) {
    super(pattern, dataRegionId);
    this.pendingQueue =
        new ArrayBlockingQueue<>(
            PipeConfig.getInstance().getRealtimeCollectorPendingQueueCapacity());
  }

  @Override
  public void customize(PipeParameters parameters, PipeCollectorRuntimeConfiguration configuration)
      throws Exception {
    throw new NotImplementedException("Not implement for customize.");
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    throw new NotImplementedException("Not implement for validate.");
  }

  @Override
  public void collect(PipeRealtimeCollectEvent event) {
    switch (event.getEvent().getType()) {
      case TABLET_INSERTION:
        collectTabletInsertion(event);
        break;
      case TSFILE_INSERTION:
        collectTsFileInsertion(event);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported event type %s for Hybrid Realtime Collector %s",
                event.getEvent().getType(), this));
    }
  }

  private void collectTabletInsertion(PipeRealtimeCollectEvent event) {
    if (isApproachingCapacity()) {
      event.getTsFileEpoch().migrateState(this, state -> TsFileEpoch.State.USING_TSFILE);
      // if the pending queue is approaching capacity, we should not collect any more tablet events.
      // all the data represented by the tablet events should be carried by the following tsfile
      // event.
      return;
    }

    if (!event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TSFILE)) {
      pendingQueue.offer(event);
    }
  }

  private void collectTsFileInsertion(PipeRealtimeCollectEvent event) {
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state ->
                state.equals(TsFileEpoch.State.EMPTY) ? TsFileEpoch.State.USING_TSFILE : state);

    if (!pendingQueue.offer(event)) {
      LOGGER.warn(
          String.format(
              "Pending Queue of Hybrid Realtime Collector %s has reached capacity, discard TsFile Event %s, current state %s",
              this, event, event.getTsFileEpoch().getState(this)));
      // TODO: more degradation strategies
      // TODO: dynamic control of the pending queue capacity
      // TODO: should be handled by the PipeRuntimeAgent
    }
  }

  private boolean isApproachingCapacity() {
    return pendingQueue.size()
        >= PipeConfig.getInstance().getRealtimeCollectorPendingQueueTabletLimit();
  }

  @Override
  public Event supply() {
    PipeRealtimeCollectEvent collectEvent = pendingQueue.poll();

    while (collectEvent != null) {
      Event suppliedEvent;
      switch (collectEvent.getEvent().getType()) {
        case TABLET_INSERTION:
          suppliedEvent = supplyTabletInsertion(collectEvent);
          break;
        case TSFILE_INSERTION:
          suppliedEvent = supplyTsFileInsertion(collectEvent);
          break;
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "Unsupported event type %s for Hybrid Realtime Collector %s",
                  collectEvent.getEvent().getType(), this));
      }
      if (suppliedEvent != null) {
        return suppliedEvent;
      }

      collectEvent = pendingQueue.poll();
    }

    // means the pending queue is empty.
    return null;
  }

  private Event supplyTabletInsertion(PipeRealtimeCollectEvent event) {
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state ->
                (state.equals(TsFileEpoch.State.EMPTY)) ? TsFileEpoch.State.USING_TABLET : state);

    if (event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TABLET)) {
      return event.getEvent();
    }
    // if the state is USING_TSFILE, discard the event and poll the next one.
    return null;
  }

  private Event supplyTsFileInsertion(PipeRealtimeCollectEvent event) {
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state -> {
              // this would not happen, but just in case.
              if (state.equals(TsFileEpoch.State.EMPTY)) {
                LOGGER.warn(
                    String.format("EMPTY TsFileEpoch when supplying TsFile Event %s", event));
                return TsFileEpoch.State.USING_TSFILE;
              }
              return state;
            });

    if (event.getTsFileEpoch().getState(this).equals(TsFileEpoch.State.USING_TSFILE)) {
      return event.getEvent();
    }
    // if the state is USING_TABLET, discard the event and poll the next one.
    return null;
  }

  @Override
  public void close() {
    super.close();
    pendingQueue.clear();
  }
}
