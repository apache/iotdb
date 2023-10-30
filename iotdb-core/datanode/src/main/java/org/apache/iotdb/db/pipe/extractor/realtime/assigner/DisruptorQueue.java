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

package org.apache.iotdb.db.pipe.extractor.realtime.assigner;

import org.apache.iotdb.commons.concurrent.IoTDBDaemonThreadFactory;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.metric.PipeEventCounter;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import static org.apache.iotdb.commons.concurrent.ThreadName.PIPE_EXTRACTOR_DISRUPTOR;

public class DisruptorQueue {

  private static final IoTDBDaemonThreadFactory THREAD_FACTORY =
      new IoTDBDaemonThreadFactory(PIPE_EXTRACTOR_DISRUPTOR.getName());

  private final Disruptor<EventContainer> disruptor;
  private final RingBuffer<EventContainer> ringBuffer;

  private final PipeMemoryBlock block;

  private final PipeEventCounter eventCounter = new PipeEventCounter();

  public DisruptorQueue(EventHandler<PipeRealtimeEvent> eventHandler) {
    PipeConfig config = PipeConfig.getInstance();
    final long ringBufferEntrySizeInBytes =
        config.getPipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes();
    final int ringBufferSize = config.getPipeExtractorAssignerDisruptorRingBufferSize();

    block = PipeResourceManager.memory().tryAllocate(ringBufferSize * ringBufferEntrySizeInBytes);

    disruptor =
        new Disruptor<>(
            EventContainer::new,
            Math.toIntExact(block.getMemoryUsageInBytes() / ringBufferEntrySizeInBytes),
            THREAD_FACTORY,
            ProducerType.MULTI,
            new BlockingWaitStrategy());
    disruptor.handleEventsWith(
        (container, sequence, endOfBatch) -> {
          eventHandler.onEvent(container.getEvent(), sequence, endOfBatch);
          EnrichedEvent innerEvent = container.getEvent().getEvent();
          eventCounter.decreaseEventCount(innerEvent);
        });
    disruptor.setDefaultExceptionHandler(new DisruptorQueueExceptionHandler());

    ringBuffer = disruptor.start();
  }

  public void publish(PipeRealtimeEvent event) {
    EnrichedEvent internalEvent = event.getEvent();
    if (internalEvent instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) internalEvent).recordDisruptorSize(ringBuffer);
    }
    ringBuffer.publishEvent((container, sequence, o) -> container.setEvent(event), event);
    eventCounter.increaseEventCount(internalEvent);
  }

  public void clear() {
    disruptor.halt();
    block.close();
  }

  private static class EventContainer {

    private PipeRealtimeEvent event;

    private EventContainer() {}

    public PipeRealtimeEvent getEvent() {
      return event;
    }

    public void setEvent(PipeRealtimeEvent event) {
      this.event = event;
    }
  }

  public int getTabletInsertionEventCount() {
    return eventCounter.getTabletInsertionEventCount();
  }

  public int getTsFileInsertionEventCount() {
    return eventCounter.getTsFileInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return eventCounter.getPipeHeartbeatEventCount();
  }
}
