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

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime.assigner;

import org.apache.iotdb.commons.concurrent.IoTDBDaemonThreadFactory;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.function.Consumer;

import static org.apache.iotdb.commons.concurrent.ThreadName.PIPE_EXTRACTOR_DISRUPTOR;

public class DisruptorQueue {

  private static final IoTDBDaemonThreadFactory THREAD_FACTORY =
      new IoTDBDaemonThreadFactory(PIPE_EXTRACTOR_DISRUPTOR.getName());

  private final PipeMemoryBlock allocatedMemoryBlock;
  private final Disruptor<EventContainer> disruptor;
  private final RingBuffer<EventContainer> ringBuffer;

  private volatile boolean isClosed = false;

  public DisruptorQueue(
      final EventHandler<PipeRealtimeEvent> eventHandler,
      final Consumer<PipeRealtimeEvent> onAssignedHook) {
    final PipeConfig config = PipeConfig.getInstance();
    final int ringBufferSize = config.getPipeExtractorAssignerDisruptorRingBufferSize();
    final long ringBufferEntrySizeInBytes =
        config.getPipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes();

    allocatedMemoryBlock =
        PipeDataNodeResourceManager.memory()
            .tryAllocate(
                ringBufferSize * ringBufferEntrySizeInBytes, currentSize -> currentSize / 2);

    disruptor =
        new Disruptor<>(
            EventContainer::new,
            Math.max(
                32,
                Math.toIntExact(
                    allocatedMemoryBlock.getMemoryUsageInBytes() / ringBufferEntrySizeInBytes)),
            THREAD_FACTORY,
            ProducerType.MULTI,
            new BlockingWaitStrategy());
    disruptor.handleEventsWith(
        (container, sequence, endOfBatch) -> {
          final PipeRealtimeEvent realtimeEvent = container.getEvent();
          eventHandler.onEvent(realtimeEvent, sequence, endOfBatch);
          onAssignedHook.accept(realtimeEvent);
        });
    disruptor.setDefaultExceptionHandler(new DisruptorQueueExceptionHandler());

    ringBuffer = disruptor.start();
  }

  public void publish(final PipeRealtimeEvent event) {
    final EnrichedEvent innerEvent = event.getEvent();
    if (innerEvent instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) innerEvent).recordDisruptorSize(ringBuffer);
    }
    ringBuffer.publishEvent((container, sequence, o) -> container.setEvent(event), event);
  }

  public void shutdown() {
    isClosed = true;
    // use shutdown instead of halt to ensure all published events have been handled
    disruptor.shutdown();
    allocatedMemoryBlock.close();
  }

  public boolean isClosed() {
    return isClosed;
  }

  private static class EventContainer {

    private PipeRealtimeEvent event;

    private EventContainer() {}

    public PipeRealtimeEvent getEvent() {
      return event;
    }

    public void setEvent(final PipeRealtimeEvent event) {
      this.event = event;
    }
  }
}
