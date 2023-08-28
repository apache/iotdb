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
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;

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

  public DisruptorQueue(EventHandler<PipeRealtimeEvent> eventHandler) {
    disruptor =
        new Disruptor<>(
            EventContainer::new,
            PipeConfig.getInstance().getPipeExtractorAssignerDisruptorRingBufferSize(),
            THREAD_FACTORY,
            ProducerType.MULTI,
            new BlockingWaitStrategy());
    disruptor.handleEventsWith(
        (container, sequence, endOfBatch) ->
            eventHandler.onEvent(container.getEvent(), sequence, endOfBatch));
    disruptor.setDefaultExceptionHandler(new DisruptorQueueExceptionHandler());

    ringBuffer = disruptor.start();
  }

  public void publish(PipeRealtimeEvent event) {
    ringBuffer.publishEvent((container, sequence, o) -> container.setEvent(event), event);
  }

  public void clear() {
    disruptor.halt();
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
}
