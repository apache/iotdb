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

package org.apache.iotdb.db.pipe.collector.realtime.assigner;

import org.apache.iotdb.commons.pipe.config.PipeConfig;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class DisruptorQueue<E> {
  private Disruptor<Container<E>> disruptor;
  private RingBuffer<Container<E>> ringBuffer;

  private DisruptorQueue() {}

  public void publish(E obj) {
    ringBuffer.publishEvent((container, sequence, o) -> container.setObj(o), obj);
  }

  public void clear() {
    disruptor.halt();
  }

  public static class Builder<E> {
    private int ringBufferSize =
        PipeConfig.getInstance().getPipeCollectorAssignerDisruptorRingBufferSize();
    private ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
    private ProducerType producerType = ProducerType.MULTI;
    private WaitStrategy waitStrategy = new BlockingWaitStrategy();
    private final List<EventHandler<E>> handlers = new ArrayList<>();

    public Builder<E> setRingBufferSize(int ringBufferSize) {
      this.ringBufferSize = ringBufferSize;
      return this;
    }

    public Builder<E> setThreadFactory(ThreadFactory threadFactory) {
      this.threadFactory = threadFactory;
      return this;
    }

    public Builder<E> setProducerType(ProducerType producerType) {
      this.producerType = producerType;
      return this;
    }

    public Builder<E> setWaitStrategy(WaitStrategy waitStrategy) {
      this.waitStrategy = waitStrategy;
      return this;
    }

    public Builder<E> addEventHandler(EventHandler<E> eventHandler) {
      this.handlers.add(eventHandler);
      return this;
    }

    public DisruptorQueue<E> build() {
      DisruptorQueue<E> disruptorQueue = new DisruptorQueue<>();
      disruptorQueue.disruptor =
          new Disruptor<>(
              Container::new, ringBufferSize, threadFactory, producerType, waitStrategy);
      for (EventHandler<E> handler : handlers) {
        disruptorQueue.disruptor.handleEventsWith(
            (container, sequence, endOfBatch) ->
                handler.onEvent(container.getObj(), sequence, endOfBatch));
      }
      disruptorQueue.disruptor.start();
      disruptorQueue.ringBuffer = disruptorQueue.disruptor.getRingBuffer();
      return disruptorQueue;
    }
  }

  private static class Container<E> {
    private E obj;

    private Container() {}

    public E getObj() {
      return obj;
    }

    public void setObj(E obj) {
      this.obj = obj;
    }
  }
}
