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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.disruptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

/**
 * Simplified Disruptor implementation for IoTDB Pipe
 *
 * <p>This implementation is based on LMAX Disruptor (https://github.com/LMAX-Exchange/disruptor)
 * and simplified for IoTDB's specific use case in the Pipe module.
 *
 * <p>Key simplifications:
 *
 * <ul>
 *   <li>Single event handler support (no complex dependency graphs)
 *   <li>Simplified lifecycle management
 *   <li>Removed wait strategies (using simple sleep-based waiting)
 * </ul>
 *
 * @param <T> event type
 */
public class Disruptor<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(Disruptor.class);

  private final RingBuffer<T> ringBuffer;
  private final ThreadFactory threadFactory;
  private BatchEventProcessor<T> processor;
  private Thread processorThread;
  private ExceptionHandler<? super T> exceptionHandler;
  private volatile boolean started = false;

  /**
   * Create a Disruptor instance
   *
   * @param eventFactory factory for creating pre-allocated events
   * @param ringBufferSize buffer size (must be power of 2)
   * @param threadFactory factory for creating consumer thread
   */
  public Disruptor(EventFactory<T> eventFactory, int ringBufferSize, ThreadFactory threadFactory) {
    this.ringBuffer = RingBuffer.createMultiProducer(eventFactory, ringBufferSize);
    this.threadFactory = threadFactory;
  }

  /**
   * Configure event handler for processing events
   *
   * <p>Creates a batch event processor that will run in its own thread
   *
   * @param handler event handler implementation
   * @return this instance for method chaining
   */
  public Disruptor<T> handleEventsWith(final EventHandler<? super T> handler) {
    SequenceBarrier barrier = ringBuffer.newBarrier();
    processor = new BatchEventProcessor<>(ringBuffer, barrier, handler);

    if (exceptionHandler != null) {
      processor.setExceptionHandler(exceptionHandler);
    }

    ringBuffer.addGatingSequences(processor.getSequence());
    return this;
  }

  /**
   * Set exception handler for error handling
   *
   * @param exceptionHandler handler for processing exceptions
   */
  public void setDefaultExceptionHandler(ExceptionHandler<? super T> exceptionHandler) {
    this.exceptionHandler = exceptionHandler;
    if (processor != null) {
      processor.setExceptionHandler(exceptionHandler);
    }
  }

  public RingBuffer<T> start() {
    if (started) {
      throw new IllegalStateException("Disruptor already started");
    }

    if (processor == null) {
      throw new IllegalStateException("No event handler configured");
    }

    processorThread = threadFactory.newThread(processor);
    processorThread.start();
    started = true;

    LOGGER.info("Disruptor started with buffer size: {}", ringBuffer.getBufferSize());
    return ringBuffer;
  }

  public void shutdown() {
    if (!started) {
      return;
    }

    if (processor != null) {
      processor.halt();
    }

    if (processorThread != null) {
      try {
        processorThread.join(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupted waiting for processor to stop");
      }
    }

    started = false;
    LOGGER.info("Disruptor shutdown completed");
  }
}
