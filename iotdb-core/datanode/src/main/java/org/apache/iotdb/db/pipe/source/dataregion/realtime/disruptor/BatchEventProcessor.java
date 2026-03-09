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

/**
 * Batch event processor for consuming events
 *
 * <p>This implementation is based on LMAX Disruptor (https://github.com/LMAX-Exchange/disruptor)
 * and simplified for IoTDB's Pipe module (removed complex lifecycle management).
 *
 * <p>Core algorithm preserved from LMAX Disruptor:
 *
 * <ul>
 *   <li>Batch processing loop
 *   <li>Sequence tracking
 *   <li>endOfBatch detection
 * </ul>
 *
 * @param <T> event type
 */
public final class BatchEventProcessor<T> implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchEventProcessor.class);

  private final RingBuffer<T> ringBuffer;
  private final SequenceBarrier sequenceBarrier;
  private final EventHandler<? super T> eventHandler;
  private final Sequence sequence = new Sequence();
  private ExceptionHandler<? super T> exceptionHandler = new DefaultExceptionHandler<>();
  private volatile boolean running = true;

  public BatchEventProcessor(
      RingBuffer<T> ringBuffer, SequenceBarrier barrier, EventHandler<? super T> eventHandler) {
    this.ringBuffer = ringBuffer;
    this.sequenceBarrier = barrier;
    this.eventHandler = eventHandler;
  }

  public Sequence getSequence() {
    return sequence;
  }

  public void setExceptionHandler(ExceptionHandler<? super T> exceptionHandler) {
    this.exceptionHandler = exceptionHandler;
  }

  public void halt() {
    running = false;
  }

  @Override
  public void run() {
    T event = null;
    long nextSequence = sequence.get() + 1L;

    while (running) {
      try {
        // Wait for available sequence
        final long availableSequence = sequenceBarrier.waitFor(nextSequence);

        // Batch process all available events
        while (nextSequence <= availableSequence) {
          event = ringBuffer.get(nextSequence);
          eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
          nextSequence++;
        }

        // Update sequence
        sequence.set(availableSequence);

      } catch (final InterruptedException ex) {
        Thread.currentThread().interrupt();
        LOGGER.info("Processor interrupted");
        break;
      } catch (final Throwable ex) {
        exceptionHandler.handleEventException(ex, nextSequence, event);
        sequence.set(nextSequence);
        nextSequence++;
      }
    }

    LOGGER.info("Processor stopped");
  }

  private static class DefaultExceptionHandler<T> implements ExceptionHandler<T> {
    @Override
    public void handleEventException(Throwable ex, long sequence, T event) {
      LoggerFactory.getLogger(getClass()).error("Exception processing: {} {}", sequence, event, ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
      LoggerFactory.getLogger(getClass()).error("Exception during onStart()", ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
      LoggerFactory.getLogger(getClass()).error("Exception during onShutdown()", ex);
    }
  }
}
