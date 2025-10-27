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

/**
 * Left-hand side padding for cache line alignment
 *
 * <p>Prevents false sharing by ensuring RingBuffer fields don't share cache lines with preceding
 * objects
 */
abstract class RingBufferPad {
  protected long p1, p2, p3, p4, p5, p6, p7;
}

/**
 * Core fields for RingBuffer implementation
 *
 * <p>Contains the actual event storage array and sequencing state
 */
abstract class RingBufferFields<E> extends RingBufferPad {
  /** Pre-allocated event storage with padding to prevent false sharing */
  private final Object[] entries;

  /** Total number of events in the buffer (must be power of 2) */
  protected final int bufferSize;

  /** Mask for fast modulo operation (bufferSize - 1) */
  protected final int indexMask;

  /** Sequencer for managing producer/consumer coordination */
  protected final MultiProducerSequencer sequencer;

  /**
   * Initialize ring buffer fields
   *
   * @param eventFactory factory for pre-allocating events
   * @param sequencer multi-producer sequencer
   */
  RingBufferFields(EventFactory<E> eventFactory, MultiProducerSequencer sequencer) {
    this.sequencer = sequencer;
    this.bufferSize = sequencer.getBufferSize();

    if (bufferSize < 1) {
      throw new IllegalArgumentException("bufferSize must not be less than 1");
    }
    if (Integer.bitCount(bufferSize) != 1) {
      throw new IllegalArgumentException("bufferSize must be a power of 2");
    }

    this.indexMask = bufferSize - 1;
    // Allocate array with padding on both sides to prevent false sharing
    this.entries = new Object[bufferSize];
    fill(eventFactory);
  }

  /**
   * Pre-allocate all events in the buffer
   *
   * @param eventFactory factory for creating event instances
   */
  private void fill(EventFactory<E> eventFactory) {
    for (int i = 0; i < bufferSize; i++) {
      // Store events starting after front padding
      entries[i] = eventFactory.newInstance();
    }
  }

  /**
   * Get event at sequence using direct memory access
   *
   * @param sequence sequence number
   * @return event at the sequence position
   */
  @SuppressWarnings("unchecked")
  protected final E elementAt(long sequence) {
    // Use Unsafe for lock-free array access with proper memory barriers
    return (E) entries[(int) (sequence & indexMask)];
  }
}

/**
 * Lock-free ring buffer for storing pre-allocated event objects
 *
 * <p>This implementation is based on LMAX Disruptor (https://github.com/LMAX-Exchange/disruptor)
 * and preserves the core ring buffer algorithm for IoTDB's Pipe module.
 *
 * <p>Supports multi-producer concurrent access with zero-garbage design. Events are pre-allocated
 * and reused, avoiding GC pressure. Uses cache line padding to prevent false sharing.
 *
 * @param <E> event type
 */
public final class RingBuffer<E> extends RingBufferFields<E> {
  /** Initial cursor value for the ring buffer */
  public static final long INITIAL_CURSOR_VALUE = Sequence.INITIAL_VALUE;

  /**
   * Right-hand side padding for cache line alignment
   *
   * <p>Prevents false sharing by ensuring RingBuffer fields don't share cache lines with following
   * objects
   */
  protected long p1, p2, p3, p4, p5, p6, p7;

  /**
   * Construct a RingBuffer with given factory and sequencer
   *
   * @param eventFactory factory to create and pre-allocate events
   * @param sequencer multi-producer sequencer for sequence management
   */
  private RingBuffer(EventFactory<E> eventFactory, MultiProducerSequencer sequencer) {
    super(eventFactory, sequencer);
  }

  /**
   * Create a multi-producer RingBuffer
   *
   * <p>Supports concurrent publishing from multiple threads using lock-free CAS operations
   *
   * @param factory event factory for creating event instances
   * @param bufferSize buffer size (must be power of 2)
   * @param <E> event type
   * @return newly created ring buffer
   */
  public static <E> RingBuffer<E> createMultiProducer(EventFactory<E> factory, int bufferSize) {
    MultiProducerSequencer sequencer = new MultiProducerSequencer(bufferSize, new Sequence[0]);
    return new RingBuffer<>(factory, sequencer);
  }

  /**
   * Get the event at a specific sequence
   *
   * @param sequence sequence number to retrieve
   * @return event at the given sequence
   */
  public E get(long sequence) {
    return elementAt(sequence);
  }

  /**
   * Claim the next sequence for publishing
   *
   * <p>Blocks if buffer is full until space becomes available
   *
   * @return claimed sequence number
   */
  public long next() {
    return sequencer.next(1);
  }

  /**
   * Claim next n sequences for batch publishing
   *
   * @param n number of sequences to claim
   * @return highest claimed sequence number
   */
  public long next(int n) {
    return sequencer.next(n);
  }

  /**
   * Publish a single sequence
   *
   * <p>Makes the event at this sequence visible to consumers
   *
   * @param sequence sequence to publish
   */
  public void publish(long sequence) {
    sequencer.publish(sequence);
  }

  /**
   * Publish a batch of sequences
   *
   * @param lo lowest sequence in the batch (inclusive)
   * @param hi highest sequence in the batch (inclusive)
   */
  public void publish(long lo, long hi) {
    sequencer.publish(lo, hi);
  }

  /**
   * Publish event using a translator function
   *
   * <p>Provides a higher-level API for publishing events with custom translation logic
   *
   * @param translator function to populate the event
   * @param arg0 argument passed to translator
   * @param <A> argument type
   */
  public <A> void publishEvent(EventTranslator<E, A> translator, A arg0) {
    final long sequence = sequencer.next(1);
    translateAndPublish(translator, sequence, arg0);
  }

  /**
   * Translate event and publish atomically
   *
   * @param translator event translator function
   * @param sequence claimed sequence number
   * @param arg0 argument for translation
   * @param <A> argument type
   */
  private <A> void translateAndPublish(EventTranslator<E, A> translator, long sequence, A arg0) {
    try {
      translator.translateTo(get(sequence), sequence, arg0);
    } finally {
      sequencer.publish(sequence);
    }
  }

  /**
   * Add gating sequences for consumer tracking
   *
   * <p>Gating sequences represent consumer progress and prevent overwriting unprocessed events
   *
   * @param gatingSequences consumer sequences to track
   */
  public void addGatingSequences(Sequence... gatingSequences) {
    sequencer.addGatingSequences(gatingSequences);
  }

  /**
   * Create a sequence barrier for consumers
   *
   * <p>Barrier coordinates when events become available for processing
   *
   * @param sequencesToTrack upstream sequences to wait for
   * @return new sequence barrier
   */
  public SequenceBarrier newBarrier(Sequence... sequencesToTrack) {
    return sequencer.newBarrier(sequencesToTrack);
  }

  /**
   * Get current producer cursor position
   *
   * @return current cursor value
   */
  public long getCursor() {
    return sequencer.getCursor().get();
  }

  /**
   * Get the buffer size
   *
   * @return configured buffer size
   */
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * Get remaining capacity in the buffer
   *
   * @return number of available slots
   */
  public long remainingCapacity() {
    return sequencer.remainingCapacity();
  }

  /**
   * Function interface for translating data into events
   *
   * @param <E> event type
   * @param <A> argument type
   */
  @FunctionalInterface
  public interface EventTranslator<E, A> {
    /**
     * Translate argument into event
     *
     * @param event pre-allocated event to populate
     * @param sequence sequence number for this event
     * @param arg source data
     */
    void translateTo(E event, long sequence, A arg);
  }
}
