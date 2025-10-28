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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Multi-producer sequencer for coordinating concurrent publishers
 *
 * <p>This implementation is based on LMAX Disruptor (https://github.com/LMAX-Exchange/disruptor)
 * and preserves the core lock-free multi-producer algorithm for IoTDB's Pipe module.
 *
 * <p>Key features preserved from LMAX Disruptor:
 *
 * <ul>
 *   <li>Lock-free CAS-based sequence claiming
 *   <li>Availability buffer for out-of-order publishing detection
 *   <li>Backpressure via gating sequences
 *   <li>Cache line padding to prevent false sharing
 * </ul>
 */
public final class MultiProducerSequencer {

  /** Ring buffer size (must be power of 2) - immutable after construction */
  private final int bufferSize;

  /**
   * Producer cursor tracking highest claimed sequence Updated via CAS in next() method Volatile
   * reads/writes handled by Sequence class
   */
  private final Sequence cursor = new Sequence();

  /**
   * Array of consumer sequences for backpressure control MUST be volatile for safe publication when
   * modified by SequenceGroups Array reference is replaced atomically via
   * AtomicReferenceFieldUpdater
   */
  volatile AtomicReference<Sequence[]> gatingSequences = new AtomicReference<>();

  /**
   * Cached minimum gating sequence to reduce contention Updated opportunistically in next() to
   * avoid expensive array scan Does not need to be perfectly accurate (conservative is safe)
   */
  private final Sequence gatingSequenceCache = new Sequence();

  /**
   * CRITICAL: Availability flags for tracking published sequences
   *
   * <p>Handles out-of-order publishing in multi-producer scenario: - Thread A claims seq 10, still
   * writing - Thread B claims seq 11, finishes and publishes - Consumer MUST wait for seq 10 before
   * reading seq 11
   *
   * <p>Memory visibility guarantees: - Writers use lazySet() for store-store barrier (cheaper than
   * volatile write) - Readers use get() for volatile read (ensures visibility across threads)
   *
   * <p>AtomicIntegerArray provides same semantics as Unsafe without reflection
   */
  private final AtomicIntegerArray availableBuffer;

  /** Mask for fast modulo: sequence & indexMask == sequence % bufferSize */
  private final int indexMask;

  /** Shift for calculating wrap count: sequence >>> indexShift */
  private final int indexShift;

  public MultiProducerSequencer(int bufferSize, Sequence[] gatingSequences) {
    if (bufferSize < 1) {
      throw new IllegalArgumentException("bufferSize must not be less than 1");
    }
    if (Integer.bitCount(bufferSize) != 1) {
      throw new IllegalArgumentException("bufferSize must be a power of 2");
    }

    this.bufferSize = bufferSize;
    this.gatingSequences.set(Objects.nonNull(gatingSequences) ? gatingSequences : new Sequence[0]);
    this.availableBuffer = new AtomicIntegerArray(bufferSize);
    this.indexMask = bufferSize - 1;
    this.indexShift = log2(bufferSize);

    initialiseAvailableBuffer();
  }

  /**
   * Claim next n sequences for publishing
   *
   * <p>Uses CAS loop to atomically claim sequence numbers. Implements backpressure by parking when
   * buffer is full.
   *
   * @param n number of sequences to claim
   * @return highest claimed sequence number
   */
  public long next(int n) {
    if (n < 1) {
      throw new IllegalArgumentException("n must be > 0");
    }

    long current;
    long next;

    do {
      current = cursor.get();
      next = current + n;

      final long wrapPoint = next - bufferSize;
      final long cachedGatingSequence = gatingSequenceCache.get();

      if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current) {
        long gatingSequence = Sequence.getMinimumSequence(gatingSequences.get(), current);

        if (wrapPoint > gatingSequence) {
          LockSupport.parkNanos(1);
          continue;
        }

        gatingSequenceCache.set(gatingSequence);
      } else if (cursor.compareAndSet(current, next)) {
        break;
      }
    } while (true);

    return next;
  }

  /** Publish sequence */
  public void publish(final long sequence) {
    setAvailable(sequence);
  }

  /** Publish batch */
  public void publish(long lo, long hi) {
    for (long l = lo; l <= hi; l++) {
      setAvailable(l);
    }
  }

  /**
   * CORE: Check if sequence is available for consumption Uses volatile read to ensure visibility of
   * published sequences
   */
  public boolean isAvailable(long sequence) {
    int index = calculateIndex(sequence);
    int flag = calculateAvailabilityFlag(sequence);
    return availableBuffer.get(index) == flag;
  }

  /** CORE: Get highest published - exact same algorithm */
  public long getHighestPublishedSequence(long lowerBound, long availableSequence) {
    for (long sequence = lowerBound; sequence <= availableSequence; sequence++) {
      if (!isAvailable(sequence)) {
        return sequence - 1;
      }
    }
    return availableSequence;
  }

  public Sequence getCursor() {
    return cursor;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public long remainingCapacity() {
    long consumed = Sequence.getMinimumSequence(gatingSequences.get(), cursor.get());
    long produced = cursor.get();
    return bufferSize - (produced - consumed);
  }

  /**
   * Add gating sequences for consumer tracking
   *
   * <p>Atomically adds sequences to track consumer progress
   *
   * @param gatingSequences consumer sequences to add
   */
  public void addGatingSequences(Sequence... gatingSequences) {
    SequenceGroups.addSequences(this, this.cursor, gatingSequences);
  }

  /**
   * Create a sequence barrier for consumers
   *
   * @param sequencesToTrack upstream sequences to wait for
   * @return new sequence barrier
   */
  public SequenceBarrier newBarrier(Sequence... sequencesToTrack) {
    return new SequenceBarrier(this, sequencesToTrack);
  }

  /** Initialize available buffer */
  private void initialiseAvailableBuffer() {
    for (int i = availableBuffer.length() - 1; i != 0; i--) {
      setAvailableBufferValue(i, -1);
    }
    setAvailableBufferValue(0, -1);
  }

  /**
   * CORE: Mark sequence as available for consumption
   *
   * <p>Uses lazySet() which provides: - Store-store barrier (ensures all prior writes are visible)
   * - Cheaper than full volatile write (no store-load barrier) - Sufficient for this use case
   * (readers use volatile get)
   */
  private void setAvailable(final long sequence) {
    setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
  }

  /**
   * Set availability flag with release semantics lazySet() ensures previous event writes are
   * visible before flag update
   */
  private void setAvailableBufferValue(int index, int flag) {
    availableBuffer.lazySet(index, flag);
  }

  /** Calculate availability flag */
  private int calculateAvailabilityFlag(final long sequence) {
    return (int) (sequence >>> indexShift);
  }

  /** Calculate index */
  private int calculateIndex(final long sequence) {
    return ((int) sequence) & indexMask;
  }

  public Sequence[] getGatingSequences() {
    return gatingSequences.get();
  }

  public boolean compareAndSetGatingSequences(
      final Sequence[] currentSequences, final Sequence[] updatedSequences) {
    return gatingSequences.compareAndSet(currentSequences, updatedSequences);
  }

  /**
   * Calculate log2 for index shift calculation
   *
   * @param i input value (must be power of 2)
   * @return log2 of input
   */
  private static int log2(int i) {
    int r = 0;
    while ((i >>= 1) != 0) {
      ++r;
    }
    return r;
  }
}
