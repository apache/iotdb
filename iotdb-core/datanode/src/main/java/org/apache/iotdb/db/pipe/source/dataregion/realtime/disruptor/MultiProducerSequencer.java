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

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.locks.LockSupport;

/**
 * Multi-producer sequencer for coordinating concurrent event publishing
 *
 * <p>Manages sequence allocation and tracking for multiple producer threads:
 *
 * <ul>
 *   <li>Lock-free sequence claiming using CAS operations
 *   <li>Available buffer tracks out-of-order publishing
 *   <li>Gating sequence cache optimizes consumer progress checks
 *   <li>Backpressure mechanism prevents buffer overwrites
 * </ul>
 */
public final class MultiProducerSequencer {
  private static final Unsafe UNSAFE;
  private static final long BASE;
  private static final long SCALE;

  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      UNSAFE = (Unsafe) field.get(null);

      // Initialize array access offsets for available buffer
      BASE = UNSAFE.arrayBaseOffset(int[].class);
      SCALE = UNSAFE.arrayIndexScale(int[].class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final int bufferSize;
  protected final Sequence cursor = new Sequence(Sequence.INITIAL_VALUE);
  protected volatile Sequence[] gatingSequences;

  // CRITICAL: Cache to avoid repeated getMinimumSequence calls
  private final Sequence gatingSequenceCache = new Sequence(Sequence.INITIAL_VALUE);

  // CRITICAL: Available buffer tracks published sequences
  private final int[] availableBuffer;
  private final int indexMask;
  private final int indexShift;

  public MultiProducerSequencer(int bufferSize, Sequence[] gatingSequences) {
    if (bufferSize < 1) {
      throw new IllegalArgumentException("bufferSize must not be less than 1");
    }
    if (Integer.bitCount(bufferSize) != 1) {
      throw new IllegalArgumentException("bufferSize must be a power of 2");
    }

    this.bufferSize = bufferSize;
    this.gatingSequences = gatingSequences != null ? gatingSequences : new Sequence[0];
    this.availableBuffer = new int[bufferSize];
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

      long wrapPoint = next - bufferSize;
      long cachedGatingSequence = gatingSequenceCache.get();

      if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current) {
        long gatingSequence = Sequence.getMinimumSequence(gatingSequences, current);

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

  /** CORE: Check if available - MUST use Unsafe.getIntVolatile */
  public boolean isAvailable(long sequence) {
    int index = calculateIndex(sequence);
    int flag = calculateAvailabilityFlag(sequence);
    long bufferAddress = (index * SCALE) + BASE;
    return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag;
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
    long consumed = Sequence.getMinimumSequence(gatingSequences, cursor.get());
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
  public final void addGatingSequences(Sequence... gatingSequences) {
    SequenceGroups.addSequences(this, this.cursor, gatingSequences);
  }

  /**
   * Remove a gating sequence
   *
   * @param sequence sequence to remove
   * @return true if sequence was found and removed
   */
  public boolean removeGatingSequence(Sequence sequence) {
    return SequenceGroups.removeSequence(this, sequence);
  }

  /**
   * Get the minimum sequence from all consumers
   *
   * @return minimum gating sequence
   */
  public long getMinimumSequence() {
    return Sequence.getMinimumSequence(gatingSequences, cursor.get());
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
    for (int i = availableBuffer.length - 1; i != 0; i--) {
      setAvailableBufferValue(i, -1);
    }
    setAvailableBufferValue(0, -1);
  }

  /**
   * CORE: Set available - MUST use Unsafe.putOrderedInt
   *
   * <p>putOrderedInt provides: - Store-store barrier (not full fence) - Cheaper than volatile write
   * - Sufficient for this use case
   */
  private void setAvailable(final long sequence) {
    setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
  }

  /** CRITICAL: Use Unsafe.putOrderedInt for correct memory semantics */
  private void setAvailableBufferValue(int index, int flag) {
    long bufferAddress = (index * SCALE) + BASE;
    UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
  }

  /** Calculate availability flag */
  private int calculateAvailabilityFlag(final long sequence) {
    return (int) (sequence >>> indexShift);
  }

  /** Calculate index */
  private int calculateIndex(final long sequence) {
    return ((int) sequence) & indexMask;
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
