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

import java.util.concurrent.atomic.AtomicLong;

/** Left-hand side padding for cache line alignment */
class LhsPadding {
  protected long p1, p2, p3, p4, p5, p6, p7;
}

/** Value class holding the actual sequence */
class Value extends LhsPadding {
  protected AtomicLong value = new AtomicLong();
}

/** Right-hand side padding for cache line alignment */
class RhsPadding extends Value {
  protected long p9, p10, p11, p12, p13, p14, p15;
}

/**
 * Lock-free sequence counter with cache line padding
 *
 * <p>This implementation is based on LMAX Disruptor (https://github.com/LMAX-Exchange/disruptor)
 * and preserves the core sequence tracking mechanism for IoTDB's Pipe module.
 *
 * <p>Key design features:
 *
 * <ul>
 *   <li>Three-level inheritance ensures proper field ordering for padding
 *   <li>Uses AtomicLong for thread-safe atomic operations
 *   <li>Cache line padding prevents false sharing between CPU cores
 *   <li>Supports both ordered writes (cheaper) and volatile writes (stronger)
 * </ul>
 */
public class Sequence extends RhsPadding {
  public static final long INITIAL_VALUE = -1L;

  /** Create sequence with initial value -1 */
  public Sequence() {
    value.set(INITIAL_VALUE);
  }

  /** Volatile read */
  public long get() {
    return value.get();
  }

  /**
   * Ordered write (store-store barrier only)
   *
   * <p>CRITICAL: Cheaper than volatile write, sufficient for most cases
   */
  public void set(final long value) {
    this.value.set(value);
  }

  /**
   * CAS operation - CORE for lock-free design
   *
   * @param expectedValue expected current value
   * @param newValue new value
   * @return true if successful
   */
  public boolean compareAndSet(final long expectedValue, final long newValue) {
    return value.compareAndSet(expectedValue, newValue);
  }

  /** Atomically increment */
  public long incrementAndGet() {
    return addAndGet(1L);
  }

  /** Atomically add */
  public long addAndGet(final long increment) {
    long currentValue;
    long newValue;

    do {
      currentValue = get();
      newValue = currentValue + increment;
    } while (!compareAndSet(currentValue, newValue));

    return newValue;
  }

  @Override
  public String toString() {
    return Long.toString(get());
  }

  /** Get minimum sequence from array - CORE utility method */
  public static long getMinimumSequence(final Sequence[] sequences, long minimum) {
    for (int i = 0, n = sequences.length; i < n; i++) {
      long value = sequences[i].get();
      minimum = Math.min(minimum, value);
    }
    return minimum;
  }

  public static long getMinimumSequence(final Sequence[] sequences) {
    return getMinimumSequence(sequences, Long.MAX_VALUE);
  }
}
