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

package org.apache.iotdb.commons.external.caffeine.cache;

import org.apache.iotdb.commons.external.caffeine.cache.testing.ConcurrentTestHarness;

import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.iotdb.commons.external.caffeine.cache.Buffer.FAILED;
import static org.apache.iotdb.commons.external.caffeine.cache.Buffer.FULL;
import static org.apache.iotdb.commons.external.caffeine.cache.Buffer.SUCCESS;
import static org.apache.iotdb.commons.external.caffeine.cache.testing.TruthAssert.assertThat;

/**
 * The tests cases for the {@link BoundedBuffer}.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("ClassEscapesDefinedScope")
public final class BoundedBufferTest {

  @Test
  public void offer() {
    var buffer = new BoundedBuffer<Boolean>();
    var invalid = new AtomicReference<Integer>();
    ConcurrentTestHarness.timeTasks(
        10,
        () -> {
          for (int i = 0; i < 100; i++) {
            int added = buffer.offer(true);
            if ((added != SUCCESS) && (added != FULL) && (added != FAILED)) {
              invalid.set(added);
            }
          }
        });
    assertThat(invalid.get()).isNull();
    assertThat(buffer.writes()).isGreaterThan(0);
    assertThat(buffer.writes()).isEqualTo(buffer.size());
  }

  @Test
  public void offer_fillReportsFull() {
    var buffer = new BoundedBuffer<Boolean>();
    for (int i = 0; i < (BoundedBuffer.BUFFER_SIZE - 1); i++) {
      assertThat(buffer.offer(true)).isEqualTo(SUCCESS);
    }

    // the insertion that fills the last free slot is recorded and signals a drain
    assertThat(buffer.offer(true)).isEqualTo(FULL);
    assertThat(buffer.writes()).isEqualTo(BoundedBuffer.BUFFER_SIZE);

    // an insertion into a full buffer is rejected
    assertThat(buffer.offer(true)).isEqualTo(FULL);
    assertThat(buffer.writes()).isEqualTo(BoundedBuffer.BUFFER_SIZE);

    long[] read = new long[1];
    buffer.drainTo(e -> read[0]++);
    assertThat(read[0]).isEqualTo(BoundedBuffer.BUFFER_SIZE);
  }

  @Test
  public void drain() {
    var buffer = new BoundedBuffer<Boolean>();
    for (int i = 0; i < BoundedBuffer.BUFFER_SIZE; i++) {
      assertThat(buffer.offer(true)).isAnyOf(SUCCESS, FULL);
    }
    long[] read = new long[1];
    buffer.drainTo(e -> read[0]++);
    assertThat(read[0]).isEqualTo(buffer.reads());
    assertThat(read[0]).isEqualTo(buffer.writes());
  }

  @Test
  @SuppressWarnings("ThreadPriorityCheck")
  public void offerAndDrain() {
    var buffer = new BoundedBuffer<Boolean>();
    var lock = new ReentrantLock();
    var reads = new AtomicInteger();
    ConcurrentTestHarness.timeTasks(
        10,
        () -> {
          for (int i = 0; i < 1000; i++) {
            boolean shouldDrain = (buffer.offer(true) == FULL);
            if (shouldDrain && lock.tryLock()) {
              buffer.drainTo(e -> reads.incrementAndGet());
              lock.unlock();
            }
            Thread.yield();
          }
        });
    buffer.drainTo(e -> reads.incrementAndGet());
    assertThat(reads.longValue()).isEqualTo(buffer.reads());
    assertThat(reads.longValue()).isEqualTo(buffer.writes());
  }

  @Test
  public void overflow() {
    Boolean first = null;
    var buffer = new BoundedBuffer.RingBuffer<>(first);
    buffer.writeCounter = Long.MAX_VALUE;
    buffer.readCounter = Long.MAX_VALUE;

    assertThat(buffer.offer(true)).isEqualTo(SUCCESS);
    var data = new ArrayList<>();
    buffer.drainTo(data::add);

    for (var e : buffer.buffer) {
      assertThat(e).isNull();
    }
    assertThat(data).containsExactly(true);
    assertThat(buffer.readCounter).isEqualTo(Long.MIN_VALUE);
    assertThat(buffer.writeCounter).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
  public void reflectivelyConstruct() throws ReflectiveOperationException {
    var constructor = BBHeader.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    constructor.newInstance();
  }

  @Test
  public void disabledBuffer() {
    Buffer<Integer> disabled = Buffer.disabled();
    assertThat(disabled).isSameInstanceAs(DisabledBuffer.INSTANCE);
    assertThat(disabled.offer(1)).isEqualTo(SUCCESS);
    disabled.drainTo(
        e -> {
          throw new AssertionError("Should not drain");
        });
    assertThat(disabled.reads()).isEqualTo(0L);
    assertThat(disabled.writes()).isEqualTo(0L);
    assertThat(disabled.size()).isEqualTo(0L);
    for (DisabledBuffer val : DisabledBuffer.values()) {
      assertThat(DisabledBuffer.valueOf(val.name())).isSameInstanceAs(DisabledBuffer.INSTANCE);
    }
  }
}
