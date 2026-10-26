/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.rpc;

import java.io.IOException;
import java.util.Arrays;

/**
 * Helper class that wraps a byte[] so that it can expand and be reused. Incremental writes use
 * ensureCapacity; resizeIfNecessary also considers shrinking and must receive a complete frame's
 * size.
 *
 * <p>Resizing policies: Expanding: If the required size > current capacity * 1.5, expand to the
 * required size, otherwise expand to current capacity * 1.5 when the memory budget allows it.
 * Shrinking: If the required size < current capacity * 0.6, and such small requests last for more
 * than 5 times, shrink to the middle of the required size and current capacity. The required size
 * is bounded below by the initial capacity.
 */
class AutoResizingBuffer implements AutoCloseable {

  private byte[] array;
  private int bufTooLargeCounter = RpcUtils.MAX_BUFFER_OVERSIZE_TIME;
  private final int initialCapacity;
  private long lastShrinkTime;
  private int accountedCapacity;
  private boolean closed;
  private AutoResizingBufferMemoryManager.MemoryAccount memoryAccount;

  public AutoResizingBuffer(int initialCapacity) throws IOException {
    this.initialCapacity = initialCapacity;
    this.memoryAccount = AutoResizingBufferMemoryManager.createMemoryAccount();
    memoryAccount.allocate(initialCapacity);
    try {
      this.array = new byte[initialCapacity];
    } catch (RuntimeException | Error e) {
      memoryAccount.release(initialCapacity);
      throw e;
    }
    this.accountedCapacity = initialCapacity;
  }

  public void resizeIfNecessary(int size) throws IOException {
    ensureCapacity(size);
    size = Math.max(size, initialCapacity);
    final int currentCapacity = this.array.length;
    final double loadFactor = 0.6;
    if (currentCapacity * loadFactor > size
        && bufTooLargeCounter-- <= 0
        && System.currentTimeMillis() - lastShrinkTime > RpcUtils.MIN_SHRINK_INTERVAL) {
      resize(size + (currentCapacity - size) / 2, size);
      bufTooLargeCounter = RpcUtils.MAX_BUFFER_OVERSIZE_TIME;
      lastShrinkTime = System.currentTimeMillis();
    }
  }

  void ensureCapacity(int size) throws IOException {
    reserveCurrentCapacityIfReleased();
    final int currentCapacity = this.array.length;
    if (currentCapacity < size) {
      // Increase by a factor of 1.5x
      int growCapacity = currentCapacity + (currentCapacity >> 1);
      int newCapacity = Math.max(growCapacity, size);
      resize(newCapacity, size);
      bufTooLargeCounter = RpcUtils.MAX_BUFFER_OVERSIZE_TIME;
    }
  }

  public byte[] array() {
    return this.array;
  }

  @Override
  public void close() {
    if (!closed) {
      memoryAccount.release(accountedCapacity);
      accountedCapacity = 0;
      closed = true;
    }
  }

  private void resize(int newCapacity, int requiredCapacity) throws IOException {
    final int currentCapacity = array.length;
    if (newCapacity > currentCapacity) {
      final int delta =
          (int)
              memoryAccount.allocate(
                  newCapacity - currentCapacity, requiredCapacity - currentCapacity);
      newCapacity = currentCapacity + delta;
      try {
        array = Arrays.copyOf(array, newCapacity);
        accountedCapacity += delta;
      } catch (RuntimeException | Error e) {
        memoryAccount.release(delta);
        throw e;
      }
    } else if (newCapacity < currentCapacity) {
      array = Arrays.copyOf(array, newCapacity);
      final int delta = currentCapacity - newCapacity;
      memoryAccount.release(delta);
      accountedCapacity -= delta;
    }
  }

  private void reserveCurrentCapacityIfReleased() throws IOException {
    if (closed) {
      AutoResizingBufferMemoryManager.MemoryAccount newMemoryAccount =
          AutoResizingBufferMemoryManager.createMemoryAccount();
      newMemoryAccount.allocate(array.length);
      memoryAccount = newMemoryAccount;
      accountedCapacity = array.length;
      closed = false;
    }
  }
}
