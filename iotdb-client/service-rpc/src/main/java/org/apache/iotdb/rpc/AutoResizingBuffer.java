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
import java.util.concurrent.locks.ReentrantLock;

/**
 * Helper class that wraps a byte[] so that it can expand and be reused. Users should call
 * resizeIfNecessary to make sure the buffer has suitable capacity, and then use the array as
 * needed.
 *
 * <p>Resizing policies: Expanding: If the required size > current capacity * 1.5, expand to the
 * required size, otherwise expand to current capacity * 1.5. Shrinking: If the required size
 * (bounded below by the initial capacity) is less than current capacity * 0.6, and such small
 * requests last for more than 5 times, shrink to the middle of the required size and current
 * capacity. Unused buffers can also shrink back to their initial capacity under memory pressure.
 */
class AutoResizingBuffer implements AutoCloseable {

  private byte[] array;
  private int bufTooLargeCounter = RpcUtils.MAX_BUFFER_OVERSIZE_TIME;
  private final int initialCapacity;
  private long lastShrinkTime;
  private int accountedCapacity;
  private boolean closed;
  private AutoResizingBufferMemoryManager.MemoryAccount memoryAccount;
  private final ReentrantLock lock = new ReentrantLock();
  private boolean unused = true;

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
    AutoResizingBufferMemoryManager.registerBuffer(this);
  }

  public void resizeIfNecessary(int size) throws IOException {
    lock.lock();
    try {
      unused = false;
      reserveCurrentCapacityIfReleased();
      final int currentCapacity = this.array.length;
      final int requiredCapacity = Math.max(initialCapacity, size);
      final double loadFactor = 0.6;
      if (currentCapacity < requiredCapacity) {
        // Increase by a factor of 1.5x
        int growCapacity = currentCapacity + (currentCapacity >> 1);
        int newCapacity = Math.max(growCapacity, requiredCapacity);
        resize(newCapacity);
        bufTooLargeCounter = RpcUtils.MAX_BUFFER_OVERSIZE_TIME;
      } else if (currentCapacity * loadFactor > requiredCapacity
          && bufTooLargeCounter-- <= 0
          && System.currentTimeMillis() - lastShrinkTime > RpcUtils.MIN_SHRINK_INTERVAL) {
        // Avoid repeatedly copying buffers when frame sizes fluctuate.
        resize(requiredCapacity + (currentCapacity - requiredCapacity) / 2);
        bufTooLargeCounter = RpcUtils.MAX_BUFFER_OVERSIZE_TIME;
        lastShrinkTime = System.currentTimeMillis();
      }
    } finally {
      lock.unlock();
    }
  }

  /** The transport must only call this after consuming or sending the entire frame. */
  public void markUnused() {
    lock.lock();
    try {
      unused = true;
    } finally {
      lock.unlock();
    }
  }

  boolean tryReleaseUnusedMemory(AutoResizingBufferMemoryControl memoryControl) {
    // Allocation can already hold another buffer's lock. Never wait for an active buffer.
    if (!lock.tryLock()) {
      return false;
    }
    try {
      if (closed
          || !unused
          || array.length <= initialCapacity
          || !memoryAccount.usesMemoryControl(memoryControl)) {
        return false;
      }
      array = new byte[initialCapacity];
      memoryAccount.release(accountedCapacity - initialCapacity);
      accountedCapacity = initialCapacity;
      bufTooLargeCounter = RpcUtils.MAX_BUFFER_OVERSIZE_TIME;
      lastShrinkTime = System.currentTimeMillis();
      return true;
    } finally {
      lock.unlock();
    }
  }

  public byte[] array() {
    return this.array;
  }

  @Override
  public void close() {
    lock.lock();
    try {
      if (!closed) {
        memoryAccount.release(accountedCapacity);
        accountedCapacity = 0;
        closed = true;
        AutoResizingBufferMemoryManager.unregisterBuffer(this);
      }
    } finally {
      lock.unlock();
    }
  }

  private void resize(int newCapacity) throws IOException {
    final int currentCapacity = array.length;
    if (newCapacity > currentCapacity) {
      final int delta = newCapacity - currentCapacity;
      memoryAccount.allocate(delta);
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
      AutoResizingBufferMemoryManager.registerBuffer(this);
    }
  }
}
