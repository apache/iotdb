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

package org.apache.iotdb.rpc;

import org.apache.iotdb.rpc.i18n.RpcMessages;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class AutoResizingBufferMemoryManager {
  private static final int MEMORY_ALLOCATE_MAX_RETRIES = 5;
  private static final long DEFAULT_MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS = 2_000L;

  private static final AutoResizingBufferMemoryControl NO_OP_MEMORY_CONTROL =
      new AutoResizingBufferMemoryControl() {
        @Override
        public boolean allocate(long sizeInBytes) {
          return true;
        }

        @Override
        public void release(long sizeInBytes) {
          // Do nothing.
        }
      };

  private static volatile AutoResizingBufferMemoryControl memoryControl = NO_OP_MEMORY_CONTROL;
  private static volatile long memoryAllocateRetryIntervalInMs =
      DEFAULT_MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS;
  private static final AtomicLong memoryAllocationCount = new AtomicLong();
  private static final AtomicLong memoryAllocationFailureCount = new AtomicLong();
  // Do not keep transports alive just so their unused buffers can be reclaimed.
  private static final Set<AutoResizingBuffer> buffers =
      Collections.newSetFromMap(new WeakHashMap<>());

  private AutoResizingBufferMemoryManager() {
    // Utility class.
  }

  public static void setMemoryControl(AutoResizingBufferMemoryControl memoryControl) {
    AutoResizingBufferMemoryManager.memoryControl = Objects.requireNonNull(memoryControl);
  }

  static void resetMemoryControl() {
    memoryControl = NO_OP_MEMORY_CONTROL;
    memoryAllocateRetryIntervalInMs = DEFAULT_MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS;
    memoryAllocationCount.set(0);
    memoryAllocationFailureCount.set(0);
  }

  static void setMemoryAllocateRetryIntervalInMs(long memoryAllocateRetryIntervalInMs) {
    AutoResizingBufferMemoryManager.memoryAllocateRetryIntervalInMs =
        memoryAllocateRetryIntervalInMs;
  }

  static MemoryAccount createMemoryAccount() {
    return new MemoryAccount(memoryControl);
  }

  static void registerBuffer(AutoResizingBuffer buffer) {
    synchronized (buffers) {
      buffers.add(buffer);
    }
  }

  static void unregisterBuffer(AutoResizingBuffer buffer) {
    synchronized (buffers) {
      buffers.remove(buffer);
    }
  }

  private static boolean releaseUnusedBuffers(AutoResizingBufferMemoryControl memoryControl) {
    final AutoResizingBuffer[] candidates;
    synchronized (buffers) {
      candidates = buffers.toArray(new AutoResizingBuffer[0]);
    }
    boolean released = false;
    for (AutoResizingBuffer buffer : candidates) {
      released |= buffer.tryReleaseUnusedMemory(memoryControl);
    }
    return released;
  }

  private static void allocate(AutoResizingBufferMemoryControl memoryControl, long sizeInBytes)
      throws IOException {
    if (sizeInBytes <= 0) {
      return;
    }
    for (int i = 0; i < MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (memoryControl.allocate(sizeInBytes)
          || (releaseUnusedBuffers(memoryControl) && memoryControl.allocate(sizeInBytes))) {
        memoryAllocationCount.incrementAndGet();
        return;
      }
      try {
        Thread.sleep(memoryAllocateRetryIntervalInMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format(RpcMessages.AUTO_RESIZING_BUFFER_ALLOCATE_INTERRUPTED, sizeInBytes), e);
      }
    }
    memoryAllocationFailureCount.incrementAndGet();
    final long totalMemory = memoryControl.getTotalMemorySizeInBytes();
    final long usedMemory = memoryControl.getUsedMemoryInBytes();
    throw new IOException(
        String.format(
            RpcMessages
                .EXCEPTION_AUTORESIZINGBUFFER_FAILED_TO_ALLOCATE_ARG_BYTES_AFTER_ARG_RETRIES_POOL_TOTAL_ARG_BYTES_USED_ARG_BYTES_AVAILABLE_ARG_BYTES_8244057A,
            sizeInBytes,
            MEMORY_ALLOCATE_MAX_RETRIES,
            totalMemory,
            usedMemory,
            totalMemory < 0 || usedMemory < 0 ? -1 : totalMemory - usedMemory));
  }

  private static void release(AutoResizingBufferMemoryControl memoryControl, long sizeInBytes) {
    if (sizeInBytes <= 0) {
      return;
    }
    memoryControl.release(sizeInBytes);
  }

  public static long getMemoryAllocationCount() {
    return memoryAllocationCount.get();
  }

  public static long getMemoryAllocationFailureCount() {
    return memoryAllocationFailureCount.get();
  }

  static final class MemoryAccount {
    private final AutoResizingBufferMemoryControl memoryControl;

    private MemoryAccount(AutoResizingBufferMemoryControl memoryControl) {
      this.memoryControl = memoryControl;
    }

    void allocate(long sizeInBytes) throws IOException {
      AutoResizingBufferMemoryManager.allocate(memoryControl, sizeInBytes);
    }

    void release(long sizeInBytes) {
      AutoResizingBufferMemoryManager.release(memoryControl, sizeInBytes);
    }

    boolean usesMemoryControl(AutoResizingBufferMemoryControl memoryControl) {
      return this.memoryControl == memoryControl;
    }
  }
}
