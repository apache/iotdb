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
import java.util.Objects;
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

        @Override
        public long getUsedMemoryInBytes() {
          return 0;
        }
      };

  private static volatile AutoResizingBufferMemoryControl memoryControl = NO_OP_MEMORY_CONTROL;
  private static volatile long memoryTotalSizeInBytes;
  private static volatile long memoryAllocateRetryIntervalInMs =
      DEFAULT_MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS;
  private static final AtomicLong memoryAllocationCount = new AtomicLong();
  private static final AtomicLong memoryAllocationFailureCount = new AtomicLong();

  private AutoResizingBufferMemoryManager() {
    // Utility class.
  }

  public static void setMemoryControl(AutoResizingBufferMemoryControl memoryControl) {
    setMemoryControl(memoryControl, 0);
  }

  public static void setMemoryControl(
      AutoResizingBufferMemoryControl memoryControl, long memoryTotalSizeInBytes) {
    AutoResizingBufferMemoryManager.memoryControl = Objects.requireNonNull(memoryControl);
    AutoResizingBufferMemoryManager.memoryTotalSizeInBytes = Math.max(0, memoryTotalSizeInBytes);
  }

  static void resetMemoryControl() {
    memoryControl = NO_OP_MEMORY_CONTROL;
    memoryTotalSizeInBytes = 0;
    memoryAllocateRetryIntervalInMs = DEFAULT_MEMORY_ALLOCATE_RETRY_INTERVAL_IN_MS;
    memoryAllocationCount.set(0);
    memoryAllocationFailureCount.set(0);
  }

  static void setMemoryAllocateRetryIntervalInMs(long memoryAllocateRetryIntervalInMs) {
    AutoResizingBufferMemoryManager.memoryAllocateRetryIntervalInMs =
        memoryAllocateRetryIntervalInMs;
  }

  static void allocate(long sizeInBytes) throws IOException {
    if (sizeInBytes <= 0) {
      return;
    }
    for (int i = 0; i < MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (memoryControl.allocate(sizeInBytes)) {
        memoryAllocationCount.incrementAndGet();
        return;
      }
      try {
        Thread.sleep(memoryAllocateRetryIntervalInMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format(
                RpcMessages
                    .EXCEPTION_AUTO_RESIZING_BUFFER_WAS_INTERRUPTED_WHILE_ALLOCATING_ARG_BYTES_516F5DA1,
                sizeInBytes),
            e);
      }
    }
    memoryAllocationFailureCount.incrementAndGet();
    throw new IOException(
        String.format(
            RpcMessages
                .EXCEPTION_AUTO_RESIZING_BUFFER_FAILED_TO_ALLOCATE_ARG_BYTES_AFTER_ARG_RETRIES_836DB2FD,
            sizeInBytes,
            MEMORY_ALLOCATE_MAX_RETRIES));
  }

  static void release(long sizeInBytes) {
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

  public static long getMemoryTotalSizeInBytes() {
    return memoryTotalSizeInBytes;
  }

  public static long getMemoryUsedSizeInBytes() {
    return memoryControl.getUsedMemoryInBytes();
  }

  public static long getMemoryAvailableSizeInBytes() {
    return Math.max(0, memoryTotalSizeInBytes - getMemoryUsedSizeInBytes());
  }
}
