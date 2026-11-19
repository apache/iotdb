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

import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private static final Set<ServerConnectionState> serverConnections = ConcurrentHashMap.newKeySet();
  private static final Map<TTransport, ServerConnectionState> serverConnectionsByTransport =
      new IdentityHashMap<>();
  private static final Object serverConnectionLock = new Object();
  private static final AtomicBoolean reclaimingServerBuffers = new AtomicBoolean();

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

  private static void allocate(AutoResizingBufferMemoryControl memoryControl, long sizeInBytes)
      throws IOException {
    if (sizeInBytes <= 0) {
      return;
    }
    for (int i = 0; i < MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (memoryControl.allocate(sizeInBytes)) {
        memoryAllocationCount.incrementAndGet();
        return;
      }
      if (reclaimIdleServerBuffers() > 0) {
        continue;
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
    throw new IOException(
        String.format(
            RpcMessages.AUTO_RESIZING_BUFFER_ALLOCATE_FAILED,
            sizeInBytes,
            MEMORY_ALLOCATE_MAX_RETRIES));
  }

  static void registerServerTransport(TElasticFramedTransport transport) {
    synchronized (serverConnectionLock) {
      if (transport.getServerConnectionState() != null) {
        return;
      }
      TTransport socket = transport.getSocket();
      ServerConnectionState state = serverConnectionsByTransport.get(socket);
      if (state == null) {
        state = new ServerConnectionState();
        serverConnectionsByTransport.put(socket, state);
        serverConnections.add(state);
      }
      state.registerTransport(transport);
      transport.setServerConnectionState(state);
    }
  }

  static void unregisterServerTransport(TElasticFramedTransport transport) {
    synchronized (serverConnectionLock) {
      ServerConnectionState state = transport.getServerConnectionState();
      if (state == null) {
        return;
      }
      if (state.unregisterTransport(transport)) {
        serverConnections.remove(state);
        serverConnectionsByTransport.remove(transport.getSocket());
      }
      transport.setServerConnectionState(null);
    }
  }

  private static long reclaimIdleServerBuffers() {
    if (!reclaimingServerBuffers.compareAndSet(false, true)) {
      return 0;
    }
    try {
      long reclaimedBytes = 0;
      for (ServerConnectionState connection : serverConnections) {
        reclaimedBytes += connection.reclaimIdleBuffers();
      }
      return reclaimedBytes;
    } finally {
      reclaimingServerBuffers.set(false);
    }
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

    long allocate(long preferredSizeInBytes, long requiredSizeInBytes) throws IOException {
      // Growth headroom is optional. If it does not fit, only wait for the bytes actually needed
      // by the frame instead of rejecting a request that still fits within the memory budget.
      if (preferredSizeInBytes > requiredSizeInBytes
          && memoryControl.allocate(preferredSizeInBytes)) {
        memoryAllocationCount.incrementAndGet();
        return preferredSizeInBytes;
      }
      allocate(requiredSizeInBytes);
      return requiredSizeInBytes;
    }

    void release(long sizeInBytes) {
      AutoResizingBufferMemoryManager.release(memoryControl, sizeInBytes);
    }
  }

  static final class ServerConnectionState {
    private final Set<TElasticFramedTransport> transports = new HashSet<>();
    private boolean requestInProgress;

    private synchronized void registerTransport(TElasticFramedTransport transport) {
      transports.add(transport);
    }

    private synchronized boolean unregisterTransport(TElasticFramedTransport transport) {
      transports.remove(transport);
      return transports.isEmpty();
    }

    synchronized void setRequestInProgress(boolean requestInProgress) {
      this.requestInProgress = requestInProgress;
    }

    private synchronized long reclaimIdleBuffers() {
      if (requestInProgress) {
        return 0;
      }
      long reclaimedBytes = 0;
      for (TElasticFramedTransport transport : transports) {
        reclaimedBytes += transport.trimBuffersForReclamation();
      }
      return reclaimedBytes;
    }
  }
}
