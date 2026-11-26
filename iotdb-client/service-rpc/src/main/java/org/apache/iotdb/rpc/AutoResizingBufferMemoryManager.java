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
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
  private static final ThreadLocal<ServerBufferPool> constructingServerBufferPool =
      new ThreadLocal<>();
  private static final AtomicBoolean reclaimingServerBuffers = new AtomicBoolean();
  private static final Set<ServerBufferPool> serverBufferPools = ConcurrentHashMap.newKeySet();
  private static final AtomicLong serverBufferPoolsVersion = new AtomicLong();
  private static final AtomicReference<CandidateSnapshot> otherPoolCandidateSnapshot =
      new AtomicReference<>();

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
    return new MemoryAccount(memoryControl, constructingServerBufferPool.get());
  }

  public static ServerBufferPool createServerBufferPool() {
    ServerBufferPool pool = new ServerBufferPool();
    serverBufferPools.add(pool);
    serverBufferPoolsVersion.incrementAndGet();
    otherPoolCandidateSnapshot.set(null);
    return pool;
  }

  private static void allocate(
      AutoResizingBufferMemoryControl memoryControl,
      long sizeInBytes,
      ServerBufferPool preferredPool)
      throws IOException {
    if (sizeInBytes <= 0) {
      return;
    }
    for (int i = 0; i < MEMORY_ALLOCATE_MAX_RETRIES; i++) {
      if (memoryControl.allocate(sizeInBytes)
          || tryAllocateAfterReclaim(memoryControl, sizeInBytes, preferredPool)) {
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
    throw new IOException(
        String.format(
            RpcMessages.AUTO_RESIZING_BUFFER_ALLOCATE_FAILED,
            sizeInBytes,
            MEMORY_ALLOCATE_MAX_RETRIES));
  }

  private static boolean tryAllocateAfterReclaim(
      AutoResizingBufferMemoryControl memoryControl,
      long sizeInBytes,
      ServerBufferPool preferredPool) {
    if (!reclaimingServerBuffers.compareAndSet(false, true)) {
      return false;
    }
    try {
      if (preferredPool != null && tryAllocateFromPool(memoryControl, sizeInBytes, preferredPool)) {
        return true;
      }
      return tryAllocateFromOtherPools(memoryControl, sizeInBytes, preferredPool);
    } finally {
      reclaimingServerBuffers.set(false);
    }
  }

  private static boolean tryAllocateFromPool(
      AutoResizingBufferMemoryControl memoryControl, long sizeInBytes, ServerBufferPool pool) {
    if (memoryControl.allocate(sizeInBytes)) {
      return true;
    }
    ServerConnectionCandidate candidate;
    while ((candidate = pool.nextReclamationCandidate()) != null) {
      candidate.connection().reclaimIdleBuffers(candidate.idleSinceNanos());
      if (memoryControl.allocate(sizeInBytes)) {
        return true;
      }
    }
    return false;
  }

  private static boolean tryAllocateFromOtherPools(
      AutoResizingBufferMemoryControl memoryControl,
      long sizeInBytes,
      ServerBufferPool preferredPool) {
    if (memoryControl.allocate(sizeInBytes)) {
      return true;
    }
    ServerConnectionCandidate candidate;
    while ((candidate = nextReclamationCandidateFromOtherPools(preferredPool)) != null) {
      candidate.connection().reclaimIdleBuffers(candidate.idleSinceNanos());
      if (memoryControl.allocate(sizeInBytes)) {
        return true;
      }
    }
    return false;
  }

  private static ServerConnectionCandidate nextReclamationCandidateFromOtherPools(
      ServerBufferPool preferredPool) {
    while (true) {
      long version = serverBufferPoolsVersion.get();
      CandidateSnapshot snapshot = otherPoolCandidateSnapshot.get();
      if (snapshot == null
          || snapshot.version != version
          || snapshot.excludedPool != preferredPool) {
        List<ServerConnectionCandidate> candidates = new ArrayList<>();
        long now = System.nanoTime();
        for (ServerBufferPool pool : serverBufferPools) {
          if (pool != preferredPool) {
            pool.collectCandidates(candidates, now);
          }
        }
        candidates.sort(ServerConnectionCandidate.RECLAMATION_ORDER);
        if (version != serverBufferPoolsVersion.get()) {
          continue;
        }
        CandidateSnapshot newSnapshot = new CandidateSnapshot(version, candidates, preferredPool);
        if (!otherPoolCandidateSnapshot.compareAndSet(snapshot, newSnapshot)) {
          continue;
        }
        snapshot = newSnapshot;
      }
      if (version != serverBufferPoolsVersion.get()) {
        continue;
      }
      return snapshot.nextCandidate();
    }
  }

  private static long reclaimIdleBuffers(ServerBufferPool pool, long targetBytes) {
    if (targetBytes <= 0 || !reclaimingServerBuffers.compareAndSet(false, true)) {
      return 0;
    }
    try {
      return pool.reclaimIdleBuffersInternal(targetBytes);
    } finally {
      reclaimingServerBuffers.set(false);
    }
  }

  private static long reclaimOneIdleConnection(ServerBufferPool pool) {
    if (!reclaimingServerBuffers.compareAndSet(false, true)) {
      return 0;
    }
    try {
      ServerConnectionCandidate candidate;
      while ((candidate = pool.nextReclamationCandidate()) != null) {
        long reclaimedBytes = candidate.connection().reclaimIdleBuffers(candidate.idleSinceNanos());
        if (reclaimedBytes > 0) {
          return reclaimedBytes;
        }
      }
      return 0;
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
    private final ServerBufferPool serverBufferPool;

    private MemoryAccount(
        AutoResizingBufferMemoryControl memoryControl, ServerBufferPool serverBufferPool) {
      this.memoryControl = memoryControl;
      this.serverBufferPool = serverBufferPool;
    }

    void allocate(long sizeInBytes) throws IOException {
      AutoResizingBufferMemoryManager.allocate(memoryControl, sizeInBytes, serverBufferPool);
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

    MemoryAccount reopen() {
      return new MemoryAccount(AutoResizingBufferMemoryManager.memoryControl, serverBufferPool);
    }

    void release(long sizeInBytes) {
      AutoResizingBufferMemoryManager.release(memoryControl, sizeInBytes);
    }
  }

  public static final class ServerBufferPool implements AutoCloseable {
    private final Map<TTransport, ServerConnectionState> connectionsByTransport =
        new IdentityHashMap<>();
    private final AtomicLong reclamationCandidatesVersion = new AtomicLong();
    private volatile CandidateSnapshot candidateSnapshot;
    private long exhaustedCandidateVersion = Long.MIN_VALUE;
    private volatile boolean closed;
    private boolean retired;

    private ServerBufferPool() {}

    /** Reclaims idle buffers from this pool until at least {@code targetBytes} are released. */
    public long reclaimIdleBuffers(long targetBytes) {
      if (closed) {
        return 0;
      }
      return AutoResizingBufferMemoryManager.reclaimIdleBuffers(this, targetBytes);
    }

    /** Reclaims buffers from the highest-priority idle connection in this pool. */
    public long reclaimOneIdleConnection() {
      if (closed) {
        return 0;
      }
      return AutoResizingBufferMemoryManager.reclaimOneIdleConnection(this);
    }

    /** Retires a stopped server while keeping its live connections eligible for reclamation. */
    public synchronized void retire() {
      if (closed) {
        return;
      }
      retired = true;
      if (connectionsByTransport.isEmpty()) {
        serverBufferPools.remove(this);
      }
    }

    /** Associates initial buffer allocations and subsequent growth with this server pool. */
    public TTransportFactory wrapTransportFactory(TTransportFactory factory) {
      return new TTransportFactory() {
        @Override
        public TTransport getTransport(TTransport transport) throws TTransportException {
          ServerBufferPool previousPool = constructingServerBufferPool.get();
          constructingServerBufferPool.set(ServerBufferPool.this);
          try {
            TTransport serverTransport = factory.getTransport(transport);
            if (serverTransport instanceof TElasticFramedTransport framedTransport) {
              framedTransport.registerForServerBufferReclamation(ServerBufferPool.this);
            }
            return serverTransport;
          } finally {
            if (previousPool == null) {
              constructingServerBufferPool.remove();
            } else {
              constructingServerBufferPool.set(previousPool);
            }
          }
        }
      };
    }

    synchronized void register(TElasticFramedTransport transport) {
      if (closed || transport.getServerConnectionState() != null) {
        return;
      }
      ServerConnectionState state =
          connectionsByTransport.computeIfAbsent(
              transport.getSocket(), socket -> new ServerConnectionState(this));
      state.registerTransport(transport);
      transport.setServerConnectionState(state);
      invalidateReclamationCandidates();
      if (retired) {
        serverBufferPools.add(this);
        serverBufferPoolsVersion.incrementAndGet();
        otherPoolCandidateSnapshot.set(null);
      }
    }

    synchronized void unregister(TElasticFramedTransport transport) {
      ServerConnectionState state = transport.getServerConnectionState();
      if (state != null && state.unregisterTransport(transport)) {
        connectionsByTransport.remove(transport.getSocket());
        if (retired && connectionsByTransport.isEmpty()) {
          serverBufferPools.remove(this);
        }
      }
      invalidateReclamationCandidates();
      transport.setServerConnectionState(null);
    }

    private synchronized void collectCandidates(
        List<ServerConnectionCandidate> candidates, long now) {
      if (closed) {
        return;
      }
      for (ServerConnectionState connection : connectionsByTransport.values()) {
        ServerConnectionCandidate candidate = connection.getReclamationCandidate(now);
        if (candidate != null) {
          candidates.add(candidate);
        }
      }
    }

    private long reclaimIdleBuffersInternal(long targetBytes) {
      long reclaimedBytes = 0;
      ServerConnectionCandidate candidate;
      while (reclaimedBytes < targetBytes && (candidate = nextReclamationCandidate()) != null) {
        reclaimedBytes += candidate.connection().reclaimIdleBuffers(candidate.idleSinceNanos());
      }
      return reclaimedBytes;
    }

    private synchronized ServerConnectionCandidate nextReclamationCandidate() {
      long version = reclamationCandidatesVersion.get();
      if (exhaustedCandidateVersion == version) {
        return null;
      }
      CandidateSnapshot snapshot = candidateSnapshot;
      if (snapshot == null || snapshot.version != version) {
        List<ServerConnectionCandidate> candidates = new ArrayList<>();
        collectCandidates(candidates, System.nanoTime());
        candidates.sort(ServerConnectionCandidate.RECLAMATION_ORDER);
        snapshot = new CandidateSnapshot(version, candidates, null);
        candidateSnapshot = snapshot;
      }
      ServerConnectionCandidate candidate = snapshot.nextCandidate();
      if (candidate == null) {
        candidateSnapshot = null;
        exhaustedCandidateVersion = version;
      }
      return candidate;
    }

    private void invalidateReclamationCandidates() {
      reclamationCandidatesVersion.incrementAndGet();
      candidateSnapshot = null;
      serverBufferPoolsVersion.incrementAndGet();
      otherPoolCandidateSnapshot.set(null);
    }

    @Override
    public synchronized void close() {
      closed = true;
      connectionsByTransport.clear();
      invalidateReclamationCandidates();
      serverBufferPools.remove(this);
    }
  }

  static final class ServerConnectionState {
    private final ServerBufferPool pool;
    private final Set<TElasticFramedTransport> transports = new HashSet<>();
    private boolean requestInProgress;
    private long idleSinceNanos = System.nanoTime();

    private ServerConnectionState(ServerBufferPool pool) {
      this.pool = pool;
    }

    private synchronized void registerTransport(TElasticFramedTransport transport) {
      transports.add(transport);
    }

    private synchronized boolean unregisterTransport(TElasticFramedTransport transport) {
      transports.remove(transport);
      return transports.isEmpty();
    }

    synchronized void setRequestInProgress(boolean requestInProgress) {
      if (this.requestInProgress != requestInProgress) {
        if (!requestInProgress) {
          idleSinceNanos = System.nanoTime();
        }
        this.requestInProgress = requestInProgress;
        pool.invalidateReclamationCandidates();
      }
    }

    private synchronized ServerConnectionCandidate getReclamationCandidate(long now) {
      if (requestInProgress) {
        return null;
      }
      long reclaimableBytes = 0;
      long capacityBytes = 0;
      for (TElasticFramedTransport transport : transports) {
        reclaimableBytes += transport.getReclaimableBufferBytes();
        capacityBytes += transport.getAccountedBufferBytes();
      }
      if (reclaimableBytes <= 0 || capacityBytes <= 0) {
        return null;
      }
      return new ServerConnectionCandidate(
          this,
          idleSinceNanos,
          Math.max(0, now - idleSinceNanos),
          reclaimableBytes,
          (double) reclaimableBytes / capacityBytes);
    }

    private synchronized long reclaimIdleBuffers(long expectedIdleSinceNanos) {
      if (pool.closed || requestInProgress || idleSinceNanos != expectedIdleSinceNanos) {
        return 0;
      }
      long reclaimedBytes = 0;
      for (TElasticFramedTransport transport : transports) {
        reclaimedBytes += transport.trimBuffersForReclamation();
      }
      return reclaimedBytes;
    }
  }

  private record ServerConnectionCandidate(
      ServerConnectionState connection,
      long idleSinceNanos,
      long idleTimeNanos,
      long reclaimableBytes,
      double reclaimableRatio) {
    private static final Comparator<ServerConnectionCandidate> RECLAMATION_ORDER =
        Comparator.comparingLong(ServerConnectionCandidate::idleTimeNanos)
            .reversed()
            .thenComparing(
                Comparator.comparingDouble(ServerConnectionCandidate::reclaimableRatio).reversed())
            .thenComparing(
                Comparator.comparingLong(ServerConnectionCandidate::reclaimableBytes).reversed());
  }

  private static final class CandidateSnapshot {
    private final long version;
    private final List<ServerConnectionCandidate> candidates;
    private final ServerBufferPool excludedPool;
    private int nextCandidateIndex;

    private CandidateSnapshot(
        long version, List<ServerConnectionCandidate> candidates, ServerBufferPool excludedPool) {
      this.version = version;
      this.candidates = candidates;
      this.excludedPool = excludedPool;
    }

    private synchronized ServerConnectionCandidate nextCandidate() {
      if (nextCandidateIndex >= candidates.size()) {
        return null;
      }
      return candidates.get(nextCandidateIndex++);
    }
  }
}
