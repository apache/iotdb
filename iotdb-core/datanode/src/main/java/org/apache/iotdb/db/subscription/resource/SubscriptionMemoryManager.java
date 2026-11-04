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

package org.apache.iotdb.db.subscription.resource;

import org.apache.iotdb.commons.memory.AtomicLongMemoryBlock;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * DataNode-wide memory manager for materialized subscription data.
 *
 * <p>Consensus subscription queues use a handle rather than the legacy manager-level allocation
 * methods. Handles are accounted independently. Each active queue keeps half of its equal share
 * protected; another queue may borrow only the remaining idle space. Borrowed allocations are
 * drained rather than revoked when the active queue set changes. An entry larger than the maximum
 * amount one queue can hold under this policy is rejected explicitly.
 */
public class SubscriptionMemoryManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionMemoryManager.class);

  private static final String MEMORY_BLOCK_NAME = "Subscription";

  private static final long OVERCOMMIT_WARNING_INTERVAL_NS = TimeUnit.SECONDS.toNanos(30);

  private final IMemoryBlock memoryBlock;
  private static final int PROTECTED_SHARE_DIVISOR = 2;
  private final Map<Long, MemoryHandle> handles = new HashMap<>();
  private long nextHandleId;

  // Kept for callers of the original manager-level API. It is deliberately not a fair-share owner.
  private long legacyUsedMemoryInBytes;

  private volatile long oversizedEntryCount = 0L;

  private long lastOvercommitWarningTimeNs;

  SubscriptionMemoryManager() {
    memoryBlock =
        IoTDBDescriptor.getInstance()
            .getMemoryConfig()
            .getSubscriptionMemoryManager()
            .exactAllocate(MEMORY_BLOCK_NAME, MemoryBlockType.DYNAMIC);
  }

  @TestOnly
  public SubscriptionMemoryManager(final long totalMemorySizeInBytes) {
    memoryBlock =
        new AtomicLongMemoryBlock(
            MEMORY_BLOCK_NAME, null, totalMemorySizeInBytes, MemoryBlockType.DYNAMIC);
  }

  /** Registers a queue for fair-share accounting. */
  public synchronized MemoryHandle registerQueue() {
    final MemoryHandle handle = new MemoryHandle(++nextHandleId);
    handles.put(handle.id, handle);
    return handle;
  }

  /**
   * Reserves memory through the compatibility owner. Consensus queues use a handle, which rejects
   * oversized entries instead of overcommitting the node budget.
   *
   * <p>A single entry larger than the whole budget is allowed only while the budget is otherwise
   * empty. This avoids permanently blocking progress while keeping the overrun bounded by one
   * entry.
   *
   * <p>This is a soft limit for such entries. Other queues can be blocked until the oversized entry
   * is released; the exception is exposed through metrics and a rate-limited warning.
   */
  public synchronized boolean tryAllocate(final long sizeInBytes) {
    return tryAllocateLegacy(sizeInBytes).isAccepted();
  }

  private AllocationResult tryAllocateLegacy(final long sizeInBytes) {
    if (sizeInBytes <= 0L) {
      return AllocationResult.accepted(getTotalMemorySizeInBytes(), getFreeMemorySizeInBytes());
    }
    final long total = getTotalMemorySizeInBytes();
    final long used = getUsedMemorySizeInBytes();
    if (total > 0L && used <= total && sizeInBytes <= total - used) {
      if (!memoryBlock.allocate(sizeInBytes)) {
        return AllocationResult.rejected(
            AllocationRejectionReason.MEMORY_LIMIT, total, getFreeMemorySizeInBytes());
      }
      legacyUsedMemoryInBytes += sizeInBytes;
      return AllocationResult.accepted(total, total - used - sizeInBytes);
    }
    if (total > 0L && used == 0L && sizeInBytes > total) {
      memoryBlock.forceAllocateWithoutLimitation(sizeInBytes);
      legacyUsedMemoryInBytes += sizeInBytes;
      oversizedEntryCount++;
      final long nowNs = System.nanoTime();
      if (oversizedEntryCount == 1L
          || nowNs - lastOvercommitWarningTimeNs >= OVERCOMMIT_WARNING_INTERVAL_NS) {
        lastOvercommitWarningTimeNs = nowNs;
        LOGGER.warn(
            DataNodePipeMessages
                .LOG_SUBSCRIPTION_MEMORY_OVERCOMMIT_ENTRYBYTES_ARG_BUDGETBYTES_ARG_OVERCOMMITBYTES_ARG_OTHER_QUEUES_MAY_BE_BLOCKED_UNTIL_RELEASE_DF9B914E,
            sizeInBytes,
            memoryBlock.getTotalMemorySizeInBytes(),
            getOvercommitSizeInBytes());
      }
      return AllocationResult.accepted(total, total - sizeInBytes);
    }
    return AllocationResult.rejected(
        sizeInBytes > total
            ? AllocationRejectionReason.OVERSIZED_ENTRY
            : AllocationRejectionReason.MEMORY_LIMIT,
        total,
        total - used);
  }

  public synchronized void release(final long sizeInBytes) {
    if (sizeInBytes <= 0L) {
      return;
    }
    final long released = Math.min(sizeInBytes, legacyUsedMemoryInBytes);
    if (released > 0L) {
      legacyUsedMemoryInBytes -= released;
      memoryBlock.release(released);
    }
  }

  public synchronized long getTotalMemorySizeInBytes() {
    return memoryBlock.getTotalMemorySizeInBytes();
  }

  public synchronized long getUsedMemorySizeInBytes() {
    return memoryBlock.getUsedMemoryInBytes();
  }

  public synchronized long getFreeMemorySizeInBytes() {
    return memoryBlock.getFreeMemoryInBytes();
  }

  public synchronized long getOvercommitSizeInBytes() {
    return Math.max(0L, getUsedMemorySizeInBytes() - getTotalMemorySizeInBytes());
  }

  public long getOversizedEntryCount() {
    return oversizedEntryCount;
  }

  private synchronized AllocationResult tryAllocate(
      final MemoryHandle handle, final long sizeInBytes) {
    final AllocationResult decision = inspectAllocationUnderLock(handle, sizeInBytes);
    if (!decision.isAccepted() || sizeInBytes <= 0L) {
      return decision;
    }

    if (!memoryBlock.allocate(sizeInBytes)) {
      return AllocationResult.rejected(
          AllocationRejectionReason.MEMORY_LIMIT,
          handle.getMemoryQuotaInBytesUnderLock(),
          getFreeMemorySizeInBytes());
    }
    handle.usedMemoryInBytes += sizeInBytes;
    return AllocationResult.accepted(
        handle.getMemoryQuotaInBytesUnderLock(), handle.getFreeMemorySizeInBytes());
  }

  private AllocationResult inspectAllocationUnderLock(
      final MemoryHandle handle, final long sizeInBytes) {
    if (sizeInBytes <= 0L) {
      return AllocationResult.accepted(
          handle.getMemoryQuotaInBytes(), handle.getFreeMemorySizeInBytes());
    }
    if (!handle.active || handle.closed || !handles.containsKey(handle.id)) {
      return AllocationResult.rejected(
          AllocationRejectionReason.MEMORY_LIMIT, getTotalMemorySizeInBytes(), 0L);
    }

    final long total = getTotalMemorySizeInBytes();
    final long used = getUsedMemorySizeInBytes();
    final long quota = handle.getMemoryQuotaInBytesUnderLock();
    final long protectedShare = quota / PROTECTED_SHARE_DIVISOR;
    final long maximum = handle.getMaximumMemorySizeInBytesUnderLock();
    long reservedByOthers = 0L;
    for (final MemoryHandle other : handles.values()) {
      if (other != handle && !other.closed && (other.active || other.usedMemoryInBytes > 0L)) {
        reservedByOthers += Math.max(protectedShare, other.usedMemoryInBytes);
      }
    }
    final long availableToOwner = Math.max(0L, total - legacyUsedMemoryInBytes - reservedByOthers);
    final long ownerUsed = handle.usedMemoryInBytes;
    final boolean fitsInNode = total > 0L && used <= total && sizeInBytes <= total - used;
    final boolean fitsInShare =
        ownerUsed <= availableToOwner && sizeInBytes <= availableToOwner - ownerUsed;

    if (fitsInNode && fitsInShare) {
      return AllocationResult.accepted(quota, availableToOwner - ownerUsed);
    }

    final AllocationRejectionReason reason;
    if (sizeInBytes > maximum) {
      reason = AllocationRejectionReason.OVERSIZED_ENTRY;
    } else if (ownerUsed > maximum || sizeInBytes > maximum - ownerUsed) {
      reason = AllocationRejectionReason.MEMORY_QUOTA;
    } else {
      reason = AllocationRejectionReason.MEMORY_LIMIT;
    }
    return AllocationResult.rejected(
        reason, quota, Math.min(availableToOwner - ownerUsed, total - used));
  }

  private synchronized void release(final MemoryHandle handle, final long sizeInBytes) {
    if (sizeInBytes <= 0L || handle.closed) {
      return;
    }
    final long released = Math.min(sizeInBytes, handle.usedMemoryInBytes);
    if (released > 0L) {
      handle.usedMemoryInBytes -= released;
      memoryBlock.release(released);
    }
  }

  private synchronized void unregister(final MemoryHandle handle) {
    if (handle.closed) {
      return;
    }
    final long remaining = handle.usedMemoryInBytes;
    if (remaining > 0L) {
      memoryBlock.release(remaining);
      handle.usedMemoryInBytes = 0L;
    }
    handles.remove(handle.id);
    handle.closed = true;
  }

  private synchronized int activeHandleCountUnderLock() {
    int count = 0;
    for (final MemoryHandle handle : handles.values()) {
      // An inactive queue keeps its share until lifecycle cleanup releases its allocations.
      if (!handle.closed && (handle.active || handle.usedMemoryInBytes > 0L)) {
        count++;
      }
    }
    return Math.max(1, count);
  }

  public enum AllocationRejectionReason {
    NONE("NONE"),
    MEMORY_QUOTA("SUBSCRIPTION_MEMORY_QUOTA"),
    MEMORY_LIMIT("SUBSCRIPTION_MEMORY_LIMIT"),
    OVERSIZED_ENTRY("SUBSCRIPTION_OVERSIZED_ENTRY");

    private final String code;

    AllocationRejectionReason(final String code) {
      this.code = code;
    }

    public String getCode() {
      return code;
    }
  }

  public static final class AllocationResult {
    private final boolean accepted;
    private final AllocationRejectionReason rejectionReason;
    private final long quotaInBytes;
    private final long freeMemoryInBytes;

    private AllocationResult(
        final boolean accepted,
        final AllocationRejectionReason rejectionReason,
        final long quotaInBytes,
        final long freeMemoryInBytes) {
      this.accepted = accepted;
      this.rejectionReason = rejectionReason;
      this.quotaInBytes = quotaInBytes;
      this.freeMemoryInBytes = freeMemoryInBytes;
    }

    private static AllocationResult accepted(
        final long quotaInBytes, final long freeMemoryInBytes) {
      return new AllocationResult(
          true, AllocationRejectionReason.NONE, quotaInBytes, freeMemoryInBytes);
    }

    private static AllocationResult rejected(
        final AllocationRejectionReason reason,
        final long quotaInBytes,
        final long freeMemoryInBytes) {
      return new AllocationResult(false, reason, quotaInBytes, freeMemoryInBytes);
    }

    public boolean isAccepted() {
      return accepted;
    }

    public AllocationRejectionReason getRejectionReason() {
      return rejectionReason;
    }

    public long getQuotaInBytes() {
      return quotaInBytes;
    }

    public long getFreeMemoryInBytes() {
      return freeMemoryInBytes;
    }
  }

  /** Per-queue view of the shared subscription memory budget. */
  public final class MemoryHandle implements AutoCloseable {
    private final long id;
    private long usedMemoryInBytes;
    private boolean active = true;
    private boolean closed;

    private MemoryHandle(final long id) {
      this.id = id;
    }

    public AllocationResult tryAllocate(final long sizeInBytes) {
      return SubscriptionMemoryManager.this.tryAllocate(this, sizeInBytes);
    }

    public void release(final long sizeInBytes) {
      SubscriptionMemoryManager.this.release(this, sizeInBytes);
    }

    public long getUsedMemorySizeInBytes() {
      synchronized (SubscriptionMemoryManager.this) {
        return usedMemoryInBytes;
      }
    }

    public long getMemoryQuotaInBytes() {
      synchronized (SubscriptionMemoryManager.this) {
        return getMemoryQuotaInBytesUnderLock();
      }
    }

    private long getMemoryQuotaInBytesUnderLock() {
      return Math.max(0L, getTotalMemorySizeInBytes() - legacyUsedMemoryInBytes)
          / activeHandleCountUnderLock();
    }

    public long getMaximumMemorySizeInBytes() {
      synchronized (SubscriptionMemoryManager.this) {
        return getMaximumMemorySizeInBytesUnderLock();
      }
    }

    private long getMaximumMemorySizeInBytesUnderLock() {
      final long budget = Math.max(0L, getTotalMemorySizeInBytes() - legacyUsedMemoryInBytes);
      final long protectedShare = getMemoryQuotaInBytesUnderLock() / PROTECTED_SHARE_DIVISOR;
      return Math.max(0L, budget - protectedShare * (activeHandleCountUnderLock() - 1L));
    }

    public long getFreeMemorySizeInBytes() {
      synchronized (SubscriptionMemoryManager.this) {
        // Probe the size of one byte to account for protected shares and live borrowed allocations.
        return Math.max(
            0L,
            SubscriptionMemoryManager.this
                .inspectAllocationUnderLock(this, 1L)
                .getFreeMemoryInBytes());
      }
    }

    public boolean isActive() {
      synchronized (SubscriptionMemoryManager.this) {
        return active && !closed;
      }
    }

    public void setActive(final boolean active) {
      synchronized (SubscriptionMemoryManager.this) {
        if (!closed) {
          this.active = active;
        }
      }
    }

    public boolean canAllocate(final long sizeInBytes) {
      synchronized (SubscriptionMemoryManager.this) {
        return SubscriptionMemoryManager.this
            .inspectAllocationUnderLock(this, sizeInBytes)
            .isAccepted();
      }
    }

    public AllocationRejectionReason inspectRejection(final long sizeInBytes) {
      synchronized (SubscriptionMemoryManager.this) {
        return SubscriptionMemoryManager.this
            .inspectAllocationUnderLock(this, sizeInBytes)
            .getRejectionReason();
      }
    }

    @Override
    public void close() {
      SubscriptionMemoryManager.this.unregister(this);
    }
  }
}
