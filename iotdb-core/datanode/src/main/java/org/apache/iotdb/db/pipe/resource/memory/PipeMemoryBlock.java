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

package org.apache.iotdb.db.pipe.resource.memory;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.LongUnaryOperator;

public class PipeMemoryBlock implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMemoryBlock.class);

  /** Maximum number of characters retained in an assigner diagnostic snapshot. */
  private static final int MAX_ASSIGNER_LENGTH = 2048;

  private static final AtomicLong NEXT_BLOCK_ID = new AtomicLong(1);

  private final PipeMemoryManager pipeMemoryManager;

  private final ReentrantLock lock = new ReentrantLock();

  private final long blockId;
  private final String name;
  private final PipeMemoryBlockCategory category;
  // The child keeps its parent alive while it is in use so the accounting chain cannot be
  // truncated by GC. The parent only keeps weak references to children, avoiding a parent-child
  // retention cycle and allowing forgotten zero-sized children to be collected.
  private final PipeMemoryBlock parent;
  private final Set<PipeMemoryBlock> children =
      Collections.newSetFromMap(new java.util.WeakHashMap<>());
  private final int hierarchyLevel;
  private final long allocationTime;
  private final AtomicReference<String> assigner = new AtomicReference<>();

  private final AtomicLong memoryUsageInBytes = new AtomicLong(0);
  // This is a high-water mark for observability, not a hard allocation limit.
  private final AtomicLong maxMemorySizeInBytes = new AtomicLong(0);

  private final AtomicReference<LongUnaryOperator> shrinkMethod = new AtomicReference<>();
  private final AtomicReference<BiConsumer<Long, Long>> shrinkCallback = new AtomicReference<>();
  private final AtomicReference<LongUnaryOperator> expandMethod = new AtomicReference<>();
  private final AtomicReference<BiConsumer<Long, Long>> expandCallback = new AtomicReference<>();

  private volatile boolean isReleased = false;

  public PipeMemoryBlock(final String name, final long memoryUsageInBytes) {
    this(
        PipeDataNodeResourceManager.memory(),
        name,
        memoryUsageInBytes,
        PipeMemoryBlockCategory.OTHER,
        null,
        null);
  }

  PipeMemoryBlock(
      final PipeMemoryManager pipeMemoryManager,
      final String name,
      final long memoryUsageInBytes,
      final PipeMemoryBlockCategory category,
      final String assigner,
      final PipeMemoryBlock parent) {
    this.pipeMemoryManager = Objects.requireNonNull(pipeMemoryManager);
    this.blockId = NEXT_BLOCK_ID.getAndIncrement();
    this.name = Objects.requireNonNull(name);
    this.category = category == null ? PipeMemoryBlockCategory.OTHER : category;
    this.parent = parent;
    if (parent != null) {
      synchronized (parent.children) {
        parent.children.add(this);
      }
    }
    this.hierarchyLevel = parent == null ? 0 : parent.getHierarchyLevel() + 1;
    this.allocationTime = System.currentTimeMillis();
    this.assigner.set(truncateAssigner(assigner));
    this.memoryUsageInBytes.set(Math.max(0, memoryUsageInBytes));
    this.maxMemorySizeInBytes.set(Math.max(0, memoryUsageInBytes));
  }

  /** Returns the globally unique identifier of this block instance. */
  public long getBlockId() {
    return blockId;
  }

  public String getName() {
    return name;
  }

  public PipeMemoryBlockCategory getCategory() {
    return category;
  }

  public PipeMemoryBlock getParentBlock() {
    return parent;
  }

  public Long getParentBlockId() {
    final PipeMemoryBlock parentBlock = getParentBlock();
    return parentBlock == null ? null : parentBlock.getBlockId();
  }

  public int getHierarchyLevel() {
    return hierarchyLevel;
  }

  public long getAllocationTime() {
    return allocationTime;
  }

  public long getAllocationTimeInMillis() {
    return allocationTime;
  }

  public String getAssigner() {
    return assigner.get();
  }

  /**
   * Replace the assigner diagnostic snapshot. The supplied object is converted immediately; no
   * event object is retained by the memory block.
   */
  public PipeMemoryBlock setAssigner(final Object assignerObject) {
    assigner.set(snapshotAssigner(assignerObject));
    return this;
  }

  public boolean isRootBlock() {
    return parent == null;
  }

  Set<PipeMemoryBlock> getChildrenSnapshot() {
    synchronized (children) {
      return Set.copyOf(children);
    }
  }

  void removeFromParent() {
    final PipeMemoryBlock parentBlock = getParentBlock();
    if (parentBlock != null) {
      synchronized (parentBlock.children) {
        parentBlock.children.remove(this);
      }
    }
  }

  /** Returns bytes charged to the global pool by this row (children report zero). */
  public long getAccountedMemoryUsageInBytes() {
    return isRootBlock() ? getMemoryUsageInBytes() : 0;
  }

  PipeMemoryManager getPipeMemoryManager() {
    return pipeMemoryManager;
  }

  static String snapshotAssigner(final Object assignerObject) {
    if (assignerObject == null) {
      return null;
    }

    try {
      if (assignerObject instanceof String) {
        return truncateAssigner((String) assignerObject);
      }
      if (assignerObject instanceof EnrichedEvent) {
        final EnrichedEvent event = (EnrichedEvent) assignerObject;
        // coreReportMessage() implementations may serialize a complete Tablet and recursively
        // expand their source events. Building that unbounded string merely to truncate it below
        // can exhaust the heap on the parser hot path, so retain only stable identity fields.
        return truncateAssigner(
            event.getClass().getSimpleName()
                + '['
                + truncateAssigner(event.getPipeName())
                + ','
                + event.getCreationTime()
                + ','
                + event.getRegionId()
                + ']');
      }

      // Avoid invoking an arbitrary toString() from an allocation hot path. Call sites that need
      // a richer value can provide an already bounded String explicitly.
      return truncateAssigner(assignerObject.getClass().getSimpleName());
    } catch (final Exception ignored) {
      return assignerObject.getClass().getSimpleName();
    }
  }

  private static String truncateAssigner(final String value) {
    if (value == null || value.length() <= MAX_ASSIGNER_LENGTH) {
      return value;
    }
    return value.substring(0, MAX_ASSIGNER_LENGTH);
  }

  public long getMemoryUsageInBytes() {
    return memoryUsageInBytes.get();
  }

  public void setMemoryUsageInBytes(final long memoryUsageInBytes) {
    final long normalizedMemoryUsageInBytes = Math.max(0, memoryUsageInBytes);
    this.memoryUsageInBytes.set(normalizedMemoryUsageInBytes);
    maxMemorySizeInBytes.accumulateAndGet(normalizedMemoryUsageInBytes, Math::max);
  }

  public long getMaxMemorySizeInBytes() {
    return maxMemorySizeInBytes.get();
  }

  public PipeMemoryBlock setShrinkMethod(final LongUnaryOperator shrinkMethod) {
    this.shrinkMethod.set(shrinkMethod);
    pipeMemoryManager.addShrinkableBlock(this);
    return this;
  }

  public PipeMemoryBlock setShrinkCallback(final BiConsumer<Long, Long> shrinkCallback) {
    this.shrinkCallback.set(shrinkCallback);
    return this;
  }

  public PipeMemoryBlock setExpandMethod(final LongUnaryOperator extendMethod) {
    this.expandMethod.set(extendMethod);
    pipeMemoryManager.addExpandableBlock(this);
    return this;
  }

  public PipeMemoryBlock setExpandCallback(final BiConsumer<Long, Long> expandCallback) {
    this.expandCallback.set(expandCallback);
    return this;
  }

  boolean shrink() {
    if (isReleased) {
      return false;
    }
    if (lock.tryLock()) {
      try {
        return doShrink();
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  private boolean doShrink() {
    if (shrinkMethod.get() == null) {
      return false;
    }

    final long oldMemorySizeInBytes = memoryUsageInBytes.get();
    final long newMemorySizeInBytes = shrinkMethod.get().applyAsLong(memoryUsageInBytes.get());

    final long memoryInBytesCanBeReleased = oldMemorySizeInBytes - newMemorySizeInBytes;
    if (memoryInBytesCanBeReleased <= 0
        || !pipeMemoryManager.release(this, memoryInBytesCanBeReleased)) {
      return false;
    }

    if (shrinkCallback.get() != null) {
      try {
        shrinkCallback.get().accept(oldMemorySizeInBytes, newMemorySizeInBytes);
      } catch (Exception e) {
        LOGGER.warn(DataNodePipeMessages.FAILED_TO_EXECUTE_THE_SHRINK_CALLBACK, e);
      }
    }
    return true;
  }

  boolean expand() {
    if (isReleased) {
      return false;
    }
    if (lock.tryLock()) {
      try {
        return doExpand();
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  private boolean doExpand() {
    if (expandMethod.get() == null) {
      return false;
    }

    final long oldMemorySizeInBytes = memoryUsageInBytes.get();
    final long newMemorySizeInBytes = expandMethod.get().applyAsLong(memoryUsageInBytes.get());

    final long memoryInBytesNeededToBeAllocated = newMemorySizeInBytes - oldMemorySizeInBytes;
    if (memoryInBytesNeededToBeAllocated <= 0
        || !pipeMemoryManager.tryAllocate(this, memoryInBytesNeededToBeAllocated)) {
      return false;
    }

    if (expandCallback.get() != null) {
      try {
        expandCallback.get().accept(oldMemorySizeInBytes, newMemorySizeInBytes);
      } catch (Exception e) {
        LOGGER.warn(DataNodePipeMessages.FAILED_TO_EXECUTE_THE_EXPAND_CALLBACK, e);
      }
    }
    return true;
  }

  boolean isReleased() {
    return isReleased;
  }

  void markAsReleased() {
    isReleased = true;
  }

  @Override
  public String toString() {
    return "PipeMemoryBlock{"
        + "blockId="
        + blockId
        + ", category="
        + category
        + ", name='"
        + name
        + '\''
        + ", usedMemoryInBytes="
        + memoryUsageInBytes.get()
        + ", maxMemorySizeInBytes="
        + maxMemorySizeInBytes.get()
        + ", parentBlockId="
        + getParentBlockId()
        + ", assigner='"
        + assigner.get()
        + '\''
        + ", isReleased="
        + isReleased
        + '}';
  }

  @Override
  public void close() {
    boolean isInterrupted = false;

    while (true) {
      try {
        if (lock.tryLock(50, TimeUnit.MICROSECONDS)) {
          try {
            pipeMemoryManager.release(this);
            if (Objects.nonNull(shrinkMethod.get())) {
              pipeMemoryManager.removeShrinkableBlock(this);
            }
            if (Objects.nonNull(expandMethod.get())) {
              pipeMemoryManager.removeExpandableBlock(this);
            }
            if (isInterrupted) {
              LOGGER.warn(DataNodePipeMessages.IS_RELEASED_AFTER_THREAD_INTERRUPTION, this);
            }
            break;
          } finally {
            lock.unlock();
          }
        }
      } catch (final InterruptedException e) {
        // Each time the close task is run, it means that the interrupt status left by the previous
        // tryLock does not need to be retained. Otherwise, it will lead to an infinite loop.
        isInterrupted = true;
        LOGGER.warn(DataNodePipeMessages.INTERRUPTED_WHILE_WAITING_FOR_THE_LOCK, e);
      }
    }

    // Restore the interrupt status of the current thread
    if (isInterrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
