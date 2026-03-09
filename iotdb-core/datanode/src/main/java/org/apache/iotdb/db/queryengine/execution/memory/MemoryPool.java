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

package org.apache.iotdb.db.queryengine.execution.memory;

import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.runtime.MemoryLeakException;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/** A thread-safe memory pool. */
public class MemoryPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryPool.class);

  public static class MemoryReservationFuture<V> extends AbstractFuture<V> {

    private final String queryId;
    private final String fragmentInstanceId;
    private final String planNodeId;
    private final long bytesToReserve;

    /**
     * MemoryReservationFuture is created when SinkHandle or SourceHandle tries to reserve memory
     * from pool. This field is max Bytes that SinkHandle or SourceHandle can reserve.
     */
    private final long maxBytesCanReserve;

    private MemoryReservationFuture(
        String queryId,
        String fragmentInstanceId,
        String planNodeId,
        long bytesToReserve,
        long maxBytesCanReserve) {
      this.queryId = Validate.notNull(queryId, "queryId cannot be null");
      this.fragmentInstanceId =
          Validate.notNull(fragmentInstanceId, "fragmentInstanceId cannot be null");
      this.planNodeId = Validate.notNull(planNodeId, "planNodeId cannot be null");
      Validate.isTrue(bytesToReserve > 0L, "bytesToReserve should be greater than zero.");
      Validate.isTrue(maxBytesCanReserve > 0L, "maxBytesCanReserve should be greater than zero.");
      this.bytesToReserve = bytesToReserve;
      this.maxBytesCanReserve = maxBytesCanReserve;
    }

    public String getQueryId() {
      return queryId;
    }

    public String getFragmentInstanceId() {
      return fragmentInstanceId;
    }

    public String getPlanNodeId() {
      return planNodeId;
    }

    public long getBytesToReserve() {
      return bytesToReserve;
    }

    public long getMaxBytesCanReserve() {
      return maxBytesCanReserve;
    }

    public static <V> MemoryReservationFuture<V> create(
        String queryId,
        String fragmentInstanceId,
        String planNodeId,
        long bytesToReserve,
        long maxBytesCanReserve) {
      return new MemoryReservationFuture<>(
          queryId, fragmentInstanceId, planNodeId, bytesToReserve, maxBytesCanReserve);
    }

    @Override
    public boolean set(@Nullable V value) {
      return super.set(value);
    }
  }

  private final String id;
  private final IMemoryBlock memoryBlock;
  private final long maxBytesPerFragmentInstance;

  /** queryId -> fragmentInstanceId -> planNodeId -> bytesReserved. */
  private final Map<String, Map<String, Map<String, Long>>> queryMemoryReservations =
      new ConcurrentHashMap<>();

  private final Queue<MemoryReservationFuture<Void>> memoryReservationFutures =
      new ConcurrentLinkedQueue<>();

  public MemoryPool(String id, MemoryManager memoryManager, long maxBytesPerFragmentInstance) {
    this.id = Validate.notNull(id, "id can not be null.");
    this.memoryBlock =
        memoryManager.exactAllocate(memoryManager.getName(), MemoryBlockType.DYNAMIC);
    Validate.isTrue(
        this.memoryBlock.getTotalMemorySizeInBytes() > 0L,
        "max bytes should be greater than zero: %d",
        this.memoryBlock.getTotalMemorySizeInBytes());
    Validate.isTrue(
        maxBytesPerFragmentInstance > 0L
            && maxBytesPerFragmentInstance <= this.memoryBlock.getTotalMemorySizeInBytes(),
        "max bytes per FI should be in (0,maxBytes]. maxBytesPerFI: %d, maxBytes: %d",
        maxBytesPerFragmentInstance,
        this.memoryBlock.getTotalMemorySizeInBytes());
    this.maxBytesPerFragmentInstance = maxBytesPerFragmentInstance;
  }

  public String getId() {
    return id;
  }

  public long getMaxBytes() {
    return memoryBlock.getTotalMemorySizeInBytes();
  }

  public long getRemainingBytes() {
    return memoryBlock.getFreeMemoryInBytes();
  }

  public int getQueryMemoryReservationSize() {
    return queryMemoryReservations.size();
  }

  public int getMemoryReservationSize() {
    return memoryReservationFutures.size();
  }

  /**
   * Before executing, we register memory map which is related to queryId, fragmentInstanceId, and
   * planNodeId to queryMemoryReservationsMap first.
   */
  public void registerPlanNodeIdToQueryMemoryMap(
      String queryId, String fragmentInstanceId, String planNodeId) {
    synchronized (queryMemoryReservations) {
      queryMemoryReservations
          .computeIfAbsent(queryId, x -> new ConcurrentHashMap<>())
          .computeIfAbsent(fragmentInstanceId, x -> new ConcurrentHashMap<>())
          .putIfAbsent(planNodeId, 0L);
    }
  }

  /**
   * If all fragmentInstanceIds related to one queryId have been registered, when the last fragment
   * instance is deregister, the queryId can be cleared.
   *
   * <p>If some fragmentInstanceIds have not been registered when queryId is cleared, they will
   * register queryId again with lock, so there is no concurrency problem.
   *
   * @throws MemoryLeakException throw {@link MemoryLeakException}
   */
  public void deRegisterFragmentInstanceFromQueryMemoryMap(
      String queryId, String fragmentInstanceId, boolean forceDeregister) {
    Map<String, Map<String, Long>> queryRelatedMemory = queryMemoryReservations.get(queryId);
    if (queryRelatedMemory != null) {
      Map<String, Long> fragmentRelatedMemory = queryRelatedMemory.get(fragmentInstanceId);
      boolean hasPotentialMemoryLeak = false;
      // fragmentRelatedMemory could be null if the FI has not reserved any memory(For example,
      // next() of root operator returns no data)
      if (fragmentRelatedMemory != null) {
        hasPotentialMemoryLeak =
            fragmentRelatedMemory.values().stream().anyMatch(value -> value != 0L);
      }
      if (!forceDeregister && hasPotentialMemoryLeak) {
        // If hasPotentialMemoryLeak is true, it means that LocalSourceChannel/LocalSourceHandles
        // have not been closed.
        // We should wait for them to be closed. Make sure this method is called again with
        // forceDeregister == true, after all LocalSourceChannel/LocalSourceHandles are closed.
        return;
      }
      synchronized (queryMemoryReservations) {
        queryRelatedMemory.remove(fragmentInstanceId);
        if (queryRelatedMemory.isEmpty()) {
          queryMemoryReservations.remove(queryId);
        }
      }
      if (hasPotentialMemoryLeak) {
        // hasPotentialMemoryLeak means that fragmentRelatedMemory is not null
        List<Map.Entry<String, Long>> invalidEntryList =
            fragmentRelatedMemory.entrySet().stream()
                .filter(entry -> entry.getValue() != 0L)
                .collect(Collectors.toList());
        throw new MemoryLeakException(
            String.format(
                "PlanNode related memory is not zero when trying to deregister FI from query memory pool. QueryId is : %s, FragmentInstanceId is : %s, Non-zero PlanNode related memory is : %s.",
                queryId, fragmentInstanceId, invalidEntryList));
      }
    }
  }

  /**
   * Reserve memory with bytesToReserve.
   *
   * @return if reserve succeed, pair.right will be true, otherwise false
   * @throws IllegalArgumentException throw exception if current query requests more memory than can
   *     be allocated.
   */
  public Pair<ListenableFuture<Void>, Boolean> reserve(
      String queryId,
      String fragmentInstanceId,
      String planNodeId,
      long bytesToReserve,
      long maxBytesCanReserve) {
    Validate.notNull(queryId, "queryId can not be null.");
    Validate.notNull(fragmentInstanceId, "fragmentInstanceId can not be null.");
    Validate.notNull(planNodeId, "planNodeId can not be null.");
    Validate.isTrue(
        bytesToReserve > 0L && bytesToReserve <= maxBytesPerFragmentInstance,
        "bytesToReserve should be in (0,maxBytesPerFI]. maxBytesPerFI: %d, bytesToReserve: %d",
        maxBytesPerFragmentInstance,
        bytesToReserve);
    if (bytesToReserve > maxBytesCanReserve) {
      LOGGER.warn(
          "Cannot reserve {}(Max: {}) bytes memory from MemoryPool for planNodeId{}",
          bytesToReserve,
          maxBytesCanReserve,
          planNodeId);
      throw new IllegalArgumentException(
          String.format(
              "Query is aborted since it requests more memory than can be allocated, bytesToReserve: %sB, maxBytesCanReserve: %sB",
              bytesToReserve, maxBytesCanReserve));
    }

    ListenableFuture<Void> result;
    if (tryReserve(queryId, fragmentInstanceId, planNodeId, bytesToReserve, maxBytesCanReserve)) {
      result = Futures.immediateFuture(null);
      return new Pair<>(result, Boolean.TRUE);
    } else {
      LOGGER.debug(
          "Blocked reserve request: {} bytes memory for planNodeId{}", bytesToReserve, planNodeId);
      rollbackReserve(queryId, fragmentInstanceId, planNodeId, bytesToReserve);
      result =
          MemoryReservationFuture.create(
              queryId, fragmentInstanceId, planNodeId, bytesToReserve, maxBytesCanReserve);
      memoryReservationFutures.add((MemoryReservationFuture<Void>) result);
      return new Pair<>(result, Boolean.FALSE);
    }
  }

  @TestOnly
  public boolean tryReserveForTest(
      String queryId,
      String fragmentInstanceId,
      String planNodeId,
      long bytesToReserve,
      long maxBytesCanReserve) {
    Validate.notNull(queryId, "queryId can not be null.");
    Validate.notNull(fragmentInstanceId, "fragmentInstanceId can not be null.");
    Validate.notNull(planNodeId, "planNodeId can not be null.");
    Validate.isTrue(
        bytesToReserve > 0L && bytesToReserve <= maxBytesPerFragmentInstance,
        "bytesToReserve should be in (0,maxBytesPerFI]. maxBytesPerFI: %d, bytesToReserve: %d",
        maxBytesPerFragmentInstance,
        bytesToReserve);

    if (tryReserve(queryId, fragmentInstanceId, planNodeId, bytesToReserve, maxBytesCanReserve)) {
      return true;
    } else {
      rollbackReserve(queryId, fragmentInstanceId, planNodeId, bytesToReserve);
      return false;
    }
  }

  /**
   * Cancel the specified memory reservation. If the reservation has finished, do nothing.
   *
   * @param future The future returned from {@link #reserve(String, String, String, long, long)}
   * @return If the future has not complete, return the number of bytes being reserved. Otherwise,
   *     return 0.
   */
  @SuppressWarnings("squid:S2445")
  public synchronized long tryCancel(ListenableFuture<Void> future) {
    Validate.notNull(future, "The future to be cancelled can not be null.");
    // add synchronized on the future to avoid that the future is concurrently completed by
    // MemoryPool.free() which may lead to memory leak.
    synchronized (future) {
      // If the future is not a MemoryReservationFuture, it must have been completed.
      if (future.isDone()) {
        return 0L;
      }
      Validate.isTrue(
          future instanceof MemoryReservationFuture,
          "invalid future type " + future.getClass().getSimpleName());
      future.cancel(true);
    }
    return ((MemoryReservationFuture<Void>) future).getBytesToReserve();
  }

  public void free(String queryId, String fragmentInstanceId, String planNodeId, long bytes) {
    Validate.notNull(queryId, "queryId can not be null.");
    Validate.isTrue(bytes > 0L);

    try {
      queryMemoryReservations
          .get(queryId)
          .get(fragmentInstanceId)
          .computeIfPresent(
              planNodeId,
              (k, reservedMemory) -> {
                if (reservedMemory < bytes) {
                  throw new IllegalArgumentException("Free more memory than has been reserved.");
                }
                return reservedMemory - bytes;
              });
    } catch (NullPointerException e) {
      throw new IllegalArgumentException("RelatedMemoryReserved can't be null when freeing memory");
    }

    memoryBlock.release(bytes);

    if (memoryReservationFutures.isEmpty()) {
      return;
    }
    Iterator<MemoryReservationFuture<Void>> iterator = memoryReservationFutures.iterator();
    while (iterator.hasNext()) {
      MemoryReservationFuture<Void> future = iterator.next();
      synchronized (future) {
        if (future.isCancelled() || future.isDone()) {
          iterator.remove();
          continue;
        }
        long bytesToReserve = future.getBytesToReserve();
        String curQueryId = future.getQueryId();
        String curFragmentInstanceId = future.getFragmentInstanceId();
        String curPlanNodeId = future.getPlanNodeId();
        long maxBytesCanReserve = future.getMaxBytesCanReserve();
        if (tryReserve(
            curQueryId, curFragmentInstanceId, curPlanNodeId, bytesToReserve, maxBytesCanReserve)) {
          future.set(null);
          iterator.remove();
        } else {
          rollbackReserve(curQueryId, curFragmentInstanceId, curPlanNodeId, bytesToReserve);
        }
      }
    }
  }

  public long getQueryMemoryReservedBytes(String queryId) {
    if (!queryMemoryReservations.containsKey(queryId)) {
      return 0L;
    }
    long sum = 0;
    for (Map<String, Long> map : queryMemoryReservations.get(queryId).values()) {
      sum = sum + map.values().stream().reduce(0L, Long::sum);
    }
    return sum;
  }

  public long getReservedBytes() {
    return memoryBlock.getUsedMemoryInBytes();
  }

  public boolean tryReserve(
      String queryId,
      String fragmentInstanceId,
      String planNodeId,
      long bytesToReserve,
      long maxBytesCanReserve) {
    long tryUsedBytes = memoryBlock.forceAllocateWithoutLimitation(bytesToReserve);
    long queryRemainingBytes =
        maxBytesCanReserve
            - queryMemoryReservations
                .get(queryId)
                .get(fragmentInstanceId)
                .merge(planNodeId, bytesToReserve, Long::sum);
    return tryUsedBytes <= memoryBlock.getTotalMemorySizeInBytes() && queryRemainingBytes >= 0;
  }

  private void rollbackReserve(
      String queryId, String fragmentInstanceId, String planNodeId, long bytesToReserve) {
    queryMemoryReservations
        .get(queryId)
        .get(fragmentInstanceId)
        .merge(planNodeId, -bytesToReserve, Long::sum);
    memoryBlock.release(bytesToReserve);
  }
}
