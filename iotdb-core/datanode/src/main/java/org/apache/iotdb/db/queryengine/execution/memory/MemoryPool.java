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
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

/** A thread-safe memory pool. */
public class MemoryPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryPool.class);

  public static class MemoryReservationFuture<V> extends AbstractFuture<V> {

    private final String queryId;
    private final String fragmentInstanceId;
    private final String planNodeId;
    private final long bytesToReserve;
    // Assigned once by the pool before publication. Zero denotes an untracked future.
    private long reservationId;
    private FragmentMemoryReservation fragment;

    /**
     * MemoryReservationFuture is created when an exchange handle tries to reserve memory from the
     * pool. Local planning gives handles their FI-side limit in shared mode or their independent
     * plan-node limit in legacy mode. Retain the supplied limit when retrying this request.
     */
    private final long maxBytesCanReserve;

    private MemoryReservationFuture(
        String queryId,
        String fragmentInstanceId,
        String planNodeId,
        long bytesToReserve,
        long maxBytesCanReserve) {
      this.queryId =
          Validate.notNull(
              queryId, DataNodeQueryMessages.EXCEPTION_QUERYID_CANNOT_BE_NULL_861D7663);
      this.fragmentInstanceId =
          Validate.notNull(
              fragmentInstanceId,
              DataNodeQueryMessages.EXCEPTION_FRAGMENTINSTANCEID_CANNOT_BE_NULL_C722F460);
      this.planNodeId =
          Validate.notNull(
              planNodeId, DataNodeQueryMessages.EXCEPTION_PLANNODEID_CANNOT_BE_NULL_4533C72B);
      Validate.isTrue(
          bytesToReserve > 0L,
          DataNodeQueryMessages.EXCEPTION_BYTESTORESERVE_SHOULD_BE_GREATER_THAN_ZERO_DOT_56D15DE0);
      Validate.isTrue(
          maxBytesCanReserve > 0L,
          DataNodeQueryMessages
              .EXCEPTION_MAXBYTESCANRESERVE_SHOULD_BE_GREATER_THAN_ZERO_DOT_E9F7D365);
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

  public static class MemoryReservationResult {
    private final ListenableFuture<Void> future;
    private final boolean reserveSuccess;
    private final long reservedBytes;

    public MemoryReservationResult(
        ListenableFuture<Void> future, boolean reserveSuccess, long reservedBytes) {
      this.future = future;
      this.reserveSuccess = reserveSuccess;
      this.reservedBytes = reservedBytes;
    }

    public ListenableFuture<Void> getFuture() {
      return future;
    }

    public boolean isReserveSuccess() {
      return reserveSuccess;
    }

    public long getReservedBytes() {
      return reservedBytes;
    }
  }

  private final String id;
  private final IMemoryBlock memoryBlock;

  private static class PlanNodeReservation {
    private final MemorySide side;
    private volatile long bytesReserved;
    private boolean deregistered;

    private PlanNodeReservation(MemorySide side) {
      this.side = side;
    }

    @Override
    public String toString() {
      return side + ":" + bytesReserved;
    }
  }

  private static class FragmentMemoryReservation {
    private final Map<String, PlanNodeReservation> planNodes = new ConcurrentHashMap<>();
    private final AtomicLong inputUsedBytes = new AtomicLong();
    private final AtomicLong outputUsedBytes = new AtomicLong();
    private final Map<Long, MemoryReservationFuture<Void>> pendingReservations =
        new ConcurrentHashMap<>();
    // Enabled by local planning before drivers start; request limits are supplied by handles.
    private volatile boolean sharedBudget;
    // Protected by this monitor, together with waiter publication and retry.
    private boolean deregistered;

    private AtomicLong getUsedBytes(MemorySide side) {
      return side == MemorySide.INPUT ? inputUsedBytes : outputUsedBytes;
    }
  }

  /** queryId -> fragmentInstanceId -> FI side totals and planNode details. */
  private final Map<String, Map<String, FragmentMemoryReservation>> queryMemoryReservations =
      new ConcurrentHashMap<>();

  // Sequence keys retain FIFO wakeup order and allow removal without scanning unrelated requests.
  // Only blocked reservations use these indexes; successful admission keeps its existing fast path.
  private final Map<Long, MemoryReservationFuture<Void>> memoryReservationFutures =
      new ConcurrentSkipListMap<>();
  private final AtomicLong nextReservationId = new AtomicLong();

  public MemoryPool(String id, MemoryManager memoryManager, long maxBytesPerFragmentInstance) {
    this.id = Validate.notNull(id, DataNodeQueryMessages.EXCEPTION_ID_CAN_NOT_BE_NULL_DOT_BDD2AD7D);
    this.memoryBlock =
        memoryManager.exactAllocate(memoryManager.getName(), MemoryBlockType.DYNAMIC);
    Validate.isTrue(
        this.memoryBlock.getTotalMemorySizeInBytes() > 0L,
        DataNodeQueryMessages.EXCEPTION_MAX_BYTES_SHOULD_BE_GREATER_THAN_ZERO_COLON_ARG_EA1FB495,
        this.memoryBlock.getTotalMemorySizeInBytes());
    Validate.isTrue(
        maxBytesPerFragmentInstance > 0L
            && maxBytesPerFragmentInstance <= this.memoryBlock.getTotalMemorySizeInBytes(),
        DataNodeQueryMessages
            .EXCEPTION_MAX_BYTES_PER_FI_SHOULD_BE_IN_LEFT_PAREN_0_COMMA_MAXBYTES_RIGHT_BRACKET_DOT_MAXB_4D37C457,
        maxBytesPerFragmentInstance,
        this.memoryBlock.getTotalMemorySizeInBytes());
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
      String queryId, String fragmentInstanceId, String planNodeId, MemorySide side) {
    java.util.Objects.requireNonNull(side);
    synchronized (queryMemoryReservations) {
      queryMemoryReservations
          .computeIfAbsent(queryId, x -> new ConcurrentHashMap<>())
          .computeIfAbsent(fragmentInstanceId, x -> new FragmentMemoryReservation())
          .planNodes
          .compute(
              planNodeId,
              (key, reservation) -> {
                if (reservation != null && reservation.side != side) {
                  throw new IllegalArgumentException(
                      String.format(
                          DataNodeQueryMessages
                              .EXCEPTION_PLAN_NODE_ARG_IS_ALREADY_REGISTERED_ON_MEMORY_SIDE_ARG_NOT_ARG_4C1BB625,
                          planNodeId,
                          reservation.side,
                          side));
                }
                return reservation == null ? new PlanNodeReservation(side) : reservation;
              });
    }
  }

  /** Enable shared accounting after the complete FI has been checked by local planning. */
  public void registerFragmentInstanceSharedBudget(String queryId, String fragmentInstanceId) {
    synchronized (queryMemoryReservations) {
      FragmentMemoryReservation fragment =
          queryMemoryReservations
              .computeIfAbsent(queryId, x -> new ConcurrentHashMap<>())
              .computeIfAbsent(fragmentInstanceId, x -> new FragmentMemoryReservation());
      synchronized (fragment) {
        fragment.sharedBudget = true;
      }
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
    Map<String, FragmentMemoryReservation> queryRelatedMemory =
        queryMemoryReservations.get(queryId);
    if (queryRelatedMemory == null) {
      return;
    }
    FragmentMemoryReservation fragment = queryRelatedMemory.get(fragmentInstanceId);
    boolean hasPotentialMemoryLeak = false;
    synchronized (queryMemoryReservations) {
      if (fragment != null) {
        synchronized (fragment) {
          if (queryRelatedMemory.get(fragmentInstanceId) != fragment) {
            return;
          }
          if (!forceDeregister && hasReservedMemory(fragment)) {
            // Local queues can still hold this FI's allocation after its driver has closed.
            return;
          }
          fragment.deregistered = true;
          setPlanNodesDeregistered(fragment, true);
          // Wait for reservations that had already acquired their plan-node accounting lock.
          hasPotentialMemoryLeak = hasReservedMemory(fragment);
          if (!forceDeregister && hasPotentialMemoryLeak) {
            setPlanNodesDeregistered(fragment, false);
            fragment.deregistered = false;
            return;
          }
          queryRelatedMemory.remove(fragmentInstanceId, fragment);
        }
      }
      if (queryRelatedMemory.isEmpty()
          && queryMemoryReservations.get(queryId) == queryRelatedMemory) {
        queryMemoryReservations.remove(queryId);
      }
    }
    // Detach without completing the futures. Only owners may cancel and deduct pending bytes;
    // cancellation listeners otherwise mistake cancellation for admission. Publication and
    // retries cannot enter this account after it has been marked as deregistered.
    if (fragment != null) {
      for (MemoryReservationFuture<Void> future : fragment.pendingReservations.values()) {
        removePendingReservation(future);
      }
    }
    if (hasPotentialMemoryLeak) {
      List<Map.Entry<String, PlanNodeReservation>> invalidEntryList =
          fragment.planNodes.entrySet().stream()
              .filter(entry -> entry.getValue().bytesReserved != 0L)
              .collect(Collectors.toList());
      throw new MemoryLeakException(
          String.format(
              DataNodeQueryMessages
                  .QUERY_EXCEPTION_PLANNODE_RELATED_MEMORY_IS_NOT_ZERO_WHEN_TRYING_TO_DEREGISTER_E01109C5,
              queryId,
              fragmentInstanceId,
              invalidEntryList));
    }
  }

  private static boolean hasReservedMemory(FragmentMemoryReservation fragment) {
    return fragment.inputUsedBytes.get() != 0L
        || fragment.outputUsedBytes.get() != 0L
        || fragment.planNodes.values().stream().anyMatch(value -> value.bytesReserved != 0L);
  }

  private static void setPlanNodesDeregistered(
      FragmentMemoryReservation fragment, boolean deregistered) {
    for (PlanNodeReservation reservation : fragment.planNodes.values()) {
      synchronized (reservation) {
        reservation.deregistered = deregistered;
      }
    }
  }

  /**
   * Reserve memory with bytesToReserve respect priority.
   *
   * @return if reserve succeed, reservedBytes may be zero or equals with bytesToReserve; if reserve
   *     failed, reservedBytes must be equals with bytesToReserve
   * @throws IllegalArgumentException throw exception if current query requests more memory than can
   *     be allocated.
   */
  public MemoryReservationResult reserveWithPriority(
      String queryId,
      String fragmentInstanceId,
      String planNodeId,
      long bytesToReserve,
      long maxBytesCanReserve,
      boolean isHighestPriority) {
    Validate.notNull(queryId, DataNodeQueryMessages.EXCEPTION_QUERYID_CAN_NOT_BE_NULL_DOT_16639DBE);
    Validate.notNull(
        fragmentInstanceId,
        DataNodeQueryMessages.EXCEPTION_FRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_E88CF18B);
    Validate.notNull(
        planNodeId, DataNodeQueryMessages.EXCEPTION_PLANNODEID_CAN_NOT_BE_NULL_DOT_44027620);
    Validate.isTrue(
        bytesToReserve > 0L,
        DataNodeQueryMessages.EXCEPTION_BYTESTORESERVE_SHOULD_BE_GREATER_THAN_ZERO_DOT_56D15DE0);
    // A request must fit both the caller's FI/handle limit and the physical global pool.
    maxBytesCanReserve = Math.min(maxBytesCanReserve, getMaxBytes());
    if (bytesToReserve > maxBytesCanReserve) {
      LOGGER.warn(
          DataNodeQueryMessages
              .CANNOT_RESERVE_ARG_MAX_ARG_BYTES_MEMORY_FROM_MEMORYPOOL_FOR_PLANNODEIDARG,
          bytesToReserve,
          maxBytesCanReserve,
          planNodeId);
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages
                  .QUERY_EXCEPTION_QUERY_IS_ABORTED_SINCE_IT_REQUESTS_MORE_MEMORY_THAN_CAN_D77C2921,
              bytesToReserve,
              maxBytesCanReserve));
    }

    if (tryReserve(queryId, fragmentInstanceId, planNodeId, bytesToReserve, maxBytesCanReserve)) {
      return new MemoryReservationResult(immediateVoidFuture(), true, bytesToReserve);
    } else {
      if (isHighestPriority) {
        // SHOW QUERIES: treat as success with zero bytes reserved from pool when insufficient.
        return new MemoryReservationResult(immediateVoidFuture(), true, 0L);
      }
      LOGGER.debug(
          DataNodeQueryMessages.BLOCKED_RESERVE_REQUEST_ARG_BYTES_MEMORY_FOR_PLANNODEIDARG,
          bytesToReserve,
          planNodeId);
      MemoryReservationFuture<Void> result =
          MemoryReservationFuture.create(
              queryId, fragmentInstanceId, planNodeId, bytesToReserve, maxBytesCanReserve);
      FragmentMemoryReservation fragment =
          getFragmentReservation(queryId, fragmentInstanceId, planNodeId);
      synchronized (fragment) {
        // Deregistration must not miss a request published after its pending-request scan.
        if (fragment.deregistered) {
          throw new IllegalArgumentException(
              String.format(
                  DataNodeQueryMessages
                      .EXCEPTION_MEMORY_RESERVATION_IS_NOT_REGISTERED_FOR_QUERY_ARG_FRAGMENT_ARG_PLAN_NODE_ARG_67A4B81F,
                  queryId,
                  fragmentInstanceId,
                  planNodeId));
        }
        result.fragment = fragment;
        result.reservationId = nextReservationId.incrementAndGet();
        fragment.pendingReservations.put(result.reservationId, result);
        memoryReservationFutures.put(result.reservationId, result);
      }
      // A concurrent free may have missed this request before it was published. Retry after
      // publication, under the same future lock used by free and tryCancel to avoid charging twice.
      try {
        if (tryCompleteReservation(result)) {
          removePendingReservation(result);
        }
      } catch (RuntimeException | Error e) {
        removePendingReservation(result);
        throw e;
      }
      return new MemoryReservationResult(result, false, bytesToReserve);
    }
  }

  @TestOnly
  public boolean tryReserveForTest(
      String queryId,
      String fragmentInstanceId,
      String planNodeId,
      long bytesToReserve,
      long maxBytesCanReserve) {
    Validate.notNull(queryId, DataNodeQueryMessages.EXCEPTION_QUERYID_CAN_NOT_BE_NULL_DOT_16639DBE);
    Validate.notNull(
        fragmentInstanceId,
        DataNodeQueryMessages.EXCEPTION_FRAGMENTINSTANCEID_CAN_NOT_BE_NULL_DOT_E88CF18B);
    Validate.notNull(
        planNodeId, DataNodeQueryMessages.EXCEPTION_PLANNODEID_CAN_NOT_BE_NULL_DOT_44027620);
    Validate.isTrue(
        bytesToReserve > 0L,
        DataNodeQueryMessages.EXCEPTION_BYTESTORESERVE_SHOULD_BE_GREATER_THAN_ZERO_DOT_56D15DE0);

    return tryReserve(queryId, fragmentInstanceId, planNodeId, bytesToReserve, maxBytesCanReserve);
  }

  /**
   * Marks a fragment account as deregistered without removing it from the map, mirroring the
   * production mark that precedes the pending-request drain. Lets tests deterministically exercise
   * the publication rejection of a closed account.
   */
  @TestOnly
  public void markFragmentDeregisteredForTest(String queryId, String fragmentInstanceId) {
    FragmentMemoryReservation fragment =
        queryMemoryReservations.get(queryId).get(fragmentInstanceId);
    synchronized (fragment) {
      fragment.deregistered = true;
      setPlanNodesDeregistered(fragment, true);
    }
  }

  /**
   * Cancel the specified memory reservation. If the reservation has finished, do nothing.
   *
   * @param future The future returned from {@link #reserveWithPriority(String, String, String,
   *     long, long, boolean)}
   * @return If the future has not complete, return the number of bytes being reserved. Otherwise,
   *     return 0.
   */
  @SuppressWarnings("squid:S2445")
  public synchronized long tryCancel(ListenableFuture<Void> future) {
    Validate.notNull(
        future,
        DataNodeQueryMessages.EXCEPTION_THE_FUTURE_TO_BE_CANCELLED_CAN_NOT_BE_NULL_DOT_73CE402A);
    // add synchronized on the future to avoid that the future is concurrently completed by
    // MemoryPool.free() which may lead to memory leak.
    synchronized (future) {
      // If the future is not a MemoryReservationFuture, it must have been completed.
      if (future.isDone()) {
        if (future instanceof MemoryReservationFuture) {
          removePendingReservation((MemoryReservationFuture<Void>) future);
        }
        return 0L;
      }
      Validate.isTrue(
          future instanceof MemoryReservationFuture,
          DataNodeQueryMessages.EXCEPTION_INVALID_FUTURE_TYPE_14507EF5
              + future.getClass().getSimpleName());
      future.cancel(true);
      removePendingReservation((MemoryReservationFuture<Void>) future);
    }
    return ((MemoryReservationFuture<Void>) future).getBytesToReserve();
  }

  public void free(String queryId, String fragmentInstanceId, String planNodeId, long bytes) {
    Validate.notNull(queryId, DataNodeQueryMessages.EXCEPTION_QUERYID_CAN_NOT_BE_NULL_DOT_16639DBE);
    Validate.isTrue(bytes > 0L);

    Map<String, FragmentMemoryReservation> fragments = queryMemoryReservations.get(queryId);
    FragmentMemoryReservation fragment =
        fragments == null ? null : fragments.get(fragmentInstanceId);
    if (fragment == null) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages
              .QUERY_EXCEPTION_RELATEDMEMORYRESERVED_CAN_T_BE_NULL_WHEN_FREEING_MEMORY_C80009F2);
    }
    PlanNodeReservation reservation =
        requireReservation(
            fragment.planNodes.get(planNodeId), queryId, fragmentInstanceId, planNodeId);
    Object accountingLock = fragment.sharedBudget ? fragment : reservation;
    synchronized (accountingLock) {
      if (reservation.bytesReserved < bytes) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.FREE_MORE_MEMORY_THAN_HAS_BEEN_RESERVED);
      }
      reservation.bytesReserved -= bytes;
      fragment.getUsedBytes(reservation.side).addAndGet(-bytes);
      memoryBlock.release(bytes);
    }
    // Complete futures outside the accounting lock: listeners can reserve or free other inputs.

    if (memoryReservationFutures.isEmpty()) {
      return;
    }
    Iterator<MemoryReservationFuture<Void>> iterator = memoryReservationFutures.values().iterator();
    while (iterator.hasNext()) {
      MemoryReservationFuture<Void> future = iterator.next();
      if (tryCompleteReservation(future)) {
        removePendingReservation(future);
      }
    }
  }

  private void removePendingReservation(MemoryReservationFuture<Void> future) {
    memoryReservationFutures.remove(future.reservationId, future);
    if (future.fragment != null) {
      future.fragment.pendingReservations.remove(future.reservationId, future);
    }
  }

  private boolean tryCompleteReservation(MemoryReservationFuture<Void> future) {
    synchronized (future) {
      if (future.isDone()) {
        return true;
      }
      long bytesToReserve = future.getBytesToReserve();
      String queryId = future.getQueryId();
      String fragmentInstanceId = future.getFragmentInstanceId();
      String planNodeId = future.getPlanNodeId();
      synchronized (future.fragment) {
        if (future.fragment.deregistered) {
          return true;
        }
        if (!tryReserve(
            queryId,
            fragmentInstanceId,
            planNodeId,
            bytesToReserve,
            future.getMaxBytesCanReserve())) {
          return false;
        }
      }
      // Owner callbacks may register or release other accounts.
      future.set(null);
      return true;
    }
  }

  public long getQueryMemoryReservedBytes(String queryId) {
    Map<String, FragmentMemoryReservation> fragments = queryMemoryReservations.get(queryId);
    if (fragments == null) {
      return 0L;
    }
    long sum = 0;
    for (FragmentMemoryReservation fragment : fragments.values()) {
      sum += fragment.inputUsedBytes.get() + fragment.outputUsedBytes.get();
    }
    return sum;
  }

  public long getFragmentMemoryReservedBytes(
      String queryId, String fragmentInstanceId, MemorySide side) {
    Map<String, FragmentMemoryReservation> fragments = queryMemoryReservations.get(queryId);
    FragmentMemoryReservation fragment =
        fragments == null ? null : fragments.get(fragmentInstanceId);
    return fragment == null ? 0L : fragment.getUsedBytes(side).get();
  }

  public long getPlanNodeMemoryReservedBytes(
      String queryId, String fragmentInstanceId, String planNodeId) {
    Map<String, FragmentMemoryReservation> fragments = queryMemoryReservations.get(queryId);
    FragmentMemoryReservation fragment =
        fragments == null ? null : fragments.get(fragmentInstanceId);
    PlanNodeReservation reservation = fragment == null ? null : fragment.planNodes.get(planNodeId);
    return reservation == null ? 0L : reservation.bytesReserved;
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
    FragmentMemoryReservation fragment =
        getFragmentReservation(queryId, fragmentInstanceId, planNodeId);
    PlanNodeReservation reservation =
        requireReservation(
            fragment.planNodes.get(planNodeId), queryId, fragmentInstanceId, planNodeId);
    // Legacy quotas are independent per plan node. Only shared quotas require the FI lock.
    Object accountingLock = fragment.sharedBudget ? fragment : reservation;
    synchronized (accountingLock) {
      if (reservation.deregistered) {
        return false;
      }
      long usedBytes =
          fragment.sharedBudget
              ? fragment.getUsedBytes(reservation.side).get()
              : reservation.bytesReserved;
      // Do not publish speculative charges. Two competing requests must not reject one another
      // against different counters and then both wait after their rollbacks restore capacity.
      if (bytesToReserve > maxBytesCanReserve - usedBytes
          || !memoryBlock.allocate(bytesToReserve)) {
        return false;
      }
      fragment.getUsedBytes(reservation.side).addAndGet(bytesToReserve);
      reservation.bytesReserved += bytesToReserve;
      return true;
    }
  }

  private FragmentMemoryReservation getFragmentReservation(
      String queryId, String fragmentInstanceId, String planNodeId) {
    Map<String, FragmentMemoryReservation> fragments = queryMemoryReservations.get(queryId);
    FragmentMemoryReservation fragment =
        fragments == null ? null : fragments.get(fragmentInstanceId);
    if (fragment == null) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_MEMORY_RESERVATION_IS_NOT_REGISTERED_FOR_QUERY_ARG_FRAGMENT_ARG_PLAN_NODE_ARG_67A4B81F,
              queryId,
              fragmentInstanceId,
              planNodeId));
    }
    return fragment;
  }

  private PlanNodeReservation requireReservation(
      PlanNodeReservation reservation,
      String queryId,
      String fragmentInstanceId,
      String planNodeId) {
    if (reservation == null) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_MEMORY_RESERVATION_IS_NOT_REGISTERED_FOR_QUERY_ARG_FRAGMENT_ARG_PLAN_NODE_ARG_67A4B81F,
              queryId,
              fragmentInstanceId,
              planNodeId));
    }
    return reservation;
  }
}
