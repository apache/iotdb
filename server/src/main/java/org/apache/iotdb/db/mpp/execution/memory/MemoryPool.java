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

package org.apache.iotdb.db.mpp.execution.memory;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

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
  private final long maxBytes;
  private final long maxBytesPerFragmentInstance;

  private long reservedBytes = 0L;
  /** queryId -> fragmentInstanceId -> planNodeId -> bytesReserved */
  private final Map<String, Map<String, Map<String, Long>>> queryMemoryReservations =
      new HashMap<>();

  private final Queue<MemoryReservationFuture<Void>> memoryReservationFutures = new LinkedList<>();

  public MemoryPool(String id, long maxBytes, long maxBytesPerFragmentInstance) {
    this.id = Validate.notNull(id);
    Validate.isTrue(maxBytes > 0L, "max bytes should be greater than zero: %d", maxBytes);
    this.maxBytes = maxBytes;
    Validate.isTrue(
        maxBytesPerFragmentInstance > 0L && maxBytesPerFragmentInstance <= maxBytes,
        "max bytes per query should be greater than zero while less than or equal to max bytes. maxBytesPerQuery: %d, maxBytes: %d",
        maxBytesPerFragmentInstance,
        maxBytes);
    this.maxBytesPerFragmentInstance = maxBytesPerFragmentInstance;
  }

  public String getId() {
    return id;
  }

  public long getMaxBytes() {
    return maxBytes;
  }

  /** @return if reserve succeed, pair.right will be true, otherwise false */
  public Pair<ListenableFuture<Void>, Boolean> reserve(
      String queryId,
      String fragmentInstanceId,
      String planNodeId,
      long bytesToReserve,
      long maxBytesCanReserve) {
    Validate.notNull(queryId);
    Validate.notNull(fragmentInstanceId);
    Validate.notNull(planNodeId);
    Validate.isTrue(
        bytesToReserve > 0L && bytesToReserve <= maxBytesPerFragmentInstance,
        "bytes should be greater than zero while less than or equal to max bytes per fragment instance: %d",
        bytesToReserve);
    if (bytesToReserve > maxBytesCanReserve) {
      LOGGER.warn(
          "Cannot reserve {}(Max: {}) bytes memory from MemoryPool for planNodeId{}",
          bytesToReserve,
          maxBytesCanReserve,
          planNodeId);
      throw new IllegalArgumentException(
          "Query is aborted since it requests more memory than can be allocated.");
    }

    ListenableFuture<Void> result;
    synchronized (this) {
      if (maxBytes - reservedBytes < bytesToReserve
          || maxBytesCanReserve
                  - queryMemoryReservations
                      .getOrDefault(queryId, Collections.emptyMap())
                      .getOrDefault(fragmentInstanceId, Collections.emptyMap())
                      .getOrDefault(planNodeId, 0L)
              < bytesToReserve) {
        LOGGER.debug(
            "Blocked reserve request: {} bytes memory for planNodeId{}",
            bytesToReserve,
            planNodeId);
        result =
            MemoryReservationFuture.create(
                queryId, fragmentInstanceId, planNodeId, bytesToReserve, maxBytesCanReserve);
        memoryReservationFutures.add((MemoryReservationFuture<Void>) result);
        return new Pair<>(result, Boolean.FALSE);
      } else {
        reservedBytes += bytesToReserve;
        queryMemoryReservations
            .computeIfAbsent(queryId, x -> new HashMap<>())
            .computeIfAbsent(fragmentInstanceId, x -> new HashMap<>())
            .merge(planNodeId, bytesToReserve, Long::sum);
        result = Futures.immediateFuture(null);
        return new Pair<>(result, Boolean.TRUE);
      }
    }
  }

  @TestOnly
  public boolean tryReserve(
      String queryId,
      String fragmentInstanceId,
      String planNodeId,
      long bytesToReserve,
      long maxBytesCanReserve) {
    Validate.notNull(queryId);
    Validate.notNull(fragmentInstanceId);
    Validate.notNull(planNodeId);
    Validate.isTrue(
        bytesToReserve > 0L && bytesToReserve <= maxBytesPerFragmentInstance,
        "bytes should be greater than zero while less than or equal to max bytes per fragment instance: %d",
        bytesToReserve);

    if (maxBytes - reservedBytes < bytesToReserve
        || maxBytesCanReserve
                - queryMemoryReservations
                    .getOrDefault(queryId, Collections.emptyMap())
                    .getOrDefault(fragmentInstanceId, Collections.emptyMap())
                    .getOrDefault(planNodeId, 0L)
            < bytesToReserve) {
      return false;
    }
    synchronized (this) {
      if (maxBytes - reservedBytes < bytesToReserve
          || maxBytesCanReserve
                  - queryMemoryReservations
                      .getOrDefault(queryId, Collections.emptyMap())
                      .getOrDefault(fragmentInstanceId, Collections.emptyMap())
                      .getOrDefault(planNodeId, 0L)
              < bytesToReserve) {
        return false;
      }
      reservedBytes += bytesToReserve;
      queryMemoryReservations
          .computeIfAbsent(queryId, x -> new HashMap<>())
          .computeIfAbsent(fragmentInstanceId, x -> new HashMap<>())
          .merge(planNodeId, bytesToReserve, Long::sum);
    }
    return true;
  }

  /**
   * Cancel the specified memory reservation. If the reservation has finished, do nothing.
   *
   * @param future The future returned from {@link #reserve(String, String, String, long, long)}
   * @return If the future has not complete, return the number of bytes being reserved. Otherwise,
   *     return 0.
   */
  public synchronized long tryCancel(ListenableFuture<Void> future) {
    Validate.notNull(future);
    // If the future is not a MemoryReservationFuture, it must have been completed.
    if (future.isDone()) {
      return 0L;
    }
    Validate.isTrue(
        future instanceof MemoryReservationFuture,
        "invalid future type " + future.getClass().getSimpleName());
    future.cancel(true);
    return ((MemoryReservationFuture<Void>) future).getBytesToReserve();
  }

  /**
   * Complete the specified memory reservation. If the reservation has finished, do nothing.
   *
   * @param future The future returned from {@link #reserve(String, String, String, long, long)}
   * @return If the future has not complete, return the number of bytes being reserved. Otherwise,
   *     return 0.
   */
  public synchronized long tryComplete(ListenableFuture<Void> future) {
    Validate.notNull(future);
    // If the future is not a MemoryReservationFuture, it must have been completed.
    if (future.isDone()) {
      return 0L;
    }
    Validate.isTrue(
        future instanceof MemoryReservationFuture,
        "invalid future type " + future.getClass().getSimpleName());
    ((MemoryReservationFuture<Void>) future).set(null);
    return ((MemoryReservationFuture<Void>) future).getBytesToReserve();
  }

  public void free(String queryId, String fragmentInstanceId, String planNodeId, long bytes) {
    List<MemoryReservationFuture<Void>> futureList = new ArrayList<>();
    synchronized (this) {
      Validate.notNull(queryId);
      Validate.isTrue(bytes > 0L);

      Long queryReservedBytes =
          queryMemoryReservations
              .getOrDefault(queryId, Collections.emptyMap())
              .getOrDefault(fragmentInstanceId, Collections.emptyMap())
              .get(planNodeId);
      Validate.notNull(queryReservedBytes);
      Validate.isTrue(bytes <= queryReservedBytes);

      queryReservedBytes -= bytes;
      if (queryReservedBytes == 0) {
        queryMemoryReservations.get(queryId).get(fragmentInstanceId).remove(planNodeId);
      } else {
        queryMemoryReservations
            .get(queryId)
            .get(fragmentInstanceId)
            .put(planNodeId, queryReservedBytes);
      }
      reservedBytes -= bytes;

      if (memoryReservationFutures.isEmpty()) {
        return;
      }
      Iterator<MemoryReservationFuture<Void>> iterator = memoryReservationFutures.iterator();
      while (iterator.hasNext()) {
        MemoryReservationFuture<Void> future = iterator.next();
        if (future.isCancelled() || future.isDone()) {
          continue;
        }
        long bytesToReserve = future.getBytesToReserve();
        String curQueryId = future.getQueryId();
        String curFragmentInstanceId = future.getFragmentInstanceId();
        String curPlanNodeId = future.getPlanNodeId();
        // check total reserved bytes in memory pool
        if (maxBytes - reservedBytes < bytesToReserve) {
          continue;
        }
        // check total reserved bytes of one Sink/Source handle
        if (future.getMaxBytesCanReserve()
                - queryMemoryReservations
                    .getOrDefault(curQueryId, Collections.emptyMap())
                    .getOrDefault(curFragmentInstanceId, Collections.emptyMap())
                    .getOrDefault(curPlanNodeId, 0L)
            >= bytesToReserve) {
          reservedBytes += bytesToReserve;
          queryMemoryReservations
              .computeIfAbsent(curQueryId, x -> new HashMap<>())
              .computeIfAbsent(curFragmentInstanceId, x -> new HashMap<>())
              .merge(curPlanNodeId, bytesToReserve, Long::sum);
          futureList.add(future);
          iterator.remove();
        }
      }
    }

    // why we need to put this outside MemoryPool's lock?
    // If we put this block inside the MemoryPool's lock, we will get deadlock case like the
    // following:
    // Assuming that thread-A: LocalSourceHandle.receive() -> A-SharedTsBlockQueue.remove() ->
    // MemoryPool.free() (hold MemoryPool's lock) -> future.set(null) -> try to get
    // B-SharedTsBlockQueue's lock
    // thread-B: LocalSourceHandle.receive() -> B-SharedTsBlockQueue.remove() (hold
    // B-SharedTsBlockQueue's lock) -> try to get MemoryPool's lock
    for (MemoryReservationFuture<Void> future : futureList) {
      try {
        future.set(null);
      } catch (Throwable t) {
        // ignore it, because we still need to notify other future
        LOGGER.error("error happened while trying to free memory: ", t);
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
    return reservedBytes;
  }
}
