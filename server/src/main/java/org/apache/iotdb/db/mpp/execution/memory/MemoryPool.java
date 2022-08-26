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

import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.Validate;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/** A thread-safe memory pool. */
public class MemoryPool {

  public static class MemoryReservationFuture<V> extends AbstractFuture<V> {
    private final String queryId;
    private final long bytes;

    private MemoryReservationFuture(String queryId, long bytes) {
      this.queryId = Validate.notNull(queryId, "queryId cannot be null");
      Validate.isTrue(bytes > 0L, "bytes should be greater than zero.");
      this.bytes = bytes;
    }

    public long getBytes() {
      return bytes;
    }

    public String getQueryId() {
      return queryId;
    }

    public static <V> MemoryReservationFuture<V> create(String queryId, long bytes) {
      return new MemoryReservationFuture<>(queryId, bytes);
    }

    @Override
    public boolean set(@Nullable V value) {
      return super.set(value);
    }
  }

  private final String id;
  private final long maxBytes;
  private final long maxBytesPerQuery;

  private long reservedBytes = 0L;
  private final Map<String, Long> queryMemoryReservations = new HashMap<>();
  private final Queue<MemoryReservationFuture<Void>> memoryReservationFutures = new LinkedList<>();

  public MemoryPool(String id, long maxBytes, long maxBytesPerQuery) {
    this.id = Validate.notNull(id);
    Validate.isTrue(maxBytes > 0L, "max bytes should be greater than zero.");
    this.maxBytes = maxBytes;
    Validate.isTrue(
        maxBytesPerQuery > 0L && maxBytesPerQuery <= maxBytes,
        "max bytes per query should be greater than zero while less than or equal to max bytes.");
    this.maxBytesPerQuery = maxBytesPerQuery;
  }

  public String getId() {
    return id;
  }

  public long getMaxBytes() {
    return maxBytes;
  }

  /** @return if reserve succeed, pair.right will be true, otherwise false */
  public Pair<ListenableFuture<Void>, Boolean> reserve(String queryId, long bytes) {
    Validate.notNull(queryId);
    Validate.isTrue(
        bytes > 0L && bytes <= maxBytesPerQuery,
        "bytes should be greater than zero while less than or equal to max bytes per query.");

    ListenableFuture<Void> result;
    synchronized (this) {
      if (maxBytes - reservedBytes < bytes
          || maxBytesPerQuery - queryMemoryReservations.getOrDefault(queryId, 0L) < bytes) {
        result = MemoryReservationFuture.create(queryId, bytes);
        memoryReservationFutures.add((MemoryReservationFuture<Void>) result);
        return new Pair<>(result, Boolean.FALSE);
      } else {
        reservedBytes += bytes;
        queryMemoryReservations.merge(queryId, bytes, Long::sum);
        result = Futures.immediateFuture(null);
        return new Pair<>(result, Boolean.TRUE);
      }
    }
  }

  public boolean tryReserve(String queryId, long bytes) {
    Validate.notNull(queryId);
    Validate.isTrue(
        bytes > 0L && bytes <= maxBytesPerQuery,
        "bytes should be greater than zero while less than or equal to max bytes per query.");

    if (maxBytes - reservedBytes < bytes
        || maxBytesPerQuery - queryMemoryReservations.getOrDefault(queryId, 0L) < bytes) {
      return false;
    }
    synchronized (this) {
      if (maxBytes - reservedBytes < bytes
          || maxBytesPerQuery - queryMemoryReservations.getOrDefault(queryId, 0L) < bytes) {
        return false;
      }
      reservedBytes += bytes;
      queryMemoryReservations.merge(queryId, bytes, Long::sum);
    }

    return true;
  }

  /**
   * Cancel the specified memory reservation. If the reservation has finished, do nothing.
   *
   * @param future The future returned from {@link #reserve(String, long)}
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
    return ((MemoryReservationFuture<Void>) future).getBytes();
  }

  /**
   * Complete the specified memory reservation. If the reservation has finished, do nothing.
   *
   * @param future The future returned from {@link #reserve(String, long)}
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
    return ((MemoryReservationFuture<Void>) future).getBytes();
  }

  public synchronized void free(String queryId, long bytes) {
    Validate.notNull(queryId);
    Validate.isTrue(bytes > 0L);

    Long queryReservedBytes = queryMemoryReservations.get(queryId);
    Validate.notNull(queryReservedBytes);
    Validate.isTrue(bytes <= queryReservedBytes);

    queryReservedBytes -= bytes;
    if (queryReservedBytes == 0) {
      queryMemoryReservations.remove(queryId);
    } else {
      queryMemoryReservations.put(queryId, queryReservedBytes);
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
      long bytesToReserve = future.getBytes();
      if (maxBytes - reservedBytes < bytesToReserve) {
        return;
      }
      if (maxBytesPerQuery - queryMemoryReservations.getOrDefault(future.getQueryId(), 0L)
          >= bytesToReserve) {
        reservedBytes += bytesToReserve;
        queryMemoryReservations.merge(future.getQueryId(), bytesToReserve, Long::sum);
        future.set(null);
        iterator.remove();
      }
    }
  }

  public long getQueryMemoryReservedBytes(String queryId) {
    return queryMemoryReservations.getOrDefault(queryId, 0L);
  }

  public long getReservedBytes() {
    return reservedBytes;
  }
}
