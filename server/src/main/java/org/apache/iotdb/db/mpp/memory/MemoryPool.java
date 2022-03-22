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

package org.apache.iotdb.db.mpp.memory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.Map;

/** Manages certain amount of memory. */
public class MemoryPool {

  private final String id;
  private final long maxBytes;
  private final long maxBytesPerQuery;

  private long reservedBytes = 0L;
  private final Map<String, Long> queryMemoryReservations = new HashMap<>();
  private final Map<String, SettableFuture<Void>> queryIdToFuture = new HashMap<>();

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

  public ListenableFuture<Void> reserve(String queryId, long bytes) {
    Validate.notNull(queryId);
    Validate.isTrue(
        bytes > 0L && bytes <= maxBytesPerQuery,
        "bytes should be greater than zero while less than or equal to max bytes per query.");

    ListenableFuture<Void> result;
    synchronized (this) {
      reservedBytes += bytes;
      queryMemoryReservations.merge(queryId, bytes, Long::sum);
      if (reservedBytes > maxBytes
          || queryMemoryReservations.getOrDefault(queryId, 0L) > maxBytesPerQuery) {
        queryIdToFuture.put(queryId, SettableFuture.create());
        result = queryIdToFuture.get(queryId);
      } else {
        result = Futures.immediateFuture(null);
      }
    }

    return result;
  }

  public boolean tryReserve(String queryId, long bytes) {
    Validate.notNull(queryId);
    Validate.isTrue(
        bytes > 0L && bytes <= maxBytesPerQuery,
        "bytes should be greater than zero while less than or equal to max bytes per query.");

    if (bytes > maxBytesPerQuery)
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

    if (reservedBytes <= maxBytes
        && queryMemoryReservations.getOrDefault(queryId, 0L) <= maxBytesPerQuery) {
      SettableFuture<Void> future = queryIdToFuture.get(queryId);
      if (future != null) {
        future.set(null);
      }
    }
  }

  public synchronized long getQueryMemoryReservedBytes(String queryId) {
    return queryMemoryReservations.getOrDefault(queryId, 0L);
  }

  public long getReservedBytes() {
    return reservedBytes;
  }
}
