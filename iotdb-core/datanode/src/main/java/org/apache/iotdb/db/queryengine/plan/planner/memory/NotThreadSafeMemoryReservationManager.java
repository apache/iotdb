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

package org.apache.iotdb.db.queryengine.plan.planner.memory;

import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;

import org.apache.tsfile.utils.Pair;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkState;

@NotThreadSafe
public class NotThreadSafeMemoryReservationManager implements MemoryReservationManager {
  // To avoid reserving memory too frequently, we choose to do it in batches. This is the lower
  // bound for each batch.
  private static final long MEMORY_BATCH_THRESHOLD = 1024L * 1024L;

  private final LocalExecutionPlanner LOCAL_EXECUTION_PLANNER = LocalExecutionPlanner.getInstance();

  private final QueryId queryId;

  private final String contextHolder;

  private boolean isHighestPriority;

  private long reservedBytesInTotal = 0;

  /**
   * Bytes logically reserved but not taken from the operators pool due to highest-priority
   * fallback.
   */
  private long fallbackBytesInTotal = 0;

  private long bytesToBeReserved = 0;

  private long bytesToBeReleased = 0;

  public NotThreadSafeMemoryReservationManager(final QueryId queryId, final String contextHolder) {
    this.queryId = queryId;
    this.contextHolder = contextHolder;
  }

  @Override
  public void setHighestPriority(boolean isHighestPriority) {
    this.isHighestPriority = isHighestPriority;
  }

  @TestOnly
  public long getReservedBytesInTotalForTest() {
    return reservedBytesInTotal;
  }

  @TestOnly
  public long getFallbackBytesInTotalForTest() {
    return fallbackBytesInTotal;
  }

  @Override
  public void reserveMemoryCumulatively(final long size) {
    bytesToBeReserved += size;
    if (bytesToBeReserved >= MEMORY_BATCH_THRESHOLD) {
      reserveMemoryImmediately();
    }
  }

  @Override
  public void reserveMemoryImmediately() {
    if (bytesToBeReserved != 0) {
      long actualReserved =
          LOCAL_EXECUTION_PLANNER.reserveFromFreeMemoryForOperators(
              bytesToBeReserved,
              reservedBytesInTotal,
              queryId.getId(),
              contextHolder,
              isHighestPriority);
      if (actualReserved == 0) {
        fallbackBytesInTotal += bytesToBeReserved;
      } else {
        reservedBytesInTotal += actualReserved;
      }
      bytesToBeReserved = 0;
    }
  }

  @Override
  public void reserveMemoryImmediately(final long size) {
    if (size != 0) {
      long actualReserved =
          LOCAL_EXECUTION_PLANNER.reserveFromFreeMemoryForOperators(
              size, reservedBytesInTotal, queryId.getId(), contextHolder, isHighestPriority);
      if (actualReserved == 0) {
        fallbackBytesInTotal += size;
      } else {
        reservedBytesInTotal += actualReserved;
      }
    }
  }

  @Override
  public void releaseMemoryCumulatively(final long size) {
    if (size <= 0) {
      return;
    }
    bytesToBeReleased += size;
    if (bytesToBeReleased >= MEMORY_BATCH_THRESHOLD) {
      releaseBytesImmediately(bytesToBeReleased);
      bytesToBeReleased = 0;
    }
  }

  private void releaseBytesImmediately(final long size) {
    long poolBytes = deductReleaseAccounting(size);
    if (poolBytes > 0) {
      LOCAL_EXECUTION_PLANNER.releaseToFreeMemoryForOperators(poolBytes);
    }
  }

  /** Deduct release size from pending reserve, fallback quota, then pool reservation in order. */
  private long deductReleaseAccounting(final long size) {
    long remaining = size;
    if (remaining <= bytesToBeReserved) {
      bytesToBeReserved -= remaining;
      return 0L;
    }
    remaining -= bytesToBeReserved;
    bytesToBeReserved = 0;

    if (remaining <= fallbackBytesInTotal) {
      fallbackBytesInTotal -= remaining;
      return 0L;
    }
    remaining -= fallbackBytesInTotal;
    fallbackBytesInTotal = 0;

    reservedBytesInTotal -= remaining;
    checkState(reservedBytesInTotal >= 0, "Released bytes has been larger than reserved!");
    return remaining;
  }

  @Override
  public void releaseAllReservedMemory() {
    if (reservedBytesInTotal != 0) {
      LOCAL_EXECUTION_PLANNER.releaseToFreeMemoryForOperators(reservedBytesInTotal);
      reservedBytesInTotal = 0;
    }
    fallbackBytesInTotal = 0;
    bytesToBeReserved = 0;
    bytesToBeReleased = 0;
  }

  @Override
  public Pair<Long, Long> releaseMemoryVirtually(final long size) {
    long poolBytes = deductReleaseAccounting(size);
    return new Pair<>(size - poolBytes, poolBytes);
  }

  @Override
  public void reserveMemoryVirtually(
      final long bytesToBeReserved, final long bytesAlreadyReserved) {
    reservedBytesInTotal += bytesAlreadyReserved;
    reserveMemoryCumulatively(bytesToBeReserved);
  }
}
