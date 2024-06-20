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

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;

public class UnsynchronizedMemoryReservationContext implements MemoryReservationContext {
  // To avoid reserving memory too frequently, we choose to do it in batches. This is the lower
  // bound for each batch.
  private static final long MEMORY_BATCH_THRESHOLD = 1024L * 1024L;

  private final LocalExecutionPlanner LOCAL_EXECUTION_PLANNER = LocalExecutionPlanner.getInstance();

  private final QueryId queryId;

  private final String contextHolder;

  private long reservedBytesInTotal = 0;

  private long bytesToBeReserved = 0;

  private long bytesToBeReleased = 0;

  public UnsynchronizedMemoryReservationContext(final QueryId queryId, final String contextHolder) {
    this.queryId = queryId;
    this.contextHolder = contextHolder;
  }

  @Override
  public void reserveMemoryAccumulatively(final long size) {
    this.bytesToBeReserved += size;
    if (this.bytesToBeReserved >= MEMORY_BATCH_THRESHOLD) {
      reserveMemoryImmediately();
    }
  }

  @Override
  public void reserveMemoryImmediately() {
    if (bytesToBeReserved != 0) {
      LOCAL_EXECUTION_PLANNER.reserveFromFreeMemoryForOperators(
          bytesToBeReserved, reservedBytesInTotal, queryId.getId(), contextHolder);
      this.reservedBytesInTotal += bytesToBeReserved;
      this.bytesToBeReserved = 0;
    }
  }

  @Override
  public void releaseMemoryAccumulatively(final long size) {
    this.bytesToBeReleased += size;
    if (bytesToBeReleased >= MEMORY_BATCH_THRESHOLD) {
      long bytesToRelease;
      if (this.bytesToBeReleased <= bytesToBeReserved) {
        bytesToBeReserved -= this.bytesToBeReleased;
      } else {
        bytesToRelease = this.bytesToBeReleased - bytesToBeReserved;
        bytesToBeReserved = 0;
        LOCAL_EXECUTION_PLANNER.releaseToFreeMemoryForOperators(bytesToRelease);
        reservedBytesInTotal -= bytesToRelease;
      }
      this.bytesToBeReleased = 0;
    }
  }

  @Override
  public void releaseAllReservedMemory() {
    if (reservedBytesInTotal != 0) {
      LOCAL_EXECUTION_PLANNER.releaseToFreeMemoryForOperators(reservedBytesInTotal);
      reservedBytesInTotal = 0;
      bytesToBeReserved = 0;
      bytesToBeReleased = 0;
    }
  }
}
