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

/**
 * SynchronizedMemoryReservationContext synchronizes the memory reservation and release operations.
 * However, synchronization is not necessary every time for the memory reservation and release
 * operations in single thread context. For example, each SeriesScanUtil itself reserves and
 * releases memory in a single thread context, but SeriesScanUtils in the same FI may share the same
 * SynchronizedMemoryReservationContext. So we can use this wrapper to batch the reserve and release
 * operations to reduce the overhead of synchronization. The actual reserve and release operations
 * are delegated to the wrapped MemoryReservationContext.
 */
public class SynchronizedMemoryReservationContextWrapper implements MemoryReservationContext {

  private static final long MEMORY_BATCH_THRESHOLD = 1024L * 1024L;
  private final MemoryReservationContext memoryReservationContext;

  private long bytesToBeReserved = 0;

  private long bytesToBeReleased = 0;

  public SynchronizedMemoryReservationContextWrapper(
      MemoryReservationContext memoryReservationContext) {
    this.memoryReservationContext = memoryReservationContext;
  }

  @Override
  public void reserveMemoryAccumulatively(long size) {
    this.bytesToBeReserved += size;
    if (this.bytesToBeReserved >= MEMORY_BATCH_THRESHOLD) {
      memoryReservationContext.reserveMemoryAccumulatively(bytesToBeReserved);
      this.bytesToBeReserved = 0;
    }
  }

  @Override
  public void reserveMemoryImmediately() {
    memoryReservationContext.reserveMemoryImmediately();
  }

  @Override
  public void releaseMemoryAccumulatively(long size) {
    this.bytesToBeReleased += size;
    if (this.bytesToBeReleased >= MEMORY_BATCH_THRESHOLD) {
      memoryReservationContext.releaseMemoryAccumulatively(this.bytesToBeReleased);
      this.bytesToBeReleased = 0;
    }
  }

  @Override
  public void releaseAllReservedMemory() {
    memoryReservationContext.releaseAllReservedMemory();
  }
}
