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
 * SynchronizedMemoryReservationManager synchronizes the memory reservation and release operations.
 * However, synchronization is not necessary every time for the memory reservation and release
 * operations in single thread context. For example, each SeriesScanUtil itself reserves and
 * releases memory in a single thread context, but SeriesScanUtils in the same FI may share the same
 * SynchronizedMemoryReservationManager. So we can use this wrapper to batch the reserve and release
 * operations to reduce the overhead of synchronization. The actual reserve and release operations
 * are delegated to the wrapped MemoryReservationManager.
 */
public class SynchronizedMemoryReservationManagerWrapper implements MemoryReservationManager {

  private static final long MEMORY_BATCH_THRESHOLD = 1024L * 1024L;
  private final MemoryReservationManager memoryReservationManager;

  private long bytesToBeReserved = 0;

  private long bytesToBeReleased = 0;

  public SynchronizedMemoryReservationManagerWrapper(
      MemoryReservationManager memoryReservationManager) {
    this.memoryReservationManager = memoryReservationManager;
  }

  @Override
  public void reserveMemoryCumulatively(long size) {
    this.bytesToBeReserved += size;
    if (this.bytesToBeReserved >= MEMORY_BATCH_THRESHOLD) {
      memoryReservationManager.reserveMemoryCumulatively(bytesToBeReserved);
      this.bytesToBeReserved = 0;
    }
  }

  @Override
  public void reserveMemoryImmediately() {
    memoryReservationManager.reserveMemoryImmediately();
  }

  @Override
  public void releaseMemoryCumulatively(long size) {
    this.bytesToBeReleased += size;
    if (this.bytesToBeReleased >= MEMORY_BATCH_THRESHOLD) {
      memoryReservationManager.releaseMemoryCumulatively(this.bytesToBeReleased);
      this.bytesToBeReleased = 0;
    }
  }

  @Override
  public void releaseAllReservedMemory() {
    memoryReservationManager.releaseAllReservedMemory();
  }
}
