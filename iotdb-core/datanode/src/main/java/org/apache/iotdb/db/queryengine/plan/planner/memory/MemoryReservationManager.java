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

import org.apache.tsfile.utils.Pair;

public interface MemoryReservationManager {
  /**
   * Reserve memory for the given size. The memory reservation request will be accumulated and the
   * actual memory will be reserved when the accumulated memory exceeds the threshold.
   *
   * @param size the size of memory to reserve
   */
  void reserveMemoryCumulatively(final long size);

  /** Reserve memory for the accumulated memory size immediately. */
  void reserveMemoryImmediately();

  /**
   * Release memory for the given size.
   *
   * @param size the size of memory to release
   */
  void releaseMemoryCumulatively(final long size);

  /**
   * Release all reserved memory immediately. Make sure this method is called when the lifecycle of
   * this manager ends, Or the memory to be released in the batch may not be released correctly.
   */
  void releaseAllReservedMemory();

  /**
   * Release memory virtually without actually freeing the memory. This is used for memory
   * reservation transfer scenarios where memory ownership needs to be transferred between different
   * FragmentInstances without actual memory deallocation.
   *
   * <p>NOTE: When calling this method, it should be guaranteed that bytesToBeReserved +
   * reservedBytesInTotal >= size to ensure proper memory accounting and prevent negative
   * reservation values.
   *
   * @param size the size of memory to release virtually
   * @return a Pair where the left element is the amount of memory released from the pending
   *     reservation queue (bytesToBeReserved), and the right element is the amount of memory that
   *     has already been reserved
   */
  Pair<Long, Long> releaseMemoryVirtually(final long size);

  /**
   * Reserve memory virtually without actually allocating new memory. This is used to transfer
   * memory ownership from one FragmentInstances to another by reserving the memory that was
   * previously released virtually. It updates the internal reservation state without changing the
   * actual memory allocation.
   *
   * @param bytesToBeReserved the amount of memory that needs to be reserved cumulatively.
   * @param bytesAlreadyReserved the amount of memory that has already been reserved
   */
  void reserveMemoryVirtually(final long bytesToBeReserved, final long bytesAlreadyReserved);
}
