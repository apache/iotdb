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
}
