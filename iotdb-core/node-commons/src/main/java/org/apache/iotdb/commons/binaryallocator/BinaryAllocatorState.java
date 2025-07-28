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

package org.apache.iotdb.commons.binaryallocator;

/**
 * The state transmission of a binary allocator.
 *
 * <pre>
 *     ----------------------------------------
 *     |                                      |
 *     |              ----------              |
 *     |              |        |              |
 *     |              v        |              v
 * UNINITIALIZED --> OPEN ---> PENDING -->  CLOSE
 *                    ^                       ^
 *                    |                       |
 *                    -------------------------
 * </pre>
 *
 * State Transition Logic:
 *
 * <ul>
 *   <li><b>UNINITIALIZED -> CLOSE</b>: When enable_binary_allocator = false
 *   <li><b>UNINITIALIZED -> OPEN</b>: When enable_binary_allocator = true
 *   <li><b>OPEN -> CLOSE</b>: When enable_binary_allocator is hot reload to false
 *   <li><b>CLOSE -> OPEN</b>: When enable_binary_allocator is hot reload to true
 *   <li><b>PENDING -> CLOSE</b>: When enable_binary_allocator is hot reload to false
 *   <li><b>OPEN -> PENDING</b>: When in OPEN state and GC time percentage exceeds 30%, indicating
 *       allocator ineffectiveness
 *   <li><b>PENDING -> OPEN</b>: When GC time percentage drops below 5%, returning to normal state
 *       and re-enabling the allocator. Or when enable_binary_allocator is hot reload to true.
 * </ul>
 */
public enum BinaryAllocatorState {
  /** Binary allocator is open for allocation. */
  OPEN,

  /** Binary allocator is close. All allocations are from the JVM heap. */
  CLOSE,

  /**
   * Binary allocator is temporarily closed by GC evictor. All allocations are from the JVM heap.
   * Allocator can be restarted afterward.
   */
  PENDING,

  /** The initial state of the allocator. */
  UNINITIALIZED;

  @Override
  public String toString() {
    return name();
  }
}
