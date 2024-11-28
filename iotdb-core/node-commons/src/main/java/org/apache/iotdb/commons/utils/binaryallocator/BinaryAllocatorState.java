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

package org.apache.iotdb.commons.utils.binaryallocator;

/**
 * The state transmission of a binary allocator.
 *
 * <pre>
 *     ----------------------------------------
 *     |                                      |
 *     |              ----------              |
 *     |              |        |              |
 *     |              v        |              v
 * UNINITIALIZED --> OPEN ---> TMP_CLOSE --> CLOSE
 *                    |                       ^
 *                    |                       |
 *                    -------------------------
 * </pre>
 */
public enum BinaryAllocatorState {
  /**
   * Binary allocator is open for allocation.
   *
   * <p>1.When configuration 'enableBinaryAllocator' is set to true, binary allocator state becomes
   * OPEN.
   *
   * <p>2.When current state is TMP_CLOSE and severe GC overhead is detected, the state will be set
   * to OPEN.
   */
  OPEN,

  /**
   * Binary allocator is close. When configuration 'enableBinaryAllocator' is set to false, binary
   * allocator state becomes CLOSE.
   */
  CLOSE,

  /** Binary allocator is temporarily closed by GC evictor. */
  TMP_CLOSE,

  /** The initial state of a binary allocator. */
  UNINITIALIZED;

  @Override
  public String toString() {
    return name();
  }
}
