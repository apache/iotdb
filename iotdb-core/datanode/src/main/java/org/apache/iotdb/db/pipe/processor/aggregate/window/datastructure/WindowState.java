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

package org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure;

public enum WindowState {

  // Normally calculate the value
  COMPUTE,

  // Do nothing to the value
  IGNORE_VALUE,

  // This value is not calculated in this round of emit.
  // For example, current window is [1, 2], 3 comes, emit [1, 2] and compute 3, do not wipe the
  // window
  EMIT_WITHOUT_COMPUTE,

  // This value is calculated in this round of emit.
  // For example, current window is [1, 2], 3 comes, emit [1, 2, 3] and compute 3, do not wipe the
  // window
  EMIT_WITH_COMPUTE,

  // Purely wipe the window
  PURGE,

  // This value is not calculated in this round of emit, and wipe the window.
  // For example, current window is [1, 2],  3 comes, emit [1, 2] and wipe the window
  EMIT_AND_PURGE_WITHOUT_COMPUTE,

  // This value is calculated in this round of emit, and wipe the window.
  // For example, current window is [1, 2],  3 comes, emit [1, 2, 3] and wipe the window
  EMIT_AND_PURGE_WITH_COMPUTE;

  public boolean isEmit() {
    return isEmitWithCompute() || isEmitWithoutCompute();
  }

  public boolean isEmitWithoutCompute() {
    return this == EMIT_WITHOUT_COMPUTE || this == EMIT_AND_PURGE_WITHOUT_COMPUTE;
  }

  public boolean isEmitWithCompute() {
    return this == EMIT_WITH_COMPUTE || this == EMIT_AND_PURGE_WITH_COMPUTE;
  }

  public boolean isPurge() {
    return this == PURGE
        || this == EMIT_AND_PURGE_WITHOUT_COMPUTE
        || this == EMIT_AND_PURGE_WITH_COMPUTE;
  }

  public boolean isCalculate() {
    return this == COMPUTE
        || this == EMIT_WITHOUT_COMPUTE
        || this == EMIT_WITH_COMPUTE
        || this == EMIT_AND_PURGE_WITH_COMPUTE;
  }
}
