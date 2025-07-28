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

package org.apache.iotdb.db.queryengine.execution.operator.process.function.partition;

public class PartitionState {

  public static final PartitionState NEED_MORE_DATA_STATE =
      new PartitionState(StateType.NEED_MORE_DATA, null);
  public static final PartitionState FINISHED_STATE = new PartitionState(StateType.FINISHED, null);
  public static final PartitionState INIT_STATE = new PartitionState(StateType.INIT, null);

  public static PartitionState newPartitionState(Slice slice) {
    return new PartitionState(StateType.NEW_PARTITION, slice);
  }

  public static PartitionState iteratingState(Slice slice) {
    return new PartitionState(StateType.ITERATING, slice);
  }

  public enum StateType {
    INIT,
    NEW_PARTITION,
    NEED_MORE_DATA,
    ITERATING,
    FINISHED,
  }

  private final StateType stateType;
  // Nullable
  private final Slice slice;

  protected PartitionState(StateType stateType, Slice slice) {
    this.stateType = stateType;
    this.slice = slice;
  }

  public StateType getStateType() {
    return stateType;
  }

  // Nullable
  public Slice getSlice() {
    return slice;
  }
}
