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

import org.apache.iotdb.udf.api.relational.access.Record;

import java.util.Iterator;

public class PartitionState {

  public static final PartitionState NEED_MORE_DATA_STATE =
      new PartitionState(StateType.NEED_MORE_DATA, null);
  public static final PartitionState FINISHED_STATE = new PartitionState(StateType.FINISHED, null);
  public static final PartitionState INIT_STATE = new PartitionState(StateType.INIT, null);

  public static PartitionState newPartitionState(Iterator<Record> recordIterator) {
    return new PartitionState(StateType.NEW_PARTITION, recordIterator);
  }

  public static PartitionState iteratingState(Iterator<Record> recordIterator) {
    return new PartitionState(StateType.ITERATING, recordIterator);
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
  private final Iterator<Record> recordIterator;

  protected PartitionState(StateType stateType, Iterator<Record> recordIterator) {
    this.stateType = stateType;
    this.recordIterator = recordIterator;
  }

  public StateType getStateType() {
    return stateType;
  }

  // Nullable
  public Iterator<Record> getRecordIterator() {
    return recordIterator;
  }
}
