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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array;

import org.apache.iotdb.udf.api.State;

import org.apache.tsfile.block.column.Column;

import java.util.function.Supplier;

public class StateBigArray {

  private final ObjectBigArray<State> array;
  private final Supplier<State> stateSupplier;

  public StateBigArray(Supplier<State> stateSupplier) {
    this.array = new ObjectBigArray<>();
    this.stateSupplier = stateSupplier;
  }

  public State get(long index) {
    return array.get(index);
  }

  public void set(long index, State value) {
    array.set(index, value);
  }

  public void update(long index, Column[] arguments) {
    State state = array.get(index);
    if (state == null) {
      state = stateSupplier.get();
      array.set(index, state);
    }
  }

  public void ensureCapacity(long length) {
    array.ensureCapacity(length);
  }

  public void reset() {
    array.reset();
  }
}
