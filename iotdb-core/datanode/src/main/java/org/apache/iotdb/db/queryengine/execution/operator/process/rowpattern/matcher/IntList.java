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

package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Arrays;

public class IntList {
  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(IntList.class);

  private int[] values;
  private int next;

  public IntList(int capacity) {
    this.values = new int[capacity];
  }

  private IntList(int[] values, int next) {
    this.values = values;
    this.next = next;
  }

  public void add(int value) {
    ensureCapacity(next);
    values[next] = value;
    next++;
  }

  public int get(int index) {
    return values[index];
  }

  public void set(int index, int value) {
    ensureCapacity(index);
    values[index] = value;
    next = Math.max(next, index + 1);
  }

  public int size() {
    return next;
  }

  public void clear() {
    next = 0;
  }

  public IntList copy() {
    return new IntList(values.clone(), next);
  }

  public ArrayView toArrayView() {
    return new ArrayView(values, next);
  }

  private void ensureCapacity(int index) {
    if (index >= values.length) {
      values = Arrays.copyOf(values, Math.max(values.length * 2, index + 1));
    }
  }

  public long getSizeInBytes() {
    return INSTANCE_SIZE + RamUsageEstimator.sizeOf(values);
  }
}
