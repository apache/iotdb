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

package org.apache.iotdb.commons.path.fa.match;

import org.apache.iotdb.commons.path.fa.IFAState;

/** This class is used for recording the states, matched by one item, in order. */
public class MatchedStateSet {
  private static final int INITIAL_SIZE = 8;

  /**
   * Status of all states, identified by index in target FA. stateStatus[k] == true means state with
   * index equals k is already in this set.
   */
  private final boolean[] stateStatus;

  /** The existing state in this state, stored in the putting order. */
  private int[] existingState = new int[INITIAL_SIZE];

  private int end = 0;

  /**
   * Construct an empty set with given capacity. This set stores states no more than given cpacity.
   *
   * @param capacity generally, we use stateSize as capacity
   */
  MatchedStateSet(int capacity) {
    stateStatus = new boolean[capacity];
  }

  void add(IFAState state) {
    if (stateStatus[state.getIndex()]) {
      return;
    }
    if (end == existingState.length) {
      int[] array = new int[existingState.length * 2];
      System.arraycopy(existingState, 0, array, 0, end);
      existingState = array;
    }
    existingState[end++] = state.getIndex();
    stateStatus[state.getIndex()] = true;
  }

  int getStateIndex(int stateOrdinal) {
    return existingState[stateOrdinal];
  }

  int size() {
    return end;
  }
}
