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
package org.apache.iotdb.commons.path.fa.dfa.graph;

import org.apache.iotdb.commons.path.fa.IFAState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Closure {
  private final byte[] bitmap;
  private final List<IFAState> states = new ArrayList<>();

  public Closure(int capacity) {
    this.bitmap = new byte[capacity];
  }

  public void addState(IFAState state) {
    if (bitmap[state.getIndex()] == 0) {
      bitmap[state.getIndex()] = 1;
      states.add(state);
    }
  }

  public List<IFAState> getStates() {
    return states;
  }

  public boolean isFinal() {
    boolean res = false;
    for (IFAState state : states) {
      res |= state.isFinal();
    }
    return res;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Closure closure = (Closure) o;
    return Arrays.equals(bitmap, closure.bitmap);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bitmap);
  }
}
