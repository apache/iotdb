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
package org.apache.iotdb.commons.path.fa.dfa;

import org.apache.iotdb.commons.path.fa.IFAState;

import java.util.Objects;

public class DFAState implements IFAState {
  private final int index;
  private final boolean isFinal;

  public DFAState(int state) {
    this.index = state;
    this.isFinal = false;
  }

  public DFAState(int state, boolean isFinal) {
    this.index = state;
    this.isFinal = isFinal;
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public boolean isInitial() {
    return index == 0;
  }

  @Override
  public boolean isFinal() {
    return isFinal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DFAState state = (DFAState) o;
    return index == state.index && isFinal == state.isFinal;
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, isFinal);
  }
}
