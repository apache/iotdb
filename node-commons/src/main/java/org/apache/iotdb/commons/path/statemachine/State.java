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
package org.apache.iotdb.commons.path.statemachine;

import java.util.Objects;

public class State {
  private final int index;
  private final boolean isFinal;

  public State(int state) {
    this.index = state;
    this.isFinal = false;
  }

  public State(int state, boolean isFinal) {
    this.index = state;
    this.isFinal = isFinal;
  }

  public int getIndex() {
    return index;
  }

  public boolean isFinal() {
    return isFinal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    State state = (State) o;
    return index == state.index && isFinal == state.isFinal;
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, isFinal);
  }
}
