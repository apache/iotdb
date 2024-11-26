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

package org.apache.tsfile.common.regexp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DFA {
  private final int start;
  private final ArrayList<Integer> acceptStates;
  private final List<List<Transition>> transitions;

  public DFA(int start, ArrayList<Integer> acceptStates, List<List<Transition>> transitions) {
    this.start = start;
    this.acceptStates = Objects.requireNonNull(acceptStates, "acceptStates is null");
    this.transitions = Collections.unmodifiableList(new ArrayList<>(transitions));
  }

  public List<List<Transition>> getTransitions() {
    return transitions;
  }

  public ArrayList<Integer> getAcceptStates() {
    return acceptStates;
  }

  public int getStart() {
    return start;
  }

  public static class Transition {
    private final int value;
    private final int target;

    public Transition(int value, int target) {
      this.value = value;
      this.target = target;
    }

    public int getTarget() {
      return target;
    }

    public int getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.format("-[%s]-> %s", value, target);
    }
  }

  public static class Builder {
    private int nextId;
    private int start;
    private final ArrayList<Integer> acceptStates = new ArrayList<>();
    private final List<List<Transition>> transitions = new ArrayList<>();

    public int addState(boolean accept) {
      int state = nextId++;
      transitions.add(new ArrayList<>());
      if (accept) {
        acceptStates.add(state);
      }
      return state;
    }

    public int addStartState(boolean accept) {
      start = addState(accept);
      return start;
    }

    public void addTransition(int from, int value, int to) {
      transitions.get(from).add(new Transition(value, to));
    }

    public DFA build() {
      return new DFA(start, acceptStates, transitions);
    }
  }
}
