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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class PatternDFA {

  Map<String, Transfer> transferMap = new HashMap<>();

  private PatternDFA(Builder builder) {
    System.out.println(builder.pathPattern);

    // 1. build transfer
    boolean wildcard = false;
    for (String node : builder.pathPattern.getNodes()) {
      if (IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(node)
          || IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD.equals(node)) {
        wildcard = true;
      } else {
        if (!transferMap.containsKey(node)) {
          transferMap.put(node, new Transfer(node));
        }
      }
    }
    if (wildcard) {
      transferMap.put(
          IoTDBConstant.ONE_LEVEL_PATH_WILDCARD,
          new Transfer(
              IoTDBConstant.ONE_LEVEL_PATH_WILDCARD, new ArrayList<>(transferMap.keySet())));
    }

    // 2. build NFA
    int index = 0;
    List<State> stateList = new ArrayList<>();
    Map<String, List<List<State>>> nfaColumn = new HashMap<>();
    // init start state
    stateList.add(new State(index));
    for (String transfer : transferMap.keySet()) {
      nfaColumn.put(transfer, new ArrayList<>());
      nfaColumn.get(transfer).add(new ArrayList<>());
    }
    for (int i = 0; i < builder.pathPattern.getNodeLength(); i++) {
      String node = builder.pathPattern.getNodes()[i];
      int preIndex = index;
      State state =
          i == builder.pathPattern.getNodeLength() - 1
              ? new State(++index, true)
              : new State(++index);
      stateList.add(state);
      for (String transfer : transferMap.keySet()) {
        nfaColumn.get(transfer).add(new ArrayList<>());
      }
      if (IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(node)) {
        for (String transfer : transferMap.keySet()) {
          nfaColumn.get(transfer).get(preIndex).add(state);
        }
      } else if (IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD.equals(node)) {
        for (String transfer : transferMap.keySet()) {
          nfaColumn.get(transfer).get(preIndex).add(state);
          nfaColumn.get(transfer).get(index).add(state);
        }
      } else {
        nfaColumn.get(node).get(preIndex).add(state);
      }
    }
    System.out.println();
    System.out.println();
    System.out.println("NFA:");
    System.out.println(
        "==================================================================================================");
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(String.format("|%-15s|", "State"));
    for (Transfer transfer : transferMap.values()) {
      stringBuilder.append(String.format("%-15s|", transfer.toString()));
    }
    stringBuilder.append(String.format("%-15s|", "Final"));
    System.out.println(stringBuilder);
    for (int i = 0; i <= index; i++) {
      stringBuilder = new StringBuilder();
      stringBuilder.append(String.format("|%-15d|", i));
      for (String transfer : transferMap.keySet()) {
        stringBuilder.append(
            String.format(
                "%-15s|",
                StringUtils.join(
                    nfaColumn.get(transfer).get(i).stream()
                        .map(State::getIndex)
                        .collect(Collectors.toList()),
                    ",")));
      }
      stringBuilder.append(String.format("%-15s|", stateList.get(i).isFinal()));
      System.out.println(stringBuilder);
    }

    // NFA to DFA
    toDFA(stateList, nfaColumn);
  }

  void toDFA(List<State> nfaStateList, Map<String, List<List<State>>> nfaColumn) {
    int index = 0;
    List<State> stateList = new ArrayList<>();
    Map<String, List<State>> dfaColumn = new HashMap<>();
    Map<Closure, State> closureStateMap = new HashMap<>();
    // init start state
    for (String transfer : transferMap.keySet()) {
      dfaColumn.put(transfer, new ArrayList<>());
      dfaColumn.get(transfer).add(null);
    }
    State curState = new State(index);
    stateList.add(curState);
    Closure curClosure = new Closure(new HashSet<>(Collections.singletonList(curState)));
    closureStateMap.put(curClosure, curState);
    Stack<Closure> closureStack = new Stack<>();
    closureStack.push(curClosure);
    while (!closureStack.isEmpty()) {
      curClosure = closureStack.pop();
      for (Transfer transfer : transferMap.values()) {
        Closure nextClosure = getNextClosure(nfaColumn, curClosure, transfer);
        if (!nextClosure.getStateSet().isEmpty()) {
          if (closureStateMap.containsKey(nextClosure)) { // already exist
            dfaColumn
                .get(transfer.getKey())
                .set(closureStateMap.get(curClosure).getIndex(), closureStateMap.get(nextClosure));
          } else { // new closure
            State newState = new State(++index, nextClosure.isFinal());
            stateList.add(newState);
            closureStateMap.put(nextClosure, newState);
            for (List<State> column : dfaColumn.values()) {
              column.add(null);
            }
            dfaColumn
                .get(transfer.getKey())
                .set(closureStateMap.get(curClosure).getIndex(), newState);
            closureStack.push(nextClosure);
          }
        }
      }
    }

    // print
    System.out.println();
    System.out.println();
    System.out.println("DFA:");
    System.out.println(
        "==================================================================================================");
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(String.format("|%-15s|", "State"));
    for (Transfer transfer : transferMap.values()) {
      stringBuilder.append(String.format("%-15s|", transfer.toString()));
    }
    stringBuilder.append(String.format("%-15s|", "Final"));
    System.out.println(stringBuilder);
    for (int i = 0; i <= index; i++) {
      stringBuilder = new StringBuilder();
      stringBuilder.append(String.format("|%-15d|", i));
      for (String transfer : transferMap.keySet()) {
        State tmp = dfaColumn.get(transfer).get(i);
        stringBuilder.append(String.format("%-15s|", tmp == null ? "" : tmp.getIndex()));
      }
      stringBuilder.append(String.format("%-15s|", stateList.get(i).isFinal()));
      System.out.println(stringBuilder);
    }
  }

  private Closure getNextClosure(
      Map<String, List<List<State>>> nfaColumn, Closure curClosure, Transfer transfer) {
    Set<State> nextStateSet = new HashSet<>();
    List<List<State>> transferColumn = nfaColumn.get(transfer.getKey());
    for (State state : curClosure.getStateSet()) {
      nextStateSet.addAll(transferColumn.get(state.getIndex()));
    }
    return new Closure(nextStateSet);
  }

  public static void main(String[] args) throws IllegalPathException {
    PatternDFA patternDFA = new Builder().pattern(new PartialPath("root.sg.**.b.*")).build();
  }

  public static final class Builder {
    private PartialPath pathPattern;

    private Builder() {}

    public Builder pattern(PartialPath pattern) {
      this.pathPattern = pattern;
      return this;
    }

    public PatternDFA build() {
      return new PatternDFA(this);
    }
  }
}
