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
package org.apache.iotdb.commons.path.dfa.graph;

import org.apache.iotdb.commons.path.dfa.DFAState;
import org.apache.iotdb.commons.path.dfa.IDFAState;
import org.apache.iotdb.commons.path.dfa.IDFATransition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class DFAGraph {
  private final List<IDFAState> dfaStateList = new ArrayList<>();
  private final Map<IDFATransition, List<IDFAState>> dfaTransitionTable = new HashMap<>();

  public DFAGraph(NFAGraph nfaGraph, Map<String, IDFATransition> transitionMap) {
    // init start state
    int index = 0;
    // Map NFAStateClosure to DFASate
    Map<Closure, DFAState> closureStateMap = new HashMap<>();
    for (IDFATransition transition : transitionMap.values()) {
      dfaTransitionTable.put(transition, new ArrayList<>());
      dfaTransitionTable.get(transition).add(null);
    }
    DFAState curState = new DFAState(index);
    dfaStateList.add(curState);
    Closure curClosure = new Closure(new HashSet<>(Collections.singletonList(curState)));
    closureStateMap.put(curClosure, curState);
    Stack<Closure> closureStack = new Stack<>();
    closureStack.push(curClosure);

    // construct DFA
    while (!closureStack.isEmpty()) {
      curClosure = closureStack.pop();
      for (IDFATransition transition : transitionMap.values()) {
        Closure nextClosure = getNextClosure(nfaGraph, curClosure, transition);
        if (!nextClosure.getStateSet().isEmpty()) {
          if (closureStateMap.containsKey(nextClosure)) {
            // closure already exist
            dfaTransitionTable
                .get(transition)
                .set(closureStateMap.get(curClosure).getIndex(), closureStateMap.get(nextClosure));
          } else {
            // new closure
            DFAState newState = new DFAState(++index, nextClosure.isFinal());
            dfaStateList.add(newState);
            closureStateMap.put(nextClosure, newState);
            for (List<IDFAState> column : dfaTransitionTable.values()) {
              column.add(null);
            }
            dfaTransitionTable
                .get(transition)
                .set(closureStateMap.get(curClosure).getIndex(), newState);
            closureStack.push(nextClosure);
          }
        }
      }
    }
  }

  public void print(Map<String, IDFATransition> transitionMap) {
    // print
    System.out.println();
    System.out.println();
    System.out.println("DFA:");
    System.out.println(
        "==================================================================================================");
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(String.format("|%-15s|", "State"));
    for (IDFATransition transfer : transitionMap.values()) {
      stringBuilder.append(String.format("%-15s|", transfer.toString()));
    }
    stringBuilder.append(String.format("%-15s|", "Final"));
    System.out.println(stringBuilder);
    for (int i = 0; i < dfaStateList.size(); i++) {
      stringBuilder = new StringBuilder();
      stringBuilder.append(String.format("|%-15d|", i));
      for (IDFATransition transition : transitionMap.values()) {
        IDFAState tmp = dfaTransitionTable.get(transition).get(i);
        stringBuilder.append(String.format("%-15s|", tmp == null ? "" : tmp.getIndex()));
      }
      stringBuilder.append(String.format("%-15s|", dfaStateList.get(i).isFinal()));
      System.out.println(stringBuilder);
    }
  }

  private Closure getNextClosure(NFAGraph nfaGraph, Closure curClosure, IDFATransition transfer) {
    Set<IDFAState> nextStateSet = new HashSet<>();
    List<List<IDFAState>> transferColumn = nfaGraph.getTransitionColumn(transfer);
    for (IDFAState state : curClosure.getStateSet()) {
      nextStateSet.addAll(transferColumn.get(state.getIndex()));
    }
    return new Closure(nextStateSet);
  }

  public List<IDFATransition> getTransition(
      IDFAState state, Map<String, IDFATransition> transitionMap) {
    List<IDFATransition> res = new ArrayList<>();
    for (IDFATransition transition : transitionMap.values()) {
      if (dfaTransitionTable.get(transition).get(state.getIndex()) != null) {
        res.add(transition);
      }
    }
    return res;
  }

  public IDFAState getNextState(IDFAState currentState, IDFATransition transition) {
    return dfaTransitionTable.get(transition).get(currentState.getIndex());
  }
}
