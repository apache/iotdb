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
import org.apache.iotdb.commons.path.fa.IFATransition;
import org.apache.iotdb.commons.path.fa.dfa.DFAState;
import org.apache.iotdb.commons.utils.TestOnly;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * DFA graph for given path pattern. State 0 is initial state. Run PatternDFATest#printFASketch for
 * more detail.
 */
public class DFAGraph {
  private final List<IFAState> dfaStateList = new ArrayList<>();
  private final List<IFAState>[] dfaTransitionTable;

  public DFAGraph(NFAGraph nfaGraph, Collection<IFATransition> transitions) {
    dfaTransitionTable = new List[transitions.size()];
    int closureSize = nfaGraph.getStateSize();
    // init start state
    int index = 0;
    // Map NFAStateClosure to DFASate
    Map<Closure, DFAState> closureStateMap = new HashMap<>();
    for (IFATransition transition : transitions) {
      dfaTransitionTable[transition.getIndex()] = new ArrayList<>();
      dfaTransitionTable[transition.getIndex()].add(null);
    }
    DFAState curState = new DFAState(index);
    dfaStateList.add(curState);
    Closure curClosure = new Closure(closureSize);
    curClosure.addState(curState);
    closureStateMap.put(curClosure, curState);
    Stack<Closure> closureStack = new Stack<>();
    closureStack.push(curClosure);

    // construct DFA using subset construction
    while (!closureStack.isEmpty()) {
      curClosure = closureStack.pop();
      for (IFATransition transition : transitions) {
        Closure nextClosure = new Closure(closureSize);
        boolean isEmpty = getNextClosure(nfaGraph, curClosure, nextClosure, transition);
        if (!isEmpty) {
          if (closureStateMap.containsKey(nextClosure)) {
            // closure already exist
            dfaTransitionTable[transition.getIndex()].set(
                closureStateMap.get(curClosure).getIndex(), closureStateMap.get(nextClosure));
          } else {
            // new closure
            DFAState newState = new DFAState(++index, nextClosure.isFinal());
            dfaStateList.add(newState);
            closureStateMap.put(nextClosure, newState);
            for (List<IFAState> column : dfaTransitionTable) {
              column.add(null);
            }
            dfaTransitionTable[transition.getIndex()].set(
                closureStateMap.get(curClosure).getIndex(), newState);
            closureStack.push(nextClosure);
          }
        }
      }
    }
  }

  @TestOnly
  public void print(Map<String, IFATransition> transitionMap) {
    System.out.println();
    System.out.println();
    System.out.println("DFA:");
    System.out.println(
        "==================================================================================================");
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(String.format("|%-15s|", "State"));
    for (IFATransition transfer : transitionMap.values()) {
      stringBuilder.append(String.format("%-15s|", transfer.toString()));
    }
    stringBuilder.append(String.format("%-15s|", "Final"));
    System.out.println(stringBuilder);
    for (int i = 0; i < dfaStateList.size(); i++) {
      stringBuilder = new StringBuilder();
      stringBuilder.append(String.format("|%-15d|", i));
      for (IFATransition transition : transitionMap.values()) {
        IFAState tmp = dfaTransitionTable[transition.getIndex()].get(i);
        stringBuilder.append(String.format("%-15s|", tmp == null ? "" : tmp.getIndex()));
      }
      stringBuilder.append(String.format("%-15s|", dfaStateList.get(i).isFinal()));
      System.out.println(stringBuilder);
    }
  }

  /**
   * get next closure
   *
   * @param nfaGraph
   * @param curClosure
   * @param transfer
   * @return if empty. return true
   */
  private boolean getNextClosure(
      NFAGraph nfaGraph, Closure curClosure, Closure nextClosure, IFATransition transfer) {
    boolean isEmpty = true;
    for (IFAState state : curClosure.getStates()) {
      if (state != null) {
        for (IFAState nextState : nfaGraph.getTransitions(transfer, state)) {
          nextClosure.addState(nextState);
          isEmpty = false;
        }
      }
    }
    return isEmpty;
  }

  public Map<String, IFATransition> getPreciseMatchTransition(
      IFAState state, Collection<IFATransition> transitions) {
    Map<String, IFATransition> res = new HashMap<>();
    for (IFATransition transition : transitions) {
      if (dfaTransitionTable[transition.getIndex()].get(state.getIndex()) != null) {
        res.put(transition.getAcceptEvent(), transition);
      }
    }
    return res;
  }

  public List<IFATransition> getTransition(
      IFAState state, Collection<IFATransition> transitionList) {
    List<IFATransition> res = new ArrayList<>();
    for (IFATransition transition : transitionList) {
      if (dfaTransitionTable[transition.getIndex()].get(state.getIndex()) != null) {
        res.add(transition);
      }
    }
    return res;
  }

  public IFAState getNextState(IFAState currentState, IFATransition transition) {
    return dfaTransitionTable[transition.getIndex()].get(currentState.getIndex());
  }

  public IFAState getInitialState() {
    return dfaStateList.get(0);
  }

  public int getStateSize() {
    return dfaStateList.size();
  }

  public IFAState getState(int index) {
    return dfaStateList.get(index);
  }
}
