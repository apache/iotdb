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

import org.apache.iotdb.commons.path.PathPatternNode;
import org.apache.iotdb.commons.path.PathPatternTree;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DFA graph for given path pattern. State 0 is initial state. Run PatternDFATest#printFASketch for
 * more detail.
 */
public class DFAGraph {
  private final List<IFAState> dfaStateList = new ArrayList<>();

  // [stateIndex][transitionIndex]
  private final Map<Integer, Map<Integer, IFAState>> dfaTransitionTable = new HashMap<>();

  public DFAGraph(NFAGraph nfaGraph, Collection<IFATransition> transitions) {
    int closureSize = nfaGraph.getStateSize();
    // init start state
    int index = 0;
    // Map NFAStateClosure to DFASate
    Map<Closure, DFAState> closureStateMap = new HashMap<>();
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
            updateDfaTransitionTable(
                closureStateMap.get(curClosure).getIndex(),
                transition.getIndex(),
                closureStateMap.get(nextClosure));
          } else {
            // new closure
            DFAState newState = new DFAState(++index, nextClosure.isFinal());
            dfaStateList.add(newState);
            closureStateMap.put(nextClosure, newState);
            updateDfaTransitionTable(
                closureStateMap.get(curClosure).getIndex(), transition.getIndex(), newState);
            closureStack.push(nextClosure);
          }
        }
      }
    }
  }

  public DFAGraph(PathPatternTree patternTree, Map<String, IFATransition> transitionMap) {
    // init start state
    IFAState curState = new DFAState(0);
    dfaStateList.add(curState);
    init(patternTree.getRoot(), transitionMap, curState, new AtomicInteger(0));
  }

  private void init(
      PathPatternNode<Void, PathPatternNode.VoidSerializer> node,
      Map<String, IFATransition> transitionMap,
      IFAState curState,
      AtomicInteger stateIndexGenerator) {
    IFATransition transition = transitionMap.get(node.getName());
    int stateIndex = curState.getIndex();
    int transitionIndex = transition.getIndex();
    DFAState newState = (DFAState) getFromDfaTransitionTable(stateIndex, transitionIndex);
    if (newState == null) {
      newState = new DFAState(stateIndexGenerator.incrementAndGet());
      dfaStateList.add(newState);
      updateDfaTransitionTable(stateIndex, transitionIndex, newState);
    }
    if (node.isPathPattern()) {
      newState.setFinal(true);
    }
    for (PathPatternNode<Void, PathPatternNode.VoidSerializer> child :
        node.getChildren().values()) {
      init(child, transitionMap, newState, stateIndexGenerator);
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
        IFAState tmp = getFromDfaTransitionTable(i, transition.getIndex());
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
      if (getFromDfaTransitionTable(state.getIndex(), transition.getIndex()) != null) {
        res.put(transition.getAcceptEvent(), transition);
      }
    }
    return res;
  }

  public List<IFATransition> getTransition(
      IFAState state, Collection<IFATransition> transitionList) {
    List<IFATransition> res = new ArrayList<>();
    for (IFATransition transition : transitionList) {
      if (getFromDfaTransitionTable(state.getIndex(), transition.getIndex()) != null) {
        res.add(transition);
      }
    }
    return res;
  }

  public IFAState getNextState(IFAState currentState, IFATransition transition) {
    return getFromDfaTransitionTable(currentState.getIndex(), transition.getIndex());
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

  private void updateDfaTransitionTable(int stateIndex, int transitionIndex, IFAState newState) {
    dfaTransitionTable.compute(
        stateIndex,
        (k, v) -> {
          if (v == null) {
            v = new HashMap<>();
          }
          v.put(transitionIndex, newState);
          return v;
        });
  }

  private IFAState getFromDfaTransitionTable(int stateIndex, int transitionIndex) {
    if (dfaTransitionTable.containsKey(stateIndex)) {
      return dfaTransitionTable.get(stateIndex).get(transitionIndex);
    } else {
      return null;
    }
  }
}
