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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.fa.IFAState;
import org.apache.iotdb.commons.path.fa.IFATransition;
import org.apache.iotdb.commons.path.fa.IPatternFA;
import org.apache.iotdb.commons.path.fa.dfa.graph.DFAGraph;
import org.apache.iotdb.commons.path.fa.dfa.graph.NFAGraph;
import org.apache.iotdb.commons.path.fa.dfa.transition.DFAPreciseTransition;
import org.apache.iotdb.commons.path.fa.dfa.transition.DFAWildcardTransition;
import org.apache.iotdb.commons.utils.TestOnly;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PatternDFA implements IPatternFA {

  private final List<IFATransition> preciseMatchTransitionList = new ArrayList<>();
  private final List<IFATransition> batchMatchTransitionList = new ArrayList<>();
  // Map<AcceptEvent, IFATransition>
  private final Map<String, IFATransition> transitionMap = new HashMap<>();
  private final DFAGraph dfaGraph;

  // cached
  private final Map<String, IFATransition>[] preciseMatchTransitionCached;
  private final List<IFATransition>[] batchMatchTransitionCached;

  public PatternDFA(PartialPath pathPattern, boolean isPrefix) {
    // 1. build transition
    boolean wildcard = false;
    AtomicInteger transitionIndex = new AtomicInteger();
    for (String node : pathPattern.getNodes()) {
      if (IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(node)
          || IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD.equals(node)) {
        wildcard = true;
      } else {
        transitionMap.computeIfAbsent(
            node,
            i -> {
              IFATransition transition =
                  new DFAPreciseTransition(transitionIndex.getAndIncrement(), node);
              preciseMatchTransitionList.add(transition);
              return transition;
            });
      }
    }
    if (wildcard || isPrefix) {
      IFATransition transition =
          new DFAWildcardTransition(
              transitionIndex.getAndIncrement(), new ArrayList<>(transitionMap.keySet()));
      transitionMap.put(transition.getAcceptEvent(), transition);
      batchMatchTransitionList.add(transition);
    }

    // 2. build NFA
    NFAGraph nfaGraph = new NFAGraph(pathPattern, isPrefix, transitionMap);

    // 3. NFA to DFA
    dfaGraph = new DFAGraph(nfaGraph, transitionMap.values());
    preciseMatchTransitionCached = new HashMap[dfaGraph.getStateSize()];
    batchMatchTransitionCached = new List[dfaGraph.getStateSize()];
  }

  @Override
  public Map<String, IFATransition> getPreciseMatchTransition(IFAState state) {
    if (preciseMatchTransitionCached[state.getIndex()] == null) {
      preciseMatchTransitionCached[state.getIndex()] =
          dfaGraph.getPreciseMatchTransition(state, preciseMatchTransitionList);
    }
    return preciseMatchTransitionCached[state.getIndex()];
  }

  @Override
  public Iterator<IFATransition> getPreciseMatchTransitionIterator(IFAState state) {
    if (preciseMatchTransitionCached[state.getIndex()] == null) {
      preciseMatchTransitionCached[state.getIndex()] =
          dfaGraph.getPreciseMatchTransition(state, preciseMatchTransitionList);
    }
    return preciseMatchTransitionCached[state.getIndex()].values().iterator();
  }

  @Override
  public Iterator<IFATransition> getFuzzyMatchTransitionIterator(IFAState state) {
    if (batchMatchTransitionCached[state.getIndex()] == null) {
      batchMatchTransitionCached[state.getIndex()] =
          dfaGraph.getTransition(state, batchMatchTransitionList);
    }
    return batchMatchTransitionCached[state.getIndex()].iterator();
  }

  @Override
  public int getFuzzyMatchTransitionSize(IFAState state) {
    if (batchMatchTransitionCached[state.getIndex()] == null) {
      batchMatchTransitionCached[state.getIndex()] =
          dfaGraph.getTransition(state, batchMatchTransitionList);
    }
    return batchMatchTransitionCached[state.getIndex()].size();
  }

  @Override
  public IFAState getNextState(IFAState currentState, IFATransition transition) {
    return dfaGraph.getNextState(currentState, transition);
  }

  @Override
  public IFAState getInitialState() {
    return dfaGraph.getInitialState();
  }

  @Override
  public int getStateSize() {
    return dfaGraph.getStateSize();
  }

  @Override
  public IFAState getState(int index) {
    return dfaGraph.getState(index);
  }

  @Override
  public boolean mayTransitionOverlap() {
    return false;
  }

  @TestOnly
  public List<IFATransition> getTransition(IFAState state) {
    return dfaGraph.getTransition(state, transitionMap.values());
  }
}
