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
package org.apache.iotdb.commons.path.dfa;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.dfa.graph.DFAGraph;
import org.apache.iotdb.commons.path.dfa.graph.NFAGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PatternDFA implements IPatternFA {

  // TODO: maybe transitionMap can be represented as List
  private final Map<String, IFATransition> transitionMap = new HashMap<>();
  private final DFAGraph dfaGraph;

  private PatternDFA(Builder builder) {
    System.out.println(builder.pathPattern);

    // 1. build transition
    boolean wildcard = false;
    for (String node : builder.pathPattern.getNodes()) {
      if (IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(node)
          || IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD.equals(node)) {
        wildcard = true;
      } else {
        DFATransition transition = new DFATransition(node);
        transitionMap.put(transition.getAcceptEvent(), transition);
      }
    }
    if (wildcard) {
      DFATransition transition =
          new DFATransition(
              IoTDBConstant.ONE_LEVEL_PATH_WILDCARD, new ArrayList<>(transitionMap.keySet()));
      transitionMap.put(transition.getAcceptEvent(), transition);
    }

    // 2. build NFA
    NFAGraph nfaGraph = new NFAGraph(builder.pathPattern, transitionMap);
    nfaGraph.print(transitionMap);

    // 3. NFA to DFA
    dfaGraph = new DFAGraph(nfaGraph, transitionMap);
    dfaGraph.print(transitionMap);
  }

  @Override
  public List<IFATransition> getTransition(IFAState state) {
    return dfaGraph.getTransition(state, transitionMap);
  }

  @Override
  public Map<String, IFATransition> getPreciseMatchTransition(IFAState state) {
    // todo
    return null;
  }

  @Override
  public List<IFATransition> getBatchMatchTransition(IFAState state) {
    // todo
    return null;
  }

  @Override
  public IFAState getNextState(IFAState currentState, IFATransition transition) {
    return dfaGraph.getNextState(currentState, transition);
  }

  @Override
  public IFAState getInitialState() {
    return dfaGraph.getInitialState();
  }

  public static final class Builder {
    private PartialPath pathPattern;

    public Builder() {}

    public Builder pattern(PartialPath pattern) {
      this.pathPattern = pattern;
      return this;
    }

    public PatternDFA build() {
      return new PatternDFA(this);
    }
  }

  public static void main(String[] args) throws IllegalPathException {
    PatternDFA patternDFA = new Builder().pattern(new PartialPath("root.sg.**.b.*")).build();
  }
}
