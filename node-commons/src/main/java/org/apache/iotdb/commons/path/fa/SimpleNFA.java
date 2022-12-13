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

package org.apache.iotdb.commons.path.fa;

import org.apache.iotdb.commons.path.PartialPath;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

/**
 * Given path pattern root.sg.*.s, the SimpleNFA is:
 *
 * <p>initial -(root)-> state[0] -(sg)-> state[1] -(*)-> state[2] -(s)-> state[3] <br>
 * state[3] is final state
 *
 * <p>Given path pattern root.**.s, the SimpleNFA is:
 *
 * <p>initial -(root)-> state[0] -(*)-> state[1] -(s)-> state[2] <br>
 * with extra: state[1] -(*)-> state[1] and state[2] -(*)-> state[1] <br>
 * state[3] is final state
 *
 * <p>Given path pattern root.sg.d with prefix match, the SimpleNFA is:
 *
 * <p>initial -(root)-> state[0] -(sg)-> state[1] -(d)-> state[2] -(*)-> state[3] <br>
 * with extra: state[3] -(*)-> state[3] <br>
 * both state[2] and state[3] are final states
 */
public class SimpleNFA implements IPatternFA {

  // raw nodes of pathPattern
  private final String[] nodes;

  // used for pattern node like d*
  private final Map<Integer, Pattern> regexPatternMap = new HashMap<>();

  // initial state of this NFA and the only transition from this state is "root"
  private final SimpleNFAState initialState = new InitialState();
  // all states corresponding to raw pattern nodes, with an extra prefixMatch state
  private final SimpleNFAState[] states;

  // singletonMap, "root"
  private final Map<String, IFATransition> initialTransition;
  // the transition matches only specific value
  // state[i] -> Map<preciseMatchTransition.value, preciseMatchTransition>
  // the next states from state[i] by preciseMatchTransition will only be state[i + 1]
  private final Map<String, IFATransition>[] preciseMatchTransitionTable;
  // the fuzzy transition matches multi values, like ** or * can match any value
  // state[i] -> List<FuzzyMatchTransition>
  // the next states from state[i] by fuzzyMatchTransition may be state[i + 1] or
  // state[lastIndexOf("**")]
  private final List<IFATransition>[] fuzzyMatchTransitionTable;

  public SimpleNFA(PartialPath pathPattern, boolean isPrefixMatch) {
    this.nodes = pathPattern.getNodes();

    states = new SimpleNFAState[this.nodes.length + 1];
    preciseMatchTransitionTable = new Map[states.length];
    fuzzyMatchTransitionTable = new List[states.length];

    // nodes[0] must be root
    states[0] = new NameMatchState(0);
    initialTransition = Collections.singletonMap(nodes[0], states[0]);
    int lastMultiWildcard = -1;
    for (int i = 1; i < nodes.length; i++) {
      if (nodes[i].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        lastMultiWildcard = i;
        states[i] = new AllMatchState(i);
        fuzzyMatchTransitionTable[i - 1] = Collections.singletonList(states[i]);
        preciseMatchTransitionTable[i - 1] = Collections.emptyMap();
      } else if (nodes[i].equals(ONE_LEVEL_PATH_WILDCARD)) {
        states[i] = new AllMatchState(i);
        if (lastMultiWildcard == -1) {
          fuzzyMatchTransitionTable[i - 1] = Collections.singletonList(states[i]);
        } else {
          fuzzyMatchTransitionTable[i - 1] = Arrays.asList(states[i], states[lastMultiWildcard]);
        }
        preciseMatchTransitionTable[i - 1] = Collections.emptyMap();
      } else if (nodes[i].contains(ONE_LEVEL_PATH_WILDCARD)) {
        states[i] = new RegexMatchState(i);
        regexPatternMap.put(i, Pattern.compile(nodes[i].replace("*", ".*")));
        if (lastMultiWildcard == -1) {
          fuzzyMatchTransitionTable[i - 1] = Collections.singletonList(states[i]);
        } else {
          fuzzyMatchTransitionTable[i - 1] = Arrays.asList(states[i], states[lastMultiWildcard]);
        }
        preciseMatchTransitionTable[i - 1] = Collections.emptyMap();
      } else {
        states[i] = new NameMatchState(i);
        if (lastMultiWildcard == -1) {
          fuzzyMatchTransitionTable[i - 1] = Collections.emptyList();
        } else {
          fuzzyMatchTransitionTable[i - 1] = Collections.singletonList(states[lastMultiWildcard]);
        }
        preciseMatchTransitionTable[i - 1] = Collections.singletonMap(nodes[i], states[i]);
      }
    }

    preciseMatchTransitionTable[nodes.length - 1] = Collections.emptyMap();
    if (isPrefixMatch) {
      states[nodes.length] = new AllMatchState(nodes.length);
      fuzzyMatchTransitionTable[nodes.length - 1] = Collections.singletonList(states[nodes.length]);
      preciseMatchTransitionTable[nodes.length] = Collections.emptyMap();
      fuzzyMatchTransitionTable[nodes.length] = Collections.singletonList(states[nodes.length]);
    } else {
      if (lastMultiWildcard == -1) {
        fuzzyMatchTransitionTable[nodes.length - 1] = Collections.emptyList();
      } else {
        fuzzyMatchTransitionTable[nodes.length - 1] =
            Collections.singletonList(states[lastMultiWildcard]);
      }
    }
  }

  @Override
  public Map<String, IFATransition> getPreciseMatchTransition(IFAState state) {
    if (state.isInitial()) {
      return initialTransition;
    }
    return preciseMatchTransitionTable[state.getIndex()];
  }

  @Override
  public Iterator<IFATransition> getPreciseMatchTransitionIterator(IFAState state) {
    if (preciseMatchTransitionTable[state.getIndex()].isEmpty()) {
      return Collections.emptyIterator();
    } else {
      return new SingleTransitionIterator(states[state.getIndex() + 1]);
    }
  }

  @Override
  public List<IFATransition> getFuzzyMatchTransition(IFAState state) {
    if (state.isInitial()) {
      return Collections.emptyList();
    }
    return fuzzyMatchTransitionTable[state.getIndex()];
  }

  @Override
  public IFAState getNextState(IFAState sourceState, IFATransition transition) {
    return (SimpleNFAState) transition;
  }

  @Override
  public IFAState getInitialState() {
    return initialState;
  }

  @Override
  public int getStateSize() {
    return states.length;
  }

  @Override
  public IFAState getState(int index) {
    return states[index];
  }

  // Each node in raw nodes of path pattern maps to a state.
  // Since the transition is defined by the node of next state, we directly let this class implement
  // IFATransition.
  private abstract class SimpleNFAState implements IFAState, IFATransition {

    protected final int patternIndex;

    private SimpleNFAState(int patternIndex) {
      this.patternIndex = patternIndex;
    }

    @Override
    public boolean isInitial() {
      return patternIndex == -1;
    }

    @Override
    public boolean isFinal() {
      return patternIndex >= nodes.length - 1;
    }

    @Override
    public int getIndex() {
      return patternIndex;
    }

    @Override
    public String getValue() {
      return nodes[patternIndex];
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SimpleNFAState nfaState = (SimpleNFAState) o;
      return patternIndex == nfaState.patternIndex;
    }

    @Override
    public int hashCode() {
      return patternIndex;
    }
  }

  private class InitialState extends SimpleNFAState {

    private InitialState() {
      super(-1);
    }

    @Override
    public boolean isInitial() {
      return true;
    }

    @Override
    public boolean isMatch(String event) {
      return false;
    }
  }

  /**
   * This state may map to prefix match state with patternIndex == nodes.length, or path wildcard
   * state with nodes[patternIndex] is MULTI_LEVEL_PATH_WILDCARD or ONE_LEVEL_PATH_WILDCARD.
   */
  private class AllMatchState extends SimpleNFAState {

    private AllMatchState(int patternIndex) {
      super(patternIndex);
    }

    @Override
    public boolean isMatch(String event) {
      return true;
    }
  }

  /** nodes[patternIndex] contains *, like d*. */
  private class RegexMatchState extends SimpleNFAState {

    private RegexMatchState(int patternIndex) {
      super(patternIndex);
    }

    @Override
    public boolean isMatch(String event) {
      return regexPatternMap.get(patternIndex).matcher(event).matches();
    }
  }

  /** nodes[patternIndex] is a specified name */
  private class NameMatchState extends SimpleNFAState {

    private NameMatchState(int patternIndex) {
      super(patternIndex);
    }

    @Override
    public boolean isMatch(String event) {
      return nodes[patternIndex].equals(event);
    }
  }

  private class SingleTransitionIterator implements Iterator<IFATransition> {

    private IFATransition transition;

    private SingleTransitionIterator(IFATransition transition) {
      this.transition = transition;
    }

    @Override
    public boolean hasNext() {
      return transition != null;
    }

    @Override
    public IFATransition next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      IFATransition result = transition;
      transition = null;
      return result;
    }
  }
}
