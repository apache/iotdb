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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * both state[2] and state[3] are final states
 */
public class SimpleNFA implements IPatternFA {

  // raw nodes of pathPattern
  private final String[] nodes;

  // used for pattern node like d*
  private final Map<Integer, Pattern> regexPatternMap = new HashMap<>();

  // initial state of this NFA and the only transition from this state is "root"
  private final SimpleNFAState initialState = new SimpleNFAState(-1);
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
    this.nodes = optimizePathPattern(pathPattern);

    states = new SimpleNFAState[this.nodes.length + 1];
    for (int i = 0; i < states.length; i++) {
      states[i] = new SimpleNFAState(i);
    }

    initialTransition = Collections.singletonMap(nodes[0], states[0]);

    preciseMatchTransitionTable = new Map[states.length];
    fuzzyMatchTransitionTable = new List[states.length];

    generateFATransition(isPrefixMatch);
  }

  private void generateFATransition(boolean isPrefixMatch) {
    // the index of the last multiLevelWildcard before current index
    int lastMultiWildcard = -1;
    for (int i = 0; i < nodes.length - 1; i++) {
      if (nodes[i].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        lastMultiWildcard = i;
      }
      if (lastMultiWildcard == -1) {
        if (nodes[i + 1].contains(ONE_LEVEL_PATH_WILDCARD)) {
          fuzzyMatchTransitionTable[i] = Collections.singletonList(states[i + 1]);
          preciseMatchTransitionTable[i] = Collections.emptyMap();
        } else {
          fuzzyMatchTransitionTable[i] = Collections.emptyList();
          preciseMatchTransitionTable[i] = Collections.singletonMap(nodes[i + 1], states[i + 1]);
        }
      } else {
        // with dfs or greedy strategy, the state should be advanced as possible
        if (nodes[i + 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
          fuzzyMatchTransitionTable[i] = Collections.singletonList(states[i + 1]);
          preciseMatchTransitionTable[i] = Collections.emptyMap();
        } else if (nodes[i + 1].contains(ONE_LEVEL_PATH_WILDCARD)) {
          fuzzyMatchTransitionTable[i] = Arrays.asList(states[i + 1], states[lastMultiWildcard]);
          preciseMatchTransitionTable[i] = Collections.emptyMap();
        } else {
          fuzzyMatchTransitionTable[i] = Collections.singletonList(states[lastMultiWildcard]);
          preciseMatchTransitionTable[i] = Collections.singletonMap(nodes[i + 1], states[i + 1]);
        }
      }
    }

    if (isPrefixMatch) {
      preciseMatchTransitionTable[nodes.length - 1] = Collections.emptyMap();
      fuzzyMatchTransitionTable[nodes.length - 1] = Collections.singletonList(states[nodes.length]);
    } else if (nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
      preciseMatchTransitionTable[nodes.length - 1] = Collections.emptyMap();
      fuzzyMatchTransitionTable[nodes.length - 1] =
          Collections.singletonList(states[nodes.length - 1]);
    } else {
      preciseMatchTransitionTable[nodes.length - 1] = Collections.emptyMap();
      if (lastMultiWildcard == -1) {
        fuzzyMatchTransitionTable[nodes.length - 1] = Collections.emptyList();
      } else {
        fuzzyMatchTransitionTable[nodes.length - 1] =
            Collections.singletonList(states[lastMultiWildcard]);
      }
    }

    preciseMatchTransitionTable[nodes.length] = Collections.emptyMap();
    fuzzyMatchTransitionTable[nodes.length] = Collections.singletonList(states[nodes.length]);
  }

  /**
   * Optimize the given path pattern. Currently, the node name used for one level match will be
   * transformed into a regex. e.g. given pathPattern {"root", "sg", "d*", "s"} and the
   * optimizedPathPattern is {"root", "sg", "d.*", "s"}.
   */
  private String[] optimizePathPattern(PartialPath pathPattern) {
    String[] rawNodes = pathPattern.getNodes();
    List<String> optimizedNodes = new ArrayList<>(rawNodes.length);
    for (String rawNode : rawNodes) {
      if (rawNode.equals(MULTI_LEVEL_PATH_WILDCARD)) {
        optimizedNodes.add(MULTI_LEVEL_PATH_WILDCARD);
      } else if (rawNode.equals(ONE_LEVEL_PATH_WILDCARD)) {
        optimizedNodes.add(ONE_LEVEL_PATH_WILDCARD);
      } else if (rawNode.contains(ONE_LEVEL_PATH_WILDCARD)) {
        optimizedNodes.add(rawNode.replace("*", ".*"));
        regexPatternMap.put(
            optimizedNodes.size() - 1,
            Pattern.compile(optimizedNodes.get(optimizedNodes.size() - 1)));
      } else {
        optimizedNodes.add(rawNode);
      }
    }

    return optimizedNodes.toArray(new String[0]);
  }

  @Override
  public Map<String, IFATransition> getPreciseMatchTransition(IFAState state) {
    if (state.isInitial()) {
      return initialTransition;
    }
    return preciseMatchTransitionTable[((SimpleNFAState) state).patternIndex];
  }

  @Override
  public List<IFATransition> getFuzzyMatchTransition(IFAState state) {
    if (state.isInitial()) {
      return Collections.emptyList();
    }
    return fuzzyMatchTransitionTable[((SimpleNFAState) state).patternIndex];
  }

  @Override
  public IFAState getNextState(IFAState currentState, IFATransition transition) {
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
  private class SimpleNFAState implements IFAState, IFATransition {

    private final int patternIndex;

    SimpleNFAState(int patternIndex) {
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
    public boolean isMatch(String event) {
      if (patternIndex == nodes.length) {
        return true;
      }
      if (nodes[patternIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        return true;
      }
      if (nodes[patternIndex].equals(ONE_LEVEL_PATH_WILDCARD)) {
        return true;
      }
      if (nodes[patternIndex].contains(ONE_LEVEL_PATH_WILDCARD)) {
        return regexPatternMap.get(patternIndex).matcher(event).matches();
      }
      return nodes[patternIndex].equals(event);
    }

    @Override
    public boolean isFuzzy() {
      return patternIndex == nodes.length || nodes[patternIndex].contains(ONE_LEVEL_PATH_WILDCARD);
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
}
