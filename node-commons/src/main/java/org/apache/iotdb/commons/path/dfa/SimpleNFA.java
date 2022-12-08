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

public class SimpleNFA implements IPatternFA {

  private final String[] nodes;
  private final boolean isPrefixMatch;

  private final SimpleNFAState[] states;

  private final SimpleNFAState initialState = new SimpleNFAState(-1);

  private final Map<Integer, Pattern> patternMap = new HashMap<>();

  private final Map<String, IFATransition> initialTransition;

  private final Map<String, IFATransition>[] preciseMatchTransition;

  private final List<IFATransition>[] batchMatchTransition;

  public SimpleNFA(PartialPath pathPattern, boolean isPrefixMatch) {
    this.nodes = optimizePathPattern(pathPattern);
    this.isPrefixMatch = isPrefixMatch;

    states = new SimpleNFAState[this.nodes.length + 1];
    for (int i = 0; i < states.length; i++) {
      states[i] = new SimpleNFAState(i);
    }

    initialTransition = Collections.singletonMap(nodes[0], states[0]);

    preciseMatchTransition = new Map[states.length];
    batchMatchTransition = new List[states.length];

    for (int i = 0; i < nodes.length - 1; i++) {
      if (nodes[i].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        if (nodes[i + 1].contains(ONE_LEVEL_PATH_WILDCARD)) {
          batchMatchTransition[i] = Arrays.asList(states[i + 1], states[i]);
          preciseMatchTransition[i] = Collections.emptyMap();
        } else {
          batchMatchTransition[i] = Collections.singletonList(states[i]);
          preciseMatchTransition[i] = Collections.singletonMap(nodes[i + 1], states[i + 1]);
        }
      } else {
        if (nodes[i + 1].contains(ONE_LEVEL_PATH_WILDCARD)) {
          batchMatchTransition[i] = Collections.singletonList(states[i + 1]);
          preciseMatchTransition[i] = Collections.emptyMap();
        } else {
          batchMatchTransition[i] = Collections.emptyList();
          preciseMatchTransition[i] = Collections.singletonMap(nodes[i + 1], states[i + 1]);
        }
      }
    }

    if (isPrefixMatch) {
      preciseMatchTransition[nodes.length - 1] = Collections.emptyMap();
      batchMatchTransition[nodes.length - 1] = Collections.singletonList(states[nodes.length]);
    } else if (nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
      preciseMatchTransition[nodes.length - 1] = Collections.emptyMap();
      batchMatchTransition[nodes.length - 1] = Collections.singletonList(states[nodes.length - 1]);
    } else {
      preciseMatchTransition[nodes.length - 1] = Collections.emptyMap();
      batchMatchTransition[nodes.length - 1] = Collections.emptyList();
    }

    preciseMatchTransition[nodes.length] = Collections.emptyMap();
    batchMatchTransition[nodes.length] = Collections.singletonList(states[nodes.length]);
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
        patternMap.put(
            optimizedNodes.size() - 1,
            Pattern.compile(optimizedNodes.get(optimizedNodes.size() - 1)));
      } else {
        optimizedNodes.add(rawNode);
      }
    }

    return optimizedNodes.toArray(new String[0]);
  }

  @Override
  public List<IFATransition> getTransition(IFAState state) {
    SimpleNFAState nfaState = (SimpleNFAState) state;
    if (nfaState.isInitial()) {
      return Collections.singletonList(states[0]);
    }
    if (nfaState.patternIndex == nodes.length) {
      // prefix match
      return Collections.singletonList(nfaState);
    }

    if (nfaState.patternIndex == nodes.length - 1) {
      if (isPrefixMatch) {
        return Collections.singletonList(states[nodes.length]);
      } else if (nodes[nfaState.patternIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        return Collections.singletonList(nfaState);
      }

      return Collections.emptyList();
    }

    if (nodes[nfaState.patternIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
      return Arrays.asList(states[nfaState.patternIndex + 1], nfaState);
    } else {
      return Collections.singletonList(states[nfaState.patternIndex + 1]);
    }
  }

  @Override
  public Map<String, IFATransition> getPreciseMatchTransition(IFAState state) {
    if (state.isInitial()) {
      return initialTransition;
    }
    return preciseMatchTransition[((SimpleNFAState) state).patternIndex];
  }

  @Override
  public List<IFATransition> getBatchMatchTransition(IFAState state) {
    if (state.isInitial()) {
      return Collections.emptyList();
    }
    return batchMatchTransition[((SimpleNFAState) state).patternIndex];
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
        return patternMap.get(patternIndex).matcher(event).matches();
      }
      return nodes[patternIndex].equals(event);
    }

    @Override
    public boolean isBatch() {
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
