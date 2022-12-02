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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.dfa.DFAState;
import org.apache.iotdb.commons.path.dfa.IDFAState;
import org.apache.iotdb.commons.path.dfa.IDFATransition;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NFAGraph {
  private final List<IDFAState> nfaStateList = new ArrayList<>();
  private final Map<IDFATransition, List<List<IDFAState>>> nfaTransitionTable = new HashMap<>();

  public NFAGraph(PartialPath pathPattern, Map<String, IDFATransition> transitionMap) {
    // init start state, index=0
    int index = 0;
    nfaStateList.add(new DFAState(index));
    for (IDFATransition transition : transitionMap.values()) {
      nfaTransitionTable.put(transition, new ArrayList<>());
      nfaTransitionTable.get(transition).add(new ArrayList<>());
    }
    // traverse pathPattern and construct NFA
    for (int i = 0; i < pathPattern.getNodeLength(); i++) {
      String node = pathPattern.getNodes()[i];
      int preIndex = index;
      // if it is tail node, transit to final state
      DFAState state =
          i == pathPattern.getNodeLength() - 1
              ? new DFAState(++index, true)
              : new DFAState(++index);
      nfaStateList.add(state);
      for (IDFATransition transition : transitionMap.values()) {
        nfaTransitionTable.get(transition).add(new ArrayList<>());
      }
      // construct transition
      if (IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(node)) {
        for (IDFATransition transition : transitionMap.values()) {
          nfaTransitionTable.get(transition).get(preIndex).add(state);
        }
      } else if (IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD.equals(node)) {
        for (IDFATransition transition : transitionMap.values()) {
          nfaTransitionTable.get(transition).get(preIndex).add(state);
          nfaTransitionTable.get(transition).get(index).add(state);
        }
      } else {
        nfaTransitionTable.get(transitionMap.get(node)).get(preIndex).add(state);
      }
    }
  }

  public void print(Map<String, IDFATransition> transitionMap) {
    System.out.println();
    System.out.println();
    System.out.println("NFA:");
    System.out.println(
        "==================================================================================================");
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(String.format("|%-15s|", "State"));
    for (IDFATransition transfer : transitionMap.values()) {
      stringBuilder.append(String.format("%-15s|", transfer.toString()));
    }
    stringBuilder.append(String.format("%-15s|", "Final"));
    System.out.println(stringBuilder);
    for (int i = 0; i < nfaStateList.size(); i++) {
      stringBuilder = new StringBuilder();
      stringBuilder.append(String.format("|%-15d|", i));
      for (IDFATransition transition : transitionMap.values()) {
        stringBuilder.append(
            String.format(
                "%-15s|",
                StringUtils.join(
                    nfaTransitionTable.get(transition).get(i).stream()
                        .map(IDFAState::getIndex)
                        .collect(Collectors.toList()),
                    ",")));
      }
      stringBuilder.append(String.format("%-15s|", nfaStateList.get(i).isFinal()));
      System.out.println(stringBuilder);
    }
  }

  public List<List<IDFAState>> getTransitionColumn(IDFATransition transition) {
    return nfaTransitionTable.get(transition);
  }
}
