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
package org.apache.iotdb.commons.path;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.fa.IFAState;
import org.apache.iotdb.commons.path.fa.IFATransition;
import org.apache.iotdb.commons.path.fa.IPatternFA;
import org.apache.iotdb.commons.path.fa.dfa.PatternDFA;
import org.apache.iotdb.commons.path.fa.dfa.graph.DFAGraph;
import org.apache.iotdb.commons.path.fa.dfa.graph.NFAGraph;
import org.apache.iotdb.commons.path.fa.dfa.transition.DFAPreciseTransition;
import org.apache.iotdb.commons.path.fa.dfa.transition.DFAWildcardTransition;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PatternDFATest {

  @Test
  @Ignore
  public void printFASketch() throws IllegalPathException {
    // Map<AcceptEvent, IFATransition>
    Map<String, IFATransition> transitionMap = new HashMap<>();
    PartialPath pathPattern = new PartialPath("root.**.d.s1");
    // 1. build transition
    boolean wildcard = false;
    AtomicInteger transitionIndex = new AtomicInteger();
    for (String node : pathPattern.getNodes()) {
      if (IoTDBConstant.ONE_LEVEL_PATH_WILDCARD.equals(node)
          || IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD.equals(node)) {
        wildcard = true;
      } else {
        transitionMap.computeIfAbsent(
            node, i -> new DFAPreciseTransition(transitionIndex.getAndIncrement(), node));
      }
    }
    if (wildcard) {
      IFATransition transition =
          new DFAWildcardTransition(
              transitionIndex.getAndIncrement(), new ArrayList<>(transitionMap.keySet()));
      transitionMap.put(transition.getAcceptEvent(), transition);
    }
    // 2. build NFA
    NFAGraph nfaGraph = new NFAGraph(pathPattern, false, transitionMap);
    nfaGraph.print(transitionMap);
    // 3. NFA to DFA
    DFAGraph dfaGraph = new DFAGraph(nfaGraph, transitionMap.values());
    dfaGraph.print(transitionMap);
  }

  @Test
  public void testMatchFullPath() throws IllegalPathException {
    PartialPath p1 = new PartialPath("root.sg1.d1.*");

    Assert.assertTrue(p1.matchFullPath(new PartialPath("root.sg1.d1.s2")));
    Assert.assertTrue(checkMatchUsingDFA(p1, new PartialPath("root.sg1.d1.s2")));
    Assert.assertFalse(p1.matchFullPath(new PartialPath("root.sg1.d1")));
    Assert.assertFalse(checkMatchUsingDFA(p1, new PartialPath("root.sg1.d1")));
    Assert.assertFalse(p1.matchFullPath(new PartialPath("root.sg2.d1.*")));
    Assert.assertFalse(checkMatchUsingDFA(p1, new PartialPath("root.sg2.d1.*")));
    Assert.assertFalse(p1.matchFullPath(new PartialPath("", false)));
    Assert.assertFalse(checkMatchUsingDFA(p1, new PartialPath("", false)));

    PartialPath path = new PartialPath("root.sg1.d1.s1");
    String[] patterns1 = {
      "root.sg1.d1.s1",
      "root.sg1.*.s1",
      "root.*.d1.*",
      "root.*.*.*",
      "root.**",
      "root.**.s1",
      "root.sg1.**",
    };
    for (String pattern : patterns1) {
      Assert.assertTrue(new PartialPath(pattern).matchFullPath(path));
      Assert.assertTrue(checkMatchUsingDFA(new PartialPath(pattern), path));
    }

    String[] patterns2 = {
      "root2.sg1.d1.s1",
      "root.sg1.*.s2",
      "root.*.d2.s1",
      "root.*",
      "root.*.*",
      "root2.**",
      "root.**.s2",
      "root.**.d1",
      "root.sg2.**",
    };
    for (String pattern : patterns2) {
      Assert.assertFalse(new PartialPath(pattern).matchFullPath(path));
      Assert.assertFalse(checkMatchUsingDFA(new PartialPath(pattern), path));
    }
  }

  private boolean checkMatchUsingDFA(PartialPath pattern, PartialPath fullPath) {
    PatternDFA patternDFA =
        (PatternDFA) new IPatternFA.Builder().pattern(pattern).isPrefixMatch(false).buildDFA();
    IFAState curState = patternDFA.getInitialState();
    for (String node : fullPath.getNodes()) {
      Iterator<IFATransition> preciseMatchTransitionIterator =
          patternDFA.getPreciseMatchTransitionIterator(curState);
      Iterator<IFATransition> batchMatchTransitionIterator =
          patternDFA.getFuzzyMatchTransitionIterator(curState);
      List<IFATransition> transitionList = patternDFA.getTransition(curState);
      while (preciseMatchTransitionIterator.hasNext()) {
        IFATransition transition = preciseMatchTransitionIterator.next();
        Assert.assertTrue(transitionList.contains(transition));
      }
      while (batchMatchTransitionIterator.hasNext()) {
        IFATransition transition = batchMatchTransitionIterator.next();
        Assert.assertTrue(transitionList.contains(transition));
      }

      if (transitionList.isEmpty()) {
        return false;
      }
      for (IFATransition transition : transitionList) {
        if (transition.isMatch(node)) {
          curState = patternDFA.getNextState(curState, transition);
          break;
        }
      }
    }
    return curState.isFinal();
  }
}
