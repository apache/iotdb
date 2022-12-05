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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.dfa.IFAState;
import org.apache.iotdb.commons.path.dfa.IFATransition;
import org.apache.iotdb.commons.path.dfa.IPatternFA;
import org.apache.iotdb.commons.path.dfa.PatternDFA;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PatternDFATest {
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
      //                "root.s*.d1.s1",
      //                "root.*g1.d1.s1",
      //                "root.s*.d1.*",
      //                "root.s*.d*.s*",
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
      //                "root.*.d*.s2",
      //                "root.*.a*.s1",
      "root.*",
      "root.*.*",
      //                "root.s*.d*.a*",
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

  // TODO: remove this
  @Test
  public void test() throws IllegalPathException {

    Assert.assertFalse(
        new PartialPath("root.s*.d1.s1").matchFullPath(new PartialPath("root.sg1.d1.s1")));
    //    Assert.assertFalse(
    //        checkMatchUsingDFA(new PartialPath("root.*"), new PartialPath("root.sg1.d1.s1")));
  }

  private boolean checkMatchUsingDFA(PartialPath pattern, PartialPath fullPath) {
    IPatternFA patternDFA = new PatternDFA.Builder().pattern(pattern).build();
    IFAState curState = patternDFA.getInitialState();
    for (String node : fullPath.getNodes()) {
      List<IFATransition> transitionList = patternDFA.getTransition(curState);
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
