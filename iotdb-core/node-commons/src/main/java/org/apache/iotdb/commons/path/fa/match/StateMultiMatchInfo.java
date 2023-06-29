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

package org.apache.iotdb.commons.path.fa.match;

import org.apache.iotdb.commons.path.fa.IFAState;
import org.apache.iotdb.commons.path.fa.IFATransition;
import org.apache.iotdb.commons.path.fa.IPatternFA;

import java.util.Iterator;

/** This class is used for cases need traceback. */
public class StateMultiMatchInfo implements IStateMatchInfo {

  private final IPatternFA patternFA;

  private final MatchedStateSet matchedStateSet;

  private int sourceStateIndex;

  private Iterator<IFATransition> sourceTransitionIterator;

  private boolean hasFinalState = false;

  public StateMultiMatchInfo(IPatternFA patternFA) {
    this.patternFA = patternFA;
    matchedStateSet = new MatchedStateSet(patternFA.getStateSize());
  }

  public StateMultiMatchInfo(
      IPatternFA patternFA,
      IFAState matchedState,
      Iterator<IFATransition> sourceTransitionIterator) {
    this.patternFA = patternFA;
    matchedStateSet = new MatchedStateSet(patternFA.getStateSize());
    matchedStateSet.add(matchedState);
    sourceStateIndex = 0;
    this.sourceTransitionIterator = sourceTransitionIterator;
    this.hasFinalState = matchedState.isFinal();
  }

  @Override
  public boolean hasFinalState() {
    return hasFinalState;
  }

  @Override
  public boolean hasOnlyPreciseMatchTransition() {
    return false;
  }

  @Override
  public boolean hasNoPreciseMatchTransition() {
    return false;
  }

  @Override
  public boolean isSingleFuzzyMatchTransition() {
    return false;
  }

  @Override
  public IFAState getOneMatchedState() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addMatchedState(IFAState state) {
    matchedStateSet.add(state);
    if (state.isFinal()) {
      hasFinalState = true;
    }
  }

  @Override
  public IFAState getMatchedState(int stateOrdinal) {
    return patternFA.getState(matchedStateSet.getStateIndex(stateOrdinal));
  }

  @Override
  public int getMatchedStateSize() {
    return matchedStateSet.size();
  }

  @Override
  public int getSourceStateOrdinal() {
    return sourceStateIndex;
  }

  @Override
  public void setSourceStateOrdinal(int sourceStateOrdinal) {
    this.sourceStateIndex = sourceStateOrdinal;
  }

  @Override
  public Iterator<IFATransition> getSourceTransitionIterator() {
    return sourceTransitionIterator;
  }

  @Override
  public void setSourceTransitionIterator(Iterator<IFATransition> sourceTransitionIterator) {
    this.sourceTransitionIterator = sourceTransitionIterator;
  }
}
