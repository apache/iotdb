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

/** This class is only used for cases not need traceback yet. */
public class StateSingleMatchInfo implements IStateMatchInfo {

  private final IPatternFA patternFA;

  private final IFAState matchedState;

  public StateSingleMatchInfo(IPatternFA patternFA, IFAState matchedState) {
    this.patternFA = patternFA;
    this.matchedState = matchedState;
  }

  @Override
  public boolean hasFinalState() {
    return matchedState.isFinal();
  }

  @Override
  public boolean hasOnlyPreciseMatchTransition() {
    return patternFA.getFuzzyMatchTransitionSize(matchedState) == 0;
  }

  @Override
  public boolean hasNoPreciseMatchTransition() {
    return patternFA.getPreciseMatchTransition(matchedState).isEmpty();
  }

  @Override
  public boolean isSingleFuzzyMatchTransition() {
    return patternFA.getFuzzyMatchTransitionSize(matchedState) == 1;
  }

  @Override
  public IFAState getOneMatchedState() {
    return matchedState;
  }

  @Override
  public void addMatchedState(IFAState state) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IFAState getMatchedState(int stateOrdinal) {
    if (stateOrdinal == 0) {
      return matchedState;
    } else {
      throw new IllegalStateException();
    }
  }

  @Override
  public int getMatchedStateSize() {
    return 1;
  }

  @Override
  public int getSourceStateOrdinal() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSourceStateOrdinal(int sourceStateOrdinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<IFATransition> getSourceTransitionIterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSourceTransitionIterator(Iterator<IFATransition> sourceTransitionIterator) {
    throw new UnsupportedOperationException();
  }
}
