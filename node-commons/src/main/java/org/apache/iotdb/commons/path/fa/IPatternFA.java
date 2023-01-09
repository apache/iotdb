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

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * This interface defines the behaviour of a FA(Finite Automation), generated from a path pattern or
 * a pattern tree.
 */
public interface IPatternFA {

  /**
   * @param state the source state of the returned transitions
   * @return transitions, that the given state has and only match one specified event rather than
   *     batch events
   */
  Map<String, IFATransition> getPreciseMatchTransition(IFAState state);

  /**
   * @param state the source state of the returned transitions
   * @return transitions, that the given state has and only match one specified event rather than
   *     batch events
   */
  Iterator<IFATransition> getPreciseMatchTransitionIterator(IFAState state);

  /**
   * @param state state the source state of the returned transitions
   * @return transitions, that the given state has and can match batch events
   */
  Iterator<IFATransition> getFuzzyMatchTransitionIterator(IFAState state);

  /**
   * @param state state the source state of the returned transitions
   * @return num of transitions, that the given state has and can match batch events
   */
  int getFuzzyMatchTransitionSize(IFAState state);

  /**
   * @param sourceState source state
   * @param transition transition that the source state has
   * @return next state
   */
  IFAState getNextState(IFAState sourceState, IFATransition transition);

  /** @return the initial state of this FA */
  IFAState getInitialState();

  /** @return the size of states this FA has */
  int getStateSize();

  /**
   * @param index the index of the target state, used for uniquely identifying states in FA
   * @return the state identified by given index
   */
  IFAState getState(int index);

  /**
   * Determines if there is overlap between the state transfer events of this FA. If it returns
   * true, then there may be overlap. If it returns false, there must be no overlap.
   *
   * @return may transition overlap
   */
  boolean mayTransitionOverlap();

  final class Builder {
    private PartialPath pathPattern;
    private boolean isPrefixMatch = false;

    public Builder() {}

    public Builder pattern(PartialPath pattern) {
      this.pathPattern = pattern;
      return this;
    }

    public Builder isPrefixMatch(boolean isPrefixMatch) {
      this.isPrefixMatch = isPrefixMatch;
      return this;
    }

    public PartialPath getPathPattern() {
      return pathPattern;
    }

    public boolean isPrefixMatch() {
      return isPrefixMatch;
    }

    public IPatternFA buildNFA() {
      return FAFactory.getInstance().constructNFA(this);
    }

    public IPatternFA buildDFA() {
      return FAFactory.getInstance().constructDFA(this);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Builder builder = (Builder) o;
      return isPrefixMatch == builder.isPrefixMatch
          && Objects.equals(pathPattern, builder.pathPattern);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pathPattern, isPrefixMatch);
    }
  }
}
